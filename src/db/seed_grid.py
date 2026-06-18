"""Build and load the global era5_land_base_grid (PLANNING.md §8.1, §10 DAG 1).

Enumerates all ``NLAT * NLON`` 0.25° cells vectorized, derives ``child_id`` /
``parent_id`` (reusing ``src/grid/encoding`` vectorized helpers), ``lat`` / ``lon``
centers, ``elevation`` (geopotential ``z / 9.80665``) and ``is_land`` (clip §6.4), then
bulk-loads via ``COPY`` into a staging table and computes ``geom`` in SQL with
``ST_MakeEnvelope`` — avoiding ~1M Python WKT strings.

Idempotent: applies the DDL (``CREATE TABLE IF NOT EXISTS``) and truncate-then-loads.
Run once by a maintainer to produce the shippable seed.
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.config import load_config
from src.db import load as db_load
from src.grid.encoding import cell_codes, parent_codes
from src.grid.land_mask import compute_is_land, to_latlon_grid
from src.grid.spec import LAND_FRACTION_X, LAT_ORIGIN, NLAT, NLON, RESOLUTION

GRAVITY = 9.80665  # m/s^2 — geopotential -> elevation (§5.4)
TABLE = "era5_land_base_grid"
STAGING = "_grid_staging"
SCHEMA_SQL = Path(__file__).with_name("schema.sql")
_COLUMNS = ["child_id", "parent_id", "lat", "lon", "is_land", "elevation"]


def load_elevation(geopotential_nc: str | Path | xr.Dataset | xr.DataArray) -> np.ndarray:
    """Return a ``(NLAT, NLON)`` elevation grid (meters) indexed by canonical indices.

    Cells absent from the geopotential file are left as NaN.
    """
    da = to_latlon_grid(geopotential_nc, var="z")
    lat = np.asarray(da["latitude"].values if "latitude" in da.coords else da["lat"].values)
    lon = np.asarray(da["longitude"].values if "longitude" in da.coords else da["lon"].values)
    z = np.asarray(da.values, dtype=np.float64)

    lat_i = np.rint((LAT_ORIGIN - lat) / RESOLUTION).astype(np.int64)
    lon_i = np.rint((lon % 360.0) / RESOLUTION).astype(np.int64)
    np.clip(lat_i, 0, NLAT - 1, out=lat_i)
    np.clip(lon_i, 0, NLON - 1, out=lon_i)

    elev = np.full((NLAT, NLON), np.nan, dtype=np.float64)
    elev[np.ix_(lat_i, lon_i)] = z / GRAVITY
    return elev


def build_rows(
    geopotential_nc: str | Path | xr.Dataset | xr.DataArray,
    land_mask_nc: str | Path | xr.Dataset | xr.DataArray,
    *,
    b: int = 4,
    fraction_x: float = LAND_FRACTION_X,
) -> pd.DataFrame:
    """Build the full global grid frame (one row per 0.25° cell)."""
    elev = load_elevation(geopotential_nc)                       # (NLAT, NLON)
    is_land = compute_is_land(land_mask_nc, fraction_x=fraction_x)  # (NLAT, NLON)

    lat_idx, lon_idx = np.meshgrid(
        np.arange(NLAT, dtype=np.int64),
        np.arange(NLON, dtype=np.int64),
        indexing="ij",
    )
    lat_idx = lat_idx.ravel()
    lon_idx = lon_idx.ravel()

    lat = LAT_ORIGIN - lat_idx * RESOLUTION
    lon = lon_idx * RESOLUTION
    lon = np.where(lon > 180.0, lon - 360.0, lon)               # store as -180..180

    return pd.DataFrame(
        {
            "child_id": cell_codes(lat_idx, lon_idx),
            "parent_id": parent_codes(lat_idx, lon_idx, b),
            "lat": lat,
            "lon": lon,
            "is_land": is_land.ravel(),
            "elevation": elev.ravel(),
        }
    )


def load_grid(conn, df: pd.DataFrame, *, chunk_rows: int = 100_000) -> None:
    """Apply DDL, truncate, COPY the frame into staging, and insert with geom."""
    db_load.execute_script(conn, SCHEMA_SQL.read_text())
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {TABLE}")
        cur.execute(
            f"CREATE TEMP TABLE {STAGING} ("
            "child_id CHAR(4), parent_id CHAR(4), lat DOUBLE PRECISION, "
            "lon DOUBLE PRECISION, is_land BOOLEAN, elevation DOUBLE PRECISION"
            ") ON COMMIT DROP"
        )

    for start in range(0, len(df), chunk_rows):
        buf = io.StringIO()
        df.iloc[start : start + chunk_rows].to_csv(
            buf, columns=_COLUMNS, index=False, header=False
        )
        buf.seek(0)
        db_load.copy_csv(conn, STAGING, _COLUMNS, buf)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TABLE} "
            "(child_id, parent_id, lat, lon, is_land, elevation, geom) "
            "SELECT child_id, parent_id, lat, lon, is_land, elevation, "
            "ST_MakeEnvelope(lon-0.125, lat-0.125, lon+0.125, lat+0.125, 4326) "
            f"FROM {STAGING}"
        )
    conn.commit()


def main() -> None:
    cfg = load_config()
    ap = argparse.ArgumentParser(description="Build and load era5_land_base_grid.")
    ap.add_argument(
        "--geopotential",
        default=str(cfg.paths.bronze_static_dir / "geopotential.nc"),
    )
    ap.add_argument(
        "--land-mask",
        default=str(cfg.paths.bronze_static_dir / "era5_land_mask.nc"),
    )
    ap.add_argument("--block-size-b", type=int, default=4)
    ap.add_argument("--land-fraction-x", type=float, default=LAND_FRACTION_X)
    args = ap.parse_args()

    df = build_rows(
        args.geopotential,
        args.land_mask,
        b=args.block_size_b,
        fraction_x=args.land_fraction_x,
    )
    print(f"built {len(df):,} cells; {int(df['is_land'].sum()):,} land")

    conn = db_load.connect()
    try:
        load_grid(conn, df)
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {TABLE}")
            print(f"loaded {cur.fetchone()[0]:,} rows into {TABLE}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
