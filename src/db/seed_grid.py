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
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

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
_COLUMNS = ["child_id", "parent_id", "lat", "lon", "is_land", "elevation", "t_zone"]

# tzid -> standard UTC offset (minutes). Populated on demand; a handful of distinct
# zones cover the whole grid, so caching turns ~1M cell lookups into a few dozen.
_STD_OFFSET_CACHE: dict[str, int] = {}


def std_offset_minutes(tzid: str) -> int:
    """Standard (non-DST) UTC offset in minutes for an IANA ``tzid`` (§5.3).

    The pipeline is DST-free (ERA5 is a fixed grid; a cell's local day must not jump an
    hour twice a year), so we take each zone's *standard* offset — the Jan/Jul sample whose
    ``dst()`` is zero. Zones without DST return their single offset; the rare zone with no
    non-DST month in the sample falls back to the smaller-magnitude of the two.
    """
    cached = _STD_OFFSET_CACHE.get(tzid)
    if cached is not None:
        return cached
    tz = ZoneInfo(tzid)
    offsets = []
    for month in (1, 7):
        dt = datetime(2023, month, 1, tzinfo=tz)
        off = int(dt.utcoffset().total_seconds() // 60)
        if dt.dst() == timedelta(0):
            _STD_OFFSET_CACHE[tzid] = off
            return off
        offsets.append(off)
    off = min(offsets, key=abs)
    _STD_OFFSET_CACHE[tzid] = off
    return off


def _longitude_offset_minutes(lon: float) -> int:
    """Solar-time fallback offset (minutes) for cells with no tz polygon (ocean/islands)."""
    return int(round(lon / 15.0)) * 60


def assign_timezone(lat, lon, is_land) -> np.ndarray:
    """Per-cell standard UTC offset (minutes) from the political tz shapefile (§5.3).

    Land cells resolve their IANA zone by cell center via ``timezonefinder``, then map to a
    standard offset. Non-land cells (bronze is land-only) and land points outside every tz
    polygon fall back to the longitude solar offset. Returns an ``int16`` array.

    The ~700k ocean cells get the solar fallback **vectorized** (instant); only the ~300k
    land cells pay the per-point ``timezonefinder`` lookup. We use ``TimezoneFinderL`` (the
    bundled H3-shortcut variant) not the full polygon ``TimezoneFinder``: the latter does a
    point-in-polygon test per call that is ~100× slower (minutes → hours on the Pi target),
    and at 0.25° the H3 answer is identical except for a thin band of border cells — well
    within the boundary approximation we already accept. Progress is printed so the build is
    visibly alive.
    """
    from timezonefinder import TimezoneFinderL

    tf = TimezoneFinderL()
    lat = np.asarray(lat, dtype=np.float64).ravel()
    lon = np.asarray(lon, dtype=np.float64).ravel()
    is_land = np.asarray(is_land, dtype=bool).ravel()

    # Vectorized solar fallback for every cell; land cells overwritten below.
    out = (np.round(lon / 15.0).astype(np.int64) * 60).astype(np.int16)

    land_idx = np.flatnonzero(is_land)
    total = land_idx.size
    unknown: dict[str, int] = {}  # tzid the local tzdata lacks -> cell count (solar fallback)
    print(f"assign_timezone: {total:,} land cells to resolve...", flush=True)
    for k, i in enumerate(land_idx):
        tzid = tf.timezone_at(lng=float(lon[i]), lat=float(lat[i]))
        if tzid:
            try:
                out[i] = std_offset_minutes(tzid)
            except ZoneInfoNotFoundError:
                # tzid newer than the container's tzdata (e.g. America/Coyhaique). Keep the
                # solar fallback already in out[i] and report it; upgrade tzdata for exactness.
                unknown[tzid] = unknown.get(tzid, 0) + 1
        if k and k % 50_000 == 0:
            print(f"assign_timezone: {k:,}/{total:,} land cells", flush=True)
    if unknown:
        print(f"assign_timezone: {sum(unknown.values()):,} cells fell back to solar for "
              f"tzids missing from local tzdata: {sorted(unknown)}", flush=True)
    return out


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

    is_land_flat = is_land.ravel()
    return pd.DataFrame(
        {
            "child_id": cell_codes(lat_idx, lon_idx),
            "parent_id": parent_codes(lat_idx, lon_idx, b),
            "lat": lat,
            "lon": lon,
            "is_land": is_land_flat,
            "elevation": elev.ravel(),
            "t_zone": assign_timezone(lat, lon, is_land_flat),
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
            "lon DOUBLE PRECISION, is_land BOOLEAN, elevation DOUBLE PRECISION, "
            "t_zone SMALLINT"
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
            "(child_id, parent_id, lat, lon, is_land, elevation, t_zone, geom) "
            "SELECT child_id, parent_id, lat, lon, is_land, elevation, t_zone, "
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
