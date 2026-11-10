"""Build and load ``chirps_base_grid`` — the 0.05° fine grid, scoped to one extent.

The 0.05° counterpart of ``src/db/seed_grid.py``, with one structural difference: that
module enumerates the whole globe (1,038,240 cells) and ships the result as a seed. A global
0.05° grid is 17,280,000 rows, so this one enumerates **only the requested extent** and runs
at deploy time instead.

That makes the table region-scoped while the *codes* stay globally deterministic — the
"design for global, run for regional" split (PLANNING.md §2). Two consequences worth naming:

- Extents **accumulate**. Loading a second region adds its cells; it does not replace the
  first. Overlapping extents are idempotent because ``fine_id`` is the primary key and the
  row content is a pure function of it. ``--replace`` forces a clean rebuild.
- There is no land mask and no ``t_zone``. CHIRPS is a land rainfall product that masks
  ocean itself, and being a finished daily product it has no local-day offset to apply
  (``fine_spec`` D4). ``is_valid`` is left NULL and populated after the first backfill from
  cells that actually produced rows — exact by construction, and it avoids inventing a land
  mask at a resolution finer than any mask this project has.
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import numpy as np
import pandas as pd

from src.db import load as db_load
from src.grid.fine_encoding import cell_indices, fine_codes, fine_parent_codes
from src.grid.fine_spec import BLOCK_B, LAT_ORIGIN, RESOLUTION

TABLE = "chirps_base_grid"
STAGING = "_fine_grid_staging"
SCHEMA_SQL = Path(__file__).with_name("fine_grid_schema.sql")
_COLUMNS = ["fine_id", "fparent_id", "lat", "lon"]
_HALF = RESOLUTION / 2.0


def extent_indices(extent: list[float]) -> tuple[np.ndarray, np.ndarray]:
    """``(lat_idx, lon_idx)`` axes covering ``extent`` = ``[S, W, N, E]``.

    The extent is treated as a box of cell **edges**, so a boundary that lands exactly on a
    multiple of 0.05 (the usual case, since callers snap) contributes the cell inside the box
    and not the one beyond it. Both bounds are inclusive of any cell the box touches.

    **The box is exact, and that is enough to cover the export.** The GEE raster comes back
    one row and one column *larger* than the box: ``export._land_region`` intersects the bbox
    with LSIB at ``max_error=1000.0`` m, and that simplification lets the clipped geometry
    bulge past a bbox edge into the next pixel. Those extra pixels are always **masked**,
    though, and never reach Parquet — a bulge is bounded by ~0.009° while a pixel's centre
    sits ``RESOLUTION / 2`` = 0.025° inside its edge, so the bulge cannot reach the centre
    that would have to be sampled. Measured on Tocantins 1981, both sources: raster
    168 x 102 = 17,136 px/day, encoded cells 16,867 = 167 x 101, dropped 269 = exactly the
    extra row and column.

    So do not pad this. An earlier version did, on the strength of the cost probe's ``cells``
    figure — but that counts raster *shape*, not valid data, and over-reported by those same
    269. The invariant to hold instead is that no ``wth_precip_alt.fine_id`` is missing from
    ``chirps_base_grid``; nothing enforces it (there is no FK), so the verification gate
    checks it in SQL.
    """
    south, west, north, east = extent
    if south >= north or west >= east:
        raise ValueError(f"extent {extent} is empty or inverted; expected [S, W, N, E]")
    if west < -180.0 or east > 180.0:
        raise ValueError(f"extent {extent} must be in -180..180; antimeridian is not split")

    # North edge -> first row; the south edge is a boundary, so step just inside it.
    lat_hi, _ = cell_indices(north - _HALF, west + _HALF)
    lat_lo, _ = cell_indices(south + _HALF, west + _HALF)
    _, lon_lo = cell_indices(north - _HALF, west + _HALF)
    _, lon_hi = cell_indices(north - _HALF, east - _HALF)

    return (
        np.arange(lat_hi, lat_lo + 1, dtype=np.int64),
        np.arange(lon_lo, lon_hi + 1, dtype=np.int64),
    )


def build_rows(extent: list[float], *, b: int = BLOCK_B) -> pd.DataFrame:
    """One row per fine cell inside ``extent``: ``fine_id, fparent_id, lat, lon``."""
    lat_axis, lon_axis = extent_indices(extent)
    lat_idx, lon_idx = np.meshgrid(lat_axis, lon_axis, indexing="ij")
    lat_idx = lat_idx.ravel()
    lon_idx = lon_idx.ravel()

    lat = LAT_ORIGIN - (lat_idx + 0.5) * RESOLUTION
    lon = (lon_idx + 0.5) * RESOLUTION
    lon = np.where(lon > 180.0, lon - 360.0, lon)  # store as -180..180

    return pd.DataFrame(
        {
            "fine_id": fine_codes(lat_idx, lon_idx),
            "fparent_id": fine_parent_codes(lat_idx, lon_idx, b),
            "lat": lat,
            "lon": lon,
        }
    )


def load_grid(
    conn, df: pd.DataFrame, *, chunk_rows: int = 100_000, replace: bool = False
) -> int:
    """Apply DDL, COPY the frame into staging, upsert with geom. Returns rows in table.

    Upsert rather than truncate-and-insert, so extents accumulate (see module docstring).
    ``replace=True`` truncates first, for a rebuild after a spec change.
    """
    db_load.execute_script(conn, SCHEMA_SQL.read_text())
    with conn.cursor() as cur:
        if replace:
            cur.execute(f"TRUNCATE {TABLE}")
        cur.execute(
            f"CREATE TEMP TABLE {STAGING} ("
            "fine_id CHAR(5), fparent_id CHAR(5), "
            "lat DOUBLE PRECISION, lon DOUBLE PRECISION"
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
        # Half-cell offsets are derived from RESOLUTION rather than written as literals —
        # seed_grid.py hardcodes 0.125 and that is a trap worth not repeating.
        cur.execute(
            f"INSERT INTO {TABLE} (fine_id, fparent_id, lat, lon, geom) "
            "SELECT fine_id, fparent_id, lat, lon, "
            "ST_MakeEnvelope(lon-%s, lat-%s, lon+%s, lat+%s, 4326) "
            f"FROM {STAGING} "
            "ON CONFLICT (fine_id) DO UPDATE SET "
            "fparent_id = EXCLUDED.fparent_id, lat = EXCLUDED.lat, "
            "lon = EXCLUDED.lon, geom = EXCLUDED.geom",
            (_HALF, _HALF, _HALF, _HALF),
        )
        cur.execute(f"SELECT count(*) FROM {TABLE}")
        total = cur.fetchone()[0]
    conn.commit()
    return total


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Build and load {TABLE} for one extent.")
    ap.add_argument(
        "--extent", nargs=4, type=float, required=True, metavar=("S", "W", "N", "E")
    )
    ap.add_argument("--block-size-b", type=int, default=BLOCK_B)
    ap.add_argument(
        "--replace",
        action="store_true",
        help="truncate before loading (drops every other extent already built)",
    )
    args = ap.parse_args()

    df = build_rows(list(args.extent), b=args.block_size_b)
    print(f"built {len(df):,} fine cells over {args.extent}")
    print(f"  {df['fparent_id'].nunique():,} parents")

    conn = db_load.connect()
    try:
        total = load_grid(conn, df, replace=args.replace)
        print(f"loaded; {TABLE} now holds {total:,} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
