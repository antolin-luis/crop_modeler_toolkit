"""Load ``soil_profile_points`` from the SoilGrids-for-DSSAT point layer, and build the
``soil_era5_map`` bridge to the 0.25° weather grid.

The source (``Point5m_SoilGrids-for-DSSAT-10km_v1.shp.zip``) is a global 5 arc-min point
shapefile, 1,984,797 land points, whose ``SoilProfil`` attribute is a DSSAT ``ID_SOIL`` —
the value that goes in a FILEX. Pairing that with the weather side is the whole point: a
gold-layer caller that has a ``child_id`` and wants a ``.WTH`` plus its matching soil
profile gets both from one equality join.

Three structural choices, each following an existing module:

- **Only the ``.dbf`` is read.** X/Y are attributes, so the 55 MB ``.shp`` carries no
  information the DBF does not. ``src/grid/dbf.py`` streams it out of the zip in chunks;
  nothing is unpacked to disk and peak memory is set by ``chunk_rows``.
- **Geometry is built in SQL**, via ``ST_MakePoint`` in the final ``INSERT ... SELECT``,
  for the reason ``seed_grid`` gives: two million Python WKT strings are pure waste when
  Postgres already has the numbers.
- **``child_id`` is arithmetic, not spatial.** The soil points are 5 arc-min centres, not
  ERA5 centres, so they are snapped to the nearest 0.25° centre and encoded with the same
  vectorized encoders the grid itself was built with. ``geom`` and its GiST index stay
  reserved for real polygon work, per ``grid_query``'s note.

Unlike ``seed_fine_grid``, this is a **global truncate-and-load**: the whole layer is only
~2 M rows (~250 MB with indexes), so there is no reason to scope it to an extent and then
have to remember which extents were built.
"""

from __future__ import annotations

import argparse
import io
import os
from pathlib import Path

import numpy as np
import pandas as pd

from src.db import load as db_load
from src.grid import dbf
from src.grid.encode_long import DEFAULT_BLOCK_SIZE_B
from src.grid.encoding import cell_codes, parent_codes
from src.grid.spec import LAT_ORIGIN, NLAT, NLON, RESOLUTION

TABLE = "soil_profile_points"
MAP_TABLE = "soil_era5_map"
GRID_TABLE = "era5_land_base_grid"
STAGING = "_soil_staging"
SCHEMA_SQL = Path(__file__).with_name("soil_schema.sql")
# Lives beside the schema rather than in sql/ so the DAG can reach it: only ./src and
# ./airflow/dags are mounted into the Airflow containers. Importing it by hand in DBeaver
# still works — it is the same file, just a different path.
HELPERS_SQL = Path(__file__).with_name("soil_helpers.sql")

DEFAULT_SOURCE = "Point5m_SoilGrids-for-DSSAT-10km_v1.shp.zip"

# Source attribute -> column. The source names are the shapefile's own (10-char DBF limit
# is why it is "SoilProfil"); everything downstream sees the schema names instead.
_SOURCE_COLUMNS = {"CELL5M": "cell5m", "SoilProfil": "soil_id", "X": "lon", "Y": "lat",
                   "ISO2": "iso2"}
_COLUMNS = ["cell5m", "soil_id", "iso2", "lat", "lon", "child_id", "parent_id"]


def resolve_source(source: str | Path | None = None) -> Path:
    """Locate the source archive, anchoring a bare name to ``$DATA_DIR/bronze/static``.

    Same contract as ``config.resolve_bronze_dir``, and for the same reason: only
    ``$DATA_DIR`` is bind-mounted into the Airflow containers, so a path outside it either
    does not exist there or does not survive a container recreate. A bare filename is
    anchored to the static-inputs directory (where the geopotential and land-mask ``.nc``
    already live), never to the process's working directory.

    ``DATA_DIR`` is read from the environment rather than through ``load_config()`` so a
    soil-only run does not require ``CDS_KEY`` to be set.
    """
    data_dir = Path(os.path.normpath(os.environ.get("DATA_DIR", "/data")))
    static_dir = data_dir / "bronze" / "static"

    name = str(source or DEFAULT_SOURCE)
    path = Path(os.path.normpath(name))
    if not path.is_absolute():
        path = Path(os.path.normpath(static_dir / path))
    if not path.is_relative_to(data_dir):
        raise ValueError(
            f"source {name!r} resolves to {str(path)!r}, outside DATA_DIR "
            f"({str(data_dir)!r}) — that path is not on the mounted volume and is not "
            f"visible to the Airflow containers. Pass a bare filename, e.g. "
            f"{path.name!r}, which means {str(static_dir / path.name)!r}."
        )
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found. Copy the shapefile archive there, e.g.\n"
            f"    cp {DEFAULT_SOURCE} .localdata/bronze/static/"
        )
    return path


def build_rows(chunk: pd.DataFrame, *, b: int = DEFAULT_BLOCK_SIZE_B) -> pd.DataFrame:
    """One DBF chunk -> the frame that gets COPYed, ``child_id``/``parent_id`` included.

    The pure half of this module, and the one worth testing offline. Points are snapped to
    the nearest 0.25° cell centre before encoding: ``encoding.cell_code``'s contract is
    that inputs *are* centres, and a 5 arc-min point almost never is. ``np.rint`` is
    banker's rounding, matching the scalar encoders' ``round`` exactly on a cell boundary.
    """
    missing = set(_SOURCE_COLUMNS) - set(chunk.columns)
    if missing:
        raise KeyError(f"source chunk is missing field(s): {sorted(missing)}")

    out = chunk.rename(columns=_SOURCE_COLUMNS)[list(_SOURCE_COLUMNS.values())].copy()
    lat = out["lat"].to_numpy(dtype=np.float64)
    lon = out["lon"].to_numpy(dtype=np.float64)

    lat_idx = np.rint((LAT_ORIGIN - lat) / RESOLUTION).astype(np.int64)
    np.clip(lat_idx, 0, NLAT - 1, out=lat_idx)
    # Modulo, not clip, on longitude: it is periodic, so a point just west of the prime
    # meridian rounds to index NLON and belongs at index 0, not at 1439.
    lon_idx = np.rint((lon % 360.0) / RESOLUTION).astype(np.int64) % NLON

    out["child_id"] = cell_codes(lat_idx, lon_idx)
    out["parent_id"] = parent_codes(lat_idx, lon_idx, b)
    return out[_COLUMNS]


def load_points(
    conn,
    source: str | Path | None = None,
    *,
    chunk_rows: int = 100_000,
    b: int = DEFAULT_BLOCK_SIZE_B,
    member: str | None = None,
) -> int:
    """Apply DDL, truncate, stream the DBF into staging, insert with geom. Returns rows.

    One transaction: the staging table is ``ON COMMIT DROP`` and the single ``commit`` at
    the end is what makes the reload atomic — a failure halfway through two million rows
    leaves the old table intact rather than a half-loaded one.
    """
    path = resolve_source(source)
    db_load.execute_script(conn, SCHEMA_SQL.read_text())
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {TABLE}")
        cur.execute(
            f"CREATE TEMP TABLE {STAGING} ("
            "cell5m INTEGER, soil_id VARCHAR(12), iso2 VARCHAR(4), "
            "lat DOUBLE PRECISION, lon DOUBLE PRECISION, "
            "child_id CHAR(4), parent_id CHAR(4)"
            ") ON COMMIT DROP"
        )

    staged = 0
    with dbf.open_table(path, member) as (header, fh):
        print(f"{path.name}: {header.n_records:,} records", flush=True)
        for chunk in dbf.iter_chunks(
            fh, header, chunk_rows=chunk_rows, columns=list(_SOURCE_COLUMNS)
        ):
            rows = build_rows(chunk, b=b)
            buf = io.StringIO()
            rows.to_csv(buf, columns=_COLUMNS, index=False, header=False)
            buf.seek(0)
            db_load.copy_csv(conn, STAGING, _COLUMNS, buf)
            staged += len(rows)
            print(f"  staged {staged:,} / {header.n_records:,}", flush=True)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TABLE} "
            "(cell5m, soil_id, iso2, lat, lon, child_id, parent_id, geom) "
            "SELECT cell5m, soil_id, iso2, lat, lon, child_id, parent_id, "
            "ST_SetSRID(ST_MakePoint(lon, lat), 4326) "
            f"FROM {STAGING}"
        )
        cur.execute(f"SELECT count(*) FROM {TABLE}")
        total = cur.fetchone()[0]
    conn.commit()
    return total


def build_map(conn) -> int:
    """Rebuild ``soil_era5_map`` from the loaded points. Returns rows written.

    A plain equality join on ``child_id`` — no ``ST_`` predicate anywhere. The point was
    already assigned to its cell arithmetically at load time, so a spatial join here would
    re-derive, more slowly, something the code space already guarantees.

    ``dist_deg`` scales the longitude term by ``cos(lat)`` so the "nearest" point is
    nearest on the ground rather than in degree space, which matters at high latitude where
    a 0.25° cell is a narrow sliver. ``cell5m`` breaks ties so a rebuild is deterministic.
    """
    db_load.execute_script(conn, SCHEMA_SQL.read_text())
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {GRID_TABLE}")
        if cur.fetchone()[0] == 0:
            raise RuntimeError(
                f"{GRID_TABLE} is empty — restore the grid seed (or run the grid_build "
                f"DAG) before building {MAP_TABLE}"
            )

        cur.execute(f"TRUNCATE {MAP_TABLE}")
        cur.execute(
            f"INSERT INTO {MAP_TABLE} (child_id, cell5m, soil_id, dist_deg, is_nearest) "
            "SELECT child_id, cell5m, soil_id, dist_deg, "
            "       row_number() OVER (PARTITION BY child_id "
            "                          ORDER BY dist_deg, cell5m) = 1 "
            "FROM (SELECT p.child_id, p.cell5m, p.soil_id, "
            "             sqrt(power(p.lat - g.lat, 2) "
            "                  + power((p.lon - g.lon) * cos(radians(p.lat)), 2))::real "
            "             AS dist_deg "
            f"      FROM {TABLE} p JOIN {GRID_TABLE} g ON g.child_id = p.child_id) d"
        )
        cur.execute(f"SELECT count(*) FROM {MAP_TABLE}")
        total = cur.fetchone()[0]
    conn.commit()
    return total


def install_helpers(conn) -> int:
    """Create the coordinate-lookup SQL functions. Returns how many now exist.

    Run by the DAG so a fresh build leaves a database that is actually usable from SQL —
    the tables alone are not enough, and "the DAG succeeded but ``soil_id_at`` does not
    exist" is a confusing place to land.

    Must run **after** ``load_points``: the function bodies reference
    ``soil_profile_points``, and Postgres parse-analyzes a SQL-language body at CREATE
    time, so installing them first fails with ``relation "soil_profile_points" does not
    exist``.
    """
    db_load.execute_script(conn, HELPERS_SQL.read_text())
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = 'public' AND p.proname LIKE 'soil%'"
        )
        total = cur.fetchone()[0]
    conn.commit()
    return total


def validate(conn) -> dict:
    """Post-load sanity report. Raises on the one condition that is always a bug."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*), count(DISTINCT iso2) FROM {TABLE}")
        n_points, n_iso2 = cur.fetchone()
        cur.execute(
            f"SELECT length(soil_id), count(*) FROM {TABLE} GROUP BY 1 ORDER BY 1"
        )
        id_lengths = {row[0]: row[1] for row in cur.fetchall()}
        cur.execute(
            f"SELECT count(*) FROM {TABLE} p "
            f"LEFT JOIN {GRID_TABLE} g ON g.child_id = p.child_id "
            "WHERE g.child_id IS NULL"
        )
        orphans = cur.fetchone()[0]
        cur.execute(f"SELECT count(*), count(DISTINCT child_id) FROM {MAP_TABLE}")
        n_map, n_cells = cur.fetchone()

    report = {
        "points": n_points,
        "countries": n_iso2,
        "soil_id_lengths": id_lengths,
        "orphan_points": orphans,
        "map_rows": n_map,
        "era5_cells_covered": n_cells,
    }
    if orphans:
        # child_id is a pure function of the coordinates and the grid is global, so a
        # point that fails to join means the encoders and the grid disagree — a real bug,
        # not bad input.
        raise RuntimeError(
            f"{orphans:,} soil points have a child_id absent from {GRID_TABLE}; "
            f"the grid and the encoders disagree. Report: {report}"
        )
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=f"Load {TABLE} and build {MAP_TABLE}.")
    ap.add_argument(
        "--source",
        default=DEFAULT_SOURCE,
        help="shapefile .zip (or .dbf/.shp); a bare name means $DATA_DIR/bronze/static/",
    )
    ap.add_argument("--member", default=None, help="which .dbf inside a multi-member zip")
    ap.add_argument("--block-size-b", type=int, default=DEFAULT_BLOCK_SIZE_B)
    ap.add_argument("--chunk-rows", type=int, default=100_000)
    ap.add_argument("--skip-map", action="store_true", help=f"load points, skip {MAP_TABLE}")
    args = ap.parse_args()

    conn = db_load.connect()
    try:
        total = load_points(
            conn,
            args.source,
            chunk_rows=args.chunk_rows,
            b=args.block_size_b,
            member=args.member,
        )
        print(f"loaded; {TABLE} now holds {total:,} rows")
        print(f"installed {install_helpers(conn)} soil_* SQL functions")
        if not args.skip_map:
            print(f"built {build_map(conn):,} rows in {MAP_TABLE}")
            for key, value in validate(conn).items():
                print(f"  {key}: {value}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
