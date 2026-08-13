"""DAG — soil_grid_build.

Loads the SoilGrids-for-DSSAT 5 arc-min point layer into ``soil_profile_points``, installs
the coordinate-lookup SQL functions, and builds ``soil_era5_map``, which pairs every 0.25°
weather cell with the DSSAT soil profile ID (``ID_SOIL``) nearest its centre. One-off, like
``grid_build`` and ``chirps_grid_build`` — rerun only to pick up a new release of the
source layer.

A completed run leaves a database that is usable from SQL immediately: no separate script
to import before ``SELECT * FROM soil_id_at(lat, lon)`` works.

**Prerequisites.** Two, both easy to miss:

1. The archive must sit under ``$DATA_DIR/bronze/static/`` (``.localdata/bronze/static/``
   on the host). Only ``$DATA_DIR`` is bind-mounted into these containers, so a file left
   at the repo root is invisible here.
2. ``era5_land_base_grid`` must be populated — the shipped seed is restored at first boot,
   so normally it already is. ``build_map`` fails loudly rather than writing an empty
   bridge table if it is not.

Global and truncate-and-load: the layer is ~2M rows, small enough that scoping it to an
extent would only create a "which regions did I build?" question with no upside.

Runs with default params; nothing needs to be passed::

    {"source": "Point5m_SoilGrids-for-DSSAT-10km_v1.shp.zip"}
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def _load_points(params: dict) -> int:
    from src.db import load as db_load
    from src.db.seed_soil import load_points

    chunk_rows = int(params["chunk_rows"])
    if chunk_rows < 1:
        raise ValueError(f"chunk_rows must be >= 1, got {chunk_rows}")
    b = int(params["block_size_b"])
    if b < 1:
        raise ValueError(f"block_size_b must be >= 1, got {b}")

    conn = db_load.connect()
    try:
        total = load_points(
            conn,
            params["source"] or None,
            chunk_rows=chunk_rows,
            b=b,
            member=params["member"] or None,
        )
    finally:
        conn.close()
    print(f"soil_profile_points now holds {total:,} rows")
    return total


def _install_helpers(params: dict) -> int:
    from src.db import load as db_load
    from src.db.seed_soil import install_helpers

    conn = db_load.connect()
    try:
        total = install_helpers(conn)
    finally:
        conn.close()
    print(f"{total} soil_* SQL functions installed (soil_id_at, soil_profile_at, ...)")
    return total


def _build_map(params: dict) -> int:
    from src.db import load as db_load
    from src.db.seed_soil import build_map

    if not params["build_map"]:
        print("build_map=False — skipping soil_era5_map")
        return 0

    conn = db_load.connect()
    try:
        total = build_map(conn)
    finally:
        conn.close()
    print(f"soil_era5_map now holds {total:,} rows")
    return total


def _validate(params: dict) -> dict:
    from src.db import load as db_load
    from src.db.seed_soil import validate

    if not params["build_map"]:
        print("build_map=False — skipping validation (it reads soil_era5_map)")
        return {}

    conn = db_load.connect()
    try:
        report = validate(conn)
    finally:
        conn.close()
    for key, value in report.items():
        print(f"{key}: {value}")
    return report


with DAG(
    dag_id="soil_grid_build",
    schedule=None,  # one-off; rerun only for a new release of the source layer
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["soil", "dssat", "seed"],
    render_template_as_native_obj=True,
    params={
        # Bare filename -> $DATA_DIR/bronze/static/<name>. Accepts .zip, .dbf or .shp.
        "source": "Point5m_SoilGrids-for-DSSAT-10km_v1.shp.zip",
        # Only needed if the zip holds more than one .dbf.
        "member": "",
        # Must match the value era5_land_base_grid was built with — it is baked into every
        # stored parent_id. IMMUTABLE for the life of the database.
        "block_size_b": 4,
        # Rows staged per COPY. Sets peak memory; 100k is ~60 MB on the Pi.
        "chunk_rows": 100000,
        # Build the child_id -> soil profile bridge after loading the points.
        "build_map": True,
    },
) as dag:
    load_points = PythonOperator(task_id="load_points", python_callable=_load_points)
    # Its own task rather than a step inside load_points: it is the piece most likely to be
    # wanted on its own (someone edits a function, or drops one by hand), and a separate
    # task can be cleared and rerun in seconds instead of reloading two million rows.
    install_helpers = PythonOperator(
        task_id="install_helpers", python_callable=_install_helpers
    )
    build_map = PythonOperator(task_id="build_map", python_callable=_build_map)
    validate = PythonOperator(task_id="validate", python_callable=_validate)

    load_points >> install_helpers >> build_map >> validate
