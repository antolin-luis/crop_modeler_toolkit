"""DAG 1 — grid_build (PLANNING.md §10).

One-off / seed. Builds the global era5_land_base_grid from the two static .nc
(geopotential, ERA5-Land mask) and block size b: child_id, parent_id, lat/lon,
is_land (clip §6.4), elevation, geom. Idempotent (truncate-then-load).

Normally users never run this — they get the committed SQL-dump seed restored at
container init. It exists so a maintainer can regenerate that seed.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def _ensure_static_inputs() -> dict:
    from src.grid.static_inputs import ensure_static_inputs

    paths = ensure_static_inputs()
    return {k: str(v) for k, v in paths.items()}


def _build_grid(params: dict, ti) -> int:
    from src.db import load as db_load
    from src.db.seed_grid import build_rows, load_grid

    paths = ti.xcom_pull(task_ids="ensure_static_inputs")
    df = build_rows(
        paths["geopotential"],
        paths["land_mask"],
        b=int(params["block_size_b"]),
        fraction_x=float(params["land_fraction_x"]),
    )
    conn = db_load.connect()
    try:
        load_grid(conn, df)
    finally:
        conn.close()
    return len(df)


with DAG(
    dag_id="grid_build",
    schedule=None,  # one-off / seed
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["era5", "grid", "seed"],
    params={"block_size_b": 4, "land_fraction_x": 0.5},
) as dag:
    ensure = PythonOperator(
        task_id="ensure_static_inputs",
        python_callable=_ensure_static_inputs,
    )
    build = PythonOperator(
        task_id="build_grid",
        python_callable=_build_grid,
    )
    ensure >> build
