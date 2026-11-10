"""DAG — chirps_grid_build.

Builds ``chirps_base_grid``, the 0.05° fine grid, for one extent. The CHIRPS counterpart of
``grid_build``, and the first task to run for a new region.

Unlike ``grid_build`` this is **not** a maintainer-only regeneration of a shipped seed —
users run it. A global 0.05° grid is 17.3M rows and cannot be shipped, so every deployment
materializes the extents it actually needs. Codes stay globally deterministic, so cells built
here match what any other deployment would compute.

Idempotent, and extents accumulate: running it for a second region adds rows rather than
replacing the first. Set ``replace`` to truncate instead.

Trigger with, e.g. Tocantins::

    {"extent": [-13.50, -50.75, -5.15, -45.70]}
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


def _build_fine_grid(params: dict) -> int:
    from src.db import load as db_load
    from src.db.seed_fine_grid import build_rows, load_grid

    extent = [float(x) for x in params["extent"]]
    df = build_rows(extent, b=int(params["block_b"]))
    print(
        f"built {len(df):,} fine cells over {extent} "
        f"({df['fparent_id'].nunique():,} parents)"
    )

    conn = db_load.connect()
    try:
        total = load_grid(conn, df, replace=bool(params["replace"]))
    finally:
        conn.close()
    print(f"chirps_base_grid now holds {total:,} rows")
    return len(df)


with DAG(
    dag_id="chirps_grid_build",
    schedule=None,  # per-region, run once before that region's first backfill
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["chirps", "grid", "seed"],
    render_template_as_native_obj=True,  # keep `extent` a real list, not a string
    params={
        # [S, W, N, E], snapped to 0.05°. Default is Tocantins, Brazil.
        "extent": [-13.50, -50.75, -5.15, -45.70],
        # Fine parent block factor. IMMUTABLE once any data is loaded — it is baked into
        # every stored fparent_id and every wth_precip_alt partition name.
        "block_b": 20,
        # Truncate before loading. Drops every other extent already built; only for a
        # rebuild after a spec change.
        "replace": False,
    },
) as dag:
    PythonOperator(
        task_id="build_fine_grid",
        python_callable=_build_fine_grid,
    )
