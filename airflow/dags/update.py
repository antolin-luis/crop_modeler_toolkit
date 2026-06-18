"""DAG 4 — update (PLANNING.md §10).

Daily append of the latest available day(s) plus a 3-month rolling ERA5T re-fetch:
re-download the trailing ~3–4 months, overwrite bronze parquet, and propagate to
silver (re-derive + upsert, flipping is_preliminary -> FALSE once final). The
re-fetch is not complete until silver reflects the correction (§11.3).

STUB (roadmap Step 1): structure only; real logic lands in roadmap Step 5.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="update",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["era5", "update", "era5t"],
) as dag:
    EmptyOperator(task_id="daily_append_and_era5t_refetch")
