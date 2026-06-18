"""DAG 3 — transform_silver (PLANNING.md §10).

Waits for a year's variables in bronze, merges wide on (child_id, date), derives
wind, rh (Tetens §12.1), et0 (FAO-56 §12.2), runs the QA node, and upserts into
wth_base (is_preliminary set per source state).

STUB (roadmap Step 1): structure only; real logic lands in roadmap Step 4.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="transform_silver",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["era5", "silver", "transform"],
    params={"timezone": "UTC-03:00"},
) as dag:
    EmptyOperator(task_id="merge_derive_qa_upsert")
