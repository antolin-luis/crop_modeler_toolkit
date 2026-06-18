"""DAG 2 — download_bronze (PLANNING.md §10).

Downloads ERA5 daily variables to bronze parquet (one file per variable per year)
over an extent, year-major / variable-minor, via the adaptive splitter (§11.2).
Maintains an idempotent manifest. Never passes the CDS `grid` parameter.

STUB (roadmap Step 1): structure only; real logic lands in roadmap Step 3.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="download_bronze",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["era5", "bronze", "cds"],
    params={
        "extent": [-90.0, -180.0, 90.0, 180.0],  # snapped to 0.25° vertices
        "start_year": 1995,
        "end_year": 1995,
        "variables": ["tmax", "tmin", "precip", "srad", "wind_u", "wind_v", "tdew"],
        "timezone": "UTC-03:00",
    },
) as dag:
    EmptyOperator(task_id="download_variables")
