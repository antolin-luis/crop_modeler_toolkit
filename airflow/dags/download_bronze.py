"""DAG 2 — download_bronze (PLANNING.md §10, §11).

Backfills the bronze layer: one Parquet per variable per year over ``extent``, fetched
through the adaptive splitter (§11.2) and tracked by an idempotent manifest (§11.4).

Tasks are mapped one per ``(year, variable)`` and ordered **year-major, variable-minor**
(``Year1{var1..varN}, Year2{...}``). All CDS tasks are bound to the ``cds_pool`` Airflow
pool, which caps simultaneous CDS requests (the CDS penalizes heavy/parallel use, §10).
Create the pool once: ``airflow pools set cds_pool 2 "CDS request cap"``.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

CDS_POOL = "cds_pool"


def _plan(**context) -> list[dict]:
    """Build the year-major, variable-minor list of per-task op_kwargs."""
    params = context["params"]
    extent = [float(v) for v in params["extent"]]
    timezone = params["timezone"]
    years = range(int(params["start_year"]), int(params["end_year"]) + 1)
    return [
        {"variable": variable, "year": year, "extent": extent, "timezone": timezone}
        for year in years
        for variable in params["variables"]
    ]


def _download(variable: str, year: int, extent: list, timezone: str) -> str:
    from src.cds.client import CDSClient
    from src.cds.download import download_variable_year
    from src.cds.manifest import Manifest
    from src.config import load_config

    bronze_dir = load_config().paths.bronze_dir
    manifest = Manifest.for_bronze_dir(bronze_dir)
    path = download_variable_year(
        CDSClient(),
        variable,
        year,
        extent,
        timezone,
        manifest=manifest,
        bronze_dir=bronze_dir,
    )
    return str(path)


with DAG(
    dag_id="download_bronze",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["era5", "bronze", "cds"],
    params={
        "extent": [-90.0, -180.0, 90.0, 180.0],  # [S, W, N, E], snapped to 0.25°
        "start_year": 1995,
        "end_year": 1995,
        "variables": ["tmax", "tmin", "precip", "srad", "wind_u", "wind_v", "tdew"],
        "timezone": "UTC-03:00",
    },
) as dag:
    plan = PythonOperator(
        task_id="plan",
        python_callable=_plan,
    )
    download = PythonOperator.partial(
        task_id="download",
        python_callable=_download,
        pool=CDS_POOL,
    ).expand(op_kwargs=plan.output)
    plan >> download
