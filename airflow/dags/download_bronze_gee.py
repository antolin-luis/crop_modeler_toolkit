"""DAG 2b — download_bronze_gee (GEE backend; mirrors download_bronze).

Backfills the bronze layer from **Google Earth Engine** instead of the CDS: one Parquet per
variable per year over ``extent``, with hourly→daily reduction done server-side and the
daily result exported via GCS (PLANNING.md §7; docs/step3b_bronze_download_gee.md). Output
schema and paths are identical to the CDS DAG, so the two backends are interchangeable.

Tasks are mapped one per ``(year, variable)``, year-major / variable-minor, and bound to the
``gee_pool`` Airflow pool, which caps simultaneous EECU/export tasks (stay under the
noncommercial monthly EECU quota). Create the pool once:
``airflow pools set gee_pool 2 "GEE export cap"``.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

GEE_POOL = "gee_pool"


def _plan(**context) -> list[dict]:
    """Build the year-major, variable-minor list of per-task op_kwargs."""
    params = context["params"]
    extent = [float(v) for v in params["extent"]]
    chunk_days = int(params["chunk_days"])
    land_only = bool(params["land_only"])
    data_root = params.get("data_root") or None
    years = range(int(params["start_year"]), int(params["end_year"]) + 1)
    return [
        {
            "variable": variable,
            "year": year,
            "extent": extent,
            "chunk_days": chunk_days,
            "land_only": land_only,
            "data_root": data_root,
        }
        for year in years
        for variable in params["variables"]
    ]


def _download(
    variable: str,
    year: int,
    extent: list,
    chunk_days: int,
    land_only: bool,
    data_root: str | None = None,
) -> str:
    # No timezone param: the per-cell local-day offset comes from the tz-polygon asset,
    # derived per UTC offset present in the extent (src/gee/export.tz_zones). §5.3.
    from src.config import load_config, resolve_bronze_dir
    from src.gee.client import GEEClient
    from src.gee.download import download_variable_year

    # Manifest is shared with the CDS backend: a (variable, year) done by either is done.
    from src.cds.manifest import Manifest

    tz_asset = load_config().gee.tz_asset
    if not tz_asset:
        raise RuntimeError("GEE_TZ_ASSET is unset — see scripts/build_tz_asset.py")

    bronze_dir = resolve_bronze_dir(data_root)
    manifest = Manifest.for_bronze_dir(bronze_dir)
    path = download_variable_year(
        GEEClient(),
        variable,
        year,
        extent,
        tz_asset=tz_asset,
        manifest=manifest,
        bronze_dir=bronze_dir,
        chunk_days=int(chunk_days),
        land_only=bool(land_only),
    )
    return str(path)


with DAG(
    dag_id="download_bronze_gee",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["era5", "bronze", "gee"],
    params={
        "extent": [-90.0, -180.0, 90.0, 180.0],  # [S, W, N, E], snapped to 0.25°
        "start_year": 1995,
        "end_year": 1995,
        "variables": ["tmax", "tmin", "precip", "srad", "wind_u", "wind_v", "tdew"],
        "chunk_days": 30,   # daily bands held in RAM per encode step (Pi memory lever)
        "land_only": True,  # clip export to land (LSIB); drop masked-ocean cells
        "data_root": "",    # per-run data root override; blank = env DATA_DIR. Give each
                            # region its own root (e.g. /data/hn) to avoid manifest clashes.
    },
) as dag:
    plan = PythonOperator(
        task_id="plan",
        python_callable=_plan,
    )
    download = PythonOperator.partial(
        task_id="download",
        python_callable=_download,
        pool=GEE_POOL,
    ).expand(op_kwargs=plan.output)
    plan >> download
