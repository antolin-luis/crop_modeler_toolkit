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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

GEE_POOL = "gee_pool"


def _plan(**context) -> list[dict]:
    """Build the year-major, variable-minor list of per-task op_kwargs."""
    params = context["params"]
    extent = [float(v) for v in params["extent"]]
    chunk_days = int(params["chunk_days"])
    land_only = bool(params["land_only"])
    data_root = params.get("data_root") or None
    sample = params.get("sample") or None
    years = range(int(params["start_year"]), int(params["end_year"]) + 1)
    return [
        {
            "variable": variable,
            "year": year,
            "extent": extent,
            "chunk_days": chunk_days,
            "land_only": land_only,
            "data_root": data_root,
            "sample": sample,
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
    sample: str | None = None,
) -> dict:
    # No timezone param: the per-cell local-day offset comes from the tz-polygon asset,
    # derived per UTC offset present in the extent (src/gee/export.tz_zones). §5.3.
    import logging

    from src.config import load_config, resolve_bronze_dir
    from src.gee.client import GEEClient
    from src.gee.download import download_variable_year

    # Manifest is shared with the CDS backend: a (variable, year) done by either is done.
    from src.cds.manifest import Manifest

    log = logging.getLogger(__name__)

    tz_asset = load_config().gee.tz_asset
    if not tz_asset:
        raise RuntimeError("GEE_TZ_ASSET is unset — see scripts/build_tz_asset.py")

    bronze_dir = resolve_bronze_dir(data_root)
    manifest = Manifest.for_bronze_dir(bronze_dir)
    rec: dict = {}
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
        sample=sample,
        metrics_out=rec,
    )
    # Cost accounting (docs/cost_model_climate_context.md §3). `rec` is empty on a
    # manifest hit — nothing ran, so nothing was measured. eecu_hours may legitimately be
    # None: EE does not always report it, and the task_id in the JSONL is the handle for
    # reading it off the EE task list by hand.
    log.info(
        "%s %s: %s rows, %s cells x %s days, %s EECU-h, %s bytes in %s blob(s), "
        "export %ss / download %ss / encode %ss",
        variable, year,
        rec.get("bronze_rows"), rec.get("cells"), rec.get("days"),
        rec.get("eecu_hours"), rec.get("bytes_remote"), rec.get("n_blobs"),
        rec.get("t_export_s"), rec.get("t_download_s"), rec.get("t_encode_s"),
    )
    if rec.get("record_write_error"):
        log.warning("cost record not persisted: %s", rec["record_write_error"])
    return {
        "variable": variable,
        "year": year,
        "path": str(path),
        "sample": sample or "",
        "bronze_rows": rec.get("bronze_rows"),
        "cells": rec.get("cells"),
        "days": rec.get("days"),
        "n_units": rec.get("n_units"),
        "eecu_hours": rec.get("eecu_hours"),
        "bytes_remote": rec.get("bytes_remote"),
        "n_blobs": rec.get("n_blobs"),
        "compression_ratio": rec.get("compression_ratio"),
        "t_export_s": rec.get("t_export_s"),
        "t_download_s": rec.get("t_download_s"),
        "t_encode_s": rec.get("t_encode_s"),
        "task_id": rec.get("task_id"),
    }


with DAG(
    dag_id="download_bronze_gee",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["era5", "bronze", "gee"],
    # Native rendering so `conf="{{ dag_run.conf }}"` on the transform trigger resolves to the
    # actual dict (variables stays a list, years stay ints) instead of a stringified repr.
    render_template_as_native_obj=True,
    params={
        "extent": [-90.0, -180.0, 90.0, 180.0],  # [S, W, N, E], snapped to 0.25°
        "start_year": 1995,
        "end_year": 1995,
        "variables": ["tmax", "tmin", "precip", "srad", "wind_u", "wind_v", "tdew"],
        "chunk_days": 30,   # daily bands held in RAM per encode step (Pi memory lever)
        "land_only": True,  # clip export to land (LSIB); drop masked-ocean cells
        "data_root": "",    # per-run data root override; blank = env DATA_DIR. Give each
                            # region its own root (e.g. /data/hn) to avoid manifest clashes.
        "sample": "",       # calibration sample id (e.g. "E1") stamped on each cost record
                            # in <bronze>/_gee_metrics.jsonl. Blank for ordinary backfills.
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

    # After every (year, variable) download lands, kick transform_silver with the *same* conf
    # (extent/years/variables/data_root) so bronze -> silver runs end-to-end in one trigger.
    # transform_silver reads bronze from the same data_root and ignores the extra download-only
    # keys (extent/chunk_days/land_only); params it doesn't receive keep their own defaults.
    kick_transform = TriggerDagRunOperator(
        task_id="kick_transform",
        trigger_dag_id="transform_silver",
        conf="{{ dag_run.conf }}",
        reset_dag_run=True,       # re-runnable: replace a same-logical-date run instead of erroring
        wait_for_completion=False,
    )

    plan >> download >> kick_transform
