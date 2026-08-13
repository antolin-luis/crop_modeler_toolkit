"""DAG — download_bronze_chirps.

CHIRPS bronze backfill: one mapped task per ``(source, year)``, exporting on CHIRPS's native
0.05° grid to ``bronze/<source>/<source>_<year>.parquet``.

Separate from ``download_bronze_gee`` rather than a variant of it. The two share their
export machinery (``src/gee/metrics.run_export``) and their manifest, but nothing else:
CHIRPS needs no timezone asset, no reducer per variable, no chunking at this extent, and no
ERA5T preliminary window. Folding it in would have meant a second mode inside a DAG whose
params already carry seven ERA5-specific knobs.

Runs ``chirps_grid_build`` for the same extent **first** — the transform needs
``chirps_base_grid`` to exist, and the fine grid is region-scoped so it is not there by
default.

Trigger, e.g. one year of v3 over Tocantins::

    {"extent": [-13.50, -50.75, -5.15, -45.70],
     "start_year": 2020, "end_year": 2020,
     "sources": ["chirps_v3_rnl"], "data_root": "TO"}

Full history is 1981 → present for both sources. That is ~91 mapped tasks for one source
and ~182 for both, so it fits one trigger — unlike the ERA5 backfill, which has to be run in
year windows to stay under ``core.max_map_length``.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

GEE_POOL = "gee_pool"


def _plan(params: dict) -> list[dict]:
    """One entry per (source, year), year-major so a partial run leaves whole years done."""
    from src.gee.chirps import covers_extent, source_spec

    extent = [float(x) for x in params["extent"]]
    sources = list(params["sources"])
    start_year = int(params["start_year"])
    end_year = int(params["end_year"])
    if end_year < start_year:
        raise ValueError(f"end_year {end_year} precedes start_year {start_year}")

    # Fail the plan, not task 40 of 182, on a source that cannot cover this extent or does
    # not reach this far back.
    for source in sources:
        spec = source_spec(source)
        if not covers_extent(source, extent):
            raise ValueError(
                f"{source} covers latitudes {spec.lat_bounds}; extent {extent} reaches "
                "past it. Cells outside would come back masked — a silent hole."
            )
        if start_year < spec.first_year:
            raise ValueError(
                f"{source} starts in {spec.first_year}; start_year {start_year} predates "
                "it. Earlier years are ERA5-only, in wth_base."
            )

    return [
        {
            "source": source,
            "year": year,
            "extent": extent,
            "chunk_days": int(params["chunk_days"]),
            "land_only": bool(params["land_only"]),
            "max_attempts": int(params["max_attempts"]),
            "data_root": params["data_root"] or None,
            "sample": params["sample"] or None,
        }
        for year in range(start_year, end_year + 1)
        for source in sources
    ]


def _download(
    source: str,
    year: int,
    extent: list,
    chunk_days: int,
    land_only: bool,
    max_attempts: int,
    data_root: str | None = None,
    sample: str | None = None,
) -> dict:
    # No timezone asset: CHIRPS is a finished daily product, so there is no local-day
    # window to build and zones is always 1 (src/gee/chirps.py).
    import logging

    from src.cds.manifest import Manifest
    from src.config import resolve_bronze_dir
    from src.gee.chirps_download import download_source_year
    from src.gee.client import GEEClient

    log = logging.getLogger(__name__)

    # An export polls every 20s for up to 6 h and otherwise logs nothing, so a slow year is
    # indistinguishable from a hung one. Log transitions only.
    seen: dict = {}

    def _log_poll(status: dict) -> None:
        from src.gee.export import task_attempt

        key = (status.get("state"), task_attempt(status))
        if key == seen.get("last"):
            return
        seen["last"] = key
        log.info("%s %s: export state=%s attempt=%s", source, year, key[0], key[1])

    bronze_dir = resolve_bronze_dir(data_root)
    manifest = Manifest.for_bronze_dir(bronze_dir)
    rec: dict = {}
    try:
        path = download_source_year(
            GEEClient(),
            source,
            year,
            extent,
            manifest=manifest,
            bronze_dir=bronze_dir,
            chunk_days=int(chunk_days),
            land_only=bool(land_only),
            sample=sample,
            metrics_out=rec,
            max_attempts=max_attempts,
            on_poll=_log_poll,
        )
    except Exception:
        # An export that burned the attempt cap means this extent has outgrown a single
        # unchunked task — the fine grid would then need chunks.plan_chunks generalized,
        # and its own ceiling probe. Do not just raise max_attempts.
        if rec.get("attempts") and max_attempts and rec["attempts"] > max_attempts:
            log.error(
                "%s %s ABORTED at attempt %s (cap %s): EE kept restarting this export. "
                "The extent needs chunking at 0.05° — see docs and re-probe the ceiling.",
                source, year, rec.get("attempts"), max_attempts,
            )
        raise
    log.info(
        "%s %s: %s rows, %s cells x %s days, attempt %s, %s EECU-h, %s bytes in %s "
        "blob(s), export %ss / download %ss / encode %ss",
        source, year,
        rec.get("bronze_rows"), rec.get("cells"), rec.get("days"), rec.get("attempts"),
        rec.get("eecu_hours"), rec.get("bytes_remote"), rec.get("n_blobs"),
        rec.get("t_export_s"), rec.get("t_download_s"), rec.get("t_encode_s"),
    )
    if rec.get("record_write_error"):
        log.warning("cost record not persisted: %s", rec["record_write_error"])
    return {
        "source": source,
        "year": year,
        "path": str(path),
        "sample": sample or "",
        "bronze_rows": rec.get("bronze_rows"),
        "cells": rec.get("cells"),
        "days": rec.get("days"),
        "attempts": rec.get("attempts"),
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
    dag_id="download_bronze_chirps",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["chirps", "bronze", "gee"],
    render_template_as_native_obj=True,  # keep lists/ints real through dag_run.conf
    # One run at a time. Re-triggering while a run is live used to start a second one (the
    # default cap is 16), and since each run ends in its own kick_transform, concurrent
    # downloads multiply into concurrent transforms — which is what froze the Pi on
    # 2026-08-08. Queuing is the right behaviour anyway: the manifest makes a re-run cheap,
    # but only once the first run has finished writing it.
    max_active_runs=1,
    params={
        # [S, W, N, E], snapped to 0.05°. Default is Tocantins, Brazil.
        "extent": [-13.50, -50.75, -5.15, -45.70],
        # CHIRPS begins 1981 for every version; there is no earlier data to ask for.
        "start_year": 1981,
        "end_year": 1981,
        # Keys of src/gee/chirps.CHIRPS_SOURCES.
        "sources": ["chirps_v3_rnl", "chirps_v2"],
        "chunk_days": 30,   # daily bands held in RAM per encode step (Pi memory lever)
        "land_only": True,  # clip export to land (LSIB); drop masked cells
        "max_attempts": 2,  # EE restarts tolerated. A restart means the export is too big
                            # for one task; waiting out a third spends quota on the same
                            # failure. At 0.05° the fix is chunking, not patience.
        "data_root": "",    # per-run data root override; blank = env DATA_DIR. Give each
                            # region its own root to avoid manifest clashes. A bare folder
                            # name is enough: "TO" means $DATA_DIR/TO.
        "sample": "",       # calibration sample id stamped on each cost record
    },
) as dag:
    plan = PythonOperator(task_id="plan", python_callable=_plan)
    download = PythonOperator.partial(
        task_id="download",
        python_callable=_download,
        pool=GEE_POOL,
    ).expand(op_kwargs=plan.output)

    # Build the conf from `params`, NOT from `dag_run.conf`. `params` is defaults merged with
    # whatever the trigger supplied, so every key is always present. `dag_run.conf` carries
    # only the keys explicitly passed: a CLI trigger like `-c '{"start_year": 1982}'` would
    # hand the transform no data_root, it would fall back to $DATA_DIR instead of the root
    # this download actually wrote to, find no bronze, and skip every year — silently, until
    # the `verify` task added downstream catches it. Passing params closes that hole here.
    kick_transform = TriggerDagRunOperator(
        task_id="kick_transform",
        trigger_dag_id="transform_precip_alt",
        # Deterministic run id, so re-running this task RESETS the transform run it already
        # created instead of minting another. Without it TriggerDagRunOperator timestamps the
        # run id, reset_dag_run has nothing to match, and every re-execution starts a fresh
        # concurrent transform.
        #
        # That is not hypothetical: `airflow tasks clear -t 'download|kick_transform' -s
        # <date> -e <date+1>` matches every DagRun in the window, not one. Clearing to retry
        # a single failed year re-fired kick_transform in five earlier runs at once, each
        # spawning its own transform, and the resulting ~10 GB of task processes froze an
        # 8 GB Pi (2026-08-08). Pair this with max_active_runs=1 above.
        trigger_run_id="chirps__{{ dag_run.run_id }}",
        conf={
            "start_year": "{{ params.start_year }}",
            "end_year": "{{ params.end_year }}",
            "sources": "{{ params.sources }}",
            "data_root": "{{ params.data_root }}",
        },
        reset_dag_run=True,
        wait_for_completion=False,
    )

    plan >> download >> kick_transform
