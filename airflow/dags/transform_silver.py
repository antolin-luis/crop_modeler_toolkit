"""DAG 3 — transform_silver (PLANNING.md §10).

Reads the bronze Parquets for a year, merges them wide on ``(child_id, date)``, converts
units (§5.1), derives ``wind``, ``rh`` (Tetens §12.1) and ``et0`` (FAO-56 §12.2), runs the
QA node (§8.4), and upserts into ``wth_base`` with ``is_preliminary`` set from the ERA5T
rolling cutoff (§11.3).

One mapped task per **year**; inside a task the work is chunked by ``parent_id`` batches
and committed per batch, so memory stays bounded regardless of extent (§2) and a failure
resumes cheaply. No Airflow pool — unlike the download DAGs this is local CPU/IO with no
external rate limit.

Four stages: ``field_qa_scan`` (per year, detection only) → ``transform`` (per year) →
``collect_repairs`` → ``auto_repair``. The scan is first because it reads bronze and needs
nothing from the load; the repair is last because it rewrites ``wth_base`` rows that do not
exist until the year is loaded. ``auto_repair`` fires ``repair_silver`` for a **newly
detected** field defect only — an incident already repaired, accepted as a source defect,
or dismissed is never re-fired, or the same three incidents would be repaired on every run.
Set ``auto_repair=false`` to keep detection without it.

"Waits for a year's variables in bronze" (§10) is a file check, not a sensor: the download
DAGs write Parquet only after a variable-year is complete, so presence *is* the signal. A
year with no bronze files is skipped with a log line rather than failing the run.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_VARIABLES = ["tmax", "tmin", "precip", "srad", "wind_u", "wind_v", "tdew"]

# Caps concurrent year tasks. Unlike max_active_tasks below (a DAG attribute, editable
# only in code), pool slots are editable live in the UI under Admin → Pools — so the
# operator can throttle a running backfill on a memory-constrained host without a code
# change. Created by airflow-init alongside cds_pool / gee_pool.
SILVER_POOL = "silver_pool"


def _plan(**context) -> list[dict]:
    """One entry per year in the requested range."""
    params = context["params"]
    return [
        {
            "year": year,
            "variables": list(params["variables"]),
            "parent_batch_size": int(params["parent_batch_size"]),
            "final_cutoff": params.get("final_cutoff") or None,
            "preliminary_months": int(params["preliminary_months"]),
            "data_root": params.get("data_root") or None,
        }
        for year in range(int(params["start_year"]), int(params["end_year"]) + 1)
    ]


def _scan_year(year: int, variables: list, data_root: str | None = None, **_) -> list[dict]:
    """Field-level QA pre-pass for one year (§8.4) — its own task, detection only.

    **A pre-pass, never a per-batch check.** The load commits ~8 parents (~128 cells) at a
    time, so a "constant across every cell" test inside that loop compares 128 values and
    sees nothing; the scan has to see a whole var-year first.

    Split out of the load so a finding is a *task-level* event with its own log and its own
    XCom, rather than a warning buried in the middle of a year's load output. Nothing here
    repairs anything: findings go to the registry, and what happens next is the DAG's
    decision, downstream.
    """
    import logging

    from src.config import resolve_bronze_dir
    from src.db import issues as db_issues
    from src.db import load as db_load
    from src.db import silver_load
    from src.transform import field_qa, merge

    log = logging.getLogger(__name__)
    year = int(year)
    bronze_dir = resolve_bronze_dir(data_root)

    present = merge.available_variables(bronze_dir, year, variables)
    if not present:
        return []

    findings = field_qa.scan_archive(bronze_dir, [year], present)
    if findings.empty:
        log.info("year %s: field QA clean over %s", year, present)
        return []

    conn = db_load.connect()
    try:
        silver_load.ensure_schema(conn)
        db_issues.record_findings(conn, findings)
    finally:
        conn.close()

    out = []
    for finding in findings.itertuples(index=False):
        log.warning(
            "field QA: %s %s %s over %d cells (%s) — recorded in %s",
            finding.variable, finding.date, finding.detector, finding.cells,
            finding.detail, db_issues.TABLE,
        )
        # str dates and plain ints: this crosses an XCom, which is JSON.
        out.append(
            {
                "variable": finding.variable,
                "date": str(finding.date),
                "detector": finding.detector,
                "cells": int(finding.cells),
                "year": year,
            }
        )
    return out


def _collect_repairs(**context) -> list[dict]:
    """Turn this run's findings into one ``repair_silver`` conf per issue worth repairing.

    Returns ``[]`` — which skips the trigger tasks entirely — when auto-repair is off or
    nothing new was found. The filtering is in ``issues.select_repairable``: only a
    ``detected`` issue qualifies, so an incident already repaired, already judged an
    accepted source defect, or already dismissed is not re-fired on every transform.
    """
    import logging

    from src.db import issues as db_issues
    from src.db import load as db_load

    log = logging.getLogger(__name__)
    params = context["params"]
    if not params.get("auto_repair"):
        return []

    scanned = context["ti"].xcom_pull(task_ids="field_qa_scan") or []
    findings = [f for year in scanned if year for f in year]
    if not findings:
        return []

    conn = db_load.connect()
    try:
        repairable = db_issues.select_repairable(findings, db_issues.fetch_all(conn))
    finally:
        conn.close()

    dry_run = bool(params.get("auto_repair_dry_run"))
    for target in repairable:
        log.warning(
            "auto-repair: triggering repair_silver for issue %s (%s %s), dry_run=%s",
            target["issue_id"], target["variable"], target["date"], dry_run,
        )
    return [{"issue_id": t["issue_id"], "dry_run": dry_run, "method": "auto"}
            for t in repairable]


def _transform_year(
    year: int,
    variables: list,
    parent_batch_size: int,
    final_cutoff: str | None,
    preliminary_months: int,
    data_root: str | None = None,
) -> dict:
    import logging
    from datetime import date

    import pendulum as _pendulum

    from src.config import resolve_bronze_dir
    from src.db import load as db_load
    from src.db import silver_load
    from src.transform import merge, qa

    log = logging.getLogger(__name__)
    year = int(year)
    bronze_dir = resolve_bronze_dir(data_root)

    present = merge.available_variables(bronze_dir, year, variables)
    if not present:
        log.warning("no bronze parquet for %s under %s — skipping", year, bronze_dir)
        return {"year": year, "loaded": 0, "quarantined": 0, "skipped": True}

    if final_cutoff:
        cutoff = date.fromisoformat(str(final_cutoff))
    else:
        cutoff = silver_load.preliminary_cutoff(
            _pendulum.now("UTC").date(), months=int(preliminary_months)
        )
    # No local-day offset here: it is per-cell on the grid (era5_land_base_grid.t_zone),
    # applied at GEE reduction time — not a transform-side choice (§5.3).
    log.info("year %s: variables=%s, ERA5T cutoff=%s", year, present, cutoff)

    conn = db_load.connect()
    loaded = quarantined = 0
    try:
        silver_load.ensure_schema(conn)

        for batch in merge.iter_parent_batches(
            bronze_dir, year, present, batch_size=int(parent_batch_size)
        ):
            silver_load.ensure_partitions(conn, batch)
            cell_meta = silver_load.fetch_cell_meta(conn, batch)

            wide = merge.build_wide(bronze_dir, year, batch, cell_meta, present)
            good, failures = qa.split_valid(wide)
            good = silver_load.assign_preliminary(good, cutoff)

            loaded += silver_load.upsert_wide(conn, good)
            quarantined += silver_load.record_failures(conn, failures, batch, year)

            report = qa.calendar_report(wide, year)
            short = report.loc[report["missing"] > 0]
            if not short.empty:
                log.warning(
                    "year %s batch %s: %d cells short of the calendar (max %d days)",
                    year, batch[0], len(short), int(short["missing"].max()),
                )
            log.info("year %s batch %s..%s: +%d rows", year, batch[0], batch[-1], len(good))
    finally:
        conn.close()

    log.info("year %s done: %d rows loaded, %d quarantined", year, loaded, quarantined)
    return {"year": year, "loaded": loaded, "quarantined": quarantined, "skipped": False}


with DAG(
    dag_id="transform_silver",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # Backstop for SILVER_POOL (which is the tunable knob). Measured on the Pi 5 target:
    # one year task peaks at ~220 MB RSS — almost all of it the pandas/pyarrow/numpy
    # import baseline, which is per-process and fixed — plus Airflow's task-runner
    # overhead. Airflow's default of 16 concurrent tasks is therefore ~5-7 GB, enough to
    # thrash an 8 GB Pi with a 200 MB swap file into a hard freeze. A year takes ~9 s, so
    # a 47-year backfill is ~3 min at 3-way concurrency: nothing to win by raising this,
    # and a machine to lose. In-task memory is governed separately by parent_batch_size.
    max_active_tasks=3,
    tags=["era5", "silver", "transform"],
    params={
        "start_year": 2020,
        "end_year": 2020,
        "variables": DEFAULT_VARIABLES,
        "parent_batch_size": 8,      # parents per commit — the memory lever
        "preliminary_months": 3,     # ERA5T rolling window (§11.3)
        "final_cutoff": "",          # ISO date; blank = derive from preliminary_months
        "data_root": "",             # per-run data root override; blank = env DATA_DIR.
                                     # Must match the root the bronze was downloaded to.
        # Fire repair_silver on a newly *detected* field defect. Only `detected` qualifies
        # (issues.select_repairable), so an incident already repaired, accepted or
        # dismissed is never re-fired. Repair is still a separate DAG with its own run and
        # its own audit trail; this only pulls the trigger.
        "auto_repair": True,
        "auto_repair_dry_run": False,
    },
) as dag:
    plan = PythonOperator(task_id="plan", python_callable=_plan)
    field_qa_scan = PythonOperator.partial(
        task_id="field_qa_scan",
        python_callable=_scan_year,
        pool=SILVER_POOL,
    ).expand(op_kwargs=plan.output)
    transform = PythonOperator.partial(
        task_id="transform",
        python_callable=_transform_year,
        pool=SILVER_POOL,
    ).expand(op_kwargs=plan.output)
    collect_repairs = PythonOperator(
        task_id="collect_repairs", python_callable=_collect_repairs
    )
    # Mapped over an empty list when there is nothing to repair, which skips it.
    auto_repair = TriggerDagRunOperator.partial(
        task_id="auto_repair",
        trigger_dag_id="repair_silver",
        wait_for_completion=False,
    ).expand(conf=collect_repairs.output)

    # The scan runs first — it reads bronze, so it needs nothing from the load — but the
    # repair runs *after* the transform, because a repair rewrites rows in wth_base and
    # there is nothing to rewrite until the year is loaded.
    plan >> field_qa_scan >> transform >> collect_repairs >> auto_repair
