"""DAG 3 — transform_silver (PLANNING.md §10).

Reads the bronze Parquets for a year, merges them wide on ``(child_id, date)``, converts
units (§5.1), derives ``wind``, ``rh`` (Tetens §12.1) and ``et0`` (FAO-56 §12.2), runs the
QA node (§8.4), and upserts into ``wth_base`` with ``is_preliminary`` set from the ERA5T
rolling cutoff (§11.3).

One mapped task per **year**; inside a task the work is chunked by ``parent_id`` batches
and committed per batch, so memory stays bounded regardless of extent (§2) and a failure
resumes cheaply. No Airflow pool — unlike the download DAGs this is local CPU/IO with no
external rate limit.

"Waits for a year's variables in bronze" (§10) is a file check, not a sensor: the download
DAGs write Parquet only after a variable-year is complete, so presence *is* the signal. A
year with no bronze files is skipped with a log line rather than failing the run.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

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
    from src.db import issues as db_issues
    from src.db import load as db_load
    from src.db import silver_load
    from src.transform import field_qa, merge, qa

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

        # Field-level QA pre-pass (§8.4). Must run here — before the batch loop — because
        # a "constant across every cell" test inside the loop only ever sees one batch's
        # ~128 cells. Detection only: findings go to the registry and a WARNING, never to
        # an automatic repair, so an upstream defect stays visible instead of being
        # silently filled in.
        findings = field_qa.scan_archive(bronze_dir, [year], present)
        if not findings.empty:
            db_issues.record_findings(conn, findings)
            for finding in findings.itertuples(index=False):
                log.warning(
                    "field QA: %s %s %s over %d cells (%s) — recorded in %s",
                    finding.variable, finding.date, finding.detector, finding.cells,
                    finding.detail, db_issues.TABLE,
                )

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

    log.info(
        "year %s done: %d rows loaded, %d quarantined, %d field-QA findings",
        year, loaded, quarantined, len(findings),
    )
    return {
        "year": year,
        "loaded": loaded,
        "quarantined": quarantined,
        "field_qa_findings": len(findings),
        "skipped": False,
    }


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
    },
) as dag:
    plan = PythonOperator(task_id="plan", python_callable=_plan)
    transform = PythonOperator.partial(
        task_id="transform",
        python_callable=_transform_year,
        pool=SILVER_POOL,
    ).expand(op_kwargs=plan.output)
    plan >> transform
