"""DAG — qa_scan: field-level bronze QA over the whole archive (PLANNING.md §8.4).

``transform_silver`` already runs the same scan as a pre-pass for the year it is loading.
This DAG exists for the other direction: sweeping bronze that was *already* transformed,
which is how the 1987-01-26 and 1981-08-11 ``tmin`` incidents were found in the first
place — both had been sitting in ``wth_base`` for weeks, passing every row-level check.

Detection only. Findings land in ``wth_data_issues`` and nothing is repaired: repair is
``repair_silver``, triggered deliberately with an explicit scope. Silent imputation is how
an upstream data defect stops being visible.

One mapped task per **variable**, not per year: a variable's scan is a sequence of cheap
per-file aggregates (``date`` and ``value`` columns only), and mapping this way keeps the
task count at 7 rather than ~320.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

DEFAULT_VARIABLES = ["tmax", "tmin", "precip", "srad", "wind_u", "wind_v", "tdew"]


def _plan(**context) -> list[dict]:
    """One entry per variable, each covering the full requested year range."""
    params = context["params"]
    years = list(range(int(params["start_year"]), int(params["end_year"]) + 1))
    return [
        {
            "variable": variable,
            "years": years,
            "record": bool(params["record"]),
            "data_root": params.get("data_root") or None,
        }
        for variable in params["variables"]
    ]


def _scan_variable(
    variable: str,
    years: list,
    record: bool = True,
    data_root: str | None = None,
) -> dict:
    import logging

    from src.config import resolve_bronze_dir
    from src.db import issues as db_issues
    from src.db import load as db_load
    from src.db import silver_load
    from src.transform import field_qa

    log = logging.getLogger(__name__)
    bronze_dir = resolve_bronze_dir(data_root)

    findings = field_qa.scan_archive(bronze_dir, years, [variable])
    log.info(
        "%s: scanned %d years under %s — %d findings",
        variable, len(years), bronze_dir, len(findings),
    )

    for finding in findings.itertuples(index=False):
        log.warning(
            "field QA: %s %s %s over %d cells (%s)",
            finding.variable, finding.date, finding.detector, finding.cells, finding.detail,
        )

    if not record or findings.empty:
        return {"variable": variable, "findings": len(findings), "recorded": 0}

    conn = db_load.connect()
    try:
        silver_load.ensure_schema(conn)
        recorded = db_issues.record_findings(conn, findings)
    finally:
        conn.close()

    return {"variable": variable, "findings": len(findings), "recorded": recorded}


with DAG(
    dag_id="qa_scan",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # Each task streams one file at a time through pyarrow, reading two columns; the
    # working set is a per-date aggregate, not the file. Cheap enough to run wide, but the
    # Pi's IO is the real limit.
    max_active_tasks=3,
    tags=["era5", "silver", "qa"],
    params={
        "start_year": 1981,
        "end_year": 2026,
        # From the download contract, never from listing bronze/ — that directory also
        # holds CHIRPS exports, a separate product on its own grid and out of scope here.
        "variables": DEFAULT_VARIABLES,
        "record": True,      # False = log findings without touching the registry
        "data_root": "",     # blank = env DATA_DIR
    },
) as dag:
    plan = PythonOperator(task_id="plan", python_callable=_plan)
    scan = PythonOperator.partial(
        task_id="scan",
        python_callable=_scan_variable,
    ).expand(op_kwargs=plan.output)
    plan >> scan
