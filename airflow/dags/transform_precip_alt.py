"""DAG — transform_precip_alt.

CHIRPS bronze → ``wth_precip_alt``. One mapped task per year; each year loops its sources
and commits per fparent batch, so a long backfill resumes rather than restarts.

Separate from ``transform_silver`` rather than a mode inside it. The two share only the
COPY-into-staging-then-upsert pattern; this one has no wide merge, no unit conversion, no
derived variables, no ERA5T preliminary window, and writes a different table on a different
grid. ``download_bronze_chirps`` triggers it automatically, and it can also be run alone
over bronze already on disk.

Requires ``chirps_base_grid`` to exist for the extent — run ``chirps_grid_build`` first.
The transform itself does not read that table (fine_id/fparent_id come from bronze), but
nothing downstream is queryable without it.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

SILVER_POOL = "silver_pool"

DEFAULT_SOURCES = ["chirps_v3_rnl", "chirps_v2"]


def _plan(**context) -> list[dict]:
    """One entry per year in the requested range."""
    params = context["params"]
    return [
        {
            "year": year,
            "sources": list(params["sources"]),
            "fparent_batch_size": int(params["fparent_batch_size"]),
            "data_root": params.get("data_root") or None,
        }
        for year in range(int(params["start_year"]), int(params["end_year"]) + 1)
    ]


def _transform_year(
    year: int,
    sources: list,
    fparent_batch_size: int,
    data_root: str | None = None,
) -> dict:
    import logging

    from src.config import resolve_bronze_dir
    from src.db import load as db_load
    from src.db import precip_load
    from src.gee.chirps import source_code
    from src.transform import precip_alt

    log = logging.getLogger(__name__)
    year = int(year)
    bronze_dir = resolve_bronze_dir(data_root)

    present = precip_alt.available_sources(bronze_dir, year, sources)
    if not present:
        log.warning("no CHIRPS bronze for %s under %s — skipping", year, bronze_dir)
        return {"year": year, "loaded": 0, "quarantined": 0, "skipped": True}

    # No local-day offset and no ERA5T cutoff: CHIRPS ships as a finished daily product,
    # so its day is the product's and its history is settled (src/gee/chirps.py).
    log.info("year %s: sources=%s", year, present)

    conn = db_load.connect()
    loaded = quarantined = 0
    try:
        precip_load.ensure_schema(conn)
        precip_load.register_sources(conn, present)

        for source in present:
            code = source_code(source)
            for batch in precip_alt.iter_fparent_batches(
                bronze_dir, source, year, batch_size=int(fparent_batch_size)
            ):
                precip_load.ensure_partitions(conn, batch)
                frame = precip_alt.load_source_year(bronze_dir, source, year, batch)
                good, failures = precip_alt.split_valid(frame)

                loaded += precip_load.upsert_precip(conn, good)
                quarantined += precip_load.record_failures(
                    conn, failures, batch, year, code
                )

                report = precip_alt.calendar_report(frame, year)
                short = report.loc[report["missing"] > 0]
                if not short.empty:
                    log.warning(
                        "%s %s batch %s: %d cells short of the calendar (max %d days). "
                        "CHIRPS masks pixels it cannot estimate, so this may be legitimate.",
                        source, year, batch[0], len(short), int(short["missing"].max()),
                    )
                log.info(
                    "%s %s batch %s..%s: +%d rows",
                    source, year, batch[0], batch[-1], len(good),
                )
    finally:
        conn.close()

    log.info("year %s done: %d rows loaded, %d quarantined", year, loaded, quarantined)
    return {"year": year, "loaded": loaded, "quarantined": quarantined, "skipped": False}


def _verify(ti) -> dict:
    """Fail the run if no year loaded anything. A skipped year is fine; all of them is not.

    ``_transform_year`` deliberately skips a year whose bronze is absent rather than failing:
    a 45-year backfill must resume over gaps, and a missing year is normal mid-run. But that
    makes "nothing was there" and "everything worked" the same green task, and the usual
    cause of *every* year skipping is a ``data_root`` that does not match where bronze was
    downloaded — which is exactly the mistake this pipeline has already made twice, both
    times reporting success. Over 45 years that would be 45 green tasks and an empty table.

    So: per-year skips warn, a total of zero rows fails.
    """
    import logging

    log = logging.getLogger(__name__)
    results = [r for r in (ti.xcom_pull(task_ids="transform") or []) if r]

    loaded = sum(int(r.get("loaded") or 0) for r in results)
    quarantined = sum(int(r.get("quarantined") or 0) for r in results)
    skipped = [int(r["year"]) for r in results if r.get("skipped")]

    if skipped:
        log.warning(
            "%d of %d years had no bronze and were skipped: %s",
            len(skipped), len(results), skipped,
        )
    if results and not loaded:
        raise RuntimeError(
            f"every one of the {len(results)} year(s) skipped — 0 rows loaded. Bronze was "
            "not found for any of them, which usually means data_root does not match where "
            "download_bronze_chirps wrote (it defaults to $DATA_DIR, NOT to the root the "
            "download used). Check the transform's data_root param against the download's."
        )

    log.info(
        "verified: %d rows loaded across %d year(s), %d quarantined, %d skipped",
        loaded, len(results) - len(skipped), quarantined, len(skipped),
    )
    return {"loaded": loaded, "quarantined": quarantined, "skipped_years": skipped}


with DAG(
    dag_id="transform_precip_alt",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["chirps", "silver", "transform"],
    render_template_as_native_obj=True,
    # ONE run at a time. This is a memory guard, not a style choice: a transform task holds
    # ~850 MB (Airflow + pandas + pyarrow + psycopg2 baseline, plus a batch of parents), and
    # max_active_tasks below caps tasks only *within* a run. Airflow's default
    # max_active_runs is 16, so nothing stopped concurrent runs from multiplying that.
    #
    # Observed 2026-08-08: every download trigger fires its own kick_transform, and
    # reset_dag_run only dedupes an identical run_id. Four transform runs ended up live at
    # once — all starting at year 1981, so all re-upserting the same partitions — and 4 x 3
    # tasks x 850 MB froze an 8 GB Pi hard enough to need a power cycle.
    #
    # With this at 1, a second trigger queues behind the first instead of racing it.
    max_active_runs=1,
    # Same Pi memory rationale as transform_silver: concurrent years each hold a batch of
    # parents in RAM. A fine parent is 400 cells to a 0.25° parent's 16, so the batch size
    # below carries the difference rather than this.
    max_active_tasks=3,
    params={
        "start_year": 1981,
        "end_year": 1981,
        "sources": DEFAULT_SOURCES,
        "fparent_batch_size": 4,  # fine parents per commit — the memory lever
        "data_root": "",          # must match the root the bronze was downloaded to
    },
) as dag:
    plan = PythonOperator(task_id="plan", python_callable=_plan)
    transform = PythonOperator.partial(
        task_id="transform",
        python_callable=_transform_year,
        pool=SILVER_POOL,
    ).expand(op_kwargs=plan.output)
    verify = PythonOperator(task_id="verify", python_callable=_verify)

    plan >> transform >> verify
