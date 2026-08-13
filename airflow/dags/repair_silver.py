"""DAG — repair_silver: apply the repair ladder to one registry finding (PLANNING.md §8.4).

**Detection is automatic; repair is not.** ``transform_silver`` and ``qa_scan`` write
findings to ``wth_data_issues`` and log loudly, and stop there. This DAG is the only thing
that changes a stored value, and it has to be triggered deliberately with an explicit
scope. Had the pipeline auto-filled instead, nobody would ever have learned that ERA5-Land
ships a corrupt band — the defect would have been quietly averaged away.

Three things happen together or not at all, per parent batch:

1. the repaired values are written **one column at a time** (``silver_load.update_column``
   — never ``upsert_wide``, which would null out the other seven variables),
2. every changed cell-day gets a ``wth_imputation_log`` row holding its *original* value,
   so the repair is reversible when a clean source appears,
3. cell-days that were quarantined for this date are reinstated into ``wth_base``, because
   a corrupt field leaves both wrong rows *and* a hole (712 cells on 1987-01-26).

``dry_run`` is the default: it prints the diff and writes nothing.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# The cheap read: a plain date range around the target, which the planner prunes to a few
# days. Wide enough to flank an isolated gap with room to spare.
NARROW_WINDOW_DAYS = 5

# The expensive read: the same day-of-year in every year of the archive. Matches
# repair.CLIMATOLOGY_WINDOW_DAYS, and is only issued when a day-of-year rung is actually
# reached — see _repair_batch.
CLIMATOLOGY_WINDOW_DAYS = 7

# wth_base columns are REAL (float4), so a value recorded in the registry as a float8 never
# compares equal. Half a millikelvin is far tighter than any real spread between cells and
# far looser than the float4 rounding gap.
VALUE_MATCH_TOLERANCE = 5e-4


def _resolve(**context) -> list[dict]:
    """Turn the run params into one repair job, failing loudly if the scope is ambiguous."""
    params = context["params"]

    from src.db import issues as db_issues
    from src.db import load as db_load

    issue_id = params.get("issue_id") or None
    variable = params.get("variable") or None
    date = params.get("date") or None

    if not issue_id and not (variable and date):
        raise ValueError("give either issue_id, or both variable and date")

    conn = db_load.connect()
    try:
        found = (
            db_issues.fetch_issue(conn, int(issue_id))
            if issue_id
            else db_issues.fetch_for(conn, variable, date)
        )
    finally:
        conn.close()

    if found.empty:
        raise ValueError(f"no registry issue for issue_id={issue_id} variable={variable} date={date}")

    return [
        {
            "issue_id": int(row.issue_id),
            "variable": row.variable,
            "date": str(row.date),
            "detector": row.detector,
            "detail": row.detail,
            "method": params["method"],
            "seed": int(params["seed"]),
            "dry_run": bool(params["dry_run"]),
            "parent_batch_size": int(params["parent_batch_size"]),
        }
        for row in found.itertuples(index=False)
    ]


def constant_value(detector: str, detail: dict) -> float | None:
    """The single value a ``constant_field`` finding is made of, if it is one.

    A `constant_field` incident is defined by a *value*, not by a date, and that difference
    matters: the same calendar date holds cells that are fine. Returns ``None`` for
    detectors that do not describe one value, which makes the repair fall back to
    date scoping.
    """
    if detector != "constant_field":
        return None
    low, high = detail.get("min"), detail.get("max")
    if low is None or high is None or low != high:
        return None
    return float(low)


def _repair(
    issue_id: int,
    variable: str,
    date: str,
    detector: str = "constant_field",
    detail: dict | None = None,
    method: str = "auto",
    seed: int = 0,
    dry_run: bool = True,
    parent_batch_size: int = 8,
) -> dict:
    import logging
    from datetime import date as date_cls

    from src.db import issues as db_issues
    from src.db import load as db_load
    from src.db import silver_load

    log = logging.getLogger(__name__)
    target = date_cls.fromisoformat(date)
    conn = db_load.connect()

    updated = reinstated = logged = 0
    try:
        silver_load.ensure_schema(conn)

        # Every date already known to be bad for this variable — they must not anchor an
        # interpolation or contribute to a climatology.
        flagged = db_issues.fetch_open(conn)
        exclude = [
            row.date for row in flagged.itertuples(index=False) if row.variable == variable
        ]

        # An incident is a set of *cells*, not a whole calendar date. On 1987-01-26 exactly
        # 341 cells are fine — leftovers from an older, wider extent whose own corrupt day
        # is 01-25 — and repairing them would overwrite good data with an interpolation
        # anchored on their corrupt neighbour. Scope to the cells that actually carry the
        # defect; fall back to the whole date only for detectors with no single value.
        constant = constant_value(detector, detail or {})
        targets = _fetch_targets(conn, variable, target, constant)
        parents = sorted({p for p, _ in targets})
        target_cells = {c for _, c in targets}

        log.info(
            "repairing %s on %s: %d cells over %d parents (value=%s, method=%s, dry_run=%s)",
            variable, target, len(target_cells), len(parents), constant, method, dry_run,
        )

        for start in range(0, len(parents), parent_batch_size):
            batch = parents[start : start + parent_batch_size]

            values, applied = _repair_batch(
                conn, batch, variable, target, method, exclude, seed, constant
            )
            if values is None:
                continue
            values = values[values.index.isin(target_cells)].dropna()
            if values.empty:
                log.warning("batch %s..%s: no rung produced a value", batch[0], batch[-1])
                continue

            current = _fetch_current(conn, batch, variable, target)
            frame = current.merge(
                values.rename("new_value").reset_index(), on="child_id", how="right"
            )
            frame = frame.dropna(subset=["parent_id"])

            if dry_run:
                log.info(
                    "DRY RUN %s..%s (%s): %s",
                    batch[0], batch[-1], applied,
                    frame[["child_id", "value", "new_value"]].head(10).to_string(index=False),
                )
                updated += len(frame)
                continue

            # One transaction per batch. A repaired value without its log row is an
            # unrecorded, irreversible edit, and a reinstated row without its repair is a
            # hole filled with the corrupt value — so all three land together or none do.
            try:
                repaired = frame.assign(date=target)[
                    ["parent_id", "child_id", "date", "new_value"]
                ]
                batch_updated = silver_load.update_column(
                    conn, variable, repaired.rename(columns={"new_value": "value"}),
                    commit=False,
                )
                batch_logged = silver_load.record_imputations(
                    conn,
                    frame.assign(
                        date=target, variable=variable, method=applied, issue_id=issue_id
                    ).rename(columns={"value": "original_value"})[
                        ["parent_id", "child_id", "date", "variable",
                         "method", "original_value", "new_value", "issue_id"]
                    ],
                    commit=False,
                )
                batch_reinstated = _reinstate_quarantined(
                    conn, batch, target, variable, values, issue_id, applied
                )
                conn.commit()
            except Exception:
                conn.rollback()
                log.error("batch %s..%s failed; rolled back", batch[0], batch[-1])
                raise

            updated += batch_updated
            logged += batch_logged
            reinstated += batch_reinstated

        if not dry_run:
            db_issues.set_status(
                conn,
                issue_id,
                db_issues.STATUS_IMPUTED,
                f"{updated} cells repaired; {reinstated} reinstated",
            )
            # The value is usable but an independent source is still owed — no date-scoped
            # download exists in either backend yet, so this stays visible as open work.
            log.warning(
                "issue %s repaired by estimation; a clean-source re-fetch is still owed",
                issue_id,
            )
    finally:
        conn.close()

    return {
        "issue_id": issue_id, "variable": variable, "date": date,
        "updated": updated, "reinstated": reinstated, "logged": logged,
        "dry_run": dry_run,
    }


def _mask_constant(history, constant):
    """Blank the corrupt value wherever it appears in the surrounding history.

    A corrupt UTC band does not respect local calendar days: `era5_land_base_grid.t_zone` is
    per cell, so one bad band lands on different local dates in different offset zones. On
    1987-01-26 that puts the same constant on 01-25 for a whole group of cells — which is
    exactly the day an interpolation for 01-26 wants to anchor on. Masking it means a
    corrupt neighbour can never become an anchor; affected cells return NaN and fall to the
    next rung instead of quietly averaging the defect back in.
    """
    if constant is None or history.empty:
        return history
    bad = history["value"].between(
        constant - VALUE_MATCH_TOLERANCE, constant + VALUE_MATCH_TOLERANCE
    )
    return history.assign(value=history["value"].mask(bad))


def _fetch_targets(conn, variable, target, constant) -> list[tuple[str, str]]:
    """``(parent_id, child_id)`` of every cell this incident actually affects.

    Two sources, because a corrupt field leaves two marks: rows carrying the bad value, and
    the *hole* where QA rejected the rows it happened to catch (712 cells on 1987-01-26).
    Both need repairing, so both are targets.

    With ``constant`` known, the wth_base side matches on the value rather than the date.
    REAL is float4, so the stored value is compared within a tolerance rather than by
    equality.
    """
    if constant is None:
        value_predicate = f"{variable} IS NOT NULL"
        params = (target,)
    else:
        value_predicate = f"{variable} BETWEEN %s AND %s"
        params = (constant - VALUE_MATCH_TOLERANCE, constant + VALUE_MATCH_TOLERANCE, target)

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT parent_id, child_id FROM wth_base WHERE {value_predicate} AND date = %s "
            "UNION "
            "SELECT parent_id, child_id FROM wth_qa_failures WHERE date = %s",
            (*params, target),
        )
        return [(p.strip(), c.strip()) for p, c in cur.fetchall()]


def _repair_batch(conn, batch, variable, target, method, exclude, seed, constant=None):
    """Repair one parent batch, fetching only as much history as the rung actually needs.

    **Tiered on purpose.** Interpolation — the rung that resolves both `tmin` incidents —
    looks at the immediate flanks and nothing else, so it needs a 7-day slice. The
    climatology and analog-day rungs need the same day-of-year across every year, which is
    a ~1,000-day slice behind a non-indexable ``extract(doy ...)`` predicate that cannot
    prune partitions by date.

    Fetching the wide window unconditionally made this task scan ~830k rows per batch over
    84 batches to answer a question the narrow window answers in seven days of data. On the
    Pi that was enough sustained IO and cache pressure to exhaust a 200 MB swap file and
    hard-freeze the host. So: narrow first, widen only when the narrow rungs come back
    empty.
    """
    from src.transform import repair

    narrow = _mask_constant(_fetch_history(conn, batch, variable, target, wide=False), constant)
    if not narrow.empty:
        values, applied = repair.repair_field(
            narrow, variable, target, method=method, exclude_dates=exclude, seed=seed
        )
        if values.notna().any():
            return values, applied

    # Only the day-of-year rungs are left, and only they justify the wide read.
    if not repair.needs_doy_history(variable, method):
        return None, None

    wide = _mask_constant(_fetch_history(conn, batch, variable, target, wide=True), constant)
    if wide.empty:
        return None, None

    values, applied = repair.repair_field(
        wide, variable, target, method=method, exclude_dates=exclude, seed=seed
    )
    return values, applied


def _fetch_history(conn, parents, variable, target, *, wide: bool):
    """Surrounding history for one parent batch.

    ``wide=False`` is a plain date range around the target — a handful of days, and the
    planner prunes to them. ``wide=True`` adds the same day-of-year in every other year,
    which no index can serve; only the climatology and analog-day rungs need it.

    **The ``::bpchar[]`` cast is load-bearing, not cosmetic.** ``parent_id`` is ``CHAR(4)``
    and psycopg2 sends a Python list as ``text[]``, so a bare ``= ANY(%s)`` makes Postgres
    coerce the *column* — ``(parent_id)::text = ANY(...)`` — and a cast on the partition key
    defeats partition pruning entirely. That turned an 8-partition lookup into 1,659
    sequential scans of a 172M-row table: 167 s for a query that should be milliseconds, per
    batch, which is what froze the host. Casting the parameter instead keeps the comparison
    on the raw column type and the plan becomes an index-only scan per partition.
    """
    import pandas as pd

    if wide:
        sql = (
            f"SELECT child_id, date, {variable} FROM wth_base "
            "WHERE parent_id = ANY(%s::bpchar[]) "
            "AND abs(extract(doy FROM date) - extract(doy FROM %s::date)) <= %s"
        )
        params = (list(parents), target, CLIMATOLOGY_WINDOW_DAYS)
    else:
        sql = (
            f"SELECT child_id, date, {variable} FROM wth_base "
            "WHERE parent_id = ANY(%s::bpchar[]) AND date BETWEEN %s AND %s"
        )
        params = (
            list(parents),
            target - timedelta(days=NARROW_WINDOW_DAYS),
            target + timedelta(days=NARROW_WINDOW_DAYS),
        )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    frame = pd.DataFrame(rows, columns=["child_id", "date", "value"])
    frame["child_id"] = frame["child_id"].str.strip()
    return frame


def _fetch_current(conn, parents, variable, target):
    import pandas as pd

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT parent_id, child_id, {variable} FROM wth_base "
            "WHERE parent_id = ANY(%s::bpchar[]) AND date = %s",
            (list(parents), target),
        )
        rows = cur.fetchall()
    frame = pd.DataFrame(rows, columns=["parent_id", "child_id", "value"])
    for column in ("parent_id", "child_id"):
        frame[column] = frame[column].str.strip()
    return frame


def _reinstate_quarantined(conn, parents, target, variable, values, issue_id, method) -> int:
    """Move this date's quarantined cell-days back into ``wth_base`` with the repaired value.

    A corrupt field leaves two marks: wrong rows, and a *hole* where QA rejected the rows
    it happened to catch. Fixing only the wrong rows would leave 1987 permanently 720 rows
    short of its calendar.

    These rows get a ``wth_imputation_log`` entry too, and they need it more than the
    others: reinstating deletes the ``wth_qa_failures`` row, which is the only remaining
    copy of what the cell-day originally held. Without the log the reinstatement would be
    the one irreversible step in the whole repair.
    """
    import pandas as pd

    from src.db import silver_load

    columns = ["parent_id", "child_id", "date", "tmax", "tmin", "precip",
               "srad", "wind", "tdew", "rh", "et0"]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(columns)} FROM wth_qa_failures "
            "WHERE parent_id = ANY(%s::bpchar[]) AND date = %s",
            (list(parents), target),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    frame = pd.DataFrame(rows, columns=columns)
    for column in ("parent_id", "child_id"):
        frame[column] = frame[column].str.strip()

    original = frame.set_index("child_id")[variable]
    frame[variable] = frame["child_id"].map(values)
    frame = frame.dropna(subset=[variable])
    if frame.empty:
        return 0

    silver_load.record_imputations(
        conn,
        frame.assign(
            variable=variable,
            method=f"{method} (reinstated from quarantine)",
            original_value=frame["child_id"].map(original),
            new_value=frame[variable],
            issue_id=issue_id,
        )[["parent_id", "child_id", "date", "variable",
           "method", "original_value", "new_value", "issue_id"]],
        commit=False,
    )

    frame = frame.assign(
        is_preliminary=False, imputed=silver_load.IMPUTED_BITS[variable]
    )
    # commit=False throughout: the caller owns the transaction so this lands with the
    # repair and its log row, never on its own.
    silver_load.ensure_partitions(conn, frame["parent_id"], commit=False)
    written = silver_load.upsert_wide(conn, frame, commit=False)

    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM wth_qa_failures WHERE parent_id = ANY(%s::bpchar[]) AND date = %s "
            "AND child_id = ANY(%s::bpchar[])",
            (list(parents), target, list(frame["child_id"])),
        )
    return written


with DAG(
    dag_id="repair_silver",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=1,
    tags=["era5", "silver", "qa", "repair"],
    params={
        "issue_id": "",          # registry issue to repair; or give variable + date
        "variable": "",
        "date": "",              # ISO date
        "method": "auto",        # auto walks that variable's ladder (repair.LADDER)
        "seed": 0,               # analog-day draw; same seed reproduces the same donor
        "dry_run": True,         # default: print the diff, write nothing
        # Higher than transform_silver's 8: a repair batch holds one variable over a
        # ~11-day window, not seven variables over a year, so the memory lever can be
        # slacker and fewer round trips is the thing that matters here.
        "parent_batch_size": 64,
    },
) as dag:
    resolve = PythonOperator(task_id="resolve", python_callable=_resolve)
    apply = PythonOperator.partial(
        task_id="apply",
        python_callable=_repair,
    ).expand(op_kwargs=resolve.output)
    resolve >> apply
