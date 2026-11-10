"""DAG — repair_silver: apply the repair ladder to one registry finding (PLANNING.md §8.4).

**Detection is automatic; repair is not.** ``transform_silver`` and ``qa_scan`` write
findings to ``wth_data_issues`` and log loudly, and stop there. This DAG is the only thing
that changes a stored value, and it has to be triggered deliberately with an explicit
scope. Had the pipeline auto-filled instead, nobody would ever have learned that ERA5-Land
ships a corrupt band — the defect would have been quietly averaged away.

Four things happen together or not at all, per parent batch:

1. the repaired values are written **one column at a time** (``silver_load.update_column``
   — never ``upsert_wide``, which would null out the other seven variables),
2. every changed cell-day gets a ``wth_imputation_log`` row holding its *original* value,
   so the repair is reversible when a clean source appears,
3. cell-days that were quarantined for this date are reinstated into ``wth_base``, because
   a corrupt field leaves both wrong rows *and* a hole (712 cells on 1987-01-26),
4. ``rh`` and ``et0`` are re-derived from the repaired inputs, because a row whose derived
   columns still describe the defect is wrong in the hardest way to notice.

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

# Variables that feed Tetens RH (§12.1) and FAO-56 ET0 (§12.2). Repairing one of them
# leaves `rh` and `et0` describing the value that was just replaced — and wrongly, not
# merely staler: Tetens takes (tmax+tmin)/2, so a corrupt −5.49 °C tmin deflates es(tmean)
# and *inflates* rh, some of it clipping at 100. A repaired row whose derived columns still
# come from the defect is exactly the plausible-looking-but-wrong shape this whole module
# exists to end, so they are recomputed in the same transaction. `precip` feeds neither.
DERIVED_INPUTS = frozenset({"tmax", "tmin", "tdew", "srad", "wind"})
DERIVED_COLUMNS = ("rh", "et0")

# The full observation row: every ET0/RH input plus the two derived columns.
WIDE_COLUMNS = ["parent_id", "child_id", "date", "tmax", "tmin", "precip",
                "srad", "wind", "tdew", "rh", "et0"]


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
            "recompute_only": bool(params["recompute_only"]),
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
    recompute_only: bool = False,
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

    updated = reinstated = logged = rederived = 0
    try:
        silver_load.ensure_schema(conn)

        if recompute_only:
            rederived = _recompute_only(
                conn, issue_id, variable, target, parent_batch_size, dry_run
            )
            log.info("issue %s: %d derived values recomputed", issue_id, rederived)
            return {
                "issue_id": issue_id, "variable": variable, "date": date,
                "updated": 0, "reinstated": 0, "logged": 0,
                "rederived": rederived, "dry_run": dry_run,
            }

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
                # After the reinstatement, so the rows it just put back are re-derived too.
                batch_derived = _recompute_derived(conn, batch, target, variable, issue_id)
                conn.commit()
            except Exception:
                conn.rollback()
                log.error("batch %s..%s failed; rolled back", batch[0], batch[-1])
                raise

            updated += batch_updated
            logged += batch_logged
            reinstated += batch_reinstated
            rederived += batch_derived

        if not dry_run:
            # `refetch_pending`, not `imputed`: every rung implemented today is an
            # estimate, and `imputed` counts as resolved, so `fetch_open` would stop
            # listing the issue and the owed re-fetch would survive only as a log line
            # nobody reads twice. No date-scoped download exists in either backend yet
            # (the CDS splitter floors at one month, GEE exports whole years), so the debt
            # is real and stays visible until Step 5 can settle it.
            db_issues.set_status(
                conn,
                issue_id,
                db_issues.STATUS_REFETCH_PENDING,
                f"{updated} cells repaired this run, {reinstated} reinstated, "
                f"{rederived} derived values recomputed; "
                f"{_logged_total(conn, issue_id)} cell-days logged for this issue",
            )
            log.warning(
                "issue %s repaired by estimation; a clean-source re-fetch is still owed",
                issue_id,
            )
    finally:
        conn.close()

    return {
        "issue_id": issue_id, "variable": variable, "date": date,
        "updated": updated, "reinstated": reinstated, "logged": logged,
        "rederived": rederived, "dry_run": dry_run,
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


def _recompute_derived(conn, parents, target, variable, issue_id) -> int:
    """Re-derive ``rh`` and ``et0`` for the cell-days this repair just rewrote.

    Scoped by the ``imputed`` bit rather than by a cell list, so it covers the reinstated
    rows too — those come back carrying the ``rh``/``et0`` that ``wth_qa_failures`` froze
    at quarantine time, computed from the corrupt input.

    Derivation goes through ``merge.add_derived``, the same code path the transform uses,
    so a repaired row is derived identically to an observed one — including its handling of
    cells with no grid metadata, which get a NULL ``et0`` rather than a wrong one. Both
    columns get their own ``imputed`` bit and their own log row (``derived_from(...)``), so
    a consumer can tell an ET0 derived from an estimate from an observed one.

    Caller owns the transaction: this must land with the repair that made it necessary.
    """
    import pandas as pd

    from src.db import silver_load
    from src.transform import merge

    if variable not in DERIVED_INPUTS:
        return 0

    bit = silver_load.IMPUTED_BITS[variable]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {', '.join(WIDE_COLUMNS)} FROM wth_base "
            f"WHERE parent_id = ANY(%s::bpchar[]) AND date = %s AND imputed & {bit} > 0",
            (list(parents), target),
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    frame = pd.DataFrame(rows, columns=WIDE_COLUMNS)
    for column in ("parent_id", "child_id"):
        frame[column] = frame[column].str.strip()

    meta = silver_load.fetch_cell_meta(conn, parents)
    derived = merge.add_derived(frame.drop(columns=list(DERIVED_COLUMNS)), meta)

    keys = ["parent_id", "child_id", "date"]
    originals = frame.set_index(keys)
    index = pd.MultiIndex.from_frame(derived[keys])

    written = 0
    for column in DERIVED_COLUMNS:
        written += silver_load.update_column(
            conn,
            column,
            derived[keys].assign(value=derived[column]),
            commit=False,
        )
        silver_load.record_imputations(
            conn,
            derived[keys].assign(
                variable=column,
                method=f"derived_from({variable})",
                original_value=originals[column].reindex(index).to_numpy(),
                new_value=derived[column],
                issue_id=issue_id,
            ),
            commit=False,
        )
    return written


def _recompute_only(conn, issue_id, variable, target, parent_batch_size, dry_run) -> int:
    """Re-derive ``rh``/``et0`` for an issue that was repaired *before* the recompute existed.

    A plain re-run cannot do this. ``_fetch_targets`` scopes a ``constant_field`` incident
    by its corrupt **value**, and after a successful repair no row carries that value any
    more — so the ladder finds nothing, the batch loop never runs, and the derived columns
    stay wrong. The record of what was repaired lives in ``wth_imputation_log``, so the
    parent list comes from there: exact, small, and indexed, where a
    ``date``-only predicate over ``wth_base`` would scan all 1,659 partitions.
    """
    import logging

    log = logging.getLogger(__name__)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT parent_id FROM wth_imputation_log "
            "WHERE issue_id = %s AND variable = %s",
            (issue_id, variable),
        )
        parents = sorted(p.strip() for (p,) in cur.fetchall())

    if not parents:
        log.warning("issue %s: nothing logged for %s — nothing to recompute", issue_id, variable)
        return 0
    if variable not in DERIVED_INPUTS:
        log.info("issue %s: %s feeds neither rh nor et0", issue_id, variable)
        return 0
    if dry_run:
        log.info(
            "DRY RUN: would recompute rh/et0 on %s over %d parents", target, len(parents)
        )
        return 0

    rederived = 0
    for start in range(0, len(parents), parent_batch_size):
        batch = parents[start : start + parent_batch_size]
        try:
            rederived += _recompute_derived(conn, batch, target, variable, issue_id)
            conn.commit()
        except Exception:
            conn.rollback()
            log.error("batch %s..%s failed; rolled back", batch[0], batch[-1])
            raise
    return rederived


def _logged_total(conn, issue_id: int) -> int:
    """Cell-days recorded against one issue, across every run of this DAG.

    The per-run counters describe one trigger; an issue repaired in several passes (a
    9,842-cell incident, an interrupted run) needs the cumulative figure or its registry
    resolution understates what was actually done.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM wth_imputation_log WHERE issue_id = %s", (issue_id,)
        )
        return int(cur.fetchone()[0])


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

    columns = WIDE_COLUMNS
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
        # Skip the ladder and only re-derive rh/et0 for cell-days already repaired. For
        # issues repaired before the recompute existed: a normal re-run finds no targets,
        # because the corrupt value it scopes on is exactly what the repair removed.
        "recompute_only": False,
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
