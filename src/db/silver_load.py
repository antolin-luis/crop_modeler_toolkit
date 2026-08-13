"""Wide silver frame → ``wth_base`` (PLANNING.md §8.2).

Load path: ``CREATE TEMP TABLE`` → ``COPY`` → ``INSERT … SELECT … ON CONFLICT DO UPDATE``.
Never row-by-row ``INSERT`` (§8.2), and never a plain ``COPY`` straight into ``wth_base``
either — ``COPY`` cannot upsert, and the daily-append and ERA5T re-fetch paths (§11.3,
DAG 4) both re-write existing cell-days.

Two constraints drive the shape of this module:

- **Partitions must exist before the insert.** ``wth_base`` is ``PARTITION BY LIST
  (parent_id)``; an insert with no matching partition is an error, not an auto-create. So
  every batch calls :func:`ensure_partitions` first.
- **``is_preliminary`` is a property of the source, not of the row's completeness**
  (§8.3). It is derived from the date against a rolling cutoff (§11.3): ERA5T covers
  roughly the trailing three months and is later replaced by final ERA5.

Connection handling and the ``COPY`` primitive are reused from ``src/db/load.py``.
"""

from __future__ import annotations

import calendar
import io
from datetime import date
from pathlib import Path

import pandas as pd

from src.db import load as db_load

SCHEMA_SQL = Path(__file__).with_name("silver_schema.sql")
TABLE = "wth_base"
FAILURES_TABLE = "wth_qa_failures"
IMPUTATION_LOG_TABLE = "wth_imputation_log"

# wth_base columns written by the loader (ingested_at is set by the INSERT itself).
COLUMNS = [
    "parent_id", "child_id", "date",
    "tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0",
    "is_preliminary", "imputed",
]

# Provenance bitmask on wth_base.imputed, in DDL column order — a repair sets the bit for
# the variable it rewrote, so a consumer can tell an imputed tmin from an observed one.
# A normal transform writes 0 for every row: nothing here is imputed.
IMPUTED_BITS = {
    "tmax": 1, "tmin": 2, "precip": 4, "srad": 8,
    "wind": 16, "tdew": 32, "rh": 64, "et0": 128,
}
FAILURE_COLUMNS = [
    "parent_id", "child_id", "date",
    "tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0",
    "reason",
]

_STAGING_DDL = """
    parent_id CHAR(4), child_id CHAR(4), date DATE,
    tmax REAL, tmin REAL, precip REAL, srad REAL, wind REAL, tdew REAL,
    rh REAL, et0 REAL
"""


def ensure_schema(conn) -> None:
    """Apply the silver DDL (idempotent — every statement is IF NOT EXISTS)."""
    db_load.execute_script(conn, SCHEMA_SQL.read_text())
    conn.commit()


def partition_name(parent_id: str) -> str:
    """Partition table name for one ``parent_id``. The ``wth_`` prefix keeps the
    identifier valid even though a parent code may start with a digit."""
    return f"wth_{parent_id}"


def ensure_partitions(conn, parent_ids, *, commit: bool = True) -> None:
    """Create any missing ``wth_base`` partitions for ``parent_ids`` (§8.2)."""
    with conn.cursor() as cur:
        for parent_id in sorted(set(parent_ids)):
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {partition_name(parent_id)} "
                f"PARTITION OF {TABLE} FOR VALUES IN (%s)",
                (parent_id,),
            )
    if commit:
        conn.commit()


def preliminary_cutoff(run_date: date, months: int = 3) -> date:
    """First date still considered ERA5T-preliminary: ``run_date`` minus ``months`` (§11.3)."""
    month = run_date.month - months
    year = run_date.year
    while month <= 0:
        month += 12
        year -= 1
    day = min(run_date.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def assign_preliminary(wide: pd.DataFrame, cutoff: date) -> pd.DataFrame:
    """Set ``is_preliminary`` = date on/after ``cutoff`` (§11.3)."""
    dates = pd.to_datetime(wide["date"]).dt.date
    return wide.assign(is_preliminary=[d >= cutoff for d in dates])


def fetch_cell_meta(conn, parent_ids) -> pd.DataFrame:
    """Static per-cell ``lat`` / ``elevation`` from the grid seed — the ET0 inputs (§12.2)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT child_id, lat, elevation FROM era5_land_base_grid "
            "WHERE parent_id = ANY(%s)",
            (list(parent_ids),),
        )
        rows = cur.fetchall()
    meta = pd.DataFrame(rows, columns=["child_id", "lat", "elevation"])
    # CHAR(4) comes back space-padded from psycopg2; bronze codes are not.
    meta["child_id"] = meta["child_id"].str.strip()
    return meta


def _copy_frame(conn, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    """``COPY`` a frame's ``columns`` into ``table`` (NaN → NULL via empty CSV fields)."""
    buf = io.StringIO()
    frame.to_csv(buf, columns=columns, index=False, header=False)
    buf.seek(0)
    db_load.copy_csv(conn, table, columns, buf)


def upsert_wide(conn, wide: pd.DataFrame, *, commit: bool = True) -> int:
    """Upsert a wide frame into ``wth_base``; returns the row count written.

    Caller must have run :func:`ensure_schema` and :func:`ensure_partitions` for the
    frame's parents. Commits on success so a long backfill resumes per batch.
    """
    if wide.empty:
        return 0

    if "imputed" not in wide.columns:
        wide = wide.assign(imputed=0)

    staging = "_wth_staging"
    with conn.cursor() as cur:
        cur.execute(
            f"DROP TABLE IF EXISTS {staging}; "
            f"CREATE TEMP TABLE {staging} ({_STAGING_DDL}, "
            "is_preliminary BOOLEAN, imputed SMALLINT) ON COMMIT DROP"
        )

    _copy_frame(conn, staging, COLUMNS, wide)

    assignments = ", ".join(
        f"{c} = EXCLUDED.{c}"
        for c in ("tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0",
                  "is_preliminary", "imputed")
    )
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
            f"SELECT {', '.join(COLUMNS)} FROM {staging} "
            "ON CONFLICT (parent_id, child_id, date) DO UPDATE SET "
            f"{assignments}, ingested_at = now()"
        )
    if commit:
        conn.commit()
    return len(wide)


def update_column(conn, variable: str, repaired: pd.DataFrame, *, commit: bool = True) -> int:
    """Rewrite **one** column of existing ``wth_base`` rows; returns the row count updated.

    Deliberately *not* :func:`upsert_wide`. That one assigns every non-key column from
    ``EXCLUDED``, so feeding it a frame that carries only the repaired variable would null
    out the other seven — which is exactly how a 5.4M-row layer got wrecked once already.
    A repair touches one variable, so the write must touch one column.

    ``repaired`` needs ``parent_id, child_id, date, value``. The matching ``imputed`` bit is
    OR-ed in rather than assigned, so repairing ``tmin`` on a cell-day whose ``precip`` was
    already imputed does not erase that fact. Rows with no existing ``wth_base`` row are
    left alone: reinstating a quarantined cell-day is an insert, and that is the caller's
    job (it needs the full row, not one column).

    ``commit=False`` leaves the transaction open so the caller can bundle this with
    :func:`record_imputations` — a repair written *without* its log row is unrecorded and
    therefore irreversible, so the two must land together or not at all.
    """
    if variable not in IMPUTED_BITS:
        raise ValueError(
            f"{variable!r} is not a wth_base value column; known: {', '.join(IMPUTED_BITS)}"
        )
    if repaired.empty:
        return 0

    staging = "_wth_repair"
    with conn.cursor() as cur:
        cur.execute(
            f"DROP TABLE IF EXISTS {staging}; "
            f"CREATE TEMP TABLE {staging} "
            "(parent_id CHAR(4), child_id CHAR(4), date DATE, value REAL) ON COMMIT DROP"
        )

    _copy_frame(conn, staging, ["parent_id", "child_id", "date", "value"], repaired)

    # The redundant-looking parent_id list is what makes this prunable. Postgres cannot
    # prune LIST partitions from a join condition (`t.parent_id = s.parent_id`) because the
    # staging table's values are unknown at plan time, so without this the UPDATE touches
    # all 1,659 partitions of a 172M-row table. The explicit `= ANY(...)` — cast to
    # bpchar[] so the partition key is not coerced to text — restricts it to the handful
    # actually involved.
    parents = sorted(set(repaired["parent_id"]))
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {TABLE} t SET {variable} = s.value, "
            f"imputed = t.imputed | {IMPUTED_BITS[variable]}, ingested_at = now() "
            f"FROM {staging} s "
            "WHERE t.parent_id = s.parent_id AND t.child_id = s.child_id AND t.date = s.date "
            "AND t.parent_id = ANY(%s::bpchar[])",
            (parents,),
        )
        updated = cur.rowcount
    if commit:
        conn.commit()
    return updated


def record_imputations(conn, log_rows: pd.DataFrame, *, commit: bool = True) -> int:
    """Append to ``wth_imputation_log`` — one row per repaired cell-day-variable.

    Holds the original value, so a repair is reversible when a fixed source appears. A
    re-repair of the same cell-day-variable overwrites its log row rather than duplicating.
    """
    if log_rows.empty:
        return 0

    columns = ["parent_id", "child_id", "date", "variable",
               "method", "original_value", "new_value", "issue_id"]
    staging = "_wth_imputation_staging"
    with conn.cursor() as cur:
        # INCLUDING DEFAULTS is load-bearing: a bare LIKE copies the NOT NULL on
        # `applied_at` but not its DEFAULT now(), so the COPY below — which does not supply
        # that column — fails the constraint.
        cur.execute(
            f"DROP TABLE IF EXISTS {staging}; "
            f"CREATE TEMP TABLE {staging} "
            f"(LIKE {IMPUTATION_LOG_TABLE} INCLUDING DEFAULTS) ON COMMIT DROP"
        )

    _copy_frame(conn, staging, columns, log_rows)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {IMPUTATION_LOG_TABLE} ({', '.join(columns)}) "
            f"SELECT {', '.join(columns)} FROM {staging} "
            "ON CONFLICT (parent_id, child_id, date, variable) DO UPDATE SET "
            "method = EXCLUDED.method, original_value = EXCLUDED.original_value, "
            "new_value = EXCLUDED.new_value, issue_id = EXCLUDED.issue_id, "
            "applied_at = now()"
        )
    if commit:
        conn.commit()
    return len(log_rows)


def record_failures(conn, failures: pd.DataFrame, parent_ids, year: int) -> int:
    """Replace the quarantine rows for ``(parent_ids, year)`` with ``failures`` (§8.4).

    Clearing first keeps the table truthful: a cell-day that fails today and passes after
    a bronze re-fetch must not linger as a stale failure.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {FAILURES_TABLE} "
            "WHERE parent_id = ANY(%s) AND date >= %s AND date <= %s",
            (list(parent_ids), date(year, 1, 1), date(year, 12, 31)),
        )
    if not failures.empty:
        _copy_frame(conn, FAILURES_TABLE, FAILURE_COLUMNS, failures)
    conn.commit()
    return len(failures)
