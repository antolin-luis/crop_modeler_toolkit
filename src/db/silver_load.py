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
import re
from datetime import date
from pathlib import Path

import pandas as pd

from src.db import load as db_load

# Same ``UTC`` / ``UTC±HH:MM`` form the download DAGs take (mirrors src/gee/daily.py's
# parser). Kept here, not imported from src/gee, so the silver path never pulls in the
# earthengine dependency.
_TZ_RE = re.compile(r"^UTC(?:([+-])(\d{2}):(\d{2}))?$")

SCHEMA_SQL = Path(__file__).with_name("silver_schema.sql")
TABLE = "wth_base"
FAILURES_TABLE = "wth_qa_failures"

# wth_base columns written by the loader (ingested_at is set by the INSERT itself).
COLUMNS = [
    "parent_id", "child_id", "date",
    "tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0",
    "is_preliminary",
]
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


def ensure_partitions(conn, parent_ids) -> None:
    """Create any missing ``wth_base`` partitions for ``parent_ids`` (§8.2)."""
    with conn.cursor() as cur:
        for parent_id in sorted(set(parent_ids)):
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {partition_name(parent_id)} "
                f"PARTITION OF {TABLE} FOR VALUES IN (%s)",
                (parent_id,),
            )
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


def parse_offset_minutes(timezone: str) -> int:
    """Parse a ``UTC`` / ``UTC±HH:MM`` local-day offset to signed minutes (§5.3).

    Minutes, not hours, so fractional zones (``UTC+05:30`` → 330) are exact. ``UTC`` → 0.
    """
    m = _TZ_RE.match(timezone.strip())
    if not m:
        raise ValueError(f"bad timezone {timezone!r}; expected 'UTC' or 'UTC±HH:MM'")
    sign, hh, mm = m.groups()
    if sign is None:
        return 0
    magnitude = int(hh) * 60 + int(mm)
    return magnitude if sign == "+" else -magnitude


def upsert_cell_timezone(conn, child_ids, offset_minutes: int) -> int:
    """Record the local-day offset for ``child_ids`` in ``cell_timezone`` (§5.3).

    Idempotent per cell: ``ON CONFLICT DO UPDATE`` keeps the latest offset, matching
    ``wth_base``'s own last-write-wins upsert. Called once per loaded batch; the offset is
    uniform within a region, so the repeated writes are cheap and self-correcting.
    """
    codes = sorted({c for c in child_ids})
    if not codes:
        return 0

    staging = "_tz_staging"
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {staging} "
            "(child_id CHAR(4), utc_offset_minutes SMALLINT) ON COMMIT DROP"
        )
    buf = io.StringIO()
    for code in codes:
        buf.write(f"{code},{offset_minutes}\n")
    buf.seek(0)
    db_load.copy_csv(conn, staging, ["child_id", "utc_offset_minutes"], buf)

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO cell_timezone (child_id, utc_offset_minutes) "
            f"SELECT child_id, utc_offset_minutes FROM {staging} "
            "ON CONFLICT (child_id) DO UPDATE SET "
            "utc_offset_minutes = EXCLUDED.utc_offset_minutes, updated_at = now()"
        )
    conn.commit()
    return len(codes)


def _copy_frame(conn, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    """``COPY`` a frame's ``columns`` into ``table`` (NaN → NULL via empty CSV fields)."""
    buf = io.StringIO()
    frame.to_csv(buf, columns=columns, index=False, header=False)
    buf.seek(0)
    db_load.copy_csv(conn, table, columns, buf)


def upsert_wide(conn, wide: pd.DataFrame) -> int:
    """Upsert a wide frame into ``wth_base``; returns the row count written.

    Caller must have run :func:`ensure_schema` and :func:`ensure_partitions` for the
    frame's parents. Commits on success so a long backfill resumes per batch.
    """
    if wide.empty:
        return 0

    staging = "_wth_staging"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {staging} ({_STAGING_DDL}, is_preliminary BOOLEAN) ON COMMIT DROP")

    _copy_frame(conn, staging, COLUMNS, wide)

    assignments = ", ".join(
        f"{c} = EXCLUDED.{c}"
        for c in ("tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0",
                  "is_preliminary")
    )
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
            f"SELECT {', '.join(COLUMNS)} FROM {staging} "
            "ON CONFLICT (parent_id, child_id, date) DO UPDATE SET "
            f"{assignments}, ingested_at = now()"
        )
    conn.commit()
    return len(wide)


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
