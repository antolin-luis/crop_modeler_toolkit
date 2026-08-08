"""Silver loader for ``wth_precip_alt`` — CHIRPS on the 0.05° fine grid.

The fine-grid counterpart of ``src/db/silver_load.py``, and structurally the same:
``CREATE TEMP TABLE`` → ``COPY`` → ``INSERT … SELECT … ON CONFLICT DO UPDATE``. Never
row-by-row ``INSERT``, and never a plain ``COPY`` into the target — ``COPY`` cannot upsert,
and re-running a year (after a bronze re-fetch, or to add a second source) must be
idempotent.

Two differences from the ERA5 loader, both from CHIRPS being a finished daily product:
there is no unit conversion (already mm/day) and no ``is_preliminary`` flag (v2.0 final and
v3.0 RNL are settled histories, not rolling revisions).

Partition names are prefixed ``pcp_``, not ``wth_``. Fine and ERA5 parent codes come from
different code spaces — a 5-char ``00JON`` and a 4-char ``0S0N`` are unrelated — so sharing
the prefix would risk a collision in the global table namespace.
"""

from __future__ import annotations

import io
from datetime import date
from pathlib import Path

import pandas as pd

from src.db import load as db_load

SCHEMA_SQL = Path(__file__).with_name("precip_schema.sql")
TABLE = "wth_precip_alt"
SOURCE_TABLE = "wth_precip_alt_source"
FAILURES_TABLE = "wth_precip_alt_qa_failures"

COLUMNS = ["fparent_id", "fine_id", "date", "source", "precip"]
FAILURE_COLUMNS = [*COLUMNS, "reason"]

_STAGING_DDL = (
    "fparent_id CHAR(5), fine_id CHAR(5), date DATE, source SMALLINT, precip REAL"
)


def ensure_schema(conn) -> None:
    """Apply ``precip_schema.sql`` (idempotent)."""
    db_load.execute_script(conn, SCHEMA_SQL.read_text())
    conn.commit()


def partition_name(fparent_id: str) -> str:
    """Partition table name for one ``fparent_id``.

    The ``pcp_`` prefix keeps the identifier valid even though a parent code may start with a
    digit, and keeps fine-grid partitions out of the ``wth_`` namespace the 0.25° grid uses.
    """
    return f"pcp_{fparent_id.strip()}"


def ensure_partitions(conn, fparent_ids) -> None:
    """Create any missing ``wth_precip_alt`` partitions.

    A LIST insert with no matching partition errors, so this must run before every load.
    """
    with conn.cursor() as cur:
        for fparent_id in sorted({f.strip() for f in fparent_ids}):
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {partition_name(fparent_id)} "
                f"PARTITION OF {TABLE} FOR VALUES IN (%s)",
                (fparent_id,),
            )
    conn.commit()


def register_sources(conn, names) -> None:
    """Upsert the ``wth_precip_alt_source`` rows for ``names`` from the CHIRPS registry.

    Keeps the SMALLINT codes stored in the fact table decodable without reading Python.
    """
    from src.gee.chirps import source_code, source_spec

    with conn.cursor() as cur:
        for name in sorted(set(names)):
            spec = source_spec(name)
            cur.execute(
                f"INSERT INTO {SOURCE_TABLE} (source, code, collection, note) "
                "VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (source) DO UPDATE SET "
                "code = EXCLUDED.code, collection = EXCLUDED.collection, "
                "note = EXCLUDED.note",
                (source_code(name), name, spec.collection, spec.note),
            )
    conn.commit()


def _copy_frame(conn, table: str, columns: list[str], frame: pd.DataFrame) -> None:
    """``COPY`` a frame's ``columns`` into ``table`` (NaN → NULL via empty CSV fields)."""
    buf = io.StringIO()
    frame.to_csv(buf, columns=columns, index=False, header=False)
    buf.seek(0)
    db_load.copy_csv(conn, table, columns, buf)


def upsert_precip(conn, frame: pd.DataFrame) -> int:
    """Upsert a frame into ``wth_precip_alt``; returns the row count written.

    Caller must have run :func:`ensure_schema` and :func:`ensure_partitions` for the frame's
    parents. Commits on success so a long backfill resumes per batch.

    ``source`` being in the conflict target is what lets v2 and v3 coexist for the same
    cell-day rather than overwrite each other.
    """
    if frame.empty:
        return 0

    staging = "_precip_staging"
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {staging} ({_STAGING_DDL}) ON COMMIT DROP")

    _copy_frame(conn, staging, COLUMNS, frame)

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
            f"SELECT {', '.join(COLUMNS)} FROM {staging} "
            "ON CONFLICT (fparent_id, fine_id, date, source) DO UPDATE SET "
            "precip = EXCLUDED.precip, ingested_at = now()"
        )
    conn.commit()
    return len(frame)


def record_failures(
    conn, failures: pd.DataFrame, fparent_ids, year: int, source: int
) -> int:
    """Replace the quarantine rows for ``(fparent_ids, year, source)`` with ``failures``.

    Clearing first keeps the table truthful: a cell-day that fails today and passes after a
    bronze re-fetch must not linger as a stale failure.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {FAILURES_TABLE} "
            "WHERE fparent_id = ANY(%s) AND source = %s AND date >= %s AND date <= %s",
            (list(fparent_ids), source, date(year, 1, 1), date(year, 12, 31)),
        )
    if not failures.empty:
        _copy_frame(conn, FAILURES_TABLE, FAILURE_COLUMNS, failures)
    conn.commit()
    return len(failures)


def mark_valid_cells(conn) -> int:
    """Set ``chirps_base_grid.is_valid`` from cells that actually produced rows.

    Exact by construction, and it avoids inventing a land mask at a resolution finer than any
    mask this project has. Run after a backfill; returns the number of cells marked valid.
    """
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE chirps_base_grid g SET is_valid = EXISTS ("
            f"  SELECT 1 FROM {TABLE} p WHERE p.fine_id = g.fine_id"
            ")"
        )
        cur.execute("SELECT count(*) FROM chirps_base_grid WHERE is_valid")
        total = cur.fetchone()[0]
    conn.commit()
    return total
