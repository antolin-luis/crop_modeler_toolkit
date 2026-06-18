"""Bulk-load helpers (PLANNING.md §8: COPY, not row-by-row INSERT).

A thin ``psycopg2`` ``copy_expert`` wrapper reused by the grid seed (Step 2) and the
silver loader (Step 4). Connection details come from ``src/config.py``.
"""

from __future__ import annotations

from typing import IO

import psycopg2

from src.config import load_config


def connect() -> psycopg2.extensions.connection:
    """Open a psycopg2 connection using the configured Postgres DSN."""
    return psycopg2.connect(load_config().postgres.dsn)


def copy_csv(
    conn: psycopg2.extensions.connection,
    table: str,
    columns: list[str],
    fileobj: IO[str],
) -> None:
    """``COPY table (columns) FROM STDIN`` reading CSV from ``fileobj``.

    Caller controls the transaction (commit/rollback) so multiple COPYs can share one.
    """
    cols = ", ".join(columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)"
    with conn.cursor() as cur:
        cur.copy_expert(sql, fileobj)


def execute_script(conn: psycopg2.extensions.connection, sql: str) -> None:
    """Run a multi-statement SQL script (e.g. schema.sql) on ``conn``."""
    with conn.cursor() as cur:
        cur.execute(sql)
