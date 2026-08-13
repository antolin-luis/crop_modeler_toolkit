"""Field-level QA issue registry (PLANNING.md §8.4).

``wth_qa_failures`` is a *cell-day* quarantine: rows that failed a physical check and were
kept out of ``wth_base``. This module owns a different table for a different thing —
``wth_data_issues``, a registry of **whole-field** defects that outlives the incident.

Why a registry and not just a log line: a corrupt ERA5 band has a lifecycle. It is found,
triaged, maybe repaired locally, and maybe re-fetched from a clean source months later.
``accepted_source_defect`` matters as much as a repair — some defects have no fix and no
honest imputation, and recording that decision is the only thing that stops the next
operator from rediscovering it.

Detection writes here; it never repairs. Repair is a separate, deliberately triggered DAG
(§8.4) — silent imputation is how an upstream defect stops being visible.
"""

from __future__ import annotations

import json

import pandas as pd

from src.transform import field_qa

TABLE = "wth_data_issues"

# Terminal states need no further action; the rest are open work.
STATUS_DETECTED = "detected"
STATUS_REFETCH_PENDING = "refetch_pending"
STATUS_REFETCHED = "refetched"
STATUS_IMPUTED = "imputed"
STATUS_ACCEPTED_SOURCE_DEFECT = "accepted_source_defect"
STATUS_FALSE_POSITIVE = "false_positive"

STATUSES = frozenset(
    {
        STATUS_DETECTED,
        STATUS_REFETCH_PENDING,
        STATUS_REFETCHED,
        STATUS_IMPUTED,
        STATUS_ACCEPTED_SOURCE_DEFECT,
        STATUS_FALSE_POSITIVE,
    }
)

# States that need no further operator action. `refetch_pending` is *not* one of them: the
# value is usable but a clean source is still owed.
RESOLVED_STATUSES = frozenset(
    {STATUS_REFETCHED, STATUS_IMPUTED, STATUS_ACCEPTED_SOURCE_DEFECT, STATUS_FALSE_POSITIVE}
)


def record_findings(conn, findings: pd.DataFrame) -> int:
    """Upsert scan findings into the registry; returns the row count written.

    Findings arrive one per *file* (detection is per file — a regional defect vanishes if
    the chunks are merged first) but an incident is one row here, so they are consolidated
    on the way in. Without that, the three files carrying 1987-01-26 would overwrite each
    other on the ``(variable, date, detector)`` key and the row would claim 5,528 cells
    instead of 9,842.

    Re-running a scan must not resurrect a triaged issue, so an existing row keeps its
    ``status`` and ``resolution`` — only the observed facts (``cells``, ``detail``) are
    refreshed. A re-scan is evidence about the data, not a decision about it.
    """
    if findings.empty:
        return 0

    findings = field_qa.consolidate(findings)

    rows = [
        (
            row.variable,
            row.date,
            row.detector,
            int(row.cells),
            json.dumps(row.detail),
        )
        for row in findings.itertuples(index=False)
    ]

    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO {TABLE} (variable, date, detector, cells, detail) "
            "VALUES (%s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT (variable, date, detector) DO UPDATE SET "
            "cells = EXCLUDED.cells, detail = EXCLUDED.detail",
            rows,
        )
    conn.commit()
    return len(rows)


def fetch_open(conn) -> pd.DataFrame:
    """Issues still needing action, newest first."""
    return _select(
        conn,
        "WHERE status NOT IN %s ORDER BY detected_at DESC",
        (tuple(sorted(RESOLVED_STATUSES)),),
    )


def fetch_issue(conn, issue_id: int) -> pd.DataFrame:
    return _select(conn, "WHERE issue_id = %s", (issue_id,))


def fetch_for(conn, variable: str, date) -> pd.DataFrame:
    return _select(conn, "WHERE variable = %s AND date = %s", (variable, date))


def set_status(conn, issue_id: int, status: str, resolution: str | None = None) -> None:
    """Move one issue to ``status``, stamping ``resolved_at`` for terminal states."""
    if status not in STATUSES:
        raise ValueError(f"unknown issue status {status!r}; known: {', '.join(sorted(STATUSES))}")

    resolved_at = "now()" if status in RESOLVED_STATUSES else "NULL"
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {TABLE} SET status = %s, resolution = %s, resolved_at = {resolved_at} "
            "WHERE issue_id = %s",
            (status, resolution, issue_id),
        )
    conn.commit()


_COLUMNS = [
    "issue_id", "variable", "date", "detector", "cells", "detail",
    "status", "resolution", "detected_at", "resolved_at",
]


def _select(conn, where: str, params: tuple) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(_COLUMNS)} FROM {TABLE} {where}", params)
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=_COLUMNS)
