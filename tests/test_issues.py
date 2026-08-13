"""Field-QA issue registry tests (PLANNING.md §8.4).

No live database: the same fake-connection style as test_silver_load.py, recording SQL and
parameters so the statement shapes are locked without needing Postgres.
"""

from datetime import date

import pandas as pd
import pytest

from src.db import issues
from src.transform import field_qa


class FakeCursor:
    def __init__(self, log, rows=None):
        self.log = log
        self._rows = rows or []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def executemany(self, sql, seq):
        self.log.append((" ".join(sql.split()), list(seq)))

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows=None):
        self.log = []
        self.commits = 0
        self._rows = rows

    def cursor(self):
        return FakeCursor(self.log, self._rows)

    def commit(self):
        self.commits += 1

    @property
    def statements(self):
        return [sql for sql, _ in self.log]


def _findings(**overrides):
    """One per-file finding, the shape field_qa.scan_* actually emits."""
    row = {
        "variable": "tmin",
        "date": date(1987, 1, 26),
        "detector": field_qa.DETECTOR_CONSTANT,
        "cells": 412,
        "detail": {
            "file": "tmin_1987.parquet", "chunk_id": None, "distinct": 1,
            "min": -5.489386, "max": -5.489386, "stddev": 0.0,
        },
    }
    return pd.DataFrame([{**row, **overrides}])


def _per_file_findings():
    """The real 1987-01-26 shape: one corrupt band, three files, disjoint cells."""
    chunks = [(None, "tmin_1987.parquet", 412),
              ("s20r004c-003", "tmin_1987__s20r004c-003.parquet", 3902),
              ("s20r005c-003", "tmin_1987__s20r005c-003.parquet", 5528)]
    return pd.concat(
        [
            _findings(cells=cells, detail={
                "file": name, "chunk_id": chunk, "distinct": 1,
                "min": -5.489386, "max": -5.489386, "stddev": 0.0,
            })
            for chunk, name, cells in chunks
        ],
        ignore_index=True,
    )


def test_record_findings_upserts_and_serializes_detail():
    conn = FakeConn()
    assert issues.record_findings(conn, _findings()) == 1

    sql, rows = conn.log[0]
    assert sql.startswith("INSERT INTO wth_data_issues")
    assert "ON CONFLICT (variable, date, detector) DO UPDATE SET" in sql
    assert rows[0][:4] == ("tmin", date(1987, 1, 26), field_qa.DETECTOR_CONSTANT, 412)
    assert '"chunk_id": null' in rows[0][4]  # detail arrives as JSON text, cast to jsonb
    assert conn.commits == 1


def test_per_file_findings_are_consolidated_into_one_incident():
    """The registry keys on (variable, date, detector).

    Writing the three per-file rows raw would let them overwrite each other and leave the
    incident claiming the last chunk's 5,528 cells instead of the real 9,842.
    """
    conn = FakeConn()
    assert issues.record_findings(conn, _per_file_findings()) == 1

    _, rows = conn.log[0]
    assert rows[0][3] == 412 + 3902 + 5528
    assert rows[0][4].count('"file"') == 3  # per-file evidence kept, not discarded


def test_rescan_refreshes_evidence_but_never_reopens_a_triaged_issue():
    """A re-scan is evidence about the data, not a decision about it."""
    conn = FakeConn()
    issues.record_findings(conn, _findings())

    update = conn.log[0][0].split("DO UPDATE SET", 1)[1]
    assert "cells = EXCLUDED.cells" in update
    assert "detail = EXCLUDED.detail" in update
    assert "status" not in update
    assert "resolution" not in update


def test_record_findings_empty_is_a_noop():
    conn = FakeConn()
    assert issues.record_findings(conn, _findings().iloc[0:0]) == 0
    assert conn.statements == []


def test_set_status_stamps_resolved_at_for_terminal_states():
    conn = FakeConn()
    issues.set_status(conn, 7, issues.STATUS_IMPUTED, "interpolated from 01-25/01-27")

    sql, params = conn.log[0]
    assert sql.startswith("UPDATE wth_data_issues SET")
    assert "resolved_at = now()" in sql
    assert params == (issues.STATUS_IMPUTED, "interpolated from 01-25/01-27", 7)


def test_refetch_pending_is_not_resolved():
    """The value is usable, but a clean source is still owed — this must stay open work."""
    conn = FakeConn()
    issues.set_status(conn, 7, issues.STATUS_REFETCH_PENDING)

    assert "resolved_at = NULL" in conn.log[0][0]
    assert issues.STATUS_REFETCH_PENDING not in issues.RESOLVED_STATUSES


def test_accepted_source_defect_is_terminal():
    # Some ERA5 defects have no fix and no honest imputation; recording that decision is
    # worth as much as recording a repair.
    assert issues.STATUS_ACCEPTED_SOURCE_DEFECT in issues.RESOLVED_STATUSES


def test_set_status_rejects_an_unknown_state():
    with pytest.raises(ValueError, match="unknown issue status"):
        issues.set_status(FakeConn(), 1, "kinda_fixed")


def test_fetch_open_excludes_resolved_states():
    conn = FakeConn(rows=[])
    issues.fetch_open(conn)

    sql, params = conn.log[0]
    assert "WHERE status NOT IN %s" in sql
    assert set(params[0]) == issues.RESOLVED_STATUSES


def test_registry_statuses_are_documented_in_the_ddl():
    from src.db import silver_load

    ddl = silver_load.SCHEMA_SQL.read_text()
    assert "CREATE TABLE IF NOT EXISTS wth_data_issues" in ddl
    for status in issues.STATUSES:
        assert status in ddl
