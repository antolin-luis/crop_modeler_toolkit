"""Silver loader tests (PLANNING.md §8.2, §11.3).

No live database: a fake connection records the SQL and COPY payloads, so the statement
shapes that matter — partition creation before insert, the ON CONFLICT upsert, NaN → NULL
in the COPY stream — are locked without needing Postgres. Live coverage is the manual
verification in docs/step4_silver_transform.md.
"""

from datetime import date

import numpy as np
import pandas as pd
import pytest

from src.db import silver_load


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

    def copy_expert(self, sql, fileobj):
        self.log.append((" ".join(sql.split()), fileobj.read()))

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


def _wide(**overrides):
    row = {
        "parent_id": "0XKE", "child_id": "EU9K", "date": date(2020, 1, 1),
        "tmax": 30.0, "tmin": 18.0, "precip": 5.0, "srad": 22.0,
        "wind": 5.0, "tdew": 18.0, "rh": 55.0, "et0": 4.5,
        "is_preliminary": False,
    }
    return pd.DataFrame([{**row, **overrides}])


def test_preliminary_cutoff_walks_back_three_months():
    assert silver_load.preliminary_cutoff(date(2026, 7, 22)) == date(2026, 4, 22)
    assert silver_load.preliminary_cutoff(date(2026, 2, 10)) == date(2025, 11, 10)


def test_preliminary_cutoff_clamps_to_month_length():
    # 31 May minus 3 months lands in February.
    assert silver_load.preliminary_cutoff(date(2026, 5, 31)) == date(2026, 2, 28)
    assert silver_load.preliminary_cutoff(date(2024, 5, 31)) == date(2024, 2, 29)


def test_assign_preliminary_splits_on_the_cutoff():
    frame = pd.DataFrame({"date": [date(2026, 3, 1), date(2026, 5, 1)]})
    flagged = silver_load.assign_preliminary(frame, date(2026, 4, 22))
    assert list(flagged["is_preliminary"]) == [False, True]


def test_ensure_partitions_creates_one_per_parent():
    conn = FakeConn()
    silver_load.ensure_partitions(conn, ["0XKF", "0XKE", "0XKE"])

    creates = [s for s in conn.statements if s.startswith("CREATE TABLE IF NOT EXISTS wth_")]
    assert len(creates) == 2  # deduplicated
    assert "PARTITION OF wth_base FOR VALUES IN (%s)" in creates[0]
    assert [p for _, p in conn.log] == [("0XKE",), ("0XKF",)]  # sorted, parameterized


def test_partition_name_is_a_valid_identifier():
    # Parent codes can start with a digit; the prefix keeps the identifier legal.
    assert silver_load.partition_name("0XKE") == "wth_0XKE"


def test_upsert_uses_copy_then_on_conflict():
    conn = FakeConn()
    written = silver_load.upsert_wide(conn, _wide())

    assert written == 1
    assert any(s.startswith("CREATE TEMP TABLE _wth_staging") for s in conn.statements)
    assert any(s.startswith("COPY _wth_staging") for s in conn.statements)

    insert = next(s for s in conn.statements if s.startswith("INSERT INTO wth_base"))
    assert "ON CONFLICT (parent_id, child_id, date) DO UPDATE SET" in insert
    assert "ingested_at = now()" in insert
    assert "is_preliminary = EXCLUDED.is_preliminary" in insert
    assert conn.commits == 1


def test_copy_payload_writes_nan_as_null():
    conn = FakeConn()
    silver_load.upsert_wide(conn, _wide(et0=np.nan, rh=np.nan))

    payload = next(body for sql, body in conn.log if sql.startswith("COPY _wth_staging"))
    # Trailing "...,,,False" — two empty fields where rh and et0 were.
    assert payload.strip().endswith(",,,False")
    assert "nan" not in payload.lower()


def test_upsert_empty_frame_is_a_noop():
    conn = FakeConn()
    assert silver_load.upsert_wide(conn, _wide().iloc[0:0]) == 0
    assert conn.statements == []


def test_record_failures_clears_the_year_first():
    conn = FakeConn()
    failures = _wide().drop(columns=["is_preliminary"]).assign(reason="precip<0")

    assert silver_load.record_failures(conn, failures, ["0XKE"], 2020) == 1

    delete = conn.statements[0]
    assert delete.startswith("DELETE FROM wth_qa_failures")
    assert conn.log[0][1] == (["0XKE"], date(2020, 1, 1), date(2020, 12, 31))
    assert any(s.startswith("COPY wth_qa_failures") for s in conn.statements)


def test_record_failures_still_clears_when_nothing_failed():
    conn = FakeConn()
    empty = _wide().drop(columns=["is_preliminary"]).assign(reason="x").iloc[0:0]

    assert silver_load.record_failures(conn, empty, ["0XKE"], 2020) == 0
    assert conn.statements[0].startswith("DELETE FROM wth_qa_failures")
    assert not any(s.startswith("COPY") for s in conn.statements)


def test_fetch_cell_meta_strips_char_padding():
    conn = FakeConn(rows=[("EU9K", -34.9, 50.0), ("EU9L", -34.9, 55.0)])
    meta = silver_load.fetch_cell_meta(conn, ["0XKE"])

    assert list(meta.columns) == ["child_id", "lat", "elevation"]
    assert list(meta["child_id"]) == ["EU9K", "EU9L"]


@pytest.mark.parametrize("column", ["parent_id", "child_id", "date", "is_preliminary"])
def test_loader_columns_match_the_ddl(column):
    ddl = silver_load.SCHEMA_SQL.read_text()
    assert column in silver_load.COLUMNS
    assert column in ddl


@pytest.mark.parametrize("tz,minutes", [
    ("UTC", 0),
    ("UTC-03:00", -180),
    ("UTC-06:00", -360),
    ("UTC+05:30", 330),   # fractional zone — why minutes, not hours
    ("UTC+00:00", 0),
])
def test_parse_offset_minutes(tz, minutes):
    assert silver_load.parse_offset_minutes(tz) == minutes


def test_parse_offset_minutes_rejects_garbage():
    with pytest.raises(ValueError, match="bad timezone"):
        silver_load.parse_offset_minutes("America/Montevideo")


def test_upsert_cell_timezone_dedupes_and_conflicts():
    conn = FakeConn()
    n = silver_load.upsert_cell_timezone(conn, ["EU9K", "EU9L", "EU9K"], -180)

    assert n == 2  # deduplicated
    assert any(s.startswith("CREATE TEMP TABLE _tz_staging") for s in conn.statements)
    assert any(s.startswith("COPY _tz_staging") for s in conn.statements)

    insert = next(s for s in conn.statements if s.startswith("INSERT INTO cell_timezone"))
    assert "ON CONFLICT (child_id) DO UPDATE SET" in insert
    assert "utc_offset_minutes = EXCLUDED.utc_offset_minutes" in insert
    assert "updated_at = now()" in insert

    payload = next(b for sql, b in conn.log if sql.startswith("COPY _tz_staging"))
    assert payload == "EU9K,-180\nEU9L,-180\n"  # sorted, one row per unique cell


def test_upsert_cell_timezone_empty_is_noop():
    conn = FakeConn()
    assert silver_load.upsert_cell_timezone(conn, [], -180) == 0
    assert conn.statements == []


def test_cell_timezone_table_in_ddl():
    ddl = silver_load.SCHEMA_SQL.read_text()
    assert "CREATE TABLE IF NOT EXISTS cell_timezone" in ddl
    assert "utc_offset_minutes SMALLINT" in ddl
