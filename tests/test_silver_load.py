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
    def __init__(self, log, rows=None, rowcount=1):
        self.log = log
        self._rows = rows or []
        # What update_column and record_failures report back; psycopg2 sets it after
        # execute(). Overridable so a test can say "no rows matched".
        self.rowcount = rowcount

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
    def __init__(self, rows=None, rowcount=1):
        self.log = []
        self.commits = 0
        self._rows = rows
        self._rowcount = rowcount

    def cursor(self):
        return FakeCursor(self.log, self._rows, self._rowcount)

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
    assert any("CREATE TEMP TABLE _wth_staging" in s for s in conn.statements)
    assert any(s.startswith("COPY _wth_staging") for s in conn.statements)

    insert = next(s for s in conn.statements if s.startswith("INSERT INTO wth_base"))
    assert "ON CONFLICT (parent_id, child_id, date) DO UPDATE SET" in insert
    assert "ingested_at = now()" in insert
    assert "is_preliminary = EXCLUDED.is_preliminary" in insert
    assert conn.commits == 1


def test_upsert_never_overwrites_a_repaired_column():
    """Bronze stays corrupt after a repair, so a re-transform must not put it back.

    Without the bit guard, re-running transform_silver over 1987 would restore the −5.49 °C
    tmin and clear the flag, silently, while wth_imputation_log went on claiming the repair.
    """
    conn = FakeConn()
    silver_load.upsert_wide(conn, _wide())

    insert = next(s for s in conn.statements if s.startswith("INSERT INTO wth_base"))
    assert "INSERT INTO wth_base AS t" in insert
    for column, bit in silver_load.IMPUTED_BITS.items():
        assert (
            f"{column} = CASE WHEN t.imputed & {bit} > 0 "
            f"THEN t.{column} ELSE EXCLUDED.{column} END"
        ) in insert
        assert f"{column} = EXCLUDED.{column}," not in insert


def test_upsert_ors_the_imputed_mask_rather_than_assigning_it():
    # A transform supplies imputed = 0 for every row; assigning it would wipe the flags of
    # a repair that touched a different variable in the same cell-day.
    conn = FakeConn()
    silver_load.upsert_wide(conn, _wide())

    insert = next(s for s in conn.statements if s.startswith("INSERT INTO wth_base"))
    assert "imputed = t.imputed | EXCLUDED.imputed" in insert
    # is_preliminary describes the source, not the repair, so it still assigns outright.
    assert "is_preliminary = EXCLUDED.is_preliminary" in insert


def test_copy_payload_writes_nan_as_null():
    conn = FakeConn()
    silver_load.upsert_wide(conn, _wide(et0=np.nan, rh=np.nan))

    payload = next(body for sql, body in conn.log if sql.startswith("COPY _wth_staging"))
    # Trailing "...,,,False,0" — two empty fields where rh and et0 were, then the
    # is_preliminary / imputed tail.
    assert payload.strip().endswith(",,,False,0")
    assert "nan" not in payload.lower()


def test_upsert_empty_frame_is_a_noop():
    conn = FakeConn()
    assert silver_load.upsert_wide(conn, _wide().iloc[0:0]) == 0
    assert conn.statements == []


def test_record_failures_clears_the_year_first():
    # rowcount=0: nothing on this cell-day was repaired, so the failure is a real one.
    conn = FakeConn(rowcount=0)
    failures = _wide().drop(columns=["is_preliminary"]).assign(reason="precip<0")

    assert silver_load.record_failures(conn, failures, ["0XKE"], 2020) == 1

    delete = conn.statements[0]
    assert delete.startswith("DELETE FROM wth_qa_failures")
    assert conn.log[0][1] == (["0XKE"], date(2020, 1, 1), date(2020, 12, 31))
    assert any(s.startswith("COPY wth_qa_failures") for s in conn.statements)


def test_record_failures_drops_cell_days_already_repaired():
    """Bronze stays corrupt after a repair, so a re-transform recomputes the same failing
    values — but the rows it would quarantine are in wth_base, repaired and protected. A
    quarantine entry pointing at a row that is present contradicts itself."""
    conn = FakeConn()
    failures = _wide().assign(reason="et0<0")
    recorded = silver_load.record_failures(conn, failures, ["0Y4H"], 1987)

    purge = next(s for s in conn.statements if s.startswith("DELETE FROM wth_qa_failures f"))
    assert "USING wth_base b" in purge
    assert "b.imputed > 0" in purge
    # bpchar[] keeps the join partition-prunable on the wth_base side.
    assert "b.parent_id = ANY(%s::bpchar[])" in purge
    # FakeCursor.rowcount is 1, so the one failure is reported as purged, not quarantined.
    assert recorded == 0


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


def test_cell_timezone_table_is_retired():
    # The per-cell offset now lives on the grid (era5_land_base_grid.t_zone); the silver DDL
    # must no longer create the old cell_timezone table.
    ddl = silver_load.SCHEMA_SQL.read_text()
    assert "cell_timezone" not in ddl


# --- imputation provenance (§8.4) ------------------------------------------------------


def test_upsert_defaults_imputed_to_zero_when_absent():
    """A normal transform writes no provenance: nothing it loads is imputed."""
    conn = FakeConn()
    silver_load.upsert_wide(conn, _wide())

    payload = next(body for sql, body in conn.log if sql.startswith("COPY _wth_staging"))
    assert payload.strip().endswith(",0")

    ddl = next(sql for sql, _ in conn.log if "CREATE TEMP TABLE" in sql)
    assert "imputed SMALLINT" in ddl


def test_upsert_carries_an_explicit_imputed_bitmask():
    conn = FakeConn()
    silver_load.upsert_wide(conn, _wide().assign(imputed=silver_load.IMPUTED_BITS["tmin"]))

    payload = next(body for sql, body in conn.log if sql.startswith("COPY _wth_staging"))
    assert payload.strip().endswith(",2")

    # OR-ed, not assigned: a reinstated row carrying one bit must not clear another
    # variable's flag on the same cell-day.
    insert = next(sql for sql, _ in conn.log if sql.startswith("INSERT INTO wth_base"))
    assert "imputed = t.imputed | EXCLUDED.imputed" in insert


def test_imputed_bits_match_the_wth_base_column_order():
    # The bitmask is documented in silver_schema.sql as DDL order; drift would silently
    # relabel every stored provenance value.
    order = ["tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0"]
    assert list(silver_load.IMPUTED_BITS) == order
    assert [silver_load.IMPUTED_BITS[v] for v in order] == [1, 2, 4, 8, 16, 32, 64, 128]


# --- column-scoped repair writes (§8.4) ------------------------------------------------


def _repaired(**overrides):
    row = {"parent_id": "0XKE", "child_id": "EU9K", "date": date(1987, 1, 26), "value": 21.8}
    return pd.DataFrame([{**row, **overrides}])


def test_update_column_touches_only_the_repaired_variable():
    """The whole point of not reusing upsert_wide.

    upsert_wide assigns every non-key column from EXCLUDED, so a frame carrying only the
    repaired variable would null out the other seven.
    """
    conn = FakeConn()
    silver_load.update_column(conn, "tmin", _repaired())

    update = next(s for s in conn.statements if s.startswith("UPDATE wth_base"))
    assert "tmin = s.value" in update
    for other in ("tmax", "precip", "srad", "wind", "tdew", "rh", "et0"):
        assert f"{other} = " not in update
    assert "ON CONFLICT" not in update


def test_update_column_ors_the_imputed_bit_rather_than_assigning_it():
    # Repairing tmin must not erase the record that precip was imputed earlier.
    conn = FakeConn()
    silver_load.update_column(conn, "tmin", _repaired())

    update = next(s for s in conn.statements if s.startswith("UPDATE wth_base"))
    assert "imputed = t.imputed | 2" in update


def test_update_column_matches_on_the_full_primary_key():
    conn = FakeConn()
    silver_load.update_column(conn, "precip", _repaired())

    update = next(s for s in conn.statements if s.startswith("UPDATE wth_base"))
    assert "t.parent_id = s.parent_id" in update
    assert "t.child_id = s.child_id" in update
    assert "t.date = s.date" in update


def test_update_column_rejects_a_non_value_column():
    with pytest.raises(ValueError, match="not a wth_base value column"):
        silver_load.update_column(FakeConn(), "is_preliminary", _repaired())


def test_update_column_empty_is_a_noop():
    conn = FakeConn()
    assert silver_load.update_column(conn, "tmin", _repaired().iloc[0:0]) == 0
    assert conn.statements == []


def test_record_imputations_keeps_the_original_value():
    conn = FakeConn()
    log_rows = pd.DataFrame([{
        "parent_id": "0XKE", "child_id": "EU9K", "date": date(1987, 1, 26),
        "variable": "tmin", "method": "interpolate_temporal(1987-01-25..1987-01-27)",
        "original_value": -5.489386, "new_value": 21.8, "issue_id": 1,
    }])

    assert silver_load.record_imputations(conn, log_rows) == 1

    payload = next(b for s, b in conn.log if s.startswith("COPY _wth_imputation_staging"))
    assert "-5.489386" in payload  # without it the repair would be destructive

    insert = next(s for s in conn.statements if s.startswith("INSERT INTO wth_imputation_log"))
    assert "ON CONFLICT (parent_id, child_id, date, variable) DO UPDATE SET" in insert


def test_repair_writers_can_defer_their_commit():
    """A repaired value without its log row is an unrecorded, irreversible edit.

    The two must land in one transaction, so both writers have to be able to keep quiet
    until the caller commits.
    """
    conn = FakeConn()
    silver_load.update_column(conn, "tmin", _repaired(), commit=False)
    assert conn.commits == 0

    log_rows = pd.DataFrame([{
        "parent_id": "0XKE", "child_id": "EU9K", "date": date(1987, 1, 26),
        "variable": "tmin", "method": "interpolate_temporal", "original_value": -5.49,
        "new_value": 21.8, "issue_id": 1,
    }])
    silver_load.record_imputations(conn, log_rows, commit=False)
    assert conn.commits == 0

    silver_load.upsert_wide(conn, _wide(), commit=False)
    silver_load.ensure_partitions(conn, ["0XKE"], commit=False)
    assert conn.commits == 0


def test_imputation_staging_inherits_defaults():
    """A bare LIKE copies NOT NULL but not DEFAULT now(), so the COPY fails on applied_at."""
    conn = FakeConn()
    log_rows = pd.DataFrame([{
        "parent_id": "0XKE", "child_id": "EU9K", "date": date(1987, 1, 26),
        "variable": "tmin", "method": "m", "original_value": 1.0,
        "new_value": 2.0, "issue_id": 1,
    }])
    silver_load.record_imputations(conn, log_rows)

    ddl = next(s for s in conn.statements if "CREATE TEMP TABLE _wth_imputation" in s)
    assert "INCLUDING DEFAULTS" in ddl


def test_update_column_scopes_to_the_frames_parents_so_partitions_prune():
    """Postgres cannot prune LIST partitions from a join condition.

    Without an explicit parent list the UPDATE walks all 1,659 partitions of a 172M-row
    table; the bpchar cast keeps the partition key from being coerced to text, which would
    also defeat pruning.
    """
    conn = FakeConn()
    frame = pd.concat([_repaired(), _repaired(parent_id="0XKF", child_id="EU9L")])
    silver_load.update_column(conn, "tmin", frame)

    update, params = next((s, p) for s, p in conn.log if s.startswith("UPDATE wth_base"))
    assert "t.parent_id = ANY(%s::bpchar[])" in update
    assert params == (["0XKE", "0XKF"],)
