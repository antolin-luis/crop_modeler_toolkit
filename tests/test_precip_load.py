"""CHIRPS silver loader tests.

No live database: a fake connection records the SQL and COPY payloads, so the statement
shapes that matter — partition creation before insert, the four-column ON CONFLICT that lets
v2 and v3 coexist, the pcp_ prefix — are locked without needing Postgres. Same approach as
tests/test_silver_load.py; live coverage is the end-to-end run.
"""

import numpy as np
import pandas as pd

from src.db import precip_load
from src.transform import precip_alt


class FakeCursor:
    def __init__(self, log, rows=None):
        self.log = log
        self._rows = rows or [(0,)]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def copy_expert(self, sql, fileobj):
        self.log.append((" ".join(sql.split()), fileobj.read()))

    def fetchone(self):
        return self._rows[0]

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
    def sql(self):
        return [s for s, _ in self.log]


def frame(n=3, source=3):
    return pd.DataFrame(
        {
            "fparent_id": ["00JON"] * n,
            "fine_id": [f"60ST{c}" for c in "0123456789"[:n]],
            "date": [pd.Timestamp("2020-01-01").date()] * n,
            "source": [source] * n,
            "precip": np.arange(n, dtype=float),
        }
    )


# --- partitions ------------------------------------------------------------------

def test_partition_name_uses_the_pcp_prefix():
    """Not wth_: fine and ERA5 parent codes are different code spaces sharing a namespace."""
    assert precip_load.partition_name("00JON") == "pcp_00JON"


def test_partition_name_strips_char_padding():
    """psycopg2 returns CHAR(5) space-padded; an unstripped name is invalid SQL."""
    assert precip_load.partition_name("00JON  ") == "pcp_00JON"


def test_ensure_partitions_creates_each_parent_once_sorted():
    conn = FakeConn()
    precip_load.ensure_partitions(conn, ["00JOO", "00JON", "00JON"])

    creates = [s for s in conn.sql if s.startswith("CREATE TABLE")]
    assert len(creates) == 2
    assert "pcp_00JON" in creates[0] and "pcp_00JOO" in creates[1]
    assert all("PARTITION OF wth_precip_alt" in s for s in creates)
    assert conn.commits == 1


# --- upsert ----------------------------------------------------------------------

def test_upsert_conflict_target_includes_source():
    """Without `source` in the target, loading v2 would overwrite v3 for the same cell-day."""
    conn = FakeConn()
    precip_load.upsert_precip(conn, frame())

    insert = next(s for s in conn.sql if s.startswith("INSERT INTO wth_precip_alt"))
    assert "ON CONFLICT (fparent_id, fine_id, date, source) DO UPDATE" in insert
    assert "precip = EXCLUDED.precip" in insert
    assert "ingested_at = now()" in insert


def test_upsert_goes_through_staging_and_copy_not_row_inserts():
    conn = FakeConn()
    precip_load.upsert_precip(conn, frame())

    assert any(s.startswith("CREATE TEMP TABLE _precip_staging") for s in conn.sql)
    assert any(s.startswith("COPY _precip_staging") for s in conn.sql)
    assert sum(s.startswith("INSERT INTO wth_precip_alt") for s in conn.sql) == 1


def test_upsert_staging_is_dropped_on_commit():
    """A leaked temp table breaks the next batch in the same session."""
    conn = FakeConn()
    precip_load.upsert_precip(conn, frame())
    create = next(s for s in conn.sql if "_precip_staging" in s and "CREATE" in s)
    assert "ON COMMIT DROP" in create


def test_upsert_empty_frame_is_a_noop():
    conn = FakeConn()
    assert precip_load.upsert_precip(conn, frame(0)) == 0
    assert conn.log == []


def test_copy_payload_writes_nan_as_null():
    conn = FakeConn()
    f = frame()
    f.loc[1, "precip"] = np.nan
    precip_load.upsert_precip(conn, f)

    payload = next(p for s, p in conn.log if s.startswith("COPY"))
    assert payload.splitlines()[1].endswith(",")  # empty field == NULL


def test_copy_column_order_matches_the_insert():
    conn = FakeConn()
    precip_load.upsert_precip(conn, frame())
    copy = next(s for s in conn.sql if s.startswith("COPY _precip_staging"))
    assert "(fparent_id, fine_id, date, source, precip)" in copy


# --- failures --------------------------------------------------------------------

def test_record_failures_clears_the_window_for_that_source_only():
    """v3's quarantine must not be wiped when v2 is re-transformed."""
    conn = FakeConn()
    precip_load.record_failures(conn, frame(0).assign(reason=[]), ["00JON"], 2020, 3)

    delete = next(s for s in conn.sql if s.startswith("DELETE"))
    assert "source = %s" in delete
    params = next(p for s, p in conn.log if s.startswith("DELETE"))
    assert params[1] == 3


def test_mark_valid_cells_derives_from_loaded_rows():
    conn = FakeConn(rows=[(16867,)])
    assert precip_load.mark_valid_cells(conn) == 16867
    update = next(s for s in conn.sql if s.startswith("UPDATE chirps_base_grid"))
    assert "EXISTS" in update and "wth_precip_alt" in update


# --- QA ---------------------------------------------------------------------------

def test_negative_precip_is_quarantined():
    f = frame(3)
    f.loc[1, "precip"] = -5.0
    good, bad = precip_alt.split_valid(f)
    assert len(good) == 2 and len(bad) == 1
    assert bad.iloc[0]["reason"] == "precip<0"


def test_above_catalog_max_is_quarantined():
    f = frame(2)
    f.loc[0, "precip"] = 2000.0
    good, bad = precip_alt.split_valid(f)
    assert len(good) == 1
    assert bad.iloc[0]["reason"] == "precip>catalog_max"


def test_nan_precip_is_not_a_failure():
    """A masked pixel is absent data, not bad data."""
    f = frame(2)
    f.loc[0, "precip"] = np.nan
    good, bad = precip_alt.split_valid(f)
    assert len(good) == 2 and bad.empty


def test_sub_tolerance_negative_noise_is_snapped_not_quarantined():
    f = frame(2)
    f.loc[0, "precip"] = -0.001
    snapped = precip_alt.snap_accumulation_noise(f)
    assert snapped.loc[0, "precip"] == 0.0
    good, bad = precip_alt.split_valid(snapped)
    assert bad.empty


def test_real_negative_survives_the_snap_and_is_quarantined():
    f = frame(2)
    f.loc[0, "precip"] = -5.0
    good, bad = precip_alt.split_valid(precip_alt.snap_accumulation_noise(f))
    assert len(bad) == 1


def test_calendar_report_counts_missing_days():
    f = frame(1)
    report = precip_alt.calendar_report(f, 2020)
    assert int(report.iloc[0]["expected"]) == 366
    assert int(report.iloc[0]["missing"]) == 365


# --- bronze discovery -------------------------------------------------------------

def test_source_year_paths_globs_rather_than_resolving_one_name(tmp_path):
    """Exact-name resolution is the defect that made silver silently miss chunked bronze."""
    d = tmp_path / "chirps_v3_rnl"
    d.mkdir()
    (d / "chirps_v3_rnl_2020.parquet").touch()
    (d / "chirps_v3_rnl_2020__s20r004c-002.parquet").touch()
    (d / "chirps_v3_rnl_2021.parquet").touch()

    found = precip_alt.source_year_paths(tmp_path, "chirps_v3_rnl", 2020)
    assert len(found) == 2


def test_available_sources_skips_years_with_no_files(tmp_path):
    d = tmp_path / "chirps_v2"
    d.mkdir()
    (d / "chirps_v2_1999.parquet").touch()
    assert precip_alt.available_sources(tmp_path, 1999, ["chirps_v2", "chirps_v3_rnl"]) == [
        "chirps_v2"
    ]
    assert precip_alt.available_sources(tmp_path, 2000, ["chirps_v2"]) == []


def test_load_source_year_on_missing_files_returns_the_silver_columns(tmp_path):
    out = precip_alt.load_source_year(tmp_path, "chirps_v2", 1999)
    assert out.empty
    assert list(out.columns) == ["fparent_id", "fine_id", "date", "source", "precip"]


# --- bronze -> silver seam (real parquet) -----------------------------------------

def _write_bronze(tmp_path, source, year):
    """Write a real bronze parquet through the same encoder the download path uses."""
    from src.grid.encode_fine import encode_fine_grid
    from src.grid.fine_spec import LAT_ORIGIN, RESOLUTION

    lat = LAT_ORIGIN - (np.arange(1404, 1406) + 0.5) * RESOLUTION
    lon = (np.arange(6232, 6234) + 0.5) * RESOLUTION
    lon = np.where(lon > 180.0, lon - 360.0, lon)
    times = pd.to_datetime([f"{year}-01-01", f"{year}-01-02"])
    values = np.arange(2 * 2 * 2, dtype=float).reshape(2, 2, 2)

    bronze = encode_fine_grid(values, lat, lon, times, source=source)
    d = tmp_path / source
    d.mkdir(parents=True, exist_ok=True)
    bronze.to_parquet(d / f"{source}_{year}.parquet", index=False)
    return bronze


def test_bronze_parquet_round_trips_into_the_silver_column_set(tmp_path):
    bronze = _write_bronze(tmp_path, "chirps_v3_rnl", 2020)
    out = precip_alt.load_source_year(tmp_path, "chirps_v3_rnl", 2020)

    assert list(out.columns) == ["fparent_id", "fine_id", "date", "source", "precip"]
    assert len(out) == len(bronze)
    assert (out["source"] == 3).all()
    # `value` became `precip`, unconverted — CHIRPS is already mm/day.
    np.testing.assert_allclose(
        out.sort_values(["fine_id", "date"])["precip"].to_numpy(),
        bronze.sort_values(["fine_id", "date"])["value"].to_numpy(),
    )


def test_fparent_filter_pushes_down(tmp_path):
    _write_bronze(tmp_path, "chirps_v3_rnl", 2020)
    everything = precip_alt.load_source_year(tmp_path, "chirps_v3_rnl", 2020)
    one = precip_alt.load_source_year(
        tmp_path, "chirps_v3_rnl", 2020, [everything["fparent_id"].iloc[0]]
    )
    assert 0 < len(one) <= len(everything)
    assert one["fparent_id"].nunique() == 1


def test_the_two_sources_carry_different_codes_for_the_same_cell_day(tmp_path):
    """The property that makes v2 and v3 coexist rather than overwrite."""
    _write_bronze(tmp_path, "chirps_v3_rnl", 2020)
    _write_bronze(tmp_path, "chirps_v2", 2020)

    v3 = precip_alt.load_source_year(tmp_path, "chirps_v3_rnl", 2020)
    v2 = precip_alt.load_source_year(tmp_path, "chirps_v2", 2020)

    assert set(v3["fine_id"]) == set(v2["fine_id"])
    assert v3["source"].unique().tolist() == [3]
    assert v2["source"].unique().tolist() == [2]


def test_fparent_batches_are_sorted_and_chunked(tmp_path):
    _write_bronze(tmp_path, "chirps_v3_rnl", 2020)
    batches = list(
        precip_alt.iter_fparent_batches(tmp_path, "chirps_v3_rnl", 2020, batch_size=1)
    )
    flat = [p for b in batches for p in b]
    assert flat == sorted(flat)
    assert all(len(b) <= 1 for b in batches)


def test_fparent_batches_on_missing_bronze_yields_nothing(tmp_path):
    assert list(precip_alt.iter_fparent_batches(tmp_path, "chirps_v2", 1999)) == []
