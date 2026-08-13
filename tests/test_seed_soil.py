"""SoilGrids-for-DSSAT intake tests.

Offline, only ``build_rows``, ``resolve_source`` and the SQL that ``build_map`` emits are
exercised. ``load_points`` needs Postgres and is covered by the end-to-end run, matching
how ``tests/test_seed_fine_grid.py`` and ``tests/test_seed_grid.py`` treat their loaders.

The load-side risk this file exists to pin: the soil points are 5 arc-min centres, which
are almost never 0.25° centres, so they must be snapped before encoding. Skip the snap and
every ``child_id`` is still a valid-looking code — just the wrong cell.
"""

from __future__ import annotations

import re

import pandas as pd
import pytest

from src.db import seed_soil
from src.grid.encoding import cell_code, code_to_latlon, parent_code

B = 4

# Real values, read out of Point5m_SoilGrids-for-DSSAT-10km_v1.dbf.
SAMPLE = pd.DataFrame(
    {
        "CELL5M": [2455938, 2455939, 2460257],
        "SoilProfil": ["AD02455938", "AD02455939", "AD02460257"],
        "X": [1.542, 1.625, 1.458],
        "Y": [42.625, 42.625, 42.542],
        "ISO2": ["AD", "AD", "AD"],
    }
)


class FakeCursor:
    def __init__(self, log, rows):
        self.log = log
        self._rows = list(rows)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def fetchone(self):
        return self._rows.pop(0) if self._rows else (0,)

    def fetchall(self):
        return []


class FakeConn:
    def __init__(self, rows=()):
        self.log = []
        self.committed = 0
        self._rows = rows

    def cursor(self):
        return FakeCursor(self.log, self._rows)

    def commit(self):
        self.committed += 1

    def close(self):
        pass


def test_build_rows_columns_and_passthrough():
    rows = seed_soil.build_rows(SAMPLE, b=B)

    assert list(rows.columns) == seed_soil._COLUMNS
    assert rows["cell5m"].tolist() == [2455938, 2455939, 2460257]
    assert rows["soil_id"].tolist() == ["AD02455938", "AD02455939", "AD02460257"]
    assert rows["iso2"].tolist() == ["AD", "AD", "AD"]
    # X/Y are carried through untouched — the point keeps its own coordinates; only the
    # cell assignment is snapped.
    assert rows["lon"].tolist() == pytest.approx([1.542, 1.625, 1.458])
    assert rows["lat"].tolist() == pytest.approx([42.625, 42.625, 42.542])


def test_neighbouring_points_land_in_different_cells():
    """The companion to the tie test: 0.25° apart must not collapse to one cell."""
    frame = pd.DataFrame(
        {
            "CELL5M": [1, 2],
            "SoilProfil": ["A1", "A2"],
            "X": [1.50, 1.80],
            "Y": [42.50, 42.50],
            "ISO2": ["AD", "AD"],
        }
    )

    rows = seed_soil.build_rows(frame, b=B)

    assert rows["child_id"].tolist() == [cell_code(42.50, 1.50), cell_code(42.50, 1.75)]


def test_points_are_snapped_to_the_containing_era5_centre():
    rows = seed_soil.build_rows(SAMPLE, b=B)

    # All three 5 arc-min points fall in the same 0.25° cell, centred (42.50, 1.50).
    # Note row 0: lat 42.625 sits exactly halfway between the 42.50 and 42.75 centres, and
    # rounding is banker's, so it goes to the even index -> 42.50. Same for lon 1.625 on
    # row 1. That tie-to-even is the encoders' documented contract, not an accident here.
    assert rows["child_id"].tolist() == [cell_code(42.50, 1.50)] * 3

    for _, row in rows.iterrows():
        lat_c, lon_c = code_to_latlon(row["child_id"])
        assert abs(lat_c - row["lat"]) <= 0.125 + 1e-9
        assert abs(lon_c - row["lon"]) <= 0.125 + 1e-9


def test_parent_id_agrees_with_the_scalar_encoder():
    rows = seed_soil.build_rows(SAMPLE, b=B)

    for _, row in rows.iterrows():
        lat_c, lon_c = code_to_latlon(row["child_id"])
        assert row["parent_id"] == parent_code(lat_c, lon_c, B)


def test_longitude_wraps_at_the_antimeridian():
    """A point just west of 180° rounds up to index NLON, which is index 0 — the cell at
    the prime meridian. Clipping instead of wrapping would park it at 179.75°."""
    frame = pd.DataFrame(
        {
            "CELL5M": [1, 2],
            "SoilProfil": ["FJ01", "GB02"],
            "X": [179.99, -0.01],
            "Y": [-17.0, 51.5],
            "ISO2": ["FJ", "GB"],
        }
    )

    rows = seed_soil.build_rows(frame, b=B)

    assert rows.loc[0, "child_id"] == cell_code(-17.0, -180.0)
    assert rows.loc[1, "child_id"] == cell_code(51.5, 0.0)


def test_poles_do_not_run_off_the_grid():
    frame = pd.DataFrame(
        {
            "CELL5M": [1, 2],
            "SoilProfil": ["A1", "A2"],
            "X": [0.0, 0.0],
            "Y": [89.99, -89.99],
            "ISO2": ["XX", "XX"],
        }
    )

    rows = seed_soil.build_rows(frame, b=B)

    assert rows.loc[0, "child_id"] == cell_code(90.0, 0.0)
    assert rows.loc[1, "child_id"] == cell_code(-90.0, 0.0)


def test_build_rows_rejects_a_frame_missing_a_source_field():
    with pytest.raises(KeyError, match="ISO2"):
        seed_soil.build_rows(SAMPLE.drop(columns=["ISO2"]), b=B)


def test_build_map_joins_on_child_id_and_uses_no_postgis():
    """The cell assignment is already in the table, so the bridge is an equality join.
    A spatial predicate here would re-derive it, slower, and hide the code-space contract.
    """
    conn = FakeConn(rows=[(1_038_240,), (1_984_797,)])

    total = seed_soil.build_map(conn)

    assert total == 1_984_797
    insert = next(sql for sql, _ in conn.log if sql.startswith("INSERT INTO soil_era5_map"))
    assert "JOIN era5_land_base_grid g ON g.child_id = p.child_id" in insert
    assert "ST_" not in insert
    assert "row_number() OVER (PARTITION BY child_id ORDER BY dist_deg, cell5m) = 1" in insert
    assert any(sql.startswith("TRUNCATE soil_era5_map") for sql, _ in conn.log)
    assert conn.committed == 1


def test_build_map_refuses_to_run_against_an_empty_grid():
    conn = FakeConn(rows=[(0,)])

    with pytest.raises(RuntimeError, match="is empty"):
        seed_soil.build_map(conn)


def test_install_helpers_runs_the_shipped_script():
    """The DAG installs the lookup functions so a completed run leaves a database that
    answers soil_id_at() with nothing further to import. Regression: a run that loaded the
    tables but not the functions looked successful and then failed in DBeaver."""
    conn = FakeConn(rows=[(5,)])

    assert seed_soil.install_helpers(conn) == 5

    executed = conn.log[0][0]
    assert "CREATE OR REPLACE FUNCTION soil_id_at" in executed
    assert "CREATE OR REPLACE FUNCTION soil_profile_at" in executed
    assert conn.committed == 1


def test_helpers_script_ships_beside_the_loader():
    """It lives in src/db/, not sql/, because only ./src is mounted into the Airflow
    containers — a file in sql/ is invisible to the DAG."""
    assert seed_soil.HELPERS_SQL.exists()
    assert seed_soil.HELPERS_SQL.parent == seed_soil.SCHEMA_SQL.parent


def test_helper_input_parameters_are_prefixed():
    """In a SQL-language function a bare `lat` binds to soil_profile_points.lat, not to the
    parameter, which silently made `WHERE s.cell5m = soil_cell5m(lat, lon)` true for every
    row — the function then returned whichever row it scanned first, for every input.

    Only INPUT parameters need the prefix; RETURNS TABLE output columns are deliberately
    named lat/lon and must stay that way, since callers select them.
    """
    script = seed_soil.HELPERS_SQL.read_text()
    signatures = re.findall(
        r"CREATE OR REPLACE FUNCTION\s+(\w+)\s*\((.*?)\)\s*RETURNS", script, re.DOTALL
    )

    assert {name for name, _ in signatures} == {
        "soil_cell5m", "soil_cell5m_center", "soil_profile_at",
        "soil_profile_near", "soil_id_at",
    }
    for name, params in signatures:
        for param in params.split(","):
            first = param.strip().split()[0]
            assert first.startswith("p_"), f"{name}: input parameter {first!r} needs a p_ prefix"


def test_validate_raises_when_a_point_has_no_grid_cell():
    # counts, id-length rows, orphan count, map counts
    conn = FakeConn(rows=[(1_984_797, 225), (7,), (0, 0)])

    with pytest.raises(RuntimeError, match="disagree"):
        seed_soil.validate(conn)


def test_resolve_source_anchors_a_bare_name_to_the_static_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    static = tmp_path / "bronze" / "static"
    static.mkdir(parents=True)
    (static / seed_soil.DEFAULT_SOURCE).write_bytes(b"")

    assert seed_soil.resolve_source() == static / seed_soil.DEFAULT_SOURCE
    assert seed_soil.resolve_source(seed_soil.DEFAULT_SOURCE) == (
        static / seed_soil.DEFAULT_SOURCE
    )
    # An absolute path under DATA_DIR means the same thing.
    assert seed_soil.resolve_source(static / seed_soil.DEFAULT_SOURCE) == (
        static / seed_soil.DEFAULT_SOURCE
    )


def test_resolve_source_refuses_a_path_outside_data_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))

    with pytest.raises(ValueError, match="outside DATA_DIR"):
        seed_soil.resolve_source("/etc/passwd")
    # ".." is collapsed lexically, so climbing out of bronze/static and past DATA_DIR is
    # caught rather than resolved against the filesystem.
    with pytest.raises(ValueError, match="outside DATA_DIR"):
        seed_soil.resolve_source("../../../escape.zip")


def test_resolve_source_missing_file_names_the_fix(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    with pytest.raises(FileNotFoundError, match="bronze/static"):
        seed_soil.resolve_source()
