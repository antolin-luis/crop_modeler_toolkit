"""Point read API tests (PLANNING.md §8.1).

A fake connection captures the SQL, so this locks the two things that make the lookup
cheap: the cell is resolved arithmetically (no grid-table join, no PostGIS), and the
WHERE clause leads with ``parent_id`` so Postgres can prune partitions.
"""

from datetime import date

import pandas as pd

from src.db import query
from src.grid.encoding import cell_code, code_to_latlon, parent_code

MONTEVIDEO = (-34.9, -56.2)
B = 4


class FakeCursor:
    def __init__(self, log, rows):
        self.log = log
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.log.append((" ".join(sql.split()), params))

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows=()):
        self.log = []
        self.closed = False
        self._rows = list(rows)

    def cursor(self):
        return FakeCursor(self.log, self._rows)

    def close(self):
        self.closed = True


def test_locate_matches_the_scalar_encoders():
    child, parent = query.locate(*MONTEVIDEO)
    assert child == cell_code(*MONTEVIDEO)
    assert parent == parent_code(*MONTEVIDEO, B)


def test_locate_snaps_arbitrary_coordinates_to_the_cell():
    # Both points sit in the cell centered on (-35.0, -56.25), so they share a code.
    assert query.locate(-34.9, -56.2)[0] == query.locate(-34.95, -56.31)[0]
    assert query.locate(-34.9, -56.2)[0] == cell_code(-35.0, -56.25)


def test_cell_center_round_trips():
    center = query.cell_center(*MONTEVIDEO)
    assert center == code_to_latlon(cell_code(*MONTEVIDEO))
    assert abs(center[0] - MONTEVIDEO[0]) <= 0.125
    assert abs(center[1] - MONTEVIDEO[1]) <= 0.125


def test_query_is_partition_pruned_and_ordered():
    conn = FakeConn()
    query.fetch_series(*MONTEVIDEO, conn=conn)

    sql, params = conn.log[0]
    assert "FROM wth_base w" in sql
    assert "w.parent_id = %s AND w.child_id = %s" in sql  # partition key leads
    assert sql.endswith("ORDER BY w.date")
    assert params == [parent_code(*MONTEVIDEO, B), cell_code(*MONTEVIDEO)]
    assert "ST_" not in sql                  # no PostGIS


def test_query_left_joins_grid_for_offset():
    conn = FakeConn()
    query.fetch_series(*MONTEVIDEO, conn=conn)

    sql, _ = conn.log[0]
    # utc_offset_minutes now comes from the grid's t_zone (single source of truth), via a
    # cheap PK LEFT JOIN — not the retired cell_timezone table.
    assert "LEFT JOIN era5_land_base_grid g ON g.child_id = w.child_id" in sql
    assert "g.t_zone" in sql
    assert "cell_timezone" not in sql


def test_date_bounds_are_optional_and_parameterized():
    conn = FakeConn()
    query.fetch_series(*MONTEVIDEO, date(2020, 1, 1), date(2020, 12, 31), conn=conn)

    sql, params = conn.log[0]
    assert "date >= %s" in sql and "date <= %s" in sql
    assert params[2:] == [date(2020, 1, 1), date(2020, 12, 31)]


def test_series_is_date_indexed_with_silver_columns():
    rows = [
        (date(2020, 1, 1), 30.0, 18.0, 5.0, 22.0, 5.0, 18.0, 55.0, 4.5, False, -180),
        (date(2020, 1, 2), 31.0, 19.0, 0.0, 24.0, 4.0, 17.0, 48.0, 5.1, False, -180),
    ]
    frame = query.fetch_series(*MONTEVIDEO, conn=FakeConn(rows))

    assert frame.index.name == "date"
    assert list(frame.columns) == query.SERIES_COLUMNS[1:]
    assert frame.loc[date(2020, 1, 1), "tmax"] == 30.0
    assert frame.loc[date(2020, 1, 1), "utc_offset_minutes"] == -180
    assert len(frame) == 2


def test_empty_result_is_an_empty_frame_not_an_error():
    frame = query.fetch_series(*MONTEVIDEO, conn=FakeConn())
    assert isinstance(frame, pd.DataFrame)
    assert frame.empty


def test_caller_supplied_connection_is_not_closed():
    conn = FakeConn()
    query.fetch_series(*MONTEVIDEO, conn=conn)
    assert not conn.closed
