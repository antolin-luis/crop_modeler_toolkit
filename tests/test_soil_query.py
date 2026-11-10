"""Soil point-lookup tests.

A fake connection captures the SQL, so this pins the property that makes the lookup cheap:
a coordinate resolves to its ``cell5m`` arithmetically and the exact path is a primary-key
read with no PostGIS at all. ``geom`` is only allowed to appear in the nearest fallback.

``locate`` is checked against real ``(cell5m, lat, lon)`` triples from the loaded table, so
the encoding is pinned to the data rather than to a re-derivation of the same formula.
"""

from __future__ import annotations

import pytest

from src.db import soil_query as sq

# Real rows from soil_profile_points, spread across hemispheres and the antimeridian.
KNOWN = [
    (2455938, 42.625, 1.542),      # Andorra
    (2460257, 42.542, 1.458),
    (368414, 82.875, -78.792),     # Canadian high Arctic
    (6177624, -29.208, -177.958),  # NZ, just east of the antimeridian
    (6472845, -34.875, -56.208),   # Uruguay
    (5194220, -10.208, -48.292),   # Tocantins, Brazil
]


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

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        return self._rows.pop(0) if self._rows else []


class FakeConn:
    def __init__(self, rows=()):
        self.log = []
        self._rows = list(rows)

    def cursor(self):
        return FakeCursor(self.log, self._rows)

    def close(self):
        pass


ROW = (6472845, "UY06472845", "UY", -34.875, -56.208, "FGHR", "0YYF")


def test_locate_matches_the_stored_ids():
    for cell5m, lat, lon in KNOWN:
        assert sq.locate(lat, lon) == cell5m


def test_cell_center_inverts_locate():
    for cell5m, lat, lon in KNOWN:
        c_lat, c_lon = sq.cell_center(cell5m)
        assert c_lat == pytest.approx(lat, abs=0.001)
        assert c_lon == pytest.approx(lon, abs=0.001)


def test_locate_is_containing_cell_not_nearest_centre():
    """A coordinate belongs to the cell whose box it falls in. Rounding to the nearest
    centre instead would move points near a cell edge into the neighbour."""
    lat, lon = sq.cell_center(2455938)
    step = 1.0 / sq.CELLS_PER_DEGREE

    # Anywhere strictly inside the box is the same cell...
    assert sq.locate(lat + step * 0.49, lon + step * 0.49) == 2455938
    assert sq.locate(lat - step * 0.49, lon - step * 0.49) == 2455938
    # ...and one step over is exactly one column / one row away.
    assert sq.locate(lat, lon + step) == 2455938 + 1
    assert sq.locate(lat - step, lon) == 2455938 + sq.NCOLS


def test_poles_and_antimeridian_stay_on_the_grid():
    assert 0 <= sq.locate(90.0, 0.0) < sq.NCOLS * sq.NROWS
    assert 0 <= sq.locate(-90.0, 0.0) < sq.NCOLS * sq.NROWS
    # lon 180 and -180 are the same meridian; both must land in column 0's neighbourhood
    # rather than one column past the end of the row.
    assert sq.locate(0.0, 180.0) == sq.locate(0.0, -180.0)
    assert sq.locate(0.0, 179.999) % sq.NCOLS == sq.NCOLS - 1


def test_locate_rejects_an_impossible_latitude():
    with pytest.raises(ValueError, match="outside -90..90"):
        sq.locate(91.0, 0.0)


def test_exact_lookup_is_a_pk_read_with_no_postgis():
    conn = FakeConn(rows=[ROW])

    profile = sq.profile_at(-34.9, -56.2, conn=conn)

    assert profile.soil_id == "UY06472845"
    assert profile.dist_km == 0.0
    assert len(conn.log) == 1  # the fallback never ran
    sql, params = conn.log[0]
    assert sql == (
        "SELECT cell5m, soil_id, iso2, lat, lon, child_id, parent_id "
        "FROM soil_profile_points WHERE cell5m = %s"
    )
    assert params == (6472845,)
    assert "ST_" not in sql


def test_a_miss_falls_back_to_the_nearest_profile():
    conn = FakeConn(rows=[None, (*ROW, 6.9077)])

    profile = sq.profile_at(-34.95, -56.05, conn=conn)

    assert profile.soil_id == "UY06472845"
    assert profile.dist_km == pytest.approx(6.9077)
    fallback, params = conn.log[1]
    assert "ORDER BY s.geom <-> p.pt LIMIT 1" in fallback
    assert "ST_DWithin" in fallback
    # (lon, lat, dx, dy, metres) — dx is widened by 1/cos(lat), so it exceeds dy away
    # from the equator; a square degree box would under-reach in longitude.
    lon, lat, dx, dy, metres = params
    assert (lon, lat) == (-56.05, -34.95)
    assert dx > dy
    assert metres == 25_000.0


def test_nearest_false_makes_a_miss_a_miss():
    conn = FakeConn(rows=[None])

    assert sq.profile_at(0.0, -30.0, nearest=False, conn=conn) is None
    assert len(conn.log) == 1


def test_no_profile_within_the_radius_returns_none():
    conn = FakeConn(rows=[None, None])

    assert sq.profile_at(0.0, -30.0, max_km=5.0, conn=conn) is None


def test_char_columns_come_back_stripped():
    """child_id/parent_id are CHAR(4) and psycopg2 space-pads them."""
    padded = (6472845, "UY06472845", "UY  ", -34.875, -56.208, "FGHR", "0YYF")
    conn = FakeConn(rows=[padded])

    profile = sq.profile_at(-34.9, -56.2, conn=conn)

    assert profile.iso2 == "UY"
    assert profile.child_id == "FGHR"
    assert profile.parent_id == "0YYF"


def test_batch_lookup_is_one_query_and_preserves_input_order():
    conn = FakeConn(rows=[[ROW]])

    frame = sq.profiles_at([(0.0, -30.0), (-34.9, -56.2)], conn=conn)

    assert len(conn.log) == 1
    sql, params = conn.log[0]
    assert "cell5m = ANY(%s)" in sql
    assert sorted(params[0]) == sorted([sq.locate(0.0, -30.0), 6472845])
    # Row order follows the request, and the miss keeps its row with NULLs.
    assert frame["lat"].tolist() == [0.0, -34.9]
    assert frame.loc[0, "soil_id"] != frame.loc[0, "soil_id"]  # NaN
    assert frame.loc[1, "soil_id"] == "UY06472845"


def test_batch_lookup_of_nothing():
    assert sq.profiles_at([]).empty
