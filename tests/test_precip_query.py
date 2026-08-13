"""Fine-grid read API — src/db/precip_query.py, src/db/chirps_map.py.

No live database: a fake connection records the SQL, so the properties that matter — that
``fparent_id`` is always in the WHERE (or the query scans every partition), and that it is
derived locally rather than queried — are locked offline.
"""


from src.db import chirps_map, precip_query
from src.grid.fine_encoding import fine_parent_of

PALMAS = (-10.24, -48.36)
PALMAS_FINE_ID = "60ST4"
PALMAS_FPARENT = "00JON"


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

    def fetchone(self):
        return self._rows[0] if self._rows else (0,)


class FakeConn:
    """``rows`` is one row-set reused for every query, or a list of them consumed in order."""

    def __init__(self, rows=None, row_sets=None):
        self.log = []
        self.commits = 0
        self._rows = rows or []
        self._row_sets = list(row_sets) if row_sets else None

    def cursor(self):
        if self._row_sets is not None:
            rows = self._row_sets.pop(0) if self._row_sets else []
            return FakeCursor(self.log, rows)
        return FakeCursor(self.log, self._rows)

    def commit(self):
        self.commits += 1

    @property
    def sql(self):
        return [s for s, _ in self.log]


def test_locate_fine_needs_no_database():
    assert precip_query.locate_fine(*PALMAS) == (PALMAS_FINE_ID, PALMAS_FPARENT)


def test_point_query_prunes_on_fparent():
    """Without fparent_id in the WHERE, Postgres scans every partition."""
    conn = FakeConn()
    precip_query.fetch_series(*PALMAS, conn=conn)

    sql, params = conn.log[0]
    assert "p.fparent_id = %s" in sql
    assert "p.fine_id = %s" in sql
    assert params[:2] == [PALMAS_FPARENT, PALMAS_FINE_ID]


def test_point_query_is_one_round_trip():
    conn = FakeConn()
    precip_query.fetch_series(*PALMAS, conn=conn)
    assert len(conn.log) == 1


def test_date_bounds_are_parameterised():
    conn = FakeConn()
    precip_query.fetch_series(*PALMAS, "2020-01-01", "2020-12-31", conn=conn)
    sql, params = conn.log[0]
    assert "p.date >= %s" in sql and "p.date <= %s" in sql
    assert params[2:] == ["2020-01-01", "2020-12-31"]


def test_sources_accept_names_or_codes():
    for given, expected in [(["chirps_v3_rnl"], [3]), ([2], [2]), (["chirps_v2", 3], [2, 3])]:
        conn = FakeConn()
        precip_query.fetch_series(*PALMAS, sources=given, conn=conn)
        _, params = conn.log[0]
        assert params[-1] == expected


def test_both_sources_come_back_as_separate_rows():
    """Loading two CHIRPS versions is the point; the query must not collapse them."""
    conn = FakeConn()
    precip_query.fetch_series(*PALMAS, conn=conn)
    sql = conn.sql[0]
    assert "p.source" in sql
    assert "ORDER BY p.date, p.source" in sql
    assert "GROUP BY" not in sql


def test_multi_cell_query_derives_parents_locally():
    """The whole reason fine_parent_of exists: no second round-trip for the parent set."""
    fine_ids = ["60ST4", "60ST5", "60SU4"]
    conn = FakeConn()
    precip_query.fetch_cells_series(fine_ids, conn=conn)

    assert len(conn.log) == 1
    sql, params = conn.log[0]
    assert "p.fparent_id = ANY(%s)" in sql
    assert params[0] == sorted({fine_parent_of(f) for f in fine_ids})
    assert params[1] == fine_ids


def test_multi_cell_query_strips_char_padding():
    """psycopg2 returns CHAR(5) space-padded; unstripped codes match nothing."""
    conn = FakeConn()
    precip_query.fetch_cells_series(["60ST4  "], conn=conn)
    _, params = conn.log[0]
    assert params[1] == ["60ST4"]


def test_empty_cell_list_does_not_hit_the_database():
    conn = FakeConn()
    out = precip_query.fetch_cells_series([], conn=conn)
    assert conn.log == []
    assert out.empty


def test_region_lookup_uses_gist_on_geom():
    """geom + GiST is for polygon work only; the point path never touches it."""
    conn = FakeConn(rows=[])
    precip_query.cells_in_region("POLYGON((0 0,1 0,1 1,0 1,0 0))", conn=conn)
    sql = conn.sql[0]
    assert "ST_Intersects(geom" in sql
    assert "chirps_base_grid" in sql


def test_region_series_is_two_round_trips():
    """One GiST lookup, then one partition-pruned fact read — never one query per cell."""
    conn = FakeConn(row_sets=[[("60ST4",), ("60ST5",)], []])
    precip_query.fetch_region_series("POLYGON((0 0,1 0,1 1,0 1,0 0))", conn=conn)

    assert len(conn.log) == 2
    assert "ST_Intersects" in conn.sql[0]
    assert "wth_precip_alt" in conn.sql[1]
    # The second query prunes on parents derived from the first query's result.
    assert conn.log[1][1][0] == sorted({fine_parent_of(f) for f in ("60ST4", "60ST5")})


# --- cross-grid map ---------------------------------------------------------------

def test_map_build_weights_by_intersection_area():
    conn = FakeConn(rows=[(67000,)])
    chirps_map.build_map(conn)

    insert = next(s for s in conn.sql if s.startswith("INSERT INTO chirps_era5_map"))
    assert "ST_Intersection" in insert
    assert "ST_Area(e.geom)" in insert
    # Only overlapping pairs, not the cross product.
    assert "f.geom && e.geom" in insert


def test_map_build_is_idempotent():
    conn = FakeConn(rows=[(0,)])
    chirps_map.build_map(conn)
    insert = next(s for s in conn.sql if s.startswith("INSERT INTO chirps_era5_map"))
    assert "ON CONFLICT (fine_id, child_id) DO UPDATE" in insert


def test_map_build_does_not_truncate_unless_asked():
    conn = FakeConn(rows=[(0,)])
    chirps_map.build_map(conn)
    assert not any(s.startswith("TRUNCATE") for s in conn.sql)

    conn = FakeConn(rows=[(0,)])
    chirps_map.build_map(conn, replace=True)
    assert any(s.startswith("TRUNCATE") for s in conn.sql)


def test_compare_view_exposes_the_day_definition_offset():
    """The offset between the two date columns must be visible, not silently joined away."""
    conn = FakeConn()
    chirps_map.create_view(conn)

    view_sql = conn.sql[0]
    assert "era5_t_zone_minutes" in view_sql
    assert "chirps_coverage" in view_sql

    comment = next(p for s, p in conn.log if s.startswith("COMMENT ON VIEW"))
    assert "LOCAL day" in comment[0]
    assert "CHIRPS product day" in comment[0]


def test_compare_view_divides_by_actual_coverage():
    """A partly-covered ERA5 cell must average over what exists, not over a full cell."""
    conn = FakeConn()
    chirps_map.create_view(conn)
    view_sql = conn.sql[0]
    assert "nullif(sum(m.weight), 0)" in view_sql
