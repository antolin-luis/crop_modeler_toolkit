"""Area-weighted 0.05° → 0.25° mapping, and the comparison view.

The two grids do **not** nest. ERA5 cell edges land on ``0.125 + k*0.25``, which is not a
multiple of 0.05, so no arithmetic maps one onto the other — a fine cell straddles up to
four ERA5 cells and an ERA5 cell overlaps 36 fine cells (16 fully, 20 partially). Moving
between them is therefore an area-weighted spatial join, computed once and stored where it
can be inspected and undone, rather than a division applied at read time.

That is the rule stated in docs/climate_context_layer.md §96: regridding is a silver
derivation, not something done silently at ingest. Aggregating up (0.05° → 0.25°) is the
only direction offered — going the other way would manufacture detail ERA5 does not have.
"""

from __future__ import annotations

from src.db import load as db_load

MAP_TABLE = "chirps_era5_map"
FINE_GRID = "chirps_base_grid"
ERA5_GRID = "era5_land_base_grid"
VIEW = "precip_compare"


def build_map(conn, *, replace: bool = False) -> int:
    """Populate ``chirps_era5_map`` from the two grids' geometries. Returns rows written.

    Weights are the fine cell's share of each ERA5 cell it touches, so they sum to 1.0 per
    ``child_id`` **over the fine cells present** — which is the honest denominator: if the
    fine grid only covers part of an ERA5 cell, the aggregate is over what exists, and the
    coverage column below is what tells you so.

    Only cells that actually overlap are stored, so this is ~4 rows per fine cell rather
    than the full cross product.
    """
    with conn.cursor() as cur:
        if replace:
            cur.execute(f"TRUNCATE {MAP_TABLE}")
        cur.execute(
            f"""
            INSERT INTO {MAP_TABLE} (fine_id, child_id, weight)
            SELECT f.fine_id,
                   e.child_id,
                   ST_Area(ST_Intersection(f.geom, e.geom)) / ST_Area(e.geom)
            FROM {FINE_GRID} f
            JOIN {ERA5_GRID} e
              ON f.geom && e.geom
             AND ST_Area(ST_Intersection(f.geom, e.geom)) > 0
            ON CONFLICT (fine_id, child_id) DO UPDATE SET weight = EXCLUDED.weight
            """
        )
        cur.execute(f"SELECT count(*) FROM {MAP_TABLE}")
        total = cur.fetchone()[0]
    conn.commit()
    return total


def coverage_report(conn) -> list[tuple]:
    """``(child_id, covered_fraction, n_fine)`` — how much of each ERA5 cell the fine grid has.

    A partially-covered ERA5 cell still produces a comparison number, and that number is an
    average over a fraction of the cell. This is what lets a consumer see which ones, rather
    than discovering it as an unexplained bias at the extent's edge.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT child_id, sum(weight) AS covered, count(*) AS n_fine
            FROM {MAP_TABLE}
            GROUP BY child_id
            ORDER BY covered
            """
        )
        return cur.fetchall()


# The offset the view cannot reconcile is stated in its own comment, not buried in docs:
# wth_base.date is a per-cell LOCAL day, wth_precip_alt.date is the CHIRPS product day.
# Comparing them means comparing two slightly different 24-hour windows. At monthly or
# seasonal aggregation that is immaterial; on a single day it is not.
_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {VIEW} AS
SELECT
    w.parent_id,
    w.child_id,
    w.date,
    p.source,
    w.precip                              AS era5_precip,
    sum(p.precip * m.weight) / nullif(sum(m.weight), 0)  AS chirps_precip,
    sum(m.weight)                         AS chirps_coverage,
    count(*)                              AS n_fine_cells,
    g.t_zone                              AS era5_t_zone_minutes
FROM {MAP_TABLE} m
JOIN wth_precip_alt p ON p.fine_id = m.fine_id
JOIN wth_base       w ON w.child_id = m.child_id AND w.date = p.date
JOIN {ERA5_GRID}    g ON g.child_id = m.child_id
GROUP BY w.parent_id, w.child_id, w.date, p.source, w.precip, g.t_zone
"""

_VIEW_COMMENT = (
    "CHIRPS area-averaged to the 0.25 deg ERA5 grid, beside ERA5's own precip. "
    "CAVEAT: era5_precip is on a per-cell LOCAL day (offset = era5_t_zone_minutes); "
    "chirps_precip is on the CHIRPS product day, a fixed UTC-anchored window. The join "
    "on date treats them as equal, which they are not. Sound at monthly/seasonal "
    "aggregation; do not read a single-day difference as a measurement disagreement. "
    "chirps_coverage < 1.0 means the fine grid covers only part of that ERA5 cell."
)


def create_view(conn) -> None:
    """(Re)create the ``precip_compare`` view and its caveat comment."""
    with conn.cursor() as cur:
        cur.execute(_VIEW_SQL)
        cur.execute(f"COMMENT ON VIEW {VIEW} IS %s", (_VIEW_COMMENT,))
    conn.commit()


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Build chirps_era5_map and precip_compare.")
    ap.add_argument("--replace", action="store_true", help="truncate the map first")
    args = ap.parse_args()

    conn = db_load.connect()
    try:
        total = build_map(conn, replace=args.replace)
        print(f"{MAP_TABLE}: {total:,} rows")
        create_view(conn)
        print(f"{VIEW}: created")

        report = coverage_report(conn)
        partial = [r for r in report if r[1] < 0.999]
        print(f"{len(report):,} ERA5 cells mapped; {len(partial):,} only partly covered")
        for child_id, covered, n_fine in partial[:5]:
            print(f"  {child_id}: {covered:.3f} covered by {n_fine} fine cells")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
