"""Region → parent lookups against the seeded grid (the inverse of ``silver_load``'s).

``src/db/silver_load.fetch_cell_meta`` consumes a parent list; nothing produced one. Chunked
exports need the producing side: given an extent, which parents actually hold land, and how
many land cells each has. Without it a chunked backfill would submit exports for blocks that
are pure ocean — every one of them a full EE task, a queue slot and a wasted couple of
minutes, for a GeoTIFF with no valid pixel in it.

Deliberately **not** a PostGIS query. The grid carries ``lat``/``lon`` columns, the boxes
here are axis-aligned, and one sequential pass over 1 M rows beats a GiST lookup per chunk;
``geom`` stays reserved for real polygon work (``src/db/query.py`` says the same).
"""

from __future__ import annotations

from dataclasses import dataclass

from src.db.load import connect

_LAND_IN_BBOX = """
    SELECT parent_id, t_zone, count(*)
    FROM era5_land_base_grid
    WHERE is_land
      AND lat BETWEEN %s AND %s
      AND lon BETWEEN %s AND %s
    GROUP BY parent_id, t_zone
"""


@dataclass(frozen=True)
class ChunkStats:
    """What the grid knows about one export box before any EE call is made."""

    land_parents: int
    land_cells: int
    zones: int  # distinct t_zone offsets — the GEE export mosaics one reduction per zone

    @property
    def cell_zones(self) -> int:
        """``land_cells x zones`` — the quantity that predicts whether an export lands.

        Measured 2026-07-31 across 47 probe records: every export at or below **58,554**
        completed on the first attempt, and the one above it (the whole-Brazil extent at
        ~90,748) was restarted by EE three times in each of two independent runs. Neither
        term alone separates the cases — a 19,518-cell export succeeded, and so did a
        4-zone one. See docs/cost_model_climate_context.md §9.4.
        """
        return self.land_cells * max(self.zones, 1)


def land_parents_in_bbox(
    extent: list[float], *, conn=None
) -> dict[str, tuple[int, frozenset]]:
    """``{parent_id: (land_cells, frozenset_of_t_zone)}`` for land cells centred in ``extent``.

    ``extent`` is ``[S, W, N, E]`` in -180..180. An extent crossing the antimeridian would
    need two calls; the chunker rejects those upstream. ``t_zone`` comes from the same tz
    shapefile that built ``GEE_TZ_ASSET``, so the zone count here matches the number of
    reduction zones ``export.tz_zones`` will derive — without spending a ``getInfo``.
    """
    south, west, north, east = extent
    owned = conn is None
    conn = conn or connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_LAND_IN_BBOX, (south, north, west, east))
            acc: dict[str, tuple[int, set]] = {}
            for parent_id, t_zone, count in cur.fetchall():
                cells, zones = acc.get(parent_id, (0, set()))
                zones.add(t_zone)
                acc[parent_id] = (cells + int(count), zones)
        return {pid: (cells, frozenset(zones)) for pid, (cells, zones) in acc.items()}
    finally:
        if owned:
            conn.close()


def chunk_land_stats(chunks, *, conn=None) -> dict[str, ChunkStats]:
    """``{chunk_id: ChunkStats}`` for a list of :class:`~src.gee.chunks.Chunk`.

    One query covers the union of the chunks' boxes, then the counts are bucketed in
    Python — chunks tile without overlap, so no cell is double-counted. Chunks with no land
    are present in the result with zeroes, so the caller can report what it dropped rather
    than silently losing it.
    """
    if not chunks:
        return {}
    south = min(c.extent[0] for c in chunks)
    west = min(c.extent[1] for c in chunks)
    north = max(c.extent[2] for c in chunks)
    east = max(c.extent[3] for c in chunks)
    per_parent = land_parents_in_bbox([south, west, north, east], conn=conn)

    out: dict[str, ChunkStats] = {}
    for chunk in chunks:
        parents = cells = 0
        zones: set = set()
        for pid in chunk.parent_ids:
            hit = per_parent.get(pid)
            if not hit:
                continue
            parents += 1
            cells += hit[0]
            zones |= hit[1]
        out[chunk.chunk_id] = ChunkStats(parents, cells, len(zones))
    return out
