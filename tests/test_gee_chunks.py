"""Chunk geometry: tiles must cover, never overlap, and stay canonical.

The overlap property is the load-bearing one — bronze from two chunks is concatenated
without deduplication, so a cell appearing in two chunk boxes would silently double a
cell-day in silver's upsert path.
"""

from __future__ import annotations

import pytest

from src.gee.chunks import Chunk, chunk_id, tile_extent
from src.grid.encoding import cell_code, parent_bbox, parent_code, parent_code_bbox
from src.grid.spec import NLAT, NLON, RESOLUTION

BRAZIL = [-34.0, -74.0, 5.5, -34.75]
B = 4


def _cells_in(extent: list[float]) -> set[str]:
    """Every child code whose centre falls inside ``extent``."""
    south, west, north, east = extent
    out = set()
    lat = 90.0
    for lat_idx in range(NLAT):
        lat = 90.0 - lat_idx * RESOLUTION
        if not (south <= lat <= north):
            continue
        for lon_idx in range(NLON):
            lon = lon_idx * RESOLUTION
            lon = lon - 360.0 if lon > 180.0 else lon
            if west <= lon <= east:
                out.add(cell_code(lat, lon))
    return out


# --- parent inverse ---------------------------------------------------------------
def test_parent_bbox_round_trips_through_parent_code():
    for lat, lon in [(0.0, 0.0), (-15.25, -47.5), (45.5, 120.75), (-89.75, 179.75)]:
        code = parent_code(lat, lon, B)
        south, west, north, east = parent_code_bbox(code, B)
        assert south <= lat <= north
        assert west <= lon <= east
        # The corner *cells* of the box belong to the same parent. Probed at their
        # centres, not at the box edge: a point on the edge is exactly halfway between
        # two cells and ties-to-even decides which, which is a property of the encoder's
        # snap rule, not of the box.
        half = RESOLUTION / 2
        for plat, plon in [
            (south + half, west + half),
            (north - half, east - half),
        ]:
            assert parent_code(plat, plon, B) == code


def test_parent_bbox_is_one_degree_at_b4():
    south, west, north, east = parent_bbox(10, 20, B)
    assert round(north - south, 10) == 1.0
    assert round(east - west, 10) == 1.0


def test_last_parent_row_is_short_because_nlat_is_not_a_multiple_of_b():
    # NLAT=721: parent row 180 holds only lat_idx 720 (the south pole row).
    last_row = (NLAT - 1) // B
    south, _, north, _ = parent_bbox(last_row, 0, B)
    assert round(north - south, 10) == RESOLUTION
    assert round(south, 10) == -90.0 - RESOLUTION / 2


def test_blocks_past_the_seam_report_negative_lon():
    # Parent column 181 holds centres 181.00-181.75, i.e. -179.00 to -178.25 in storage.
    _, west, _, east = parent_bbox(90, 181, B)
    assert -180.0 <= west < east <= -178.0


def test_block_straddling_the_antimeridian_raises_rather_than_wrapping():
    # Column 180 holds centres 180.00-180.75: edges 179.875-180.875, across the seam.
    with pytest.raises(ValueError, match="antimeridian"):
        parent_bbox(90, 180, B)


# --- tiling -----------------------------------------------------------------------
@pytest.mark.parametrize("k", [1, 5, 10, 20])
def test_tiles_cover_the_extent_without_overlap(k):
    chunks = tile_extent(BRAZIL, k)
    seen: set[str] = set()
    for chunk in chunks:
        cells = _cells_in(chunk.extent)
        assert not (cells & seen), f"{chunk.chunk_id} overlaps an earlier chunk"
        seen |= cells
    assert _cells_in(BRAZIL) <= seen


@pytest.mark.parametrize("k", [5, 10, 20])
def test_chunk_boxes_are_square_multiples_of_the_parent(k):
    for chunk in tile_extent(BRAZIL, k):
        south, west, north, east = chunk.extent
        assert round(north - south, 10) == float(k)
        assert round(east - west, 10) == float(k)
        assert chunk.n_parents == k * k
        assert len(chunk.parent_ids) == chunk.n_parents


def test_chunk_ids_are_canonical_not_extent_relative():
    """The same box keeps its id when the requested extent changes around it."""
    wide = {c.chunk_id: tuple(c.extent) for c in tile_extent([-40.0, -80.0, 10.0, -30.0], 10)}
    narrow = {c.chunk_id: tuple(c.extent) for c in tile_extent(BRAZIL, 10)}
    shared = set(wide) & set(narrow)
    assert shared
    for cid in shared:
        assert wide[cid] == narrow[cid]


def test_parent_ids_belong_to_their_chunk_box():
    for chunk in tile_extent([-10.0, -50.0, 0.0, -40.0], 5):
        for pid in chunk.parent_ids:
            south, west, north, east = parent_code_bbox(pid, B)
            assert chunk.extent[0] <= south and north <= chunk.extent[2]
            assert chunk.extent[1] <= west and east <= chunk.extent[3]


def test_chunk_id_encodes_size_so_ladder_sizes_never_collide():
    assert chunk_id(5, 3, 7) != chunk_id(10, 3, 7)
    small = {c.chunk_id for c in tile_extent(BRAZIL, 5)}
    big = {c.chunk_id for c in tile_extent(BRAZIL, 10)}
    assert not (small & big)


def test_single_parent_chunk_matches_the_parent_grid():
    chunks = tile_extent([-2.0, -50.0, 0.0, -48.0], 1)
    assert all(c.n_parents == 1 for c in chunks)
    for chunk in chunks:
        assert chunk.extent == parent_code_bbox(chunk.parent_ids[0], B)


def test_antimeridian_extent_is_rejected_not_silently_reversed():
    with pytest.raises(NotImplementedError):
        tile_extent([-10.0, 170.0, 10.0, -170.0], 10)


def test_extent_crossing_the_prime_meridian_is_contiguous():
    """W=-10, E=10 is one band of chunks, not a range that runs backwards."""
    chunks = tile_extent([0.0, -10.0, 10.0, 10.0], 5)
    # E=10.0 is itself a cell centre, so it opens a fifth column.
    wests = sorted({c.extent[1] for c in chunks})
    assert wests == [-10.125, -5.125, -0.125, 4.875, 9.875]
    seen: set[str] = set()
    for chunk in chunks:
        cells = _cells_in(chunk.extent)
        assert not (cells & seen)
        seen |= cells


def test_global_extent_tiles_the_whole_grid():
    chunks = tile_extent([-90.0, -180.0, 90.0, 180.0], 10)
    # 36 columns of 10°; 19 rows because the 721st latitude row spills into its own.
    assert len(chunks) == 36 * 19
    assert min(c.extent[1] for c in chunks) == -180.0  # clamped at the seam
    assert min(c.extent[0] for c in chunks) == -90.0  # and at the pole
    # The seam is tiled once: lon 180 and lon -180 are the same cell.
    assert len({c.chunk_id for c in chunks}) == len(chunks)
    assert max(c.extent[3] for c in chunks) <= 180.0


def test_degenerate_extent_raises():
    with pytest.raises(ValueError):
        tile_extent([10.0, -50.0, 10.0, -40.0], 10)


def test_chunk_is_hashable_and_frozen():
    chunk = tile_extent(BRAZIL, 10)[0]
    assert isinstance(chunk, Chunk)
    with pytest.raises(Exception):
        chunk.chunk_id = "nope"  # type: ignore[misc]
