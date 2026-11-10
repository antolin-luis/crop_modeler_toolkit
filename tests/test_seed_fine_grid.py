"""Extent-scoped fine grid build — src/db/seed_fine_grid.py.

Offline: only ``build_rows``/``extent_indices`` are exercised. ``load_grid`` needs Postgres
and is covered by the end-to-end run, matching how ``tests/test_seed_grid.py`` treats its
own loader.
"""

import numpy as np
import pytest

from src.db.seed_fine_grid import build_rows, extent_indices
from src.grid.fine_encoding import code_to_latlon, fine_parent_code_bbox, fine_parent_of
from src.grid.fine_spec import BLOCK_B, RESOLUTION

# Snapped to 0.05°; the figures below are the plan's and are what the first backfill assumes.
TOCANTINS = [-13.50, -50.75, -5.15, -45.70]
TOCANTINS_CELLS = 167 * 101  # 16,867
TOCANTINS_PARENTS = 54  # 9 parent rows x 6 parent cols

# Measured, Tocantins 1981, both sources: the GEE raster is one row and one column larger
# than the box (168 x 102 = 17,136 px/day), but those 269 extra pixels are masked and are
# dropped before Parquet, leaving exactly the box. See extent_indices' docstring for why
# that is structural rather than lucky — and why this grid must NOT be padded to 168 x 102.
TOCANTINS_RASTER_PX = 168 * 102
TOCANTINS_DROPPED_PX = TOCANTINS_RASTER_PX - TOCANTINS_CELLS  # 269

# A single 1° box, aligned to a parent boundary — should be exactly one full parent.
ONE_DEGREE = [-11.0, -49.0, -10.0, -48.0]


def test_tocantins_extent_has_the_expected_shape():
    lat_axis, lon_axis = extent_indices(TOCANTINS)
    assert len(lat_axis) == 167
    assert len(lon_axis) == 101


def test_tocantins_build_matches_the_planned_counts():
    df = build_rows(TOCANTINS)
    assert len(df) == TOCANTINS_CELLS
    assert df["fparent_id"].nunique() == TOCANTINS_PARENTS


def test_grid_covers_every_cell_the_export_actually_delivers():
    """The invariant the (reverted) padding was reaching for, pinned to measured numbers.

    The export raster is 168 x 102, but 269 of those pixels are masked — a bulge past the
    bbox is bounded by the LSIB clip's ~0.009° simplification, while a pixel centre sits
    0.025° inside its edge, so a bulged pixel's centre is never sampled. The delivered cells
    are exactly the box, so the exact box covers them. Nothing enforces this in the database
    (wth_precip_alt has no FK to chirps_base_grid), which is why it is checked here and again
    in SQL at the verification gate.
    """
    assert TOCANTINS_DROPPED_PX == 269
    assert len(build_rows(TOCANTINS)) == TOCANTINS_CELLS == 16_867


def test_every_fine_id_is_unique():
    df = build_rows(TOCANTINS)
    assert df["fine_id"].is_unique


def test_cells_stay_inside_the_requested_extent():
    """Centres sit half a cell inside each edge — the box is edges, not centres."""
    south, west, north, east = TOCANTINS
    df = build_rows(TOCANTINS)
    assert df["lat"].min() == pytest.approx(south + RESOLUTION / 2)
    assert df["lat"].max() == pytest.approx(north - RESOLUTION / 2)
    assert df["lon"].min() == pytest.approx(west + RESOLUTION / 2)
    assert df["lon"].max() == pytest.approx(east - RESOLUTION / 2)


def test_a_one_degree_box_is_exactly_one_full_parent():
    """400 cells, one parent — the property that makes partition sizing predictable."""
    df = build_rows(ONE_DEGREE)
    assert len(df) == BLOCK_B * BLOCK_B == 400
    assert df["fparent_id"].nunique() == 1
    assert fine_parent_code_bbox(df["fparent_id"].iloc[0]) == ONE_DEGREE


def test_fparent_id_agrees_with_deriving_it_from_the_fine_id():
    """The seeder and the query path must produce identical parents, or pruning misses."""
    df = build_rows(ONE_DEGREE)
    derived = df["fine_id"].map(fine_parent_of)
    assert (derived == df["fparent_id"]).all()


def test_stored_latlon_matches_decoding_the_code():
    df = build_rows(ONE_DEGREE)
    for row in df.head(50).itertuples():
        lat, lon = code_to_latlon(row.fine_id)
        assert lat == pytest.approx(row.lat)
        assert lon == pytest.approx(row.lon)


def test_longitude_is_stored_in_minus_180_to_180():
    df = build_rows([-1.0, 179.0, 0.0, 180.0])
    assert df["lon"].min() >= -180.0
    assert df["lon"].max() <= 180.0


def test_overlapping_extents_produce_identical_rows_for_shared_cells():
    """Extents accumulate by upsert, so a shared cell must encode identically either way."""
    wide = build_rows([-11.0, -49.0, -9.0, -47.0]).set_index("fine_id")
    narrow = build_rows(ONE_DEGREE).set_index("fine_id")
    shared = wide.index.intersection(narrow.index)

    assert len(shared) == 400
    np.testing.assert_allclose(wide.loc[shared, "lat"], narrow.loc[shared, "lat"])
    np.testing.assert_allclose(wide.loc[shared, "lon"], narrow.loc[shared, "lon"])
    assert (wide.loc[shared, "fparent_id"] == narrow.loc[shared, "fparent_id"]).all()


@pytest.mark.parametrize(
    "extent",
    [
        [0.0, 0.0, 0.0, 1.0],       # zero height
        [0.0, 0.0, 1.0, 0.0],       # zero width
        [1.0, 0.0, 0.0, 1.0],       # inverted latitude
    ],
)
def test_empty_or_inverted_extent_raises(extent):
    with pytest.raises(ValueError, match="empty or inverted"):
        extent_indices(extent)


def test_antimeridian_crossing_extent_raises():
    """Not split silently — the caller has to decide how to handle the seam."""
    with pytest.raises(ValueError, match="antimeridian"):
        extent_indices([-1.0, 179.0, 0.0, 181.0])
