"""Fine (0.05° CHIRPS) grid encoding — src/grid/fine_encoding.py.

The properties worth locking are the ones that would fail *silently*: a half-cell shift, a
code-width collision with the 0.25° grid, or the vectorized path drifting from the scalar
one during a seed build.
"""

import numpy as np
import pytest

from src.grid import encoding as era5_encoding
from src.grid.fine_encoding import (
    cell_center,
    cell_indices,
    cell_indices_arr,
    code_indices,
    code_to_latlon,
    fine_code,
    fine_codes,
    fine_parent_bbox,
    fine_parent_code,
    fine_parent_code_bbox,
    fine_parent_codes,
    fine_parent_indices,
    fine_parent_of,
)
from src.grid.fine_spec import BLOCK_B, CODE_SPACE, CODE_WIDTH, LAT_ORIGIN, NLAT, NLON, RESOLUTION

# Palmas, Tocantins. Indices/codes hand-computed in the plan; if these change, the grid
# changed, and every stored fine_id is invalidated.
PALMAS = (-10.24, -48.36)
PALMAS_INDICES = (1404, 6232)
PALMAS_FINE_ID = "60ST4"
PALMAS_FPARENT = "00JON"


def test_known_cell_encodes_to_expected_indices_and_code():
    assert cell_indices(*PALMAS) == PALMAS_INDICES
    assert fine_code(*PALMAS) == PALMAS_FINE_ID
    assert fine_parent_code(*PALMAS) == PALMAS_FPARENT


def test_grid_capacity_fits_the_code_space():
    assert NLAT * NLON == 17_280_000
    assert NLAT * NLON < CODE_SPACE


def test_codes_are_always_five_chars_and_never_collide_with_the_era5_grid():
    """Width is the type tag: 4 chars is always 0.25°, 5 chars is always 0.05°."""
    for lat, lon in [(59.99, -179.99), (-59.99, 179.99), (0.0, 0.0), PALMAS]:
        assert len(fine_code(lat, lon)) == CODE_WIDTH
        assert len(fine_parent_code(lat, lon)) == CODE_WIDTH
        assert len(era5_encoding.cell_code(lat, lon)) == 4


def test_decoding_a_four_char_code_is_rejected():
    era5_code = era5_encoding.cell_code(*PALMAS)
    with pytest.raises(ValueError, match="0.25"):
        code_indices(era5_code)


@pytest.mark.parametrize(
    "lat,lon",
    [
        PALMAS,
        (0.0, 0.0),
        (59.999, -179.999),
        (-59.999, 179.999),
        (-13.5, -50.75),  # Tocantins SW corner
        (-5.15, -45.70),  # Tocantins NE corner
        (12.0, -90.0),  # Honduras extent corner
    ],
)
def test_round_trip_code_to_centre_and_back(lat, lon):
    """A cell centre must re-encode to the same code — the basic closure property."""
    code = fine_code(lat, lon)
    c_lat, c_lon = code_to_latlon(code)
    assert fine_code(c_lat, c_lon) == code


def test_centre_is_offset_half_a_cell_from_the_edge():
    """The whole point of D2: edges on multiples of 0.05, centres at (i+0.5)*0.05.

    A half-cell error here produces output that looks completely reasonable, which is why
    it gets its own test rather than riding on the round-trip.
    """
    # Both 0.0s are cell EDGES, so the cell containing (0, 0) is centred half a cell south
    # and half a cell east of it — never on it.
    lat, lon = cell_center(0.0, 0.0)
    assert lon == pytest.approx(0.025)
    assert lat == pytest.approx(-0.025)
    # Same at the grid's north edge: lat 60.0 is row 0, centred at 59.975.
    assert cell_center(LAT_ORIGIN, 0.0)[0] == pytest.approx(LAT_ORIGIN - 0.025)
    # Contrast the 0.25° grid, where a coordinate on a multiple IS a centre.
    assert era5_encoding.code_to_latlon(era5_encoding.cell_code(0.0, 0.0)) == (0.0, 0.0)


def test_coordinate_on_a_cell_edge_bins_to_the_cell_south_and_east_of_it():
    """Edge coordinates are the common case on a 0.05° grid; the rule must be stable."""
    lat_idx, _ = cell_indices(-10.25, 0.0)
    assert lat_idx == cell_indices(-10.25 + RESOLUTION / 2, 0.0)[0] + 1
    # and float dust must not flip it
    assert cell_indices(-10.25 - 1e-12, 0.0)[0] == lat_idx
    assert cell_indices(-10.25 + 1e-12, 0.0)[0] == lat_idx


def test_longitude_convention_is_accepted_either_way():
    assert fine_code(-10.24, -48.36) == fine_code(-10.24, 311.64)


def test_out_of_band_latitude_clamps_rather_than_raising():
    """CHIRPS stops at 60S/60N; a caller outside it gets the edge row, not an exception."""
    assert cell_indices(75.0, 0.0)[0] == 0
    assert cell_indices(-75.0, 0.0)[0] == NLAT - 1


# --- parents ---------------------------------------------------------------------

def test_parent_holds_exactly_block_b_squared_cells():
    """No short edge blocks anywhere — NLAT and NLON are both divisible by BLOCK_B."""
    assert NLAT % BLOCK_B == 0
    assert NLON % BLOCK_B == 0

    lat_i = np.arange(NLAT)
    counts = np.bincount(lat_i // BLOCK_B)
    assert set(counts.tolist()) == {BLOCK_B}


def test_parent_of_derives_the_same_code_as_the_coordinate_path():
    """fine_parent_of is what saves a DB round-trip on region queries; it must agree."""
    for lat, lon in [PALMAS, (0.0, 0.0), (-13.5, -50.75), (59.99, 179.99)]:
        assert fine_parent_of(fine_code(lat, lon)) == fine_parent_code(lat, lon)


def test_parent_bbox_lands_on_integer_degrees():
    assert fine_parent_code_bbox(PALMAS_FPARENT) == [-11.0, -49.0, -10.0, -48.0]


def test_parent_bbox_contains_its_member_cells():
    south, west, north, east = fine_parent_code_bbox(PALMAS_FPARENT)
    lat, lon = code_to_latlon(PALMAS_FINE_ID)
    assert south < lat < north
    assert west < lon < east


def test_parent_round_trip_indices():
    p_row, p_col = fine_parent_indices(PALMAS_FPARENT)
    assert (p_row, p_col) == (70, 311)
    assert fine_parent_bbox(p_row, p_col) == fine_parent_code_bbox(PALMAS_FPARENT)


def test_no_parent_block_straddles_the_antimeridian():
    """180° is itself a parent boundary at b=20, so the straddle case cannot arise."""
    for p_col in range(NLON // BLOCK_B):
        south, west, north, east = fine_parent_bbox(70, p_col)
        assert west < east
        assert -180.0 <= west <= 180.0
        assert -180.0 <= east <= 180.0


def test_parent_blocks_tile_without_gap_or_overlap():
    boxes = [fine_parent_bbox(r, 311) for r in range(NLAT // BLOCK_B)]
    for upper, lower in zip(boxes, boxes[1:]):
        assert upper[0] == pytest.approx(lower[2])  # south of one == north of the next


def test_parent_block_outside_the_grid_raises():
    with pytest.raises(ValueError, match="outside the fine grid"):
        fine_parent_bbox(NLAT // BLOCK_B, 0)


# --- vectorized paths ------------------------------------------------------------

def test_vectorized_codes_agree_with_scalar():
    """The seed build uses the array path; a drift here corrupts the whole grid table."""
    rng = np.random.default_rng(0)
    lat_i = rng.integers(0, NLAT, 500)
    lon_i = rng.integers(0, NLON, 500)

    vec_cells = fine_codes(lat_i, lon_i)
    vec_parents = fine_parent_codes(lat_i, lon_i)
    for k, (la, lo) in enumerate(zip(lat_i, lon_i)):
        lat = LAT_ORIGIN - (la + 0.5) * RESOLUTION
        lon = (lo + 0.5) * RESOLUTION
        assert vec_cells[k] == fine_code(lat, lon)
        assert vec_parents[k] == fine_parent_code(lat, lon)


def test_vectorized_binning_agrees_with_scalar():
    lats = np.array([-10.24, 0.0, 59.99, -59.99, -13.5])
    lons = np.array([-48.36, 0.0, -179.99, 179.99, -50.75])
    lat_i, lon_i = cell_indices_arr(lats, lons)
    for k, (la, lo) in enumerate(zip(lats, lons)):
        assert (int(lat_i[k]), int(lon_i[k])) == cell_indices(la, lo)


def test_vectorized_codes_are_all_five_chars():
    lat_i, lon_i = cell_indices_arr(np.array([-10.24]), np.array([-48.36]))
    assert fine_codes(lat_i, lon_i).dtype.kind == "U"
    assert all(len(c) == CODE_WIDTH for c in fine_codes(lat_i, lon_i))
