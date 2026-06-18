"""Tests that lock the grid contract (PLANNING.md §6).

These guard the one thing PLANNING.md says must never change: the deterministic
encoding. A future edit that shifts the origin or alphabet must break these.
"""

import pytest

from src.grid.encoding import cell_code, code_to_latlon, parent_code
from src.grid.spec import CODE_SPACE, NLAT, NLON


# Cell-center coordinates (exact multiples of 0.25°). Avoid lon == ±180 exactly:
# -180 % 360 == 180, which decodes to +180 (a documented antimeridian quirk, §16).
ROUND_TRIP_SAMPLES = [
    (90.0, 0.0),       # north pole, prime meridian -> "0000"
    (-90.0, 0.0),      # south pole
    (0.0, 0.0),        # equator / prime meridian
    (0.0, 179.75),     # just west of antimeridian (positive)
    (0.0, -179.75),    # just east of antimeridian (negative)
    (-23.0, -46.0),    # southern hemisphere, western longitude (Brazil-ish)
    (45.25, 90.5),     # arbitrary interior cell
]


@pytest.mark.parametrize("lat,lon", ROUND_TRIP_SAMPLES)
def test_round_trip(lat, lon):
    lat2, lon2 = code_to_latlon(cell_code(lat, lon))
    assert lat2 == pytest.approx(lat)
    assert lon2 == pytest.approx(lon)


def test_capacity():
    # PLANNING.md §6.1: the whole globe fits in a 4-char base-36 code with headroom.
    assert NLAT * NLON == 1_038_240
    assert NLAT * NLON < CODE_SPACE == 36**4


def test_known_codes():
    # Hand-computed pins so a silent origin shift cannot pass.
    assert cell_code(90.0, 0.0) == "0000"     # lat_idx=0, lon_idx=0 -> n=0
    assert cell_code(0.0, 0.0) == "B400"      # lat_idx=360, lon_idx=0 -> n=518400
    assert code_to_latlon("0000") == (90.0, 0.0)


def test_parent_grouping():
    b = 4
    # Two children in the same b x b block share a parent.
    p_origin = parent_code(90.0, 0.0, b)
    p_same_block = parent_code(90.0 - 3 * 0.25, 0.0 + 3 * 0.25, b)  # within 4x4
    assert p_origin == p_same_block

    # A child one block over (4 cells east) lands in a different parent.
    p_next_block = parent_code(90.0, 0.0 + b * 0.25, b)
    assert p_next_block != p_origin
