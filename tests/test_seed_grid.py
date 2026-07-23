"""Seed-build tests (PLANNING.md §8.1) — no live CDS, no DB.

Exercises ``build_rows`` with tiny synthetic geopotential + land-mask fixtures. The
grid is always the full global enumeration; the fixtures only populate elevation /
is_land over a small patch (the rest is NaN / False), so row count is invariant.
"""

import numpy as np
import pytest
import xarray as xr

from src.db.seed_grid import (
    GRAVITY,
    _fill_coastal_from_nearest_land,
    _longitude_offset_minutes,
    assign_timezone,
    build_rows,
    std_offset_minutes,
)
from src.grid.encoding import cell_code, parent_code
from src.grid.spec import NLAT, NLON

B = 4


@pytest.fixture
def geopotential():
    # Four 0.25° cells at the grid origin; elevation = z/g = [[0,100],[200,300]].
    z = np.array([[0.0, 100.0], [200.0, 300.0]]) * GRAVITY
    return xr.DataArray(
        z,
        dims=("latitude", "longitude"),
        coords={"latitude": [90.0, 89.75], "longitude": [0.0, 0.25]},
        name="z",
    )


@pytest.fixture
def land_mask():
    # One land cell near the origin; NaN (sea) elsewhere.
    vals = np.array([[1.0, np.nan]])
    return xr.DataArray(
        vals,
        dims=("latitude", "longitude"),
        coords={"latitude": [89.0], "longitude": [0.0, 0.1]},
    )


def test_global_row_count(geopotential, land_mask):
    df = build_rows(geopotential, land_mask, b=B)
    assert len(df) == NLAT * NLON == 1_038_240


def test_codes_consistent_with_scalar_encoding(geopotential, land_mask):
    df = build_rows(geopotential, land_mask, b=B)
    # Sample across the whole grid (incl. high-lon cells stored as negative lon).
    for i in (0, 1, NLON, 500_000, len(df) - 1):
        row = df.iloc[i]
        assert row["child_id"] == cell_code(row["lat"], row["lon"])
        assert row["parent_id"] == parent_code(row["lat"], row["lon"], B)


def test_lat_lon_ranges(geopotential, land_mask):
    df = build_rows(geopotential, land_mask, b=B)
    assert df["lat"].between(-90.0, 90.0).all()
    # lon stored -180..180; the +180 meridian keeps +180.0 (matches code_to_latlon).
    assert df["lon"].between(-180.0, 180.0).all()


def test_elevation_finite_where_geopotential_present(geopotential, land_mask):
    df = build_rows(geopotential, land_mask, b=B)
    # Cell (lat_idx=0, lon_idx=0) is row 0: elevation 0.0; (0,1) is row 1: 100 m.
    assert df.iloc[0]["elevation"] == pytest.approx(0.0)
    assert df.iloc[1]["elevation"] == pytest.approx(100.0)
    assert df.iloc[NLON]["elevation"] == pytest.approx(200.0)  # (lat_idx=1, lon_idx=0)
    # A far-away cell has no geopotential -> NaN.
    assert np.isnan(df.iloc[500_000]["elevation"])


def test_is_land_mostly_false_with_tiny_fixture(geopotential, land_mask):
    df = build_rows(geopotential, land_mask, b=B)
    # Only the single fixture cell can be land; the global grid is overwhelmingly sea.
    assert df["is_land"].sum() <= 1


# --- per-cell timezone (t_zone) -----------------------------------------------------
@pytest.mark.parametrize("tzid, minutes", [
    ("America/Sao_Paulo", -180),     # Brazil (SE) = UTC-3, no DST since 2019
    ("America/Tegucigalpa", -360),   # Honduras = UTC-6
    ("America/Montevideo", -180),    # Uruguay = UTC-3
    ("Asia/Kolkata", 330),           # India = UTC+5:30 (fractional -> minutes)
    ("Asia/Kathmandu", 345),         # Nepal = UTC+5:45
    ("UTC", 0),
])
def test_std_offset_minutes(tzid, minutes):
    assert std_offset_minutes(tzid) == minutes


@pytest.mark.parametrize("lon, minutes", [
    (-45.0, -180), (-60.0, -240), (-75.0, -300), (0.0, 0), (30.0, 120),
])
def test_longitude_offset_fallback(lon, minutes):
    assert _longitude_offset_minutes(lon) == minutes


def test_assign_timezone_land_uses_shapefile_ocean_uses_longitude():
    # Montevideo (land) -> -180 from the tz polygon; open Pacific (not land) -> longitude.
    lat = [-34.9, 0.0]
    lon = [-56.2, -140.0]
    is_land = [True, False]
    out = assign_timezone(lat, lon, is_land)
    assert out.dtype == np.int16
    assert out[0] == -180
    assert out[1] == _longitude_offset_minutes(-140.0)  # solar fallback, not a tz polygon


def test_assign_timezone_isolated_ocean_land_stays_solar():
    # A land-flagged cell over open ocean with no resolved land neighbor keeps the solar
    # fallback (nothing to fill from).
    out = assign_timezone([0.0], [-140.0], [True])
    assert out[0] == _longitude_offset_minutes(-140.0)


def test_fill_coastal_from_nearest_land_within_cap_only():
    # Regular 0.25° lattice: a resolved land cell (offset -180), an unresolved neighbor one
    # cell away (inherits it), and an unresolved cell 20 cells away (> 8-cell cap, untouched).
    lat = np.array([0.0, 0.0, 0.0])
    lon = np.array([0.0, 0.25, 5.0])
    SOLAR = np.int16(-999)  # sentinel standing in for the solar fallback already in `out`
    out = np.array([-180, SOLAR, SOLAR], dtype=np.int16)
    resolved = np.array([0], dtype=np.int64)
    unresolved = np.array([1, 2], dtype=np.int64)

    filled = _fill_coastal_from_nearest_land(lat, lon, out, resolved, unresolved)

    assert filled == 1
    assert out[1] == -180      # adjacent coastal cell inherits the political offset
    assert out[2] == SOLAR     # beyond the ring cap -> keeps its solar fallback


def test_fill_coastal_picks_nearest_when_two_offsets_reach():
    # Two resolved land cells with different offsets; the unresolved water cell adopts the
    # geometrically nearer one.
    lat = np.array([0.0, 0.0, 0.0])
    lon = np.array([0.0, 1.0, 0.25])   # idx2 (unresolved) is 1 cell from idx0, 3 from idx1
    out = np.array([-180, -240, np.int16(0)], dtype=np.int16)
    resolved = np.array([0, 1], dtype=np.int64)
    unresolved = np.array([2], dtype=np.int64)

    _fill_coastal_from_nearest_land(lat, lon, out, resolved, unresolved)

    assert out[2] == -180  # nearest resolved land, not the farther -240


def test_build_rows_has_t_zone(geopotential, land_mask):
    df = build_rows(geopotential, land_mask, b=B)
    assert "t_zone" in df.columns
    assert df["t_zone"].notna().all()          # NOT NULL in the DDL
    assert df["t_zone"].between(-720, 840).all()  # valid UTC offset range (minutes)
