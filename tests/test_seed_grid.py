"""Seed-build tests (PLANNING.md §8.1) — no live CDS, no DB.

Exercises ``build_rows`` with tiny synthetic geopotential + land-mask fixtures. The
grid is always the full global enumeration; the fixtures only populate elevation /
is_land over a small patch (the rest is NaN / False), so row count is invariant.
"""

import numpy as np
import pytest
import xarray as xr

from src.db.seed_grid import GRAVITY, build_rows
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
