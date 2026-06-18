"""Land-clip tests (PLANNING.md §6.4).

Synthetic ERA5-Land (0.1°) arrays over a few 0.25° cells. The key property is the
non-integer 2.5 ratio: fine centroids do NOT nest cleanly, so a single 0.25° row of
ten 0.1° centroids splits 3/2/3/2 across four coarse cells.
"""

import numpy as np
import xarray as xr

from src.grid.land_mask import compute_is_land
from src.grid.spec import LAT_ORIGIN, RESOLUTION

# lat 89.0 -> coarse lat_idx = floor((90-89)/0.25) = 4
LAT = 89.0
LAT_IDX = int(np.floor((LAT_ORIGIN - LAT) / RESOLUTION))
# lon 0.0..0.9 step 0.1 -> coarse lon_idx counts: idx0=3, idx1=2, idx2=3, idx3=2
LONS = np.round(np.arange(0.0, 1.0, 0.1), 2)


def _mask(land_flags: list[bool]) -> xr.DataArray:
    """Build a (1, 10) ERA5-Land DataArray; land=finite, sea=NaN."""
    vals = np.where(land_flags, 1.0, np.nan).reshape(1, len(land_flags))
    return xr.DataArray(
        vals,
        dims=("latitude", "longitude"),
        coords={"latitude": [LAT], "longitude": LONS},
    )


def test_all_land_cells_true():
    out = compute_is_land(_mask([True] * 10), fraction_x=0.5)
    for lon_idx in (0, 1, 2, 3):
        assert out[LAT_IDX, lon_idx]


def test_no_overlap_cells_false():
    out = compute_is_land(_mask([True] * 10), fraction_x=0.5)
    # A coarse cell no fine centroid maps to stays False (default).
    assert not out[LAT_IDX, 50]
    assert not out[LAT_IDX + 1, 0]


def test_fraction_threshold_and_binning():
    # idx0 fine = {0.0,0.1,0.2}: only 0.0 land -> 1/3 = 0.33 < 0.5 -> False
    # idx2 fine = {0.5,0.6,0.7}: 0.5,0.6 land -> 2/3 = 0.67 >= 0.5 -> True
    flags = [True, False, False,   # 0.0 0.1 0.2  -> idx0
             True, True,           # 0.3 0.4      -> idx1
             True, True, False,    # 0.5 0.6 0.7  -> idx2
             False, False]         # 0.8 0.9      -> idx3
    out = compute_is_land(_mask(flags), fraction_x=0.5)
    assert not out[LAT_IDX, 0]   # 0.33
    assert out[LAT_IDX, 1]       # 1.0
    assert out[LAT_IDX, 2]       # 0.67
    assert not out[LAT_IDX, 3]   # 0.0


def test_fraction_knob_moves_boundary():
    flags = [True, False, False, True, True, True, True, False, False, False]
    strict = compute_is_land(_mask(flags), fraction_x=0.7)
    loose = compute_is_land(_mask(flags), fraction_x=0.3)
    # idx2 = 2/3 = 0.67: included when X=0.3, excluded when X=0.7
    assert loose[LAT_IDX, 2]
    assert not strict[LAT_IDX, 2]
