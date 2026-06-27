"""Shared grid encoder tests (src/grid/encode_long.encode_grid).

Direct coverage of the raster→long step both backends share: correct codes/dates and the
daily-sanity guard. Mirrors the CDS netCDF test but exercises the encoder on plain arrays.
"""

import numpy as np
import pandas as pd
import pytest

from src.grid.encode_long import encode_grid
from src.grid.encoding import cell_code, code_to_latlon, parent_code

B = 4


def _grid():
    # 2x2 cells at the grid origin: lat {90.0, 89.75}, lon {0.0, 0.25}.
    lat = np.array([90.0, 89.75])
    lon = np.array([0.0, 0.25])
    return lat, lon


def test_encode_grid_codes_and_dates():
    lat, lon = _grid()
    times = pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"])
    values = np.arange(3 * 4, dtype=np.float64).reshape(3, 2, 2)

    df = encode_grid(values, lat, lon, times, b=B)

    assert list(df.columns) == ["child_id", "parent_id", "date", "value"]
    assert len(df) == 3 * 4
    origin = df[df["date"] == pd.Timestamp("2020-01-01").date()].iloc[0]
    assert origin["child_id"] == cell_code(90.0, 0.0)
    assert origin["parent_id"] == parent_code(90.0, 0.0, B)
    assert not df.duplicated(["child_id", "date"]).any()
    assert np.isfinite(df["value"]).all()


def test_encode_grid_child_id_round_trips():
    lat, lon = _grid()
    df = encode_grid(
        np.zeros((1, 2, 2)), lat, lon, pd.to_datetime(["2020-01-01"]), b=B
    )
    assert (89.75, 0.25) == code_to_latlon(cell_code(89.75, 0.25))
    assert cell_code(89.75, 0.25) in set(df["child_id"])


def test_encode_grid_rejects_two_per_day():
    lat, lon = _grid()
    # Two timesteps on the same calendar day → not daily (§5.2).
    times = pd.to_datetime(["2020-01-01T00:00", "2020-01-01T12:00"])
    with pytest.raises(ValueError, match="not daily"):
        encode_grid(np.zeros((2, 2, 2)), lat, lon, times, b=B)


def test_encode_grid_snaps_off_center_coords():
    # Coordinates a hair off the canonical centers must snap to the same cells (away from
    # the 0/360 longitude seam, where a tiny negative lon legitimately wraps).
    lat = np.array([89.749, 89.501])
    lon = np.array([0.249, 0.501])
    df = encode_grid(
        np.zeros((1, 2, 2)), lat, lon, pd.to_datetime(["2020-01-01"]), b=B
    )
    assert cell_code(89.75, 0.25) in set(df["child_id"])
    assert cell_code(89.5, 0.5) in set(df["child_id"])
