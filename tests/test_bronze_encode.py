"""Bronze encode tests (PLANNING.md §7, §5.2) — synthetic netCDF → Parquet frame.

No live CDS: build a tiny daily netCDF at the grid origin, encode it, and check the
``child_id``/``parent_id``/``date``/``value`` columns. A second fixture with two
timestamps on the same day must be rejected by the daily-sanity guard.
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from src.cds.download import encode_netcdf
from src.grid.encoding import cell_code, parent_code

B = 4


def _write_nc(path, times):
    # 2x2 cells at the grid origin: lat {90.0, 89.75}, lon {0.0, 0.25}.
    nt = len(times)
    values = np.arange(nt * 4, dtype=np.float64).reshape(nt, 2, 2)
    da = xr.DataArray(
        values,
        dims=("time", "latitude", "longitude"),
        coords={
            "time": pd.to_datetime(times),
            "latitude": [90.0, 89.75],
            "longitude": [0.0, 0.25],
        },
        name="t2m",
    )
    da.to_dataset().to_netcdf(path)


def test_encode_daily_netcdf(tmp_path):
    nc = tmp_path / "tmax_2020.nc"
    times = ["2020-01-01", "2020-01-02", "2020-01-03"]
    _write_nc(nc, times)

    df = encode_netcdf(nc, b=B)

    assert list(df.columns) == ["child_id", "parent_id", "date", "value"]
    assert len(df) == 3 * 4  # 3 days × 4 cells
    # Origin cell (90.0, 0.0) encodes to the known codes from the scalar functions.
    origin = df[(df["date"] == pd.Timestamp("2020-01-01").date())].iloc[0]
    assert origin["child_id"] == cell_code(90.0, 0.0)
    assert origin["parent_id"] == parent_code(90.0, 0.0, B)
    # One value per cell per day, all finite.
    assert not df.duplicated(["child_id", "date"]).any()
    assert np.isfinite(df["value"]).all()
    assert set(df["date"]) == {pd.Timestamp(t).date() for t in times}


def test_round_trip_child_id(tmp_path):
    from src.grid.encoding import code_to_latlon

    nc = tmp_path / "tmax_2020.nc"
    _write_nc(nc, ["2020-01-01"])
    df = encode_netcdf(nc, b=B)
    lat, lon = code_to_latlon(cell_code(89.75, 0.25))
    assert (lat, lon) == (89.75, 0.25)
    assert cell_code(89.75, 0.25) in set(df["child_id"])


def test_hourly_netcdf_rejected(tmp_path):
    nc = tmp_path / "bad.nc"
    # Two timestamps on the SAME calendar day → not daily (§5.2).
    _write_nc(nc, ["2020-01-01T00:00", "2020-01-01T12:00"])
    with pytest.raises(ValueError, match="not daily"):
        encode_netcdf(nc, b=B)
