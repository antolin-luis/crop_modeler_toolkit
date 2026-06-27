"""GEE GeoTIFF reader test — synthetic multiband tiff → (values, lat, lon, times) → frame.

No live GEE / GCS: write a small multiband GeoTIFF with date-named band descriptions
exactly as the export produces, read it back through ``read_daily_geotiffs``, and encode
with the shared ``encode_grid``.
"""

import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin

from src.gee.export import read_daily_geotiffs
from src.grid.encode_long import encode_grid
from src.grid.encoding import cell_code, parent_code

B = 4
RES = 0.25


def _write_tiff(path, dates):
    """2x2 cells at the grid origin; one band per date, description '<i>_<date>'."""
    nt = len(dates)
    values = np.arange(nt * 4, dtype=np.float64).reshape(nt, 2, 2)
    # Top-left corner so pixel CENTERS land on (90.0,0.0),(89.75,0.25).
    transform = from_origin(-RES / 2, 90.0 + RES / 2, RES, RES)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=2,
        width=2,
        count=nt,
        dtype="float64",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        for i in range(nt):
            dst.write(values[i], i + 1)
            dst.set_band_description(i + 1, f"{i}_{dates[i]}")
    return values


def test_read_and_encode_geotiff(tmp_path):
    tiff = tmp_path / "tmax_2020.tif"
    dates = ["2020-01-01", "2020-01-02", "2020-01-03"]
    _write_tiff(tiff, dates)

    values, lat, lon, times = read_daily_geotiffs([tiff])

    assert values.shape == (3, 2, 2)
    assert set(pd.to_datetime(times).strftime("%Y-%m-%d")) == set(dates)
    # lat/lon recovered as the canonical cell centers (order may be top-down).
    assert set(np.round(lat, 2)) == {90.0, 89.75}
    assert set(np.round(lon, 2)) == {0.0, 0.25}

    df = encode_grid(values, lat, lon, times, b=B)
    assert len(df) == 3 * 4
    origin = df[df["date"] == pd.Timestamp("2020-01-01").date()]
    assert cell_code(90.0, 0.0) in set(origin["child_id"])
    assert parent_code(90.0, 0.0, B) in set(origin["parent_id"])
    assert not df.duplicated(["child_id", "date"]).any()
