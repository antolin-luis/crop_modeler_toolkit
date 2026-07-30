"""Streaming GeoTIFF encode tests — parity with whole-load, NaN-drop, and the writer loop.

No live GEE / GCS: write small synthetic multiband GeoTIFFs (same shape the export
produces) and exercise ``iter_geotiff_chunks`` + ``download_variable_year`` with the EE/GCS
boundary monkeypatched to local files.
"""

import json

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import rasterio
from rasterio.transform import from_origin

from src.gee import download as dl
from src.gee import metrics as met
from src.gee.export import BlobStats, iter_geotiff_chunks, read_daily_geotiffs
from src.grid.encode_long import encode_grid
from src.grid.encoding import cell_code

B = 4
RES = 0.25


def _write_tiff(path, dates, *, nodata_cell=None):
    """3x3 cells at the grid origin; one band per date, description '<i>_<date>'.

    ``nodata_cell`` = (row, col) to mark nodata in every band (simulates masked ocean).
    """
    nt = len(dates)
    values = np.arange(nt * 9, dtype=np.float64).reshape(nt, 3, 3)
    nodata = -9999.0
    if nodata_cell is not None:
        r, c = nodata_cell
        values[:, r, c] = nodata
    transform = from_origin(-RES / 2, 90.0 + RES / 2, RES, RES)
    with rasterio.open(
        path, "w", driver="GTiff", height=3, width=3, count=nt,
        dtype="float64", crs="EPSG:4326", transform=transform, nodata=nodata,
    ) as dst:
        for i in range(nt):
            dst.write(values[i], i + 1)
            dst.set_band_description(i + 1, f"{i}_{dates[i]}")
    return values


def _encode_all(paths):
    v, lat, lon, t = read_daily_geotiffs(paths)
    return encode_grid(v, lat, lon, t, b=B)


def _encode_streamed(paths, chunk_days):
    frames = [
        encode_grid(v, lat, lon, t, b=B)
        for v, lat, lon, t in iter_geotiff_chunks(paths, chunk_days=chunk_days)
    ]
    return pd.concat(frames, ignore_index=True)


def test_stream_matches_whole_load(tmp_path):
    tiff = tmp_path / "tmax_2020.tif"
    dates = [f"2020-01-{d:02d}" for d in range(1, 8)]  # 7 days
    _write_tiff(tiff, dates)

    whole = _encode_all([tiff]).sort_values(["child_id", "date"]).reset_index(drop=True)
    streamed = (
        _encode_streamed([tiff], chunk_days=2)
        .sort_values(["child_id", "date"])
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(whole, streamed)


def test_stream_chunk_boundaries(tmp_path):
    tiff = tmp_path / "tmax_2020.tif"
    dates = [f"2020-01-{d:02d}" for d in range(1, 8)]
    _write_tiff(tiff, dates)
    sizes = [v.shape[0] for v, *_ in iter_geotiff_chunks([tiff], chunk_days=3)]
    assert sizes == [3, 3, 1]  # 7 bands in windows of 3


def test_nodata_cell_becomes_nan(tmp_path):
    tiff = tmp_path / "tmax_2020.tif"
    dates = ["2020-01-01", "2020-01-02"]
    _write_tiff(tiff, dates, nodata_cell=(0, 0))  # origin cell masked
    df = _encode_streamed([tiff], chunk_days=1)
    masked = df[df["child_id"] == cell_code(90.0, 0.0)]
    assert masked["value"].isna().all()  # dropped later by download_variable_year


class _FakeManifest:
    def __init__(self):
        self.done = set()

    def is_var_year_done(self, v, y):
        return (v, y) in self.done

    def mark_var_year_done(self, v, y):
        self.done.add((v, y))


class _FakeClient:
    gcs_prefix = "bronze-gee"

    def require_bucket(self):
        return "fake-bucket"


def test_download_variable_year_streams_and_drops_ocean(tmp_path, monkeypatch):
    # Build a tiff with one masked (ocean) cell; route the EE/GCS calls to it.
    tiff = tmp_path / "src.tif"
    dates = ["2020-01-01", "2020-01-02", "2020-01-03"]
    _write_tiff(tiff, dates, nodata_cell=(2, 2))

    # The export/wait/download triplet now lives in src.gee.metrics.run_export, so the
    # EE/GCS boundary is patched there; tz_zones/build_daily_collection stay above it.
    monkeypatch.setattr(dl, "tz_zones", lambda *a, **k: [(-3.0, None)])
    monkeypatch.setattr(dl, "build_daily_collection", lambda *a, **k: object())
    monkeypatch.setattr(met, "start_export", lambda *a, **k: object())
    monkeypatch.setattr(met, "wait_for_task", lambda *a, **k: None)
    size = tiff.stat().st_size
    monkeypatch.setattr(
        met,
        "download_prefix_measured",
        lambda *a, **k: ([tiff], BlobStats(1, size, size, size)),
    )

    bronze = tmp_path / "bronze"
    mani = _FakeManifest()
    rec: dict = {}
    out = dl.download_variable_year(
        _FakeClient(), "tmax", 2020, [-35.0, -58.5, -30.0, -53.0],
        tz_asset="projects/test/assets/tz", manifest=mani, bronze_dir=bronze, b=B,
        chunk_days=2, sample="E1", metrics_out=rec,
    )
    assert out.exists()
    assert mani.is_var_year_done("tmax", 2020)

    df = pq.read_table(out).to_pandas()
    # 9 cells minus the 1 masked, times 3 days = 24 rows; no NaN; no dup cell/day.
    assert len(df) == 8 * 3
    assert df["value"].notna().all()
    assert not df.duplicated(["child_id", "date"]).any()
    # the masked corner cell (89.5, 0.5) is absent
    assert cell_code(89.5, 0.5) not in set(df["child_id"])

    # cost record: counted (not derived) units, bytes from the fake blob, EECU honestly
    # absent because the patched wait_for_task returns no status dict.
    assert rec["sample"] == "E1"
    assert (rec["bronze_rows"], rec["cells"], rec["days"]) == (24, 8, 3)
    assert rec["n_units"] == 24 and rec["cells_exact"] is True
    assert rec["bytes_remote"] == size and rec["n_blobs"] == 1
    # 3x3 cells x 3 days of raster; 1 cell is masked ocean, so 24 of 27 are useful.
    assert rec["raster_pixels"] == 27
    assert rec["land_fraction"] == round(24 / 27, 12)
    assert rec["eecu_hours"] is None and rec["eecu_per_unit"] is None
    assert "record_write_error" not in rec

    lines = (bronze / "_gee_metrics.jsonl").read_text().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["run_id"] == rec["run_id"]
