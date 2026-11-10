"""Sentinel-2 NDVI extraction: array -> pixel mapping, filtering, splitting, planning.

The EE side is faked: ``computePixels`` is replaced by a function returning a structured
array, and ``s2.ndvi_stack`` by a stub, so nothing here needs credentials.
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock

import numpy as np
import pytest

from src.fields import extract, s2

# Three pixels of a 3x2 bbox: (ix, iy) = (100, 501) top-left, (102, 501), (101, 500).
GRID = extract.FieldGrid(
    field_id=1, epsg=32632, region={},
    pixel_id=np.array([10, 11, 12]), ix=np.array([100, 102, 101]), iy=np.array([501, 501, 500]),
)
DATES = ["2025-08-03", "2025-08-05"]


def _array(values: dict[str, list[list[float]]]) -> np.ndarray:
    names = list(values)
    arr = np.zeros((2, 3), dtype=[(n, "f4") for n in names])
    for n in names:
        arr[n] = np.array(values[n], dtype="f4")
    return arr


def test_grid_is_snapped_to_10m_and_north_up():
    g = GRID.grid()
    assert g["dimensions"] == {"width": 3, "height": 2}
    t = g["affineTransform"]
    assert (t["translateX"], t["translateY"], t["scaleY"]) == (1000, 5020, -10)
    assert g["crsCode"] == "EPSG:32632"


def test_to_rows_maps_cells_and_filters():
    arr = _array({
        # row 0 = iy 501 (north), row 1 = iy 500
        "ndvi_20250803": [[0.1, 9, 0.3], [9, 0.5, 9]],
        "cs_20250803":   [[0.9, 0, 0.5], [0, 0.7, 0]],   # pixel 11 below threshold
        "ndvi_20250805": [[s2.NODATA, 0, 0.4], [0, np.nan, 0]],
        "cs_20250805":   [[0.9, 0, 0.9], [0, 0.9, 0]],   # 10 nodata, 12 NaN
    })
    rows = extract.to_rows(arr, GRID, DATES, cs_threshold=0.6)
    got = {(r.pixel_id, r.date): round(float(r.ndvi), 3) for r in rows.itertuples()}
    assert got == {(10, "2025-08-03"): 0.1, (12, "2025-08-03"): 0.5,
                   (11, "2025-08-05"): 0.4}


def test_fetch_rows_halves_dates_when_too_big(monkeypatch):
    monkeypatch.setattr(s2, "ndvi_stack", lambda col, dates: list(dates))
    calls = []

    def compute(req):
        dates = req["expression"]
        calls.append(len(dates))
        if len(dates) > 1:
            raise RuntimeError("Total request size (60000000 bytes) must be less than or "
                               "equal to 50331648 bytes.")
        tag = s2.band_tag(dates[0])
        return _array({f"ndvi_{tag}": [[0.2] * 3] * 2, f"cs_{tag}": [[1.0] * 3] * 2})

    rows = extract.fetch_rows(GRID, MagicMock(), DATES, 0.6, compute=compute)
    assert calls == [2, 1, 1]
    assert len(rows) == 6


def test_fetch_rows_reraises_other_errors(monkeypatch):
    monkeypatch.setattr(s2, "ndvi_stack", lambda col, dates: dates)

    def compute(req):
        raise RuntimeError("User memory limit exceeded")

    with pytest.raises(RuntimeError, match="memory"):
        extract.fetch_rows(GRID, MagicMock(), DATES, 0.6, compute=compute)


def test_plan_windows_years_and_skips():
    start, end = dt.date(2024, 3, 1), dt.date(2025, 6, 30)
    done = {
        (1, 2024): (dt.date(2024, 3, 1), dt.date(2025, 1, 1), 0.6),   # covered
        (2, 2024): (dt.date(2024, 3, 1), dt.date(2025, 1, 1), 0.8),   # stricter -> redo
        (1, 2025): (dt.date(2025, 1, 1), dt.date(2025, 5, 1), 0.6),   # shorter -> redo
    }
    w = extract.plan_windows([1, 2], start, end, 0.6, done)
    assert [(x["field_id"], x["year"]) for x in w] == [(2, 2024), (1, 2025), (2, 2025)]
    assert w[0]["start"] == "2024-03-01" and w[0]["end"] == "2025-01-01"
    assert w[-1]["end"] == "2025-07-01"
    assert len(extract.plan_windows([1, 2], start, end, 0.6, done, force=True)) == 4


def test_plan_windows_rejects_inverted_range():
    with pytest.raises(ValueError):
        extract.plan_windows([1], dt.date(2025, 1, 2), dt.date(2025, 1, 1), 0.6, {})


def test_graph_names_bands_per_date(monkeypatch):
    fake_ee = MagicMock()
    monkeypatch.setattr(s2, "ee", fake_ee)
    col = MagicMock()
    s2.ndvi_stack(col, DATES)
    mosaic = col.filterDate.return_value.map.return_value.qualityMosaic
    mosaic.assert_called_with("cs_cdf")
    renames = [c.args for c in mosaic.return_value.select.call_args_list]
    assert renames == [(["ndvi", "cs_cdf"], ["ndvi_20250803", "cs_20250803"]),
                       (["ndvi", "cs_cdf"], ["ndvi_20250805", "cs_20250805"])]
    fake_ee.Image.cat.return_value.unmask.assert_called_with(s2.NODATA)


def test_graph_filters_native_zone_and_links_cloud_score(monkeypatch):
    fake_ee = MagicMock()
    monkeypatch.setattr(s2, "ee", fake_ee)
    s2.collection("region", "2025-01-01", "2026-01-01", 32632)
    fake_ee.Filter.stringStartsWith.assert_called_with("MGRS_TILE", "32")
    fake_ee.ImageCollection.assert_any_call(s2.CS_COLLECTION)
    assert s2.utm_zone_prefix(32705) == "05"
