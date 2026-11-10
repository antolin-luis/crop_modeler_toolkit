"""Packed GEE export: all variables of a year in one export, split back per variable.

Pins the contract that makes packing invisible downstream: the same
``bronze/<var>/<var>_<year>.parquet`` files and the same manifest marks as the per-variable
path. EE/GCS are monkeypatched to a local multiband GeoTIFF, like ``test_gee_stream.py``.
"""

from unittest.mock import MagicMock

import numpy as np
import pyarrow.parquet as pq
import pytest
import rasterio
from rasterio.transform import from_origin

from src.gee import daily
from src.gee import download as dl
from src.gee import metrics as met
from src.gee.chunks import CELL_ZONE_CEILING, fits_packed, pack_groups
from src.gee.export import (
    BlobStats,
    ExportRestartedError,
    band_labels,
    iter_packed_geotiff_chunks,
)

B = 4
RES = 0.25
VARS = ["tmax", "wind_u", "precip"]
DATES = ["2020-01-01", "2020-01-02", "2020-01-03"]


def _write_packed(path, variables, dates):
    """3x3 cells at the grid origin; bands '<k>_<i>_<var>_<date>' as toBands names them."""
    labels = [(v, d) for v in variables for d in dates]
    with rasterio.open(
        path, "w", driver="GTiff", height=3, width=3, count=len(labels), dtype="float64",
        crs="EPSG:4326", transform=from_origin(-RES / 2, 90.0 + RES / 2, RES, RES),
        nodata=-9999.0,
    ) as dst:
        for i, (v, d) in enumerate(labels):
            dst.write(np.full((3, 3), 100.0 * VARS.index(v) + i, dtype="float64"), i + 1)
            dst.set_band_description(i + 1, f"{VARS.index(v)}_{i}_{v}_{d}")


def test_band_labels_match_whole_names():
    descs = ["1_0_wind_u_2020-01-01", "0_5_tmax_2020-12-31", "2_1_1_precip_2021-02-28"]
    assert band_labels(descs, ["u", "wind_u", "tmax", "precip"]) == [
        ("wind_u", "2020-01-01"), ("tmax", "2020-12-31"), ("precip", "2021-02-28")
    ]
    with pytest.raises(ValueError, match="names none"):
        band_labels(["0_tdew_2020-01-01"], VARS)


def test_packed_reader_never_mixes_variables(tmp_path):
    tiff = tmp_path / "packed_2020.tif"
    _write_packed(tiff, VARS, DATES)
    windows = list(iter_packed_geotiff_chunks([tiff], VARS, chunk_days=2))
    assert [(v, a.shape[0]) for v, a, *_ in windows] == [
        ("tmax", 2), ("tmax", 1), ("wind_u", 2), ("wind_u", 1), ("precip", 2), ("precip", 1)
    ]
    v, arr, _, _, times = windows[2]
    assert v == "wind_u" and arr[0, 0, 0] == 100.0 + 3
    assert [t.strftime("%Y-%m-%d") for t in times] == DATES[:2]


class _Manifest:
    def __init__(self, done=()):
        self.done = set(done)

    def is_var_year_done(self, v, y):
        return (v, y) in self.done

    def mark_var_year_done(self, v, y):
        self.done.add((v, y))


class _Client:
    gcs_prefix = "bronze-gee"

    def require_bucket(self):
        return "fake-bucket"


def _patch_export(monkeypatch, tiff, built):
    monkeypatch.setattr(dl, "tz_zones", lambda *a, **k: [(1.0, None)])

    def _build(variables, year, *, zones):
        built.append(list(variables))
        return object()

    monkeypatch.setattr(dl, "build_packed_collection", _build)
    monkeypatch.setattr(met, "start_export", lambda *a, **k: object())
    monkeypatch.setattr(met, "wait_for_task", lambda *a, **k: None)
    size = tiff.stat().st_size
    monkeypatch.setattr(
        met, "download_prefix_measured",
        lambda *a, **k: ([tiff], BlobStats(1, size, size, size)),
    )


def test_download_year_packed_writes_per_variable_files(tmp_path, monkeypatch):
    tiff = tmp_path / "src.tif"
    _write_packed(tiff, VARS, DATES)
    built: list = []
    _patch_export(monkeypatch, tiff, built)
    bronze, mani, rec = tmp_path / "bronze", _Manifest(), {}

    outs = dl.download_year_packed(
        _Client(), VARS, 2020, [51.4, 11.65, 51.85, 12.1], tz_asset="t", manifest=mani,
        bronze_dir=bronze, b=B, chunk_days=2, metrics_out=rec,
    )
    assert [p.name for p in outs] == ["tmax_2020.parquet", "wind_u_2020.parquet",
                                      "precip_2020.parquet"]
    assert mani.done == {(v, 2020) for v in VARS}
    df = pq.read_table(bronze / "wind_u" / "wind_u_2020.parquet").to_pandas()
    assert list(df.columns) == ["child_id", "parent_id", "date", "value"]
    assert len(df) == 9 * 3 and set(df["value"]) == {103.0, 104.0, 105.0}
    assert rec["kind"] == "bronze_year_packed" and rec["variable"] == "tmax+wind_u+precip"
    assert not list(bronze.rglob("*.tmp"))


def test_download_year_packed_exports_only_missing(tmp_path, monkeypatch):
    tiff = tmp_path / "src.tif"
    _write_packed(tiff, ["wind_u", "precip"], DATES)
    built: list = []
    _patch_export(monkeypatch, tiff, built)
    bronze = tmp_path / "bronze"
    (bronze / "tmax").mkdir(parents=True)
    (bronze / "tmax" / "tmax_2020.parquet").write_bytes(b"done")
    mani = _Manifest({("tmax", 2020)})

    dl.download_year_packed(
        _Client(), VARS, 2020, [51.4, 11.65, 51.85, 12.1], tz_asset="t", manifest=mani,
        bronze_dir=bronze, b=B,
    )
    assert built == [["wind_u", "precip"]]
    assert (bronze / "tmax" / "tmax_2020.parquet").read_bytes() == b"done"

    built.clear()
    dl.download_year_packed(
        _Client(), VARS, 2020, [51.4, 11.65, 51.85, 12.1], tz_asset="t", manifest=mani,
        bronze_dir=bronze, b=B,
    )
    assert built == []   # everything done: no export at all


def test_packed_collection_labels_and_merges(monkeypatch):
    fake_ee = MagicMock()
    monkeypatch.setattr(daily, "ee", fake_ee)
    cols = {v: MagicMock(name=v) for v in VARS}
    monkeypatch.setattr(daily, "build_daily_collection", lambda v, y, zones: cols[v])
    daily.build_packed_collection(VARS, 2020, zones=[(1.0, None)])
    first = cols["tmax"].map.return_value
    first.merge.assert_called_once_with(cols["wind_u"].map.return_value)
    first.merge.return_value.merge.assert_called_once_with(cols["precip"].map.return_value)

    # The per-image function prefixes the variable onto the date label and casts to double.
    one = cols["wind_u"].map.call_args.args[0]
    one(MagicMock())
    fake_ee.String.assert_any_call("wind_u_")
    assert fake_ee.Image.return_value.toDouble.called


def test_fits_packed_budget():
    assert fits_packed(4, 1, 7)
    assert not fits_packed(0, 1, 7)
    edge = CELL_ZONE_CEILING // 7
    assert fits_packed(edge, 1, 7) and not fits_packed(edge + 1, 1, 7)
    assert not fits_packed(9_968, 1, 7)   # Uruguay-size stays per-variable


def test_pack_groups_balanced_and_capped():
    seven = ["tmax", "tmin", "precip", "srad", "wind_u", "wind_v", "tdew"]
    assert pack_groups(seven) == [seven[:3], seven[3:5], seven[5:]]
    assert pack_groups(seven, 4) == [seven[:4], seven[4:]]
    assert pack_groups(seven, 7) == [seven]
    assert pack_groups(["tmax"]) == [["tmax"]]
    assert pack_groups([]) == []


def test_adaptive_splits_restarted_groups_down_to_singles(monkeypatch):
    calls = []

    def fake(client, variables, year, extent, **kw):
        calls.append((list(variables), kw.get("max_attempts")))
        if len(variables) > 1 and "tmax" in variables:
            raise ExportRestartedError("restarted")
        return [f"{v}_{year}" for v in variables]

    monkeypatch.setattr(dl, "download_year_packed", fake)
    out = dl.download_year_packed_adaptive(
        None, ["tmax", "tmin", "precip"], 2025, [], log=lambda *_: None, max_attempts=2
    )
    assert out == ["tmax_2025", "tmin_2025", "precip_2025"]
    # Every attempt keeps the caller's restart tolerance; only exhausting it splits.
    assert calls == [(["tmax", "tmin", "precip"], 2), (["tmax", "tmin"], 2),
                     (["tmax"], 2), (["tmin"], 2), (["precip"], 2)]


def test_adaptive_reraises_for_a_single_variable(monkeypatch):
    def fake(*a, **k):
        raise ExportRestartedError("restarted")

    monkeypatch.setattr(dl, "download_year_packed", fake)
    with pytest.raises(ExportRestartedError):
        dl.download_year_packed_adaptive(None, ["tmax"], 2025, [], log=lambda *_: None)


def test_concurrent_groups_of_a_year_get_disjoint_gcs_paths(tmp_path, monkeypatch):
    from src.gee.export import own_shards

    prefixes = []
    tiff = tmp_path / "src.tif"
    _write_packed(tiff, ["tmax"], DATES)
    _patch_export(monkeypatch, tiff, [])
    real = met.run_export

    def spy(*a, **k):
        prefixes.append(k["name_prefix"])
        return real(*a, **k)

    monkeypatch.setattr(dl, "run_export", spy)
    for group in (["tmax"], ["precip"]):
        _write_packed(tiff, group, DATES)
        dl.download_year_packed(_Client(), group, 2020, [51.4, 11.65, 51.85, 12.1],
                                tz_asset="t", manifest=_Manifest(), bronze_dir=tmp_path / "b")
    a, b = prefixes
    assert a != b

    class Blob:
        def __init__(self, name):
            self.name = name

    # Neither group's shard listing can claim the other's files.
    assert own_shards([Blob(b + ".tif")], a) == []
    assert own_shards([Blob(a + ".tif")], b) == []
