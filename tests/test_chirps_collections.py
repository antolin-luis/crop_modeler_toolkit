"""CHIRPS source registry and daily collection builder (mocked ee).

No live Earth Engine: ``ee`` is replaced with a MagicMock and the per-day callback handed to
``ee.List.map`` is captured and invoked, the same pattern as ``tests/test_gee_daily.py``.
"""

import calendar
from unittest.mock import MagicMock

import pytest

import src.gee.chirps as chirps_mod
from src.gee.chirps import (
    ALL_SOURCES,
    BAND,
    CHIRPS_SOURCES,
    MAX_PRECIP_MM,
    covers_extent,
    source_code,
    source_spec,
)

TOCANTINS = [-13.50, -50.75, -5.15, -45.70]


# --- registry (pure) ---------------------------------------------------------------

def test_v2_and_v3_live_under_different_orgs():
    """CHG vs CHC is a one-character trap that fails late and unhelpfully inside EE."""
    assert CHIRPS_SOURCES["chirps_v2"].collection == "UCSB-CHG/CHIRPS/DAILY"
    assert CHIRPS_SOURCES["chirps_v3_rnl"].collection == "UCSB-CHC/CHIRPS/V3/DAILY_RNL"


def test_every_source_starts_in_1981():
    """No CHIRPS product reaches further back; 45 years is the ceiling, not 50."""
    assert {s.first_year for s in CHIRPS_SOURCES.values()} == {1981}


def test_v3_covers_more_latitude_than_v2():
    assert CHIRPS_SOURCES["chirps_v2"].lat_bounds == (-50.0, 50.0)
    assert CHIRPS_SOURCES["chirps_v3_rnl"].lat_bounds == (-60.0, 60.0)


def test_source_codes_are_unique_and_stable():
    """These are written into a PK column; changing one silently re-labels stored rows."""
    codes = {name: source_code(name) for name in ALL_SOURCES}
    assert codes == {"chirps_v2": 2, "chirps_v3_rnl": 3, "chirps_v3_sat": 4}
    assert len(set(codes.values())) == len(codes)


def test_unknown_source_raises_with_the_known_names():
    with pytest.raises(KeyError, match="chirps_v3_rnl"):
        source_spec("chirps_v4")


def test_tocantins_is_covered_by_both_versions():
    assert covers_extent("chirps_v2", TOCANTINS)
    assert covers_extent("chirps_v3_rnl", TOCANTINS)


def test_extent_below_50s_is_covered_only_by_v3():
    """Southern Patagonia: v2 stops at 50S, so it would return masked pixels there."""
    patagonia = [-56.0, -75.0, -50.5, -65.0]
    assert not covers_extent("chirps_v2", patagonia)
    assert covers_extent("chirps_v3_rnl", patagonia)


def test_catalog_max_is_the_documented_value():
    assert MAX_PRECIP_MM == pytest.approx(1444.34)


# --- build_daily_collection (mocked ee) --------------------------------------------

def _fake_ee_with_capture():
    fake_ee = MagicMock()
    captured = {}

    def _capture_map(fn):
        captured["fn"] = fn
        return fake_ee.ImageCollection.return_value

    fake_ee.List.sequence.return_value.map.side_effect = _capture_map
    return fake_ee, captured


def test_selects_the_requested_collection_and_band(monkeypatch):
    fake_ee, _ = _fake_ee_with_capture()
    monkeypatch.setattr(chirps_mod, "ee", fake_ee)

    chirps_mod.build_daily_collection("chirps_v3_rnl", 2020)

    fake_ee.ImageCollection.assert_any_call("UCSB-CHC/CHIRPS/V3/DAILY_RNL")
    fake_ee.ImageCollection.return_value.select.assert_called_once_with(BAND)


def test_v2_and_v3_build_from_different_collections(monkeypatch):
    for name, expected in [
        ("chirps_v2", "UCSB-CHG/CHIRPS/DAILY"),
        ("chirps_v3_sat", "UCSB-CHC/CHIRPS/V3/DAILY_SAT"),
    ]:
        fake_ee, _ = _fake_ee_with_capture()
        monkeypatch.setattr(chirps_mod, "ee", fake_ee)
        chirps_mod.build_daily_collection(name, 2020)
        fake_ee.ImageCollection.assert_any_call(expected)


def test_every_band_is_cast_to_float(monkeypatch):
    """Mixed band dtypes make EE reject the whole export.

    The missing-day placeholder is ``ee.Image.constant(0)``, a *Byte* image; real CHIRPS is
    Float32. The placeholder only fires for a day the product lacks, so 1981-2025 exported
    fine and 2026 — the partial year past the catalog's end — failed with "Exported bands
    must have compatible data types; found inconsistent types: Float32 and Byte." after 90
    of 92 tasks had already succeeded. The cast must apply to every band, not just the
    placeholder, so the type does not depend on whether a given day exists.
    """
    fake_ee, captured = _fake_ee_with_capture()
    monkeypatch.setattr(chirps_mod, "ee", fake_ee)

    chirps_mod.build_daily_collection("chirps_v3_rnl", 2026)
    day_image = captured["fn"](0)

    renamed = fake_ee.Image.return_value.rename.return_value
    renamed.toFloat.assert_called()
    # ...and the stamped image is the cast one, not the pre-cast one.
    assert day_image is renamed.toFloat.return_value.set.return_value


@pytest.mark.parametrize("year,expected", [(2020, 366), (2021, 365), (1981, 365)])
def test_one_band_per_calendar_day(monkeypatch, year, expected):
    """Band count must equal day count or the date-to-band mapping on read shifts."""
    fake_ee, _ = _fake_ee_with_capture()
    monkeypatch.setattr(chirps_mod, "ee", fake_ee)

    chirps_mod.build_daily_collection("chirps_v3_rnl", year)

    assert expected == (366 if calendar.isleap(year) else 365)
    fake_ee.List.sequence.assert_called_once_with(0, expected - 1)


def test_no_reducer_and_no_timezone_zones_are_used(monkeypatch):
    """CHIRPS is already daily: no hourly reduce, no local-day mosaic, zones == 1."""
    fake_ee, captured = _fake_ee_with_capture()
    monkeypatch.setattr(chirps_mod, "ee", fake_ee)

    chirps_mod.build_daily_collection("chirps_v3_rnl", 2020)
    captured["fn"](0)

    fake_ee.Reducer.max.assert_not_called()
    fake_ee.Reducer.sum.assert_not_called()
    fake_ee.Reducer.mean.assert_not_called()
    fake_ee.ImageCollection.return_value.mosaic.assert_not_called()


def test_each_day_is_stamped_with_a_date_property(monkeypatch):
    """`date` is the only contract start_export requires of the collection."""
    fake_ee, captured = _fake_ee_with_capture()
    monkeypatch.setattr(chirps_mod, "ee", fake_ee)

    chirps_mod.build_daily_collection("chirps_v3_rnl", 2020)
    captured["fn"](5)

    props = fake_ee.Image.return_value.rename.return_value.toFloat.return_value.set.call_args[
        0
    ][0]
    assert set(props) == {"system:time_start", "date"}
    fake_ee.Date.fromYMD.return_value.advance.return_value.format.assert_any_call(
        "YYYY-MM-dd"
    )


def test_a_missing_day_becomes_a_masked_image_not_a_gap(monkeypatch):
    """A dropped band would shift every later date by one — worse than a masked day."""
    fake_ee, captured = _fake_ee_with_capture()
    monkeypatch.setattr(chirps_mod, "ee", fake_ee)

    chirps_mod.build_daily_collection("chirps_v3_rnl", 2020)
    captured["fn"](0)

    fake_ee.Algorithms.If.assert_called_once()
    fake_ee.Image.constant.assert_called_once_with(0)
    fake_ee.Image.constant.return_value.selfMask.assert_called_once()


def test_day_window_advances_one_calendar_day(monkeypatch):
    fake_ee, captured = _fake_ee_with_capture()
    monkeypatch.setattr(chirps_mod, "ee", fake_ee)

    chirps_mod.build_daily_collection("chirps_v3_rnl", 2020)
    captured["fn"](3)

    day_start = fake_ee.Date.fromYMD.return_value.advance.return_value
    day_start.advance.assert_any_call(1, "day")
    fake_ee.Date.fromYMD.assert_called_once_with(2020, 1, 1)
