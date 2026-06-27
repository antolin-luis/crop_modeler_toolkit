"""Server-side daily-reduce tests — local-day window math + reducer choice (mocked ee).

No live Earth Engine: ``ee`` is replaced with a MagicMock and the per-day callback handed
to ``ee.List.map`` is captured and invoked, so we can assert the window shift and reducer
without any network call.
"""

from unittest.mock import MagicMock

import pytest

import src.gee.daily as daily_mod
from src.gee.daily import parse_offset_hours


# --- parse_offset_hours (pure) ------------------------------------------------------
@pytest.mark.parametrize(
    "tz, expected",
    [
        ("UTC", 0.0),
        ("UTC-03:00", -3.0),
        ("UTC+00:00", 0.0),
        ("UTC+05:30", 5.5),
        ("UTC-09:30", -9.5),
        ("  UTC-03:00  ", -3.0),
    ],
)
def test_parse_offset_hours(tz, expected):
    assert parse_offset_hours(tz) == expected


@pytest.mark.parametrize("bad", ["", "GMT-3", "UTC-3", "-03:00", "UTC-0300"])
def test_parse_offset_hours_rejects_bad(bad):
    with pytest.raises(ValueError):
        parse_offset_hours(bad)


# --- build_daily_collection (mocked ee) ---------------------------------------------
def _fake_ee_with_capture():
    """Return (fake_ee, captured) where captured['fn'] is the per-day callback."""
    fake_ee = MagicMock()
    captured = {}

    def _capture_map(fn):
        captured["fn"] = fn
        return fake_ee.ImageCollection.return_value

    fake_ee.List.sequence.return_value.map.side_effect = _capture_map
    return fake_ee, captured


def test_selects_collection_band_and_reducer(monkeypatch):
    fake_ee, _ = _fake_ee_with_capture()
    monkeypatch.setattr(daily_mod, "ee", fake_ee)

    daily_mod.build_daily_collection("tmax", 2020, offset_hours=-3.0)

    fake_ee.ImageCollection.assert_any_call("ECMWF/ERA5/HOURLY")
    fake_ee.ImageCollection.return_value.select.assert_called_once_with("temperature_2m")
    fake_ee.Reducer.max.assert_called_once()


@pytest.mark.parametrize(
    "variable, reducer_attr",
    [("tmax", "max"), ("tmin", "min"), ("precip", "sum"), ("wind_u", "mean")],
)
def test_reducer_per_variable(monkeypatch, variable, reducer_attr):
    fake_ee, _ = _fake_ee_with_capture()
    monkeypatch.setattr(daily_mod, "ee", fake_ee)

    daily_mod.build_daily_collection(variable, 2021, offset_hours=-3.0)

    getattr(fake_ee.Reducer, reducer_attr).assert_called_once()


@pytest.mark.parametrize("year, last_index", [(2020, 365), (2021, 364)])
def test_day_sequence_length_handles_leap(monkeypatch, year, last_index):
    fake_ee, _ = _fake_ee_with_capture()
    monkeypatch.setattr(daily_mod, "ee", fake_ee)

    daily_mod.build_daily_collection("tmax", year, offset_hours=-3.0)

    fake_ee.List.sequence.assert_called_once_with(0, last_index)


def test_local_day_window_shift(monkeypatch):
    # For UTC-3 (offset -3), the window starts at local_start.advance(+3h) and is 24h wide.
    fake_ee, captured = _fake_ee_with_capture()
    monkeypatch.setattr(daily_mod, "ee", fake_ee)

    daily_mod.build_daily_collection("tmax", 2020, offset_hours=-3.0)
    captured["fn"](5)  # run the per-day callback for some day index

    # year_start.advance(day,'day') -> local_start; local_start.advance(-offset,'hour').
    local_start = fake_ee.Date.fromYMD.return_value.advance.return_value
    local_start.advance.assert_any_call(3.0, "hour")  # -(-3.0)
    # win_start.advance(24,'hour') -> win_end (win_start is local_start.advance.return_value)
    win_start = local_start.advance.return_value
    win_start.advance.assert_any_call(24, "hour")
