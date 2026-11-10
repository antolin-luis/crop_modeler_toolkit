"""Sowing-date heuristic (``pltdt_finder/greenup.py``) on synthetic autumn series.

Pins the two traps found on the Weizen fields: a catch crop that greens up in September and
is then tilled in must not be taken for the crop, and a lone cloud-shadow dip between two
close dates must not be taken for tillage.
"""

from __future__ import annotations

import pandas as pd

from pltdt_finder.greenup import estimate_sowing, gdd_back

KW = dict(season_start="2025-08-01", season_end="2025-12-31", bare_end="2025-11-15",
          earliest="2025-09-01")


def _series(points: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame({"date": pd.to_datetime(list(points)),
                         "ndvi_median": list(points.values()), "clear_frac": 1.0})


def test_catch_crop_then_tillage_then_greenup():
    s = _series({"2025-08-10": 0.20, "2025-09-01": 0.50, "2025-09-12": 0.62,
                 "2025-10-02": 0.20, "2025-10-22": 0.30, "2025-11-03": 0.44,
                 "2025-11-21": 0.55})
    e = estimate_sowing(s, 1, **KW)
    assert e.removed == pd.Timestamp("2025-09-12")
    assert e.last_bare == pd.Timestamp("2025-10-02")
    assert e.greenup == pd.Timestamp("2025-10-22")
    assert e.window_start == pd.Timestamp("2025-09-13")
    assert e.sowing == pd.Timestamp("2025-09-27") and e.confident


def test_close_cloud_dip_is_not_tillage():
    s = _series({"2025-08-10": 0.80, "2025-09-15": 0.20, "2025-11-03": 0.51,
                 "2025-11-06": 0.49, "2025-11-13": 0.20, "2025-11-21": 0.64})
    e = estimate_sowing(s, 1, **KW)
    assert e.removed == pd.Timestamp("2025-08-10")
    assert e.greenup == pd.Timestamp("2025-11-03")


def test_lone_low_across_cloud_gap_is_kept():
    s = _series({"2025-09-14": 0.82, "2025-09-17": 0.44, "2025-10-02": 0.24,
                 "2025-11-03": 0.43, "2025-11-21": 0.54})
    e = estimate_sowing(s, 1, **KW)
    assert e.last_bare == pd.Timestamp("2025-10-02")
    assert e.greenup == pd.Timestamp("2025-11-03")


def test_no_bare_phase_is_low_confidence():
    s = _series({"2025-08-13": 0.46, "2025-08-18": 0.48, "2025-08-25": 0.23,
                 "2025-09-02": 0.31, "2025-09-07": 0.47, "2025-11-03": 0.57})
    assert not estimate_sowing(s, 1, **KW).confident


def _tmean(start: str, end: str, value: float) -> pd.Series:
    idx = pd.date_range(start, end, freq="D")
    return pd.Series(value, index=idx)


def test_gdd_back_constant_temperature():
    t = _tmean("2025-08-01", "2025-12-31", 10.0)
    g = pd.Timestamp("2025-11-01")
    assert gdd_back(t, g, 250, pd.Timestamp("2025-08-01")) == g - pd.Timedelta(days=25)
    assert gdd_back(t, g, 120, pd.Timestamp("2025-08-01")) == g - pd.Timedelta(days=12)


def test_gdd_back_clamps_at_floor_and_ignores_frost():
    t = _tmean("2025-08-01", "2025-12-31", -2.0)
    floor = pd.Timestamp("2025-10-01")
    assert gdd_back(t, pd.Timestamp("2025-11-01"), 250, floor) == floor


def test_gdd_lag_longer_when_cold():
    s = _series({"2025-08-10": 0.20, "2025-09-01": 0.50, "2025-09-12": 0.62,
                 "2025-10-02": 0.20, "2025-10-22": 0.30, "2025-11-03": 0.44,
                 "2025-11-21": 0.55})
    fixed = estimate_sowing(s, 1, **KW)
    cold = estimate_sowing(s, 1, **KW, tmean=_tmean("2025-08-01", "2025-12-31", 8.0))
    warm = estimate_sowing(s, 1, **KW, tmean=_tmean("2025-08-01", "2025-12-31", 15.0))
    g = pd.Timestamp("2025-10-22")
    assert cold.greenup == warm.greenup == g
    assert cold.sowing == g - pd.Timedelta(days=32)          # ceil(250 / 8)
    assert warm.sowing == g - pd.Timedelta(days=17)          # ceil(250 / 15)
    assert cold.sowing < fixed.sowing < warm.sowing
    assert cold.sowing_fixed == fixed.sowing and cold.lag_days == 32
    assert cold.window_end == g - pd.Timedelta(days=15)      # 120 / 8
    assert cold.window_start <= cold.sowing <= cold.window_end
