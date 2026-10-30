"""Sowing-date heuristic (``pltdt_finder/greenup.py``) on synthetic autumn series.

Pins the two traps found on the Weizen fields: a catch crop that greens up in September and
is then tilled in must not be taken for the crop, and a lone cloud-shadow dip between two
close dates must not be taken for tillage.
"""

from __future__ import annotations

import pandas as pd

from pltdt_finder.greenup import estimate_sowing

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
