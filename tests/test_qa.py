"""Silver QA node tests (PLANNING.md §8.4)."""

import numpy as np
import pandas as pd

from src.transform.qa import calendar_report, split_valid

COLUMNS = ["parent_id", "child_id", "date", "tmax", "tmin", "precip", "srad",
           "wind", "tdew", "rh", "et0"]


def _row(**overrides):
    base = {
        "parent_id": "0XKE", "child_id": "EU9K", "date": pd.Timestamp("2020-01-01").date(),
        "tmax": 30.0, "tmin": 18.0, "precip": 5.0, "srad": 22.0,
        "wind": 5.0, "tdew": 18.0, "rh": 55.0, "et0": 4.5,
    }
    return {**base, **overrides}


def _frame(*rows):
    return pd.DataFrame(list(rows), columns=COLUMNS)


def test_clean_frame_passes_untouched():
    good, failures = split_valid(_frame(_row()))
    assert len(good) == 1
    assert failures.empty


def test_each_rule_quarantines_with_its_reason():
    cases = {
        "tmax<tmin": _row(tmax=10.0, tmin=20.0),
        "precip<0": _row(precip=-0.1),
        "srad<0": _row(srad=-1.0),
        "rh_out_of_range": _row(rh=140.0),
        "et0<0": _row(et0=-0.5),
    }
    for reason, row in cases.items():
        good, failures = split_valid(_frame(row))
        assert good.empty, reason
        assert list(failures["reason"]) == [reason]


def test_multiple_reasons_are_joined():
    _, failures = split_valid(_frame(_row(tmax=10.0, tmin=20.0, precip=-1.0)))
    assert failures.loc[0, "reason"] == "tmax<tmin;precip<0"


def test_nan_never_fails_a_check():
    # A NULL derived value is the documented outcome of a missing input (§8.3).
    good, failures = split_valid(_frame(_row(rh=np.nan, et0=np.nan, srad=np.nan)))
    assert len(good) == 1
    assert failures.empty


def test_split_keeps_good_rows_and_drops_bad_ones():
    good, failures = split_valid(_frame(_row(), _row(child_id="EU9L", precip=-2.0)))
    assert list(good["child_id"]) == ["EU9K"]
    assert list(failures["child_id"]) == ["EU9L"]
    assert list(good.index) == [0] and list(failures.index) == [0]  # reset


def test_calendar_report_counts_missing_leap_day():
    dates = pd.date_range("2020-01-01", "2020-12-31", freq="D")
    dates = dates[dates != pd.Timestamp("2020-02-29")]  # drop the leap day
    frame = pd.DataFrame({
        "child_id": "EU9K",
        "date": [d.date() for d in dates],
    })

    report = calendar_report(frame, 2020)
    assert report.loc[0, "expected"] == 366
    assert report.loc[0, "days"] == 365
    assert report.loc[0, "missing"] == 1


def test_calendar_report_complete_non_leap_year():
    dates = pd.date_range("2021-01-01", "2021-12-31", freq="D")
    frame = pd.DataFrame({"child_id": "EU9K", "date": [d.date() for d in dates]})

    report = calendar_report(frame, 2021)
    assert report.loc[0, "expected"] == 365
    assert report.loc[0, "missing"] == 0
