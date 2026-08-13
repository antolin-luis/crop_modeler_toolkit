"""Repair ladder tests (PLANNING.md §8.4).

Synthetic per-cell series; no DB. The cases that matter are not "does it produce a number"
but "does it produce the *right kind* of number" — interpolation must recover a held-out
day, climatology must exclude flagged years from its own mean, and precip must never be
routed through an average.
"""

import numpy as np
import pandas as pd
import pytest

from src.transform import repair

CELLS = ["EU9K", "EU9L", "EU9M"]


def _history(dates, values_by_cell):
    """``values_by_cell`` maps child_id -> one value per date."""
    return pd.DataFrame(
        [
            {"child_id": cell, "date": d, "value": float(v)}
            for cell, values in values_by_cell.items()
            for d, v in zip(dates, values)
        ]
    )


def _smooth_series(start="1987-01-20", days=14, seed=0):
    """A plausible temperature series: smooth, autocorrelated, one value per cell-day."""
    dates = pd.date_range(start, periods=days, freq="D")
    rng = np.random.default_rng(seed)
    base = 21.0 + np.cumsum(rng.normal(0.0, 0.4, size=days))
    return dates, {cell: base + i * 0.3 for i, cell in enumerate(CELLS)}


# --- interpolate_temporal --------------------------------------------------------------


def test_interpolation_recovers_a_held_out_day():
    dates, values = _smooth_series()
    target = dates[7]
    truth = {cell: v[7] for cell, v in values.items()}

    history = _history(dates, values)
    history = history[history["date"] != target]  # hold the day out

    filled, method = repair.interpolate_temporal(history, target)

    for cell, expected in truth.items():
        assert filled[cell] == pytest.approx(expected, abs=0.5)
    assert method.startswith(repair.METHOD_INTERPOLATE)
    assert "1987-01-26..1987-01-28" in method


def test_interpolation_refuses_a_gap_longer_than_the_limit():
    dates, values = _smooth_series()
    target = dates[7]
    history = _history(dates, values)
    # Blank the whole neighbourhood: the flanking days no longer describe this day.
    near = dates[(dates > target - pd.Timedelta(days=4)) & (dates < target + pd.Timedelta(days=4))]
    history = history[~history["date"].isin(near)]

    filled, _ = repair.interpolate_temporal(history, target)
    assert filled.isna().all()


def test_interpolation_returns_nan_rather_than_extrapolating():
    """A target past the end of the series has no flank on one side."""
    dates, values = _smooth_series()
    filled, _ = repair.interpolate_temporal(_history(dates, values), dates[-1] + pd.Timedelta(days=1))
    assert filled.isna().all()


# --- climatology_fill ------------------------------------------------------------------


def test_climatology_averages_the_same_doy_across_other_years():
    dates = pd.DatetimeIndex([pd.Timestamp(f"{y}-01-26") for y in (1984, 1985, 1986, 1987)])
    values = {cell: [10.0, 20.0, 30.0, -999.0] for cell in CELLS}

    filled, method = repair.climatology_fill(_history(dates, values), pd.Timestamp("1987-01-26"))

    # The target year is excluded from its own climatology.
    for cell in CELLS:
        assert filled[cell] == pytest.approx(20.0)
    assert "n=3" in method


def test_climatology_excludes_flagged_days_from_its_own_mean():
    dates = pd.DatetimeIndex([pd.Timestamp(f"{y}-01-26") for y in (1984, 1985, 1986, 1987)])
    values = {cell: [10.0, 20.0, -5.49, -999.0] for cell in CELLS}

    filled, method = repair.climatology_fill(
        _history(dates, values),
        pd.Timestamp("1987-01-26"),
        exclude_dates=[pd.Timestamp("1986-01-26")],
    )

    for cell in CELLS:
        assert filled[cell] == pytest.approx(15.0)
    assert "n=2" in method


def test_climatology_returns_nan_with_no_usable_window():
    dates = pd.DatetimeIndex([pd.Timestamp("1987-01-26")])
    filled, _ = repair.climatology_fill(
        _history(dates, {c: [1.0] for c in CELLS}), pd.Timestamp("1987-01-26")
    )
    assert filled.isna().all()


# --- analog_day_fill -------------------------------------------------------------------


def _precip_history():
    """Zero-inflated, heavy-tailed: most days dry, the total in a few events."""
    dates = pd.DatetimeIndex(
        [pd.Timestamp(f"{y}-05-{d:02d}") for y in (1994, 1995, 1996) for d in range(14, 25)]
    )
    rng = np.random.default_rng(7)
    wet = rng.random(len(dates)) < 0.35
    amounts = np.where(wet, rng.gamma(2.0, 8.0, size=len(dates)), 0.0)
    return dates, {cell: amounts * (1.0 + 0.1 * i) for i, cell in enumerate(CELLS)}


def test_analog_day_returns_a_value_that_exists_in_the_window():
    """The whole point: borrow a real day, never compute an average of many."""
    dates, values = _precip_history()
    history = _history(dates, values)

    filled, method = repair.analog_day_fill(history, pd.Timestamp("1998-05-19"))

    for cell in CELLS:
        observed = history.loc[history["child_id"] == cell, "value"].to_numpy()
        assert np.isclose(observed, filled[cell]).any()
    assert method.startswith(repair.METHOD_ANALOG_DAY)


def test_analog_day_preserves_the_windows_dry_day_fraction():
    """A mean fill would make every cell-day slightly wet — the drizzle-forever failure."""
    dates, values = _precip_history()
    history = _history(dates, values)

    dry_draws = 0
    trials = 40
    for seed in range(trials):
        filled, _ = repair.analog_day_fill(history, pd.Timestamp("1998-05-19"), seed=seed)
        if float(filled[CELLS[0]]) == 0.0:
            dry_draws += 1

    window_dry = float((history[history["child_id"] == CELLS[0]]["value"] == 0.0).mean())
    assert dry_draws / trials == pytest.approx(window_dry, abs=0.2)
    assert dry_draws > 0  # a mean fill would never produce a dry day


def test_analog_day_uses_one_donor_day_for_every_cell():
    """Rainfall is spatially correlated — an independent draw per cell would shred the
    field into incoherent noise, no more physical than a mean."""
    dates, values = _precip_history()
    history = _history(dates, values)

    filled, method = repair.analog_day_fill(history, pd.Timestamp("1998-05-19"))
    donor = pd.Timestamp(method.split("(")[1].rstrip(")"))

    expected = history[history["date"] == donor].set_index("child_id")["value"]
    for cell in CELLS:
        assert filled[cell] == pytest.approx(expected[cell])


def test_analog_day_is_reproducible_for_a_given_seed():
    dates, values = _precip_history()
    history = _history(dates, values)

    first = repair.analog_day_fill(history, pd.Timestamp("1998-05-19"), seed=3)
    second = repair.analog_day_fill(history, pd.Timestamp("1998-05-19"), seed=3)

    assert first[1] == second[1]
    assert list(first[0]) == list(second[0])


# --- the ladder ------------------------------------------------------------------------


def test_precip_ladder_contains_no_averaging_rung():
    """The structural guard: `auto` must not be able to route precip into a mean."""
    assert not set(repair.ladder_for("precip")) & repair.AVERAGING_METHODS
    assert repair.ladder_for("precip") == (repair.METHOD_ANALOG_DAY,)


def test_smooth_variables_interpolate_before_averaging():
    for variable in ("tmax", "tmin", "tdew", "srad", "wind"):
        rungs = repair.ladder_for(variable)
        assert rungs.index(repair.METHOD_INTERPOLATE) < rungs.index(repair.METHOD_CLIMATOLOGY)


def test_repair_field_refuses_to_average_precip_even_when_asked():
    dates, values = _precip_history()
    with pytest.raises(ValueError, match="destroys the wet/dry sequence"):
        repair.repair_field(
            _history(dates, values), "precip", pd.Timestamp("1998-05-19"),
            method=repair.METHOD_CLIMATOLOGY,
        )


def test_repair_field_falls_through_to_the_next_rung():
    """Interpolation cannot span the gap, so climatology takes it."""
    dates = pd.DatetimeIndex([pd.Timestamp(f"{y}-01-26") for y in (1984, 1985, 1986, 1987)])
    values = {cell: [10.0, 20.0, 30.0, -999.0] for cell in CELLS}

    filled, method = repair.repair_field(
        _history(dates, values), "tmin", pd.Timestamp("1987-01-26")
    )

    assert method.startswith(repair.METHOD_CLIMATOLOGY)
    assert filled[CELLS[0]] == pytest.approx(20.0)


def test_repair_field_prefers_interpolation_when_it_can():
    dates, values = _smooth_series()
    target = dates[7]
    history = _history(dates, values)
    history = history[history["date"] != target]

    _, method = repair.repair_field(history, "tmin", target)
    assert method.startswith(repair.METHOD_INTERPOLATE)


def test_repair_field_rejects_an_unknown_method():
    dates, values = _smooth_series()
    with pytest.raises(ValueError, match="unknown repair method"):
        repair.repair_field(_history(dates, values), "tmin", dates[7], method="vibes")


def test_repair_field_reports_unrepaired_rather_than_guessing():
    dates = pd.DatetimeIndex([pd.Timestamp("1987-01-26")])
    filled, method = repair.repair_field(
        _history(dates, {c: [1.0] for c in CELLS}), "tmin", pd.Timestamp("1987-01-26")
    )
    assert method == "unrepaired"
    assert filled.isna().all()


def test_interpolation_refuses_a_flagged_flanking_day_as_an_anchor():
    """Anchoring on a day that is itself corrupt would propagate the defect, not fix it."""
    dates, values = _smooth_series()
    target = dates[7]
    history = _history(dates, values)
    history.loc[history["date"] == dates[6], "value"] = -5.49  # the flanking day is bad too

    filled, method = repair.interpolate_temporal(
        history[history["date"] != target], target, exclude_dates=[dates[6]]
    )

    assert dates[6].strftime("%Y-%m-%d") not in method
    assert filled.notna().all()
    assert (filled > 15.0).all()  # the corrupt −5.49 never entered the average
