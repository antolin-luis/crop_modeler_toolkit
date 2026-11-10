"""Repair ladder for field-level defects (PLANNING.md §8.4).

Every function here returns ``(values, method)`` and **writes nothing**. Persisting a
repair — setting the ``imputed`` bits, logging the original value, moving the registry row
— belongs to the ``repair_silver`` DAG, so the estimators stay pure and testable and no
code path can impute as a side effect.

**The ladder is variable-aware, because the variables do not behave alike.**

Temperature is smooth and autocorrelated: the flanking days genuinely carry the synoptic
state, so interpolation recovers something close to what was observed. On 1987-01-26 the
neighbours sit at 21.2–22.5 °C inside a cool overcast spell, and a day-of-year mean would
erase exactly that.

Precipitation is not like that. It is zero-inflated, heavy-tailed and near-uncorrelated day
to day, so both interpolation *and* averaging are wrong for it. A day/month mean is the
worst of the two: a wet-season month here is ~15 rain days out of 31 with most of the total
in a few events, so the mean describes no day that ever occurred. Filling with it yields a
drizzle-every-day series that never triggers runoff, never saturates and never dries below
wilting point — quietly biasing DSSAT's soil water balance, planting-date logic and
consecutive-dry-day drought stress. So precip gets :func:`analog_day_fill`, which borrows a
whole real day instead of computing an average of many.

The eventual first rung for precip is CHIRPS — an independent gauge+satellite product,
not an estimate at all. It is not wired here (its download path is a separate branch), and
neither is a cross-backend ERA5 re-fetch: no date-scoped download exists in either backend
(the CDS splitter floors at one month, GEE exports whole years). Until one does, a repaired
issue is marked ``refetch_pending`` so the debt stays visible and the log stays reversible.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

METHOD_INTERPOLATE = "interpolate_temporal"
METHOD_CLIMATOLOGY = "climatology_fill"
METHOD_ANALOG_DAY = "analog_day"

# Ordered rungs per variable. Anything not listed uses SMOOTH_LADDER. `precip` deliberately
# has no averaging rung at all — see the module docstring.
SMOOTH_LADDER = (METHOD_INTERPOLATE, METHOD_CLIMATOLOGY)
LADDER: dict[str, tuple[str, ...]] = {
    "tmax": SMOOTH_LADDER,
    "tmin": SMOOTH_LADDER,
    "tdew": SMOOTH_LADDER,
    "srad": SMOOTH_LADDER,
    "wind": SMOOTH_LADDER,
    "precip": (METHOD_ANALOG_DAY,),
}

# Methods that produce a value by averaging several days. Used to keep `precip` away from
# them structurally rather than by convention.
AVERAGING_METHODS = frozenset({METHOD_CLIMATOLOGY})

# Rungs that need the same day-of-year across *every* year, rather than the days either
# side of the target. A caller that reads history from a database wants to know this: the
# day-of-year read cannot be served by a date index and is far more expensive.
DOY_METHODS = frozenset({METHOD_CLIMATOLOGY, METHOD_ANALOG_DAY})


def needs_doy_history(variable: str, method: str = "auto") -> bool:
    """Whether repairing ``variable`` may reach a rung that needs multi-year history."""
    rungs = ladder_for(variable) if method == "auto" else (method,)
    return bool(set(rungs) & DOY_METHODS)

MAX_INTERPOLATION_GAP_DAYS = 3
CLIMATOLOGY_WINDOW_DAYS = 7


def ladder_for(variable: str) -> tuple[str, ...]:
    """Ordered repair methods for ``variable``."""
    return LADDER.get(variable, SMOOTH_LADDER)


def _pivot(history: pd.DataFrame) -> pd.DataFrame:
    """``[child_id, date, value]`` -> cells x dates, so every rung is a column lookup."""
    frame = history.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.pivot_table(index="child_id", columns="date", values="value", aggfunc="first")


def _doy_distance(dates: pd.DatetimeIndex, target: pd.Timestamp) -> np.ndarray:
    """Day-of-year distance, wrapping across the new year."""
    delta = np.abs(dates.dayofyear.to_numpy() - target.dayofyear)
    return np.minimum(delta, 365 - delta)


def interpolate_temporal(
    history: pd.DataFrame,
    target_date,
    *,
    max_gap_days: int = MAX_INTERPOLATION_GAP_DAYS,
    exclude_dates=(),
) -> tuple[pd.Series, str]:
    """Linear interpolation per cell between the nearest clean flanking days.

    Only for isolated gaps: beyond ``max_gap_days`` on either side the flanking days no
    longer describe the missing day's weather and the caller should fall to the next rung.
    Cells without a usable flank on both sides come back as NaN rather than as a guess.

    ``exclude_dates`` keeps a *known-bad* day from becoming an anchor — anchoring the
    1987-01-26 repair on a flanking day that is itself flagged would propagate the defect
    instead of removing it.
    """
    target = pd.Timestamp(target_date)
    wide = _pivot(history)
    dates = wide.columns

    excluded = [pd.Timestamp(d) for d in exclude_dates] + [target]
    dates = dates[~dates.isin(excluded)]

    before = dates[(dates < target) & (dates >= target - pd.Timedelta(days=max_gap_days))]
    after = dates[(dates > target) & (dates <= target + pd.Timedelta(days=max_gap_days))]
    if before.empty or after.empty:
        return pd.Series(np.nan, index=wide.index, name="value"), METHOD_INTERPOLATE

    prev_date, next_date = before.max(), after.min()
    prev_values, next_values = wide[prev_date], wide[next_date]

    span = (next_date - prev_date).days
    weight = (target - prev_date).days / span
    values = prev_values + (next_values - prev_values) * weight

    method = f"{METHOD_INTERPOLATE}({prev_date.date()}..{next_date.date()})"
    return values.rename("value"), method


def climatology_fill(
    history: pd.DataFrame,
    target_date,
    *,
    window_days: int = CLIMATOLOGY_WINDOW_DAYS,
    exclude_dates=(),
) -> tuple[pd.Series, str]:
    """Per-cell mean over the same day-of-year ±``window_days`` in other years.

    The last rung, and only for gaps too long to interpolate: averaging flattens variance,
    and a DSSAT run fed enough climatological days simulates an average season that never
    occurred. ``exclude_dates`` keeps known-bad days out of their own climatology.
    """
    target = pd.Timestamp(target_date)
    wide = _pivot(history)
    dates = wide.columns

    excluded = {pd.Timestamp(d) for d in exclude_dates} | {target}
    in_window = (_doy_distance(dates, target) <= window_days) & (dates.year != target.year)
    usable = dates[in_window & ~dates.isin(list(excluded))]
    if usable.empty:
        return pd.Series(np.nan, index=wide.index, name="value"), METHOD_CLIMATOLOGY

    values = wide[usable].mean(axis=1)
    method = f"{METHOD_CLIMATOLOGY}(n={len(usable)})"
    return values.rename("value"), method


def analog_day_fill(
    history: pd.DataFrame,
    target_date,
    *,
    window_days: int = CLIMATOLOGY_WINDOW_DAYS,
    exclude_dates=(),
    seed: int = 0,
) -> tuple[pd.Series, str]:
    """Borrow one whole observed day from the same day-of-year window in other years.

    **One donor day for every cell, not an independent draw per cell.** Rainfall is
    spatially correlated — a real day has fronts and dry sectors — and drawing per cell
    would shred that into spatially incoherent noise, which is no more physical than a
    mean. Taking one real day preserves both the wet/dry structure in time and the coherent
    field in space.

    The draw is seeded, so re-running a repair reproduces it exactly and the method string
    names the day that was borrowed.
    """
    target = pd.Timestamp(target_date)
    wide = _pivot(history)
    dates = wide.columns

    excluded = {pd.Timestamp(d) for d in exclude_dates} | {target}
    in_window = (_doy_distance(dates, target) <= window_days) & (dates.year != target.year)
    candidates = dates[in_window & ~dates.isin(list(excluded))]
    if candidates.empty:
        return pd.Series(np.nan, index=wide.index, name="value"), METHOD_ANALOG_DAY

    rng = np.random.default_rng(seed)
    donor = candidates[int(rng.integers(len(candidates)))]

    method = f"{METHOD_ANALOG_DAY}({donor.date()})"
    return wide[donor].rename("value"), method


_ESTIMATORS = {
    METHOD_INTERPOLATE: interpolate_temporal,
    METHOD_CLIMATOLOGY: climatology_fill,
    METHOD_ANALOG_DAY: analog_day_fill,
}


def repair_field(
    history: pd.DataFrame,
    variable: str,
    target_date,
    *,
    method: str = "auto",
    exclude_dates=(),
    seed: int = 0,
) -> tuple[pd.Series, str]:
    """Walk ``variable``'s ladder until a rung produces values; return ``(values, method)``.

    ``method="auto"`` uses :func:`ladder_for`, which is what keeps `precip` away from an
    averaging rung structurally rather than by convention. Naming a method explicitly
    overrides the ladder but still refuses an averaging method for `precip`.
    """
    if method != "auto" and method not in _ESTIMATORS:
        raise ValueError(f"unknown repair method {method!r}; known: {', '.join(_ESTIMATORS)}")

    rungs = ladder_for(variable) if method == "auto" else (method,)

    for rung in rungs:
        if variable == "precip" and rung in AVERAGING_METHODS:
            raise ValueError(
                f"{rung!r} averages several days, which destroys the wet/dry sequence "
                "DSSAT integrates over; precip repairs use analog-day resampling"
            )

        kwargs = {"exclude_dates": exclude_dates}
        if rung == METHOD_ANALOG_DAY:
            kwargs["seed"] = seed

        values, applied = _ESTIMATORS[rung](history, target_date, **kwargs)
        if values.notna().any():
            return values, applied

    return pd.Series(np.nan, index=_pivot(history).index, name="value"), "unrepaired"
