"""First-pass sowing-date estimate from a field's NDVI series (heuristic, no weather yet).

A winter cereal leaves a characteristic autumn trace:

    previous cover (crop / catch crop / lucerne)  ->  removal  ->  bare seedbed  ->  green-up

Sowing happens after the last cover is removed and before NDVI rises above bare soil for
good. Volunteers and catch crops green up in August/September too and are then tilled in,
so the crop's green-up is the rise after the **last** bare-soil date, not the first rise.
Per field and season:

- ``removed`` R: the last date with NDVI >= ``cover_ndvi`` that is later followed by bare
  soil (<= ``bare_ndvi``) before ``bare_end``. Defaults to ``season_start``.
- ``bare`` b: the lowest NDVI after R and up to ``bare_end`` — the seedbed's soil signal.
- ``last_bare`` L: the last date up to ``bare_end`` with NDVI <= b + ``rise``.
- ``greenup`` G: the first date after L with NDVI > b + ``rise``.
- Sowing window: ``[R + 1 day, G - lag_min]``. Point estimate: ``max(R + 1, G - lag_typ)``.

Seeds sit underground on a bare-looking field until emergence, so sowing can fall before L;
only G bounds it. The lags stand in for sowing -> emergence -> first detectable leaves,
roughly 10-25 days in a central-European autumn. They are fixed constants here; a
growing-degree-day version from the cell's ERA5 temperature is the natural next step.

Only dates with ``clear_frac >= min_clear`` are used — a median from 5 % of a field is not
a field. A date more than ``spike`` below *both* neighbours, with those neighbours at most
``spike_gap_days`` apart, is dropped as residual cloud shadow — dense sampling around it
shows it is transient. Across a long cloud gap a lone low date is kept: it may be the
only look at the bare seedbed. ``bare_end`` keeps late-autumn dips
(frost, snow, low sun) out of the bare search.

``confident`` is False when the estimate lands before ``earliest`` — no winter cereal is
sown in August here, so that means the series showed no clear bare-soil phase.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class Estimate:
    field_id: int
    removed: pd.Timestamp | None
    bare_ndvi: float | None
    last_bare: pd.Timestamp | None
    greenup: pd.Timestamp | None
    window_start: pd.Timestamp | None
    window_end: pd.Timestamp | None
    sowing: pd.Timestamp | None
    confident: bool = False


def clear_series(series: pd.DataFrame, start: str, end: str, min_clear: float) -> pd.DataFrame:
    s = series[
        (series["clear_frac"] >= min_clear)
        & (series["date"] >= start)
        & (series["date"] <= end)
    ]
    return s.sort_values("date").reset_index(drop=True)


def despike(s: pd.DataFrame, spike: float, gap_days: int) -> pd.DataFrame:
    """Drop interior dates more than ``spike`` below both close-by neighbours."""
    v = s["ndvi_median"].astype(float).to_numpy()
    d = list(s["date"])
    keep = [True] * len(v)
    for i in range(1, len(v) - 1):
        close = (d[i + 1] - d[i - 1]).days <= gap_days
        if close and v[i] < v[i - 1] - spike and v[i] < v[i + 1] - spike:
            keep[i] = False
    return s[keep].reset_index(drop=True)


def estimate_sowing(
    series: pd.DataFrame,
    field_id: int,
    *,
    season_start: str,
    season_end: str,
    bare_end: str,
    cover_ndvi: float = 0.45,
    bare_ndvi: float = 0.35,
    rise: float = 0.08,
    spike: float = 0.12,
    spike_gap_days: int = 30,
    min_clear: float = 0.5,
    lag_min: int = 10,
    lag_typ: int = 25,
    earliest: str | None = None,
) -> Estimate:
    """``series``: rows of one field with ``date``, ``ndvi_median``, ``clear_frac``."""
    none = Estimate(field_id, None, None, None, None, None, None, None)
    s = despike(
        clear_series(series, season_start, season_end, min_clear), spike, spike_gap_days
    )
    dates = list(s["date"])
    ndvi = list(s["ndvi_median"].astype(float))
    n_autumn = sum(d <= pd.Timestamp(bare_end) for d in dates)
    if n_autumn < 2:
        return none

    ri = max(
        (
            i for i in range(n_autumn)
            if ndvi[i] >= cover_ndvi
            and any(v <= bare_ndvi for v in ndvi[i + 1 : n_autumn])
        ),
        default=None,
    )
    start = ri + 1 if ri is not None else 0
    bare = min(ndvi[start:n_autumn])
    li = max(i for i in range(start, n_autumn) if ndvi[i] <= bare + rise)
    gi = next((i for i in range(li + 1, len(dates)) if ndvi[i] > bare + rise), None)
    if gi is None:
        return none

    removed = dates[ri] if ri is not None else pd.Timestamp(season_start)
    w0 = removed + dt.timedelta(days=1)
    g = dates[gi]
    w1 = max(w0, g - dt.timedelta(days=lag_min))
    sow = max(w0, g - dt.timedelta(days=lag_typ))
    confident = earliest is None or sow >= pd.Timestamp(earliest)
    return Estimate(field_id, removed, bare, dates[li], g, w0, w1, sow, confident)
