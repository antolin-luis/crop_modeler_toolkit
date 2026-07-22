"""Relative humidity from dewpoint via Tetens (PLANNING.md §12.1).

ERA5 gives dewpoint, not RH, so RH is derived once the per-variable parquets are merged
wide (§8.3). ``es`` is also the saturation-vapour-pressure primitive FAO-56 needs, so
``src/transform/et0.py`` imports it from here rather than redefining it.

Everything is numpy-elementwise: NaN in any input propagates to NaN out, which is the
graceful-degradation contract of §8.3 (a missing input yields a NULL derived value, not a
dropped row).
"""

from __future__ import annotations

import numpy as np

RH_MIN = 0.0
RH_MAX = 100.0


def es(t_c):
    """Saturation vapour pressure (kPa) at temperature ``t_c`` (°C) — Tetens (§12.1).

    ``es(T) = 0.6108 * exp(17.27 * T / (T + 237.3))``
    """
    t = np.asarray(t_c, dtype=np.float64)
    return 0.6108 * np.exp(17.27 * t / (t + 237.3))


def relative_humidity(tmax, tmin, tdew):
    """Mean daily RH (%) from the daily temperature extremes and dewpoint (§12.1).

    ``rh = 100 * es(tdew) / es((tmax + tmin) / 2)``, clamped to [0, 100]. The clamp is
    physical, not cosmetic: dewpoint and the temperature extremes are separate ERA5
    fields, so their daily aggregates can imply a slightly supersaturated mean.
    """
    tmax = np.asarray(tmax, dtype=np.float64)
    tmin = np.asarray(tmin, dtype=np.float64)
    tmean = (tmax + tmin) / 2.0

    ea = es(tdew)
    rh = 100.0 * ea / es(tmean)
    # np.clip would turn NaN into NaN anyway, but be explicit that NaN survives.
    return np.where(np.isnan(rh), np.nan, np.clip(rh, RH_MIN, RH_MAX))
