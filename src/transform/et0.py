"""FAO-56 Penman-Monteith reference evapotranspiration (PLANNING.md §12.2).

``et0`` in this project is **ET0**, the FAO-56 reference crop ET (mm/day) — *not* ERA5's
``pev``/``e`` (§5.1). Everything here is pure numpy and elementwise, so a whole
parent-batch of cell-days is one vectorized call.

Chapter/equation numbers below refer to FAO-56 (Allen et al., 1998), the authoritative
source for every constant. The inputs come from the wide silver frame plus two static
per-cell fields from ``era5_land_base_grid``: ``lat`` (for extraterrestrial radiation)
and ``elevation`` (for atmospheric pressure and clear-sky radiation).

**Accepted simplification (§12.2):** ``u2`` is derived from the *daily-mean* wind
components, ``sqrt(mean(u)² + mean(v)²) * 0.748``. That underestimates true mean wind
speed (Jensen gap), but ET0 is weakly sensitive to wind and the alternative is a
calibration step; the bias is documented rather than corrected.

NaN in any input propagates to NaN in the output — a cell with no ``elevation`` gets a
NULL ``et0`` rather than a wrong one (§8.3).
"""

from __future__ import annotations

import numpy as np

from src.transform.humidity import es

# --- FAO-56 constants ------------------------------------------------------------
SOLAR_CONSTANT = 0.0820          # Gsc, MJ m-2 min-1 (eq. 28)
STEFAN_BOLTZMANN = 4.903e-9      # sigma, MJ K-4 m-2 day-1 (eq. 39)
ALBEDO = 0.23                    # hypothetical grass reference crop (eq. 38)
WIND_10M_TO_2M = 0.748           # 10 m -> 2 m log-profile factor (§12.2)
SOIL_HEAT_FLUX_DAILY = 0.0       # G ~ 0 for a daily timestep (eq. 42)


def atm_pressure(elevation):
    """Atmospheric pressure (kPa) from elevation (m) — eq. 7."""
    z = np.asarray(elevation, dtype=np.float64)
    return 101.3 * ((293.0 - 0.0065 * z) / 293.0) ** 5.26


def psychrometric_gamma(pressure):
    """Psychrometric constant (kPa/°C) from pressure (kPa) — eq. 8."""
    return 0.000665 * np.asarray(pressure, dtype=np.float64)


def delta_svp(tmean):
    """Slope of the saturation vapour pressure curve (kPa/°C) at ``tmean`` — eq. 13."""
    t = np.asarray(tmean, dtype=np.float64)
    return 4098.0 * es(t) / (t + 237.3) ** 2


def extraterrestrial_radiation(lat_deg, doy):
    """Extraterrestrial radiation Ra (MJ m-2 day-1) — eq. 21, 23, 24, 25.

    Deterministic function of latitude and day-of-year. The sunset hour angle is
    clamped to the ``arccos`` domain so polar day/night does not produce NaN.
    """
    phi = np.deg2rad(np.asarray(lat_deg, dtype=np.float64))
    j = np.asarray(doy, dtype=np.float64)

    dr = 1.0 + 0.033 * np.cos(2.0 * np.pi * j / 365.0)          # eq. 23
    decl = 0.409 * np.sin(2.0 * np.pi * j / 365.0 - 1.39)       # eq. 24
    ws = np.arccos(np.clip(-np.tan(phi) * np.tan(decl), -1.0, 1.0))  # eq. 25

    return (
        (24.0 * 60.0 / np.pi)
        * SOLAR_CONSTANT
        * dr
        * (ws * np.sin(phi) * np.sin(decl) + np.cos(phi) * np.cos(decl) * np.sin(ws))
    )  # eq. 21


def clear_sky_radiation(ra, elevation):
    """Clear-sky solar radiation Rso (MJ m-2 day-1) — eq. 37."""
    z = np.asarray(elevation, dtype=np.float64)
    return (0.75 + 2e-5 * z) * np.asarray(ra, dtype=np.float64)


def net_shortwave(rs):
    """Net shortwave radiation Rns (MJ m-2 day-1) — eq. 38."""
    return (1.0 - ALBEDO) * np.asarray(rs, dtype=np.float64)


def net_longwave(tmax, tmin, ea, rs, rso):
    """Net outgoing longwave radiation Rnl (MJ m-2 day-1) — eq. 39.

    ``Rs/Rso`` is capped at 1.0 as FAO-56 requires; ERA5's radiation and the
    latitude-derived clear-sky value are independent estimates, so the ratio can
    marginally exceed unity on clear days.
    """
    tmax_k = np.asarray(tmax, dtype=np.float64) + 273.16
    tmin_k = np.asarray(tmin, dtype=np.float64) + 273.16
    ea = np.asarray(ea, dtype=np.float64)

    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.clip(np.asarray(rs) / np.asarray(rso), 0.0, 1.0)

    return (
        STEFAN_BOLTZMANN
        * ((tmax_k**4 + tmin_k**4) / 2.0)
        * (0.34 - 0.14 * np.sqrt(np.clip(ea, 0.0, None)))
        * (1.35 * ratio - 0.35)
    )


def net_radiation(tmax, tmin, srad, ea, lat_deg, doy, elevation):
    """Net radiation Rn (MJ m-2 day-1) — eq. 40 (``Rn = Rns - Rnl``)."""
    ra = extraterrestrial_radiation(lat_deg, doy)
    rso = clear_sky_radiation(ra, elevation)
    return net_shortwave(srad) - net_longwave(tmax, tmin, ea, srad, rso)


def wind_2m(wind10):
    """Wind speed at 2 m (m/s) from the 10 m speed — §12.2."""
    return np.asarray(wind10, dtype=np.float64) * WIND_10M_TO_2M


def et0_fao56(tmax, tmin, srad, wind10, tdew, elevation, lat_deg, doy):
    """Daily reference ET0 (mm/day) — FAO-56 eq. 6.

    Units in: ``tmax``/``tmin``/``tdew`` °C, ``srad`` MJ m-2 day-1, ``wind10`` m/s at
    10 m, ``elevation`` m, ``lat_deg`` degrees (−90..90), ``doy`` 1..366 — i.e. exactly
    the silver-unit columns of ``wth_base`` plus the two static grid fields.
    """
    tmax = np.asarray(tmax, dtype=np.float64)
    tmin = np.asarray(tmin, dtype=np.float64)
    tmean = (tmax + tmin) / 2.0

    # Vapour pressures: es from the extremes (eq. 12 — NOT es(Tmean)), ea from dewpoint.
    es_mean = (es(tmax) + es(tmin)) / 2.0
    ea = es(tdew)

    delta = delta_svp(tmean)
    gamma = psychrometric_gamma(atm_pressure(elevation))
    u2 = wind_2m(wind10)
    rn = net_radiation(tmax, tmin, srad, ea, lat_deg, doy, elevation)

    numerator = 0.408 * delta * (rn - SOIL_HEAT_FLUX_DAILY) + gamma * (
        900.0 / (tmean + 273.0)
    ) * u2 * (es_mean - ea)
    denominator = delta + gamma * (1.0 + 0.34 * u2)
    return numerator / denominator
