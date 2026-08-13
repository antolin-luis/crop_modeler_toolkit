"""FAO-56 Penman-Monteith tests (PLANNING.md §12.2).

Sub-steps are pinned to **published FAO-56 values** (Chapter 3 worked examples and the
Annex 2 tables), which is a real external check. The end-to-end equation has no published
anchor reproduced here — the worked example's full input set is not restated in the repo —
so it is covered by physical bounds and by the response direction to each driver, which
catches sign errors, unit slips and swapped terms.
"""

import numpy as np
import pytest

from src.transform.et0 import (
    ET0_NEGATIVE_TOLERANCE,
    atm_pressure,
    clear_sky_radiation,
    delta_svp,
    et0_fao56,
    extraterrestrial_radiation,
    net_longwave,
    net_radiation,
    net_shortwave,
    psychrometric_gamma,
    snap_negative_et0,
    wind_2m,
)

# A warm, sunny, moderately humid day — the reference case the behaviour tests perturb.
BASE = dict(
    tmax=30.0, tmin=18.0, srad=22.0, wind10=3.0, tdew=15.0,
    elevation=50.0, lat_deg=-34.9, doy=15,
)


def test_atmospheric_pressure_matches_fao56_example_2():
    # FAO-56 Example 2: elevation 1800 m -> P = 81.8 kPa, gamma = 0.054 kPa/°C.
    p = float(atm_pressure(1800.0))
    assert abs(p - 81.8) < 0.05
    assert abs(float(psychrometric_gamma(p)) - 0.054) < 0.0005


def test_delta_matches_fao56_annex2_table():
    # FAO-56 Annex 2 Table 2.4, slope of the SVP curve (kPa/°C).
    for t_c, expected in [(20.0, 0.145), (25.0, 0.189), (30.0, 0.243)]:
        assert abs(float(delta_svp(t_c)) - expected) < 0.001


def test_extraterrestrial_radiation_matches_fao56_example_8():
    # FAO-56 Example 8: 3 September (doy 246) at 20°S -> Ra = 32.2 MJ m-2 day-1.
    assert abs(float(extraterrestrial_radiation(-20.0, 246)) - 32.2) < 0.05


def test_ra_seasonality_flips_across_the_equator():
    ra_north = extraterrestrial_radiation(45.0, 172)   # northern summer solstice
    ra_south = extraterrestrial_radiation(-45.0, 172)  # southern winter
    assert float(ra_north) > float(ra_south)


def test_ra_is_zero_in_polar_night_not_nan():
    ra = float(extraterrestrial_radiation(85.0, 355))  # 85°N in December
    assert np.isfinite(ra)
    assert ra == pytest.approx(0.0, abs=1e-9)


def test_clear_sky_and_net_shortwave():
    ra = 30.0
    assert float(clear_sky_radiation(ra, 0.0)) == pytest.approx(22.5)      # 0.75 * Ra
    assert float(clear_sky_radiation(ra, 1000.0)) == pytest.approx(23.1)   # +2e-5 * z
    assert float(net_shortwave(20.0)) == pytest.approx(15.4)               # (1 - 0.23) * Rs


def test_net_longwave_capped_at_clear_sky_ratio():
    # Rs above Rso must not push the cloudiness factor past its clear-sky value.
    clear = float(net_longwave(30.0, 18.0, 1.7, rs=22.5, rso=22.5))
    over = float(net_longwave(30.0, 18.0, 1.7, rs=25.0, rso=22.5))
    assert clear == pytest.approx(over)
    # Overcast (low Rs/Rso) suppresses longwave loss.
    cloudy = float(net_longwave(30.0, 18.0, 1.7, rs=8.0, rso=22.5))
    assert 0.0 < cloudy < clear


def test_net_radiation_below_incoming_shortwave():
    rn = float(net_radiation(30.0, 18.0, 22.0, 1.7, -34.9, 15, 50.0))
    assert 0.0 < rn < float(net_shortwave(22.0))


def test_wind_10m_to_2m_factor():
    assert float(wind_2m(10.0)) == pytest.approx(7.48)


def test_et0_in_physical_range():
    et0 = float(et0_fao56(**BASE))
    assert 2.0 < et0 < 12.0


def _perturbed(**overrides):
    return float(et0_fao56(**{**BASE, **overrides}))


def test_et0_increases_with_radiation():
    assert _perturbed(srad=8.0) < _perturbed() < _perturbed(srad=30.0)


def test_et0_increases_with_wind_when_air_is_dry():
    assert _perturbed(wind10=0.5) < _perturbed() < _perturbed(wind10=8.0)


def test_et0_decreases_as_air_approaches_saturation():
    humid = _perturbed(tdew=23.9)  # dewpoint at the mean temperature -> VPD ~ 0
    assert humid < _perturbed()


def test_et0_missing_elevation_yields_nan_not_a_wrong_value():
    # §8.3: a missing input degrades the derived value to NULL, never to a guess.
    assert np.isnan(_perturbed(elevation=np.nan))


def test_et0_vectorized_matches_scalar():
    n = 4
    kwargs = {k: np.full(n, v) for k, v in BASE.items()}
    kwargs["doy"] = np.array([15, 105, 196, 288])
    vector = et0_fao56(**kwargs)
    scalars = [
        float(et0_fao56(**{**BASE, "doy": int(d)})) for d in kwargs["doy"]
    ]
    assert np.allclose(vector, scalars)


# --- negative-ET0 clamp (§8.4) ---------------------------------------------------------


def test_saturation_edge_case_clamps_to_zero_instead_of_quarantining():
    """The real 2010-07-18 rows — the only `et0<0` quarantine in the archive.

    Saturated (rh 100), near-zero radiation, breezy: FAO-56 genuinely returns a small
    negative. The physical answer is zero evaporative demand, so these must load rather
    than fail the `et0<0` check. Inputs and pre-clamp outputs are the live quarantine rows
    for cells F6HX / F6HY / F7LW / F7LX.
    """
    observed = [
        # tmax, tmin, srad, wind10, tdew, lat, elevation, et0 before the clamp
        (12.832911, 6.754785, 0.5872401, 6.9082646, 10.951593, -32.75, 225.0078, -0.0427),
        (12.763147, 7.254785, 0.5949299, 6.6830950, 11.092463, -32.75, 219.3460, -0.0234),
        (12.600763, 5.7018676, 0.5805041, 7.5639670, 10.374140, -33.00, 209.9957, -0.0638),
        (12.838343, 6.0704894, 0.5700210, 6.8585196, 10.677698, -33.00, 174.3100, -0.0434),
    ]
    for tmax, tmin, srad, wind10, tdew, lat, elevation, pre_clamp in observed:
        value = float(et0_fao56(tmax=tmax, tmin=tmin, srad=srad, wind10=wind10, tdew=tdew,
                                elevation=elevation, lat_deg=lat, doy=199))
        assert value == 0.0
        # The equation itself is unchanged — these sit inside the clamp band, which is
        # why the clamp (not a different ET0) is what lets them load.
        assert -ET0_NEGATIVE_TOLERANCE < pre_clamp < 0.0


def test_snap_leaves_positive_and_large_negative_values_alone():
    out = snap_negative_et0(np.array([2.5, -0.023, -0.064, -1.5, 0.0]))
    # Sub-tolerance negatives become zero; a large negative survives to fail QA.
    assert np.array_equal(out, np.array([2.5, 0.0, 0.0, -1.5, 0.0]))
    assert ET0_NEGATIVE_TOLERANCE == 0.1


def test_snap_preserves_nan():
    # §8.3: a missing input stays NULL, it does not become a zero.
    assert np.isnan(snap_negative_et0(np.array([np.nan]))[0])
