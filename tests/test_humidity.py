"""Tetens RH tests (PLANNING.md §12.1).

The ``es`` anchors are the published saturation-vapour-pressure values of FAO-56
Annex 2, Table 2.3 — an external check, not a restatement of the formula.
"""

import numpy as np

from src.transform.humidity import es, relative_humidity


def test_es_matches_fao56_table():
    # FAO-56 Annex 2 Table 2.3 (kPa), 3-decimal precision as printed.
    for t_c, expected in [(20.0, 2.338), (25.0, 3.168), (30.0, 4.243)]:
        assert abs(float(es(t_c)) - expected) < 0.002


def test_rh_is_100_when_dewpoint_equals_mean_temperature():
    rh = relative_humidity(tmax=25.0, tmin=15.0, tdew=20.0)  # tmean == tdew == 20
    assert abs(float(rh) - 100.0) < 1e-9


def test_rh_below_100_for_drier_air():
    rh = float(relative_humidity(tmax=30.0, tmin=10.0, tdew=5.0))  # tmean 20, tdew 5
    assert 0.0 < rh < 100.0
    # es(5)/es(20) = 0.872/2.338
    assert abs(rh - 100.0 * float(es(5.0)) / float(es(20.0))) < 1e-9


def test_rh_clamped_to_100_when_dewpoint_exceeds_mean():
    # Independent ERA5 aggregates can imply supersaturation; the clamp is physical.
    assert float(relative_humidity(tmax=12.0, tmin=8.0, tdew=15.0)) == 100.0


def test_nan_propagates():
    out = relative_humidity(
        tmax=np.array([25.0, np.nan]),
        tmin=np.array([15.0, 15.0]),
        tdew=np.array([20.0, 20.0]),
    )
    assert not np.isnan(out[0])
    assert np.isnan(out[1])


def test_vectorized_matches_scalar():
    tmax = np.array([25.0, 30.0, 12.0])
    tmin = np.array([15.0, 10.0, 8.0])
    tdew = np.array([20.0, 5.0, 15.0])
    vector = relative_humidity(tmax, tmin, tdew)
    scalars = [float(relative_humidity(a, b, c)) for a, b, c in zip(tmax, tmin, tdew)]
    assert np.allclose(vector, scalars)
