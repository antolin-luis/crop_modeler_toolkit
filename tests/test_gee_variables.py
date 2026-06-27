"""GEE variable contract tests — band/reducer mapping and parity with the CDS contract.

The two backends must expose the identical silver-variable keys so nothing downstream
branches on which one produced a bronze file.
"""

from src.cds.variables import VARIABLES as CDS_VARIABLES
from src.gee.variables import (
    ALL_VARIABLES,
    COLLECTION,
    VARIABLES,
    _VALID_REDUCERS,
    variable_spec,
)


def test_silver_keys_match_cds():
    assert set(VARIABLES) == set(CDS_VARIABLES)


def test_collection_is_quarter_degree_era5():
    # Must be 0.25° ERA5, NOT ERA5-Land (0.1°), to keep child_id codes compatible.
    assert COLLECTION == "ECMWF/ERA5_HOURLY"


def test_every_spec_is_well_formed():
    for name in ALL_VARIABLES:
        spec = variable_spec(name)
        assert spec.band
        assert spec.reducer in _VALID_REDUCERS


def test_temperature_extremes_from_hourly_t2m():
    # tmax/tmin computed by us from hourly temperature_2m (sidesteps the biased
    # *_since_previous_post_processing parameter, §5.2).
    assert variable_spec("tmax") == type(variable_spec("tmax"))("temperature_2m", "max")
    assert variable_spec("tmin").band == "temperature_2m"
    assert variable_spec("tmin").reducer == "min"


def test_accumulated_fields_use_increment_band_and_sum():
    assert variable_spec("precip") == type(variable_spec("precip"))(
        "total_precipitation_hourly", "sum"
    )
    assert variable_spec("srad").band == "surface_solar_radiation_downwards_hourly"
    assert variable_spec("srad").reducer == "sum"


def test_unknown_variable_raises():
    import pytest

    with pytest.raises(KeyError):
        variable_spec("nope")
