"""The GEE bronze download variable contract (PLANNING.md §5.1, GEE edition).

Mirror of ``src/cds/variables.py``: maps each **silver** variable name to its band in the
``ECMWF/ERA5/HOURLY`` collection and the reducer used to aggregate the hourly band to a
daily value. The silver keys are identical to the CDS contract so nothing downstream ever
branches on which backend produced a file.

Two deliberate differences from the CDS side, both correctness wins (§5.2):

- ``tmax`` / ``tmin`` are the **max / min of hourly ``temperature_2m``**, computed by us.
  This sidesteps the biased ``maximum_2m_temperature_since_previous_post_processing``
  parameter entirely.
- ``precip`` / ``srad`` are **summed** over the local-day window. In ``ECMWF/ERA5/HOURLY``
  the ``total_precipitation`` / ``surface_solar_radiation_downwards`` bands are **per-hour
  increments** (1-hour accumulations), *not* running totals since 00 UTC — verified live by
  sampling consecutive hours — so summing them over the day gives the correct daily total.
  (This is unlike ERA5-Land, whose accumulations reset at 00 UTC.)

Bronze stores the **source** value unconverted (Kelvin / metres / J·m⁻²); unit
conversions remain Step 4 (§7).

Band names are the one thing to confirm against the live catalog before a real backfill
(see ``docs/step3b_bronze_download_gee.md`` → Verification): GEE occasionally renames bands
between dataset revisions. This module is the single place to adjust if they differ.
"""

from __future__ import annotations

from dataclasses import dataclass

# 0.25° ERA5 hourly — the same grid as src/grid/spec.py, so child_id codes match the CDS
# bronze exactly. NOT ECMWF/ERA5_LAND/HOURLY (0.1°, which would break the grid spec).
# The asset id uses a slash (ECMWF/ERA5/HOURLY), not an underscore — verified live.
COLLECTION = "ECMWF/ERA5/HOURLY"


@dataclass(frozen=True)
class GeeVariableSpec:
    """How one silver variable is fetched from ``ECMWF/ERA5/HOURLY``.

    ``reducer`` is one of ``{"max", "min", "sum", "mean"}`` — the daily aggregation applied
    to ``band`` over the local-day window.
    """

    band: str
    reducer: str


# silver name -> GEE spec (§5.1). Order is the natural download order; keys must match
# src/cds/variables.py VARIABLES so the two backends are interchangeable.
VARIABLES: dict[str, GeeVariableSpec] = {
    "tmax": GeeVariableSpec("temperature_2m", "max"),
    "tmin": GeeVariableSpec("temperature_2m", "min"),
    "precip": GeeVariableSpec("total_precipitation", "sum"),
    "srad": GeeVariableSpec("surface_solar_radiation_downwards", "sum"),
    "wind_u": GeeVariableSpec("u_component_of_wind_10m", "mean"),
    "wind_v": GeeVariableSpec("v_component_of_wind_10m", "mean"),
    "tdew": GeeVariableSpec("dewpoint_temperature_2m", "mean"),
}

ALL_VARIABLES: tuple[str, ...] = tuple(VARIABLES)

_VALID_REDUCERS = frozenset({"max", "min", "sum", "mean"})


def variable_spec(silver_name: str) -> GeeVariableSpec:
    """Return the GEE spec for a silver variable name, or raise if unknown."""
    try:
        return VARIABLES[silver_name]
    except KeyError:
        raise KeyError(
            f"unknown variable {silver_name!r}; known: {', '.join(ALL_VARIABLES)}"
        ) from None
