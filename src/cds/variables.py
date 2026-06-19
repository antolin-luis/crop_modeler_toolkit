"""The bronze download variable contract (PLANNING.md §5.1).

Single source of truth mapping each silver variable name to its CDS ``variable`` and
``daily_statistic``. Imported by ``src/cds/download.py`` now and by Step 4's converter
later, so the download and transform sides can never drift apart.

Bronze stores the **source** value unconverted; unit conversions are Step 4's job (§7).

Use ``2m_temperature`` extremes for ``tmax`` / ``tmin`` — **never**
``maximum_2m_temperature_since_previous_post_processing`` or its minimum counterpart:
they carry a forecast cold bias and have an open data-quality issue as of June 2026 (§5.2).
"""

from __future__ import annotations

from dataclasses import dataclass

# Daily aggregation is computed on retrieval from this single dataset (§5).
DATASET = "derived-era5-single-levels-daily-statistics"


@dataclass(frozen=True)
class VariableSpec:
    """How one silver variable is requested from the CDS."""

    cds_variable: str
    daily_statistic: str


# silver name -> request spec (§5.1). Order is the natural download order.
VARIABLES: dict[str, VariableSpec] = {
    "tmax": VariableSpec("2m_temperature", "daily_maximum"),
    "tmin": VariableSpec("2m_temperature", "daily_minimum"),
    "precip": VariableSpec("total_precipitation", "daily_sum"),
    "srad": VariableSpec("surface_solar_radiation_downwards", "daily_sum"),
    "wind_u": VariableSpec("10m_u_component_of_wind", "daily_mean"),
    "wind_v": VariableSpec("10m_v_component_of_wind", "daily_mean"),
    "tdew": VariableSpec("2m_dewpoint_temperature", "daily_mean"),
}

ALL_VARIABLES: tuple[str, ...] = tuple(VARIABLES)


def variable_spec(silver_name: str) -> VariableSpec:
    """Return the request spec for a silver variable name, or raise if unknown."""
    try:
        return VARIABLES[silver_name]
    except KeyError:
        raise KeyError(
            f"unknown variable {silver_name!r}; known: {', '.join(ALL_VARIABLES)}"
        ) from None
