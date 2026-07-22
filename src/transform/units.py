"""Bronze (native ERA5) → silver unit conversions (PLANNING.md §5.1).

Bronze stores every value exactly as the source delivered it — Kelvin, metres,
J·m⁻² — for *both* backends (``src/cds/variables.py`` and ``src/gee/variables.py``
say so in their docstrings). This module is the promised "Step 4 converter": the one
place that turns those into the silver units of §8.2.

The import-time assertion below locks this table to the download contract, so adding a
variable on the download side without a conversion here fails immediately rather than
silently loading unconverted values.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from src.cds.variables import ALL_VARIABLES

T = TypeVar("T")  # a scalar, ndarray or Series — every conversion is elementwise

KELVIN_OFFSET = 273.15

# silver name -> native → silver conversion (§5.1). Wind components need none: they are
# m/s at 10 m in both layers; the sqrt(u²+v²) combination happens in merge.py.
CONVERSIONS: dict[str, Callable[[T], T]] = {
    "tmax": lambda v: v - KELVIN_OFFSET,     # K      -> °C
    "tmin": lambda v: v - KELVIN_OFFSET,     # K      -> °C
    "precip": lambda v: v * 1000.0,          # m      -> mm/day
    "srad": lambda v: v / 1e6,               # J/m²   -> MJ/m²/day
    "wind_u": lambda v: v,                   # m/s    -> m/s
    "wind_v": lambda v: v,                   # m/s    -> m/s
    "tdew": lambda v: v - KELVIN_OFFSET,     # K      -> °C
}

if set(CONVERSIONS) != set(ALL_VARIABLES):
    missing = set(ALL_VARIABLES) - set(CONVERSIONS)
    extra = set(CONVERSIONS) - set(ALL_VARIABLES)
    raise RuntimeError(
        "unit conversions drifted from the download contract "
        f"(missing={sorted(missing)}, extra={sorted(extra)}); see PLANNING.md §5.1"
    )


def convert(variable: str, values: T) -> T:
    """Apply the native → silver conversion for ``variable``."""
    try:
        return CONVERSIONS[variable](values)
    except KeyError:
        raise KeyError(
            f"no unit conversion for {variable!r}; known: {', '.join(ALL_VARIABLES)}"
        ) from None
