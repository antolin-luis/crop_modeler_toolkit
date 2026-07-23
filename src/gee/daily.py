"""Server-side hourly→daily reduction on the local day (PLANNING.md §5.3).

Pure Earth Engine graph building — **no I/O, no ``getInfo``**. Given a silver variable and
a year, return an ``ee.ImageCollection`` of one image per **local** day, each the variable's
daily reducer applied to the hourly ``ECMWF/ERA5/HOURLY`` band over that day's window.

Local-day, not UTC (§5.3). GEE's own ``DAILY_AGGR`` aggregates on the UTC day, which would
mix a UTC-day precip total with a local-day temperature extreme. We instead reduce the
hourly collection over a shifted window: for offset ``o`` hours (Brazil/Uruguay = −3), local
day ``D`` covers UTC ``[D 00:00 − o, D+1 00:00 − o)`` — i.e. ``[D 03:00Z, D+1 03:00Z)`` for
−3.

**Per-cell offset (zones).** An extent spanning several countries has several offsets. Each
``zone`` is an ``(offset_hours, region)`` pair: reduce over that offset's shifted window and
``clip`` to its region. The zones are disjoint (they tile the extent by timezone), so
``mosaic``-ing them yields one daily image where every pixel used its own correct 24 h. The
zones come from the same tz shapefile that stamps ``era5_land_base_grid.t_zone``, so the
pixel a cell snaps to and the cell's recorded offset agree by construction. ``region=None``
means "whole extent, one offset" — the old single-timezone behavior.

The returned collection is lazy; the export step (``src/gee/export.py``) sets the region and
triggers the actual EECU compute. Keeping this module purely temporal makes it unit-testable
against a mocked ``ee``.
"""

from __future__ import annotations

import calendar
import re

import ee

from src.gee.variables import COLLECTION, variable_spec

_TZ_RE = re.compile(r"^UTC(?:([+-])(\d{2}):(\d{2}))?$")

_REDUCERS = {
    "max": lambda: ee.Reducer.max(),
    "min": lambda: ee.Reducer.min(),
    "sum": lambda: ee.Reducer.sum(),
    "mean": lambda: ee.Reducer.mean(),
}


def parse_offset_hours(timezone: str) -> float:
    """Parse a ``UTC±HH:MM`` local-day offset to signed hours (``UTC`` → ``0.0``).

    Mirrors the CDS ``time_zone`` param form so both backends take the identical string.
    """
    m = _TZ_RE.match(timezone.strip())
    if not m:
        raise ValueError(f"bad timezone {timezone!r}; expected 'UTC' or 'UTC±HH:MM'")
    sign, hh, mm = m.groups()
    if sign is None:
        return 0.0
    magnitude = int(hh) + int(mm) / 60.0
    return magnitude if sign == "+" else -magnitude


def _reducer(name: str):
    try:
        return _REDUCERS[name]()
    except KeyError:
        raise ValueError(f"unknown reducer {name!r}") from None


def build_daily_collection(
    variable: str, year: int, *, zones: list[tuple[float, "ee.Geometry | None"]]
) -> "ee.ImageCollection":
    """Build the per-local-day ``ee.ImageCollection`` for one ``(variable, year)``.

    ``zones`` is a non-empty list of ``(offset_hours, region)`` pairs — one per distinct UTC
    offset present in the extent. Each day's image is the mosaic of every zone's reduction,
    each reduced over its own shifted 24 h window and clipped to its region. A single zone
    with ``region=None`` reproduces the old whole-extent, single-offset behavior. Each image
    is single-band (the source band) and carries ``system:time_start`` and a ``date``
    (``YYYY-MM-DD``) property at the local-day start.
    """
    if not zones:
        raise ValueError("zones must be non-empty")

    spec = variable_spec(variable)
    reducer = _reducer(spec.reducer)
    hourly = ee.ImageCollection(COLLECTION).select(spec.band)

    year_start = ee.Date.fromYMD(year, 1, 1)
    n_days = 366 if calendar.isleap(year) else 365
    band = spec.band

    def _zone_image(local_start, offset_hours, region):
        win_start = local_start.advance(-offset_hours, "hour")
        win_end = win_start.advance(24, "hour")
        reduced = hourly.filterDate(win_start, win_end).reduce(reducer).rename(band)
        return reduced if region is None else reduced.clip(region)

    def _daily(day_index):
        day_index = ee.Number(day_index)
        local_start = year_start.advance(day_index, "day")
        images = [_zone_image(local_start, off, reg) for off, reg in zones]
        combined = (
            images[0]
            if len(images) == 1
            else ee.ImageCollection(images).mosaic().rename(band)
        )
        return combined.set(
            {
                "system:time_start": local_start.millis(),
                "date": local_start.format("YYYY-MM-dd"),
            }
        )

    days = ee.List.sequence(0, n_days - 1)
    return ee.ImageCollection(days.map(_daily))
