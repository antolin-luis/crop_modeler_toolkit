"""CHIRPS daily collections — the 0.05° precipitation sources.

The CHIRPS counterpart of ``src/gee/variables.py`` + ``src/gee/daily.py``, collapsed into one
module because CHIRPS needs almost none of what ERA5 needs: it ships as a **finished daily
product**, so there is no hourly stack to reduce, no reducer to pick per variable, and no
per-cell local-day window to build. ``chirps_daily`` only re-stamps one image per calendar
day so the export's band names carry dates, which is the sole contract ``start_export``
requires.

Pure graph construction — no ``getInfo``, no I/O — so it is unit-testable against a mocked
``ee``, the same property that makes ``tests/test_gee_daily.py`` work.

**Day definition (and why there are no zones).** ``wth_base.date`` is a *local* day: the
GEE backend derives a reduction window per timezone offset from ``GEE_TZ_ASSET`` and
mosaics them (PLANNING.md §5.3). CHIRPS has no sub-daily values left to re-window, so its
day is whatever the product defines and cannot be shifted after the fact — daily reduction
is lossy. The two ``date`` columns therefore mean slightly different 24-hour windows. That
is a real property of the data, recorded in ``wth_precip_alt_source.note`` and surfaced by
the comparison view; it is never fixed by pretending the columns align.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass

import ee

BAND = "precipitation"  # same band name across every version
UNITS = "mm/day"  # already the silver unit — no conversion anywhere on this path

# The catalog's stated valid range; used by the silver QA node as an upper bound.
MAX_PRECIP_MM = 1444.34


@dataclass(frozen=True)
class ChirpsSource:
    """One CHIRPS product.

    ``lat_bounds`` is the product's own coverage, not a request parameter — an extent
    reaching past it yields masked pixels rather than an error, which is worth catching at
    plan time instead of discovering as a hole in the data.
    """

    collection: str
    lat_bounds: tuple[float, float]  # (south, north)
    first_year: int
    note: str


# Note the org differs between versions — v2 is UCSB-CH**G**, v3 is UCSB-CH**C**. They are
# easy to transpose, and the wrong one fails late inside EE with an unhelpful message.
CHIRPS_SOURCES: dict[str, ChirpsSource] = {
    "chirps_v2": ChirpsSource(
        collection="UCSB-CHG/CHIRPS/DAILY",
        lat_bounds=(-50.0, 50.0),
        first_year=1981,
        note="v2.0 final. The long-standing reference product; most published crop-model "
        "work used this one.",
    ),
    "chirps_v3_rnl": ChirpsSource(
        collection="UCSB-CHC/CHIRPS/V3/DAILY_RNL",
        lat_bounds=(-60.0, 60.0),
        first_year=1981,
        note="v3.0 reanalysis. Pentadal CHIRPS-v3 totals disaggregated to daily using ERA5, "
        "so daily values are less independent than v2's — the pentad total is the "
        "quantity actually estimated.",
    ),
    "chirps_v3_sat": ChirpsSource(
        collection="UCSB-CHC/CHIRPS/V3/DAILY_SAT",
        lat_bounds=(-60.0, 60.0),
        first_year=1981,
        note="v3.0 near-real-time. Same pentadal totals disaggregated using IMERG Late. "
        "Lower latency, less stable history.",
    ),
}

ALL_SOURCES: tuple[str, ...] = tuple(CHIRPS_SOURCES)

# Silver `source` codes. Stored as SMALLINT in wth_precip_alt because the column sits in the
# primary key of a very large table, so its width is paid in both heap and index.
SOURCE_CODES: dict[str, int] = {
    "chirps_v2": 2,
    "chirps_v3_rnl": 3,
    "chirps_v3_sat": 4,
}


def source_spec(name: str) -> ChirpsSource:
    """Look up a CHIRPS source, raising with the known names rather than a bare KeyError."""
    try:
        return CHIRPS_SOURCES[name]
    except KeyError:
        raise KeyError(
            f"unknown CHIRPS source {name!r}; known: {', '.join(ALL_SOURCES)}"
        ) from None


def source_code(name: str) -> int:
    """The SMALLINT written to ``wth_precip_alt.source``."""
    source_spec(name)  # validate
    return SOURCE_CODES[name]


def covers_extent(name: str, extent: list[float]) -> bool:
    """Whether ``extent`` = ``[S, W, N, E]`` lies inside the product's latitude coverage."""
    spec = source_spec(name)
    south, _, north, _ = extent
    return spec.lat_bounds[0] <= south and north <= spec.lat_bounds[1]


def build_daily_collection(source: str, year: int) -> "ee.ImageCollection":
    """One image per day of ``year``, each stamped with a ``date`` property.

    CHIRPS is already daily, so this re-stamps rather than reduces. A day the product is
    missing becomes a fully-masked image rather than a gap in the band list: the export's
    band count must equal the day count, or the date-to-band mapping on read silently shifts.

    **Every band is cast to Float32.** ``ee.Image.constant(0)`` is a *Byte* image, while real
    CHIRPS precipitation is Float32, and ``Export.image`` refuses a multi-band image with
    mixed types ("Exported bands must have compatible data types"). The placeholder only
    fires for a day the product does not have, so 1981-2025 exported fine and the first
    failure was 2026 — the partial year past the catalog's end, where the placeholder is the
    only thing filling July onward. The cast makes the band type independent of whether the
    day exists, which is the property that has to hold for every future partial year too.
    """
    spec = source_spec(source)
    start = ee.Date.fromYMD(year, 1, 1)
    n_days = 366 if calendar.isleap(year) else 365
    src = ee.ImageCollection(spec.collection).select(BAND)

    def _day(i):
        day_start = start.advance(ee.Number(i), "day")
        img = src.filterDate(day_start, day_start.advance(1, "day")).first()
        img = ee.Image(ee.Algorithms.If(img, img, ee.Image.constant(0).selfMask()))
        return ee.Image(img).rename(BAND).toFloat().set(
            {
                "system:time_start": day_start.millis(),
                "date": day_start.format("YYYY-MM-dd"),
            }
        )

    return ee.ImageCollection(ee.List.sequence(0, n_days - 1).map(_day))
