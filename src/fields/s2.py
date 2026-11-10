"""Earth Engine graph for per-date Sentinel-2 NDVI + Cloud Score+ — no I/O here.

Like ``src/gee/daily.py``, this module only *builds* EE objects, so tests can drive it with
a mocked ``ee``. ``extract.py`` does the ``getInfo`` / ``computePixels`` calls.

- Source is L2A surface reflectance, ``S2_SR_HARMONIZED`` (the harmonized variant removes
  the +1000 DN offset introduced with processing baseline 04.00 in 2022, so NDVI is
  comparable across the whole archive).
- Cloud/shadow masking is Cloud Score+ ``cs_cdf`` (0 = occluded, 1 = clear), joined per
  image by ``linkCollection``. The threshold is applied by the caller, not here, so the
  score itself can be stored.
- Only tiles of the requested UTM zone are used. A field near a zone edge is also covered
  by the neighbour zone's tiles, which EE would resample onto our grid; staying on the
  native grid keeps every value an actual S2 pixel.
"""

from __future__ import annotations

import ee

S2_COLLECTION = "COPERNICUS/S2_SR_HARMONIZED"
CS_COLLECTION = "GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED"
NODATA = -9999.0


def utm_zone_prefix(epsg: int) -> str:
    """``32632`` -> ``"32"``: the MGRS_TILE prefix of that zone's tiles."""
    return f"{epsg % 100:02d}"


def collection(region, start: str, end: str, epsg: int):
    """S2 L2A images over ``region`` in ``[start, end)``, native-zone tiles, cs_cdf linked."""
    return (
        ee.ImageCollection(S2_COLLECTION)
        .filterBounds(region)
        .filterDate(start, end)
        .filter(ee.Filter.stringStartsWith("MGRS_TILE", utm_zone_prefix(epsg)))
        .linkCollection(ee.ImageCollection(CS_COLLECTION), ["cs_cdf"])
    )


def acquisition_dates(col):
    """Sorted distinct UTC dates (``YYYY-MM-dd``) in ``col`` — an ``ee.List``."""
    return (
        col.aggregate_array("system:time_start")
        .map(lambda t: ee.Date(t).format("YYYY-MM-dd"))
        .distinct()
        .sort()
    )


def _ndvi(img):
    return img.normalizedDifference(["B8", "B4"]).rename("ndvi").addBands(
        img.select("cs_cdf")
    )


def band_tag(date: str) -> str:
    return date.replace("-", "")


def ndvi_stack(col, dates: list[str]):
    """One image, two bands per date: ``ndvi_YYYYMMDD`` and ``cs_YYYYMMDD``.

    Same-day images (MGRS tile overlap) collapse to the clearest pixel. Masked pixels —
    off-swath, or no Cloud Score+ match — become ``NODATA`` so they survive the trip
    through a numpy array instead of turning into an ambiguous 0.
    """
    bands = []
    for d in dates:
        day = col.filterDate(d, ee.Date(d).advance(1, "day"))
        mosaic = day.map(_ndvi).qualityMosaic("cs_cdf")
        tag = band_tag(d)
        bands.append(mosaic.select(["ndvi", "cs_cdf"], [f"ndvi_{tag}", f"cs_{tag}"]))
    return ee.Image.cat(bands).unmask(NODATA).toFloat()
