"""Export a daily ``ee.ImageCollection`` to GCS and pull it back as GeoTIFF.

The compute lives server-side; this module triggers it and retrieves the result:

1. Flatten the per-day collection to a single multi-band image (``toBands``), one band per
   local day, each band **named with its date** so the reader can recover the time axis.
2. ``Export.image.toCloudStorage`` over the extent at the **native** 0.25° grid — aligned to
   the canonical origin via ``crsTransform`` (the GEE analogue of the CDS "never regrid"
   rule; small residual misalignment is absorbed by ``encode_grid``'s nearest-index snap).
3. Poll the batch task to completion with bounded backoff.
4. Download the resulting GeoTIFF shard(s) from GCS and read them into the
   ``(values, lat, lon, times)`` tuple ``encode_grid`` consumes.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Callable

import ee

from src.grid.spec import RESOLUTION

# Canonical-grid crsTransform: pixel size 0.25°, top-left corner placed so pixel CENTERS
# fall on multiples of 0.25° starting at lon 0 / lat 90 (matches src/grid/spec.py). Centre
# of the first pixel = corner + half a pixel, so corner = (multiple·0.25) − 0.125.
_CRS = "EPSG:4326"
_HALF = RESOLUTION / 2.0
_CRS_TRANSFORM = [RESOLUTION, 0, -180.0 - _HALF, 0, -RESOLUTION, 90.0 + _HALF]

_TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "CANCEL_REQUESTED"}


def _to_multiband(daily_col: "ee.ImageCollection") -> "ee.Image":
    """Collapse the daily collection to one multi-band image, each band named by its date."""

    def _named(img):
        return ee.Image(img).rename(ee.String(ee.Image(img).get("date")))

    return daily_col.map(_named).toBands()


def _region(extent: list[float]) -> "ee.Geometry":
    """``extent`` is ``[S, W, N, E]`` → an EE rectangle ``[W, S, E, N]`` (xMin,yMin,xMax,yMax)."""
    s, w, n, e = extent
    return ee.Geometry.Rectangle([w, s, e, n], proj=_CRS, geodesic=False)


# Country polygons used to clip the export to land. ERA5 (unlike ERA5-Land) carries real
# values over ocean too, so without this the export region is the full rectangle and EE
# computes every interior-ocean tile — wasted EECU, storage, and egress. Clipping to land
# roughly halves EECU and (with NaN-drop on read) keeps bronze land-only. Verified live.
_LAND_FC = "USDOS/LSIB_SIMPLE/2017"


def _land_region(extent: list[float], *, max_error: float = 1000.0) -> "ee.Geometry":
    """Land geometry within ``extent`` = the bbox intersected with LSIB country polygons."""
    bbox = _region(extent)
    land = ee.FeatureCollection(_LAND_FC).filterBounds(bbox).geometry()
    return land.intersection(bbox, maxError=max_error)


def tz_zones(
    extent: list[float], tz_asset: str, *, max_error: float = 1000.0
) -> list[tuple[float, "ee.Geometry"]]:
    """Per-offset ``(offset_hours, region)`` zones covering ``extent`` (PLANNING.md §5.3).

    Reads the tz-polygon asset (property ``offset`` = standard UTC offset in minutes; built
    by ``scripts/build_tz_asset.py`` from the same shapefile that stamps the grid's
    ``t_zone``), keeps only offsets present in the extent bbox, and returns each offset's
    region clipped to the bbox. The one ``getInfo`` here (the distinct offset list) is why
    this lives in the export module, not the pure-graph ``daily`` module. Feed the result to
    ``daily.build_daily_collection(zones=...)``.
    """
    bbox = _region(extent)
    fc = ee.FeatureCollection(tz_asset).filterBounds(bbox)
    offsets = fc.aggregate_array("offset").distinct().getInfo()
    if not offsets:
        raise RuntimeError(
            f"tz asset {tz_asset!r} yields no offsets over extent {extent}; "
            "check the asset path and its `offset` property"
        )
    zones = []
    for off_min in sorted(offsets):
        region = fc.filter(ee.Filter.eq("offset", off_min)).geometry().intersection(
            bbox, maxError=max_error
        )
        zones.append((off_min / 60.0, region))
    return zones


def start_export(
    daily_col: "ee.ImageCollection",
    extent: list[float],
    *,
    bucket: str,
    name_prefix: str,
    description: str,
    land_only: bool = True,
) -> "ee.batch.Task":
    """Submit the GCS export task for one ``(variable, year)`` and return the started task.

    ``land_only`` (default) clips the export region to land via LSIB; ocean cells then come
    back masked (nodata) and are dropped on read. Set false to export the full rectangle
    (incl. ocean, matching the CDS bronze coverage).
    """
    image = _to_multiband(daily_col)
    region = _land_region(extent) if land_only else _region(extent)
    task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description=description,
        bucket=bucket,
        fileNamePrefix=name_prefix,
        region=region,
        crs=_CRS,
        crsTransform=_CRS_TRANSFORM,
        fileFormat="GeoTIFF",
        maxPixels=int(1e13),
        formatOptions={"cloudOptimized": True},
    )
    task.start()
    return task


def wait_for_task(
    task: "ee.batch.Task",
    *,
    poll_interval: float = 20.0,
    timeout: float = 6 * 3600.0,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> dict:
    """Block until ``task`` reaches a terminal state; return its final status dict.

    Raises on FAILED/CANCELLED or if ``timeout`` elapses first.
    """
    deadline = now() + timeout
    while True:
        status = task.status()
        state = status.get("state")
        if state in _TERMINAL:
            if state != "COMPLETED":
                raise RuntimeError(
                    f"GEE export {state}: {status.get('error_message', status)}"
                )
            return status
        if now() >= deadline:
            raise TimeoutError(f"GEE export still {state} after {timeout}s")
        sleep(poll_interval)


def download_prefix(bucket: str, name_prefix: str, dest_dir: str | Path) -> list[Path]:
    """Download every GCS object under ``name_prefix`` (the export's shard(s)) locally.

    A region-limited single image is usually one ``.tif``; large exports tile into several
    ``...-NNNNNNNNNN-NNNNNNNNNN.tif`` shards, all returned.
    """
    from google.cloud import storage  # lazy: keeps the import off the CDS-only path

    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    client = storage.Client()
    blobs = [
        b
        for b in client.list_blobs(bucket, prefix=name_prefix)
        if b.name.endswith(".tif")
    ]
    if not blobs:
        raise FileNotFoundError(
            f"no .tif under gs://{bucket}/{name_prefix} after a completed export"
        )
    paths = []
    for blob in blobs:
        local = dest_dir / Path(blob.name).name
        blob.download_to_filename(str(local))
        paths.append(local)
    return sorted(paths)


def _band_dates(descriptions) -> list[str]:
    """Pull the ``YYYY-MM-DD`` each band was named with from its GeoTIFF description."""
    import re

    if isinstance(descriptions, str):
        descriptions = (descriptions,)
    date_re = re.compile(r"\d{4}-\d{2}-\d{2}")
    out = []
    for desc in descriptions:
        m = date_re.search(str(desc))
        if not m:
            raise ValueError(f"band description {desc!r} carries no date")
        out.append(m.group(0))
    return out


def iter_geotiff_chunks(paths: list[Path], *, chunk_days: int = 30):
    """Stream exported GeoTIFF shard(s) in band-windows → ``(values, lat, lon, times)``.

    Yields one ``(values(k,h,w), lat, lon, times[k])`` tuple per ``chunk_days``-band window
    per shard, so a full year is never held in RAM at once (the fix for the Pi OOM). Nodata
    (masked ocean from a land-only export) is read as ``NaN``; ``encode_grid`` + the caller's
    ``dropna`` discard those cells. Each shard is a full-time spatial sub-block, so windows
    never span shards.
    """
    import numpy as np
    import pandas as pd
    import rasterio

    for p in paths:
        with rasterio.open(p) as src:
            dates = _band_dates(src.descriptions)
            t = src.transform
            cols = np.arange(src.width)
            rows = np.arange(src.height)
            lon = t.c + (cols + 0.5) * t.a  # pixel-center longitudes
            lat = t.f + (rows + 0.5) * t.e  # pixel-center latitudes (t.e is negative)
            nbands = src.count
            for i0 in range(0, nbands, chunk_days):
                idx = list(range(i0, min(i0 + chunk_days, nbands)))
                bands = [b + 1 for b in idx]  # rasterio bands are 1-based
                arr = src.read(bands, masked=True).filled(np.nan).astype(np.float64)
                times = pd.to_datetime([dates[b] for b in idx])
                yield arr, lat, lon, times


def read_daily_geotiffs(paths: list[Path]):
    """Read exported GeoTIFF shard(s) into ``(values, lat, lon, times)`` for ``encode_grid``.

    Band descriptions carry the date each band was named with; the band axis becomes the
    time axis. Multiple spatial shards are mosaicked back together first. Whole-load — kept
    for small rasters and tests; the production path streams via ``iter_geotiff_chunks``.
    """
    import numpy as np
    import pandas as pd
    import rioxarray  # noqa: F401 — registers the .rio accessor / open_rasterio
    from rioxarray import open_rasterio

    arrays = [open_rasterio(p) for p in paths]
    if len(arrays) == 1:
        da = arrays[0]
    else:
        from rioxarray.merge import merge_arrays

        da = merge_arrays(arrays)

    # Band descriptions = the date-named bands (e.g. "0_2020-01-01"); pull the YYYY-MM-DD.
    times = pd.to_datetime(_band_dates(da.attrs.get("long_name")))

    values = np.asarray(da.values, dtype=np.float64)  # (band/time, y/lat, x/lon)
    lat = np.asarray(da.y.values)
    lon = np.asarray(da.x.values)
    for a in arrays:
        a.close()
    return values, lat, lon, times
