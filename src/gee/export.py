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


def start_export(
    daily_col: "ee.ImageCollection",
    extent: list[float],
    *,
    bucket: str,
    name_prefix: str,
    description: str,
) -> "ee.batch.Task":
    """Submit the GCS export task for one ``(variable, year)`` and return the started task."""
    image = _to_multiband(daily_col)
    task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description=description,
        bucket=bucket,
        fileNamePrefix=name_prefix,
        region=_region(extent),
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


def read_daily_geotiffs(paths: list[Path]):
    """Read exported GeoTIFF shard(s) into ``(values, lat, lon, times)`` for ``encode_grid``.

    Band descriptions carry the date each band was named with; the band axis becomes the
    time axis. Multiple spatial shards are mosaicked back together first.
    """
    import re

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
    descriptions = da.attrs.get("long_name")
    if isinstance(descriptions, str):
        descriptions = (descriptions,)
    date_re = re.compile(r"\d{4}-\d{2}-\d{2}")
    times = []
    for desc in descriptions:
        m = date_re.search(str(desc))
        if not m:
            raise ValueError(f"band description {desc!r} carries no date")
        times.append(m.group(0))
    times = pd.to_datetime(times)

    values = np.asarray(da.values, dtype=np.float64)  # (band/time, y/lat, x/lon)
    lat = np.asarray(da.y.values)
    lon = np.asarray(da.x.values)
    for a in arrays:
        a.close()
    return values, lat, lon, times
