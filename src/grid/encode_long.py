"""Raster → long bronze-frame encoder, shared by every download backend.

Both the CDS path (``src/cds/download.py``, reading netCDF) and the GEE path
(``src/gee/download.py``, reading GeoTIFF) end with the *same* job: take a daily raster
stack on (or near) the canonical 0.25° grid and emit the long bronze frame
``[child_id, parent_id, date, value]``. That shared step lives here so the two backends
stay bit-identical — only the *reader* (netCDF vs GeoTIFF) differs.

Contract: ``values`` is ``(ntime, nlat, nlon)``; ``lat`` / ``lon`` are the raster's
coordinate axes (degrees, any longitude convention); ``times`` is one timestamp per
slice. Coordinates are snapped to the nearest canonical index (``np.rint``), so a
native-resolution ERA5 raster needs no exact alignment — the same tolerance the CDS path
has always relied on.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.grid.encoding import cell_codes, parent_codes
from src.grid.spec import LAT_ORIGIN, NLAT, NLON, RESOLUTION

# Must match the grid seed's b (immutable, PLANNING.md §6.3). Re-exported for the
# backends so there is a single definition of the default block size.
DEFAULT_BLOCK_SIZE_B = 4


def encode_grid(
    values: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    times,
    *,
    source: str = "<raster>",
    b: int = DEFAULT_BLOCK_SIZE_B,
) -> pd.DataFrame:
    """Encode a daily raster stack to ``[child_id, parent_id, date, value]``.

    ``values`` has shape ``(ntime, nlat, nlon)`` aligned to ``times`` / ``lat`` / ``lon``.
    Asserts the product is genuinely **daily** — exactly one value per cell per day —
    before returning; misconfigured requests have returned hourly data (PLANNING.md §5.2).
    ``source`` only labels that error.
    """
    values = np.asarray(values, dtype=np.float64)
    lat = np.asarray(lat)
    lon = np.asarray(lon)

    lat_i = np.rint((LAT_ORIGIN - lat) / RESOLUTION).astype(np.int64)
    lon_i = np.rint((lon % 360.0) / RESOLUTION).astype(np.int64)
    np.clip(lat_i, 0, NLAT - 1, out=lat_i)
    np.clip(lon_i, 0, NLON - 1, out=lon_i)

    lat_grid, lon_grid = np.meshgrid(lat_i, lon_i, indexing="ij")  # (nlat, nlon)
    child = cell_codes(lat_grid, lon_grid).ravel()
    parent = parent_codes(lat_grid, lon_grid, b).ravel()
    dates = pd.to_datetime(times).normalize().date  # one date per timestep
    ncell = child.size

    frame = pd.DataFrame(
        {
            "child_id": np.tile(child, len(dates)),
            "parent_id": np.tile(parent, len(dates)),
            "date": np.repeat(dates, ncell),
            "value": values.reshape(len(dates), ncell).ravel(),
        }
    )
    if frame.duplicated(["child_id", "date"]).any():
        raise ValueError(
            f"{source}: multiple values per cell per day — not daily (hourly?) (§5.2)"
        )
    return frame
