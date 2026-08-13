"""Raster → long bronze-frame encoder for the 0.05° fine grid.

The fine-grid analogue of ``src/grid/encode_long.py``: take a daily raster stack on the
CHIRPS grid and emit ``[fine_id, fparent_id, date, value]``.

**One deliberate difference from the 0.25° encoder.** That one snaps coordinates to the
nearest canonical index with ``np.rint``, because the CDS path receives netCDF whose exact
alignment it does not control. Here we *do* control it — ``start_export`` is handed
``FINE_CRS_TRANSFORM``, so every exported pixel centre lands on a fine cell centre by
construction. A raster that does not align is therefore not a tolerance to absorb but
evidence that the export used the wrong ``crsTransform`` and the values have already been
resampled. So misalignment **raises** instead of snapping: silently accepting it is exactly
how a half-cell shift reaches the database looking plausible.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.grid.fine_encoding import cell_indices_arr, fine_codes, fine_parent_codes
from src.grid.fine_spec import BLOCK_B, LAT_ORIGIN, RESOLUTION

# How far an exported pixel centre may sit from the fine cell centre before we treat it as a
# different grid. A correct export is exact to float noise; half a cell is 0.025°, so this
# is ~1/25000 of a cell — comfortably tight enough to catch a shift, loose enough to ignore
# the float error in GeoTIFF affine arithmetic.
ALIGNMENT_TOLERANCE_DEG = 1e-6


def check_alignment(lat: np.ndarray, lon: np.ndarray, *, source: str = "<raster>") -> None:
    """Raise unless every coordinate sits on a fine cell centre.

    Guards the single most expensive defect on this path: an export submitted with the 0.25°
    ``crsTransform`` returns a raster that reads fine, encodes fine, and loads fine — with
    every value resampled off a grid that is not CHIRPS's.
    """
    lat_i, lon_i = cell_indices_arr(lat, lon)
    want_lat = LAT_ORIGIN - (lat_i + 0.5) * RESOLUTION
    want_lon = (lon_i + 0.5) * RESOLUTION
    # Compare in the raster's own longitude convention.
    got_lon = np.asarray(lon, dtype=np.float64) % 360.0

    d_lat = np.max(np.abs(np.asarray(lat, dtype=np.float64) - want_lat)) if len(lat) else 0.0
    d_lon = np.max(np.abs(got_lon - want_lon)) if len(lon) else 0.0
    if d_lat > ALIGNMENT_TOLERANCE_DEG or d_lon > ALIGNMENT_TOLERANCE_DEG:
        raise ValueError(
            f"{source}: raster is not on the 0.05° fine grid "
            f"(max offset lat {d_lat:.6g}°, lon {d_lon:.6g}°). "
            "The export almost certainly used the 0.25° crsTransform — pass "
            "export.FINE_CRS_TRANSFORM. These values are resampled, not CHIRPS's."
        )


def encode_fine_grid(
    values: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    times,
    *,
    source: str = "<raster>",
    b: int = BLOCK_B,
) -> pd.DataFrame:
    """Encode a daily raster stack to ``[fine_id, fparent_id, date, value]``.

    ``values`` has shape ``(ntime, nlat, nlon)`` aligned to ``times``/``lat``/``lon``.
    Asserts the raster is on the fine grid, and that the product is genuinely **daily** —
    exactly one value per cell per day.
    """
    values = np.asarray(values, dtype=np.float64)
    lat = np.asarray(lat, dtype=np.float64)
    lon = np.asarray(lon, dtype=np.float64)

    check_alignment(lat, lon, source=source)
    lat_i, lon_i = cell_indices_arr(lat, lon)

    lat_grid, lon_grid = np.meshgrid(lat_i, lon_i, indexing="ij")  # (nlat, nlon)
    fine = fine_codes(lat_grid, lon_grid).ravel()
    fparent = fine_parent_codes(lat_grid, lon_grid, b).ravel()
    dates = pd.to_datetime(times).normalize().date
    ncell = fine.size

    frame = pd.DataFrame(
        {
            "fine_id": np.tile(fine, len(dates)),
            "fparent_id": np.tile(fparent, len(dates)),
            "date": np.repeat(dates, ncell),
            "value": values.reshape(len(dates), ncell).ravel(),
        }
    )
    if frame.duplicated(["fine_id", "date"]).any():
        raise ValueError(f"{source}: multiple values per cell per day — not daily")
    return frame
