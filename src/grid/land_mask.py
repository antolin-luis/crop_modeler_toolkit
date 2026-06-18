"""0.25° vs ERA5-Land land-mask clip (PLANNING.md §6.4).

Rule: a 0.25° cell is land if a configurable fraction ``X`` of the ERA5-Land (0.1°)
cells whose centroid falls inside the 0.25° cell footprint are land. ``0.25 / 0.1 =
2.5`` is not an integer, so the grids do not nest cleanly (~6.25 ERA5-Land cells
overlap each 0.25° cell, several partially). We therefore bin every fine centroid into
its single containing coarse cell — never assume clean nesting. ``X`` controls coastal
inclusiveness (important for coastal agriculture).

The ERA5-Land mask is any ERA5-Land variable's NaN pattern: land = non-NaN, sea = NaN.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import xarray as xr

from src.grid.spec import LAND_FRACTION_X, LAT_ORIGIN, NLAT, NLON, RESOLUTION


_LAT_NAMES = ("latitude", "lat")
_LON_NAMES = ("longitude", "lon")


def _dim(da: xr.DataArray, names: tuple[str, ...]) -> str:
    for n in names:
        if n in da.dims:
            return n
    raise KeyError(f"None of {names} found in dims {list(da.dims)}")


def to_latlon_grid(
    obj: str | Path | xr.Dataset | xr.DataArray, var: str | None = None
) -> xr.DataArray:
    """Resolve input to a 2D DataArray oriented (latitude, longitude).

    Collapses any extra dims (e.g. a singleton time) by taking the first index — but
    never the lat/lon dims, so a size-1 latitude (as in tests) survives.
    """
    if isinstance(obj, (str, Path)):
        obj = xr.open_dataset(obj)
    if isinstance(obj, xr.Dataset):
        name = var if var in obj.data_vars else next(iter(obj.data_vars))
        da = obj[name]
    else:
        da = obj
    lat_dim = _dim(da, _LAT_NAMES)
    lon_dim = _dim(da, _LON_NAMES)
    extra = {d: 0 for d in da.dims if d not in (lat_dim, lon_dim)}
    if extra:
        da = da.isel(extra)
    return da.transpose(lat_dim, lon_dim)


def _as_dataarray(land_mask_nc: str | Path | xr.Dataset | xr.DataArray) -> xr.DataArray:
    return to_latlon_grid(land_mask_nc)


def _coord(da: xr.DataArray, *names: str) -> np.ndarray:
    for n in names:
        if n in da.coords:
            return np.asarray(da[n].values)
    raise KeyError(f"None of {names} found in coords {list(da.coords)}")


def compute_is_land(
    land_mask_nc: str | Path | xr.Dataset | xr.DataArray,
    *,
    fraction_x: float = LAND_FRACTION_X,
) -> np.ndarray:
    """Compute the 0.25° land mask from an ERA5-Land field.

    Returns a ``(NLAT, NLON)`` boolean array indexed by ``(lat_idx, lon_idx)`` — the
    same canonical indices the seed build derives from its meshgrid, so callers index
    it directly. Coarse cells with no overlapping fine centroid are ``False``.
    """
    da = _as_dataarray(land_mask_nc)
    lat = _coord(da, "latitude", "lat")
    lon = _coord(da, "longitude", "lon")

    land = ~np.isnan(np.asarray(da.values))         # (nlat_fine, nlon_fine) bool

    # Bin each fine centroid into its containing 0.25° cell (canonical origin §6.1).
    lat_idx = np.floor((LAT_ORIGIN - lat) / RESOLUTION).astype(np.int64)
    lon_idx = np.floor((lon % 360.0) / RESOLUTION).astype(np.int64)
    np.clip(lat_idx, 0, NLAT - 1, out=lat_idx)
    np.clip(lon_idx, 0, NLON - 1, out=lon_idx)

    # Broadcast the 1D index vectors to the full 2D fine grid, then accumulate.
    li = np.broadcast_to(lat_idx[:, None], land.shape).ravel()
    lo = np.broadcast_to(lon_idx[None, :], land.shape).ravel()

    total = np.zeros((NLAT, NLON), dtype=np.int64)
    land_count = np.zeros((NLAT, NLON), dtype=np.int64)
    np.add.at(total, (li, lo), 1)
    np.add.at(land_count, (li, lo), land.ravel().astype(np.int64))

    with np.errstate(invalid="ignore", divide="ignore"):
        frac = np.where(total > 0, land_count / total, 0.0)
    return frac >= fraction_x
