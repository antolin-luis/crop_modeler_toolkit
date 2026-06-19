"""Bronze download orchestration: CDS request → netCDF → Parquet (PLANNING.md §7, §11).

Builds the per-(variable, year) CDS request, drives the adaptive splitter, encodes the
returned daily netCDF onto the canonical grid, and writes one Parquet per variable per
year with columns ``child_id, parent_id, date, value`` (raw / unconverted — conversions
are Step 4, §7).

Resume model (user choice): raw partial netCDFs are written to a temp dir and deleted as
soon as each is converted; per-time-chunk Parquets persist under a ``.staging_<year>``
dir keyed by date range so a crash mid-year resumes from the last good chunk (manifest
sub-chunk tracking, §11.4). The staging dir is removed once the final Parquet is written.
"""

from __future__ import annotations

import shutil
import tempfile
from calendar import monthrange
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.cds import splitter
from src.cds.manifest import Manifest
from src.cds.variables import DATASET, variable_spec
from src.grid.encoding import cell_codes, parent_codes
from src.grid.spec import LAT_ORIGIN, NLAT, NLON, RESOLUTION
from src.grid.static_inputs import _normalize_netcdf

DEFAULT_BLOCK_SIZE_B = 4  # must match the grid seed's b (immutable, §6.3)

_ALL_MONTHS = [f"{m:02d}" for m in range(1, 13)]
_ALL_DAYS = [f"{d:02d}" for d in range(1, 32)]
_LAT_NAMES = ("latitude", "lat")
_LON_NAMES = ("longitude", "lon")
_TIME_NAMES = ("valid_time", "time", "date")


def _snap(x: float) -> float:
    return round(float(x) / RESOLUTION) * RESOLUTION


def extent_key(extent: list[float]) -> str:
    """Stable key for the splitter granularity cache (snapped extent)."""
    s, w, n, e = (_snap(v) for v in extent)
    return f"{s},{w},{n},{e}"


def build_request(
    variable: str, year: int, extent: list[float], timezone: str
) -> dict:
    """Assemble the CDS request for one (variable, year) over ``extent``.

    ``extent`` is ``[S, W, N, E]`` in −180/180; CDS ``area`` order is ``[N, W, S, E]``,
    snapped to 0.25° vertices. Never sets ``grid`` — regridding increases cost (§11.1).
    Field names follow the dataset form; ``build_request`` is the single place to adjust
    if the live form differs (see plan notes).
    """
    spec = variable_spec(variable)
    s, w, n, e = (_snap(v) for v in extent)
    return {
        "variable": [spec.cds_variable],
        "daily_statistic": spec.daily_statistic,
        "time_zone": timezone,
        "frequency": "1_hourly",
        "year": str(year),
        "month": list(_ALL_MONTHS),
        "day": list(_ALL_DAYS),
        "area": [n, w, s, e],
        "data_format": "netcdf",
    }


def _coord(da: xr.DataArray, names: tuple[str, ...]) -> tuple[str, np.ndarray]:
    for name in names:
        if name in da.coords:
            return name, np.asarray(da[name].values)
    raise KeyError(f"none of {names} in coords {list(da.coords)}")


def encode_netcdf(path: str | Path, *, b: int = DEFAULT_BLOCK_SIZE_B) -> pd.DataFrame:
    """Encode one daily netCDF chunk to a long frame ``[child_id, parent_id, date, value]``.

    Asserts the product is genuinely **daily** — exactly one value per cell per day —
    before returning; misconfigured requests have returned hourly data (§5.2).
    """
    ds = xr.open_dataset(path)
    try:
        var = next(iter(ds.data_vars))
        da = ds[var]
        lat_name, lat = _coord(da, _LAT_NAMES)
        lon_name, lon = _coord(da, _LON_NAMES)
        time_name, times = _coord(da, _TIME_NAMES)
        da = da.transpose(time_name, lat_name, lon_name)
        values = np.asarray(da.values, dtype=np.float64)  # (ntime, nlat, nlon)
    finally:
        ds.close()

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
            f"{path}: multiple values per cell per day — not daily (hourly?) (§5.2)"
        )
    return frame


def _chunk_bounds(year: int, months: list[str]) -> tuple[str, str]:
    first, last = int(months[0]), int(months[-1])
    return f"{year}-{first:02d}-01", f"{year}-{last:02d}-{monthrange(year, last)[1]:02d}"


# Default to one month per request: the derived daily-statistics dataset computes
# each day on-the-fly, and requests larger than (≤2 variables, 1 month) are throttled
# to ~40 internal workers and share a deep queue. Month-sized requests stay in the
# fast lane, so 12 small requests beat one big throttled year (ECMWF "small over
# large", §11.1). The splitter still shrinks a month further only if cost-rejected.
DEFAULT_MONTHS_PER_CHUNK = 1


def _chunk_plan(year: int, ek: str) -> list[list[str]]:
    """Month groups for the year at the cached granularity (monthly by default)."""
    size = splitter.get_cached_granularity(ek) or DEFAULT_MONTHS_PER_CHUNK
    return [_ALL_MONTHS[i : i + size] for i in range(0, 12, size)]


def download_variable_year(
    client,
    variable: str,
    year: int,
    extent: list[float],
    timezone: str,
    *,
    manifest: Manifest,
    bronze_dir: str | Path,
    b: int = DEFAULT_BLOCK_SIZE_B,
) -> Path | None:
    """Download one (variable, year) to ``bronze/<var>/<var>_<year>.parquet``.

    Idempotent: returns immediately if the manifest marks the (variable, year) done;
    skips time chunks already recorded; reuses any persisted staging chunk Parquets.
    """
    out_dir = Path(bronze_dir) / variable
    out_path = out_dir / f"{variable}_{year}.parquet"
    if manifest.is_var_year_done(variable, year) and out_path.exists():
        return out_path

    ek = extent_key(extent)
    staging = out_dir / f".staging_{year}"
    staging.mkdir(parents=True, exist_ok=True)

    for months in _chunk_plan(year, ek):
        start, end = _chunk_bounds(year, months)
        chunk_pq = staging / f"chunk_{start}_{end}.parquet"
        if manifest.is_chunk_done(variable, year, start, end) and chunk_pq.exists():
            continue
        request = {**build_request(variable, year, extent, timezone), "month": months}
        with tempfile.TemporaryDirectory() as td:
            nc_paths = splitter.submit(
                client, DATASET, request, td, extent_key=ek
            )
            frames = []
            for p in nc_paths:
                _normalize_netcdf(p)  # unwrap any zip-wrapped netCDF (§ static_inputs)
                frames.append(encode_netcdf(p, b=b))
        chunk_df = pd.concat(frames, ignore_index=True)
        chunk_df.to_parquet(chunk_pq, index=False)
        manifest.mark_chunk_done(variable, year, start, end)

    parts = sorted(staging.glob("chunk_*.parquet"))
    full = pd.concat((pd.read_parquet(p) for p in parts), ignore_index=True)
    full = full.sort_values(["child_id", "date"]).reset_index(drop=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    full.to_parquet(out_path, index=False)
    manifest.mark_var_year_done(variable, year)
    shutil.rmtree(staging, ignore_errors=True)
    return out_path
