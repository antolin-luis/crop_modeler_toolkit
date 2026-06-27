"""GEE bronze download orchestration: server-side daily reduce → GeoTIFF → Parquet.

The GEE analogue of ``src/cds/download.py``. For one ``(variable, year)`` it builds the
per-local-day collection (``daily.py``), exports it to GCS and pulls it back (``export.py``),
encodes the raster onto the canonical grid with the **shared** ``encode_grid``, and writes
``bronze/<var>/<var>_<year>.parquet`` with the same ``child_id, parent_id, date, value``
schema as the CDS path — so files from either backend are interchangeable.

Idempotent via the shared ``Manifest``: a ``(variable, year)`` already marked done (by
*either* backend) and present on disk is returned untouched. There is no adaptive splitter
here — GEE has no per-request cost ceiling to probe; the single lever for staying under the
monthly EECU quota is splitting the *year range* across runs (see docs/gee_setup.md).
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from src.gee.daily import build_daily_collection, parse_offset_hours
from src.gee.export import (
    download_prefix,
    read_daily_geotiffs,
    start_export,
    wait_for_task,
)
from src.grid.encode_long import DEFAULT_BLOCK_SIZE_B, encode_grid


def download_variable_year(
    client,
    variable: str,
    year: int,
    extent: list[float],
    timezone: str,
    *,
    manifest,
    bronze_dir: str | Path,
    b: int = DEFAULT_BLOCK_SIZE_B,
) -> Path:
    """Download one ``(variable, year)`` to ``bronze/<var>/<var>_<year>.parquet``.

    ``client`` is a connected :class:`src.gee.client.GEEClient`. Idempotent: returns the
    existing Parquet if the manifest already marks the pair done.
    """
    out_dir = Path(bronze_dir) / variable
    out_path = out_dir / f"{variable}_{year}.parquet"
    if manifest.is_var_year_done(variable, year) and out_path.exists():
        return out_path

    offset = parse_offset_hours(timezone)
    daily_col = build_daily_collection(variable, year, offset_hours=offset)

    bucket = client.require_bucket()
    name_prefix = f"{client.gcs_prefix}/{variable}_{year}"
    task = start_export(
        daily_col,
        extent,
        bucket=bucket,
        name_prefix=name_prefix,
        description=f"bronze_{variable}_{year}",
    )
    wait_for_task(task)

    with tempfile.TemporaryDirectory() as td:
        paths = download_prefix(bucket, name_prefix, td)
        values, lat, lon, times = read_daily_geotiffs(paths)
        frame = encode_grid(values, lat, lon, times, source=name_prefix, b=b)

    frame = frame.sort_values(["child_id", "date"]).reset_index(drop=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_path, index=False)
    manifest.mark_var_year_done(variable, year)
    return out_path
