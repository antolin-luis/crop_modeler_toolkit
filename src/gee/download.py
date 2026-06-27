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

The GeoTIFF is encoded **streamed in band-windows** (``chunk_days``) straight to Parquet via
a ``ParquetWriter`` — a full LatAm year never lives in RAM at once (the fix for the Pi OOM).
With ``land_only`` exports, masked ocean cells read as NaN and are dropped, so bronze is
land-only and compact.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.gee.daily import build_daily_collection, parse_offset_hours
from src.gee.export import (
    download_prefix,
    iter_geotiff_chunks,
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
    chunk_days: int = 30,
    land_only: bool = True,
) -> Path:
    """Download one ``(variable, year)`` to ``bronze/<var>/<var>_<year>.parquet``.

    ``client`` is a connected :class:`src.gee.client.GEEClient`. Idempotent: returns the
    existing Parquet if the manifest already marks the pair done. ``chunk_days`` caps how
    many daily bands are held in RAM per encode step; ``land_only`` clips the export to land
    (masked ocean cells are dropped). Bronze row order is unspecified — the streamed write
    can't globally sort, and silver upserts on ``(parent_id, child_id, date)`` regardless.
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
        land_only=land_only,
    )
    wait_for_task(task)

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    writer = None
    with tempfile.TemporaryDirectory() as td:
        paths = download_prefix(bucket, name_prefix, td)
        try:
            for values, lat, lon, times in iter_geotiff_chunks(
                paths, chunk_days=chunk_days
            ):
                frame = encode_grid(values, lat, lon, times, source=name_prefix, b=b)
                frame = frame.dropna(subset=["value"]).reset_index(drop=True)
                if frame.empty:
                    continue
                table = pa.Table.from_pandas(frame, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(tmp_path, table.schema)
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()

    if writer is None:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"{name_prefix}: export produced no land cells — check extent/land_only"
        )
    os.replace(tmp_path, out_path)
    manifest.mark_var_year_done(variable, year)
    return out_path
