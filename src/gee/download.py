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
import time
from pathlib import Path

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from src.gee.daily import build_daily_collection
from src.gee.export import iter_geotiff_chunks, tz_zones
from src.gee.metrics import RunMetrics, append_record, metrics_path, run_export
from src.gee.variables import COLLECTION
from src.grid.encode_long import DEFAULT_BLOCK_SIZE_B, encode_grid

if TYPE_CHECKING:  # pragma: no cover — annotation only, keeps the import cost off the DAG
    from src.gee.chunks import Chunk


def download_variable_year(
    client,
    variable: str,
    year: int,
    extent: list[float],
    *,
    tz_asset: str,
    manifest,
    bronze_dir: str | Path,
    b: int = DEFAULT_BLOCK_SIZE_B,
    chunk_days: int = 30,
    land_only: bool = True,
    sample: str | None = None,
    metrics_out: dict | None = None,
    chunk: "Chunk | None" = None,
    land_parents: int | None = None,
    max_attempts: int | None = None,
    parallel: int | None = None,
) -> Path:
    """Download one ``(variable, year)`` to ``bronze/<var>/<var>_<year>.parquet``.

    ``client`` is a connected :class:`src.gee.client.GEEClient`. The local-day 24 h window is
    per-cell: ``tz_asset`` (the tz-polygon EE asset) is intersected with ``extent`` to derive
    one reduction zone per UTC offset present — no manual timezone input, correct for extents
    spanning several countries. Idempotent: returns the existing Parquet if the manifest
    already marks the pair done. ``chunk_days`` caps how many daily bands are held in RAM per
    encode step; ``land_only`` clips the export to land (masked ocean cells are dropped).
    Bronze row order is unspecified — the streamed write can't globally sort, and silver
    upserts on ``(parent_id, child_id, date)`` regardless.

    Every run appends one cost record to ``bronze_dir/_gee_metrics.jsonl`` (EECU, bytes
    egressed, wall-clock, units of work — see ``src/gee/metrics.py``); ``sample`` labels it
    with the calibration sample id it feeds, and ``metrics_out``, if given, receives the
    same record for the caller to log or push to XCom. A manifest hit returns early and
    records nothing, because nothing was measured.

    **Chunked mode.** Pass ``chunk`` (a :class:`src.gee.chunks.Chunk`) to fetch one
    parent-aligned block of ``extent`` instead of the whole thing — the fix for a
    continental extent that EE restarts until it is cancelled
    (docs/cost_model_climate_context.md §9.3). The chunk's own box replaces ``extent``, the
    Parquet lands at ``<var>_<year>__<chunk_id>.parquet``, and the manifest tracks the part
    rather than the year, so the year is only "done" once its caller says every part is in.
    ``max_attempts`` aborts the export when EE restarts it more than that many times, which
    is the signal that the chunk is still too big. ``land_parents``/``parallel`` are pure
    metrics context, recorded so a size ladder can be read back off the JSONL.
    """
    if chunk is not None:
        extent = list(chunk.extent)
    chunk_id = chunk.chunk_id if chunk is not None else None
    suffix = f"__{chunk_id}" if chunk_id else ""

    out_dir = Path(bronze_dir) / variable
    out_path = out_dir / f"{variable}_{year}{suffix}.parquet"
    done = (
        manifest.is_spatial_chunk_done(variable, year, chunk_id)
        if chunk_id
        else manifest.is_var_year_done(variable, year)
    )
    if done and out_path.exists():
        return out_path

    bucket = client.require_bucket()
    name_prefix = f"{client.gcs_prefix}/{variable}_{year}{suffix}"
    m = RunMetrics(
        kind="bronze_var_year",
        dataset=COLLECTION,
        name_prefix=name_prefix,
        extent=extent,
        variable=variable,
        year=year,
        sample=sample,
        land_only=land_only,
        b=b,
        chunk_days=chunk_days,
        gee_project=getattr(client, "project", None),
        chunk_id=chunk_id,
        parents=chunk.n_parents if chunk is not None else None,
        land_parents=land_parents,
        parallel=parallel,
    )
    try:
        zones = tz_zones(extent, tz_asset)
        m.note_zones(len(zones))
        daily_col = build_daily_collection(variable, year, zones=zones)

        out_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_name(out_path.name + ".tmp")
        writer = None
        with tempfile.TemporaryDirectory() as td:
            paths = run_export(
                daily_col,
                extent,
                bucket=bucket,
                name_prefix=name_prefix,
                description=f"bronze_{variable}_{year}",
                dest_dir=td,
                metrics=m,
                land_only=land_only,
                max_attempts=max_attempts,
            )
            t_encode = time.monotonic()
            try:
                for values, lat, lon, times in iter_geotiff_chunks(
                    paths, chunk_days=chunk_days
                ):
                    # Count pixels before the ocean drop: those bytes were egressed too,
                    # and they are the honest denominator for compression.
                    m.note_raster_chunk(values.size)
                    frame = encode_grid(values, lat, lon, times, source=name_prefix, b=b)
                    frame = frame.dropna(subset=["value"]).reset_index(drop=True)
                    if frame.empty:
                        continue
                    m.note_encode_chunk(frame)
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
        m.note_encode_done(seconds=time.monotonic() - t_encode, parquet_path=out_path)
        if chunk_id:
            manifest.mark_spatial_chunk_done(variable, year, chunk_id)
        else:
            manifest.mark_var_year_done(variable, year)
        return out_path
    except BaseException as exc:  # noqa: BLE001 — recorded, then re-raised untouched
        # A failed run still carries task_id and t_export_s, which is exactly what you
        # want when the failure *is* a quota event.
        m.note_error(exc)
        raise
    finally:
        _emit_metrics(m, bronze_dir, metrics_out)


def _emit_metrics(m: RunMetrics, bronze_dir: str | Path, metrics_out: dict | None) -> None:
    """Write the cost record. Must never turn a landed Parquet into a failed task."""
    try:
        record = m.to_record()
    except Exception as exc:  # pragma: no cover — accumulator is total by construction
        if metrics_out is not None:
            metrics_out["record_write_error"] = f"{type(exc).__name__}: {exc}"
        return
    if metrics_out is not None:
        metrics_out.update(record)
    try:
        append_record(metrics_path(bronze_dir), record)
    except Exception as exc:
        # src/ has no logger by convention; surface it via the DAG's XCom instead.
        if metrics_out is not None:
            metrics_out["record_write_error"] = f"{type(exc).__name__}: {exc}"
