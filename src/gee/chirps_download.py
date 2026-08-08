"""CHIRPS bronze download: native-0.05° GeoTIFF export → Parquet.

The CHIRPS analogue of ``src/gee/download.py``. For one ``(source, year)`` it builds the
daily collection (``chirps.py``), exports it to GCS on **CHIRPS's own 0.05° grid** and pulls
it back (``export.py``), encodes it with ``encode_fine_grid``, and writes
``bronze/<source>/<source>_<year>.parquet`` with a ``fine_id, fparent_id, date, value``
schema — the fine-grid mirror of the ERA5 bronze contract.

Simpler than the ERA5 path in three ways, all because CHIRPS is a finished daily product:
no timezone zones (``zones = 1``), no unit conversion (already mm/day), and no
``is_preliminary`` provenance flag — v2.0 final and v3.0 RNL are both settled histories, not
rolling ERA5T-style revisions.

Shares the ERA5 path's ``Manifest`` (keys namespace themselves by variable name), its
``RunMetrics``/``run_export`` instrumentation (so CHIRPS cost records land in the same
``_gee_metrics.jsonl`` and are directly comparable to E1), and its streamed band-window
write (a Tocantins year never lives in RAM at once).
"""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.gee.chirps import build_daily_collection, covers_extent, source_spec
from src.gee.export import FINE_CRS_TRANSFORM, iter_geotiff_chunks
from src.gee.metrics import RunMetrics, append_record, metrics_path, run_export
from src.grid.encode_fine import encode_fine_grid
from src.grid.fine_spec import BLOCK_B


def download_source_year(
    client,
    source: str,
    year: int,
    extent: list[float],
    *,
    manifest,
    bronze_dir: str | Path,
    b: int = BLOCK_B,
    chunk_days: int = 30,
    land_only: bool = True,
    sample: str | None = None,
    metrics_out: dict | None = None,
    max_attempts: int | None = None,
    on_poll=None,
) -> Path:
    """Download one ``(source, year)`` to ``bronze/<source>/<source>_<year>.parquet``.

    ``source`` is a key of ``chirps.CHIRPS_SOURCES``. Idempotent: returns the existing
    Parquet if the manifest already marks the pair done. ``chunk_days`` caps how many daily
    bands are held in RAM per encode step; ``land_only`` clips the export to land, and masked
    pixels are dropped on read.

    The export is pinned to ``FINE_CRS_TRANSFORM`` — CHIRPS's own grid. This is not a tuning
    knob: the 0.25° default would resample every pixel server-side, and the resulting values
    would not be CHIRPS's. ``encode_fine_grid`` re-checks the returned raster and raises if
    it is not on that grid.

    Unchunked by design. Tocantins is ~17,136 cells x 1 zone, roughly 3x under the measured
    export ceiling (``src/gee/chunks.py:CELL_ZONE_CEILING``), and the E3 probe confirmed one
    export per year lands at ``attempt == 1`` for both collections (~0.005 EECU-h each — the
    ERA5 cost curve overpredicts CHIRPS by ~143x, since there is no hourly reduction to pay
    for). A larger extent needs ``chunks.plan_chunks`` generalized to the fine grid first —
    and its own probe, because that ceiling was measured at 0.25° cells and 366 bands.
    """
    spec = source_spec(source)
    if year < spec.first_year:
        raise ValueError(
            f"{source} starts in {spec.first_year}; {year} predates the product. "
            "Years before that are ERA5-only, in wth_base."
        )
    if not covers_extent(source, extent):
        raise ValueError(
            f"{source} covers latitudes {spec.lat_bounds}; extent {extent} reaches past it. "
            "Cells outside would come back masked, leaving a silent hole in the backfill."
        )

    out_dir = Path(bronze_dir) / source
    out_path = out_dir / f"{source}_{year}.parquet"
    if manifest.is_var_year_done(source, year) and out_path.exists():
        return out_path

    bucket = client.require_bucket()
    name_prefix = f"{client.gcs_prefix}/{source}_{year}"
    m = RunMetrics(
        kind="bronze_var_year",
        dataset=spec.collection,
        name_prefix=name_prefix,
        extent=extent,
        variable=source,
        year=year,
        sample=sample,
        land_only=land_only,
        b=b,
        chunk_days=chunk_days,
        gee_project=getattr(client, "project", None),
        cell_column="fine_id",  # 0.05° grid; the ERA5 default child_id is not emitted here
    )
    try:
        # CHIRPS is already daily: one zone, no local-day mosaic. Recorded so the cost
        # records stay comparable to ERA5's, where this is the multiplier that hurts.
        m.note_zones(1)
        daily_col = build_daily_collection(source, year)

        out_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = out_path.with_name(out_path.name + ".tmp")
        writer = None
        with tempfile.TemporaryDirectory() as td:
            paths = run_export(
                daily_col,
                extent,
                bucket=bucket,
                name_prefix=name_prefix,
                description=f"bronze_{source}_{year}",
                dest_dir=td,
                metrics=m,
                land_only=land_only,
                crs_transform=FINE_CRS_TRANSFORM,
                max_attempts=max_attempts,
                on_poll=on_poll,
            )
            t_encode = time.monotonic()
            try:
                for values, lat, lon, times in iter_geotiff_chunks(
                    paths, chunk_days=chunk_days
                ):
                    m.note_raster_chunk(values.size)
                    frame = encode_fine_grid(
                        values, lat, lon, times, source=name_prefix, b=b
                    )
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
                f"{name_prefix}: export produced no valid cells — check extent/land_only"
            )
        os.replace(tmp_path, out_path)
        m.note_encode_done(seconds=time.monotonic() - t_encode, parquet_path=out_path)
        manifest.mark_var_year_done(source, year)
        return out_path
    except BaseException as exc:  # noqa: BLE001 — recorded, then re-raised untouched
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
        if metrics_out is not None:
            metrics_out["record_write_error"] = f"{type(exc).__name__}: {exc}"
