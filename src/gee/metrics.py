"""Per-run cost accounting for GEE exports (docs/cost_model_climate_context.md §3).

The cost model needs four numbers per run — EECU-hours (``E_s``), bytes egressed
(``B_s``), wall-clock (``T_s``) and units of work (``N_s``) — and until now the pipeline
recorded none of them: ``wait_for_task`` already returned the EE status dict but the
caller dropped it, and ``download_prefix`` had ``blob.size`` in hand and ignored it.

Three pieces live here:

- :class:`RunMetrics` — the accumulator for one run, plus :meth:`RunMetrics.to_record`.
- :func:`run_export` — the timed ``start_export → wait_for_task → download_prefix_measured``
  triplet. This is the **seam**: it takes any ``ee.ImageCollection``, so the ERA5 bronze
  path and the throwaway GFS/CHIRPS cost probes measure themselves through identical code.
- The record store — one JSON object per line in ``bronze_dir/_gee_metrics.jsonl``.

**Why a separate file and not ``_manifest.json``.** The manifest is *control* state: it
rewrites the whole file on every mark, and treats a corrupt file as empty — so a malformed
metrics blob there would silently discard ``done`` state and re-download an entire
backfill. It is also keyed last-write-wins on ``variable:year``, which cannot hold the same
variable-year measured at two different extents, and that comparison is the whole point of
calibrating (§3's linearity assumption needs two sizes, not one). Append-only JSONL also
survives a torn tail: one unreadable line is skipped, not fatal.

Records are kept under 4 KiB and written with a single ``O_APPEND`` write, so concurrent
writers under ``gee_pool`` cannot interleave lines. That bound is why this module stores
``n_blobs`` and ``bytes_per_blob_max`` rather than blob names or the raw EE status dict.
"""

from __future__ import annotations

import json
import os
import socket
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from src.gee.export import (
    download_prefix_measured,
    start_export,
    task_attempt,
    wait_for_task,
)

# v2 adds the chunking fields: chunk_id, parents, land_parents, n_zones, parallel,
# attempts, max_attempts. A v1 reader sees them as absent, not wrong.
SCHEMA_VERSION = 2
_FILENAME = "_gee_metrics.jsonl"

# float32 on the canonical grid — the denominator of the GeoTIFF compression ratio, the
# one number docs/cost_model_climate_context.md §2 currently assumes (at 2x) rather than
# measures.
_BYTES_PER_VALUE = 4


def metrics_path(bronze_dir: str | Path) -> Path:
    """The metrics JSONL for a bronze root — sits beside ``_manifest.json``."""
    return Path(bronze_dir) / _FILENAME


def append_record(path: str | Path, record: dict) -> None:
    """Append one record as a single line. Atomic against concurrent writers."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, sort_keys=True, default=str) + "\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line.encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def iter_records(path: str | Path) -> Iterator[dict]:
    """Yield every readable record; skip a torn or malformed line rather than raising."""
    try:
        text = Path(path).read_text()
    except FileNotFoundError:
        return
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            yield rec


def eecu_hours(status: dict | None) -> float | None:
    """``batch_eecu_usage_seconds`` as hours, or ``None`` if EE did not report it.

    **Never coerces a missing field to 0.0.** A silent zero would corrupt
    ``eecu_per_unit``, which is the single number this instrumentation exists to produce;
    an honest ``None`` says "read it off the EE task list" instead of lying quietly. The
    field is not guaranteed on every terminal state, and ``task_id`` is recorded precisely
    so the value stays recoverable by hand.
    """
    if not isinstance(status, dict):
        return None
    raw = status.get("batch_eecu_usage_seconds")
    if raw is None:
        return None
    try:
        return float(raw) / 3600.0
    except (TypeError, ValueError):
        return None


def ee_timings(status: dict | None) -> dict:
    """EE's own stamps → queue vs compute split (our clock can't see the queue)."""
    out = {
        "ee_creation_ms": None,
        "ee_start_ms": None,
        "ee_update_ms": None,
        "ee_queue_s": None,
        "ee_compute_s": None,
    }
    if not isinstance(status, dict):
        return out
    for key, field_name in (
        ("creation_timestamp_ms", "ee_creation_ms"),
        ("start_timestamp_ms", "ee_start_ms"),
        ("update_timestamp_ms", "ee_update_ms"),
    ):
        raw = status.get(key)
        out[field_name] = int(raw) if isinstance(raw, (int, float)) and raw else None
    if out["ee_creation_ms"] and out["ee_start_ms"]:
        out["ee_queue_s"] = (out["ee_start_ms"] - out["ee_creation_ms"]) / 1000.0
    if out["ee_start_ms"] and out["ee_update_ms"]:
        out["ee_compute_s"] = (out["ee_update_ms"] - out["ee_start_ms"]) / 1000.0
    return out


@dataclass
class RunMetrics:
    """Accumulator for one variable-year (or one probe). Reports; never raises."""

    kind: str  # 'bronze_var_year' | 'probe'
    dataset: str  # EE collection id, or 'cds:<dataset>' for a CDS probe
    name_prefix: str
    extent: list[float]
    variable: str | None = None
    year: int | None = None
    sample: str | None = None  # 'E1' | 'E2' | 'E3' — ties the row to docs §4/§9
    land_only: bool = True
    b: int | None = None
    chunk_days: int | None = None
    gee_project: str | None = None
    track_cells: bool = True

    # Chunking (src/gee/chunks.py). All None on a whole-extent run, which is what makes a
    # chunked JSONL comparable to the E0/E1 rows already recorded at v1.
    chunk_id: str | None = None
    parents: int | None = None  # parents in the chunk box
    land_parents: int | None = None  # of those, the ones holding land (from the grid)
    n_zones: int | None = None  # timezone zones mosaicked per band — the §9.3 suspect
    parallel: int | None = None  # concurrent exports in flight during this run
    max_attempts: int | None = None

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    started_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    _t0: float = field(default_factory=time.monotonic, repr=False)

    task_id: str | None = None
    task_state: str | None = None
    eecu_seconds: float | None = None
    attempts: int = 1  # highest EE attempt seen across polls
    _status: dict | None = field(default=None, repr=False)

    n_blobs: int | None = None
    bytes_remote: int | None = None
    bytes_local: int | None = None
    bytes_per_blob_max: int | None = None

    t_export_s: float | None = None
    t_download_s: float | None = None
    t_encode_s: float | None = None

    bronze_rows: int = 0
    raster_pixels: int = 0  # incl. masked ocean — the denominator of true compression
    _cells: set = field(default_factory=set, repr=False)
    _days: set = field(default_factory=set, repr=False)
    _cells_override: int | None = None
    _days_override: int | None = None
    cells_exact: bool = True

    parquet_path: str | None = None
    parquet_bytes: int | None = None
    error: str | None = None

    # --- collection -----------------------------------------------------------------
    def note_task_started(self, task) -> None:
        """Stamp the EE task id the moment it is submitted.

        Separate from :meth:`note_export` because a cancelled or failed export never
        reaches that call — and those are exactly the runs whose ``task_id`` you need, to
        look up on the EE task list what they consumed before dying.
        """
        self.task_id = getattr(task, "id", None) or self.task_id

    def note_poll(self, status: dict | None) -> None:
        """Fold in one poll of the EE status. Wired to ``wait_for_task(on_poll=...)``.

        Only the attempt counter is kept, and only its maximum: a task that dies on
        attempt 3 and is then cancelled reports ``attempt`` 3 in the status that mattered,
        but nothing guarantees the terminal status still carries it.
        """
        self.attempts = max(self.attempts, task_attempt(status))

    def note_zones(self, n_zones: int) -> None:
        """How many timezone zones this export mosaics per band (``export.tz_zones``)."""
        self.n_zones = int(n_zones)

    def note_export(self, *, task, status, seconds: float) -> None:
        """Record the export phase. ``task``/``status`` may be fakes or ``None``."""
        self.task_id = getattr(task, "id", None) or (
            status.get("id") if isinstance(status, dict) else None
        ) or self.task_id
        self.task_state = status.get("state") if isinstance(status, dict) else None
        self._status = status if isinstance(status, dict) else None
        self.note_poll(self._status)
        raw = self._status.get("batch_eecu_usage_seconds") if self._status else None
        self.eecu_seconds = float(raw) if isinstance(raw, (int, float)) else None
        self.t_export_s = seconds

    def note_download(self, *, blobs, seconds: float) -> None:
        self.n_blobs = blobs.n_blobs
        self.bytes_remote = blobs.bytes_remote
        self.bytes_local = blobs.bytes_local
        self.bytes_per_blob_max = blobs.bytes_per_blob_max
        self.t_download_s = seconds

    def note_raster_chunk(self, n_pixels: int) -> None:
        """Total pixels in one band-window **before** the ocean drop.

        Kept separate from ``note_encode_chunk`` because the two denominators answer
        different questions: raster pixels give the GeoTIFF's real compression, land-only
        rows give the cost per *useful* value. Conflating them makes a small extent with a
        lot of sea in its bbox look like it compressed badly.
        """
        self.raster_pixels += int(n_pixels)

    def note_encode_chunk(self, frame) -> None:
        """Fold one encoded band-window in. Called inside the streaming writer loop."""
        self.bronze_rows += len(frame)
        self._days.update(frame["date"].unique().tolist())
        if self.track_cells:
            self._cells.update(frame["child_id"].unique().tolist())

    def note_units(
        self, *, cells: int | None = None, days: int | None = None, exact: bool = True
    ) -> None:
        """Set ``N_s`` directly — for probes, which never run the bronze encoder."""
        if cells is not None:
            self._cells_override = int(cells)
        if days is not None:
            self._days_override = int(days)
        self.cells_exact = exact

    def note_encode_done(self, *, seconds: float, parquet_path=None) -> None:
        self.t_encode_s = seconds
        if parquet_path is not None:
            p = Path(parquet_path)
            self.parquet_path = str(p)
            self.parquet_bytes = p.stat().st_size if p.exists() else None

    def note_error(self, exc: BaseException) -> None:
        self.error = f"{type(exc).__name__}: {exc}"

    # --- derived --------------------------------------------------------------------
    @property
    def cells(self) -> int | None:
        if self._cells_override is not None:
            return self._cells_override
        if self.track_cells and self._cells:
            return len(self._cells)
        days = self.days
        if days and self.bronze_rows:
            return self.bronze_rows // days
        return None

    @property
    def days(self) -> int | None:
        if self._days_override is not None:
            return self._days_override
        return len(self._days) or None

    @property
    def cells_counted(self) -> bool:
        """False when ``cells`` was derived as ``bronze_rows / days`` rather than counted.

        The derivation is exact only for a complete land-only calendar, so a mixed JSONL
        stays interpretable: the flag says which path produced the number.
        """
        if self._cells_override is not None:
            return self.cells_exact
        return bool(self._cells)

    def to_record(self) -> dict:
        """The JSONL line. Unknowns stay ``None``; nothing here is invented."""
        cells, days = self.cells, self.days
        exact = self.cells_counted
        n_units = cells * days if cells and days else None
        eecu_h = eecu_hours(self._status)
        remote = self.bytes_remote

        rec = {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "sample": self.sample or "",
            "kind": self.kind,
            "dataset": self.dataset,
            "variable": self.variable,
            "year": self.year,
            "started_at": self.started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "extent": list(self.extent) if self.extent else None,
            "land_only": self.land_only,
            "b": self.b,
            "chunk_days": self.chunk_days,
            "name_prefix": self.name_prefix,
            "host": socket.gethostname(),
            "gee_project": self.gee_project,
            # chunking (schema v2)
            "chunk_id": self.chunk_id,
            "parents": self.parents,
            "land_parents": self.land_parents,
            "n_zones": self.n_zones,
            "parallel": self.parallel,
            # E_s
            "task_id": self.task_id,
            "task_state": self.task_state,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "eecu_seconds": self.eecu_seconds,
            "eecu_hours": eecu_h,
            # B_s
            "n_blobs": self.n_blobs,
            "bytes_remote": remote,
            "bytes_local": self.bytes_local,
            "bytes_per_blob_max": self.bytes_per_blob_max,
            # T_s
            "t_export_s": _round(self.t_export_s),
            "t_download_s": _round(self.t_download_s),
            "t_encode_s": _round(self.t_encode_s),
            "t_total_s": _round(time.monotonic() - self._t0),
            # N_s — bronze_rows is NOT R_s: silver is wide, ~7 bronze rows per wth_base
            # row, so using this as rows_per_unit inflates it 7x. R_s/D_s stay a
            # Postgres query (docs/cost_model_climate_context.md §3).
            "bronze_rows": self.bronze_rows,
            "raster_pixels": self.raster_pixels or None,
            "cells": cells,
            "cells_exact": exact,
            "days": days,
            "n_units": n_units,
            "error": self.error,
            "parquet_path": self.parquet_path,
            "parquet_bytes": self.parquet_bytes,
        }
        rec.update(ee_timings(self._status))
        # The cost-relevant rate: bytes egressed per *useful* (land) cell-day. This is
        # the one to extrapolate with — and note it is NOT extent-invariant, because a
        # small bbox pays fixed per-shard overhead over fewer useful values.
        rec["bytes_per_unit"] = _ratio(remote, n_units)
        rec["eecu_per_unit"] = _ratio(eecu_h, n_units)
        # True GeoTIFF compression: raster pixels (masked ocean included, because those
        # bytes are still stored and still egressed) vs the float32 they encode. >1 means
        # the file is smaller than raw. Deliberately NOT computed from land-only rows —
        # that conflates compression with land fraction and makes a sea-heavy bbox look
        # like a codec failure.
        rec["compression_ratio"] = _ratio(
            self.raster_pixels * _BYTES_PER_VALUE if self.raster_pixels else None, remote
        )
        # ...and the factor that separation exposes: how much of the raster is useful.
        rec["land_fraction"] = _ratio(n_units, self.raster_pixels)
        return rec


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(value, digits)


def _ratio(num, den) -> float | None:
    if num is None or not den:
        return None
    return round(num / den, 12)


def run_export(
    collection,
    extent: list[float],
    *,
    bucket: str,
    name_prefix: str,
    description: str,
    dest_dir: str | Path,
    metrics: RunMetrics,
    land_only: bool = True,
    poll_interval: float = 20.0,
    timeout: float = 6 * 3600.0,
    max_attempts: int | None = None,
    on_poll=None,
) -> list[Path]:
    """Submit, wait, fetch — each phase timed into ``metrics``. Returns local shards.

    Deliberately narrow: it does not create or remove ``dest_dir`` (the caller owns that,
    and on the bronze path it is a ``TemporaryDirectory`` whose teardown is what keeps the
    Pi's SSD from filling), does not encode anything, and never touches the manifest.

    ``collection`` is any ``ee.ImageCollection`` whose images carry a ``date`` property —
    the only thing ``start_export`` requires. Everything ERA5-specific
    (``daily.build_daily_collection``) sits *above* this call, which is what lets the GFS
    and CHIRPS cost probes measure themselves through exactly this code path.

    ``max_attempts`` bounds EE's internal restarts (see ``export.wait_for_task``); the
    attempt count reaches the record either way, because polls are folded into ``metrics``
    as they happen rather than read off the terminal status.

    ``on_poll`` is an *extra* status observer, chained after the metrics accumulator rather
    than replacing it — the caller's logging must not cost the record its attempt count. It
    exists because an export is otherwise silent for its whole duration, which for a
    multi-hour chunk is indistinguishable from a hang.
    """
    t0 = time.monotonic()
    metrics.max_attempts = max_attempts

    def _poll(status: dict) -> None:
        metrics.note_poll(status)
        if on_poll is not None:
            on_poll(status)
    task = start_export(
        collection,
        extent,
        bucket=bucket,
        name_prefix=name_prefix,
        description=description,
        land_only=land_only,
    )
    metrics.note_task_started(task)
    try:
        status = wait_for_task(
            task,
            poll_interval=poll_interval,
            timeout=timeout,
            max_attempts=max_attempts,
            on_poll=_poll,
        )
    except BaseException:
        # A cancelled or failed export has usually already consumed EECU. Re-poll once so
        # the record keeps what it spent instead of reporting nulls; best-effort, because
        # nothing here may mask the original failure.
        final = None
        try:
            final = task.status()
        except Exception:  # pragma: no cover - diagnostic only
            pass
        metrics.note_export(task=task, status=final, seconds=time.monotonic() - t0)
        raise
    metrics.note_export(task=task, status=status, seconds=time.monotonic() - t0)

    t1 = time.monotonic()
    paths, blobs = download_prefix_measured(bucket, name_prefix, dest_dir)
    metrics.note_download(blobs=blobs, seconds=time.monotonic() - t1)
    return paths
