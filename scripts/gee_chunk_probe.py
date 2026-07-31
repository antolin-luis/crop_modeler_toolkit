"""Find the export chunk size that actually completes, and how far parallelism scales.

**Maintainer one-off, not on the pipeline path.** It answers the question left open by
``docs/cost_model_climate_context.md`` §9.3: a whole-year export over Brazil never
completes — EE restarts it (``attempt`` 2, 3) until it is cancelled — so a continental
backfill has to be split. What it must be split *into* is unmeasured.

Three modes:

- ``ladder`` — the same variable-year exported at several chunk sizes (default 25 / 100 /
  400 parents = 5° / 10° / 20° boxes at b=4), a few chunks each. Answers "does it finish,
  in how long, on which attempt". ``--max-attempts 2`` is the point: a chunk that makes EE
  restart twice is over the line, and the probe stops paying for it instead of waiting out
  a 6-hour timeout.
- ``parallel`` — one size, the same chunk set run at several concurrency levels. The probe
  is a plain ``ThreadPoolExecutor`` and does **not** go through Airflow, so ``gee_pool``
  (capped at 2) does not bound it; the winning level is what that pool should later be set
  to.
- ``whole`` — one UNCHUNKED variable-year with the attempt cap, to re-test the export that
  failed on 2026-07-30. E1c found no size ceiling up to 19,518 cells, which is larger than
  the extent that failed, so "too big" no longer explains it.
- ``report`` — read the JSONL back and extrapolate to a full backfill (1950 → Jul 2026,
  7 variables). No EE, no cost.

Every run goes through the ordinary bronze path (``download_variable_year``), so it writes
real Parquet, exercises ``encode_grid`` at chunk scale, and appends the same
``_gee_metrics.jsonl`` record the DAG would — the ladder is comparable to E0/E1 rather than
measured with a different ruler.

Run::

    # what breaks, and where (serial, ~9 exports)
    uv run python scripts/gee_chunk_probe.py ladder --sample E1a \\
        --data-root .localdata/probe_ladder

    # how far concurrency scales at the chosen size (~24 exports)
    uv run python scripts/gee_chunk_probe.py parallel --chunk-parents 100 \\
        --sample E1b --data-root .localdata/probe_par

    uv run python scripts/gee_chunk_probe.py report --data-root .localdata/probe_ladder

Chunks are filtered to those holding land, read from ``era5_land_base_grid``. The probe
runs on the **host**, so point it at the published port::

    POSTGRES_HOST=localhost uv run python scripts/gee_chunk_probe.py ladder ...

``--no-db`` skips the filter and keeps every chunk in the bbox, ocean included — portable,
but it wastes an export per empty box and reports no ``land_parents``.
"""

from __future__ import annotations

import argparse
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from src.cds.manifest import Manifest
from src.config import load_config, resolve_bronze_dir
from src.gee.chunks import Chunk, tile_extent
from src.gee.client import GEEClient
from src.gee.download import download_variable_year
from src.gee.metrics import iter_records, metrics_path

# E1's extent (docs/cost_model_climate_context.md §4) — the one that did not complete.
BRAZIL_EXTENT = [-34.0, -74.0, 5.5, -34.75]

# Backfill horizon for the extrapolation: 1950 through July 2026.
BACKFILL_START_YEAR = 1950
BACKFILL_END_YEAR = 2026
BACKFILL_END_MONTHS = 7
N_VARIABLES = 7

# São Paulo regional egress, Premium tier, verified 2026-07-30 (docs §0).
EGRESS_USD_PER_GB = 0.12
# Batch EECU-hour list price. Contributor tier does not bill, but the number is the
# honest opportunity cost against the monthly quota.
EECU_USD_PER_HOUR = 0.40
EECU_QUOTA_PER_MONTH = 1000.0  # Contributor tier (docs/gee_setup.md §1)

# land_cells x timezone_zones above which EE restarts the export instead of running it.
# Measured 2026-07-31: everything <= 58,554 completed on attempt 1; the whole-Brazil
# extent at ~90,748 was restarted 3x in two independent runs. The limit sits between
# those, so the guard trips at the largest value actually observed to work and the
# --force flag exists to move it. See docs/cost_model_climate_context.md §9.4.
CELL_ZONE_CEILING = 58_554


def backfill_var_years() -> float:
    """Variable-years in a 1950 → Jul-2026 backfill: whole years plus the partial one."""
    whole = BACKFILL_END_YEAR - BACKFILL_START_YEAR
    return (whole + BACKFILL_END_MONTHS / 12.0) * N_VARIABLES


@dataclass
class Plan:
    """One export the probe intends to run."""

    chunk: Chunk
    land_parents: int
    land_cells: int
    zones: int = 0

    @property
    def cell_zones(self) -> int:
        return self.land_cells * max(self.zones, 1)

    @property
    def over_ceiling(self) -> bool:
        """True when the evidence says EE will restart this export rather than run it."""
        return bool(self.land_cells) and self.cell_zones > CELL_ZONE_CEILING


def side_of(parents_per_chunk: int) -> int:
    """Chunk side in parents. Sizes are quoted per *chunk* (100 parents = a 10x10 block)."""
    side = round(parents_per_chunk ** 0.5)
    if side * side != parents_per_chunk:
        raise SystemExit(
            f"--sizes/--chunk-parents must be perfect squares (a chunk is square); "
            f"{parents_per_chunk} is not — nearest are {side ** 2} and {(side + 1) ** 2}"
        )
    return side


def build_plan(
    extent: list[float], parents_per_chunk: int, *, use_db: bool
) -> list[Plan]:
    """Chunks covering ``extent`` at one size, land-bearing ones first (most land first)."""
    chunks = tile_extent(extent, side_of(parents_per_chunk))
    if not use_db:
        return [Plan(chunk=c, land_parents=0, land_cells=0) for c in chunks]

    from src.db.grid_query import chunk_land_stats

    stats = chunk_land_stats(chunks)
    plans = [
        Plan(
            chunk=c,
            land_parents=stats[c.chunk_id].land_parents,
            land_cells=stats[c.chunk_id].land_cells,
            zones=stats[c.chunk_id].zones,
        )
        for c in chunks
        if stats[c.chunk_id].land_cells > 0
    ]
    plans.sort(key=lambda p: p.land_cells, reverse=True)
    return plans


def eligible(plans: list[Plan], min_land_cells: int) -> list[Plan]:
    """Drop chunks too empty to size anything — a 3-cell sliver measures only overhead.

    Sizing only. A real backfill still has to fetch those chunks, or the extent has holes;
    ``build_plan`` therefore keeps them and the extrapolation counts them.
    """
    kept = [p for p in plans if p.land_cells >= min_land_cells]
    return kept or plans


def pick_samples(plans: list[Plan], n: int) -> list[Plan]:
    """``n`` chunks spread across the land-cell range — biggest, smallest, and between.

    Sampling the extremes matters more than sampling the middle: the biggest chunk is the
    one that decides whether the size is viable at all, and the smallest shows how much of
    the wall-clock is fixed overhead rather than pixels.
    """
    if n >= len(plans):
        return list(plans)
    if n == 1:
        return [plans[0]]
    step = (len(plans) - 1) / (n - 1)
    return [plans[round(i * step)] for i in range(n)]


class LockedManifest:
    """Serialises manifest access so parallel chunks cannot lose each other's marks.

    ``Manifest`` rewrites the whole JSON on every mark. The write itself is atomic
    (``os.replace``), so nothing corrupts — but two threads marking different chunks can
    interleave read-modify-write and drop one of the marks, which shows up later as a
    chunk being re-exported. One lock is cheap next to a multi-minute export.
    """

    def __init__(self, manifest: Manifest) -> None:
        self._m = manifest
        self._lock = threading.Lock()

    def __getattr__(self, name):
        attr = getattr(self._m, name)
        if not callable(attr):
            return attr

        def _locked(*args, **kwargs):
            with self._lock:
                return attr(*args, **kwargs)

        return _locked


def run_one(
    plan: Plan,
    *,
    client,
    manifest,
    variable: str,
    year: int,
    data_root: str,
    sample: str | None,
    max_attempts: int,
    parallel: int,
    chunk_days: int,
    force: bool = False,
) -> dict:
    """Export one chunk through the real bronze path; return its metrics record.

    A failure is data, not an abort: the whole point is to learn which sizes fail and on
    which attempt, so the error is captured into the record and the ladder continues.
    """
    bronze_dir = resolve_bronze_dir(data_root)
    tz_asset = load_config().gee.tz_asset
    if not tz_asset:
        raise RuntimeError("GEE_TZ_ASSET is unset — the per-cell local day needs it")
    if plan.over_ceiling and not force:
        # Not an error — a prediction, made from the grid alone, that this export would be
        # restarted rather than run. Recorded as a skip so the ladder still reports it.
        print(
            f"  {plan.chunk.chunk_id} SKIPPED: cell_zones={plan.cell_zones} exceeds the "
            f"measured ceiling {CELL_ZONE_CEILING} ({plan.land_cells} cells x "
            f"{plan.zones} zones). Pass --force to submit it anyway.",
            flush=True,
        )
        return {
            "chunk_id": plan.chunk.chunk_id,
            "skipped": "over_cell_zone_ceiling",
            "cell_zones": plan.cell_zones,
        }
    rec: dict = {}
    t0 = time.monotonic()
    try:
        download_variable_year(
            client,
            variable,
            year,
            plan.chunk.extent,
            tz_asset=tz_asset,
            manifest=manifest,
            bronze_dir=bronze_dir,
            chunk_days=chunk_days,
            sample=sample,
            metrics_out=rec,
            chunk=plan.chunk,
            land_parents=plan.land_parents or None,
            max_attempts=max_attempts,
            parallel=parallel,
        )
    except BaseException as exc:  # noqa: BLE001 — recorded, reported, not re-raised
        rec.setdefault("error", f"{type(exc).__name__}: {exc}")
    rec.setdefault("t_total_s", round(time.monotonic() - t0, 2))
    print(
        f"  {plan.chunk.chunk_id} "
        f"parents={plan.chunk.n_parents} land_parents={plan.land_parents} "
        f"cells={rec.get('cells')} attempts={rec.get('attempts')} "
        f"zones={rec.get('n_zones')} t={rec.get('t_total_s')}s "
        f"eecu_h={rec.get('eecu_hours')} {'FAIL: ' + str(rec.get('error')) if rec.get('error') else 'ok'}",
        flush=True,
    )
    return rec


def show_plan(plans: list[Plan], *, parallel: int, variable: str, year: int) -> None:
    """Print what would run. No EE, no cost — the check before spending quota."""
    print(f"  DRY RUN: {len(plans)} exports of {variable} {year}, parallel={parallel}")
    for plan in plans:
        c = plan.chunk
        flag = "  <-- OVER CEILING, will be skipped" if plan.over_ceiling else ""
        print(
            f"    {c.chunk_id} extent={c.extent} parents={c.n_parents} "
            f"land_parents={plan.land_parents} land_cells={plan.land_cells} "
            f"zones={plan.zones} cell_zones={plan.cell_zones}{flag}"
        )


def run_batch(plans: list[Plan], *, parallel: int, data_root: str, **kw):
    """Run ``plans`` with ``parallel`` exports in flight; return records and wall-clock.

    Threads are safe here: each worker's EE and GCS calls are independent HTTP requests,
    and the metrics writer does one ``O_APPEND`` write per record. The two pieces of
    shared state are handled explicitly — one EE client (``ee.Initialize`` is global, so
    re-initialising per thread buys nothing) and one lock-wrapped manifest.
    """
    client = GEEClient()
    manifest = LockedManifest(Manifest.for_bronze_dir(resolve_bronze_dir(data_root)))
    kw = dict(kw, client=client, manifest=manifest, data_root=data_root)
    t0 = time.monotonic()
    if parallel <= 1:
        records = [run_one(p, parallel=1, **kw) for p in plans]
    else:
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futures = [pool.submit(run_one, p, parallel=parallel, **kw) for p in plans]
            records = [f.result() for f in futures]
    return records, time.monotonic() - t0


def _summarize(records: list[dict]) -> dict:
    """The numbers a size decision turns on. Failures excluded from the rates.

    **Medians, not means.** EE's per-task EECU is long-tailed — one 400-parent chunk in
    E1a burned 0.626 EECU-h against a 0.167 sibling of twice the cell count — and with 3
    samples per rung a mean is that outlier. The median is what an extrapolation over
    thousands of tasks should ride on.
    """
    records = [r for r in records if not r.get("skipped")]
    ok = [r for r in records if not r.get("error")]
    times = [r["t_total_s"] for r in ok if r.get("t_total_s") is not None]
    eecu = [r["eecu_hours"] for r in ok if r.get("eecu_hours") is not None]
    byts = [r["bytes_remote"] for r in ok if r.get("bytes_remote") is not None]
    cells = [r["cells"] for r in ok if r.get("cells") is not None]
    return {
        "n": len(records),
        "n_ok": len(ok),
        "max_attempts_seen": max((r.get("attempts") or 1) for r in records),
        "t_median_s": round(statistics.median(times), 1) if times else None,
        "t_max_s": max(times) if times else None,
        "eecu_median_h": round(statistics.median(eecu), 4) if eecu else None,
        "eecu_mean_h": round(statistics.fmean(eecu), 4) if eecu else None,
        "bytes_median": round(statistics.median(byts)) if byts else None,
        "cells_median": round(statistics.median(cells)) if cells else None,
    }


def fit_per_task(records: list[dict], field: str) -> tuple[float, float, float] | None:
    """Least-squares ``(fixed, per_cell, r2)`` for ``field`` against a task's cell count.

    Extrapolating ``median × n_chunks`` — the obvious thing — is wrong here, and E1c is
    why. Chunks vary enormously in how much land they hold (a 1600-parent rung sampled
    19,518 cells and 5,585 cells), so the median of three samples is not the average
    chunk, and multiplying it by the chunk count compounds that error.

    A task's cost is genuinely two terms: a fixed one (the 366-band graph over 8,760
    hourly images, paid per submission) and a marginal one per cell. Fitting both across
    every record and multiplying by each size's **actual** total cell count — which the
    grid knows exactly — uses all the data and removes the sampling bias.

    R² runs ~0.55: EE's per-task EECU is genuinely noisy, so treat the fit as the central
    estimate it is, not a promise.
    """
    pts = [
        (r["cells"], r[field])
        for r in records
        if r.get("cells") and r.get(field) and not r.get("error")
    ]
    if len(pts) < 3:
        return None
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    if denom <= 0:
        return None
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom
    fixed = my - slope * mx
    ss_tot = sum((y - my) ** 2 for y in ys)
    ss_res = sum((y - (fixed + slope * x)) ** 2 for x, y in zip(xs, ys))
    r2 = 1 - ss_res / ss_tot if ss_tot else 0.0
    return fixed, slope, r2


def _iso_seconds(value) -> float | None:
    from datetime import datetime

    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except (TypeError, ValueError):
        return None


def _batches(records: list[dict], *, gap_s: float = 180.0) -> list[list[dict]]:
    """Split records into the batches that actually ran together.

    Throughput only means something within one batch. Grouping by the ``parallel`` field
    alone silently merges every serial run ever recorded — the ladder writes
    ``parallel=1`` too — and the resulting "serial baseline" spans hours of wall-clock in
    which nothing was running, which then inflates the apparent speedup by ~10x. A batch
    is here a run of records at one concurrency level with no idle gap longer than
    ``gap_s`` between a record starting and the batch's latest finish so far.
    """
    rows = [
        r
        for r in records
        if _iso_seconds(r.get("started_at")) and _iso_seconds(r.get("finished_at"))
    ]
    rows.sort(key=lambda r: _iso_seconds(r["started_at"]))
    out: list[list[dict]] = []
    cur: list[dict] = []
    cur_end = 0.0
    for row in rows:
        start = _iso_seconds(row["started_at"])
        end = _iso_seconds(row["finished_at"])
        same_level = cur and cur[0].get("parallel") == row.get("parallel")
        if cur and (not same_level or start > cur_end + gap_s):
            out.append(cur)
            cur, cur_end = [], 0.0
        cur.append(row)
        cur_end = max(cur_end, end)
    if cur:
        out.append(cur)
    return out


def size_of(rec: dict) -> int | None:
    """Parents per chunk, decoded from ``chunk_id`` (``s10r009c-007`` → 100)."""
    cid = str(rec.get("chunk_id") or "")
    if not cid.startswith("s") or not cid[1:3].isdigit():
        return None
    return int(cid[1:3]) ** 2


def level_throughput(records: list[dict], *, size: int | None = None) -> dict[int, dict]:
    """``{parallel_level: {tasks_per_hour, concurrency, median_task_s}}`` from E1b rows.

    Batch wall-clock is not stored, but ``started_at``/``finished_at`` are, so the span is
    recoverable and **throughput** — tasks finished per hour — falls out of it.

    Throughput is the number a backfill estimate must ride on, not concurrency. The two
    diverge because EE throttles: E1b reached 4.4 exports in flight at level 8, yet each
    task slowed from a 159 s median to 248 s, so the batch finished only ~2.6x faster than
    serial. Dividing a serial estimate by the concurrency would book that 4.4x twice and
    promise a backfill nearly 2x faster than it can run.

    ``size`` restricts the comparison to one chunk size, and callers should always set it.
    A serial ladder rung of 1600-parent chunks and a parallel sweep of 100-parent chunks
    both record ``parallel``, but dividing one by the other measures chunk size, not
    concurrency.
    """
    rows = [r for r in records if not r.get("error")]
    if size is not None:
        rows = [r for r in rows if size_of(r) == size]
    agg: dict[int, dict] = {}
    for batch in _batches(rows):
        level = batch[0].get("parallel")
        if not isinstance(level, int) or level < 1 or len(batch) < 2:
            continue
        starts = [_iso_seconds(r["started_at"]) for r in batch]
        ends = [_iso_seconds(r["finished_at"]) for r in batch]
        times = [r["t_total_s"] for r in batch if r.get("t_total_s") is not None]
        span = max(ends) - min(starts)
        if span <= 0 or not times:
            continue
        slot = agg.setdefault(level, {"tasks": 0, "span": 0.0, "times": []})
        slot["tasks"] += len(batch)
        slot["span"] += span
        slot["times"].extend(times)

    return {
        level: {
            "tasks_per_hour": round(v["tasks"] * 3600.0 / v["span"], 2),
            "concurrency": round(sum(v["times"]) / v["span"], 2),
            "median_task_s": round(statistics.median(v["times"]), 1),
            "batches_tasks": v["tasks"],
        }
        for level, v in agg.items()
        if v["span"] > 0
    }


def cmd_ladder(args) -> None:
    for size in args.sizes:
        plans = build_plan(args.extent, size, use_db=not args.no_db)
        picked = pick_samples(eligible(plans, args.min_land_cells), args.samples_per_size)
        print(
            f"\n=== {size} parents/chunk ({side_of(size)}°x{side_of(size)}° at b=4): "
            f"{len(plans)} land chunks cover the extent, sampling {len(picked)} ===",
            flush=True,
        )
        if args.dry_run:
            show_plan(picked, parallel=1, variable=args.variable, year=args.year)
            continue
        records, wall = run_batch(
            picked,
            parallel=1,
            variable=args.variable,
            year=args.year,
            data_root=args.data_root,
            sample=args.sample or None,
            max_attempts=args.max_attempts,
            chunk_days=args.chunk_days,
            force=args.force,
        )
        summary = _summarize(records)
        print(
            f"  -> {summary['n_ok']}/{summary['n']} ok, "
            f"max attempt {summary['max_attempts_seen']}, "
            f"median {summary['t_median_s']}s, mean {summary['eecu_mean_h']} EECU-h, "
            f"batch wall {wall / 60:.1f} min",
            flush=True,
        )


def cmd_parallel(args) -> None:
    plans = build_plan(args.extent, args.chunk_parents, use_db=not args.no_db)
    picked = pick_samples(eligible(plans, args.min_land_cells), args.n_chunks)
    print(
        f"{len(picked)} chunks of {args.chunk_parents} parents, "
        f"levels {args.levels}",
        flush=True,
    )
    for level in args.levels:
        # A fresh data_root per level: the manifest would otherwise skip everything
        # after the first level, and a repeat measurement is exactly what is wanted here.
        root = str(Path(args.data_root) / f"p{level}")
        print(f"\n=== parallel={level} ===", flush=True)
        if args.dry_run:
            show_plan(picked, parallel=level, variable=args.variable, year=args.year)
            continue
        records, wall = run_batch(
            picked,
            parallel=level,
            variable=args.variable,
            year=args.year,
            data_root=root,
            sample=args.sample or None,
            max_attempts=args.max_attempts,
            chunk_days=args.chunk_days,
            force=args.force,
        )
        summary = _summarize(records)
        per_chunk = wall / len(picked) if picked else None
        print(
            f"  -> {summary['n_ok']}/{summary['n']} ok, batch wall {wall / 60:.1f} min, "
            f"{per_chunk:.0f}s per chunk amortized, median task {summary['t_median_s']}s",
            flush=True,
        )


def cmd_whole(args) -> None:
    """Re-run the UNCHUNKED variable-year that failed on 2026-07-30, with an attempt cap.

    E1c removed the reason to believe the original failure was about size: a 40°x40° chunk
    of 19,518 cells (25,600 raster px) completed on the first attempt, and that is *larger*
    than the whole-Brazil bbox (24,806 px) that EE kept restarting. Either that failure was
    transient, or its cause is something the ladder does not vary. One unchunked export
    settles it, and the answer is worth a lot: unchunked is the cheapest option there is,
    because the fixed per-task term is paid once instead of 4-62 times.
    """
    bronze_dir = resolve_bronze_dir(args.data_root)
    tz_asset = load_config().gee.tz_asset
    if not tz_asset:
        raise RuntimeError("GEE_TZ_ASSET is unset — the per-cell local day needs it")
    if args.dry_run:
        print(f"DRY RUN: 1 unchunked export of {args.variable} {args.year} "
              f"over {args.extent}, max_attempts={args.max_attempts}")
        return
    print(f"unchunked {args.variable} {args.year} over {args.extent} "
          f"(max_attempts={args.max_attempts})...", flush=True)
    rec: dict = {}
    t0 = time.monotonic()
    try:
        download_variable_year(
            GEEClient(),
            args.variable,
            args.year,
            list(args.extent),
            tz_asset=tz_asset,
            manifest=Manifest.for_bronze_dir(bronze_dir),
            bronze_dir=bronze_dir,
            chunk_days=args.chunk_days,
            sample=args.sample or None,
            metrics_out=rec,
            max_attempts=args.max_attempts,
            parallel=1,
        )
    except BaseException as exc:  # noqa: BLE001 — a failure here IS the measurement
        rec.setdefault("error", f"{type(exc).__name__}: {exc}")
    print(
        f"  cells={rec.get('cells')} zones={rec.get('n_zones')} "
        f"attempts={rec.get('attempts')} t={rec.get('t_total_s') or round(time.monotonic() - t0)}s "
        f"eecu_h={rec.get('eecu_hours')} "
        f"{'FAIL: ' + str(rec.get('error')) if rec.get('error') else 'ok'}"
    )


def cmd_report(args) -> None:
    """Aggregate JSONL rows by chunk size and extrapolate to the full backfill."""
    roots = [Path(args.data_root)] + [Path(p) for p in (args.also or [])]
    records: list[dict] = []
    for root in roots:
        for path in [metrics_path(resolve_bronze_dir(str(root)))] + sorted(
            root.glob("*/bronze/_gee_metrics.jsonl")
        ):
            records.extend(iter_records(path))
    records = [r for r in records if r.get("chunk_id")]
    if not records:
        print("no chunked records found — run `ladder` first")
        return

    by_size: dict[int, list[dict]] = {}
    for rec in records:
        cid = str(rec["chunk_id"])
        size = int(cid[1:3]) ** 2 if cid.startswith("s") else 0
        by_size.setdefault(size, []).append(rec)

    var_years = backfill_var_years()
    # The swept size is whichever one was run at more than one concurrency level.
    swept_size = None
    for size in sorted(by_size):
        if len({r.get("parallel") for r in by_size[size] if r.get("parallel")}) > 1:
            swept_size = size
    levels = level_throughput(records, size=swept_size) if swept_size else {}
    best_level, best = (
        max(levels.items(), key=lambda kv: kv[1]["tasks_per_hour"])
        if levels
        else (None, None)
    )
    print(f"\nBackfill horizon: {BACKFILL_START_YEAR}–{BACKFILL_END_YEAR}-"
          f"{BACKFILL_END_MONTHS:02d}, {N_VARIABLES} variables = {var_years:.1f} variable-years")
    if levels:
        base = levels.get(1, {}).get("tasks_per_hour")
        print(f"Concurrency sweep, measured at {swept_size} parents/chunk:")
        for lvl, st in sorted(levels.items()):
            gain = f", {st['tasks_per_hour'] / base:.2f}x serial" if base else ""
            print(
                f"  p{lvl}: {st['tasks_per_hour']:>5.1f} tasks/h  "
                f"(concurrency {st['concurrency']}, median task {st['median_task_s']}s{gain})"
            )
        speedup = (
            best["tasks_per_hour"] / base if base else 1.0
        )
        print(f"Best net speedup vs serial: p{best_level} = {speedup:.2f}x")
    else:
        speedup = 1.0
        print("No parallel records yet — par_d assumes serial. Run `parallel` to fix that.")

    eecu_fit = fit_per_task(records, "eecu_hours")
    time_fit = fit_per_task(records, "t_total_s")
    if eecu_fit:
        print(
            f"\nPer-task model from all {len(records)} records:\n"
            f"  EECU-h = {eecu_fit[0]:.4f} + {eecu_fit[1]:.3e} x cells   (R2={eecu_fit[2]:.2f})"
        )
    if time_fit:
        print(f"  seconds = {time_fit[0]:.0f} + {time_fit[1]:.4f} x cells   (R2={time_fit[2]:.2f})")
    print()

    header = (
        f"{'size':>6} {'ok/n':>7} {'maxatt':>6} {'med_s':>7} {'EECU-h':>8} "
        f"{'chunks':>7} {'tot_cells':>10} {'tasks':>9} {'EECU-h tot':>11} "
        f"{'GB':>7} {'quota_mo':>9} {'serial_d':>9} {'par_d':>7}"
    )
    print(header)
    print("-" * len(header))
    totals = {}
    for size in sorted(by_size):
        s = _summarize(by_size[size])
        plans = build_plan(args.extent, size, use_db=not args.no_db)
        n_chunks = len(plans)
        tot_cells = sum(p.land_cells for p in plans)
        tasks = n_chunks * var_years
        # Modelled, not median-scaled: fixed cost x tasks + marginal cost x the extent's
        # actual cell count, which barely moves with chunk size. See fit_per_task.
        if eecu_fit and tot_cells:
            eecu_tot = (n_chunks * eecu_fit[0] + tot_cells * eecu_fit[1]) * var_years
        else:
            eecu_tot = s["eecu_median_h"] * tasks if s["eecu_median_h"] else None
        if time_fit and tot_cells:
            serial_s = (n_chunks * time_fit[0] + tot_cells * time_fit[1]) * var_years
        else:
            serial_s = s["t_median_s"] * tasks if s["t_median_s"] else None
        serial_d = serial_s / 86400 if serial_s else None
        par_d = serial_d / speedup if serial_d else None
        gb = s["bytes_median"] * tasks / 1e9 if s["bytes_median"] else None
        quota_mo = eecu_tot / EECU_QUOTA_PER_MONTH if eecu_tot else None
        totals[size] = (eecu_tot, gb, par_d)
        print(
            f"{size:>6} {s['n_ok']}/{s['n']:>5} {s['max_attempts_seen']:>6} "
            f"{s['t_median_s'] or 0:>7.0f} {s['eecu_median_h'] or 0:>8.4f} "
            f"{n_chunks:>7} {tot_cells:>10} {tasks:>9.0f} {eecu_tot or 0:>11.1f} "
            f"{gb or 0:>7.2f} {quota_mo or 0:>9.2f} {serial_d or 0:>9.1f} {par_d or 0:>7.1f}"
        )
    print(
        "\nmed_s / EECU-h are the OBSERVED medians for the sampled chunks. The totals are "
        "MODELLED\n(fixed x tasks + marginal x the extent's real cell count), because "
        "sampled chunks are\nnot average chunks — see fit_per_task.\n"
        f"quota_mo = months of the {EECU_QUOTA_PER_MONTH:,.0f} EECU-h/month Contributor "
        f"quota this consumes.\nEgress priced at ${EGRESS_USD_PER_GB}/GB; EECU at "
        f"${EECU_USD_PER_HOUR}/h list (Contributor tier does not bill)."
    )
    over = {k: v for k, v in totals.items() if v[0] and v[0] > EECU_QUOTA_PER_MONTH}
    if over:
        print(
            "\n⚠ EECU is the binding constraint, not wall-clock: sizes "
            f"{sorted(over)} exceed one month of quota for this extent alone. The fixed "
            "per-task term is what they are paying, and it buys nothing — fewer, bigger "
            "chunks spend less, down to the point where a chunk stops completing."
        )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "mode",
        choices=["ladder", "parallel", "whole", "report"],
        help="which experiment to run",
    )
    ap.add_argument(
        "--extent", nargs=4, type=float, default=BRAZIL_EXTENT,
        metavar=("S", "W", "N", "E"), help="default: the E1 Brazil extent",
    )
    ap.add_argument("--variable", default="tmin", help="one variable is enough to size a chunk")
    ap.add_argument("--year", type=int, default=2020)
    ap.add_argument(
        "--sizes", nargs="+", type=int, default=[25, 100, 400],
        help="ladder mode: parents per chunk (square numbers; 100 = 10°x10° at b=4)",
    )
    ap.add_argument("--samples-per-size", type=int, default=3)
    ap.add_argument("--chunk-parents", type=int, default=100, help="parallel mode: the size to sweep")
    ap.add_argument("--n-chunks", type=int, default=8, help="parallel mode: chunks per level")
    ap.add_argument("--levels", nargs="+", type=int, default=[1, 2, 4, 8])
    ap.add_argument(
        "--max-attempts", type=int, default=2,
        help="cancel the export once EE restarts it more than this many times",
    )
    ap.add_argument("--chunk-days", type=int, default=30, help="encode band-window (RAM lever)")
    ap.add_argument("--sample", default="", help="calibration sample id, e.g. E1a")
    ap.add_argument("--data-root", default=".localdata/probe_chunks")
    ap.add_argument(
        "--min-land-cells", type=int, default=25,
        help="skip near-empty chunks when sampling; the extrapolation still counts them",
    )
    ap.add_argument("--no-db", action="store_true", help="skip the land filter (no Postgres)")
    ap.add_argument(
        "--dry-run", action="store_true", help="print the chunks that would run; no EE"
    )
    ap.add_argument(
        "--force", action="store_true",
        help=f"submit chunks above the measured cell_zones ceiling ({CELL_ZONE_CEILING})",
    )
    ap.add_argument("--also", nargs="*", help="report mode: extra data roots to fold in")
    args = ap.parse_args()
    args.land_chunks = {}

    {
        "ladder": cmd_ladder,
        "parallel": cmd_parallel,
        "whole": cmd_whole,
        "report": cmd_report,
    }[args.mode](args)


if __name__ == "__main__":
    main()
