"""Measure EECU + egress for a GFS or CHIRPS export (cost-model samples E2 / E3).

**Maintainer one-off, not on the pipeline path.** It answers two questions from
``docs/cost_model_climate_context.md`` before either source is implemented:

- **E2 (GFS)** §5.1 — a *daily* job cannot be split across months to fit under the EECU
  quota the way a backfill can, so EECU-per-run × 30 vs the monthly quota is the single
  most important number in the whole model.
- **E3 (CHIRPS)** §5.2 — decides whether ``chirps_start_year`` stays at a 10-year window
  or a full 46-year history is affordable.

It runs through the *same* instrumented path as the ERA5 bronze backend
(``src/gee/metrics.run_export``) and appends to the *same* ``_gee_metrics.jsonl``, so the
numbers are directly comparable to E1 rather than measured with a different ruler. It
writes **no** bronze Parquet and touches **no** silver table: the collection builders live
here rather than in ``src/gee/`` precisely because the real modules are Phases 6 and 9 of
``docs/plan_climate_context_layer.md`` and a cost probe must not pre-commit their design.

Run::

    uv run python scripts/gee_cost_probe.py gfs --init 2026-08-01 \\
        --extent 12.0 -90.0 17.5 -83.0 --sample E2
    uv run python scripts/gee_cost_probe.py chirps --year 2020 \\
        --extent 12.0 -90.0 17.5 -83.0 --sample E3

``--keep-dir`` leaves the GeoTIFF shards on disk so ``du`` can independently confirm
``bytes_remote``. Never do that on the bronze path — the temp-dir teardown there is what
keeps the SSD from filling.

Caveat for E3: ``start_export`` pins the canonical 0.25° ``crsTransform``, so CHIRPS
(native 0.05°) is resampled server-side on the way out. Which reduction that *should* be —
nearest vs area-weighted mean — is a Phase 9 decision. This probe measures cost, not
values; do not read its output as a statement about the aggregation method.
"""

from __future__ import annotations

import argparse
import calendar
import re
import tempfile
from contextlib import nullcontext
from pathlib import Path

import ee

from src.config import resolve_bronze_dir
from src.gee.client import GEEClient
from src.gee.metrics import RunMetrics, append_record, metrics_path, run_export

GFS_COLLECTION = "NOAA/GFS0P25"
CHIRPS_COLLECTION = "UCSB-CHG/CHIRPS/DAILY"

# One band each: a probe sizes the *shape* of the work, and per-variable cost is linear in
# band count. Multiply by the real variable count when extrapolating.
GFS_BAND = "temperature_2m_above_ground"
CHIRPS_BAND = "precipitation"


def gfs_daily(init_date: str, *, lead_days: int) -> "ee.ImageCollection":
    """Daily max of one GFS init's forecast steps — one image per lead day.

    ``NOAA/GFS0P25`` is one image per (init, forecast hour); filtering on
    ``creation_time`` isolates a single run, which is what a daily refresh would fetch.

    ``init_date`` accepts ``YYYY-MM-DD`` (the 00Z run) or a full ``YYYY-MM-DDTHH:MM:SS``
    — GFS runs 00/06/12/18Z, and the plan's "up to 4x/day" question is about those, so a
    date-only argument silently measures only a quarter of the cadence. Verified live
    2026-07-30: one init carries 209 images over forecast_hours 0-384 (16 days).
    """
    init = ee.Date(init_date)
    src = (
        ee.ImageCollection(GFS_COLLECTION)
        .select(GFS_BAND)
        .filter(ee.Filter.eq("creation_time", init.millis()))
    )

    def _day(i):
        start = init.advance(ee.Number(i), "day")
        end = start.advance(24, "hour")
        img = src.filterDate(start, end).max().rename(GFS_BAND)
        return img.set(
            {"system:time_start": start.millis(), "date": start.format("YYYY-MM-dd")}
        )

    return ee.ImageCollection(ee.List.sequence(0, lead_days - 1).map(_day))


def chirps_daily(year: int) -> "ee.ImageCollection":
    """One image per day of ``year`` — CHIRPS is already daily, so this only re-stamps."""
    start = ee.Date.fromYMD(year, 1, 1)
    n_days = 366 if calendar.isleap(year) else 365
    src = ee.ImageCollection(CHIRPS_COLLECTION).select(CHIRPS_BAND)

    def _day(i):
        day_start = start.advance(ee.Number(i), "day")
        img = src.filterDate(day_start, day_start.advance(1, "day")).first()
        img = ee.Image(ee.Algorithms.If(img, img, ee.Image.constant(0).selfMask()))
        return ee.Image(img).rename(CHIRPS_BAND).set(
            {
                "system:time_start": day_start.millis(),
                "date": day_start.format("YYYY-MM-dd"),
            }
        )

    return ee.ImageCollection(ee.List.sequence(0, n_days - 1).map(_day))


def count_pixels(paths: list[Path]) -> tuple[int, int]:
    """``(valid_cells_in_band_1, total_pixels_all_bands)`` summed over shards.

    Valid band-1 pixels give ``cells`` for ``N_s`` — read from the raster rather than the
    bbox because the export is land-clipped; band 1 stands in for the rest, which is exact
    for a land mask. Total pixels (masked included) is the compression denominator: those
    bytes were stored and egressed regardless of whether the value was useful.
    """
    import rasterio

    valid = total = 0
    for path in paths:
        with rasterio.open(path) as src:
            valid += int(src.read(1, masked=True).count())
            total += src.width * src.height * src.count
    return valid, total


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("source", choices=["gfs", "chirps"])
    ap.add_argument(
        "--extent", nargs=4, type=float, required=True, metavar=("S", "W", "N", "E")
    )
    ap.add_argument(
        "--init",
        help="GFS init, YYYY-MM-DD (00Z) or YYYY-MM-DDTHH:MM:SS for a 06/12/18Z run (E2)",
    )
    ap.add_argument("--lead-days", type=int, default=16, help="GFS forecast horizon")
    ap.add_argument("--year", type=int, help="CHIRPS year (E3)")
    ap.add_argument("--sample", default="", help="calibration sample id, e.g. E2")
    ap.add_argument("--data-root", default=None, help="where _gee_metrics.jsonl lives")
    ap.add_argument("--keep-dir", default=None, help="keep shards here for a du check")
    ap.add_argument("--land-only", action="store_true", default=True)
    ap.add_argument("--no-land-only", dest="land_only", action="store_false")
    args = ap.parse_args()

    client = GEEClient()
    bucket = client.require_bucket()

    if args.source == "gfs":
        if not args.init:
            ap.error("--init is required for gfs")
        collection = gfs_daily(args.init, lead_days=args.lead_days)
        dataset, days = GFS_COLLECTION, args.lead_days
        tag = "probe_gfs_" + re.sub(r"[^0-9]", "", args.init)[:10]
    else:
        if not args.year:
            ap.error("--year is required for chirps")
        collection = chirps_daily(args.year)
        dataset = CHIRPS_COLLECTION
        days = 366 if calendar.isleap(args.year) else 365
        tag = f"probe_chirps_{args.year}"

    name_prefix = f"{client.gcs_prefix}/{tag}"
    m = RunMetrics(
        kind="probe",
        dataset=dataset,
        name_prefix=name_prefix,
        extent=list(args.extent),
        sample=args.sample or None,
        land_only=args.land_only,
        gee_project=client.project,
        track_cells=False,  # nothing is encoded; cells come from the raster below
    )

    print(f"{tag}: exporting {days} daily bands over {args.extent}...", flush=True)
    keep = Path(args.keep_dir) if args.keep_dir else None
    if keep:
        keep.mkdir(parents=True, exist_ok=True)
    ctx = nullcontext(str(keep)) if keep else tempfile.TemporaryDirectory()
    try:
        with ctx as dest:
            paths = run_export(
                collection,
                list(args.extent),
                bucket=bucket,
                name_prefix=name_prefix,
                description=tag,
                dest_dir=dest,
                metrics=m,
                land_only=args.land_only,
            )
            valid, total = count_pixels(paths)
            m.note_units(cells=valid, days=days)
            m.note_raster_chunk(total)
    except BaseException as exc:
        m.note_error(exc)
        raise
    finally:
        record = m.to_record()
        path = metrics_path(resolve_bronze_dir(args.data_root))
        append_record(path, record)
        print(f"recorded -> {path}", flush=True)
        for key in (
            "task_id", "eecu_hours", "bytes_remote", "n_blobs", "cells", "days",
            "n_units", "bytes_per_unit", "eecu_per_unit", "raster_pixels",
            "compression_ratio", "land_fraction",
            "t_export_s", "t_download_s", "ee_queue_s", "ee_compute_s", "error",
        ):
            print(f"  {key:20s} {record.get(key)}", flush=True)
        if record.get("eecu_hours") is None and not record.get("error"):
            print(
                "  NOTE: EE reported no batch_eecu_usage_seconds — read EECU for "
                f"task {record.get('task_id')} off https://code.earthengine.google.com/tasks",
                flush=True,
            )


if __name__ == "__main__":
    main()
