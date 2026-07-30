"""Measure SEAS5 request shape, queue wait and payload size (cost-model samples C1 / C2).

**Maintainer one-off, not on the pipeline path.** CDS is free, so this probe is not
measuring money — it measures the three things ``docs/cost_model_climate_context.md`` §4
says C1/C2 must retire before any forecast code is written:

- **C1** — one live SEAS5 issuance, 2 variables: does the assumed dataset name and param
  shape actually exist? ``docs/plan_climate_context_layer.md`` §11 flags this as a real
  risk and prescribes exactly this throwaway request before Phase 4 starts.
- **C2** — one init month across all hindcast years: how many requests does the archive
  actually take, and does the per-request cost limit force a split? That is what turns
  "~12 requests" from an estimate into a number, and ×12 gives the full hindcast.

Results append to the same ``_gee_metrics.jsonl`` as the GEE probes (``kind: "probe"``,
``dataset: "cds:<name>"``) so §9 has one source of truth. ``src/cds/`` is deliberately not
modified: these are one-offs, and the CDS path is not the cost exposure.

Run::

    uv run python scripts/cds_cost_probe.py c1 --extent 12.0 -90.0 17.5 -83.0 --dry-run
    uv run python scripts/cds_cost_probe.py c1 --extent 12.0 -90.0 17.5 -83.0
    uv run python scripts/cds_cost_probe.py c2 --extent 12.0 -90.0 17.5 -83.0 --init-month 8

Always run ``--dry-run`` first: it prints the exact request dict without submitting, which
is the cheapest way to check the dataset/param assumptions below against the live CDS
catalogue. **Every constant in the DEFAULTS block is an assumption until C1 passes.**
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

from src.cds.splitter import is_cost_error
from src.config import resolve_bronze_dir
from src.gee.export import BlobStats
from src.gee.metrics import RunMetrics, append_record, metrics_path

# --- DEFAULTS: assumptions, not facts. C1 exists to confirm or correct them. ---------
DATASET = "seasonal-monthly-single-levels"
ORIGINATING_CENTRE = "ecmwf"
SYSTEM = "51"
VARIABLES = ["2m_temperature", "total_precipitation"]
PRODUCT_TYPE = ["monthly_mean"]
LEADTIME_MONTHS = ["1", "2", "3", "4", "5", "6"]
HINDCAST_YEARS = [str(y) for y in range(1993, 2017)]  # ECMWF reforecast period


def build_request(
    *, years: list[str], month: int, extent: list[float], variables: list[str]
) -> dict:
    """SEAS5 monthly request. ``area`` subsets; ``grid`` is never passed (PLANNING §11.1).

    ``extent`` is the project's ``[S, W, N, E]``; CDS ``area`` is ``[N, W, S, E]``.
    """
    s, w, n, e = extent
    return {
        "originating_centre": ORIGINATING_CENTRE,
        "system": SYSTEM,
        "variable": variables,
        "product_type": PRODUCT_TYPE,
        "year": years,
        "month": [f"{month:02d}"],
        "leadtime_month": LEADTIME_MONTHS,
        "area": [n, w, s, e],
        "data_format": "netcdf",
    }


def _retrieve(client, request: dict, target: Path) -> tuple[float, int]:
    """One CDS retrieval → (wall seconds incl. queue, bytes). Raises on failure."""
    t0 = time.monotonic()
    client.retrieve(DATASET, request, target)
    return time.monotonic() - t0, target.stat().st_size


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("sample", choices=["c1", "c2"])
    ap.add_argument(
        "--extent", nargs=4, type=float, required=True, metavar=("S", "W", "N", "E")
    )
    ap.add_argument("--init-month", type=int, default=None, help="1-12; default: now")
    ap.add_argument("--init-year", type=int, default=None, help="C1 issuance year")
    ap.add_argument("--data-root", default=None, help="where _gee_metrics.jsonl lives")
    ap.add_argument(
        "--dry-run", action="store_true", help="print the request, submit nothing"
    )
    args = ap.parse_args()

    now = time.gmtime()
    month = args.init_month or now.tm_mon
    if args.sample == "c1":
        years = [str(args.init_year or now.tm_year)]
        label = f"probe_seas5_live_{years[0]}{month:02d}"
    else:
        years = HINDCAST_YEARS
        label = f"probe_seas5_hindcast_m{month:02d}"

    request = build_request(
        years=years, month=month, extent=list(args.extent), variables=VARIABLES
    )
    if args.dry_run:
        print(f"{DATASET}\n{json.dumps(request, indent=2, sort_keys=True)}", flush=True)
        print(
            "\ndry run: nothing submitted. Confirm the dataset id and every key above "
            "against the CDS catalogue before running for real.",
            flush=True,
        )
        return

    from src.cds.client import CDSClient

    client = CDSClient()
    m = RunMetrics(
        kind="probe",
        dataset=f"cds:{DATASET}",
        name_prefix=label,
        extent=list(args.extent),
        sample=args.sample.upper(),
        land_only=False,
    )
    t_all = time.monotonic()
    n_requests, sizes, waits, split = 0, [], [], False

    try:
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            try:
                secs, size = _retrieve(client, request, td / f"{label}.nc")
                n_requests, sizes, waits = 1, [size], [secs]
            except Exception as exc:
                if not (len(years) > 1 and is_cost_error(exc)):
                    raise
                # The finding C2 exists to produce: the bulk request does not fit, so the
                # real hindcast fetch needs one request per year (x12 init months).
                split = True
                print(f"bulk request rejected on cost — splitting by year: {exc}", flush=True)
                for year in years:
                    per_year = build_request(
                        years=[year],
                        month=month,
                        extent=list(args.extent),
                        variables=VARIABLES,
                    )
                    secs, size = _retrieve(
                        client, per_year, td / f"{label}_{year}.nc"
                    )
                    n_requests += 1
                    sizes.append(size)
                    waits.append(secs)
                    print(f"  {year}: {secs:.0f}s, {size:,} B", flush=True)
    except BaseException as exc:
        m.note_error(exc)
        raise
    finally:
        total_bytes = sum(sizes)
        m.note_export(task=None, status=None, seconds=time.monotonic() - t_all)
        m.note_download(
            blobs=BlobStats(
                n_blobs=n_requests,
                bytes_remote=total_bytes,
                bytes_local=total_bytes,
                bytes_per_blob_max=max(sizes) if sizes else 0,
            ),
            seconds=sum(waits),
        )
        # months of forecast per issuance; cells stay unknown (native ~1.0 deg grid, and
        # C1/C2 are free — request count and queue behaviour are what matter here).
        m.note_units(days=len(LEADTIME_MONTHS) * len(years), exact=False)
        record = m.to_record()
        record_path = metrics_path(resolve_bronze_dir(args.data_root))
        append_record(record_path, record)

        print(f"\nrecorded -> {record_path}", flush=True)
        print(f"  dataset              {DATASET}", flush=True)
        print(f"  requests             {n_requests} (split_by_year={split})", flush=True)
        print(f"  bytes                {total_bytes:,}", flush=True)
        if waits:
            print(
                f"  wall per request     min {min(waits):.0f}s / "
                f"max {max(waits):.0f}s / total {sum(waits):.0f}s",
                flush=True,
            )
        print(f"  error                {record.get('error')}", flush=True)
        if args.sample == "c2":
            print(
                f"\nfull hindcast projection: {n_requests * 12} requests, "
                f"~{total_bytes * 12:,} B, ~{sum(waits) * 12 / 3600:.1f} h of queue",
                flush=True,
            )


if __name__ == "__main__":
    main()
