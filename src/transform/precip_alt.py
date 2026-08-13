"""Bronze → silver transform for CHIRPS (``wth_precip_alt``).

Much thinner than ``src/transform/merge.py``, and deliberately so. That module merges seven
variables wide, converts units, and derives ``rh``/``et0``. CHIRPS is one variable, already
in mm/day, with nothing derivable from it alone — so this reads, stamps the source code,
runs QA, and hands off.

The file discovery deliberately globs (``<source>_<year>*.parquet``) rather than resolving
one exact filename. Chunked exports are not used at the current extent, but resolving by
exact name is precisely the defect that made silver silently miss chunked ERA5 bronze
(docs/plan_gee_chunked_backfill.md §A1) — a bug the 225-test suite did not catch, because
"found no files" and "there are no files" look identical downstream.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from src.gee.chirps import MAX_PRECIP_MM, source_code

KEYS = ["fparent_id", "fine_id", "date"]

# Predicate True = FAIL. NaN never fails: a masked pixel is absent data, not bad data.
CHECKS: list[tuple[str, object]] = [
    ("precip<0", lambda d: d["precip"] < 0),
    ("precip>catalog_max", lambda d: d["precip"] > MAX_PRECIP_MM),
]

# CHIRPS stores accumulations; values a hair below zero are representation noise, not a
# measurement. Snapped rather than quarantined, matching merge.NOISE_TOLERANCE.
NOISE_TOLERANCE = 0.01  # mm/day


def source_year_paths(bronze_dir: str | Path, source: str, year: int) -> list[Path]:
    """Bronze parquet files for one ``(source, year)``, sorted. See the module docstring."""
    return sorted((Path(bronze_dir) / source).glob(f"{source}_{year}*.parquet"))


def available_sources(
    bronze_dir: str | Path, year: int, sources: Sequence[str]
) -> list[str]:
    """Subset of ``sources`` with at least one parquet file for ``year``."""
    return [s for s in sources if source_year_paths(bronze_dir, s, year)]


def _dataset(paths: list[Path]):
    return ds.dataset([str(p) for p in paths], format="parquet")


def iter_fparent_batches(
    bronze_dir: str | Path,
    source: str,
    year: int,
    *,
    batch_size: int = 4,
) -> Iterator[list[str]]:
    """Yield batches of the ``fparent_id``s present in one source-year's bronze.

    Only the ``fparent_id`` column is scanned, so this is cheap. Batches are sorted for
    deterministic, resumable ordering.

    ``batch_size`` defaults lower than the ERA5 path's 8: a fine parent holds 400 cells to a
    0.25° parent's 16, so one batch of 4 is ~1,600 cell-years — comparable memory to 8
    coarse parents, on the same Pi.
    """
    paths = source_year_paths(bronze_dir, source, year)
    if not paths:
        return
    column = _dataset(paths).to_table(columns=["fparent_id"])["fparent_id"]
    ordered = sorted(column.unique().to_pylist())
    for start in range(0, len(ordered), batch_size):
        yield ordered[start : start + batch_size]


def load_source_year(
    bronze_dir: str | Path,
    source: str,
    year: int,
    fparent_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Read one bronze ``(source, year)``, optionally filtered to ``fparent_ids``.

    Returns ``[fparent_id, fine_id, date, source, precip]`` — silver's column set, with
    ``source`` already the stored SMALLINT.
    """
    paths = source_year_paths(bronze_dir, source, year)
    if not paths:
        return pd.DataFrame(columns=[*KEYS, "source", "precip"])

    filt = None if fparent_ids is None else ds.field("fparent_id").isin(list(fparent_ids))
    frame = _dataset(paths).to_table(filter=filt).to_pandas()
    frame = frame.rename(columns={"value": "precip"})
    frame["source"] = source_code(source)
    frame = snap_accumulation_noise(frame)
    return frame[[*KEYS, "source", "precip"]]


def snap_accumulation_noise(frame: pd.DataFrame, tol: float = NOISE_TOLERANCE) -> pd.DataFrame:
    """Snap sub-tolerance negative accumulations to 0.0, leaving real negatives to QA."""
    if "precip" not in frame.columns:
        return frame
    values = frame["precip"].to_numpy(dtype="float64", copy=True)
    noise = (values < 0) & (values > -tol)
    values[noise] = 0.0
    return frame.assign(precip=values)


def split_valid(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Partition into ``(good, failures)``; failures carry a semicolon-joined ``reason``."""
    if frame.empty:
        return frame, frame.assign(reason=pd.Series(dtype="object"))

    reasons = pd.Series([""] * len(frame), index=frame.index, dtype="object")
    for name, predicate in CHECKS:
        hit = predicate(frame).fillna(False).to_numpy()
        reasons[hit] = np.where(reasons[hit] == "", name, reasons[hit] + ";" + name)

    bad = reasons != ""
    return (
        frame[~bad].reset_index(drop=True),
        frame[bad].assign(reason=reasons[bad]).reset_index(drop=True),
    )


def calendar_report(frame: pd.DataFrame, year: int) -> pd.DataFrame:
    """Per-``fine_id`` ``days`` / ``expected`` / ``missing`` for ``year``. Reporting only.

    A cell short of the calendar is worth a warning, never a blocked load — CHIRPS masks
    pixels it has no estimate for, and that is legitimate absence.
    """
    import calendar as _calendar

    expected = 366 if _calendar.isleap(year) else 365
    days = frame.groupby("fine_id")["date"].nunique().rename("days").reset_index()
    days["expected"] = expected
    days["missing"] = expected - days["days"]
    return days
