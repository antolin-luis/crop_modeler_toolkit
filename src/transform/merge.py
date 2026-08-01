"""Per-variable bronze parquet → wide silver frame (PLANNING.md §8.3).

Bronze is a long ``[child_id, parent_id, date, value]`` layout, one *or more* files per
variable per year (§7 — a chunked GEE export writes one file per spatial chunk); silver is
one wide row per ``(child_id, date)`` (§8.2). This module does that reshape, applies the
§5.1 unit conversions, and derives the columns that need several aligned variables at
once: ``wind``, ``rh`` (Tetens) and ``et0`` (FAO-56).

**Everything is parent-batched.** A whole var-year for a continental extent is ~20M rows
(~1.4 GB in pandas) times seven variables — far past what the Pi can hold (§2). Reading a
batch of ``parent_id``s at a time keeps the working set bounded and independent of extent
size, and it costs nothing extra: ``parent_id`` is a stored bronze column, so the filter
is pushed down into the parquet scan.

Bronze row order is unspecified (the GEE backend writes streamed band-windows), so the
output is explicitly sorted by ``(child_id, date)`` — which is also the order that makes
a cell's time series contiguous on disk in silver (§8.2).
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from src.cds.variables import ALL_VARIABLES
from src.transform.et0 import et0_fao56
from src.transform.humidity import relative_humidity
from src.transform.units import convert

KEYS = ["parent_id", "child_id", "date"]

# Columns of wth_base that carry an observation (§8.2), in DDL order.
VALUE_COLUMNS = ["tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0"]

# Accumulated variables arrive as float32 sums of hourly increments, so a dry cell-day
# lands a hair below zero (observed floor: -1.1e-5 mm on 2020 bronze). Those are zeros,
# not bad data — snapping them keeps ~19% of a year's rows out of QA quarantine (§8.4).
# The tolerance is far below any meaningful measurement, so a genuinely negative
# accumulation still fails QA.
NOISE_TOLERANCE = {"precip": 0.01, "srad": 0.01}  # mm/day, MJ/m²/day


def var_year_paths(bronze_dir: str | Path, variable: str, year: int) -> list[Path]:
    """Bronze parquet files for one ``(variable, year)`` — the §7 layout, sorted.

    A var-year is **one or more** files. The GEE backend splits a continental extent into
    parent-aligned export chunks (``src/gee/chunks.py``) and writes one
    ``<var>_<year>__<chunk_id>.parquet`` per chunk, because a single whole-extent export
    does not complete. An unchunked var-year is the same thing with one file, so the glob
    covers both and the CDS backend needs no change.

    Chunks tile without overlap, so the files concatenate with no deduplication — see the
    ``src.gee.chunks`` module docstring for why that property is guaranteed by
    construction. Sorted so a batch reads its inputs in the same order every run.
    """
    return sorted((Path(bronze_dir) / variable).glob(f"{variable}_{year}*.parquet"))


def _dataset(paths: list[Path]):
    """Parquet dataset over one var-year's files. ``str`` — pyarrow lists want plain paths."""
    return ds.dataset([str(p) for p in paths], format="parquet")


def available_variables(
    bronze_dir: str | Path, year: int, variables: Sequence[str] = ALL_VARIABLES
) -> list[str]:
    """Subset of ``variables`` with at least one parquet file for ``year``."""
    return [v for v in variables if var_year_paths(bronze_dir, v, year)]


def iter_parent_batches(
    bronze_dir: str | Path,
    year: int,
    variables: Sequence[str] = ALL_VARIABLES,
    *,
    batch_size: int = 8,
) -> Iterator[list[str]]:
    """Yield batches of the ``parent_id``s present in ``year``'s bronze files.

    Only the ``parent_id`` column is scanned, so this is cheap even for a continental
    extent. Batches are sorted for deterministic, resumable task ordering.
    """
    parents: set[str] = set()
    for var in available_variables(bronze_dir, year, variables):
        table = _dataset(var_year_paths(bronze_dir, var, year))
        column = table.to_table(columns=["parent_id"])["parent_id"]
        parents.update(column.unique().to_pylist())

    ordered = sorted(parents)
    for start in range(0, len(ordered), batch_size):
        yield ordered[start : start + batch_size]


def load_var_year(
    bronze_dir: str | Path,
    variable: str,
    year: int,
    parent_ids: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Read one bronze ``(variable, year)``, optionally filtered to ``parent_ids``.

    Returns the long frame with ``value`` renamed to ``variable``. Chunked var-years are
    read as one dataset over their files, so the ``parent_ids`` filter still pushes down
    into each parquet scan and a parent split across chunk files comes back whole.
    """
    dataset = _dataset(var_year_paths(bronze_dir, variable, year))
    filt = None if parent_ids is None else ds.field("parent_id").isin(list(parent_ids))
    frame = dataset.to_table(filter=filt).to_pandas()
    return frame.rename(columns={"value": variable})


def merge_wide(frames: dict[str, pd.DataFrame], *, convert_units: bool = True) -> pd.DataFrame:
    """Outer-join per-variable long frames into one wide frame on ``(parent, child, date)``.

    An **outer** join is deliberate: a cell-day present for some variables but not others
    keeps its row with NaN in the gaps, and the dependent derived values degrade to NULL
    (§8.3) instead of the whole cell-day disappearing. Variables absent from ``frames``
    become all-NaN columns for the same reason.
    """
    if not frames:
        raise ValueError("merge_wide needs at least one variable frame")

    wide: pd.DataFrame | None = None
    for variable, frame in frames.items():
        part = frame[[*KEYS, variable]]
        if convert_units:
            part = part.assign(**{variable: convert(variable, part[variable].to_numpy())})
        wide = part if wide is None else wide.merge(part, on=KEYS, how="outer")

    assert wide is not None
    for variable in ALL_VARIABLES:
        if variable not in wide.columns:
            wide[variable] = np.nan

    # 10 m wind speed from its components (§5.1); the components are not stored in silver.
    wide["wind"] = np.sqrt(wide["wind_u"] ** 2 + wide["wind_v"] ** 2)
    wide = wide.drop(columns=["wind_u", "wind_v"])

    wide = snap_accumulation_noise(wide)
    return wide.sort_values(["child_id", "date"], ignore_index=True)


def snap_accumulation_noise(wide: pd.DataFrame) -> pd.DataFrame:
    """Snap sub-tolerance negative accumulations to exactly zero (see NOISE_TOLERANCE)."""
    for column, tolerance in NOISE_TOLERANCE.items():
        if column in wide.columns:
            values = wide[column]
            wide[column] = values.mask(values.between(-tolerance, 0.0, inclusive="left"), 0.0)
    return wide


def add_derived(wide: pd.DataFrame, cell_meta: pd.DataFrame) -> pd.DataFrame:
    """Add ``rh`` (Tetens §12.1) and ``et0`` (FAO-56 §12.2) to a wide frame.

    ``cell_meta`` carries the static per-cell ``lat`` and ``elevation`` from
    ``era5_land_base_grid`` (indexed by ``child_id``) — ET0 needs both. Cells missing
    from ``cell_meta`` get NaN inputs and therefore a NULL ``et0``, never a wrong one.
    """
    out = wide.merge(cell_meta[["child_id", "lat", "elevation"]], on="child_id", how="left")

    out["rh"] = relative_humidity(out["tmax"], out["tmin"], out["tdew"])
    doy = pd.to_datetime(out["date"]).dt.dayofyear.to_numpy()
    out["et0"] = et0_fao56(
        out["tmax"], out["tmin"], out["srad"], out["wind"], out["tdew"],
        out["elevation"], out["lat"], doy,
    )

    return out.drop(columns=["lat", "elevation"])


def build_wide(
    bronze_dir: str | Path,
    year: int,
    parent_ids: Sequence[str],
    cell_meta: pd.DataFrame,
    variables: Sequence[str] = ALL_VARIABLES,
) -> pd.DataFrame:
    """Load one parent batch of one year from bronze and return the derived wide frame."""
    present = available_variables(bronze_dir, year, variables)
    if not present:
        raise FileNotFoundError(f"no bronze parquet for year {year} in {bronze_dir}")

    frames = {v: load_var_year(bronze_dir, v, year, parent_ids) for v in present}
    return add_derived(merge_wide(frames), cell_meta)
