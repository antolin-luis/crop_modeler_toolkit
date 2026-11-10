"""Field-level bronze QA — whole-day corruption that row checks cannot see (§8.4).

``src.transform.qa`` validates one row against physics: ``tmax<tmin``, ``precip<0``, and
so on. A field that is *uniformly* wrong violates none of those. On 1987-01-26 the bronze
``tmin`` field is a single constant across every cell in the archive; every value is
individually plausible, so the row checks passed it and ~9k cells went into silver serving
a −5.49 °C tropical minimum.

Two properties make this module's shape non-negotiable:

**It is a pre-pass, not a row check.** ``transform_silver`` commits ~8 parents (~128
cells) at a time, so a "constant across all cells" test run inside the batch loop compares
128 values and sees nothing. The scan has to see a whole var-year before any batching.

**It scans one file at a time.** A var-year is one file *per spatial chunk*
(``merge.var_year_paths``). Merging the chunks first dilutes a regional defect below any
threshold: 1981-08-11 ``tmin`` is a single constant in the legacy region and perfectly
normal in the two Brazil chunks, so the merged day shows 7,597 distinct values and
disappears. Scan per file, union the findings, carry the chunk id.

Magnitudes are compared in **silver units**, never raw bronze. Bronze stores what the
source delivered — precip in *metres* (``src.transform.units``) — so gating raw values
against a mm tolerance would suppress every rain event under 10 mm. Every threshold here
runs through ``units.convert`` first.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds

from src.cds.variables import ALL_VARIABLES
from src.transform.merge import NOISE_TOLERANCE, var_year_paths
from src.transform.units import convert

# A constant field over a handful of cells is not evidence of anything — a small enough
# region legitimately shares one value. Below this many cells a day is not judged.
MIN_CELLS = 32

# `low_spread` fires when a day's spread collapses relative to the variable's typical
# daily spread. Measured on the full archive: it is silent on every smooth variable (the
# clean-data behaviour a guard should have) and useless on accumulated ones — see
# ACCUMULATED below.
LOW_SPREAD_FACTOR = 0.02

# Accumulated variables (precip, srad) get `constant_field` only. `low_spread` on them is
# pure noise: a region-wide day of uniformly light drizzle is real weather, and on the
# legacy region alone it produced 1,211 hits that survive every magnitude gate, against
# zero real findings. The exact-equality test does separate cleanly — of 598 raw
# `constant_field` precip hits, 597 are float noise around zero (below 1e-5 mm, the
# artifact NOISE_TOLERANCE already exists to absorb) and the one survivor is real.
ACCUMULATED = frozenset(NOISE_TOLERANCE)

DETECTOR_CONSTANT = "constant_field"
DETECTOR_LOW_SPREAD = "low_spread"

FINDING_COLUMNS = ["variable", "date", "detector", "cells", "detail"]

# pyarrow aggregate names, verified against pyarrow 24.0.0. `count_distinct` is the real
# spelling — there is no `count_distinct_exact` and no `nunique` aggregate — and grouping
# is a Table method, not a Dataset one.
_AGGREGATIONS = [
    ("value", "count_distinct"),
    ("value", "stddev"),
    ("value", "count"),
    ("value", "min"),
    ("value", "max"),
]


def chunk_id(path: Path) -> str | None:
    """Spatial chunk id encoded in a bronze filename, or ``None`` for an unchunked file.

    ``tmin_1987__s20r004c-003.parquet`` -> ``s20r004c-003`` (see ``merge.var_year_paths``).
    """
    stem = Path(path).stem
    return stem.split("__", 1)[1] if "__" in stem else None


def aggregate_by_date(path: str | Path) -> pd.DataFrame:
    """Per-date aggregate of one bronze file, reading only ``date`` and ``value``.

    Streams through pyarrow rather than materialising the file in pandas — a var-year
    chunk is ~3.9M rows, and the scan runs over the whole archive.
    """
    table = ds.dataset(str(path), format="parquet").to_table(columns=["date", "value"])
    frame = table.group_by("date").aggregate(_AGGREGATIONS).to_pandas()
    return frame.sort_values("date", ignore_index=True)


def detect_constant_field(agg: pd.DataFrame, variable: str) -> pd.DataFrame:
    """Days where every cell carries the identical value (§ module docstring).

    For accumulated variables the value must also exceed ``NOISE_TOLERANCE`` *in silver
    units* — a dry day really is one repeated near-zero, and that is data, not a defect.
    A uniform value **above** the tolerance is not a dry day and stays a finding.
    """
    hit = (agg["value_count_distinct"] == 1) & (agg["value_count"] >= MIN_CELLS)

    if variable in ACCUMULATED:
        magnitude = convert(variable, agg["value_min"]).abs()
        hit &= magnitude > NOISE_TOLERANCE[variable]

    return agg.loc[hit]


def detect_low_spread(agg: pd.DataFrame, variable: str) -> pd.DataFrame:
    """Days whose spread collapses against the variable's own typical daily spread.

    Catches a field that is *nearly* — not exactly — constant, which
    :func:`detect_constant_field` cannot see. Never applied to accumulated variables (see
    ``ACCUMULATED``); returns an empty selection for them rather than raising, so a caller
    can run both detectors over every variable unconditionally.
    """
    if variable in ACCUMULATED:
        return agg.iloc[:0]

    median = agg["value_stddev"].median()
    if not median > 0:
        return agg.iloc[:0]

    return agg.loc[
        (agg["value_stddev"] < LOW_SPREAD_FACTOR * median)
        & (agg["value_count_distinct"] > 1)
        & (agg["value_count"] >= MIN_CELLS)
    ]


def _findings(rows: pd.DataFrame, variable: str, detector: str, path: Path) -> list[dict]:
    """Shape detector hits into registry rows, recording values in silver units."""
    out = []
    for row in rows.itertuples(index=False):
        out.append(
            {
                "variable": variable,
                "date": row.date,
                "detector": detector,
                "cells": int(row.value_count),
                "detail": {
                    "file": Path(path).name,
                    "chunk_id": chunk_id(path),
                    "distinct": int(row.value_count_distinct),
                    # Silver units — the units a consumer of wth_base would see.
                    "min": float(convert(variable, row.value_min)),
                    "max": float(convert(variable, row.value_max)),
                    "stddev": float(convert(variable, row.value_stddev)),
                },
            }
        )
    return out


def scan_file(path: str | Path, variable: str) -> pd.DataFrame:
    """Run every detector over one bronze file. One row per suspect ``(variable, date)``."""
    agg = aggregate_by_date(path)
    rows: list[dict] = []
    rows += _findings(detect_constant_field(agg, variable), variable, DETECTOR_CONSTANT, Path(path))
    rows += _findings(detect_low_spread(agg, variable), variable, DETECTOR_LOW_SPREAD, Path(path))
    return pd.DataFrame(rows, columns=FINDING_COLUMNS)


def scan_var_year(bronze_dir: str | Path, variable: str, year: int) -> pd.DataFrame:
    """Scan every file of one var-year **separately** and union the findings.

    Per file, not per var-year: see the module docstring on 1981-08-11.
    """
    parts = [scan_file(path, variable) for path in var_year_paths(bronze_dir, variable, year)]
    if not parts:
        return pd.DataFrame(columns=FINDING_COLUMNS)
    return pd.concat(parts, ignore_index=True)


def consolidate(findings: pd.DataFrame) -> pd.DataFrame:
    """Collapse per-file findings into one row per ``(variable, date, detector)``.

    Detection is per file and must stay that way (see the module docstring), but an
    *incident* is not per file: 1987-01-26 is one corrupt band that shows up in all three
    of that var-year's files. The registry keys on ``(variable, date, detector)``, so
    writing the raw per-file rows would let them overwrite each other and report the last
    chunk's cell count instead of the incident's 9,842.

    Cells sum — chunk files tile without overlap (``merge.var_year_paths``) — and each
    file's evidence is kept under ``detail["files"]`` rather than thrown away.
    """
    if findings.empty:
        return pd.DataFrame(columns=FINDING_COLUMNS)

    rows = []
    for (variable, date, detector), group in findings.groupby(
        ["variable", "date", "detector"], sort=True
    ):
        details = list(group["detail"])
        rows.append(
            {
                "variable": variable,
                "date": date,
                "detector": detector,
                "cells": int(group["cells"].sum()),
                "detail": {
                    "files": details,
                    "min": min(d["min"] for d in details),
                    "max": max(d["max"] for d in details),
                },
            }
        )
    return pd.DataFrame(rows, columns=FINDING_COLUMNS)


def scan_archive(
    bronze_dir: str | Path,
    years: range | list[int],
    variables: list[str] | None = None,
) -> pd.DataFrame:
    """Scan every ``(variable, year)`` under ``bronze_dir``.

    Variables come from ``ALL_VARIABLES``, **not** from listing ``bronze_dir`` — that
    directory also holds CHIRPS exports, which are a separate product with their own
    grid and are out of scope here.
    """
    selected = list(variables) if variables is not None else list(ALL_VARIABLES)
    parts = [scan_var_year(bronze_dir, v, y) for v in selected for y in years]
    parts = [p for p in parts if not p.empty]
    if not parts:
        return pd.DataFrame(columns=FINDING_COLUMNS)
    return pd.concat(parts, ignore_index=True).sort_values(
        ["variable", "date"], ignore_index=True
    )
