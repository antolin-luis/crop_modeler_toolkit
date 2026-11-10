"""Field-level bronze QA tests (PLANNING.md §8.4).

Synthetic bronze parquets on tmp_path; no DB and no network. The thresholds under test are
calibrated against the real archive, so the false-positive cases here carry the magnitudes
actually measured there — a dry `precip` day is float noise around zero in *metres*, and
the one real `precip` finding is 19 mm.
"""

import numpy as np
import pandas as pd
import pytest

from src.transform import field_qa
from src.transform.merge import NOISE_TOLERANCE

YEAR = 1987
PARENT = "0XKE"
DATES = [pd.Timestamp(f"{YEAR}-01-{d:02d}").date() for d in (25, 26, 27)]

# Enough cells to clear MIN_CELLS; the real legacy region is 412.
CHILDREN = [f"C{i:03d}" for i in range(64)]


def _write(root, variable, values_by_date, year=YEAR, suffix=""):
    """Write one bronze parquet. ``values_by_date`` maps date -> array over CHILDREN."""
    rows = [
        {"child_id": c, "parent_id": PARENT, "date": d, "value": float(v)}
        for d, values in values_by_date.items()
        for c, v in zip(CHILDREN, values)
    ]
    path = root / variable / f"{variable}_{year}{suffix}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _varied(seed, centre, spread):
    rng = np.random.default_rng(seed)
    return centre + rng.normal(0.0, spread, size=len(CHILDREN))


def _normal_tmin():
    return {d: _varied(i, 295.0, 1.5) for i, d in enumerate(DATES)}


# --- constant_field -------------------------------------------------------------------


def test_constant_field_caught(tmp_path):
    """The 1987-01-26 shape: one day identical across every cell."""
    values = _normal_tmin()
    values[DATES[1]] = np.full(len(CHILDREN), 267.660614)

    found = field_qa.scan_file(_write(tmp_path, "tmin", values), "tmin")

    hits = found[found["detector"] == field_qa.DETECTOR_CONSTANT]
    assert list(hits["date"]) == [DATES[1]]
    assert int(hits.iloc[0]["cells"]) == len(CHILDREN)
    # Detail is recorded in silver units — °C, not K.
    assert hits.iloc[0]["detail"]["min"] == pytest.approx(-5.489386, abs=1e-5)


def test_normal_day_not_caught(tmp_path):
    found = field_qa.scan_file(_write(tmp_path, "tmin", _normal_tmin()), "tmin")
    assert found.empty


def test_below_min_cells_not_judged(tmp_path):
    """A constant field over a handful of cells is not evidence."""
    few = CHILDREN[: field_qa.MIN_CELLS - 1]
    rows = [
        {"child_id": c, "parent_id": PARENT, "date": DATES[0], "value": 267.660614}
        for c in few
    ]
    path = tmp_path / "tmin" / f"tmin_{YEAR}.parquet"
    path.parent.mkdir(parents=True)
    pd.DataFrame(rows).to_parquet(path, index=False)

    assert field_qa.scan_file(path, "tmin").empty


# --- accumulated variables: the magnitude gate ----------------------------------------


def test_dry_precip_day_not_caught(tmp_path):
    """A rainless day is one repeated near-zero — data, not a defect.

    Bronze precip is in **metres**, so 1e-12 m = 1e-9 mm: the float noise around zero that
    NOISE_TOLERANCE already exists to absorb.
    """
    values = {DATES[0]: _varied(0, 3e-3, 5e-4), DATES[2]: _varied(1, 2e-3, 4e-4)}
    values[DATES[1]] = np.full(len(CHILDREN), 1e-12)

    found = field_qa.scan_file(_write(tmp_path, "precip", values), "precip")
    assert found.empty


def test_light_uniform_precip_not_caught(tmp_path):
    """A region-wide drizzle with 0.0032 mm of spread is real weather."""
    values = {d: _varied(i, 2e-3, 3e-4) for i, d in enumerate(DATES)}
    values[DATES[1]] = np.linspace(0.0, 3.2e-6, len(CHILDREN))  # metres -> 0.0032 mm

    found = field_qa.scan_file(_write(tmp_path, "precip", values), "precip")
    assert found.empty


def test_uniform_nonzero_precip_caught(tmp_path):
    """The 1998-05-19 shape: every cell at the same 19 mm. Not a dry day."""
    values = {d: _varied(i, 3e-3, 5e-4) for i, d in enumerate(DATES)}
    values[DATES[1]] = np.full(len(CHILDREN), 0.019002625718712807)  # metres -> 19.0 mm

    found = field_qa.scan_file(_write(tmp_path, "precip", values), "precip")

    hits = found[found["detector"] == field_qa.DETECTOR_CONSTANT]
    assert list(hits["date"]) == [DATES[1]]
    assert hits.iloc[0]["detail"]["min"] == pytest.approx(19.0026, abs=1e-3)


def test_magnitude_gate_runs_in_silver_units(tmp_path):
    """Guard against gating raw bronze: 0.01 m would be a 10 mm/day threshold.

    A uniform 0.5 mm day is above the mm tolerance and must be caught; if the gate ever
    compares raw metres it would be 0.0005 < 0.01 and slip through.
    """
    values = {d: _varied(i, 3e-3, 5e-4) for i, d in enumerate(DATES)}
    values[DATES[1]] = np.full(len(CHILDREN), 5e-4)  # metres -> 0.5 mm

    found = field_qa.scan_file(_write(tmp_path, "precip", values), "precip")
    assert list(found["date"]) == [DATES[1]]
    assert NOISE_TOLERANCE["precip"] == 0.01  # the tolerance is in mm, not metres


# --- low_spread -----------------------------------------------------------------------


def test_low_spread_catches_near_constant_field(tmp_path):
    """A field that is *nearly* constant is invisible to the equality test."""
    values = _normal_tmin()
    values[DATES[1]] = 267.660614 + np.linspace(0.0, 1e-4, len(CHILDREN))

    found = field_qa.scan_file(_write(tmp_path, "tmin", values), "tmin")

    assert list(found["detector"]) == [field_qa.DETECTOR_LOW_SPREAD]
    assert list(found["date"]) == [DATES[1]]


def test_low_spread_never_applies_to_accumulated(tmp_path):
    """On precip it is pure noise — 1,211 archive hits, zero of them real."""
    values = {d: _varied(i, 3e-3, 5e-4) for i, d in enumerate(DATES)}
    values[DATES[1]] = 2e-3 + np.linspace(0.0, 1e-9, len(CHILDREN))

    found = field_qa.scan_file(_write(tmp_path, "precip", values), "precip")
    assert field_qa.DETECTOR_LOW_SPREAD not in set(found["detector"])


# --- per-file scanning ----------------------------------------------------------------


def test_scan_var_year_scans_each_file_separately(tmp_path):
    """The 1981-08-11 shape: constant in one chunk, normal in another.

    Merged, that day has thousands of distinct values and vanishes. This is the whole
    reason the scan is per file.
    """
    legacy = _normal_tmin()
    legacy[DATES[1]] = np.full(len(CHILDREN), 265.440460)
    _write(tmp_path, "tmin", legacy)
    _write(tmp_path, "tmin", _normal_tmin(), suffix="__s20r004c-003")

    found = field_qa.scan_var_year(tmp_path, "tmin", YEAR)

    assert list(found["date"]) == [DATES[1]]
    assert found.iloc[0]["detail"]["chunk_id"] is None
    assert found.iloc[0]["detail"]["file"] == f"tmin_{YEAR}.parquet"


def test_chunk_id_recorded(tmp_path):
    values = _normal_tmin()
    values[DATES[1]] = np.full(len(CHILDREN), 265.440460)
    _write(tmp_path, "tmin", values, suffix="__s20r004c-003")

    found = field_qa.scan_var_year(tmp_path, "tmin", YEAR)
    assert found.iloc[0]["detail"]["chunk_id"] == "s20r004c-003"


def test_scan_archive_ignores_non_era5_directories(tmp_path):
    """CHIRPS lives under bronze/ too and is out of scope — variables come from the
    download contract, not from listing the directory."""
    values = _normal_tmin()
    values[DATES[1]] = np.full(len(CHILDREN), 267.660614)
    _write(tmp_path, "tmin", values)
    _write(tmp_path, "chirps_v2", values)

    found = field_qa.scan_archive(tmp_path, [YEAR])
    assert set(found["variable"]) == {"tmin"}


def test_missing_var_year_is_empty(tmp_path):
    assert field_qa.scan_var_year(tmp_path, "tmin", 1999).empty
    assert list(field_qa.scan_var_year(tmp_path, "tmin", 1999).columns) == field_qa.FINDING_COLUMNS


# --- consolidation --------------------------------------------------------------------


def test_consolidate_sums_cells_across_a_var_years_files(tmp_path):
    """1987-01-26 is one corrupt band in three files, not three incidents.

    The registry keys on (variable, date, detector), so the per-file rows must be summed
    before they are written or they overwrite each other and report the last chunk's count.
    """
    values = _normal_tmin()
    values[DATES[1]] = np.full(len(CHILDREN), 267.660614)
    _write(tmp_path, "tmin", values)
    _write(tmp_path, "tmin", values, suffix="__s20r004c-003")
    _write(tmp_path, "tmin", values, suffix="__s20r005c-003")

    found = field_qa.scan_var_year(tmp_path, "tmin", YEAR)
    assert len(found) == 3

    merged = field_qa.consolidate(found)
    assert len(merged) == 1
    assert int(merged.iloc[0]["cells"]) == 3 * len(CHILDREN)
    # Per-file evidence is kept, not discarded.
    assert len(merged.iloc[0]["detail"]["files"]) == 3
    assert {f["chunk_id"] for f in merged.iloc[0]["detail"]["files"]} == {
        None, "s20r004c-003", "s20r005c-003",
    }


def test_consolidate_keeps_distinct_incidents_apart(tmp_path):
    tmin = _normal_tmin()
    tmin[DATES[1]] = np.full(len(CHILDREN), 267.660614)
    _write(tmp_path, "tmin", tmin)

    precip = {d: _varied(i, 3e-3, 5e-4) for i, d in enumerate(DATES)}
    precip[DATES[0]] = np.full(len(CHILDREN), 0.019002625718712807)
    _write(tmp_path, "precip", precip)

    merged = field_qa.consolidate(field_qa.scan_archive(tmp_path, [YEAR]))
    assert len(merged) == 2
    assert set(merged["variable"]) == {"tmin", "precip"}


def test_consolidate_empty_keeps_the_schema():
    empty = field_qa.consolidate(pd.DataFrame(columns=field_qa.FINDING_COLUMNS))
    assert empty.empty
    assert list(empty.columns) == field_qa.FINDING_COLUMNS
