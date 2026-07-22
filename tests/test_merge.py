"""Bronze long → silver wide tests (PLANNING.md §5.1, §8.3).

Synthetic bronze parquets on tmp_path; no DB and no network. Covers the unit contract,
the outer-join degradation rule, and the parent-batched reader.
"""

import numpy as np
import pandas as pd
import pytest

from src.cds.variables import ALL_VARIABLES
from src.transform import merge
from src.transform.units import CONVERSIONS, KELVIN_OFFSET

B = 4
CHILD, PARENT = "EU9K", "0XKE"
DATES = [pd.Timestamp("2020-01-01").date(), pd.Timestamp("2020-01-02").date()]

# Native ERA5 units: K, m, J/m², m/s (§5.1).
NATIVE = {
    "tmax": 303.15, "tmin": 288.15, "tdew": 291.15,
    "precip": 0.005, "srad": 22e6, "wind_u": 3.0, "wind_v": 4.0,
}


def _write_bronze(root, year=2020, variables=ALL_VARIABLES, children=(CHILD,)):
    for var in variables:
        frame = pd.DataFrame(
            [
                {"child_id": c, "parent_id": PARENT, "date": d, "value": NATIVE[var]}
                for c in children
                for d in DATES
            ]
        )
        path = merge.var_year_path(root, var, year)
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)
    return root


def _cell_meta(children=(CHILD,)):
    return pd.DataFrame(
        {"child_id": list(children), "lat": [-34.9] * len(children),
         "elevation": [50.0] * len(children)}
    )


def test_conversions_cover_the_download_contract():
    # units.py asserts this at import; lock it here too so a drift is a test failure.
    assert set(CONVERSIONS) == set(ALL_VARIABLES)


def test_merge_wide_applies_silver_units(tmp_path):
    _write_bronze(tmp_path)
    frames = {v: merge.load_var_year(tmp_path, v, 2020) for v in ALL_VARIABLES}

    wide = merge.merge_wide(frames)
    row = wide.iloc[0]

    assert row["tmax"] == pytest.approx(NATIVE["tmax"] - KELVIN_OFFSET)   # K -> °C
    assert row["tmin"] == pytest.approx(NATIVE["tmin"] - KELVIN_OFFSET)
    assert row["tdew"] == pytest.approx(NATIVE["tdew"] - KELVIN_OFFSET)
    assert row["precip"] == pytest.approx(5.0)                            # m -> mm
    assert row["srad"] == pytest.approx(22.0)                             # J/m² -> MJ/m²
    assert row["wind"] == pytest.approx(5.0)                              # sqrt(3² + 4²)
    assert "wind_u" not in wide.columns and "wind_v" not in wide.columns


def test_merge_wide_sorted_and_keyed(tmp_path):
    _write_bronze(tmp_path, children=("EU9L", CHILD))
    frames = {v: merge.load_var_year(tmp_path, v, 2020) for v in ALL_VARIABLES}
    wide = merge.merge_wide(frames)

    assert len(wide) == 4  # 2 cells × 2 days
    assert list(wide["child_id"]) == sorted(wide["child_id"])
    assert not wide.duplicated(["child_id", "date"]).any()


def test_missing_variable_becomes_null_column(tmp_path):
    # srad absent from bronze: the column exists, all NaN, and rows survive (§8.3).
    present = [v for v in ALL_VARIABLES if v != "srad"]
    _write_bronze(tmp_path, variables=present)

    assert merge.available_variables(tmp_path, 2020) == present

    frames = {v: merge.load_var_year(tmp_path, v, 2020) for v in present}
    wide = merge.merge_wide(frames)
    assert wide["srad"].isna().all()
    assert len(wide) == 2


def test_add_derived_nulls_et0_for_unknown_cell(tmp_path):
    _write_bronze(tmp_path)
    frames = {v: merge.load_var_year(tmp_path, v, 2020) for v in ALL_VARIABLES}
    wide = merge.merge_wide(frames)

    derived = merge.add_derived(wide, _cell_meta(children=("ZZZZ",)))  # no meta for CHILD
    assert derived["et0"].isna().all()
    assert derived["rh"].notna().all()  # rh needs no static input
    assert "lat" not in derived.columns and "elevation" not in derived.columns


def test_build_wide_produces_all_value_columns(tmp_path):
    _write_bronze(tmp_path)
    wide = merge.build_wide(tmp_path, 2020, [PARENT], _cell_meta())

    for column in merge.VALUE_COLUMNS:
        assert column in wide.columns
    assert wide["rh"].between(0, 100).all()
    assert np.isfinite(wide["et0"]).all()


def test_parent_batches_filter_the_scan(tmp_path):
    _write_bronze(tmp_path)
    batches = list(merge.iter_parent_batches(tmp_path, 2020, batch_size=8))
    assert batches == [[PARENT]]

    assert len(merge.load_var_year(tmp_path, "tmax", 2020, [PARENT])) == 2
    assert merge.load_var_year(tmp_path, "tmax", 2020, ["ZZZZ"]).empty


def test_float_noise_negatives_snap_to_zero(tmp_path):
    # Real 2020 bronze holds precip down to -1.1e-5 mm on dry days — zeros, not bad data.
    _write_bronze(tmp_path)
    frames = {v: merge.load_var_year(tmp_path, v, 2020) for v in ALL_VARIABLES}
    frames["precip"]["precip"] = -1.1e-8   # metres in, ~-1.1e-5 mm out
    frames["srad"]["srad"] = -5.0          # J/m² -> -5e-6 MJ/m²

    wide = merge.merge_wide(frames)
    assert (wide["precip"] == 0.0).all()
    assert (wide["srad"] == 0.0).all()


def test_real_negative_accumulation_is_left_for_qa(tmp_path):
    # Beyond tolerance it is not noise; QA must still see it (§8.4).
    _write_bronze(tmp_path)
    frames = {v: merge.load_var_year(tmp_path, v, 2020) for v in ALL_VARIABLES}
    frames["precip"]["precip"] = -0.005  # -5 mm

    wide = merge.merge_wide(frames)
    assert (wide["precip"] == -5.0).all()


def test_build_wide_without_bronze_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        merge.build_wide(tmp_path, 1999, [PARENT], _cell_meta())
