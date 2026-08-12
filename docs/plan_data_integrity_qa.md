# Field-Level QA, an Issue Registry, and a Repair Ladder

**Status:** proposed. No code written. Per CLAUDE.md the coding phase opens on a fresh
branch off `main` (suggested: `qa-field-repair`).

**Why:** on 1987-01-26 the bronze `tmin` field is a single constant — 267.660614 K
(−5.49 °C) — across **all 9,430 cells**. Every other variable that day is normal. One
corrupt hourly `temperature_2m` band, picked up everywhere by the daily-*min* reducer and
never seen by the *max* reducer.

The QA node did not notice. `src/transform/qa.py:24-30` is entirely row-level
(`tmax<tmin`, `precip<0`, `srad<0`, `rh_out_of_range`, `et0<0`), and a field that is
*uniformly* wrong violates none of them. Only 712 of ~9,430 cells were caught, and only as
a side effect of ET0 tipping negative. **~8,700 cells are in `wth_base` today serving a
−5.49 °C tropical minimum** with a plausible positive ET0 beside it.

This plan closes three gaps at once: detect field-level corruption going forward, track
every such finding over time, and repair what is already stored.

---

## 1. Facts that constrain the design

| Fact | Consequence |
|---|---|
| `transform_silver` is **parent-batched** — 8 parents (~128 cells) per commit (`airflow/dags/transform_silver.py:90-96`, `merge.iter_parent_batches`) | A "constant across all cells" test inside `build_wide` sees 128 cells, not 9,430. The field scan **must be a pre-pass over the whole var-year**, not a per-batch check. |
| `upsert_wide` assigns **every** column from `EXCLUDED` (`src/db/silver_load.py:132-143`) | A repair that rewrites one variable would null out the other seven. This is exactly how the 5.4 M-row 2020 mess was created. The repair path needs a column-scoped update, not `upsert_wide`. |
| Quarantined rows are **absent** from `wth_base` (`record_failures` writes only to `wth_qa_failures`) | 1987-01-26 also has a 712-cell *hole*. Repair must reinstate those rows, not just correct the ~8,700 wrong ones. |
| `wth_qa_failures` PK is `(parent_id, child_id, date)` with one `reason TEXT` | It is a cell-day quarantine, not an issue tracker. A registry of *field-level* findings is a separate table. |
| Bronze `precip` is legitimately all-zero on dry days over a small extent | A naive `nunique == 1` detector would flag real data. Thresholds must be **variable-aware**. Measured: **2,385 of 2,389** scan findings are exactly this. |
| Corruption can be **region-specific**. 1981-08-11 `tmin` is constant across the 412-cell legacy region while the same day in the Brazil chunks is perfectly normal (3,137 and 4,676 distinct values) | The detector must run **per bronze file** (= per chunk/region) and union the findings. Aggregating a var-year across its chunk files first would have hidden this one entirely. |
| PostgreSQL **16.4**; `wth_base` is 173 M rows over 1,659 partitions | `ADD COLUMN … DEFAULT 0` with a non-volatile default is metadata-only since PG 11 — the migration is fast, not a table rewrite. |
| No `src/gold/` module exists yet | `.WTH` provenance surfacing is a documented hook for the gold plan, not work in this one. |
| pyarrow **19.0.0**, pandas 2.1.4 | `group_by`/`aggregate` is available — see verified API below. |

### Verified APIs (Phase 0 output — confirmed by execution, not assumed)

```python
# pyarrow 19.0.0 — streams; does not materialize the var-year in pandas
ds.dataset(paths, format="parquet").to_table(columns=["date", "value"]) \
  .group_by("date") \
  .aggregate([("value", "count_distinct"), ("value", "stddev"), ("value", "count")])
# -> schema: ['date', 'value_count_distinct', 'value_stddev', 'value_count']
```

Run against `tmin_1987__s20r004c-003.parquet` this returns, for 1987-01-26:

```
date        value_count_distinct  value_stddev  value_count
1987-01-26                     1           0.0         3902
```

The detector signal is real and directly observable. Anti-pattern guard: do **not** invent
`pc.count_distinct_exact`, `Dataset.group_by` (grouping is on `Table`, not `Dataset`), or a
`nunique` aggregate name — the three above are the verified strings.

---

## 2. Design decisions

### D1 — The field scan is a per-file pre-pass, not a row check

New module `src/transform/field_qa.py`. It reads only `date` + `value` from **one bronze
file at a time** (3.4 M rows ≈ 40 MB) and returns one row per suspect
`(variable, date, file)`. It runs **once per var-year** in `transform_silver`, before the
parent batch loop — never inside it, for the reason in the facts table.

**Per file, not per var-year.** A var-year is one file per spatial chunk
(`merge.var_year_paths`). Merging the chunks before aggregating dilutes a regional defect
below the detector: 1981-08-11 `tmin` is constant in one region and normal in two others,
so the merged day has thousands of distinct values and vanishes. Scan each file, union the
findings, and carry the chunk id in the registry `detail`.

### D2 — Detectors are variable-aware

| Detector | Rule | Notes |
|---|---|---|
| `constant_field` | `count_distinct == 1` and `count > floor` | For accumulated variables (`precip`, `srad`) additionally require \|value\| > `NOISE_TOLERANCE` (0.01). |
| `low_spread` | `stddev < 0.02 × median(stddev)` **and** absolute range > the variable's noise tolerance | The relative test alone is not enough — see below. |
| `climatology_outlier` | per cell, \|x − μ_doy\| > 6σ_doy | Catches a plausible-looking but wrong field. Needs the climatology from D6 and so lands after it. |

**These thresholds are measured, not guessed.** The Phase 0 scan of all 973 bronze files
returned 2,389 findings, of which **2,385 are false positives** — every one a dry-day
`precip` field over the 412-cell legacy region, where a uniformly rainless day is real
data. Their magnitudes:

- 598 `constant_field` `precip` hits: max \|value\| **0.019 mm**, 597 of them below 1e-8 mm
  (float noise around zero, the same artifact `NOISE_TOLERANCE` in `merge.py:38-42`
  already exists to absorb).
- 1,787 `low_spread` `precip` hits: max range across all of them **0.0032 mm**.

Both sit far under the existing 0.01 tolerance, so gating on absolute magnitude removes
100% of the false positives while keeping both real findings. Reuse `NOISE_TOLERANCE`
rather than introducing a second constant.

### D3 — Repair is a ladder, and climatology is its **last** rung

1. **Re-fetch from the other backend.** The project has two independent download paths
   (CDS and GEE). If CDS ERA5-Land has a clean `tmin` for 1987-01-26, this is a *fix*, not
   an estimate, and nothing below applies. Always attempted first.
2. **Temporal interpolation** per cell, for isolated gaps ≤ 3 days. For this incident the
   neighbours (21.2–22.5 °C) carry the actual synoptic state — 01-26 sat inside a cool
   overcast spell (`tmax` 24.8 vs 28–30 on either side). A day-of-year mean would erase
   exactly that.
3. **Day-of-year climatology**, ±7-day window across all non-flagged years, per cell, for
   longer gaps. A bare single-DOY mean has only ~45 samples; the window buys sample size.

> **Why climatology is last.** It flattens variance. A DSSAT run fed enough climatological
> days simulates an average season that never occurred, biasing yield. Acceptable to close
> a long hole, wrong as a reflex.

### D4 — An imputed value is never indistinguishable from an observation

`is_preliminary` is about ERA5T vs final source state (§8.3) and must not be overloaded.
Two additions:

```sql
ALTER TABLE wth_base ADD COLUMN imputed SMALLINT NOT NULL DEFAULT 0;
-- bitmask, DDL order: tmax=1 tmin=2 precip=4 srad=8 wind=16 tdew=32 rh=64 et0=128

CREATE TABLE wth_imputation_log (
    parent_id CHAR(4) NOT NULL, child_id CHAR(4) NOT NULL, date DATE NOT NULL,
    variable TEXT NOT NULL, method TEXT NOT NULL,
    original_value REAL, new_value REAL,
    issue_id BIGINT REFERENCES wth_data_issues(issue_id),
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, child_id, date, variable)
);
```

`rh` and `et0` are **recomputed** from repaired inputs and carry their own bits, so a
consumer can tell a derived-from-imputed ET0 from an observed one. The log makes every
correction reversible when a fixed source appears.

### D5 — Detection is automatic and loud; repair is opt-in

`transform_silver` gains detection only: findings are written to the registry and logged
at `WARNING`. Repair lives in a **separate `repair_silver` DAG**, triggered deliberately
with an explicit `(variable, date)` scope.

> Silent imputation is how an upstream data defect stops being visible. Had this pipeline
> auto-filled, nobody would ever have learned that ERA5-Land ships a corrupt band.

### D6 — A registry, so this outlives the incident

```sql
CREATE TABLE wth_data_issues (
    issue_id    BIGSERIAL PRIMARY KEY,
    variable    TEXT NOT NULL,
    date        DATE NOT NULL,
    detector    TEXT NOT NULL,              -- constant_field | low_spread | climatology_outlier
    cells       INTEGER NOT NULL,
    detail      JSONB NOT NULL,             -- observed value, stddev, chunk ids
    status      TEXT NOT NULL DEFAULT 'detected',
    -- detected | refetched | imputed | accepted_source_defect | false_positive
    resolution  TEXT,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    UNIQUE (variable, date, detector)
);
```

`accepted_source_defect` matters: some ERA5 defects have no fix and no honest imputation.
Recording that decision is worth as much as recording a repair.

### D7 — Clamp ET0 at zero instead of quarantining it

2010-07-18, 4 cells, parents `0Y4H`/`0YEH`: `rh` 100, `srad` ~0.58 MJ, `wind` ~7 m/s,
`et0` between −0.064 and −0.023. FAO-56 genuinely returns a slightly negative ET0 under
saturation with near-zero radiation. The physical answer is zero evaporative demand, not a
rejected row. Clamp in `src/transform/et0.py` at a tolerance (mirroring how
`NOISE_TOLERANCE` already handles sub-zero accumulations in `merge.py:38-42`), and keep
`et0<0` in `CHECKS` for genuinely large negatives.

---

## 3. Phases

### Phase 0 — Discovery ✅ (done, recorded above)

Verified APIs section, plus a full scan of all **973** bronze parquet files for
`constant_field` and `low_spread`. **Phase 2 re-implements that scan as project code**;
the scratchpad run only sizes the job and calibrates the thresholds.

**Real findings — two incidents, both `tmin`, both already in `wth_base`:**

| Date | Region | Cells | Constant value | Status in silver |
|---|---|---|---|---|
| **1987-01-26** | all three files — global | 9,430 + 412 | 267.660614 K (−5.49 °C) | ~8,700 cells served wrong; 720 quarantined and missing |
| **1981-08-11** | legacy 412-cell region **only** | 412 | 265.440460 K (−7.71 °C) | **all 412 cells served wrong, zero quarantined** |

1981-08-11 was previously unknown. It passed every row-level check — ET0 came out at
+0.51, `tmax` 13.04 °C is plausible, and nothing else looked wrong. Verified in the live
DB: 161 rows in an 11-parent sample on that date share a single `tmin` value of −7.71 °C,
against 155–158 distinct values on the days either side.

The two incidents differ in kind, which is why D1 scans per file: 1987-01-26 carries the
*identical* constant in all three files (one globally corrupt band), while 1981-08-11 hits
one region and leaves the others untouched. Whether that second one is regional or an
artifact of the backend that produced the legacy files is exactly what D3 rung 1 will
settle.

### Phase 1 — `src/transform/field_qa.py` + tests

Implement D1/D2 detectors as pure functions over a `(date, value)` frame — no DB, no
Airflow, no file IO in the detector itself, so tests are trivial.

- `scan_var_year(bronze_dir, variable, year) -> pd.DataFrame` of findings
- `detect_constant_field`, `detect_low_spread` as separate testable predicates

**Verification:** `uv run pytest tests/test_field_qa.py`. Tests must include a synthetic
constant field (caught), a dry-day `precip` field constant at 1e-9 mm (**not** caught), a
`precip` field with 0.0032 mm spread (**not** caught), and a normal day (not caught). Then
run it for real and assert **exactly two** findings across the whole archive: `tmin`
1987-01-26 and `tmin` 1981-08-11 — no more, no fewer. That is the regression bar; the
Phase 0 numbers above are the expected output.

**Anti-pattern guard:** do not put the scan inside `build_wide` or the batch loop.

### Phase 2 — Registry schema + full-archive scan

- Add `wth_data_issues` to `src/db/silver_schema.sql` (every statement `IF NOT EXISTS`,
  matching the existing file's convention).
- `src/db/issues.py` — record/lookup/status transitions.
- A `scan` task in a new `airflow/dags/qa_scan.py` that walks every var-year in bronze and
  populates the registry.

**Verification:** run the scan over the whole archive; `wth_data_issues` contains exactly
the two Phase 0 findings — `tmin` 1987-01-26 (three files, 9,430 + 412 cells) and `tmin`
1981-08-11 (412 cells, one file) — and no `precip` rows at all. Any `precip` finding means
the D2 magnitude gate regressed.

### Phase 3 — Provenance migration

D4's `ALTER TABLE` + `wth_imputation_log`, plus `imputed` threaded through
`silver_load.COLUMNS`, `_STAGING_DDL`, and the upsert assignment list.

**Verification:** `\d wth_base` shows `imputed`; a normal `transform_silver` re-run of one
year still writes `imputed = 0` everywhere and the row counts are unchanged.

### Phase 4 — The repair ladder

`src/transform/repair.py`: `refetch_alternate_backend`, `interpolate_temporal`,
`climatology_fill`, each returning `(values, method)` and never writing to the DB itself.
Plus a column-scoped writer in `silver_load.py` — **not** `upsert_wide` (facts table).

**Verification:** unit tests per rung on synthetic series. Interpolation must reproduce a
known held-out day within tolerance; climatology must exclude flagged years from its own
mean.

### Phase 5 — `repair_silver` DAG

Params: `issue_id` or explicit `(variable, date)`, plus `method` (`auto` walks the ladder).
Writes repaired values, sets the `imputed` bits, writes `wth_imputation_log`, moves the
registry row to `imputed`/`refetched`, and **reinstates quarantined rows** from
`wth_qa_failures` for the repaired cell-days.

**Verification:** dry-run mode prints the diff without writing. Then repair one parent and
confirm `imputed`, the log, and the registry status all agree.

### Phase 6 — Execute the retro-repair

Two known incidents (Phase 0 table), same treatment:

1. Try CDS for `tmin` on 1987-01-26 and 1981-08-11 first (D3 rung 1). If clean → refetch,
   done, no imputation at all. For 1981-08-11 this also answers whether the defect is
   regional or backend-specific.
2. Otherwise interpolate per cell from the flanking days — 01-25/01-27 for the 9,842 cells
   of 1987-01-26, 08-10/08-12 for the 412 cells of 1981-08-11.
3. Reinstate the 720 quarantined rows (712 on 1987-01-26, 8 on 1987-01-25). 1981-08-11 has
   none to reinstate — nothing was ever caught.
4. Repeat for any further registry findings.

**Verification:** no cell on either date has `tmin < 0`; `count(distinct tmin)` on both
dates is back in the same range as the flanking days (155+ for the legacy region, thousands
for the chunks) rather than 1; `wth_imputation_log` has one row per repaired
cell-variable; 1987's total row count is back to `10,183 × 365 = 3,716,795`.

### Phase 7 — Final verification

- `uv run pytest tests/` green.
- `grep -rn "count_distinct_exact\|nunique\"" src/` returns nothing (invented-API guard).
- Re-run the Phase 2 scan: every finding is either resolved or explicitly
  `accepted_source_defect`.
- Confirm `transform_silver` on a clean year is unchanged in row count and runtime.

---

## 4. Out of scope

- `.WTH` provenance. DSSAT has no per-value flag concept and `src/gold/` does not exist
  yet. When the gold writer is built it should read `imputed` and declare filled days in
  the header or a sidecar — noted here so the gold plan inherits it.
- The 25-cell 2020 shortfall in the legacy region (outside the current extent). Separate,
  pre-existing, low priority.
- CHIRPS (`wth_precip_alt`). Same detectors would apply; wire it after the ERA5 path proves
  out.
