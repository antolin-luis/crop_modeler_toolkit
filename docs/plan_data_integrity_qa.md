# Field-Level QA, an Issue Registry, and a Repair Ladder

**Status:** delivered. Phases 0–7 implemented and merged (PR #12, `c137c38`); the
retro-repair of §Phase 6 has been executed against the live DB.

**Revision 3 (2026-08-12).** A post-merge audit against the code and the live DB found five
things this plan promised that the shipped code did not do — two of them load-bearing:
`rh`/`et0` were never recomputed from repaired inputs (D4), so every `tmin`-repaired
cell-day carried a humidity and an ET0 still derived from the −5.49 °C defect; and
`upsert_wide` assigned `imputed` from `EXCLUDED`, so any re-run of `transform_silver` would
silently revert all 11,007 repairs. Both are closed on branch `qa-repair-gaps`, along with
the registry landing on `refetch_pending` instead of `imputed` (D3), the 2010-07-18
quarantine hole (D7), and this status block.

A sixth surfaced only when the guard was tested against a live 1987 re-transform: bronze is
still corrupt, so QA recomputed the same failing `et0` and re-quarantined **638 cell-days
that were sitting in `wth_base`, repaired and untouched by that very run** — a quarantine
entry contradicting the row it points at. `record_failures` now purges failures whose
cell-day carries an `imputed` bit. The quarantine records rows *missing* from silver; the
unrepaired-source debt is the registry's job.

**Revision 2 (2026-08-12).** Every claim below was re-verified against the code and the
live database before implementation began. The core diagnosis held; seven claims did not
and are corrected here. The load-bearing one: D2 previously quoted a max magnitude of
0.019 mm and then concluded it "sits far under the existing 0.01 tolerance", so the gate it
designed did not do what it said and Phase 2's regression bar would have failed. That
survivor turned out to be a **third real finding** (`precip` 1998-05-19), not a threshold to
tune away. Also corrected: cell counts, library versions, the D3 ladder (rung 1 is not
buildable, and `precip` needs its own rung), and several line citations.

**Why:** on 1987-01-26 the bronze `tmin` field is a single constant — 267.660614 K
(−5.49 °C) — across **all 9,842 cells**, in each of the three var-year files independently.
Every other variable that day is normal. One corrupt hourly `temperature_2m` band, picked
up everywhere by the daily-*min* reducer and never seen by the *max* reducer.

The QA node did not notice. `src/transform/qa.py:25-29` is entirely row-level
(`tmax<tmin`, `precip<0`, `srad<0`, `rh_out_of_range`, `et0<0`), and a field that is
*uniformly* wrong violates none of them. Only 712 of the 9,842 cells were caught, and only
as a side effect of ET0 tipping negative. **9,130 cells are in `wth_base` today serving a
−5.49 °C tropical minimum** with a plausible positive ET0 beside it (verified in the live
DB: 9,130 rows at `-5.4894` on that date).

This plan closes three gaps at once: detect field-level corruption going forward, track
every such finding over time, and repair what is already stored.

---

## 1. Facts that constrain the design

| Fact | Consequence |
|---|---|
| `transform_silver` is **parent-batched** — 8 parents (~128 cells) per commit (`airflow/dags/transform_silver.py:90-110`; batch size is the DAG param at `:136`, not a module constant, defaulting to `merge.iter_parent_batches(batch_size=8)`) | A "constant across all cells" test inside `build_wide` sees 128 cells, not 9,842. The field scan **must be a pre-pass over the whole var-year**, not a per-batch check. |
| `upsert_wide` assigns **every non-key** column from `EXCLUDED` (`src/db/silver_load.py:133-144`) — *since Revision 3 each value column is guarded by its `imputed` bit and the mask is OR-ed, so a re-transform no longer reverts a repair* | A repair that rewrites one variable would null out the other seven. This is exactly how the 5.4 M-row 2020 mess was created. The repair path needs a column-scoped update, not `upsert_wide`. Verified: no `UPDATE` statement exists anywhere in `src/` or `airflow/` outside `silver_load.py:142`, so that writer is genuinely new code. |
| Quarantined rows are **absent** from `wth_base` (`record_failures` writes only to `wth_qa_failures`) | 1987-01-26 also has a 712-cell *hole*. Repair must reinstate those rows, not just correct the 9,130 wrong ones. |
| `wth_qa_failures` PK is `(parent_id, child_id, date)` with one `reason TEXT` | It is a cell-day quarantine, not an issue tracker. A registry of *field-level* findings is a separate table. |
| Bronze `precip` is legitimately all-zero on dry days over a small extent | A naive `nunique == 1` detector would flag real data. Thresholds must be **variable-aware**. Measured: the overwhelming majority of raw scan findings are exactly this. |
| Silver holds **10,183** cells per day; current bronze holds **9,842** | 341 silver cells have no bronze source — they predate the current extent. They are *clean* on 1987-01-26, which is why that day shows 341 non-corrupt values. No repair or re-transform can source them; leave them alone (see §4). |
| Bronze now also holds `chirps_v2/` and `chirps_v3_rnl/` | The scan must be scoped by the ERA5 variable list (`merge.ALL_VARIABLES`), **not** by listing `bronze/`. CHIRPS is out of scope (§4). |
| The legacy 412-cell files and the two chunk files hold **disjoint** cell sets (412 ∩ 3,902 ∩ 5,528 = ∅) | No chunk file already contains a clean replacement for the 1981-08-11 legacy cells. Verified; also means the var-year files concatenate without dedup, as `merge.var_year_paths` documents. |
| Bronze holds **native** units — `precip` in metres, `srad` in J/m², temperatures in K (`src/transform/units.py`) | Every magnitude threshold must run through `units.convert` first. `NOISE_TOLERANCE` is documented in mm; applied to raw bronze it would be a 10 mm/day gate. |
| The registry keys on `(variable, date, detector)`, but detection is **per file** | The three per-file findings for 1987-01-26 would overwrite each other and report 5,528 cells instead of 9,842. Findings are consolidated (cells summed, per-file evidence kept under `detail.files`) before the registry write. |
| Corruption can be **region-specific**. 1981-08-11 `tmin` is constant across the 412-cell legacy region while the same day in the Brazil chunks is perfectly normal (3,137 and 4,676 distinct values) | The detector must run **per bronze file** (= per chunk/region) and union the findings. Aggregating a var-year across its chunk files first would have hidden this one entirely. |
| PostgreSQL **16.4**; `wth_base` is 172.4 M rows over 1,659 partitions | `ADD COLUMN … DEFAULT 0` with a non-volatile default is metadata-only since PG 11 — the migration is fast, not a table rewrite. |
| No `src/gold/` module exists yet | `.WTH` provenance surfacing is a documented hook for the gold plan, not work in this one. |
| pyarrow **24.0.0**, pandas **3.0.3** (neither pinned in `pyproject.toml`; resolved in `uv.lock`) | `group_by`/`aggregate` is available — see verified API below, re-confirmed on 24.0.0. pandas 3.0 is a major bump, so detector code must not assume 2.x semantics. |
| `_STAGING_DDL` (`src/db/silver_load.py:47-51`) omits `is_preliminary`, which is appended inline at `:129` | Threading a new `imputed` column follows that same shape — append to the temp-table DDL at the call site, do not edit `_STAGING_DDL`. |

### Verified APIs (Phase 0 output — confirmed by execution, not assumed)

```python
# pyarrow 24.0.0 — streams; does not materialize the var-year in pandas
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
| `constant_field` | `count_distinct == 1` and `count ≥ MIN_CELLS` (32) | For accumulated variables (`precip`, `srad`) additionally require \|value\| > `NOISE_TOLERANCE` (0.01) **in silver units**. A uniform value *above* that tolerance stays a finding — see 1998-05-19 below. |
| `low_spread` | `stddev < 0.02 × median(stddev)`, **smooth variables only** | Catches a *nearly* constant field, which the equality test cannot see. Never applied to `precip`/`srad`: no magnitude gate separates a real drizzle day there — see below. |
| `climatology_outlier` | per cell, \|x − μ_doy\| > 6σ_doy | Catches a plausible-looking but wrong field. Needs the climatology from D6 and so lands after it. |

**These thresholds are measured, not guessed** — and they must be measured in the **right
units**. Bronze stores what the source delivered, so `precip` is in *metres*
(`src/transform/units.py:29`). Comparing a raw bronze value against `NOISE_TOLERANCE`
(0.01, documented as mm) would be a 0.01 m = **10 mm/day** gate, suppressing every rain
event under 10 mm. Every magnitude test runs through `units.convert` first.

Measured over the 412-cell legacy region, in silver units:

- 598 `constant_field` `precip` hits: 597 are float noise around zero (below 1e-5 mm — the
  same artifact `NOISE_TOLERANCE` at `merge.py:44` already exists to absorb). Exactly
  **one** survives the gate.
- `low_spread` `precip`: 2,299 hits, of which **1,211 survive the magnitude gate**
  (max range 0.39 mm). The gate does not separate them, and nothing it catches is real.

> **`low_spread` does not apply to accumulated variables.** On `precip` it is pure noise: a
> region-wide day of uniformly light drizzle over a ~1° region is real weather, and no
> magnitude threshold distinguishes it from a defect. Across the whole archive `low_spread`
> found **zero** real defects and 1,211 false ones on `precip`, and fired **zero** times on
> every smooth variable — which is the correct behaviour for a guard on clean data, and why
> it is kept for them. `constant_field` (exact equality) is what actually separates: 598
> raw `precip` hits down to one.

> **That one survivor is not a false positive — and it is not small.** `precip_1998.parquet`,
> 1998-05-19: all 412 legacy cells at exactly `0.019002625718712807` **metres = 19.0 mm**.
> Confirmed in silver: 412 rows at `19.0026` mm on that date. That is a substantial
> region-wide rain field pinned to a single value, not a dry day and not float noise, so it
> is a **third real finding**. (Read as "0.019 mm" it looked negligible; the unit fix is what
> makes its significance visible.)
>
> Its resolution is still decided by inspection rather than by the detector — 19 mm uniform
> across the region is clearly wrong, but whether the underlying day was wet or dry needs an
> independent source. Repair it per D3's precip rung, or record `accepted_source_defect`.
> Never auto-impute it.

### D3 — Repair is a ladder, it is **variable-aware**, and averaging is its last resort

**Rung 1 — re-fetch from an independent source — is deferred, because it cannot be built
as originally written.** No date-scoped download exists in either backend: the CDS splitter
floors at one month and says so explicitly (`src/cds/splitter.py:22` — "Below a single
month we fall back to spatial tiling rather than per-day requests"), GEE's
`build_daily_collection` is hardcoded whole-year (`src/gee/daily.py:85-86`), and
`airflow/dags/update.py` is still an `EmptyOperator` stub. Re-fetching one day therefore
means building new download granularity and, on GEE, spending EECU against the binding
quota constraint. That work belongs with the `update` DAG (roadmap Step 5), which needs
sub-year re-fetch anyway. Until then the registry carries a `refetch_pending` status so
the debt stays visible and every imputation remains reversible when a clean source arrives.

For `precip` specifically, the eventual rung 1 is **CHIRPS**, not an ERA5 re-fetch — an
independent gauge+satellite product at 0.05°, 1981–present, already probed on this project
(`chirps_base_grid` / `chirps_era5_map` exist in the DB, bronze holds 1981-83). Its code
sits on the unmerged `chirps-fine-grid` branch, so it is out of scope here (§4).

**The remaining rungs depend on the variable, because the variables do not behave alike.**

| Variable class | Rungs |
|---|---|
| Smooth (`tmax`, `tmin`, `tdew`, `srad`, `wind`) | temporal interpolation → DOY climatology |
| `precip` | analog-day resampling only |

- **Temporal interpolation** per cell, for isolated gaps ≤ 3 days. For the 1987-01-26
  incident the neighbours (21.2–22.5 °C) carry the actual synoptic state — 01-26 sat inside
  a cool overcast spell (`tmax` 24.8 vs 28–30 on either side). A day-of-year mean would
  erase exactly that.
- **Day-of-year climatology**, ±7-day window across all non-flagged years, per cell, for
  longer gaps. A bare single-DOY mean has only ~45 samples; the window buys sample size.
- **Analog-day resampling** for `precip`: collect the observed values for that cell over the
  same DOY ±7-day window across non-flagged years and draw **one actual observed day**
  (seeded RNG, so a re-run reproduces the fill), recording which real day was borrowed.

> **Why `precip` gets its own rung.** Interpolation is valid for temperature because
> temperature is smooth and autocorrelated; precipitation is zero-inflated, heavy-tailed and
> near-uncorrelated day to day, so interpolating it is as wrong as averaging it. A mean fill
> is worse still: a wet-season month here is ~15 rain days out of 31 with most of the total
> in a few events, so the daily mean describes no day that ever occurred. Filling with it
> produces a drizzle-every-day series that never triggers runoff, never saturates and never
> dries below wilting point — silently biasing DSSAT's soil water balance, planting-date
> logic and consecutive-dry-day drought stress upward. Resampling a real day preserves the
> wet/dry structure and event magnitude that the crop model actually integrates over.

> **Why climatology is last for everything else.** It flattens variance. A DSSAT run fed
> enough climatological days simulates an average season that never occurred, biasing yield.
> Acceptable to close a long hole, wrong as a reflex.

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

> **Superseded in Revision 3, by decision.** `transform_silver` now fires `repair_silver`
> automatically for an issue still at status `detected` (`auto_repair`, on by default;
> `auto_repair_dry_run` for the diff-only version). The argument below still stands and is
> why the guards exist: only a *new* detection fires, every filled value carries its
> `imputed` bit and a log row holding the original, the issue lands on `refetch_pending`
> rather than resolved, and the repair runs as its own DAG run. The defect stays visible;
> what changed is that the first response no longer waits for a human.

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
    -- detected | refetch_pending | refetched | imputed
    -- | accepted_source_defect | false_positive
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
rejected row. `et0_fao56` currently returns `numerator / denominator` raw at
`src/transform/et0.py:140` — its existing `np.clip` calls are all intermediate (arccos
domain, `rs/rso`, `sqrt(ea)`), so nothing guards the result. Clamp there at a tolerance
(mirroring how `NOISE_TOLERANCE` / `snap_accumulation_noise` already handle sub-zero
accumulations in `merge.py:44,147-153`), and keep `et0<0` in `CHECKS` for genuinely large
negatives.

---

## 3. Phases

### Phase 0 — Discovery ✅ (done, recorded above)

Verified APIs section, plus a full scan of all **973** ERA5 bronze parquet files for
`constant_field` and `low_spread`. **Phase 2 re-implements that scan as project code**;
the scratchpad run only sizes the job and calibrates the thresholds.

**Real findings — three incidents from the bronze scan, plus a fourth found only during
execution. All already in `wth_base`:**

| Variable | Date | Region | Cells | Constant value | Status in silver |
|---|---|---|---|---|---|
| `tmin` | **1987-01-26** | all three files — global | 9,842 | 267.660614 K (−5.49 °C) | 9,130 cells served wrong; 712 quarantined and missing |
| `tmin` | **1981-08-11** | legacy 412-cell region **only** | 412 | 265.440460 K (−7.71 °C) | **all 412 cells served wrong, zero quarantined** |
| `precip` | **1998-05-19** | legacy 412-cell region **only** | 412 | 0.019002625718712807 m = **19.0 mm** | all 412 cells at one non-zero value; resolution decided by inspection (D2) |
| `tmin` | **1987-01-25** | the 341 sourceless silver cells **only** | 341 | −5.489386 °C (as stored) | invisible to the scan — see below; registered by hand |

The fourth surfaced only when the 1987-01-26 repair was dry-run: interpolating 01-26 from its
flanking days produced ~3 °C instead of ~21 °C for 341 cells, because for those cells the
*anchor* day 01-25 was itself corrupt. **The scan reads bronze and those 341 cells have no
bronze source**, so no bronze-side detector can ever see this class of defect. That is a
structural limit of D1, not a threshold to tune: closing it needs a silver-side scan, which
this plan does not build. Until then the repair path's `exclude_dates` is the safeguard — it
keeps a known-bad day from becoming an interpolation anchor.

All four verified directly against bronze and the live DB:

- 1987-01-26 is `count_distinct == 1` in **each** of the three files (412 / 3,902 / 5,528),
  which hold **disjoint** cell sets. Silver shows 9,130 rows at `-5.4894`, 712 quarantined,
  and 341 clean rows — those 341 are the silver cells with no bronze source (facts table).
- 1981-08-11 was previously unknown. It passed every row-level check — ET0 came out at
  +0.51, `tmax` 13.04 °C is plausible, and nothing else looked wrong. Live DB: exactly 412
  rows share `-7.7095` on that date, against 8,258 and 7,827 distinct values on the days
  either side.
- 1987 currently holds 3,716,075 rows = `10,183 × 365 − 720`, matching the quarantine
  (712 on 01-26 + 8 on 01-25). The other 4 quarantined rows table-wide are 2010-07-18 (D7).

The incidents differ in kind, which is why D1 scans per file: 1987-01-26 carries the
*identical* constant in all three files (one globally corrupt band), while 1981-08-11 hits
one region and leaves the others untouched — merged, that day shows 7,597 distinct values
and disappears entirely. Whether the second is regional or an artifact of the backend that
produced the legacy files stays **open**: file mtimes cannot settle it, since the legacy
files are 2026-06-27 and the GEE backend landed 2026-06-26. A future re-fetch (D3 rung 1)
answers it.

### Phase 1 — `src/transform/field_qa.py` + tests ✅

Implement D1/D2 detectors as pure functions over a `(date, value)` frame — no DB, no
Airflow, no file IO in the detector itself, so tests are trivial.

- `scan_var_year(bronze_dir, variable, year) -> pd.DataFrame` of findings
- `detect_constant_field`, `detect_low_spread` as separate testable predicates

**Verification:** `uv run pytest tests/test_field_qa.py`. Tests must include a synthetic
constant field (caught), a dry-day `precip` field constant at 1e-9 mm (**not** caught), a
`precip` field with 0.0032 mm spread (**not** caught), a `precip` field constant at
0.019 mm (**caught** — above `NOISE_TOLERANCE`, the 1998-05-19 case), and a normal day (not
caught). Then run it for real and assert **exactly three** findings across the whole
archive: `tmin` 1987-01-26, `tmin` 1981-08-11, `precip` 1998-05-19 — no more, no fewer.
That is the regression bar; the Phase 0 table above is the expected output.

> **Three, against a four-row table.** The bar counts *bronze-scan* findings. The fourth
> incident, `tmin` 1987-01-25, sits on the 341 silver cells with no bronze source, so no
> bronze-side detector can ever see it — it is registered by hand. A fourth **scan** finding
> is a regression; a fourth **registry** row is not. Same reading applies to Phase 2 below.

**Anti-pattern guard:** do not put the scan inside `build_wide` or the batch loop.

### Phase 2 — Registry schema + full-archive scan ✅

- Add `wth_data_issues` to `src/db/silver_schema.sql` (every statement `IF NOT EXISTS`,
  matching the existing file's convention).
- `src/db/issues.py` — record/lookup/status transitions.
- A `scan` task in a new `airflow/dags/qa_scan.py` that walks every var-year in bronze and
  populates the registry. It enumerates variables from **`merge.ALL_VARIABLES`**, not by
  listing `bronze/` — that directory now also holds `chirps_v2/` and `chirps_v3_rnl/`,
  which are out of scope (§4).

**Verification:** run the scan over the whole archive; `wth_data_issues` contains exactly
the three Phase 0 findings — `tmin` 1987-01-26 (three files, 9,842 cells), `tmin`
1981-08-11 (412 cells, one file), `precip` 1998-05-19 (412 cells, one file) — and nothing
else. A *fourth* `precip` row means the D2 magnitude gate regressed; a CHIRPS row means the
variable scoping did.

### Phase 3 — Provenance migration ✅

D4's `ALTER TABLE` + `wth_imputation_log`, plus `imputed` threaded through
`silver_load.COLUMNS` and the upsert assignment list. Note `_STAGING_DDL` is **not** the
place for it: `is_preliminary` is appended to the temp-table DDL at the call site
(`silver_load.py:129`), and `imputed` follows that same shape.

**Verification:** `\d wth_base` shows `imputed`; a normal `transform_silver` re-run of one
year still writes `imputed = 0` everywhere and the row counts are unchanged.

### Phase 4 — The repair ladder ✅

`src/transform/repair.py`: `interpolate_temporal`, `climatology_fill`, `analog_day_fill`,
each returning `(values, method)` and never writing to the DB itself. Plus a `LADDER`
mapping variable → ordered rungs, so `method="auto"` cannot route `precip` into an average
(D3). `refetch_alternate_backend` is **not** implemented here — see D3 on why rung 1 is
deferred.

Plus a column-scoped writer in `silver_load.py` — **not** `upsert_wide` (facts table).

**Verification:** unit tests per rung on synthetic series. Interpolation must reproduce a
known held-out day within tolerance; climatology must exclude flagged years from its own
mean; `analog_day_fill` must return a value that **exists in** the source window rather
than an average of it, preserve the window's dry-day fraction, and be stable across two
runs with the same seed. A guard test asserts `LADDER["precip"]` contains no mean-based
rung.

### Phase 5 — `repair_silver` DAG ✅

Params: `issue_id` or explicit `(variable, date)`, plus `method` (`auto` walks that
variable's `LADDER`). Writes repaired values, sets the `imputed` bits, writes
`wth_imputation_log`, moves the registry row to `imputed`/`refetched`, and **reinstates
quarantined rows** from `wth_qa_failures` for the repaired cell-days.

**Verification:** dry-run mode prints the diff without writing. Then repair one parent and
confirm `imputed`, the log, and the registry status all agree.

### Phase 6 — Execute the retro-repair ✅

Three known incidents (Phase 0 table). Rung 1 is unavailable (D3), so each is marked
`refetch_pending` alongside its repair, keeping the correction reversible.

1. `tmin` 1987-01-26 — interpolate per cell from 01-25 / 01-27 for all 9,842 bronze cells
   (9,130 corrected in place + 712 reinstated). Registry → `imputed` + `refetch_pending`.
2. `tmin` 1981-08-11 — interpolate per cell from 08-10 / 08-12 for the 412 legacy cells.
   Nothing to reinstate. Registry → `imputed` + `refetch_pending`; a later re-fetch settles
   the regional-vs-backend question.
3. `precip` 1998-05-19 — **inspect first, never auto-impute.** 412 cells pinned at 19.0 mm,
   a substantial region-wide rain field that cannot be right. Repair with `analog_day_fill`
   and set the `precip` bit, or record `accepted_source_defect` if the borrowed day is no
   more defensible than the corrupt one.
4. Reinstate the 720 quarantined rows (712 on 1987-01-26, 8 on 1987-01-25).
5. Repeat for any further registry findings.

**Verification:** no cell on either `tmin` date has `tmin < 0`; `count(distinct tmin)` on
both dates is back in the same range as the flanking days (8,258 / 7,827 for 1981;
thousands for 1987) rather than 1 or 412-at-one-value; `wth_imputation_log` has one row per
repaired cell-variable; 1987's total row count is back to `10,183 × 365 = 3,716,795`. If
1998-05-19 is filled, its `count(distinct precip)` must land in the flanking-day range and
its dry-cell fraction must be plausible for May — a uniform non-zero result means the
analog draw collapsed. The 341 sourceless silver cells stay untouched (§4).

### Phase 7 — Final verification ✅

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
- **The 341 silver cells with no bronze source** (silver 10,183/day vs bronze 9,842/day).
  They predate the current extent, no download covers them, and they are clean on all three
  incident dates. Nothing here can or should touch them.
- CHIRPS (`wth_precip_alt`, `chirps_v2/`, `chirps_v3_rnl/`). Two reasons it stays out: the
  same field detectors would apply but the scan is deliberately scoped to
  `merge.ALL_VARIABLES` (Phase 2), and its download code is on the unmerged
  `chirps-fine-grid` branch. Wire it after the ERA5 path proves out — at which point it also
  becomes `precip`'s D3 rung 1, the one independent source this project actually has.
