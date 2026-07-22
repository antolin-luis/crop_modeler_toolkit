# Step 4 — Silver Transform (Bronze → `wth_base`)

> Roadmap Step 4 (PLANNING.md §15.4). Merges the per-variable bronze Parquets wide on
> `(child_id, date)`, converts to silver units (§5.1), derives `wind`, `rh` (Tetens §12.1)
> and `et0` (FAO-56 §12.2), runs the QA node (§8.4), and upserts into the partitioned
> `wth_base` (§8.2). This is the step that makes the database **queryable** — gold/DSSAT
> (Step 6) is a separate materialization and is not needed to read the data.

Runs entirely offline: no CDS, no GEE, no quota. Inputs are the Parquets already on disk
plus the static `lat`/`elevation` from `era5_land_base_grid`.

## Files

### Transform
- `src/transform/units.py` — the §5.1 conversion table (K→°C, m→mm, J/m²→MJ/m²). Asserts at
  import that its keys match `src/cds/variables.py`, so the download and transform
  contracts cannot drift.
- `src/transform/merge.py` — bronze long → silver wide. Parent-batched reads
  (`iter_parent_batches`, `load_var_year` with a pushed-down `parent_id` filter),
  `merge_wide` (outer join + unit conversion + `wind` from its components),
  `add_derived` (rh, et0), `build_wide` (the batch entry point).
- `src/transform/humidity.py` — Tetens `es()` and `relative_humidity()`, clamped to [0,100].
  `es()` is the shared primitive ET0 also uses.
- `src/transform/et0.py` — FAO-56 Penman-Monteith, pure numpy, one function per sub-step
  (`atm_pressure`, `psychrometric_gamma`, `delta_svp`, `extraterrestrial_radiation`,
  `clear_sky_radiation`, `net_longwave`, `net_radiation`).
- `src/transform/qa.py` — `split_valid` (row-level physical checks → quarantine) and
  `calendar_report` (coverage, logged not enforced).

### Database
- `src/db/silver_schema.sql` — `wth_base` (§8.2) + `wth_qa_failures` (§8.4). Kept out of
  `schema.sql`, which is the first-boot seed script, so an existing volume picks these up
  without a reset.
- `src/db/silver_load.py` — `ensure_schema`, `ensure_partitions`, `upsert_wide`
  (TEMP staging → `COPY` → `INSERT … ON CONFLICT`), `record_failures`, `fetch_cell_meta`,
  `preliminary_cutoff` / `assign_preliminary`. Reuses `src/db/load.py` for the connection
  and `COPY` primitives.
- `src/db/query.py` — the read API: `fetch_series(lat, lon, start, end)` → date-indexed
  DataFrame. Resolves the cell arithmetically; no grid join, no PostGIS.

### Orchestration
- `airflow/dags/transform_silver.py` — one mapped task per year; inside, parent batches are
  merged → derived → QA'd → upserted, committing per batch.

## Design notes

**Parent batching is the memory lever.** A continental var-year is ~20M rows (~1.4 GB in
pandas) × 7 variables. `parent_id` is a stored bronze column, so filtering by a batch of
parents pushes down into the Parquet scan and bounds the working set independently of
extent size. `parent_batch_size` (default 8) is the knob.

**`is_preliminary` is source state, never derivation completeness** (§8.3). It is set from
a rolling cutoff (§11.3): `date >= run_date − preliminary_months` (default 3). Override per
run with the `final_cutoff` param. A row can have NULL `rh`/`et0` and still be final.

**QA quarantines, it does not mutate.** Failing rows go to `wth_qa_failures` with a
semicolon-joined `reason` and never enter `wth_base`. The quarantine for a
`(parent batch, year)` is cleared before each load, so a row fixed by a bronze re-fetch
stops being reported as a failure.

**NaN is not a QA failure.** A missing input yields a NULL derived value (§8.3);
quarantining that would throw away good observations.

**Sub-tolerance negative accumulations are snapped to zero** (`merge.NOISE_TOLERANCE`).
Bronze `precip` is a float32 sum of hourly increments, so dry cell-days land marginally
below zero — the observed floor on 2020 bronze is −1.1e-5 mm. Without the snap, ~19% of a
year's rows would be quarantined as "negative precipitation". Anything beyond the
tolerance (0.01 mm / 0.01 MJ/m²) still fails QA.

**Accepted wind bias (§12.2).** `u2 = sqrt(mean(u)² + mean(v)²) × 0.748` from the
daily-mean components underestimates true mean wind speed (Jensen gap). ET0 is weakly
sensitive to wind, so this is documented rather than calibrated.

## Prerequisite: the grid table must match the bronze block size `b`

`parent_id` is a pure function of `child_id` **given `b`**, and `b` is immutable for the
life of the database (§6.3). Everything in this repo uses `b = 4`
(`src/grid/encode_long.DEFAULT_BLOCK_SIZE_B`, the `grid_build` DAG default, and the shipped
`seeds/era5_land_base_grid.sql.gz`).

Verify before the first load — if this returns nothing, ET0 will be NULL for every row
because the `cell_meta` join finds no cells:

```sql
SELECT parent_id FROM era5_land_base_grid WHERE child_id = 'EU9K';  -- expect 0XKE
```

A mismatch means the live table was built with a different `b` than bronze. Repair by
restoring the shipped seed (the authoritative artifact):

```bash
docker exec -i <postgres> psql -U era5 -d era5 -c 'DROP TABLE era5_land_base_grid CASCADE;'
zcat seeds/era5_land_base_grid.sql.gz | docker exec -i <postgres> psql -U era5 -d era5
```

## Verification

1. `uv run pytest tests/` — pass.
2. `uv run ruff check src tests` — clean.
3. Confirm the grid/bronze `b` match with the query above.
4. **Single year:** trigger `transform_silver` with `start_year = end_year = 2020`.
   Expect 412 cells × 366 days = **150,792 rows** in `wth_base`, 33 partitions,
   `wth_qa_failures` empty.
5. **Sanity SQL** — Uruguay ranges: `tmax` ≈ −5…45 °C, `precip ≥ 0` mm,
   `srad` ≈ 0…35 MJ/m²/day, `rh` ∈ [0,100] %, `et0` ≈ 0…10 mm/day, `et0` NOT NULL.
6. **Full backfill** 1980–2026; confirm 2026 rows on/after the cutoff carry
   `is_preliminary = TRUE` and 1980 rows carry `FALSE`.
7. **Idempotency:** re-run one year — row count unchanged, `ingested_at` bumped.
8. **The point of the step:**
   ```python
   from src.db.query import fetch_series
   df = fetch_series(-34.9, -56.2, "1980-01-01", "2026-06-22")  # Montevideo
   ```
   ~17k daily rows in silver units; single-cell fetch should be milliseconds (§8.2).

## Out of scope (later steps)
- ERA5T preliminary→final rolling re-fetch and the `is_preliminary` flip (Step 5, DAG 4).
- Gold `.WTH` materialization incl. `TAV`/`AMP`/`ELEV` header (Step 6).
- `CLUSTER` on the partitions — loads are already sorted by `(child_id, date)`; revisit
  only if single-cell fetch latency disappoints at continental scale.
