# Implementation Plan — Chunked GEE Backfill (production wiring)

**Status:** plan only. No code written for Part A. Part 0 below is already built and awaiting commit on branch `gee-chunk-probe`.

**Companion:** [`cost_model_climate_context.md`](cost_model_climate_context.md) §9.3–9.4 is the evidence — *what was measured and why*. This document is *what to change and in what order*.

**Scope:** wire the measured chunking decision into the production `download_bronze_gee` path, then propagate the same constraints into [`plan_climate_context_layer.md`](plan_climate_context_layer.md), whose GFS/CHIRPS/NDVI phases all ride the same GEE export machinery.

---

## 1. Why this exists

A whole-year, whole-extent GEE export over Brazil does not complete. EE restarts it (`attempt` 2, 3) until something cancels it — reproduced twice, 2026-07-30 and 2026-07-31, the second time under a deliberate attempt cap that cut the loss at 55 minutes.

Fifty probe exports later, the limit is characterised and the fix is measured. What is missing is that none of it is on the production path: `download_bronze_gee` still submits one export per `(variable, year)` over the full extent, which is exactly the thing that fails.

---

## 2. Measured facts this plan is built on

Do not re-derive these; re-measure only if the extent or the band count changes materially.

| Fact | Value | Source |
|---|---|---|
| Export failure predictor | `land_cells × zones` | §9.4, 48 records, clean separation |
| Largest success | 58,554 (19,518 cells × 3 zones) | `s40r002c-002` |
| Only failure | ~90,748 (~22,687 × 4) | whole extent, twice, `attempts=3` |
| Per-task cost | `0.0512 + 4.33e-5 × cells` EECU-h (R² 0.56) | fit over all records |
| Per-task time | `215 + 0.0869 × cells` seconds (R² 0.33) | same |
| Concurrency | net **2.64× at p4**, 3.10× at p8; EE throttles to 4.4 in flight | E1b |
| Chosen chunk size | **400 parents** (20°×20°), 4.9× ceiling margin | §9.4 decision table |
| Chosen pool | **`gee_pool` = 4** | E1b |
| Brazil full backfill at 400 | 4,289 tasks, ~778 EECU-h, ~7.6 d | modelled |

**Both predictor terms are known offline.** `era5_land_base_grid` carries `is_land` and `t_zone`, and `t_zone` came from the same shapefile that built `GEE_TZ_ASSET` — verified agreeing with EE's own zone derivation on `s40r002c-002` (3 predicted, 3 derived).

⚠ **The ceiling is band-count-specific.** It was measured at 366 daily bands per export. A 16-band GFS export or a 36-band dekadal NDVI export has a different budget — see Part B.

---

## 3. Part 0 — what already exists (commit first)

On branch `gee-chunk-probe`, 212 tests passing, ruff clean, **uncommitted**. Part A assumes it is merged.

| File | Provides |
|---|---|
| `src/gee/chunks.py` | `tile_extent(extent, k)` → canonical `k×k`-parent chunks; `Chunk`; `chunk_id` |
| `src/grid/encoding.py` | `parent_bbox`, `parent_indices`, `parent_code_bbox` — the parent inverse |
| `src/db/grid_query.py` | `chunk_land_stats` → `ChunkStats(land_parents, land_cells, zones)` + `cell_zones` |
| `src/gee/export.py` | `wait_for_task(max_attempts=…, on_poll=…)`, `task_attempt` |
| `src/gee/metrics.py` | schema **v2**: `attempts`, `max_attempts`, `chunk_id`, `parents`, `land_parents`, `n_zones`, `parallel` |
| `src/gee/download.py` | `download_variable_year(chunk=…, max_attempts=…, land_parents=…, parallel=…)` |
| `src/cds/manifest.py` | `is_spatial_chunk_done` / `mark_spatial_chunk_done` (`sp:` keyspace) |
| `scripts/gee_chunk_probe.py` | `ladder` / `parallel` / `whole` / `report` modes, ceiling guard, `--force` |

**First action of the next session:** commit this branch and open the PR (user authorization required per `CLAUDE.md`), so Part A starts from a clean `main`.

---

## 4. Part A — wire chunking into `download_bronze_gee`

### A0. Promote the ceiling constant out of the probe

`CELL_ZONE_CEILING = 58_554` currently lives in `scripts/gee_chunk_probe.py`. Production needs it, and a constant that guards a backfill does not belong in a maintainer script.

- Move to `src/gee/chunks.py` beside `tile_extent`, with the measurement provenance in the docstring.
- Probe imports it from there.
- Add `src/gee/chunks.plan_chunks(extent, parents_per_side, *, conn=None) -> list[PlannedChunk]` — tiling + `chunk_land_stats` + drop no-land chunks + flag over-ceiling ones. This is the single function both the DAG and the probe call, so the guard cannot be bypassed by accident.

*Tests:* over/under ceiling classification against synthetic stats; no-land chunks dropped; DB access mocked (the existing suite never touches Postgres).

### A1. ⚠ Silver cannot see chunked bronze — fix first

`src/transform/merge.py:48` resolves exactly one path per variable-year:

```python
return Path(bronze_dir) / variable / f"{variable}_{year}.parquet"
```

Chunked output lands as `<var>_<year>__<chunk_id>.parquet`, which this **silently does not find** — `available_variables` reports the variable absent and `transform_silver` produces nothing. This is the single highest-risk item in Part A, and it fails quietly rather than loudly.

- Replace `var_year_path` with `var_year_paths(bronze_dir, variable, year) -> list[Path]`, globbing `f"{variable}_{year}*.parquet"` sorted.
- `ds.dataset()` accepts a list of file paths, so the three call sites (`available_variables`, `iter_parent_batches`, `load_var_year`) change shape but not logic.
- Unchunked output keeps working: the glob returns the single legacy file.

*Tests:* a var-year written as 3 chunk files reads back identical to the same rows written as one file; `available_variables` sees a chunked variable; `iter_parent_batches` unions parents across chunks. *Accept:* an existing unchunked bronze dir (`.localdata/hn`) still transforms with no change.

### A2. DAG plan: expand over `(year, variable, chunk)`

In `airflow/dags/download_bronze_gee.py::_plan`, call `plan_chunks` once and produce one mapped task per chunk.

New params:

| Param | Default | Notes |
|---|---|---|
| `chunk_parents` | `400` | `0` = unchunked (legacy behaviour, for small extents) |
| `max_attempts` | `2` | EE restarts tolerated before the task is cancelled |
| `skip_over_ceiling` | `true` | fail the plan instead of submitting a doomed export |

⚠ **Airflow `max_map_length` is 1024 by default.** 76 years × 7 vars × 8 chunks = 4,288 mapped tasks — a full Brazil backfill in one trigger exceeds it. Decide in-session between:

- **(a)** keep one task per chunk and have `_plan` raise a clear error when the map would exceed the limit, forcing year-window runs (10 years × 7 × 8 = 560 — fits); or
- **(b)** map per `(year, variable)` and loop that year's chunks inside the task.

Recommend **(a)**: per-chunk mapping is what gives per-chunk retry and lets `gee_pool` fill with 4 independent exports, which is the entire point of the E1b result. A year-window run is also how the EECU quota gets spread across months anyway (§6).

### A3. Rollup: when is a variable-year "done"?

`mark_spatial_chunk_done` deliberately never marks the year. Something must, or a re-run re-plans every chunk (cheap — each chunk short-circuits on its own manifest key — but `available_variables` and the runbook both talk in var-years).

Add a `rollup` task downstream of the mapped `download`: for each `(variable, year)` in the plan, if every planned `chunk_id` is marked, call `mark_var_year_done`. Keep `Manifest` writes single-threaded here — it rewrites the whole file.

*Accept:* interrupt a run mid-way, re-trigger, and only the missing chunks are exported; after the last one lands, the var-year reads as done.

### A4. `gee_pool` 2 → 4

- `docker-compose.yml:75` — `airflow pools set gee_pool 4 "GEE export cap"`.
- `docs/runbook.md:160` and the §65 pool table say "both capped at 2" — update, and record *why* 4 (E1b: 2.64× net, versus 3.10× at p8 for double the in-flight load).
- Existing deployments: the pool already exists, and `airflow-init` will not resize it. Runbook needs the one-liner to change it live (`airflow pools set`), or via the UI.

### A5. Metrics and cost log

`_download` already logs a cost line; add `chunk_id`, `attempts`, `n_zones` to it and to the XCom dict. A chunk cancelled at the attempt cap must log loudly — it is the signal that the extent's zone count has grown past what the chosen size supports.

### A6. Runbook

New section: chunked backfill. The trigger command with `chunk_parents`, the year-window guidance, how to read `attempts` in the JSONL, and what to do when a chunk trips the ceiling (drop to the next size down — 225 or 100 — for that extent, do not `--force`).

---

## 5. Part B — propagate into `plan_climate_context_layer.md`

That plan predates all of this and assumes GEE exports simply work. Three of its phases ride the same machinery. Edits, in the order they appear in that document:

### B1. §3.4 "What does not need this" — add the export-budget caveat

The section correctly says GFS needs no `fspec`. Add that it **does** inherit the chunking constraint, and that the constraint scales with band count: the measured ceiling is `cells × zones ≤ ~58,554` **at 366 bands**. Generalise the quantity as `cells × zones × bands` (Brazil's failure ≈ 33.2 M cell-zone-bands; largest success ≈ 21.4 M) and state plainly that the per-source budget must be confirmed by one probe run before that source's first backfill, not assumed from ERA5's number.

### B2. §7 DAG table + pools

- `gee_pool` is now 4, not 2. The line "GFS work goes through the existing `gee_pool` — it competes for the same EECU quota" becomes more pointed given §6 below: GFS competes for a quota that ERA5 has already largely spent.
- Add `chunk_parents` to the new-DAG-params table for any GEE-backed DAG (`update_forecast` GFS leg, `build_context_base` CHIRPS leg).

### B3. Phase 6 (GFS), Phase 9 (CHIRPS), Phase 13 (NDVI) — per-phase notes

| Phase | Bands per export | Zones | Expected pressure |
|---|---|---|---|
| 6 GFS | 16 (one init's lead days) | per-cell local day applies → same tz zones | **Low** — 23× fewer bands than ERA5-year. Likely needs no chunking at Central America extent; confirm with one probe. |
| 9 CHIRPS | 365 | **1** — CHIRPS is already a daily product, no local-day reduction | **Low-moderate** — the zone multiplier disappears entirely, so the budget goes ~4× further than ERA5's. |
| 13 NDVI | ~36 dekads | 1 | **Negligible** |

Each phase's acceptance criteria should gain: *"one export at the target extent completes at `attempts=1`"*. Cheap to check, and it is the failure this whole exercise exists to prevent.

### B4. §10 storage budget — add an EECU row

The budget table costs rows and disk but **not EECU**, which is now the binding constraint (§6). Add a column or a companion table: EECU-h per phase per run, against the 1,000 EECU-h/month Contributor quota. GFS at daily cadence is the one to size carefully — it is the only recurring GEE cost in the plan, and E2 exists precisely to measure it.

### B5. §11 risks — two new rows

| Risk | Mitigation |
|---|---|
| A GEE export exceeds the `cells × zones × bands` budget and EE restarts it indefinitely | `max_attempts=2` on every export; `plan_chunks` ceiling guard; probe one export per new source before backfilling |
| GEE EECU quota exhausted by the ERA5 backfill, starving the context layer | Schedule ERA5 chunked backfill in year windows across months (§6); GFS daily burn measured by E2 before enabling |

### B6. §13 first branch — unchanged

`context-phase0-1-normals` is still the right first branch: it touches no GEE at all. Note only that Phase 6/9 must not start before Part A lands, since they would otherwise re-invent chunking.

---

## 6. The strategic constraint to decide, not code

**778 EECU-h is 78% of one month's Contributor quota — for Brazil alone.** LatAm has both more cells and more zones. Before any continental commitment, one of these has to be chosen, and it is a scope decision rather than an engineering one:

1. Spread the backfill across months in year windows (free, slow — several months of calendar time).
2. Cut variables (7 → 5 drops ~29%; `wind_u`/`wind_v` only matter for ET0).
3. Cut the start year (1950 → 1980 drops ~40%).
4. Reduce the extent to the countries actually served.

§6 of the cost model still projects the continental figure on the *unchunked* model and needs redoing on `0.0512 + 4.33e-5 × cells` before it can inform this.

---

## 7. Verification (end-to-end, after Part A)

1. `uv run pytest tests/` — all green, including the new merge-glob and chunk-plan tests.
2. Trigger `download_bronze_gee` with `chunk_parents=400`, one year, one variable, over the Brazil extent (`data_root=/data/chunked_smoke`). Expect 8 mapped tasks, all `attempts=1`.
3. Confirm `transform_silver` fires and silver gains rows — this is the A1 regression, and the only way to catch it is end-to-end.
4. Re-run the same trigger: every chunk short-circuits on the manifest, no export submitted.
5. `POSTGRES_HOST=localhost uv run python scripts/gee_chunk_probe.py report --data-root .localdata/chunked_smoke` — per-task EECU should sit on the fitted line.
6. Row-count parity: one chunked var-year vs the same var-year fetched unchunked at a small extent must produce identical `(child_id, date)` sets.

---

## 8. Workflow

Per `CLAUDE.md`: branch first, code on the branch, stop for user testing, wait for explicit authorization before commit/PR/merge.

- Commit/PR `gee-chunk-probe` **before** starting Part A.
- Part A branch: **`gee-chunked-dag`**. Order: A0 → **A1 (the silent-failure fix, first)** → A2 → A3 → A4 → A5 → A6.
- Part B is documentation-only and can ride the same branch or a separate `docs/context-plan-chunking` — it changes no code.
