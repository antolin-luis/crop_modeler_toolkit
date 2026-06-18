# Step 3 — Bronze Download (Planning)

> Roadmap Step 3 (PLANNING.md §15.3). Builds the bronze layer: per-variable,
> per-year Parquet of raw ERA5 daily values over a chosen extent, fetched through an
> **adaptive splitter** that keeps every CDS request under the cost ceiling, tracked
> by an idempotent **manifest**, and driven by the `download_bronze` DAG. Depends on
> Step 2 (canonical grid + vectorized encoders in `src/grid/`) which is complete.

## Context

Bronze is the raw landing zone (PLANNING.md §7): Parquet on disk, **one file per
variable per year**, native ERA5 values **unconverted** (unit conversions + derivations
happen in silver, Step 4). Each file holds every daily record of one variable for one
year over the downloaded extent, with columns `child_id, parent_id, date, value`.

The hard part is the CDS itself (§11): the new CDS enforces a per-request **cost
limit**, and `cdsapi` raises only generic exceptions — so we probe adaptively. The core
primitive is a recursive splitter that shrinks a rejected request **time-first**
(year → semester → quarter → month → spatial tiles as last resort), caches the first
granularity that works, and reuses it for the remaining ~350 backfill requests.

Decisions inherited from Step 2: doc in `docs/`; reuse `src/grid/encoding` vectorized
`cell_codes`/`parent_codes`; bronze under `cfg.paths.bronze_dir` (`/data/bronze`).

## Inputs (DAG params — already stubbed on `download_bronze`)

| Param        | Meaning                                                              |
|--------------|---------------------------------------------------------------------|
| `extent`     | `[S, W, N, E]` in −180/180, snapped to 0.25° cell-center bounds      |
| `start_year` / `end_year` | inclusive backfill range                               |
| `variables`  | subset of the silver-name contract (default = all 7)                |
| `timezone`   | local-day offset (e.g. `UTC-03:00`); same for **all** variables (§5.3)|

## Variable contract (download side, §5.1)

All from dataset `derived-era5-single-levels-daily-statistics`. Bronze stores the
**source** value; the conversion column is Step 4's job.

| silver name | CDS `variable`                       | `daily_statistic` |
|-------------|--------------------------------------|-------------------|
| `tmax`      | `2m_temperature`                     | `daily_maximum`   |
| `tmin`      | `2m_temperature`                     | `daily_minimum`   |
| `precip`    | `total_precipitation`                | `daily_sum`       |
| `srad`      | `surface_solar_radiation_downwards`  | `daily_sum`       |
| `wind_u`    | `10m_u_component_of_wind`            | `daily_mean`      |
| `wind_v`    | `10m_v_component_of_wind`            | `daily_mean`      |
| `tdew`      | `2m_dewpoint_temperature`            | `daily_mean`      |

Use `2m_temperature` extremes, **never** `*_since_previous_post_processing` (§5.2).

## Files to create / modify

### Variable contract (single source)
- `src/cds/variables.py` — the table above as a typed mapping
  (`silver_name -> (cds_variable, daily_statistic)`) + the dataset id. Imported by the
  request builder and by Step 4's converter so the two never drift.

### Adaptive splitter (§11.2) — the core primitive
- `src/cds/splitter.py`:
  - `submit(client, base_request, target_dir) -> list[Path]` — try `client.retrieve`;
    on a **cost** error, `split()` time-first and recurse; on auth/network/transient,
    `retry_with_backoff` (do **not** split).
  - `is_cost_error(exc) -> bool` — classify by HTTP 400 + `"cost"`/`"too large"` in the
    message (cdsapi has no typed errors). Guard with a max recursion depth so a
    misclassified non-cost error can't split forever.
  - `split(request) -> Iterator[request]` — generator over the time ladder:
    year → semester → quarter → month → (last resort) spatial tiles. Floor at ~1 day /
    a minimum tile.
  - **Granularity cache** keyed by extent: remember the first working level and start
    there for subsequent `(year, variable)` requests (avoid re-probing 350+ times).
  - Returns the list of partial netCDF files (one per accepted sub-request).

### Manifest (§11.4)
- `src/cds/manifest.py` — JSON on disk under `bronze_dir/_manifest.json`. Records
  completed `(year, variable[, sub-chunk])` tuples; `is_done()` / `mark_done()` so a
  restarted backfill skips finished work. Atomic write (temp + rename).

### Download orchestration (netCDF → Parquet)
- `src/cds/download.py`:
  - `build_request(variable, year, extent, timezone)` — assembles the CDS request:
    `area` from the snapped extent (CDS order `[N, W, S, E]`), `daily_statistic` +
    `time_zone` per the contract, `data_format: netcdf`. **Never sets `grid`** (§11.1).
    (Confirm exact daily-stats field names against the dataset form; normalize any
    zip-wrapped netCDF via the Step-2 `static_inputs._normalize_netcdf` pattern.)
  - `download_variable_year(...)` — manifest check → `splitter.submit` → open the
    partial netCDFs with `xarray`, **encode** `child_id`/`parent_id` from the grid
    coords via `cell_codes`/`parent_codes` (reuse `to_latlon_grid` for orientation),
    concat the time chunks, write `bronze/<var>/<var>_<year>.parquet` (pyarrow) →
    `manifest.mark_done`.
  - **Daily sanity check** (§5.2): assert exactly one value per cell per day before
    writing; misconfigured requests have returned hourly data.
- `src/cds/client.py` (modify only if needed) — keep thin; the splitter owns retries.

### DAG (§10 DAG 2) — replace stub
- `airflow/dags/download_bronze.py` — loop **year-major, variable-minor**; one task per
  `(year, variable)` (dynamic task mapping). Bind all CDS tasks to an Airflow **pool**
  that caps simultaneous requests (CDS queue limits, §10). Idempotent via the manifest.

### Tests
- `tests/test_splitter.py` — with a mock client: time-first split order; `is_cost_error`
  classification (cost vs auth/network/transient); granularity-cache reuse; depth +
  day floor guards (no infinite recursion on a misclassified error).
- `tests/test_manifest.py` — record / skip idempotency; restart skips done tuples;
  atomic write survives a simulated crash.
- `tests/test_bronze_encode.py` — synthetic daily netCDF subset → Parquet with correct
  `child_id`/`parent_id`/`date`/`value`; daily-sanity rejects an hourly fixture. No
  live CDS.

## Constraints to honor
- **No `grid` param** on any request; `area` for subsetting only, snapped to 0.25°
  vertices (§11.1).
- **Time-first** split; spatial tiling only as last resort (§11.2).
- Same **local-day** `timezone` for all variables; Brazil = UTC−3 (§5.3).
- Bronze is **raw/unconverted**; conversions are Step 4 (§7).
- Cap CDS concurrency via an Airflow pool; prefer small sub-ceiling requests (§10, §11.1).
- Pi memory: stream per time-chunk, write columnar Parquet; don't hold a full extent
  of all years in RAM.

## Out of scope (later steps)
- Wide merge, RH/ET0/QA, `wth_base` upsert (Step 4).
- ERA5T preliminary→final rolling re-fetch + `is_preliminary` (Step 5).
- Gold `.WTH`; frontend.

## Build Airflow (before verification)

The Airflow image has never been built; do it once at the start of verification (first
build is the slow part — pip-installs Airflow, arm64-native, no emulation). Not a
prerequisite for writing/unit-testing Step 3 code, only for orchestration checks.

1. `docker compose up -d` — builds the airflow image and starts
   `airflow-init` / `scheduler` / `webserver` (UI on `:8080`, `admin`/`admin`)
   alongside the already-running `postgres`.
2. Confirm **all 4 DAGs parse** with no import errors:
   `docker compose run --rm airflow-scheduler airflow dags list` — `grid_build`,
   `download_bronze`, `transform_silver`, `update` all appear, no `ImportError`.
   (Catches a broken `from src...` import before any trigger.)

## Verification
1. `uv run pytest tests/test_splitter.py tests/test_manifest.py tests/test_bronze_encode.py` — pass.
2. `uv run ruff check src tests` — clean.
3. **Smoke (live CDS, tiny demo extent + 1 short period):** trigger `download_bronze`
   from the Airflow UI (or `airflow dags trigger`) for one `(year, variable)`;
   confirm `bronze/<var>/<var>_<year>.parquet` exists, row count == `cells × days`,
   values finite, and a `child_id` decodes (`code_to_latlon`) to the expected cell.
4. **Brazil extent, 1 year × all variables:** the splitter adapts under the cost limit
   (observe the cached granularity); rerun is a no-op (manifest skips). Calendar
   completeness per cell incl. leap day.
5. Confirm the product is genuinely **daily** (one value per cell per day), not hourly.
