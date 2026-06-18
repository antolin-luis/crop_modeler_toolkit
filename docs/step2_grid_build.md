# Step 2 — Grid Build (Planning)

> Roadmap Step 2 (PLANNING.md §15). Builds the global `era5_land_base_grid`:
> static downloads (geopotential + ERA5-Land mask), land clip (§6.4), DDL (§8.1),
> `seed_grid.py`, and a shippable SQL-dump seed. Depends on Step 1 (grid encoding
> in `src/grid/`, Compose skeleton, PostGIS service) which is complete.

## Context

The silver layer's static foundation is `era5_land_base_grid` — one row per 0.25°
cell with `child_id`, `parent_id`, `lat`/`lon`, `is_land`, `elevation`, `geom`. It is
fully deterministic from the canonical spec + two static inputs, so per §8.1 it is
**shipped as a seed** (users don't rebuild ~1M polygons on a Pi). This step writes
the build code (`grid_build` DAG) and produces the committed seed.

Decisions confirmed: doc in `docs/`; static inputs via a coded CDS fetch helper with
manual-drop fallback; **global** grid distributed as a **SQL-dump seed**.

## Inputs (two static files → bronze)

`/data/bronze/static/`:
- `geopotential.nc` — ERA5 single-levels `geopotential`, single timestep, native
  0.25° (provides 0.25° grid coords **and** elevation = `z / 9.80665`, §5.4).
- `era5_land_mask.nc` — one ERA5-Land variable, single timestep, native 0.1°. Land =
  non-NaN; sea = NaN (the land reference for the clip, §6.4).

Both fetched **without** the CDS `grid` parameter (§11.1). Static → download once.

## Files to create / modify

### CDS static fetch (coded, with manual fallback)
- `src/cds/client.py` — minimal `cdsapi.Client` wrapper reading `CDS_URL`/`CDS_KEY`
  from `src/config.py`. (Full client/splitter is Step 3; here only single static
  retrievals.) Never passes `grid`.
- `src/grid/static_inputs.py` — `ensure_static_inputs(dest, *, force=False)`:
  if both `.nc` already present in `/data/bronze/static/`, no-op (manual-drop
  fallback); otherwise retrieve via `src/cds/client.py`. Two request builders:
  geopotential (single-levels, one time) and ERA5-Land mask (one variable, one time).

### Land clip (§6.4) — implement the Step-1 stub
- `src/grid/land_mask.py` — replace `NotImplementedError` with:
  `compute_is_land(land_mask_nc, *, fraction_x) -> dict[child_id|index, bool]`.
  - Load ERA5-Land grid with `xarray`; land boolean = `~isnan(var)`.
  - Bin **each 0.1° centroid** into its containing 0.25° cell using the canonical
    origin (`lat_idx = floor((LAT_ORIGIN - lat)/0.25)`, `lon_idx = floor(lon/0.25)`)
    — every fine centroid maps to exactly one 0.25° cell (handles the non-integer
    2.5 ratio without assuming clean nesting, §6.4).
  - Per 0.25° cell: `is_land = (land_count / total_count) >= fraction_x`.
  - Vectorize with `numpy`/`pandas` groupby (no Python per-cell loop — Pi memory).
  - `fraction_x` default in `src/grid/spec.py` (e.g. `LAND_FRACTION_X = 0.5`);
    documented as the coastal-inclusiveness knob.

### DDL (§8.1)
- `src/db/schema.sql` — `era5_land_base_grid` exactly per §8.1 (PK `child_id`,
  `idx_grid_parent`, GiST `idx_grid_geom`), preceded by `CREATE EXTENSION IF NOT
  EXISTS postgis;`.

### Seed builder
- `src/db/seed_grid.py` — build the **global** grid and load Postgres:
  - Enumerate all `NLAT*NLON` cells vectorized (numpy meshgrid of `lat_idx`,
    `lon_idx`); derive `lat`/`lon` (centers, lon→[-180,180]), `child_id`,
    `parent_id` (reuse `src/grid/encoding.py` — vectorize the base-36 step).
  - `elevation` ← geopotential 0.25° field aligned by `(lat_idx, lon_idx)`.
  - `is_land` ← `src/grid/land_mask.compute_is_land`.
  - Load via `COPY` into a temp/staging set of columns; compute `geom` in SQL with
    `ST_MakeEnvelope(lon-0.125, lat-0.125, lon+0.125, lat+0.125, 4326)` (avoids
    generating ~1M WKT strings in Python).
  - CLI entrypoint so a maintainer runs it once to produce the seed.
- `src/db/load.py` — small `COPY`-helper (psycopg2 `copy_expert`) reused here and in
  Step 4. Connection from `src/config.py` DSN.

### Shippable SQL-dump seed + init wiring
- `seeds/era5_land_base_grid.sql` — `pg_dump` of the populated table (committed;
  generated once by the maintainer). Contains DDL + `COPY` data.
- `seeds/00_postgis.sql` — `CREATE EXTENSION IF NOT EXISTS postgis;` (ordered first).
- `docker-compose.yml` (modify) — mount `./seeds:/docker-entrypoint-initdb.d:ro` on
  the `postgres` service so a fresh volume auto-restores the seed (runs only on empty
  data dir — the intended distribution path).

### DAG (§10 DAG 1) — replace stub
- `airflow/dags/grid_build.py` — chain:
  `ensure_static_inputs` → `build_grid` (`seed_grid.py`) → optional `dump_seed`.
  Params: `block_size_b` (default 4), `land_fraction_x`. Idempotent;
  `CREATE TABLE IF NOT EXISTS` + truncate-then-load, or `ON CONFLICT` upsert.

### Tests
- `tests/test_land_mask.py` — synthetic mini ERA5-Land array over a few 0.25° cells:
  assert fraction thresholding (all-land → True; below `X` → False; the non-nesting
  binning assigns the right fine cells to each coarse cell).
- `tests/test_seed_grid.py` — global row count == `NLAT*NLON == 1_038_240`; every
  `(child_id, parent_id)` consistent with `encoding.py`; `lat`/`lon` in range;
  elevation finite where geopotential present. (Use a small synthetic geopotential
  fixture or monkeypatch — no live CDS in tests.)

## Constraints to honor
- **No `grid` param** on any CDS request (§11.1).
- `elevation_m = z / 9.80665`; use the 0.25° model orography, not a DEM (§5.4).
- `b` is immutable once chosen; it sets `parent_id` and partition layout (§6.3).
- Non-integer 0.25/0.1 ratio → fractional overlap clip, never clean nesting (§6.4).
- Vectorize the ~1M-cell build; bounded memory (Pi 5 target).

## Out of scope (later steps)
- `wth_base` DDL / partitioning / upsert, merge/RH/ET0/QA (Step 4).
- Full adaptive splitter, manifest, year/variable bronze download (Step 3).
- Gold `.WTH`; frontend.

## Verification
1. `uv run pytest tests/test_land_mask.py tests/test_seed_grid.py` — pass.
2. `uv run ruff check src tests` — clean.
3. With CDS creds + a small test: `python -m src.grid.static_inputs` drops both
   `.nc` into `/data/bronze/static/`; rerun is a no-op (manual fallback path).
4. Build the seed once: run `seed_grid.py` against the Compose `postgres`, then
   `psql -c 'SELECT count(*) FROM era5_land_base_grid;'` → `1038240`;
   `SELECT count(*) FILTER (WHERE is_land) FROM era5_land_base_grid;` ≈ 300k;
   `SELECT postgis_version();` and one `ST_AsText(geom)` spot-check (0.25° square).
5. `pg_dump` → `seeds/era5_land_base_grid.sql`; `docker compose down -v && docker
   compose up postgres` on a fresh volume auto-restores the seed; row count matches.
6. Spot-check a known coordinate end-to-end: `cell_code(lat,lon)` (arithmetic) finds
   the same row queried by `geom` + GiST (§8.1 lookup pattern).
