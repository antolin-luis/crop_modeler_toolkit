# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

Reproducible, Docker-deployed pipeline that ingests ERA5 (0.25°) daily climate data into a PostgreSQL/PostGIS silver layer, orchestrated by Apache Airflow. Target hardware: Raspberry Pi 5 + 2 TB SSD. Outputs feed DSSAT crop simulations for crop modelers, researchers, and students.

The full specification is in `PLANNING.md` — it is the authoritative source for data contracts, formulas, and design decisions.

## Commands

```bash
# Bring up the full stack
docker compose up

# Install dependencies
uv sync

# Add a dependency
uv add <package>

# Add a dev-only dependency
uv add --dev <package>

# Run tests (once scaffold exists)
uv run pytest tests/

# Run a single test file
uv run pytest tests/test_encoding.py

# Run a single test
uv run pytest tests/test_encoding.py::test_round_trip
```

Use `uv` for all Python package management — do not use `pip`, `pip-tools`, or `poetry`.

## Committing & Pushing

Before staging, committing, or pushing, run these checks — they are mandatory, not advisory:

1. **Never commit secrets.** No credentials, API keys, passwords, or tokens in tracked files — this includes `.env`, `.cdsapirc`, and anything matching `*credentials*`, `*secret*`, `*.key`, `*.pem`. The CDS key (`CDS_KEY`) and `POSTGRES_PASSWORD` live only in `.env` (git-ignored); `.env.example` carries placeholders only. If a new secret-bearing file appears, add its pattern to `.gitignore` **before** the first commit, never after.
2. **Never commit large or generated data.** Bronze Parquet, static `.nc`/`.grib`, raw SQL dumps, and anything under `/data` or `.localdata` stay out of git (already in `.gitignore`). The **one intentional exception** is the shipped grid seed `seeds/era5_land_base_grid.sql.gz` — the distribution artifact. Do not add other large blobs; regenerate them from code instead.
3. **Scan the staged set first.** Run `git status` and `git diff --cached --stat` and confirm no secret and no unexpected file >~10 MB is staged before committing. If something large or sensitive slipped through, fix `.gitignore` and unstage it.
4. **Branch, don't push to `main` directly.** Work on a feature branch (e.g. `step3-bronze-download`) and open a PR; only commit/push when the user asks.

### Plan-driven coding workflow (mandatory)

Any code creation or modification that comes out of a `/plan` session **must** follow this sequence:

1. **Branch first.** Before writing or editing any code, create a fresh feature branch off `main`. Do this as the very first action of the coding phase — never start editing on `main`.
2. **Code on the branch.** All file changes for the plan land on that branch only.
3. **Hand off for testing.** When the code is complete, stop and let the **user test it**. Do not commit yet.
4. **Wait for explicit authorization.** Only after the user tests and explicitly authorizes do you: commit, open a PR to merge the branch into `main`, and delete the feature branch once merged.

Steps 3–4 gate on the user — never self-authorize the commit/PR/merge/delete.

## Architecture

Medallion architecture with three layers:

- **Bronze** — Parquet on disk (`/data/bronze/`), one file per variable per year. Raw ERA5 values, unconverted.
- **Silver** — PostgreSQL 16 + PostGIS. Two tables: `era5_land_base_grid` (global static geometry + per-cell `t_zone` local-day offset) and `wth_base` (wide daily observations, partitioned by `parent_id`).
- **Gold** — DSSAT `.WTH` files, materialized on demand. Partially specified; header fields (`TAV`/`AMP`/`ELEV`) are deferred.

Orchestration is four Airflow DAGs: `grid_build` (one-off seed), `download_bronze`, `transform_silver`, `update` (daily append + ERA5T rolling re-fetch).

Source modules live in `src/`:
- `grid/` — canonical constants (`spec.py`), deterministic cell encoding (`encoding.py`), land-mask clip (`land_mask.py`)
- `cds/` — CDS API client, adaptive splitter, download manifest
- `transform/` — wide merge, Tetens RH, FAO-56 ET0, QA
- `db/` — DDL, `COPY`-based loader, grid seeder

## Critical Constraints

**Grid encoding.** `child_id` and `parent_id` are pure functions of coordinates — no lookup table. The canonical constants in `src/grid/spec.py` must never change:
```
RESOLUTION=0.25, LON_ORIGIN=0.0, LAT_ORIGIN=90.0, NLON=1440, NLAT=721, ALPHABET="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
```
A shifted origin silently produces divergent codes across regions.

**CDS requests.** Never pass the `grid` parameter (server-side regridding increases cost). Use `area` for subsetting only. The adaptive splitter splits time-first (year → semester → quarter → month), spatial tiling only as a last resort.

**Temperature variables.** Use `2m_temperature` with `daily_maximum`/`daily_minimum` — do **not** use `maximum_2m_temperature_since_previous_post_processing` or its minimum counterpart. As of June 2026 those parameters have an open data-quality issue and carry a forecast cold bias.

**Day definition.** Local day, not UTC — and the offset is **per cell**, carried on `era5_land_base_grid.t_zone` (standard non-DST UTC offset, minutes; from the political tz shapefile at seed time). All variables of a cell share that one offset (avoid mixing UTC-day precip with local-day temperature). The GEE backend derives its reduction windows per-offset from a tz-polygon EE asset (`GEE_TZ_ASSET`) and takes **no** `timezone` param; CDS still takes a single `timezone` for the whole extent (correct only for single-tz extents). Daily reduction is lossy — apply the offset at reduction, never convert a daily value between timezones after the fact.

**Elevation.** Derived from geopotential: `elevation_m = z / 9.80665`. Use the 0.25° model orography for ET0 — do not substitute a high-res DEM.

**ERA5T re-fetch.** The `update` DAG re-fetches a rolling ~3-month window to replace preliminary ERA5T values with final ERA5. The re-fetch is not complete until silver reflects it (`is_preliminary` flipped to `FALSE`).

## Key Formulas

**RH (Tetens):** `es(T) = 0.6108 * exp(17.27 * T / (T + 237.3))`, `rh = 100 * es(tdew) / es((tmax+tmin)/2)`, clamped to [0, 100].

**ET0 (FAO-56 Penman-Monteith):** Full implementation per FAO-56 Chapter 4. Wind at 2 m = `sqrt(u² + v²) * 0.748`. `G = 0` for daily. Atmospheric pressure from elevation.

**Gold wind conversion:** `wind_2m_kmday = sqrt(u² + v²) × 0.748 × 86.4`.

## Silver Schema Notes

`wth_base` is partitioned `BY LIST (parent_id)`. The loader CREATEs the partition if absent before `COPY` (a LIST insert with no matching partition errors). The PK is `(parent_id, child_id, date)` — Postgres requires the partition key in every PK; `parent_id` is a pure function of `child_id` so uniqueness per cell-day still holds (and `b` must stay fixed for the DB's life). Upsert uses `ON CONFLICT (parent_id, child_id, date) DO UPDATE`. Load via `COPY`, not row-by-row `INSERT`.

For a point lookup, compute `child_id` arithmetically (no PostGIS needed). Use `geom` + GiST only for polygon/region queries.
