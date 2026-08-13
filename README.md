# Crop Modeler Toolkit — ERA5 Daily Climate Database

Reproducible, Docker-deployed pipeline that ingests ERA5 (0.25°) daily climate data
into a PostgreSQL/PostGIS silver layer, orchestrated by Apache Airflow. Outputs feed
DSSAT crop simulations for crop modelers, researchers, and students. Target hardware:
Raspberry Pi 5 + 2 TB SSD.

Architecture (medallion: bronze parquet → silver PostGIS → gold `.WTH`) and all data
contracts/formulas/design decisions live in **[PLANNING.md](PLANNING.md)** — the
authoritative spec.

## Status

Roadmap Steps 1–4 complete (PLANNING.md §15): grid seed, bronze download from **either**
the CDS or Google Earth Engine, and the silver transform. The database is queryable — a
coordinate returns a daily weather DataFrame with `tmax`, `tmin`, `precip`, `srad`, `wind`,
`tdew`, plus derived `rh` (Tetens) and `et0` (FAO-56).

Still stubs: `update` (Step 5 — daily append + rolling ERA5T re-fetch), gold `.WTH`
materialization (Step 6), frontend (Step 7).

## Quickstart

```bash
# 1. Configure (edit POSTGRES_PASSWORD and CDS_KEY)
cp .env.example .env

# 2. Python deps (uv only — no pip/poetry)
uv sync

# 3. Run tests and lint (all offline — no CDS/GEE calls)
uv run pytest
uv run ruff check src tests

# 4. Bring up the stack (PostGIS + Airflow at http://localhost:8080, admin/admin)
docker compose up -d
```

Then follow **[docs/runbook.md](docs/runbook.md)** — the end-to-end operating guide:
trigger the download and transform DAGs, and verify the result in DBeaver or from Python.

```python
from src.db.query import fetch_series
df = fetch_series(-34.9, -56.2, "2020-01-01", "2020-12-31")   # Montevideo, daily
```

## Layout

```
src/grid/        canonical grid spec, deterministic encoding, shared raster encoder,
                 streaming dBase III reader
src/cds/         CDS client, adaptive cost splitter, manifest, download
src/gee/         Earth Engine backend: daily reduce, GCS export, streamed encode
src/transform/   wide merge, unit contract, Tetens RH, FAO-56 ET0, QA
src/db/          grid + silver + soil DDL, COPY/upsert loaders, seeders, read APIs
airflow/dags/    grid_build, download_bronze[_gee], transform_silver, update,
                 soil_grid_build
seeds/           shipped global grid dump, restored at first container init
docs/            runbook + one design doc per roadmap step
tests/           463 offline tests
```

## Grid encoding

`child_id` / `parent_id` are pure functions of coordinates — no lookup table. The
canonical constants in `src/grid/spec.py` (`RESOLUTION=0.25`, `LON_ORIGIN=0.0`,
`LAT_ORIGIN=90.0`, `NLON=1440`, `NLAT=721`, base-36 alphabet) **must never change**:
a shifted origin silently produces divergent codes across regions.
