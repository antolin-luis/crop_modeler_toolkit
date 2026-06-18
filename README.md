# Crop Modeler Toolkit — ERA5 Daily Climate Database

Reproducible, Docker-deployed pipeline that ingests ERA5 (0.25°) daily climate data
into a PostgreSQL/PostGIS silver layer, orchestrated by Apache Airflow. Outputs feed
DSSAT crop simulations and agricultural insurance pricing. Target hardware:
Raspberry Pi 5 + 2 TB SSD.

Architecture (medallion: bronze parquet → silver PostGIS → gold `.WTH`) and all data
contracts/formulas/design decisions live in **[PLANNING.md](PLANNING.md)** — the
authoritative spec.

## Status

Roadmap Step 1 (Foundations) — repo scaffold, deterministic grid encoding + tests,
Docker Compose skeleton. The four Airflow DAGs are stubs; real download/transform
logic arrives in later roadmap steps (PLANNING.md §15).

## Quickstart

```bash
# 1. Configure (edit POSTGRES_PASSWORD and CDS_KEY)
cp .env.example .env

# 2. Python deps (uv only — no pip/poetry)
uv sync

# 3. Run tests (grid round-trip, capacity, known codes, parent grouping)
uv run pytest

# 4. Lint
uv run ruff check src tests

# 5. Bring up the stack (PostGIS + Airflow at http://localhost:8080, admin/admin)
docker compose up
```

## Layout

```
src/grid/        canonical grid spec + deterministic encoding (the contract)
src/cds/         CDS client / splitter / manifest        (later)
src/transform/   wide merge, Tetens RH, FAO-56 ET0, QA   (later)
src/db/          DDL, COPY loader, grid seeder           (later)
airflow/dags/    grid_build, download_bronze, transform_silver, update (stubs)
tests/           grid contract tests
```

## Grid encoding

`child_id` / `parent_id` are pure functions of coordinates — no lookup table. The
canonical constants in `src/grid/spec.py` (`RESOLUTION=0.25`, `LON_ORIGIN=0.0`,
`LAT_ORIGIN=90.0`, `NLON=1440`, `NLAT=721`, base-36 alphabet) **must never change**:
a shifted origin silently produces divergent codes across regions.
