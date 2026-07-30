# Runbook — from empty machine to queryable climate data

End-to-end operating guide for roadmap Steps 1–4: bring the stack up, build the grid,
download bronze, transform to silver, then inspect the result in DBeaver or from Python.

Per-step design rationale lives in the step docs (`docs/step2_grid_build.md`,
`step3_bronze_download.md`, `step3b_bronze_download_gee.md`, `step4_silver_transform.md`);
the authoritative spec is [PLANNING.md](../PLANNING.md). This document is the *how to run
it* companion — it assumes the design and tells you which buttons to press.

---

## 0. Prerequisites

| Need | Why |
|---|---|
| Docker + Docker Compose | runs PostGIS and Airflow |
| [uv](https://docs.astral.sh/uv/) | Python deps and host-side scripts (never pip/poetry) |
| A CDS account **or** a GEE project | one bronze backend is enough — see below |
| ~1 GB disk per (region-year × 7 vars) | bronze Parquet; Uruguay 47 yr ≈ 343 MB |

**Which bronze backend?** Both write identical Parquet and are interchangeable
(verified: identical `child_id` sets and values to float32 precision).

- **CDS** — no cloud setup, but slow: a MARS-tape queue, ~17 min for one variable-year
  over a small extent. Fine for a few years; painful for a 47-year backfill.
- **GEE** — aggregates hourly→daily server-side and is dramatically faster, but needs a
  one-off Google Cloud + GCS setup (`docs/gee_setup.md`). ~2.45 EECU-h per LatAm-year of
  all 7 variables, inside the free noncommercial quota.

### Configure

```bash
cp .env.example .env
```

Edit at minimum `POSTGRES_PASSWORD`, and `CDS_KEY` (format `uid:api-key`) if using CDS.
`.env` is git-ignored and is the single place configuration lives (PLANNING.md §13).
For GEE also set `GEE_PROJECT`, `GEE_SERVICE_ACCOUNT_FILE`, `GEE_GCS_BUCKET`, and
`GEE_TZ_ASSET` (the per-cell timezone zones — build it once, §6 of the GEE guide) — follow
`docs/gee_setup.md` first.

```bash
uv sync                        # Python deps
uv run pytest tests/ -q        # everything offline; expect all green
uv run ruff check src tests
```

---

## 1. Bring up the stack

```bash
docker compose up -d           # first run also builds the Airflow image (slow, arm64-native)
docker compose ps              # postgres healthy; airflow-init exited 0; scheduler + webserver up
```

This starts:

| Service | Purpose | Address |
|---|---|---|
| `postgres` | PostGIS 16 — silver layer **and** Airflow's own metadata DB | `localhost:5432` |
| `airflow-webserver` | UI | <http://localhost:8080> (`admin` / `admin`) |
| `airflow-scheduler` | runs the DAGs | — |
| `airflow-init` | one-shot: DB migrate, admin user, creates `cds_pool` and `gee_pool` | exits 0 |

Confirm all five DAGs parse with no import errors:

```bash
docker compose run --rm airflow-scheduler airflow dags list
# grid_build, download_bronze, download_bronze_gee, transform_silver, update
```

> On a Raspberry Pi target, `./.localdata` is bind-mounted to `/data` in the containers.
> On the real hardware point that at the SSD mount instead.

---

## 2. The grid (Step 2) — usually nothing to do

`era5_land_base_grid` is the global static grid: 1,038,240 cells, one row per 0.25° cell,
with `lat`/`lon`/`is_land`/`elevation`/`t_zone`/`geom` (`t_zone` = per-cell standard UTC
offset in minutes, the local-day definition, §5.3). It ships as a seed
(`seeds/era5_land_base_grid.sql.gz`) and **restores automatically on a fresh Postgres
volume** — `./seeds` is mounted into `/docker-entrypoint-initdb.d`, which Postgres runs
exactly once at initdb.

### Verify it (do this before the first silver load)

```sql
SELECT count(*), count(elevation), count(t_zone) FROM era5_land_base_grid;  -- 1038240 x3
SELECT parent_id FROM era5_land_base_grid WHERE child_id = 'EU9K';   -- expect 0XKE
```

That second query is a **block-size check**, and it matters. `parent_id` is a pure function
of `child_id` *given* the block size `b`, and `b` is immutable for the life of the database
(§6.3). Everything here uses `b = 4`. If the query returns anything other than `0XKE`, the
table was built with a different `b` than bronze, the silver join finds no cells, and
**every `et0` silently loads as NULL**.

Repair by restoring the shipped seed (the authoritative artifact):

```bash
docker exec -i crop_modeler_toolkit-postgres-1 \
  psql -U era5 -d era5 -c 'DROP TABLE era5_land_base_grid CASCADE;'
zcat seeds/era5_land_base_grid.sql.gz | \
  docker exec -i crop_modeler_toolkit-postgres-1 psql -U era5 -d era5
```

Only a maintainer regenerating the seed needs the `grid_build` DAG; it requires the two
static `.nc` inputs and is not part of normal operation.

---

## 3. Bronze download (Step 3)

One Parquet per variable per year under `/data/bronze/<var>/<var>_<year>.parquet`, holding
**raw ERA5 values** (Kelvin, metres, J/m²) — conversions are Step 4's job.

Both DAGs share these parameters:

| Param | Meaning |
|---|---|
| `extent` | `[S, W, N, E]` in −180/180, snapped to 0.25° |
| `start_year` / `end_year` | inclusive |
| `variables` | subset of the 7: `tmax tmin precip srad wind_u wind_v tdew` |

**Timezone differs by backend:**

- **GEE (`download_bronze_gee`) has no `timezone` param.** The local-day 24 h window is
  **per-cell**: it derives one reduction zone per UTC offset present in the extent from the
  tz-polygon asset (`GEE_TZ_ASSET`), which matches each cell's `era5_land_base_grid.t_zone`
  (§5.3). A multi-country extent is therefore correct in one run. (One-time setup:
  `scripts/build_tz_asset.py` → see step 3b doc.)
- **CDS (`download_bronze`) keeps a single `timezone`** applied to the whole extent —
  correct only for a single-timezone extent; split a multi-tz extent into one run per zone.

GEE adds `land_only` (default `True`, clips to land via LSIB) and `chunk_days`
(default 30 — lower it if the Pi runs short on memory).

### Trigger it

> **GEE prerequisite:** `GEE_TZ_ASSET` must be set and the timezone asset built once —
> `download_bronze_gee` raises `GEE_TZ_ASSET is unset` otherwise. See `docs/gee_setup.md` §6.

In the UI: DAG → **Trigger DAG w/ config** → paste the JSON. Or from the CLI:

```bash
docker compose run --rm airflow-scheduler \
  airflow dags trigger download_bronze_gee \
  -c '{"extent":[-35,-58,-30,-53],"start_year":2020,"end_year":2020}'
```

For the CDS backend use `download_bronze` and add `"timezone":"UTC-03:00"`.

> **Always pass an extent.** The default is the whole globe. A global backfill is not
> what you want as a first run.

Tasks are mapped one per `(year, variable)`, year-major, and bound to a pool
(`cds_pool` / `gee_pool`, both capped at 2) so neither service gets hammered.
Both DAGs are **idempotent** via a shared manifest — a `(variable, year)` completed by
*either* backend is skipped on re-run.

### Check the result

```bash
ls -la .localdata/bronze/tmax/
uv run python -c "
import pandas as pd; d = pd.read_parquet('.localdata/bronze/tmax/tmax_2020.parquet')
print(d.shape); print(d.head()); print(d.date.nunique(), 'days,', d.child_id.nunique(), 'cells')"
```

Expect `child_id, parent_id, date, value`, one row per cell per day, values in Kelvin
(~250–320 for `tmax`). A land-clipped Uruguay year is 412 cells × 366 days = 150,792 rows.

---

## 4. Silver transform (Step 4)

Merges the 7 bronze Parquets wide, converts units, derives `wind` / `rh` / `et0`, runs QA,
and upserts into `wth_base`. Entirely offline — no CDS, no GEE, no quota.

```bash
docker compose run --rm airflow-scheduler \
  airflow dags trigger transform_silver -c '{"start_year":2020,"end_year":2020}'
```

| Param | Default | Notes |
|---|---|---|
| `start_year` / `end_year` | 2020 | one mapped task per year |
| `variables` | all 7 | must match what bronze holds |
| `parent_batch_size` | 8 | parents per commit — the memory lever; lower it if RAM is tight |
| `preliminary_months` | 3 | ERA5T rolling window (§11.3) |
| `final_cutoff` | `""` | ISO date; blank = derive from `preliminary_months` |

A year with no bronze files is skipped with a log line, not a failure. Re-running a year is
safe: the upsert replaces the same `(parent_id, child_id, date)` rows and bumps
`ingested_at`.

---

### Backfilling the rest of the years

Bronze already on disk needs no re-download — the transform is a local, offline job. To
load every year you have:

```bash
docker compose run --rm airflow-scheduler \
  airflow dags trigger transform_silver -c '{"start_year":1980,"end_year":2026}'
```

That maps one task per year (47 tasks). The DAG pins `max_active_tasks=3`, so at most
three run at once — deliberately, see below. A year takes ~9 s, so the whole backfill is
about 3 minutes.

**Three knobs, only one of which you tune at runtime:**

| Knob | Where | Controls | Measured on the Pi 5 |
|---|---|---|---|
| `silver_pool` slots | **Admin → Pools** (live, no restart) | how many **year tasks** run at once | ~220 MB RSS per task, mostly the fixed pandas/pyarrow import cost |
| `max_active_tasks` | DAG attribute, code only | same thing, as a backstop if the pool is deleted | — |
| `parent_batch_size` | trigger config (`params`) | how much data **one task** holds at a time | 5.4 MB per batch for Uruguay at the default 8 |

Note the split: `params` are per-run inputs and show up in *Trigger DAG w/ config*;
`max_active_tasks` is a DAG attribute and does **not**. That is why concurrency is bound to
a pool — pool slots are the only one of the three you can change from the UI while a
backfill is running.

For a small extent, task concurrency is the only thing that matters: Airflow's default of
16 concurrent tasks is ~5–7 GB and will thrash an 8 GB Pi into a hard freeze. Lowering
`parent_batch_size` would not have saved you — it governs the 5 MB, not the 220 MB.

`parent_batch_size` becomes the real lever at continental scale: LatAm is ~30,000 cells,
so one batch is ~130× the Uruguay figure — hundreds of MB per task. Lower it to 2–4 there.

Also check your swap: an 8 GB Pi shipping with a 200 MB swap file has no headroom to page
out under pressure, so a memory spike freezes the machine instead of cleanly failing one
task. 2 GB (`/etc/dphys-swapfile`, `CONF_SWAPSIZE=2048`) is a saner floor.

Re-running a year that is already loaded is **safe and expected**: the upsert replaces the
same `(parent_id, child_id, date)` rows and bumps `ingested_at`. There is no need to clear
anything first.

### Adding a new region (e.g. Honduras) — read this first

Bronze files are keyed `<variable>_<year>.parquet` with **no region in the name**, and the
download manifest records `variable:year` with **no extent**. Two consequences:

1. **A new-region download over years you already have is silently skipped.** The manifest
   says `tmax:2020` is done, so the DAG returns the existing file and downloads nothing.
2. **If you force it, the new region overwrites the old.** The writer does
   `os.replace(tmp, out_path)` — a whole-file replace, never an append. Honduras data would
   destroy the Uruguay data in that file.

So the answer to "append or overwrite?" is: *neither by default — it no-ops; and overwrite
if you defeat the manifest.* Keep each region in its **own bronze root**, selected per run
by the `data_root` param — no `.env` edit, no container restart:

```bash
# Honduras -> /data/hn (i.e. ./.localdata/hn/bronze); Uruguay stays on the default /data.
# No timezone param on GEE — Honduras cells get UTC-6 from their grid t_zone automatically.
docker compose run --rm airflow-scheduler airflow dags trigger download_bronze_gee \
  -c '{"extent":[12.9,-89.4,16.6,-83.1],"start_year":1980,"end_year":2026,"data_root":"/data/hn"}'

docker compose run --rm airflow-scheduler airflow dags trigger transform_silver \
  -c '{"start_year":1980,"end_year":2026,"data_root":"/data/hn"}'
```

`data_root` defaults to blank, which means "use the env `DATA_DIR`" — so every Uruguay
command you already run is unchanged. Each root carries its own `bronze/` tree and its own
manifest, so regions never collide or silently skip each other.

**The download and transform of one region must pass the *same* `data_root`.** The
transform reads whatever root you give it; point it at the wrong one and it reports "no
bronze parquet" for years it cannot see. Finish a region end-to-end with a consistent
`data_root` before moving on.

**Silver needs no such separation.** `wth_base` is global and keyed by cell, so both
regions coexist in one table and one query API — Honduras cells simply land in their own
`parent_id` partitions.

**Local-day offset is per cell, on the grid.** `date` in `wth_base` is a *local* calendar
day, and the offset that defined it (Honduras UTC−6, Uruguay UTC−3) lives on
`era5_land_base_grid.t_zone` — assigned once from the political tz shapefile at seed time and
applied at GEE reduction. No transform param, no separate table: GEE reduces each cell over
its own 24 h window, and `fetch_series` returns the offset as a column via a grid join. A
mixed-region database is unambiguous with **nothing to pass** — a single multi-country run is
already correct.

```sql
-- which offsets are present, and how many cells each
SELECT t_zone, count(*) FROM era5_land_base_grid WHERE is_land GROUP BY 1 ORDER BY 1;
```

> **Still open.** The manifest key is `variable:year` with no region, so a *new* extent over
> years you already downloaded is skipped as "done". `data_root` sidesteps it by giving each
> region its own manifest file — keep using separate roots until region is baked into the
> key itself.

## 5. Inspect it in DBeaver

### Connect

**Database → New Database Connection → PostgreSQL**:

| Field | Value |
|---|---|
| Host | `localhost` (or the Pi's IP / hostname when connecting remotely) |
| Port | `5432` (`POSTGRES_PORT` in `.env`) |
| Database | `era5` |
| Username | `era5` |
| Password | whatever you set as `POSTGRES_PASSWORD` |

Then **Test Connection** → **Finish**. If DBeaver offers to download the PostgreSQL
driver, accept.

> **Expect clutter.** Airflow stores its own metadata in this same database, so `public`
> holds ~40 `dag*` / `task*` / `ab_*` tables alongside ours. The ones that matter are
> `era5_land_base_grid` (carries per-cell `t_zone`), `wth_base`, `wth_qa_failures`, and the
> `wth_0XXX` partitions.
>
> **Do not expose port 5432 to the internet** — the password is whatever you typed in
> `.env` and there is no TLS in front of it. Keep it on the LAN or behind a tunnel.

### Verification queries

**Did silver load?**

```sql
SELECT count(*)            AS rows,
       count(DISTINCT child_id) AS cells,
       min(date), max(date)
FROM wth_base;
-- Uruguay 2020 only: 150792 | 412 | 2020-01-01 | 2020-12-31
```

**Are the values physically sane?** (silver units: °C, mm/day, MJ/m²/day, m/s, %, mm/day)

```sql
SELECT round(min(tmax)::numeric,2) AS tmax_min, round(max(tmax)::numeric,2) AS tmax_max,
       round(min(precip)::numeric,2) AS precip_min, round(max(precip)::numeric,2) AS precip_max,
       round(min(rh)::numeric,2) AS rh_min, round(max(rh)::numeric,2) AS rh_max,
       round(min(et0)::numeric,2) AS et0_min, round(max(et0)::numeric,2) AS et0_max
FROM wth_base;
-- Uruguay: tmax ~ -5..45, precip >= 0, rh 0..100, et0 ~ 0..12
```

**Did anything fail QA?** Empty is the expected answer.

```sql
SELECT reason, count(*) FROM wth_qa_failures GROUP BY reason ORDER BY 2 DESC;
```

**Is `et0` actually populated?** All-NULL here means the block-size mismatch from §2.

```sql
SELECT count(*) AS total, count(et0) AS with_et0, count(rh) AS with_rh FROM wth_base;
```

**Did partitioning work?** One partition per `parent_id` touched.

```sql
SELECT count(*) FROM pg_tables WHERE tablename LIKE 'wth\_0%';   -- Uruguay 2020: 33
```

**ERA5T provenance** — recent data is preliminary until final ERA5 replaces it (§11.3):

```sql
SELECT is_preliminary, count(*), min(date), max(date) FROM wth_base GROUP BY 1;
```

**One cell's time series** — the point of the whole pipeline. Montevideo ≈ (−34.9, −56.2)
snaps to the cell centred on (−35.0, −56.25):

```sql
SELECT date, tmax, tmin, precip, srad, wind, rh, et0
FROM wth_base
WHERE parent_id = '0YYF' AND child_id = 'FGHR'
ORDER BY date;
```

Get the two codes for any coordinate without guessing:

```bash
uv run python -c "from src.db.query import locate; print(locate(-34.9, -56.2))"
```

Always filter on `parent_id` first — it is the partition key, so it lets Postgres skip
every other partition.

**Map view.** `era5_land_base_grid.geom` is a real PostGIS polygon column: run
`SELECT child_id, geom FROM era5_land_base_grid WHERE parent_id = '0YYF';`, click the
**Spatial/Value** tab in DBeaver's result grid, and the cells render on a basemap.

---

## 6. Read it from Python

```python
from src.db.query import fetch_series

df = fetch_series(-34.9, -56.2, "2020-01-01", "2020-12-31")   # Montevideo
print(df.head())
```

A date-indexed DataFrame in silver units. The cell is resolved arithmetically — no spatial
index, no join — so a single-cell fetch is milliseconds.

Running on the **host** (outside Docker) needs two overrides, because `.env` is written for
the container's point of view:

```bash
POSTGRES_HOST=localhost DATA_DIR=.localdata uv run python your_script.py
```

---

## 7. Troubleshooting

| Symptom | Cause & fix |
|---|---|
| Every `et0` is NULL | Grid block-size mismatch — run the `EU9K → 0XKE` check in §2 and restore the seed. |
| `PermissionError` writing `/data/bronze/...` | Containers run as uid 50000; the host bind-mount is uid 1000. `chmod 777 .localdata` (single-user Pi). |
| Host script can't read `.localdata/bronze/_manifest.json` | Same ownership split — the manifest is mode 0600 uid 50000. Read it from inside a container. |
| `PermissionError` on the GEE service-account key | Containers run **uid 50000, gid 0**, so the key must be group-readable by root — **not** world-readable. `sudo chgrp 0 <key> && chmod 640 <key>`. Avoid `chmod o+r`: it exposes the private key to every local user, and `chmod 600` locks the container out (the failure mode this row exists for). |
| `Temporary failure in name resolution` inside a task (CDS/GEE/GCS unreachable, host is fine) | The stack came up **before** the network did, so Docker baked an empty upstream into its embedded resolver. Confirm with `docker exec <scheduler> cat /etc/resolv.conf` — `NO EXTERNAL NAMESERVERS DEFINED` is the tell. Fix: `docker compose restart` once the host resolves (`getent hosts oauth2.googleapis.com`). Recurs after any boot where Wi-Fi association is slow; a fixed `dns:` entry in `docker-compose.yml` prevents it. |
| DAG re-runs with the *old* config | **Clear** replays a run with its original conf. Use **Trigger DAG w/ config** for new params. |
| A backfill is running away | `docker compose restart airflow-scheduler` kills in-flight LocalExecutor subprocesses. Submitted GEE exports continue server-side regardless. |
| `airflow dags test` "passes" but nothing ran | It does not execute `.expand()` dynamically-mapped tasks. Use `airflow dags trigger` plus a running scheduler. |
| Pi freezes during a multi-year transform | Too many concurrent year tasks, not batch size. The DAG pins `max_active_tasks=3`; if you raised it, put it back. Check swap is ≥2 GB. |
| Pi runs out of memory inside one task | Lower `parent_batch_size` (matters at continental scale); in GEE download, lower `chunk_days`. |
| Import errors after editing `src/` | `./src` is bind-mounted, so edits are live — but the scheduler caches parsed DAGs briefly. `docker compose restart airflow-scheduler`. |

---

## What is not built yet

- **Step 5** — the `update` DAG (daily append + rolling ERA5T re-fetch flipping
  `is_preliminary` to FALSE) is still a stub.
- **Step 6** — gold DSSAT `.WTH` materialization, including the `TAV`/`AMP`/`ELEV` header.
- **Step 7** — the global-grid viewer / extent-selection frontend.
