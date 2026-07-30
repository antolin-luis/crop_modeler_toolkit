# Step 3b — Bronze Download via Google Earth Engine

> An **alternative backend** to the CDS bronze download (`docs/step3_bronze_download.md`),
> built because the CDS queue + MARS-tape staging make long backfills intolerably slow
> (~7 days to cover 1980→1986). GEE serves ERA5 from hot storage and reduces hourly→daily
> **server-side**, exporting only the daily result. Output schema, paths, grid encoding,
> and manifest are **identical** to the CDS path, so files from either backend are
> interchangeable. One-time account setup lives in **`docs/gee_setup.md`** — read that first.

## Context

Bronze is the raw landing zone (PLANNING.md §7): Parquet on disk, **one file per variable
per year**, native ERA5 values **unconverted** (`child_id, parent_id, date, value`). The
GEE backend produces exactly those files.

The key design move is **local-day, server-side aggregation**. GEE's own daily product
(`ECMWF/ERA5_LAND/DAILY_AGGR`) aggregates on the **UTC** day, which violates the project's
same-local-day rule (§5.3). So we pull the **hourly** collection and reduce it ourselves
over a shifted window, entirely server-side — only the finished daily raster is exported.

## Decisions

- **Dataset: `ECMWF/ERA5/HOURLY` (0.25°)** — the same grid as `src/grid/spec.py`
  (NLON=1440, NLAT=721, origin 0/90). `child_id`/`parent_id` therefore match the CDS bronze
  exactly. *Not* `ECMWF/ERA5_LAND/HOURLY` (0.1°), which would break the immutable grid spec.
- **Transport: `Export.image.toCloudStorage` → GCS → download GeoTIFF.** Headless- and
  service-account-friendly; no Google Drive OAuth.
- **Coexist with CDS.** New `src/gee/`, new `download_bronze_gee` DAG; `src/cds/` untouched.
  Shared grid encoder, `Manifest`, and bronze schema.

## Variable contract (download side, §5.1)

All from `ECMWF/ERA5/HOURLY`; bronze stores the **source** value (conversion is Step 4).

| silver name | GEE band                              | reducer |
|-------------|---------------------------------------|---------|
| `tmax`      | `temperature_2m`                      | max     |
| `tmin`      | `temperature_2m`                      | min     |
| `precip`    | `total_precipitation`                 | sum     |
| `srad`      | `surface_solar_radiation_downwards`   | sum     |
| `wind_u`    | `u_component_of_wind_10m`             | mean    |
| `wind_v`    | `v_component_of_wind_10m`             | mean    |
| `tdew`      | `dewpoint_temperature_2m`             | mean    |

Two correctness wins over the CDS contract:

- **tmax/tmin** are the max/min of hourly `temperature_2m`, computed by us — this sidesteps
  the biased `maximum_2m_temperature_since_previous_post_processing` parameter (§5.2).
- **precip/srad** are **summed** over the local-day window. In `ECMWF/ERA5/HOURLY` the
  `total_precipitation` / `surface_solar_radiation_downwards` bands are **per-hour
  increments** (1-hour accumulations) — verified live by sampling consecutive hours — *not*
  running totals since 00 UTC, so summing them is correct. (Unlike ERA5-Land, whose
  accumulations reset at 00 UTC.)

> Band names are the one thing to confirm against the live catalog before a real backfill
> (GEE occasionally renames bands between dataset revisions):
> <https://developers.google.com/earth-engine/datasets/catalog/ECMWF_ERA5_HOURLY>.
> `src/gee/variables.py` is the single place to adjust.

## Local-day window, per cell (§5.3)

For offset `o` hours, local day `D` covers UTC `[D 00:00 − o, D+1 00:00 − o)` — i.e.
`[D 03:00Z, D+1 03:00Z)` for −3. The offset is **per cell, not a run parameter**: there is no
`timezone` param. `download_variable_year` reads the tz-polygon asset (`GEE_TZ_ASSET`),
intersects it with the extent, and builds one reduction **zone** `(offset_hours, region)` per
UTC offset present (`export.tz_zones`). `build_daily_collection(zones=...)` reduces each
zone over its own shifted window, clips to its region, and mosaics the disjoint zones into
one daily image. So an extent spanning several timezones is correct in a single run, and the
window each cell used matches its `era5_land_base_grid.t_zone` (both come from the same tz
shapefile — see setup below). The same offset still applies to every *variable* of a cell.

### One-time setup: the tz asset
`scripts/build_tz_asset.py` dissolves the timezone-boundary-builder polygons by **standard**
(non-DST) UTC offset — the exact mapping `seed_grid.std_offset_minutes` stamps into the grid's
`t_zone` — and writes a GeoJSON to upload as an EE table asset (property `offset` = minutes).
Use the *combined-with-oceans* boundary set so zones tile the globe with no coastal gaps. Set
`GEE_TZ_ASSET=projects/<ee-project>/assets/tz_by_offset` in `.env`.

## Files

- `src/gee/variables.py` — silver-name → `(band, reducer)`, `COLLECTION="ECMWF/ERA5/HOURLY"`.
  Silver keys mirror `src/cds/variables.py`.
- `src/gee/daily.py` — `build_daily_collection(variable, year, *, zones)` builds the
  per-local-day `ee.ImageCollection`, mosaicking one clipped reduction per `(offset, region)`
  zone (pure server-side graph; no I/O). `parse_offset_hours` (retained utility).
- `scripts/build_tz_asset.py` — maintainer one-off: build the tz-by-offset EE asset.
- `src/gee/client.py` — `GEEClient`: `ee.Initialize` from `GEEConfig` (service account if
  `GEE_SERVICE_ACCOUNT_FILE` set, else stored user OAuth).
- `src/gee/export.py` — flatten the collection to a date-named multi-band image, export to
  GCS aligned to the canonical grid (`crsTransform`, the GEE analogue of "never regrid"),
  poll the task to completion with backoff, download the shard(s). `tz_zones(extent, asset)`
  derives the per-offset reduction zones (the one `getInfo` on this path). Two read paths:
  `iter_geotiff_chunks` (streams band-windows — the production path) and `read_daily_geotiffs`
  (whole-load — small rasters/tests). `start_export(land_only=True)` clips the export region
  to land (LSIB) so EE skips interior-ocean tiles.
- `src/gee/download.py` — `download_variable_year(...)`: build → export+fetch → **stream**
  the GeoTIFF in `chunk_days` windows through the shared `encode_grid` → `dropna` (masked
  ocean) → append to a `ParquetWriter` (`.tmp` then atomic `os.replace`) → mark manifest. A
  full LatAm year never lives in RAM (the Pi-OOM fix). Bronze row order is unspecified (the
  streamed write can't globally sort; silver upserts on the PK regardless).
- `src/gee/metrics.py` — per-run cost accounting: `RunMetrics` (EECU, bytes egressed,
  wall-clock, units of work), the append-only `_gee_metrics.jsonl` record store, and
  `run_export` — the timed `start_export → wait_for_task → download_prefix_measured`
  triplet the bronze path and the cost probes both go through. Schema:
  `docs/cost_model_climate_context.md` §4.1.
- `airflow/dags/download_bronze_gee.py` — DAG mirroring `download_bronze`; tasks bound to
  the `gee_pool`. Takes a `sample` param (calibration label), logs a one-line cost summary
  per mapped task and returns it as a dict XCom.
- `scripts/gee_cost_probe.py` — maintainer one-off: measure a GFS or CHIRPS export through
  the same instrumented path, writing no bronze/silver (cost-model samples E2/E3).

### Reused (not reimplemented)
- `src/grid/encode_long.encode_grid` — the raster→long step, shared with the CDS netCDF
  path (one encoder, two readers, bit-identical output). Includes the daily-sanity guard.
- `src/grid/encoding.cell_codes`/`parent_codes`, `src/cds/manifest.Manifest` (source-
  agnostic; a `(variable, year)` done by *either* backend is done).

## Constraints honored
- **0.25° ERA5 only** — keeps `child_id` compatible; `src/grid/spec.py` unchanged.
- **No regridding** — export at native resolution via `crsTransform`; `encode_grid` snaps
  to nearest index for any residual offset.
- **Local-day** window, per-cell offset from the grid `t_zone`; same offset for all
  variables of a cell (§5.3).
- **Bronze is raw** — only temporal aggregation server-side; units stay native (Step 4
  converts).
- **Daily sanity** — exactly one value per cell per day before writing.

## Running it

1. Complete `docs/gee_setup.md` (project, auth, GCS bucket, `.env`).
2. Build + upload the tz asset (once) and set `GEE_TZ_ASSET` — see "One-time setup" above.
3. Create the pool once: `airflow pools set gee_pool 2 "GEE export cap"`.
4. Trigger `download_bronze_gee` with `extent` `[S, W, N, E]`, `start_year`/`end_year`,
   `variables`, plus `land_only` (default True — clip to land, drop ocean) and `chunk_days`
   (default 30 — lower it if memory is tight on the Pi). **No `timezone`** — it is per-cell.
4. Stay under the monthly EECU quota by **splitting the year range across runs** (there is
   no per-request cost ceiling to probe, so no adaptive splitter — the year range is the
   one lever). See `docs/gee_setup.md` §7.

## Verification

1. `uv run pytest tests/test_gee_variables.py tests/test_gee_daily.py tests/test_gee_encode.py tests/test_encode_long.py` — pass (all GEE-mocked, no live calls).
2. `uv run pytest tests/` — full suite green (the `encode_grid` refactor didn't change CDS output).
3. `uv run ruff check src tests` — clean.
4. **Live auth smoke:**
   `uv run python -c "import ee, src.gee.client as c; c.GEEClient(); print(ee.Number(1).getInfo())"` → `1`.
5. **Calibration run (the key cost step):** trigger `download_bronze_gee` for **1 year ×
   Uruguay × all 7 variables**, passing `"sample":"E1"` and a dedicated `"data_root"`
   (the manifest is keyed `variable:year` with no extent, so without a fresh root a re-run
   silently no-ops). Confirm `bronze/<var>/<var>_<year>.parquet` exists, row count ==
   cells × days, values finite, a `child_id` decodes (`code_to_latlon`) to the expected
   cell, and the product is genuinely daily. Then read `<data_root>/bronze/_gee_metrics.jsonl`
   — seven records, one per variable — for EECU-hours, **bytes egressed**, and the real
   GeoTIFF compression ratio, and extrapolate the full LatAm / multi-decade cost before
   scaling. `eecu_hours: null` means EE did not report it; look the `task_id` up on the EE
   task list. Full protocol: `docs/cost_model_climate_context.md` §9.1.
6. **Cross-backend parity:** for one small `(variable, year)` fetched by *both* CDS and GEE,
   confirm the same `child_id` set and `date` coverage (values differ only by each service's
   own aggregation — a sanity check, not bit-equality).
7. Re-trigger → no-op (manifest skips), same as the CDS path.

## Out of scope (later steps)
- Wide merge, RH/ET0/QA, `wth_base` upsert (Step 4) — backend-agnostic, unchanged.
- ERA5T preliminary→final re-fetch (Step 5).
- Retiring the CDS backend (the two coexist).
