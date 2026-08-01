# Implementation Plan — Climate Context Layer (bronze + silver)

**Status:** plan only. No code written. Supersedes nothing; extends `PLANNING.md`.

**Companion:** [`climate_context_layer.md`](climate_context_layer.md) is the feasibility study — *why* and *whether*. This document is *what* and *in what order*.

**Scope:** bronze ingestion + silver modeling for all ten sources behind the CENAOS/COPECO seasonal bulletin. **Gold (`.WTH`) is explicitly out of scope** — the forecast data becomes queryable, but nothing materializes weather files from it in this plan.

---

## 1. Decisions already fixed

These are settled and the plan assumes them. Changing any one reshapes the work.

| # | Decision | Consequence |
|---|---|---|
| D1 | **Silver keeps only the current forecast issuance.** New run replaces the whole horizon; old rows deleted. | No `init_date` in the PK. Refresh is whole-horizon replace, not upsert. §5.3 |
| D2 | **Bronze keeps every issuance, append-only.** | The D1 discard stays reversible; silver is rebuildable from bronze. §4 |
| D3 | **Forecast trust comes from the hindcast, not from accumulated live runs.** | Phase 5 (`forecast_hindcast_stats`) is not optional — it is what makes Phase 4 numbers usable. §6.5 |
| D4 | **Forecasts stored on native grid; regridding is a silver derivation.** | ~1.0° SEAS5/NMME need a second grid (`fcell_id`); 0.25° GFS reuses `child_id` untouched. §3 |
| D5 | **SEAS5 first, GFS second.** | Phase order 4 → 6. `forecast_value` designed once to hold both monthly and daily periods. |
| D6 | **New tables, never new columns on `wth_base`.** | `wth_base` stays exactly what it is: observed, one value, one cell, one local day. |

---

## 2. Principles carried over from `PLANNING.md`

The existing codebase has strong conventions. This layer inherits them rather than inventing parallel ones:

- **Deterministic identifiers over stored mappings.** `child_id` is a pure function of coordinates (`src/grid/spec.py`). The forecast grid follows the same rule — `fcell_id` is arithmetic, not a lookup table.
- **`COPY`, never row-by-row `INSERT`.** Reuse `src/db/load.py::copy_csv`. Every loader in this plan goes through it.
- **Bronze is Parquet on disk, one file per natural chunk**, with a manifest for idempotent resume (`src/cds/manifest.py`).
- **Bounded memory.** Target is a Pi 5. Chunk by batch and commit per batch, as `transform_silver` already does.
- **`.env` is the only file a user edits.** New settings go through `src/config.py` dataclasses.
- **A DAG parameter, not a code edit,** for anything an operator might vary per run.

---

## 3. The forecast grid (`fcell_id`)

### 3.1 Why a second grid

SEAS5 and NMME are delivered at ~1.0°. Storing them against `child_id` would require regridding at ingest, which D4 forbids. Storing them as raw lat/lon would force a PostGIS join on every query, which contradicts the project's core "identifiers are arithmetic" principle.

So: a second deterministic grid, same encoding machinery, different resolution.

### 3.2 `src/forecast/fspec.py` — canonical constants

```python
FRESOLUTION = 1.0    # degrees
FLON_ORIGIN  = 0.0
FLAT_ORIGIN  = 90.0
FNLON = 360
FNLAT = 181
# 360 * 181 = 65_160 cells < 36**4 = 1_679_616  -> CHAR(4), same width as child_id
```

**Same immutability contract as `src/grid/spec.py`.** Once forecast data lands, these constants are frozen for the life of the database. Document this at the top of the module in the same terms.

### 3.3 `src/forecast/fencoding.py`

Two functions, both pure:

- `fcell_code(lat, lon) -> str` — mirror of `grid.encoding.cell_code` at 1.0°.
- `fcell_for_child(child_id) -> str` — decode the 0.25° cell to its centroid via existing `code_to_latlon`, re-encode at 1.0°. This is the ERA5↔forecast join, and it is arithmetic — no table, no GiST.

**Refactor note:** `grid/encoding.py` currently hardcodes the 0.25° spec. Prefer parameterizing the core encode/decode on a spec object and having both `spec.py` and `fspec.py` supply one, over copy-pasting the arithmetic. If that refactor looks risky against the existing seed, copy-paste with a loud comment is acceptable — but the duplicated arithmetic must then be covered by a test asserting the two implementations agree on a shared case.

### 3.4 What does *not* need this

GFS is native 0.25° on the same origin as ERA5. Phase 6 writes `child_id` directly and never touches `fspec`/`fencoding`. This is the single biggest cost difference between the two forecast paths.

**But it does inherit the export budget.** Every GEE-backed phase here rides the same export machinery as the ERA5 backfill, and that machinery has a measured ceiling: past a certain size EE restarts the export (`attempt` 2, 3) instead of running it, indefinitely, until something cancels it. For ERA5 the failure predictor is `land_cells × zones` and the line sits at **58,554** (`docs/cost_model_climate_context.md` §9.4, `src/gee/chunks.CELL_ZONE_CEILING`).

⚠ **That number does not transfer.** It was measured at **366 daily bands** per export. The general quantity is `cells × zones × bands`: Brazil's failure was ≈ 33.2 M cell-zone-bands, the largest success ≈ 21.4 M. A 16-band GFS export and a 36-band dekadal NDVI export sit in completely different places on that scale.

So: **each new source confirms its own budget with one probe export at the target extent before its first backfill.** Do not assume ERA5's number, in either direction — it is as wrong to chunk a 16-band GFS export as if it were an ERA5 year as it is to submit a 365-band CHIRPS year unchunked because "GFS was fine".

---

## 4. Bronze layout (complete, final)

```
/data/bronze/
  static/
    geopotential.nc
    era5_land_mask.nc
  <variable>/                                   # EXISTING ERA5 — untouched
    <variable>_<year>.parquet

  indices/
    oni/oni.parquet                             # full history; rewritten each fetch
    nino34/nino34_weekly.parquet
    enso_plume/enso_plume_<YYYYMM>.parquet      # one file per IRI issuance

  forecast/
    seas5/<variable>/<init_YYYYMM>.parquet      # append-only (D2)
    seas5_hindcast/<variable>/<init_MM>.parquet # 1993-2016, static once fetched
    gfs/<variable>/<init_YYYYMMDDHH>.parquet
    nmme/<variable>/<init_YYYYMM>.parquet

  cyclones/
    ibtracs/ibtracs_<basin>.parquet
    nhc_active/<storm_id>_<advisory>.parquet

  chirps/
    chirps_<year>.parquet                       # 0.25°-aggregated, child_id schema

  ndvi/
    ndvi_<year>.parquet                         # dekadal, 0.25°-aggregated

  admin/
    gadm_<iso3>.parquet                         # static
```

**Manifest.** Extend `src/cds/manifest.py` rather than writing a second one — it already does atomic writes and two-granularity tracking. Generalize the key from `(variable, year)` to `(source, variable, chunk_key)`. The existing ERA5 keys must keep working; add the new form beside them, do not migrate.

**Retention.** Bronze forecast issuances accumulate forever by default. At ~200 KB per SEAS5 issuance this is irrelevant; at GFS's 4-runs-a-day it is not. Add a `retain_issuances` DAG param (default: keep all for SEAS5/NMME, keep 30 days for GFS).

---

## 5. Silver DDL

New file `src/db/context_schema.sql`, created by the context DAGs — same rationale as `silver_schema.sql`: an existing database picks the tables up without a volume reset.

### 5.1 `wth_normals`

```sql
CREATE TABLE IF NOT EXISTS wth_normals (
    parent_id     CHAR(4)  NOT NULL,
    child_id      CHAR(4)  NOT NULL,
    period        TEXT     NOT NULL,   -- '1991-2020'
    scale         TEXT     NOT NULL,   -- 'month' | 'pentad'
    bin           SMALLINT NOT NULL,   -- 1-12 or 1-73
    tmax_mean     REAL,
    tmin_mean     REAL,
    precip_mean   REAL,
    precip_sd     REAL,
    precip_p33    REAL,                -- terciles: the "below normal" boundary
    precip_p67    REAL,
    srad_mean     REAL,
    et0_mean      REAL,
    n_years       SMALLINT NOT NULL,   -- how many years actually contributed
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, child_id, period, scale, bin)
) PARTITION BY LIST (parent_id);
```

**Terciles matter.** Every "below/near/above normal" statement in the bulletin is a tercile statement. Storing only mean and sd forces consumers to assume normality, which for precipitation is wrong. Store `p33`/`p67` directly.

**`scale` not `doy`.** Day-of-year normals would multiply row count by ~30 over monthly for no analytical gain at seasonal timescales. Monthly + pentad (73 bins) covers the bulletin, including its 5-day charts.

**`n_years` is a QA column,** not decoration — a cell with 6 contributing years should not be silently presented as a 30-year normal.

### 5.2 `climate_index`

```sql
CREATE TABLE IF NOT EXISTS climate_index (
    index_name    TEXT     NOT NULL,   -- 'oni' | 'nino34' | 'soi'
    period_start  DATE     NOT NULL,
    period_end    DATE     NOT NULL,
    value         REAL,
    anomaly       REAL,
    phase         TEXT,                -- 'el_nino' | 'la_nina' | 'neutral'
    source        TEXT     NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (index_name, period_start)
);
```

Unpartitioned; a few thousand rows forever. `phase` is derived at load (ONI ≥ +0.5 / ≤ −0.5 sustained) because analog-year selection in Phase 11 needs it and recomputing the sustained-quarters rule at query time is error-prone.

### 5.3 `forecast_value` — the core table

```sql
CREATE TABLE IF NOT EXISTS forecast_value (
    system        TEXT     NOT NULL,   -- 'seas5' | 'gfs' | 'nmme'
    target_start  DATE     NOT NULL,
    target_end    DATE     NOT NULL,
    cell_id       CHAR(4)  NOT NULL,   -- fcell_id for seas5/nmme; child_id for gfs
    cell_grid     TEXT     NOT NULL,   -- 'era5_025' | 'fcst_100'  (which grid cell_id is on)
    variable      TEXT     NOT NULL,   -- 'precip' | 'tmax' | 'tmin' | 'tmean'
    member        SMALLINT NOT NULL,   -- ensemble member; 0 = deterministic/ensemble-mean
    value         REAL,
    anomaly       REAL,                -- vs the system's own hindcast climatology
    init_date     DATE     NOT NULL,   -- provenance: which run produced this ("as of")
    lead_days     SMALLINT NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (system, target_start, cell_id, variable, member)
) PARTITION BY LIST (system);
```

Design notes:

- **`init_date` out of the PK** — this is what makes the table current-only (D1).
- **`cell_grid` is explicit.** A `CHAR(4)` code is meaningless without knowing which grid it indexes, and this table holds both. Making it implicit from `system` would be a landmine the first time a system changes resolution.
- **`member = 0` for deterministic**, rather than `NULL`, so it can sit in the PK without nullable-key weirdness.
- **Monthly and daily coexist** via `target_start`/`target_end`. A GFS day is a one-day period; a SEAS5 month is a ~30-day period. No schema change between D5's two halves.
- **`lead_days` is not derivable at query time** once `init_date` is only provenance — and it is the honest confidence signal. A 3-day-lead row and a 200-day-lead row must not look alike to a consumer.

**Refresh — partition TRUNCATE, not DELETE.** This detail follows directly from D1 and matters operationally:

```sql
BEGIN;
TRUNCATE forecast_value_gfs;        -- the LIST partition for this system
COPY   forecast_value_gfs (...) FROM STDIN WITH (FORMAT csv);
COMMIT;
```

A daily `DELETE` of the full GFS horizon (order 10⁷ rows, §10) would generate dead tuples at a rate autovacuum on a Pi will not keep up with, and the table would bloat without bound — the exact failure mode D1 was supposed to avoid. `TRUNCATE` on a partition reclaims immediately and is transactional in Postgres, so readers still never see a half-loaded forecast. **Partitioning by `system` is therefore load-bearing, not an optimization.**

### 5.4 `forecast_hindcast_stats`

```sql
CREATE TABLE IF NOT EXISTS forecast_hindcast_stats (
    system         TEXT     NOT NULL,
    cell_id        CHAR(4)  NOT NULL,
    cell_grid      TEXT     NOT NULL,
    variable       TEXT     NOT NULL,
    target_month   SMALLINT NOT NULL,  -- 1-12
    lead_months    SMALLINT NOT NULL,  -- 0-6
    hindcast_mean  REAL,               -- model climatology
    hindcast_sd    REAL,
    obs_mean       REAL,               -- from wth_normals, same period
    obs_sd         REAL,
    bias           REAL,               -- hindcast_mean - obs_mean
    skill_acc      REAL,               -- anomaly correlation vs observed
    n_years        SMALLINT NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (system, cell_id, variable, target_month, lead_months)
);
```

Computed once from the reforecast archive, then static. `bias` is what converts a raw model value into a calibrated one; `skill_acc` is what tells a user whether to believe it at that lead time.

### 5.5 `enso_forecast`

```sql
CREATE TABLE IF NOT EXISTS enso_forecast (
    issued_on     DATE     NOT NULL,
    model         TEXT     NOT NULL,   -- 'CMC CANSIP', 'DYN AVG', ...
    model_type    TEXT     NOT NULL,   -- 'dynamical' | 'statistical' | 'average'
    target_season TEXT     NOT NULL,   -- 'ASO'
    target_start  DATE     NOT NULL,
    sst_anomaly   REAL,                -- °C, Niño 3.4
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (issued_on, model, target_start)
);
```

**Exception to D1: this one accumulates.** It is tiny (~25 models × 9 seasons per issuance ≈ 225 rows/month), and the plume's whole analytical value is watching forecasts converge or diverge across issuances. Cost of keeping it is negligible; the reasoning behind D1 (unbounded growth of a large table) does not apply.

### 5.6 `tc_track` and `tc_cell_exposure`

```sql
CREATE TABLE IF NOT EXISTS tc_track (
    storm_id      TEXT     NOT NULL,   -- IBTrACS SID
    obs_time      TIMESTAMPTZ NOT NULL,
    season        SMALLINT NOT NULL,
    basin         TEXT     NOT NULL,
    name          TEXT,
    lat           REAL     NOT NULL,
    lon           REAL     NOT NULL,
    wind_kt       REAL,
    pressure_mb   REAL,
    category      TEXT,                -- 'TD' | 'TS' | '1'..'5'
    is_forecast   BOOLEAN  NOT NULL DEFAULT FALSE,  -- NHC advisory vs IBTrACS best-track
    geom          GEOMETRY(Point, 4326),
    PRIMARY KEY (storm_id, obs_time)
);
CREATE INDEX IF NOT EXISTS tc_track_geom_gix ON tc_track USING GIST (geom);
CREATE INDEX IF NOT EXISTS tc_track_season_ix ON tc_track (season, basin);

CREATE TABLE IF NOT EXISTS tc_cell_exposure (
    storm_id            TEXT     NOT NULL,
    parent_id           CHAR(4)  NOT NULL,
    child_id            CHAR(4)  NOT NULL,
    closest_approach_km REAL     NOT NULL,
    max_wind_kt         REAL,
    exposure_start      DATE     NOT NULL,
    exposure_end        DATE     NOT NULL,
    PRIMARY KEY (storm_id, child_id)
);
```

`is_forecast` keeps live NHC advisory positions from contaminating the historical best-track archive — they share a shape but not a reliability.

`tc_cell_exposure` is the derived table that earns its keep: which cells were hit, when, how hard. Computed once per storm by buffering the track line and intersecting the grid.

### 5.7 `wth_precip_alt` (CHIRPS)

```sql
CREATE TABLE IF NOT EXISTS wth_precip_alt (
    parent_id     CHAR(4)  NOT NULL,
    child_id      CHAR(4)  NOT NULL,
    date          DATE     NOT NULL,
    source        TEXT     NOT NULL,   -- 'chirps_v3'
    precip        REAL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, child_id, date, source)
) PARTITION BY LIST (parent_id);
```

**Deliberately a separate table, and deliberately year-scoped.** See §10 — a full 46-year CHIRPS backfill is the same order of magnitude as the entire existing `wth_base`. Default the DAG to a recent window (10 years) and make the full backfill an explicit opt-in.

### 5.8 `admin_boundary` and `drought_alert`

```sql
CREATE TABLE IF NOT EXISTS admin_boundary (
    admin_id      TEXT     NOT NULL,   -- GADM GID
    iso3          TEXT     NOT NULL,
    level         SMALLINT NOT NULL,   -- 0 country, 1 dept, 2 municipality
    name          TEXT     NOT NULL,
    parent_admin  TEXT,
    geom          GEOMETRY(MultiPolygon, 4326) NOT NULL,
    PRIMARY KEY (admin_id)
);
CREATE INDEX IF NOT EXISTS admin_boundary_gix ON admin_boundary USING GIST (geom);

CREATE TABLE IF NOT EXISTS admin_cell_map (      -- materialized, not recomputed per query
    admin_id      TEXT     NOT NULL,
    parent_id     CHAR(4)  NOT NULL,
    child_id      CHAR(4)  NOT NULL,
    area_fraction REAL     NOT NULL,   -- of the cell inside this admin unit
    PRIMARY KEY (admin_id, child_id)
);

CREATE TABLE IF NOT EXISTS drought_alert (
    admin_id      TEXT     NOT NULL,
    as_of         DATE     NOT NULL,
    scale_months  SMALLINT NOT NULL,   -- SPI-3, SPI-6...
    spi           REAL,
    status        TEXT     NOT NULL,   -- 'watch' | 'green' | 'yellow' | 'red'
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (admin_id, as_of, scale_months)
);
```

`admin_cell_map` is materialized deliberately: the polygon↔cell intersection is expensive and static, and every drought recomputation would otherwise repeat it.

### 5.9 `ndvi_anomaly`

```sql
CREATE TABLE IF NOT EXISTS ndvi_anomaly (
    parent_id     CHAR(4)  NOT NULL,
    child_id      CHAR(4)  NOT NULL,
    dekad_start   DATE     NOT NULL,
    ndvi          REAL,
    ndvi_lta      REAL,                -- long-term average, same dekad
    anomaly_pct   REAL,
    PRIMARY KEY (parent_id, child_id, dekad_start)
) PARTITION BY LIST (parent_id);
```

---

## 6. New source modules

Mirroring the existing flat layout (`cds/`, `gee/`, `grid/`, `db/`, `transform/`):

```
src/
  normals/
    compute.py        # wth_base -> wth_normals (SQL-heavy, chunked by parent batch)
  indices/
    sources.py        # URLs + parsers for ONI / Niño 3.4 / SOI
    download.py       # fetch -> bronze parquet
    plume.py          # IRI ENSO plume
  forecast/
    fspec.py          # coarse-grid constants (immutable)
    fencoding.py      # fcell_code, fcell_for_child
    seas5.py          # CDS seasonal-monthly-single-levels
    hindcast.py       # reforecast fetch + stats computation
    gfs.py            # GEE NOAA/GFS0P25 daily reduce
    nmme.py           # IRI OPeNDAP
    load.py           # forecast_value TRUNCATE-partition + COPY refresh
  cyclones/
    ibtracs.py        # best-track archive
    nhc.py            # live advisories
    exposure.py       # track buffer x grid -> tc_cell_exposure
  chirps.py           # GEE UCSB-CHG/CHIRPS/DAILY -> wth_precip_alt
  ndvi.py             # GEE NDVI -> ndvi_anomaly
  admin/
    boundaries.py     # GADM -> admin_boundary + admin_cell_map
    drought.py        # SPI -> drought_alert
  analog/
    select.py         # ENSO phase -> analog years
    forecast.py       # analog-year distributions per cell/pentad
  db/
    context_schema.sql
    context_load.py   # COPY helpers for the new tables
```

Reuse, do not reimplement: `src/db/load.py::copy_csv` and `connect`, `src/cds/client.py`, `src/gee/client.py` + `export.py`, `src/cds/manifest.py`, `src/grid/encoding.py`.

---

## 7. DAGs

Four new DAGs. Splitting them (rather than one `update_context`) is driven by cadence, which spans monthly to 6-hourly.

| DAG | Schedule | Does |
|---|---|---|
| `build_context_base` | manual / annual | `wth_normals`, `admin_boundary` + `admin_cell_map`, IBTrACS historical load, hindcast stats. The expensive one-off, mirroring `grid_build`. |
| `update_indices` | monthly | ONI / Niño 3.4 / SOI → `climate_index`; IRI plume → `enso_forecast`. Tiny. |
| `update_forecast` | monthly (SEAS5, NMME) + daily (GFS) | Download → bronze → TRUNCATE-partition refresh of `forecast_value`. One mapped task per system so a SEAS5 failure does not block GFS. |
| `update_hazards` | 6-hourly during season, else daily | NHC active advisories → `tc_track`; `drought_alert` recompute. The 6-hourly cadence is precisely why this is not folded into `update_forecast`. |

**Pools.** Add `context_pool` alongside the existing `cds_pool` / `gee_pool` / `silver_pool`, so index and cyclone HTTP fetches cannot starve a running ERA5 backfill. GFS work goes through the existing `gee_pool` — now capped at **4**, not 2 (measured E1b: four in-flight exports are a net 2.64× over serial) — and it competes for the same EECU quota. Sharper than it sounds given §10 below: that is a quota the ERA5 backfill has already largely spent.

**New DAG params** (all with defaults, per §2):

| Param | DAG | Notes |
|---|---|---|
| `normals_period` | build_context_base | default `1991-2020` |
| `normals_scales` | build_context_base | default `["month","pentad"]` |
| `forecast_systems` | update_forecast | default `["seas5"]`; add `gfs`, `nmme` as phases land |
| `forecast_members` | update_forecast | `all` or an int cap |
| `retain_issuances` | update_forecast | bronze retention; default unlimited / 30 for GFS |
| `chirps_start_year` | build_context_base | default `now - 10y` — see §10 |
| `iso3_list` | build_context_base | admin boundaries to load, default `["HND"]` |
| `spi_scales` | update_hazards | default `[3, 6]` |
| `chunk_parents` | any GEE-backed DAG (`update_forecast` GFS leg, `build_context_base` CHIRPS leg) | Parents per export chunk, mirroring `download_bronze_gee`. `0` = whole extent in one export. The per-source default comes from that source's probe run (§3.4), not from ERA5's 400. |
| `max_attempts` | same | EE restarts tolerated before the export is abandoned; `2` everywhere |

---

## 8. Phases

Four stages. Each phase lists deliverable, tests, and an acceptance criterion that is checkable rather than aspirational.

### Stage 1 — Foundation (no new external dependencies)

**Phase 0 — Scaffolding**
- `src/db/context_schema.sql` with all tables from §5; `src/db/context_load.py`; `build_context_base` DAG skeleton; `context_pool`.
- Extend `src/cds/manifest.py` keys to `(source, variable, chunk)` **without breaking existing ERA5 keys**.
- *Tests:* `test_context_schema.py` (DDL applies to a clean DB and is idempotent), `test_manifest.py` extended (old ERA5 keys still resolve).
- *Accept:* `docker compose up` on an existing volume creates every new table, and a re-run of `download_bronze` still skips already-done ERA5 variable-years.

**Phase 1 — `wth_normals`**
- `src/normals/compute.py`, chunked by `parent_id` batch like `transform_silver`.
- Terciles via `percentile_cont`; `n_years` counted per cell-bin.
- *Tests:* `test_normals.py` — known series → known mean/sd/terciles; partial-coverage cell reports honest `n_years`; pentad binning correct across leap years.
- *Accept:* for a cell with full 1991–2020 coverage, monthly `precip_mean` matches a direct pandas computation over `fetch_cell_series` to within float tolerance.

**Phase 2 — `climate_index`**
- ONI + Niño 3.4 fetch/parse → bronze → silver; `phase` derived by the sustained-quarters rule.
- *Tests:* `test_indices.py` — parser against a checked-in fixture of the real file format; phase classification against known 1997-98 El Niño and 2010-11 La Niña.
- *Accept:* `climate_index` reproduces the published ONI table for 1982, 1997, 2015, 2023 (the bulletin's own analog years) exactly.

### Stage 2 — Forecast core

**Phase 3 — Forecast grid**
- `src/forecast/fspec.py`, `src/forecast/fencoding.py`; the `grid/encoding.py` parameterization from §3.3.
- *Tests:* `test_fencoding.py` — round-trip `lat/lon → fcell_code → centroid`; `fcell_for_child` agrees with a direct lat/lon encode for a spread of cells including antimeridian and poles; existing `test_encoding.py` still passes unchanged.
- *Accept:* every 0.25° cell in an existing seeded extent maps to exactly one `fcell_id`, and the reverse mapping partitions the extent with no gaps or overlaps.

**Phase 4 — SEAS5 live → `forecast_value`**
- `src/forecast/seas5.py` on the existing `CDSClient`; `src/forecast/load.py` implementing the TRUNCATE-partition refresh (§5.3); `update_forecast` DAG.
- *Tests:* `test_seas5.py` (request shape: `area` used, `grid` never passed — the §11.1 rule holds here too), `test_forecast_load.py` (refresh replaces the whole horizon; a shorter new horizon leaves no stale rows; concurrent reader never sees a partial load).
- *Accept:* two successive issuances loaded in sequence leave exactly one row per `(target_start, cell, variable, member)`, all carrying the later `init_date`.

**Phase 5 — Hindcast calibration**
- `src/forecast/hindcast.py`: bulk reforecast download (~12 requests, §3.1 of the feasibility doc), then `forecast_hindcast_stats` computed against `wth_normals`.
- *Tests:* `test_hindcast.py` — bias and anomaly-correlation math against a synthetic pair with known correlation.
- *Accept:* a raw SEAS5 anomaly for a sample cell, bias-corrected via this table, lands closer to the observed value than the raw one over a held-out year. **Phase 4 output is not presented to users before this passes.**

**Phase 6 — GFS daily via GEE**
- `src/forecast/gfs.py` reusing `src/gee/` export machinery; writes `cell_id = child_id`, `cell_grid = 'era5_025'`, `member = 0`.
- Daily schedule; bronze retention 30 days.
- *Tests:* `test_gfs.py` — daily period rows (`target_end = target_start`) coexist with SEAS5 monthly rows under one PK; `lead_days` computed correctly across an init-time boundary.
- **Export budget (§3.4).** 16 bands (one init's lead days), and the per-cell local day applies, so the same tz zones as ERA5. 23× fewer bands than an ERA5 year — pressure is **low**, and at a Central America extent it likely needs no chunking at all. Confirm with one probe run; do not assume.
- *Accept:* `forecast_value` simultaneously holds SEAS5 monthly and GFS daily rows, and a single query returns a coherent "next 16 days daily, then months 1–7" series for one location — **and that export completed at `attempts=1`**.

### Stage 3 — Enrichment

**Phase 7 — ENSO plume → `enso_forecast`** (independent; can slot anywhere after Phase 0)
- *Accept:* the current issuance reproduces the bulletin's plume chart — per-model values plus `DYN AVG` / `STAT AVG`.

**Phase 8 — IBTrACS → `tc_track` + `tc_cell_exposure`**
- Historical bulk load; `src/cyclones/exposure.py` buffers tracks and intersects the grid. First real PostGIS vector work in the project.
- *Tests:* `test_cyclones.py` — a synthetic straight-line track exposes exactly the cells within the buffer; category classification at Saffir-Simpson boundaries.
- *Accept:* Hurricane Mitch (1998) exposure over Honduras returns a plausible cell set with correct dates — a case with a known answer.

**Phase 9 — CHIRPS → `wth_precip_alt`** (GEE; default 10-year window per §10)
- **Export budget (§3.4).** 365 bands per year — ERA5's order — but CHIRPS is already a daily product, so there is **no local-day reduction and no zone mosaic**: `zones = 1`. Losing the zone multiplier takes the budget ~4× further than ERA5's at the same extent. Pressure **low-moderate**; probe one year before backfilling ten.
- *Accept:* CHIRPS and ERA5 monthly precip for the same cells correlate as expected, and their disagreement over complex terrain is visible — which is the reason to have both. One export at the target extent completes at `attempts=1`.

**Phase 10 — NMME → `forecast_value`** (schema already proven by Phase 4; mostly a new fetch adapter)

### Stage 4 — Derived products

**Phase 11 — Analog-year forecasting**
- `src/analog/select.py` (ENSO phase → analog years) + `forecast.py` (per-cell, per-pentad distributions from `wth_base`).
- *Accept:* given the bulletin's stated analogs (1982, 1997, 2015, 2023), the per-pentad output for Tegucigalpa is the same shape as the bulletin's chart for that station.

**Phase 12 — Admin boundaries + drought alerts**
- GADM seed → `admin_boundary` + materialized `admin_cell_map`; SPI → `drought_alert`.
- *Accept:* municipality alert counts are reproducible and the status thresholds are documented constants, not magic numbers.

**Phase 13 — NDVI anomaly** (lowest priority; least connected to crop modeling)
- **Export budget (§3.4).** ~36 dekads per year, `zones = 1`. Pressure **negligible**. Still: `attempts=1` on the first export, because the cost of checking is one grep.

---

## 9. Configuration additions

New dataclasses in `src/config.py`, following the existing optional-at-load-time pattern so users who want none of this need no setup:

```python
@dataclass(frozen=True)
class ContextConfig:
    normals_period: str        # '1991-2020'
    iri_base_url: str
    noaa_indices_url: str
    ibtracs_url: str
    gadm_dir: Path | None
```

`PathsConfig` gains `bronze_indices_dir`, `bronze_forecast_dir`, `bronze_cyclones_dir` as properties, mirroring the existing `bronze_static_dir`.

**No new secrets.** Every new source is either public HTTP (NOAA, IRI, IBTrACS, GADM) or already-authenticated (CDS, GEE). `.env.example` gains only URLs and paths — nothing that needs git-ignoring beyond what already is. This is worth stating because it means the `.gitignore`-before-first-commit rule in `CLAUDE.md` has nothing new to catch here.

---

## 10. Storage and performance budget (Pi 5 / 2 TB SSD)

Order-of-magnitude, Central America extent (~18° × 15°). These drive real design choices, not just reassurance.

| Table | Rows | Notes |
|---|---|---|
| `wth_normals` | ~cells × 85 bins | monthly + pentad. Modest. |
| `climate_index` | ~10³ | negligible |
| `enso_forecast` | ~10³/yr | negligible; accumulates (§5.5) |
| `forecast_value` (SEAS5) | ~10⁶ | 7 leads × 51 members × ~10³ land fcells × 2 vars |
| `forecast_value` (GFS) | ~10⁷ | 16 days × land cells × ~5 vars, **replaced daily** |
| `forecast_hindcast_stats` | ~10⁵ | static |
| `tc_track` | ~10⁵ | full North Atlantic best-track history |
| `tc_cell_exposure` | ~10⁵ | storms × exposed cells |
| `wth_precip_alt` (CHIRPS) | **~10⁸–10⁹ if full history** | ⚠ see below |
| `ndvi_anomaly` | ~10⁸ | dekadal; consider same year-scoping as CHIRPS |

**Two entries deserve attention:**

1. **GFS daily churn.** ~10⁷ rows replaced every day is exactly the workload that makes `DELETE` untenable and `TRUNCATE`-on-partition necessary (§5.3). This is the single most important operational consequence of the current-only decision.
2. **CHIRPS is not a small addition.** A full 46-year CHIRPS backfill at 0.25° is the same order of magnitude as the entire existing `wth_base` — potentially ~100 GB+. That may well be worth it, but it is a deliberate decision, not a side effect of "add CHIRPS". Hence `chirps_start_year` defaulting to a 10-year window with full backfill as opt-in.

### 10.1 EECU budget — the actually binding constraint

Rows and disk are the easy part. The scarce resource is **GEE compute**: 1,000 EECU-h/month on the Contributor tier, shared with the ERA5 backfill, which has already claimed most of it — a chunked Brazil backfill alone models at ~778 EECU-h, 78% of one month.

Per-task cost fits `0.0512 + 4.33e-5 × cells` EECU-h (`docs/cost_model_climate_context.md` §9.4). The fixed term dominates for small exports, which is why band count and task count matter more than pixel count here.

| Phase | GEE? | Cadence | EECU-h per run | Notes |
|---|---|---|---|---|
| 6 GFS | yes | **daily** | measure in E2 | The only *recurring* GEE cost in this plan, and therefore the one to size carefully. 16 bands, but 365 runs a year. |
| 9 CHIRPS | yes | one-off + annual top-up | ~ERA5 var-year per year of history | 10-year default window keeps this bounded; a full 46-year backfill is an ERA5-scale commitment on its own |
| 13 NDVI | yes | one-off + dekadal | small | ~36 bands, 1 zone |
| all others | no | — | 0 | CDS, HTTP or PostGIS only |

**E2 (measure GFS daily burn) gates enabling the daily schedule.** A recurring cost that competes with the backfill has to be a measured number before it is switched on, not after.

---

## 11. Risks

| Risk | Mitigation |
|---|---|
| `grid/encoding.py` refactor (§3.3) breaks the shipped seed | Existing `test_encoding.py` must pass untouched; verify a sample of seeded `child_id`s round-trip before merging |
| SEAS5 CDS dataset name / params differ from assumption | Phase 4 starts with a single throwaway request to confirm shape before any code |
| GEE asset IDs (`NOAA/GFS0P25`, CHIRPS, NDVI) inexact | Verify in the EE catalog at phase start; all are cheap to confirm |
| Forecast anomalies presented uncalibrated | D3 + the Phase 5 gate: Phase 4 output is not user-facing until hindcast stats exist |
| `forecast_value` bloat | TRUNCATE-partition refresh (§5.3); monitor table size in the runbook |
| IRI plume is chart-oriented, may need scraping | Prefer the IRI Data Library API; if only the chart is available, treat as best-effort and mark Phase 7 optional |
| Scope creep into gold | Explicitly out of scope; the ensemble-`.WTH` idea is recorded in the feasibility doc, not here |
| A GEE export exceeds the `cells × zones × bands` budget and EE restarts it indefinitely | `max_attempts=2` on every export so the loss is bounded; `plan_chunks` refuses over-ceiling chunks offline; one probe export per new source before its first backfill (§3.4) |
| GEE EECU quota exhausted by the ERA5 backfill, starving the context layer | Schedule the ERA5 chunked backfill in year windows across months (§10.1); measure GFS daily burn with E2 before enabling its schedule |

---

## 12. Non-goals

- Gold `.WTH` materialization, forecast-conditioned or otherwise.
- Running DSSAT.
- Replacing ERA5 as the observational base — CHIRPS is an alternative precip source, not a substitute.
- A web frontend.
- Accumulating live forecast issuances in silver (D1).

---

## 13. Suggested first branch

Per `CLAUDE.md`'s plan-driven workflow: branch first, code on the branch, stop for user testing, wait for explicit authorization before commit/PR/merge.

Recommended first branch: **`context-phase0-1-normals`** — Phase 0 scaffolding plus Phase 1 `wth_normals`. It touches no external API, needs no new credentials, is fully testable offline against the existing `wth_base`, and unlocks every anomaly-based product downstream. If something in the schema design is wrong, this is the cheapest phase in which to discover it. It also touches no GEE at all, so nothing in §3.4 blocks it.

Phases 6 and 9 are the ones that do: they must not start before the chunked-export work lands on `download_bronze_gee` (`docs/plan_gee_chunked_backfill.md` Part A), or they will re-invent chunking with a different, unmeasured ceiling.
