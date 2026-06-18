# ERA5 Daily Climate Database — Implementation Plan

> A reproducible, Docker-deployed pipeline that builds a local daily climate
> database from ERA5 (0.25°) for agricultural insurance pricing and crop
> modeling (DSSAT). Build once, then update daily. Works for any region of the
> planet with a single, consistent grid-encoding scheme.

---

## 1. Purpose & Scope

Build an open-source, container-based system that lets a user (e.g. a student or
researcher) download, refine, and serve daily ERA5 climate variables for a
chosen area of interest, with outputs directly usable for DSSAT crop simulations
and downstream insurance-pricing work.

**In scope (this build):**
- Bronze ingestion of ERA5 daily variables from the Copernicus CDS.
- A deterministic global grid (`era5_land_base_grid`) whose cell codes double as
  DSSAT `.WTH` station codes.
- A silver layer in PostgreSQL + PostGIS with derived variables (RH, ET0) ready
  for query from QGIS, DBeaver, `psycopg2`, etc.
- Orchestration via Apache Airflow.
- A daily update mechanism, including ERA5T preliminary→final re-fetch.

**Out of scope (future):**
- Gold-layer `.WTH` file materialization is specified but header computation
  (TAV/AMP/ELEV) is deferred.
- A web frontend for visualizing the global grid and selecting extents.
- Running DSSAT itself; FILEX generation; LLM/GEE integrations.

---

## 2. Guiding Principles & Constraints

- **Reproducible & easy to replicate.** The whole stack comes up with
  `docker compose up`. No step should require manual, machine-specific setup
  beyond editing `.env`.
- **Build-once, update-incrementally.** The expensive 50-year backfill runs once;
  thereafter a lightweight daily DAG keeps the base current.
- **Deterministic over stored mappings.** Cell identifiers are pure functions of
  coordinates — regenerable from scratch, reversible, and consistent for any
  region without a global download.
- **Design for global, run for regional.** Partitioning and encoding are designed
  for continental/global use; a regional run is simply "fewer partitions" with
  identical SQL.
- **Target hardware:** Raspberry Pi 5 + 2 TB SSD. Favor columnar formats, bounded
  memory, and out-of-core friendly tools.
- **Single source of truth for the grid spec.** The canonical grid definition is a
  documented constant (see §6.1); the global-consistency guarantee depends on it.

---

## 3. High-Level Architecture

Medallion architecture, three layers:

```
                    ┌─────────────────────────────────────────────┐
   CDS API  ───────▶│  BRONZE  (Parquet on disk)                   │
 (ERA5 0.25°)       │   • one parquet per variable per year        │
                    │   • 2 static .nc: geopotential + ERA5-Land    │
                    │     land mask (base references)               │
                    └───────────────────┬─────────────────────────┘
                                         │  merge wide + derive (RH, ET0)
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │  SILVER  (PostgreSQL + PostGIS)              │
                    │   • era5_land_base_grid (global, geometry)   │
                    │   • wth_base (wide, partitioned by parent_id)│
                    └───────────────────┬─────────────────────────┘
                                         │  on-demand materialization
                                         ▼
                    ┌─────────────────────────────────────────────┐
                    │  GOLD  (DSSAT .WTH files)  [future header]   │
                    └─────────────────────────────────────────────┘

Orchestration: Apache Airflow
Deployment:    Docker Compose (airflow + postgres/postgis; frontend later)
```

**Consumers of silver:** QGIS (renders `era5_land_base_grid` geometry, joins
`wth_base` attributes), DBeaver, `psycopg2`, and the future frontend.

---

## 4. Technology Stack

| Concern              | Choice                                                        |
|----------------------|---------------------------------------------------------------|
| Orchestration        | Apache Airflow                                                |
| Bronze storage       | Parquet (one file per variable per year)                      |
| Silver storage       | PostgreSQL 16 + PostGIS                                        |
| CDS access           | `cdsapi` (new CDS infrastructure)                             |
| Array / NetCDF I/O   | `xarray`, `netCDF4`/`cfgrib`                                  |
| Parquet → Postgres   | `pyarrow` / `duckdb` read → `COPY` (avoid row-by-row INSERT)  |
| DB driver            | `psycopg2`                                                    |
| Numerics             | `numpy`, `pandas`                                             |
| Containerization     | Docker Compose                                                |
| Config               | `.env` (+ `.env.example` template)                            |

---

## 5. Data Source: ERA5 0.25° Single Levels

**Primary dataset:** `derived-era5-single-levels-daily-statistics` (ERA5,
0.25°, global, 1940→present). Daily aggregation is computed on retrieval (not
permanently archived), supporting `daily_mean`, `daily_maximum`,
`daily_minimum`, and `daily_sum` (the latter for accumulated variables).

We use ERA5 0.25° (not ERA5-Land 0.1°) deliberately: it provides daily
accumulated sums in the same dataset (no hourly accumulation workaround), is
~6.25× smaller (fits the Pi), and its global land-cell count fits the 4-char
code space (see §6).

### 5.1 Variable Contract

| Output (silver)     | CDS variable                          | `daily_statistic` | Source unit | Target unit (silver) | Conversion        |
|---------------------|---------------------------------------|-------------------|-------------|----------------------|-------------------|
| `tmax`              | `2m_temperature`                      | `daily_maximum`   | K           | °C                   | `− 273.15`        |
| `tmin`              | `2m_temperature`                      | `daily_minimum`   | K           | °C                   | `− 273.15`        |
| `precip`            | `total_precipitation`                 | `daily_sum`       | m           | mm                   | `× 1000`          |
| `srad`              | `surface_solar_radiation_downwards`   | `daily_sum`       | J/m²        | MJ/m²/day            | `÷ 1e6`           |
| `wind_u`            | `10m_u_component_of_wind`             | `daily_mean`      | m/s         | m/s                  | none              |
| `wind_v`            | `10m_v_component_of_wind`             | `daily_mean`      | m/s         | m/s                  | none              |
| `tdew`              | `2m_dewpoint_temperature`             | `daily_mean`      | K           | °C                   | `− 273.15`        |
| `elevation` (static)| `geopotential`                        | (single timestep) | m²/s²       | m                    | `÷ 9.80665`       |

**Derived in silver (not downloaded):**

| Output  | Derived from                          | Method                        |
|---------|---------------------------------------|-------------------------------|
| `rh`    | `tmax`, `tmin`, `tdew`                | Tetens (see §12.1)            |
| `et0`   | all of the above + `elevation` + lat  | FAO-56 Penman-Monteith (§12.2)|
| `wind`  | `wind_u`, `wind_v`                    | `sqrt(u²+v²)` (10 m, m/s)     |

`evapo` in the project = **ET0** (FAO-56 reference evapotranspiration, mm/day),
*not* ERA5 `pev`/`e`.

### 5.2 Temperature: use `2m_temperature`, NOT the forecast extremes

The dataset also exposes `maximum_2m_temperature_since_previous_post_processing`
and `minimum_..._since_previous_post_processing`. **Do not use these.**

1. They are *forecast-only* parameters carrying the IFS lower-troposphere cold
   bias, which the analyzed `2m_temperature` corrects. ECMWF explicitly
   recommends building daily extremes from the analyzed instantaneous
   `2m_temperature`.
2. As of **June 2026** there is an open data-quality issue affecting exactly
   these `*_since_previous_post_processing` daily-statistics parameters (plus
   `10m_wind_gust` and precip rate extremes). Avoid them.

→ `tmax = daily_maximum(2m_temperature)`, `tmin = daily_minimum(2m_temperature)`.

**Sanity check after download:** confirm the product is genuinely daily (one
value per cell per day); misconfigured requests have been reported to return
hourly data.

### 5.3 Day definition

Use **local day** (the extent's timezone), recorded as silver metadata. Rationale:
clean comparison against real field weather stations (which report local day) and
DSSAT's local-day expectation. The CDS daily-statistics dataset supports a
timezone shift; only UTC or zones **west of UTC** retrieve a fully sampled first
day — Brazil (UTC−3) qualifies. Apply the *same* day definition to **all**
variables to avoid mixing UTC-day precip with local-day temperature.

### 5.4 Elevation from geopotential

`geopotential` is a **static** field (orography does not change), so download a
single timestep once. It is geopotential in **m²/s²**, not meters:
`elevation_m = z / 9.80665`. It represents the model's smoothed 0.25° orography —
the correct elevation to use for ET0 (consistent with the gridded met data; do
**not** substitute a high-res DEM point value).

---

## 6. The Grid System (deterministic encoding)

### 6.1 Canonical grid spec (DOCUMENTED CONSTANT)

```
RESOLUTION = 0.25          # degrees
LON_ORIGIN = 0.0           # longitudes 0.00 .. 359.75
LAT_ORIGIN = 90.0          # latitudes 90.00 .. -90.00 (descending)
NLON       = 1440          # 360 / 0.25
NLAT       = 721           # 180 / 0.25 + 1
ALPHABET   = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"   # base-36
```

The global-consistency guarantee depends on every component using exactly this
spec. A shifted origin produces divergent codes.

**Capacity check:** full grid = 721 × 1440 = 1,038,240 cells < 36⁴ = 1,679,616.
The whole globe fits in a 4-character base-36 code with headroom; land-only at
0.25° (~300k cells) fits comfortably. (At 0.1° this would *not* fit — another
reason for 0.25°.)

### 6.2 Child code (linear index → base-36, reversible)

The `child_id` is a pure function of the cell position. It is identical to the
DSSAT `.WTH` station code (filename `XXXXYYYY.WTH`, 4-char code + 4-digit year),
giving direct traceability: a `.WTH` file decodes straight back to a coordinate
with no lookup table.

```python
ALPH = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
NLON = 1440
RES  = 0.25

# Contract: inputs are cell CENTERS (exact multiples of 0.25°). Callers that
# hold an arbitrary coordinate must snap to the nearest center first — round()
# below is banker's rounding and ties-to-even exactly on a cell boundary.
def cell_code(lat: float, lon: float) -> str:
    lon = lon % 360.0                      # normalize to 0..360
    lat_idx = round((90.0 - lat) / RES)    # 0..720
    lon_idx = round(lon / RES)             # 0..1439
    n = lat_idx * NLON + lon_idx
    s = ""
    for _ in range(4):
        n, r = divmod(n, 36)
        s = ALPH[r] + s
    return s

def code_to_latlon(code: str) -> tuple[float, float]:
    n = 0
    for c in code:
        n = n * 36 + ALPH.index(c)
    lat_idx, lon_idx = divmod(n, NLON)
    lon = lon_idx * RES
    if lon > 180.0:
        lon -= 360.0                       # back to -180..180 for storage
    return 90.0 - lat_idx * RES, lon
```

Properties: deterministic, globally unique, region-independent, reversible. Point
lookups (coordinate → `child_id`) are pure arithmetic — **no spatial query and no
geometry needed** for points; PostGIS geometry is only for polygon/region queries.

### 6.3 Parent code (regular super-block)

`parent_id` groups a `b × b` block of child cells (so the DAG cluster parameter is
expressed as **block size `b`**, i.e. `x = b²` cells per parent — keeps partitions
balanced and deterministic; avoid arbitrary proximity clustering).

```python
def parent_code(lat: float, lon: float, b: int) -> str:
    lon = lon % 360.0
    lat_idx = round((90.0 - lat) / RES)
    lon_idx = round(lon / RES)
    p_row, p_col = lat_idx // b, lon_idx // b
    n_pcols = -(-NLON // b)                 # ceil division
    n = p_row * n_pcols + p_col
    s = ""
    for _ in range(4):
        n, r = divmod(n, 36)
        s = ALPH[r] + s
    return s
```

`child_id` and `parent_id` may collide *as strings* but live in separate columns
and are never compared in the same field — not an issue. Every child belongs to
exactly one parent by construction.

**`b` is immutable for the life of the database.** Because `parent_id` is a pure
function of `child_id` and `b`, changing `b` after data is loaded would re-map
cells to different parents and break both the partition layout and the
one-row-per-cell-per-day guarantee of `wth_base` (whose PK includes `parent_id`,
see §8.2). Pick `b` once at `grid_build` time and never change it.

### 6.4 Land definition (clip 0.25° against the ERA5-Land mask)

`0.25 / 0.1 = 2.5` — **not an integer**, so the grids do not nest cleanly; a 0.25°
cell crosses ERA5-Land cell boundaries (~6.25 ERA5-Land cells overlap each 0.25°
cell, several partially). Do not assume clean nesting.

**Rule:** mark a 0.25° cell as land if a configurable fraction `X` of the
ERA5-Land land cells whose centroid falls inside the 0.25° cell footprint are
land. `X` controls coastal inclusiveness (important for coastal agriculture).

Base inputs (both single static downloads → bronze):
- ERA5 single-levels `geopotential` (one timestep) → 0.25° grid coordinates **and**
  elevation.
- ERA5-Land land mask (or any ERA5-Land variable's NaN pattern) → land reference
  for the clip.

---

## 7. Layer: Bronze

**Storage:** Parquet on disk (`/data/bronze/...`), one file per variable per year.
Maps 1:1 to the download chunking and keeps each variable independent.

```
/data/bronze/
  static/
    geopotential.nc                  # static, downloaded once
    era5_land_mask.nc                # static, downloaded once
  <variable>/
    <variable>_<year>.parquet        # e.g. tmax/tmax_1995.parquet
```

Each parquet holds all daily records of that one variable for that year, over the
downloaded extent, with columns `child_id, parent_id, date, value` (or the native
value column name). Re-fetched periods (ERA5T window) overwrite the affected
parquet(s).

---

## 8. Layer: Silver (PostgreSQL + PostGIS)

### 8.1 `era5_land_base_grid` (global, static)

Stores the **full global grid** (enables the future global viewer / extent
selection). Static and deterministic → ship as a seed (SQL dump or parquet loaded
at container init) so users do not rebuild ~300k–1M polygons.

```sql
CREATE TABLE era5_land_base_grid (
    child_id   CHAR(4)      NOT NULL,
    parent_id  CHAR(4)      NOT NULL,
    lat        DOUBLE PRECISION NOT NULL,   -- cell center, -90..90
    lon        DOUBLE PRECISION NOT NULL,   -- cell center, -180..180
    is_land    BOOLEAN      NOT NULL,
    elevation  REAL,                        -- meters (z / 9.80665)
    geom       GEOMETRY(Polygon, 4326) NOT NULL,  -- 0.25° square cell
    PRIMARY KEY (child_id)
);
CREATE INDEX idx_grid_parent ON era5_land_base_grid (parent_id);
CREATE INDEX idx_grid_geom   ON era5_land_base_grid USING GIST (geom);
```

Lookup pattern for a coordinate:
1. Compute `child_id`/`parent_id` arithmetically (no DB hit), **or** for a
   polygon/region use `geom` + GiST.
2. Query `wth_base` filtering by `parent_id` (partition pruning) then `child_id`.

### 8.2 `wth_base` (wide, partitioned)

One row per `(child_id, date)` with all variables as columns. Partitioned by
`parent_id` (scales to global; identical SQL regardless of partition count).

```sql
CREATE TABLE wth_base (
    parent_id      CHAR(4)  NOT NULL,
    child_id       CHAR(4)  NOT NULL,
    date           DATE     NOT NULL,
    tmax           REAL,            -- °C
    tmin           REAL,            -- °C
    precip         REAL,            -- mm/day
    srad           REAL,            -- MJ/m²/day
    wind           REAL,            -- m/s @ 10 m  (→ km/day @ 2 m in gold)
    tdew           REAL,            -- °C
    rh             REAL,            -- %  (Tetens)
    et0            REAL,            -- mm/day (FAO-56)
    is_preliminary BOOLEAN NOT NULL DEFAULT TRUE,  -- ERA5T vs final (provenance)
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (parent_id, child_id, date)
) PARTITION BY LIST (parent_id);
```

- **PK `(parent_id, child_id, date)`.** PostgreSQL requires every unique/PK
  constraint on a partitioned table to include all partition-key columns, so
  `parent_id` must be in the PK. This is information-lossless: `parent_id` is a
  pure function of `child_id` (given fixed `b`, §6.3), so the PK still guarantees
  exactly one observation per cell per day and powers the upsert.
- **Partition by `parent_id`**; **the loader CREATEs the partition if it does not
  already exist** (`CREATE TABLE IF NOT EXISTS ... PARTITION OF wth_base FOR
  VALUES IN ('<parent_id>')`) before `COPY` — a LIST-partitioned insert with no
  matching partition errors out. (A `DEFAULT` partition is an optional safety net
  but defeats partition pruning if rows land in it, so prefer explicit creation.)
- Within each partition, `CLUSTER` / load sorted by `child_id` so a cell's full
  time series is contiguous (single-coordinate fetch ≈ milliseconds).
- For large extents only, consider an index on `date` to accelerate the ERA5T
  re-fetch window; do **not** sub-partition for regional extents.

**Upsert (daily append + ERA5T re-fetch):**

```sql
INSERT INTO wth_base (...) VALUES (...)
ON CONFLICT (parent_id, child_id, date) DO UPDATE SET
    tmax = EXCLUDED.tmax, tmin = EXCLUDED.tmin, precip = EXCLUDED.precip,
    srad = EXCLUDED.srad, wind = EXCLUDED.wind, tdew = EXCLUDED.tdew,
    rh = EXCLUDED.rh, et0 = EXCLUDED.et0,
    is_preliminary = EXCLUDED.is_preliminary, ingested_at = now();
```

### 8.3 Derivations during bronze → silver

RH and ET0 require multiple aligned variables, so they are computed **after** the
per-variable parquets are merged wide on `(child_id, date)`. If any input variable
is missing for a cell-day, set the dependent derived value to `NULL` (graceful
degradation). `is_preliminary` always reflects the **source state** (ERA5T vs
final), never derivation completeness — a row may have `NULL` derived columns yet
be `is_preliminary = FALSE` if its inputs came from final ERA5.

### 8.4 QA / validation node

Before/after upsert, validate:
- Calendar completeness per cell per year (including leap days).
- `tmax >= tmin`, `precip >= 0`, `0 <= rh <= 100`, `srad >= 0`, `et0 >= 0`.
- Flag/quarantine rows that fail rather than propagating bad data.

---

## 9. Layer: Gold (DSSAT `.WTH`) — partial

On-demand materialization per extent/cells. Filename: `XXXXYYYY.WTH`
(`child_id` + year). Gold-only conversions:

- `WIND` → km/day: `wind_2m_kmday = sqrt(u²+v²) × 0.748 × 86.4`
  (10 m→2 m via FAO-56 factor 0.748, then m/s → km/day ×86.4).
- `SRAD` (MJ/m²/day), `TMAX`/`TMIN` (°C), `RAIN` (mm) already in target units.

**Deferred:** header fields `ELEV`, `TAV`, `AMP`, `REFHT`, `WNDHT`. `TAV`/`AMP`
are climatology-derived (long-term annual mean and monthly-mean amplitude per
cell) — a silver computation to add when the header is implemented.

---

## 10. Airflow DAGs

Four DAGs with distinct responsibilities:

### DAG 1 — `grid_build` (one-off / seed)
- Inputs: the two static `.nc` (geopotential, ERA5-Land mask), block size `b`.
- Builds the **global** `era5_land_base_grid`: computes `child_id`, `parent_id`,
  `lat`/`lon`, `is_land` (clip rule §6.4), `elevation`, `geom`.
- Idempotent; ideally distributed as a seed so it rarely runs.

### DAG 2 — `download_bronze`
- Inputs: extent (snapped to 0.25° cell vertices), `start_year`, `end_year`,
  variable list.
- Loops year-major, variable-minor: `Year1{var1..varN}, Year2{...}, ...`.
- Uses the **adaptive splitter** (§11) per `(year, variable)` request.
- Writes Parquet per variable per year. Maintains an idempotent manifest.

### DAG 3 — `transform_silver`
- Waits for a year's variables to be present in bronze.
- Merges wide on `(child_id, date)`, derives `wind`, `rh` (Tetens), `et0`
  (FAO-56), runs the QA node, and **upserts** into `wth_base`
  (`is_preliminary` set per source state).

### DAG 4 — `update`
- Daily append of the latest available day(s).
- **3-month rolling ERA5T re-fetch:** re-download the trailing ~3–4 months,
  overwrite bronze parquet, and **propagate to silver** (re-derive + upsert,
  flipping `is_preliminary → FALSE` once final). The re-fetch is not complete
  until silver reflects the correction.

**Dependencies:** `grid_build` → (`download_bronze` → `transform_silver`); `update`
runs on a schedule and internally chains download→transform for its window.

**Concurrency:** an Airflow **pool** caps simultaneous CDS requests (the CDS has
per-user queue limits and penalizes heavy/parallel use).

### DAG parameters

| Param          | DAG(s)            | Notes                                            |
|----------------|-------------------|--------------------------------------------------|
| `block_size_b` | grid_build        | parent block; `x = b²` children per parent       |
| `extent`       | download_bronze   | bbox in -180/180; snapped to 0.25° vertices      |
| `start_year`   | download_bronze   |                                                  |
| `end_year`     | download_bronze   |                                                  |
| `variables`    | download_bronze   | subset of the variable contract                  |
| `timezone`     | download/transform| local-day offset (e.g. UTC−3); stored as metadata|

---

## 11. CDS Download Strategy

### 11.1 Cost limits & gotchas

- The new CDS enforces a **cost limit** ("cost limits exceeded / request too
  large"). Empirically, 1 year × 1 variable over Brazil at 0.25° sits near the
  ceiling — so leave headroom and **do not** assume a fixed chunk generalizes to
  larger extents.
- **Never use the `grid` parameter.** Server-side regridding paradoxically
  *increases* cost (and we want native 0.25° anyway). Use `area` for subsetting
  only — which is the extent snapped to 0.25° vertices.
- The tighter cap is likely the **netCDF** format limit; if a GRIB option exists
  for this dataset it has more headroom, but the derived daily-statistics product
  may be netCDF-only — verify before relying on it.
- ECMWF advises **small requests over large/heavy ones** to avoid queue
  penalties; slightly-sub-ceiling chunks also process faster.

### 11.2 Adaptive splitter (the core download primitive)

```
submit(request):
    try:
        return cds.retrieve(request)        # cost rejection fails fast
    except Exception as e:
        # cdsapi has NO typed CostLimitExceeded/AuthError/NetworkError — every
        # failure is a generic exception. Classify by HTTP status + message text.
        if is_cost_error(e):                 # HTTP 400 + "cost"/"too large" in msg
            for sub in split(request):       # TIME first, then SPACE
                submit(sub)                  # recurse
        else:                                # auth, network, transient, unknown
            retry_with_backoff(request)      # do NOT split these

split order (time-first):
    year → semester → quarter → month → (only if a full month over the
    whole area still fails) spatial tiles

optimizations & guards:
    - cache the first working granularity for this extent and REUSE it for all
      remaining (year, variable) requests — avoid re-probing 350+ times.
    - floor: do not subdivide below ~1 day or a minimum tile.
    - max recursion depth: guard against misclassifying a non-cost error.
```

Rationale for **time-first**: temporal chunks reassemble trivially (per-variable
parquet is date-indexed → concatenate), need no tile-boundary bookkeeping, and
reducing the period shrinks the request under *any* CDS cost model (field-count
*or* volume). Spatial tiling only helps if the limit is volume-sensitive, so it is
the fallback.

### 11.3 ERA5T preliminary vs final

ERA5T (near-real-time, ~5-day latency) is preliminary and is replaced by the final
ERA5 ~2–3 months later, occasionally with revised values. The `update` DAG must
therefore re-fetch a rolling ~3-month window and overwrite (see DAG 4). Track state
with `wth_base.is_preliminary`.

### 11.4 Idempotency

A manifest records completed `(year, variable[, sub-chunk])` tuples so restarts
skip finished work — essential for a 50-year × ~7-variable (≥350 request) backfill
where transient failures and queue timeouts are certain.

---

## 12. Derivation Formulas

### 12.1 Relative humidity (Tetens)

Saturation vapor pressure (kPa), `T` in °C:

```
es(T) = 0.6108 * exp(17.27 * T / (T + 237.3))
ea     = es(tdew)                       # actual vapor pressure
tmean  = (tmax + tmin) / 2
rh     = 100 * ea / es(tmean)           # clamp to [0, 100]
```

### 12.2 Reference ET0 (FAO-56 Penman-Monteith, daily)

```
ET0 = [0.408 * Δ * (Rn − G) + γ * (900 / (Tmean + 273)) * u2 * (es − ea)]
      / [Δ + γ * (1 + 0.34 * u2)]
```

Inputs and sub-steps (implement per FAO-56, Chapter 4):
- `Tmean = (tmax + tmin)/2`; `es = (es(tmax) + es(tmin))/2`; `ea = es(tdew)`.
- `Δ` = slope of the saturation vapor pressure curve at `Tmean`.
- Pressure from elevation `z` (m): `P = 101.3 * ((293 − 0.0065*z)/293)^5.26` (kPa);
  psychrometric constant `γ = 0.000665 * P`.
- `u2` = wind at 2 m (m/s) = `sqrt(u² + v²) * 0.748` (10 m → 2 m).
- `G ≈ 0` for daily timestep.
- `Rn` = net radiation (MJ/m²/day) from `Rs` (= `srad`), extraterrestrial radiation
  `Ra` (deterministic function of latitude + day-of-year), clear-sky `Rso`, and net
  longwave `Rnl` (function of `tmax`, `tmin`, `ea`, `Rs/Rso`).
- Latitude per cell comes from `era5_land_base_grid.lat`.

**Wind note (accepted simplification):** we use `sqrt(mean(u)² + mean(v)²)` from
the daily-mean components. This underestimates true mean wind speed (Jensen gap),
but ET0 is weakly sensitive to wind, so the simplicity is preferred over a
calibration step. Documented as a known minor bias.

---

## 13. Configuration

Single `.env` (with committed `.env.example`; real `.env` git-ignored):

```dotenv
# Postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=era5
POSTGRES_USER=era5
POSTGRES_PASSWORD=change_me

# Copernicus CDS
CDS_URL=https://cds.climate.copernicus.eu/api
CDS_KEY=your_uid:your_api_key

# (future) extensible
# LLM_API_KEY=
# GEE_SERVICE_ACCOUNT=
```

The CDS credential is mounted into the Airflow container (as `~/.cdsapirc` or via
env) through Compose. `.env` is the single place a user edits to get started.

---

## 14. Proposed Repository Structure

```
era5-climate-db/
├── docker-compose.yml
├── .env.example
├── README.md
├── PLANNING.md                      # this document
├── airflow/
│   ├── dags/
│   │   ├── grid_build.py
│   │   ├── download_bronze.py
│   │   ├── transform_silver.py
│   │   └── update.py
│   └── Dockerfile
├── src/
│   ├── grid/
│   │   ├── spec.py                  # canonical grid constants (§6.1)
│   │   ├── encoding.py              # cell_code / code_to_latlon / parent_code
│   │   └── land_mask.py             # 0.25° vs ERA5-Land clip (§6.4)
│   ├── cds/
│   │   ├── client.py
│   │   ├── splitter.py              # adaptive splitter (§11.2)
│   │   └── manifest.py
│   ├── transform/
│   │   ├── merge.py                 # per-variable parquet → wide
│   │   ├── humidity.py              # Tetens
│   │   ├── et0.py                   # FAO-56
│   │   └── qa.py
│   ├── db/
│   │   ├── schema.sql               # DDL (§8)
│   │   ├── load.py                  # parquet → COPY → upsert
│   │   └── seed_grid.py
│   └── config.py
├── seeds/
│   └── era5_land_base_grid.*        # optional pre-built global grid
└── tests/
```

---

## 15. Implementation Roadmap

1. **Foundations:** repo scaffold, Docker Compose (airflow + postgis), `.env`,
   `src/grid/spec.py`, `encoding.py` + tests (round-trip `cell_code`/`code_to_latlon`,
   capacity assertion `< 36⁴`).
2. **Grid build:** static downloads (geopotential + ERA5-Land mask), land clip,
   `era5_land_base_grid` DDL + `seed_grid.py`; produce the global seed.
3. **Bronze download:** `cds/client.py`, `splitter.py`, `manifest.py`,
   `download_bronze` DAG; validate the adaptive splitter on a small extent then a
   Brazil extent.
4. **Silver transform:** `merge.py`, `humidity.py`, `et0.py`, `qa.py`,
   `wth_base` DDL + partitioning + upsert, `transform_silver` DAG; verify
   single-coordinate fetch latency.
5. **Update:** daily append + 3-month ERA5T rolling re-fetch with bronze→silver
   propagation and `is_preliminary` handling.
6. **Gold (later):** `.WTH` materialization (km/day wind), then header (TAV/AMP/ELEV).
7. **Frontend (later):** global-grid viewer + extent selection.

**Smoke test for onboarding:** provide a tiny demo extent + short year range so a
user can validate their stack end-to-end before launching a 50-year backfill.

---

## 16. Known Issues, Caveats & Future Work

- **June 2026 CDS issue** on the `*_since_previous_post_processing` daily stats —
  avoided by using `2m_temperature` (§5.2). Monitor for resolution.
- **ET0 wind bias** — accepted simplification (§12.2).
- **2.5 grid ratio** between ERA5 (0.25°) and ERA5-Land (0.1°) — handled by the
  fractional overlap clip, not clean nesting (§6.4).
- **Antimeridian / poles** — `cell_code` normalizes longitude, but an extent
  crossing ±180° needs explicit handling; rare for agricultural AOIs, deferred with
  a README note.
- **Storage budget** — Postgres + indexes ≈ ~2× the parquet footprint and shares
  the 2 TB SSD with bronze; ample for regional, verify for continental extents.
- **netCDF vs GRIB** request-cost asymmetry — confirm whether GRIB is available for
  the daily-statistics dataset (§11.1).
- **DSSAT WTH charset** — base-36 codes may lead with digits; confirm the DSSAT
  workflow accepts them (if not, force a letter first char: 26·36³ = 1.21M still
  covers the global 0.25° grid).
- **Future env-driven integrations** — LLM API, GEE service account, frontend.

---

## 17. References

- ERA5 post-processed daily statistics on single levels (CDS:
  `derived-era5-single-levels-daily-statistics`).
- ECMWF Confluence: ERA5 family post-processed daily statistics documentation;
  ERA5 2 m temperature parameter notes; accumulated-variable conventions.
- ECMWF CDS efficiency tips / "How the CDS works" (request sizing, queue behavior).
- Allen et al. (1998), *Crop evapotranspiration — Guidelines for computing crop
  water requirements*, FAO Irrigation and Drainage Paper 56.
- DSSAT weather file (`.WTH`) format and station-code conventions.
