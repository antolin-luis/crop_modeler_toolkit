# Climate Context Layer — feasibility study & next steps

**Status:** exploratory. Nothing here is built. This document exists to decide *whether* and *in what order* to build it.

**Trigger:** the CENAOS/COPECO *Perspectiva Climática Estacional Agosto–Noviembre 2026 (Honduras)* bulletin. The question asked was: can the data behind that report be pulled into bronze and modeled in silver, alongside the existing ERA5 daily base?

**Short answer:** yes for essentially all of it, and about a third of it needs no download at all — it is derivable from `wth_base` we already have. But it does not fit the current schema, and pretending it does would be a mistake. The bulletin mixes three data *shapes* the pipeline has never handled.

---

## 1. What is actually in the bulletin

Deconstructing the 28 slides by source, not by slide:

| # | Content | Underlying source | Machine-readable? |
|---|---|---|---|
| 1 | Relative SST anomaly map, weekly, tropical Pacific | NOAA CPC / OISST v2.1 | Yes — gridded netCDF, and the derived **Niño 3.4 / ONI index** is a plain time series |
| 2 | ENSO model plume, Niño 3.4 SST forecast by ~25 models | **IRI/CPC ENSO prediction plume** | Yes — IRI Data Library, monthly issuance, per-model values |
| 3 | NMME rainfall & temperature anomaly forecast maps, monthly | **NMME** (North American Multi-Model Ensemble) | Yes — IRI Data Library OPeNDAP, ~1.0° grid |
| 4 | ECMWF seasonal precip/temp anomaly, ASO 2026, System 5 | **ECMWF SEAS5** | Yes — **via the CDS API we already use** (`seasonal-monthly-single-levels`) |
| 5 | Observed monthly rainfall + % anomaly maps (May, Jun, Jul) | National station network (CENAOS et al.), interpolated | Partly — stations are not public; the same field is reconstructable from CHIRPS |
| 6 | Satellite rainfall estimate + anomaly | **CHIRPS v3.0** | Yes — in Google Earth Engine as `UCSB-CHC/CHIRPS/V3/DAILY_RNL` (v2.0 is the separate `UCSB-CHG/CHIRPS/DAILY`; note CHC vs CHG). **Built** — see [`plan_chirps_fine_grid.md`](plan_chirps_fine_grid.md) |
| 7 | Climatological normal 1991–2020 | Derived reference period | **No download needed — derivable from our own `wth_base`** |
| 8 | NDVI anomaly vs long-term average | METOP-AVHRR via FAO GIEWS | Yes, though MODIS/VIIRS NDVI in GEE is a cleaner substitute |
| 9 | Monthly forecast built from **analog years** (1965, 1972, 1982, 1997, 2015, 2023) | Methodology, not a dataset | **Reproducible from our own history + an ENSO index** |
| 10 | Pentad (5-day) rainfall forecast vs mean, ~14 stations | Same analog method, station-level | Same — derivable |
| 11 | Municipality-level meteorological drought alert (yellow/green/watch) | Derived index (SPI-like) over admin polygons | **Derivable**, needs admin boundaries as the only new input |
| 12 | 2026 Atlantic hurricane season outlook (NOAA) + ECMWF SEAS5 tropical storm frequency | NOAA CPC outlook (text/PDF), SEAS5 TC frequency | Outlook is prose; the *historical* equivalent — **IBTrACS** best-track archive — is fully machine-readable |

Two observations worth stating plainly:

- **Items 7, 9, 10, 11 are methods, not data.** CENAOS builds them from a station archive we substitute with ERA5 + CHIRPS. Our pipeline is already the expensive half of that. Reproducing the analog-year forecast and the drought alert is a *derivation* problem, not an ingestion problem.
- **Item 4 costs us almost nothing.** SEAS5 lives on the same CDS endpoint, behind the same credentials, using the same `area` subsetting discipline. It is the cheapest possible entry point into forecasting.

---

## 2. Why this does not fit the current schema

`wth_base` encodes exactly one data shape: *one value, one 0.25° cell, one local calendar day, observed.* Every architectural choice — `child_id` as a pure function of coordinates, LIST partitioning by `parent_id`, the `(parent_id, child_id, date)` PK, `is_preliminary` for the ERA5T correction — descends from that shape.

The bulletin introduces three shapes that break it:

**Shape A — scalar time series with no geometry.** ONI, Niño 3.4, SOI. One number per month (or week) for the whole planet. No cell, no grid, no partition. Trivial to store; the only mistake available is trying to force it onto the grid.

**Shape B — the forecast horizon.** A seasonal forecast is issued periodically and predicts a rolling window ahead. It carries an **initialization date** (when the model was run), a **valid/target period** (what it predicts), an ensemble axis (51 members for SEAS5, ~25 models for the ENSO plume), and a coarser native grid (~1.0°, not 0.25°).

**Decision: silver keeps only the current issuance.** When a new run lands, it replaces the whole horizon for that system — old forecast rows for a location are deleted, not accumulated. Silver answers *"what is the best current forecast for this cell?"*, one row per target period, and nothing else. `init_date` survives as a provenance attribute ("as of"), not as a key.

This is the right call, and the reason it is cheap is worth stating: **forecast skill and bias correction do not need the live archive.** They come from the model's own **hindcast** (SEAS5 reforecasts, 1993–2016, a separate CDS dataset), which is a bounded one-time download rather than an ever-growing accumulation. Calibrating raw SEAS5 anomalies into something trustworthy over Honduran terrain is a hindcast job either way — waiting years for live issuances to pile up would be the slow, worse way to get the same answer. So the accumulate-everything design buys very little and costs monotonic growth on a Pi.

The one thing genuinely given up is *ad hoc* retrospective questions about our own operational history ("what were we telling users last March?"). If that ever matters, the cheap mitigation is a thin append-only `forecast_issued_log` — issuance metadata plus a bronze file pointer, no values — since **bronze keeps every issuance regardless**. Nothing is unrecoverable; it just requires reprocessing bronze rather than a silver query.

**Shape C — vector features.** Cyclone tracks are lines with attributes evolving along them. Municipalities are polygons. Alerts are polygon-plus-status. None are point grids. PostGIS handles all of this well and we already have the extension, but the existing schema only ever uses `geom` for the grid's own cell footprints.

**Consequence:** these belong in new tables, not new columns on `wth_base`. Adding forecast columns to `wth_base` would mean either duplicating every observed row per forecast issuance, or silently keeping only one issuance. Both are bad.

---

## 3. Proposed bronze layout

Bronze is currently `/data/bronze/<variable>/<variable>_<year>.parquet`, which is keyed on the ERA5 chunking scheme. New sources have different natural keys, so they need sibling segments rather than a shoehorn into the existing one.

```
/data/bronze/
  static/
    geopotential.nc
    era5_land_mask.nc
  <variable>/                          # existing ERA5 — unchanged
    <variable>_<year>.parquet

  indices/                             # Shape A
    oni/oni.parquet                    # full history, small, rewritten each fetch
    nino34/nino34_weekly.parquet
    enso_plume/enso_plume_<YYYYMM>.parquet   # one file per IRI issuance

  forecast/                            # Shape B
    seas5/<variable>/<init_YYYYMM>.parquet
    seas5_hindcast/<variable>/<target_YYYYMM>.parquet   # 1993-2016 reforecast, static
    nmme/<variable>/<init_YYYYMM>.parquet

  cyclones/                            # Shape C
    ibtracs/ibtracs_<basin>.parquet    # full best-track archive per basin
    nhc_active/<storm_id>_<advisory>.parquet

  chirps/
    chirps_<year>.parquet              # daily precip, same cell encoding as ERA5

  admin/
    gadm_<iso3>.parquet                # municipality polygons, static
```

Three rules that should hold:

1. **Do not restructure the existing ERA5 segment.** It works, `transform_silver` reads it, and a rename is a breaking change bought for nothing. Add beside it.
2. **One file per forecast issuance, never overwritten — in bronze.** Silver keeps only the current issuance (§2), but bronze is the raw archive and stays append-only. It is cheap (one small file per month per variable), it makes the silver replace re-runnable, and it means the decision to discard old forecasts in silver is reversible. Overwriting *bronze* would make it irreversible.
3. **Store forecasts on their native grid.** Do not regrid ~1.0° NMME/SEAS5 down to 0.25° at ingest — that manufactures precision that does not exist and destroys provenance. Regridding is a *silver derivation* or a view, applied where it can be inspected and undone.

---

## 3.1 Backend choice per source — is CDS a bottleneck again?

Reasonable concern, given that the ERA5 backfill drove us to GEE. It does not carry over, and it is worth being precise about *why* rather than assuming either way.

**What made CDS painful for ERA5 was request count, not the API itself.** `src/cds/splitter.py` describes a "~350-request backfill": the cost limit forces the adaptive splitter to chop a 50-year hourly request into hundreds of chunks, each of which queues independently. Request count is driven by volume, and the volumes here are not comparable:

| Workload | Grid | Values (Central America extent, ~18° × 15°) |
|---|---|---|
| ERA5 50-yr hourly backfill, ~7 variables | 0.25° → 4,320 cells | **~1.3 × 10¹⁰** |
| SEAS5 hindcast 1993–2016, 2 variables | 1.0° → 270 cells | ~2.7 × 10⁷ (≈110 MB float32) |
| SEAS5 one live issuance, 2 variables | 1.0° → 270 cells | ~2 × 10⁵ |

Roughly **500× smaller for the one-time hindcast, and ~5 orders of magnitude smaller per live run.** A live issuance is small enough that the splitter should never engage at all — it is one request that does not trip the cost limit.

**And cadence changes the meaning of latency.** The ERA5 backfill was *blocking*: 350 queued requests all had to land before the database was usable. Seasonal forecasts are a trickle — SEAS5 issues once a month. A one-hour queue wait on a monthly job is invisible. Slow-per-request only hurts when you need thousands of them in sequence.

Request counts, concretely:
- **Live SEAS5:** ~1–2 requests/month (CDS accepts a variable list in one request).
- **Hindcast:** `year` is a list parameter, so one request per initialization month covering all 24 years ≈ **12 requests**, maybe 2–4× that if cost-split. One time, then static.

So CDS is fine here. Also worth noting how *little* of this plan touches it — of the ten sources in §1, only SEAS5 does. ONI/Niño 3.4 are plain HTTP files, the ENSO plume is IRI, IBTrACS is NOAA NCEI, CHIRPS and NDVI are GEE, GADM is a static seed.

**Where GEE does cover forecasts.** The catalog carries no SEAS5 and no full NMME suite, so the seasonal path in §4.3 stays on CDS. But two forecast collections are there and both are relevant (verify exact asset IDs at implementation):

- **`NOAA/GFS0P25`** — GFS, **0.25°**, daily resolution out to ~384 h (16 days), four runs a day.
- **`NOAA/CFSV2/FOR6H`** — CFSv2, seasonal range. Notably, CFSv2 *is* one of the NMME models, so this is a partial NMME path that needs no new backend.

The GFS entry is more interesting than it first looks: it is on **exactly our 0.25° grid**. If the §7.2 fork goes toward daily sub-seasonal forcing for DSSAT, that path needs **no coarse forecast grid and no `fcell_id` mapping at all** — `child_id` works directly, and the whole §4.3 second-grid apparatus is only required for SEAS5/NMME. That materially reduces the cost of the daily path relative to how §4.3 makes it look.

**Summary:** seasonal/monthly → CDS (small, monthly, fine). Daily/sub-seasonal → GEE via GFS, on the native grid. Everything else → neither.

---

## 4. Proposed silver tables

Sketches, not DDL. Names are provisional.

### 4.1 `climate_index` — Shape A

```
index_name    TEXT      -- 'oni', 'nino34', 'soi'
period_start  DATE
period_end    DATE
value         REAL
anomaly       REAL
source        TEXT
ingested_at   TIMESTAMPTZ
PK (index_name, period_start)
```

Small, unpartitioned, permanently useful. This is the table that lets every other analysis be conditioned on ENSO state — including the analog-year method the bulletin uses.

### 4.2 `enso_forecast` — the model plume

```
issued_on     DATE      -- IRI issuance month
model         TEXT      -- 'CMC CANSIP', 'DYN AVG', ...
model_type    TEXT      -- 'dynamical' | 'statistical'
target_season TEXT      -- 'ASO', 'SON', ...
target_start  DATE
sst_anomaly   REAL      -- °C, Niño 3.4
PK (issued_on, model, target_start)
```

Reproduces the plume chart exactly and keeps every past issuance for skill scoring.

### 4.3 `forecast_cell` + `forecast_value` — Shape B

The grid mismatch needs an explicit home. Two options:

- **(a)** A second static grid table at the forecast resolution, using the *same* encoding math from `src/grid/spec.py` with a different `RESOLUTION`, plus a deterministic `era5_child_id → forecast_cell_id` mapping (nearest-centroid, computable arithmetically — no lookup table, consistent with the existing design philosophy).
- **(b)** Store forecasts against raw lat/lon and join spatially via PostGIS at query time.

**(a) is the better fit.** It preserves the project's core principle — identifiers are pure functions of coordinates — and makes the ERA5↔forecast join a cheap arithmetic operation rather than a GiST intersection on every query.

```
-- forecast_value  (current issuance only)
system        TEXT      -- 'seas5' | 'nmme'
target_start  DATE      -- first day of the predicted period
target_end    DATE
fcell_id      CHAR(4)   -- coarse forecast grid cell
variable      TEXT      -- 'precip' | 'tmean'
member        SMALLINT  -- ensemble member; NULL for ensemble mean
value         REAL      -- absolute
anomaly       REAL      -- vs the system's own hindcast climatology
init_date     DATE      -- provenance: which run produced this row ("as of")
lead_days     SMALLINT  -- target_start - init_date; how far out this was predicted
ingested_at   TIMESTAMPTZ
PK (system, target_start, fcell_id, variable, member)
```

`init_date` is **out of the PK** — that is what makes the table current-only. `lead_days` stays because it is the honest measure of how much to trust a given row: a 5-day-out prediction and a 200-day-out prediction sit side by side in this table and must not look equally confident to a consumer.

**Refresh is a whole-horizon replace, not a row-wise upsert.** A new run does not merely revise the target periods it shares with the previous run — its horizon may start later and end later. A partial upsert leaves stale rows from the old run hanging off the front of the window. So:

```sql
BEGIN;
DELETE FROM forecast_value WHERE system = 'seas5';
COPY forecast_value FROM ...;   -- the new issuance, whole horizon
COMMIT;
```

Single transaction, so readers never see a half-loaded forecast. This is simpler and safer than `ON CONFLICT DO UPDATE`, and it is only viable *because* of the current-only decision — a nice second-order payoff.

Partition by `system` if partitioning at all; with only the current issuance resident the table is small enough that it may not need partitioning, which is itself an argument for the design. Storing all 51 SEAS5 members is what makes probabilistic statements ("60 % chance of below-normal rainfall") possible; storing only the ensemble mean permanently forecloses that, and the ensemble mean of a precipitation forecast is a famously misleading quantity. With only one issuance resident, keeping all members costs little.

### 4.3b `forecast_hindcast_stats` — calibration reference

Derived once from the SEAS5 reforecast archive, then static:

```
system, fcell_id, variable, target_month, lead_months
hindcast_mean, hindcast_sd      -- model climatology, for anomaly + bias correction
skill_score                     -- vs observed, per lead time
```

This is where forecast trustworthiness lives now that live issuances are not accumulated. It is also what converts a raw model value into a *calibrated* one before it ever reaches a DSSAT run.

### 4.4 `tc_track` and `tc_cell_exposure` — Shape C

```
-- tc_track (IBTrACS + NHC)
storm_id      TEXT      -- IBTrACS SID
season        SMALLINT
basin         TEXT
name          TEXT
obs_time      TIMESTAMPTZ
lat, lon      REAL
wind_kt       REAL
pressure_mb   REAL
category      TEXT      -- Saffir-Simpson / TD / TS
geom          GEOMETRY(Point, 4326)
PK (storm_id, obs_time)
```

Then the derived table that actually earns its keep:

```
-- tc_cell_exposure
storm_id      TEXT
parent_id, child_id      -- ERA5 grid cell
closest_approach_km  REAL
max_wind_kt          REAL
exposure_date        DATE
```

This answers the question a crop modeler or an insurer actually asks: *which of my cells were hit, when, and how hard.* It is a spatial join of `tc_track` against the grid, computed once per storm. IBTrACS gives ~170 years of this for free.

### 4.5 `wth_normals` — no download required

```
parent_id, child_id
period        TEXT      -- '1991-2020'
month         SMALLINT  -- or doy / pentad
tmax_mean, tmin_mean, precip_mean, precip_sd, et0_mean ...
```

Computed straight from `wth_base`. This unlocks the entire anomaly half of the bulletin — every "% anomaly" map is `observed / normal - 1`. Cheapest high-value item on the list by a wide margin, and it has zero external dependencies, zero new failure modes, and no API keys.

### 4.6 `admin_boundary` + `drought_alert`

GADM municipality polygons (static seed, same pattern as the grid seed), plus a derived alert table carrying an SPI-style index aggregated from cell-level precip to polygon, with a status classification. Reproduces the bulletin's final map.

---

## 5. Derived products this unlocks

Worth naming, because they are the reason to do any of this:

- **Analog-year forecasting.** Given `climate_index` (ENSO state) and `wth_base` (long history), select analog years and build a forecast distribution — exactly CENAOS's method, but per-cell and reproducible instead of per-station and manual.
- **Forecast-conditioned DSSAT runs.** The natural extension of the gold layer: instead of one `.WTH` per cell, emit an *ensemble* of `.WTH` files sampled from the seasonal forecast or the analog years, run DSSAT across them, and report a yield distribution rather than a point estimate. This is the crop-modeling payoff, and the pipeline is already most of the way there.
- **Forecast skill scoring and bias correction.** From the hindcast archive joined to `wth_base` over 1993–2016 — a bounded, one-time computation landing in `forecast_hindcast_stats`, not a wait for live issuances to pile up. A forecast nobody has scored is a rumor.
- **Parametric risk / index insurance.** `tc_cell_exposure` plus rainfall deficits over `wth_normals` is the raw material for trigger-based agricultural insurance.

---

## 6. Sequencing

Ordered by (value ÷ effort), with dependencies respected.

| Step | Deliverable | New deps | Notes |
|---|---|---|---|
| **A** | `wth_normals` from `wth_base` | none | Pure SQL/derivation. No API, no key, no new bronze. Unlocks all anomaly work. Do this first. |
| **B** | `climate_index` (ONI, Niño 3.4) + `indices/` bronze | small HTTP fetch | Tiny data, high analytical leverage. Enables analog-year selection. |
| **C** | SEAS5 into `forecast_value` | **none — existing CDS client** | Reuses credentials, `area` discipline, splitter patterns. Current-issuance-only, whole-horizon replace. |
| **C2** | SEAS5 hindcast → `forecast_hindcast_stats` | same CDS client | One-time bulk download (1993–2016), then static. Needs A for the observed side. This is what makes C's numbers trustworthy — do not ship C to users without it. |
| **D** | `enso_forecast` plume | IRI Data Library | Independent of C; can slot in anywhere. |
| **E** | IBTrACS → `tc_track` + `tc_cell_exposure` | IBTrACS CSV | Self-contained. First real PostGIS vector work in the project. |
| **F** | CHIRPS via existing GEE backend | **none — GEE already wired** | Second precip source; valuable because ERA5 precip is weak over tropical complex terrain, which is exactly Honduras. |
| **G** | NMME | IRI OPeNDAP | Only after C, since it reuses the same forecast schema. |
| **H** | Analog-year / pentad forecast derivation | needs A + B | The bulletin's actual method. |
| **I** | `admin_boundary` + `drought_alert` | GADM seed | Needs A. Presentation layer over derived indices. |
| **J** | NDVI anomaly | GEE | Lowest priority; least connected to DSSAT. |

**Recommended first slice: A → C → C2.** A is free and unblocks everything downstream; C proves the forecast schema against a real dataset using infrastructure that already exists and is already authenticated; C2 is what turns C from a raw model dump into a calibrated product. Once that trio holds, everything else in Shape B is mechanical.

---

## 7. Open questions

1. **Does `.WTH` gold output stay observation-only, or does it become forecast-aware?** This decides whether the forecast layer is an analytical side-car or a first-class input to DSSAT. It changes the gold spec materially.
2. **Forecast horizon and time granularity — the biggest remaining fork.** "Next 30 days" and "SEAS5" are not the same product:
   - **SEAS5 seasonal** issues monthly and predicts **monthly means** out ~7 months. Good for "will this season be dry", useless as direct DSSAT input — DSSAT needs daily values.
   - **Short-range / sub-seasonal** gives **daily** resolution — GFS out to ~16 days via GEE, ECMWF extended/S2S out to ~46 days. This matches "forecast of 30 days" literally and is what a `.WTH` file could actually consume.
   - The two can coexist in `forecast_value` if `target_start`/`target_end` carry the period length, which the schema above already allows — a daily row is just a one-day period.

   Which one is built first depends on whether the goal is seasonal outlook (monthly, matches the bulletin) or DSSAT-ready daily forcing (sub-seasonal). **Worth deciding before C starts**, since it sets the backend, download volume, and cadence. The bulletin itself is monthly, so C as scoped follows SEAS5 on CDS. Note per §3.1 that the daily path via GFS is cheaper than §4.3 implies — it lands on our native 0.25° grid and skips the coarse-grid mapping entirely.
3. **Ensemble members: store all, or store quantiles?** Recommendation above is all members for SEAS5, on the grounds that quantiles are recoverable from members and members are not recoverable from quantiles. With only the current issuance resident this is now clearly affordable — but worth confirming against actual disk figures on the Pi's 2 TB SSD if the daily sub-seasonal path is taken instead, since that multiplies row count by ~30.
3. **Forecast grid resolution as a constant.** If option (a) in §4.3 is taken, the forecast `RESOLUTION` becomes as immutable as the ERA5 one — same "never change this" contract as `src/grid/spec.py`. SEAS5 and NMME are both ~1.0° but not identically gridded; one shared coarse grid or one per system needs deciding before any data lands.
4. **Extent.** Everything above assumes the Central America / LatAm extent already in use. Global ENSO indices are global regardless; forecasts and cyclone tracks should follow the same extent policy as ERA5.
5. **Update cadence.** SEAS5 and NMME issue monthly; IBTrACS updates irregularly; NHC advisories run every 6 h during an active storm. Whether the `update` DAG absorbs these or a separate `update_context` DAG owns them is a scheduling question, not a modeling one — but the 6-hourly case argues for separation.

---

## 8. Bottom line

Feasible, and less invasive than it looks. Roughly a third of the bulletin needs no new ingestion at all — it falls out of `wth_base` once normals exist.

The genuinely new engineering is small: **a forecast table that holds only the current issuance, refreshed by whole-horizon replace, with bronze keeping the raw archive behind it.** Trust in those numbers comes from the hindcast (a bounded one-time download), not from accumulating live runs — which is why the current-only design costs almost nothing and keeps silver from growing without bound on a Pi.

Get that right against SEAS5 first, using the CDS client already in the repo, and the rest of the bulletin follows as variations on patterns the pipeline already has. One decision to make before starting: monthly seasonal (matches the bulletin) or daily sub-seasonal (feeds DSSAT directly) — §7.2.
