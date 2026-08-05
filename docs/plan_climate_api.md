# Implementation Plan — Public Climate Data API (Honduras)

**Status:** plan only. No API code written. Extends `PLANNING.md` roadmap steps 6–7
(gold `.WTH`, frontend) and depends on [`plan_climate_context_layer.md`](plan_climate_context_layer.md)
for every forecast, hazard and bulletin endpoint.

**Scope:** turn the silver layer into a commercial REST API for **Honduras** — 60
endpoints, API keys with rate limits, async exports, and automated PDF reports —
hosted in the cloud. Includes the pipeline work that must land first, the cloud
migration, and a costed estimate in dev-days and dollars.

**Companion:** the executive one-pagers (`Honduras_Climate_API_Onepager*.docx` at the
repo root — English, Spanish and Brazilian Portuguese) are the TLDR of Part 1. This
document is the full plan. Regenerate all three with
`uv run --with python-docx python scripts/make_onepager.py`.

**Reference artifacts:**
- `Perspectiva Ago_Nov 2026.pdf` (repo root) — the CENAOS/COPECO seasonal bulletin the
  full report product reproduces.
- `~/code/mr/ClimateReportDataclimate` — a **working** branded monthly agroclimatic
  report generator (MCP server + Claude agent + CLI) that already reads `wth_base`.
  It supplies most of §(C); see the note below.

> **Revised 2026-08-05.** The first version of this estimate assumed no report code
> existed, because the check looked only at *configured* MCP servers rather than the
> filesystem. `ClimateReportDataclimate` already ships the map rendering, the
> WeasyPrint PDF composition, the en/es/pt i18n and the SPI/GDD/water-balance logic.
> Total drops **167 → ~156 dev-days**, and the first PDF report moves from day 79 to
> **available today**. Sections marked ⬛ were rewritten; the rest stands.
>
> Also corrected: the blanket "enforce `parent_id` pruning" rule, which measurement
> contradicted — it is 202× faster for point lookups and 2.5× *slower* for wide
> aggregates.

**Measured against the live stack on 2026-08-05**, not estimated: Honduras cardinality,
partition counts, JIT cost, pruning behaviour, report runtime. Every figure in the
Verification section is reproducible.

---

# PART 1 — EXECUTIVE SUMMARY

*Written for a reader who has never seen this codebase.*

## What exists today

A working data factory. Every day since **1950**, for **283 grid squares covering
all of Honduras** (each about 25 km across), it has assembled eight weather
measurements: maximum and minimum temperature, rainfall, solar radiation, wind,
dew point, humidity, and reference evapotranspiration (a standard measure of how
much water crops lose).

That is roughly **8 million daily records**, already downloaded and stored. The raw
download for Honduras is **finished** — 539 files, 509 MB, sitting on disk.

**Total spent to acquire it: under $0.10.** Not a typo. The satellite/reanalysis
data is free from European and American public agencies; the only real cost was
bandwidth.

## What does not exist

**Any way for a customer to reach it.** Today the only access is a database
password on a home network. There is no website, no login, no service, nothing a
paying customer could use. That is the entire gap this project would close.

**The monthly PDF report generator, by contrast, already exists and works.**
`~/code/mr/ClimateReportDataclimate` — 1,740 lines, branded, English/Spanish/
Portuguese, eight pages of rainfall and temperature versus normal, drought index,
growing-degree days, water balance and per-cell country maps. It reads the same
database directly and regenerates in 37 seconds. It ships as an MCP server, a
standalone Claude agent, and a plain CLI.

Still missing: seasonal forecasts (what the next 4 months look like), which are what
the *full* government-style quarterly bulletin needs on top of the observed half.

## What would be built

A commercial web service. A customer sends a location — a farm's coordinates, a
municipality, or the whole country — plus what they want to know. They get back
either data or a finished document. **60 distinct services**, including:

- Daily weather history for any point, going back to 1950
- What is "normal" for that place, and how far the current season is from it
- Drought status, growing-degree-days, crop water balance
- Hurricane history and exposure for a specific location
- Seasonal forecasts with an honest measure of how reliable they are
- Ready-to-use input files for DSSAT, the standard crop-simulation software
- **Automated PDF reports**: the monthly agroclimatic bulletin (already built), a
  two-page per-farm summary, and later the full seasonal bulletin

Access is controlled by API keys with usage limits — the same model Stripe or
OpenAI use. No payment processing is included in this estimate, but the design
leaves the door open so it can be added later without rebuilding anything.

## Time

One developer, working with an AI assistant. The pace is not guessed — it is
measured from this project's own history: 30 commits over 11 working days produced
8,700 lines of tested code.

| | Working days |
|---|---|
| Realistic estimate (P50) | **156** |
| Conservative (80% confidence) | **184** |
| If nearly everything goes wrong | 263 |

| Working pattern | Time to full completion |
|---|---|
| **Full-time**, 6 h/day × 5 days | **~7 months** |
| 3 h/day × 5 days | ~14 months |
| **The pace actually observed on this project** (~2 h/day) | **~1.8 years** |

**That last row is the single most important number in this document.** The
difference between 7 months and nearly two years is not technical difficulty — it is
how many hours per week get spent on it.

### You do not have to wait for all of it

The work is ordered so revenue can start early:

Dates assume a **1 Sep 2026 start**, full-time, 5 working days/week, no holidays.

| Milestone | Working days (cum.) | Date | What a customer can buy |
|---|---|---|---|
| **Monthly PDF report** | **0** | **AVAILABLE NOW** | The branded agroclimatic bulletin, as a subscription |
| **First live service** | 20 | 29 Sep 2026 | One working endpoint, secured, on the internet |
| **Sellable product** | **60** | **24 Nov 2026** | Full weather history, normals, anomalies, drought indicators, crop files, bulk downloads |
| **Reports behind the API** | 68 | 5 Dec 2026 | Self-serve per-farm and per-department reports on demand |
| **Hazard & drought** | 82 | 24 Dec 2026 | Hurricane exposure, municipal drought alerts |
| **Forecasts** | 103 | 23 Jan 2027 | Seasonal outlooks (*if they prove accurate — see risks*) |
| **Full seasonal bulletin (P50)** | 156 | **8 Apr 2027** | The complete government-style quarterly PDF |
| Conservative finish | 184 (P80) | 16 May 2027 | — |

At the pace actually observed on this project (~1.7 days/week), every date above
stretches roughly 3×, putting full completion in early 2028. **Pace, not difficulty,
is the variable.**

**The 62-day milestone is the important one.** It is a genuinely sellable product
for agronomists, crop modelers, and agricultural insurers — and it depends on
nothing that can be legally blocked or scientifically fail.

## Money

### To run it, per month

| Scale | Monthly cost |
|---|---|
| Development / a few users | **$18** |
| A real business (10,000 requests/day) | **$26** |
| Heavy use (1,000,000 requests/day) | **$51** |

These are real prices from a European cloud provider (Hetzner). Using Amazon Web
Services instead costs roughly 4–6× more for identical capability. Honduras is a
small country in data terms — about 2.3 GB total — which is why hosting is
genuinely cheap.

**Ongoing data costs: effectively zero.** Around $0.05/month. All the source data
is free.

**Hosting is not a constraint on this project.** At $18–51/month, it is cheaper
than a phone bill.

### To build it

The cost is your time. For scale, if you were hiring someone:

| Day rate | 156 days (realistic) | 184 days (conservative) |
|---|---|---|
| $300/day | $47,000 | $55,000 |
| $500/day | $78,000 | $92,000 |
| $800/day | $125,000 | $147,000 |

Plus **$2,000–5,000 for a lawyer** — see below. That is not optional.

## The two things that could stop this

Both are legal, not technical. Both are cheap to fix now and expensive to fix later.

**1. The fast data pipeline may not be licensed for commercial use.** The tool that
downloads the weather data quickly (Google Earth Engine) is being used on its free
tier, which is for non-commercial use. Selling access to a service built on it is
probably not allowed. There are three ways out — pay Google (roughly $200–400
one-off for Honduras, which is affordable), switch to the slower European source
(free, commercially permitted, already built and working), or use a third free
public source. **None of these kills the project.** But it needs answering before
money changes hands.

**2. The map boundaries are non-commercial.** The planned source for Honduras's
department and municipality outlines (GADM) explicitly forbids commercial
redistribution. The fix is trivial *right now* — swap to geoBoundaries, which is
free for commercial use — and painful once the code is written around GADM.
**Change this decision today; it costs nothing.**

There is also a scientific risk worth naming plainly: **seasonal forecasts may
simply not be accurate over Honduras's mountainous terrain.** The plan already
requires measuring the accuracy before showing any forecast to a customer. If it
turns out to be poor, the forecast features stay switched off and the rest of the
product is unaffected — which is exactly why forecasts are scheduled late.

## Bottom line

- The expensive, slow part — acquiring 76 years of daily weather for Honduras — is
  **already done**, and cost under $0.10.
- The monthly PDF report **already works** — that product is sellable today.
- What remains is software: roughly **7.2 months full-time**, or **~1.8 years at the
  current pace**.
- A **sellable API exists at 3 months full-time**.
- Running it costs **$18–51/month**. Money is not the obstacle.
- Two legal questions need answers before charging customers. Both are solvable.
  Resolve them now, while they are free to resolve.

---
---

# PART 2 — TECHNICAL PLAN

## Context

The pipeline currently ends at a Postgres table. Silver is live and queryable, but
the only read path is `import src.db.query` or psql on the LAN. There is no network
service, no auth, no packaging as a product.

Goal: a public SaaS REST API scoped to **Honduras**. A caller sends coordinates plus
a service type — series, summary, anomaly, DSSAT file, or full report — and gets it
over HTTPS behind an API key and a rate limit. The reference deliverable for
"report" is the CENAOS/COPECO *Perspectiva Climática Estacional* bulletin in the
repo root: ENSO plume, SST anomaly map, NMME/ECMWF seasonal anomaly maps, per-month
observed rainfall and anomaly maps by department.

Locked decisions: full scope including the climate context layer; public SaaS in the
cloud; reports = seasonal bulletin PDF + per-farm/per-admin summaries + a single
coordinates-in/service-out façade; API keys with rate limits and usage logging, no
billing yet.

## Honduras — measured cardinality (live, 2026-08-05)

```
extent [12.9, -89.4, 16.6, -83.1]
  land cells            283
  cells in bbox         375
  parent partitions      28
  timezone zones          1        (UTC-6, single zone)
  export ceiling check  283 × 1 = 283  vs  CELL_ZONE_CEILING 58,554   →  0.5% of ceiling
```

**Bronze is already complete**: 539 parquet files (7 variables × 77 years,
1950–2026), 509 MB in `.localdata/hn/`. Measured acquisition cost: **17.41 EECU-h**
(1.7% of one month's free quota), **364 MB egress**, **$0.044**.

Silver currently holds Uruguay, not Honduras — loading Honduras is a
`transform_silver` run over bronze that already exists, not a download.

## What Honduras scale eliminates

This is the substantive difference from a continental build, and it removes most of
the *risk*, not most of the *work*:

| Continental problem | At Honduras scale |
|---|---|
| LatAm backfill = 487–966 EECU-h, 49–97% of monthly quota, **≥2-month calendar floor** | **Already spent: 17.41 EECU-h. Floor gone.** |
| 2,133 partitions → 38 MB relcache/backend; 24,770 global → 445 MB/backend | **28 partitions → ~0.5 MB/backend. Non-issue.** |
| `b=4`→`b=16` re-partition, with a hard deadline before the LatAm load | **Unnecessary.** Revisit only on scale-out. |
| Lock exhaustion: 2,133 locks/query vs 6,400 slots, 3 concurrent queries kills it | **28 locks/query. Non-issue.** |
| Bulk export = 510 M rows / 6 GB → mandatory DuckDB path | **8 M rows / ~100 MB. Postgres serves it directly**; DuckDB optional |
| Area queries could take the DB down; need hard sync/async ceilings | **The whole country is 283 cells.** Every area query is small |
| Materialized rollups mandatory to render a bulletin | **Nice-to-have.** 8 M rows aggregates fast enough |
| CHIRPS +56 GB, NDVI +5.5 GB | **CHIRPS +0.88 GB, NDVI +52 MB** |
| DB 68–130 GB | **DB ~2.3 GB** |

**What does not shrink:** the API code, the report renderer, the SaaS plumbing, the
cloud migration, and CI. Those are geography-independent. Scoping to Honduras saves
~25 dev-days of 192 (13%) — but removes both hard calendar floors and nearly every
scaling risk.

## Measured findings that still apply

| Finding | Measured today |
|---|---|
| **`jit=on` is harmful on this partition layout** | Unpruned date query: **8,665 ms** with jit on vs **1,550 ms** off (5.6×). On a cold JIT cache: ~30 s vs 1.7 s (18×; 20.9 s of LLVM compiling 19,916 functions). One config line. |
| Postgres is entirely untuned | `shared_buffers=128MB`, `work_mem=4MB`, `jit=on`, `max_locks_per_transaction=64` — all stock |
| "18 ms planning per query" was a misread | 11.96 ms is a *cold-session* cost; warm session plans the same query in **0.109 ms**. Connection pooling fixes it for free — 110×. |
| `grid_query.py`'s seq-scan-beats-GiST claim | True for its multi-chunk use case, **13.7× wrong for a single API bbox**: GiST 4.9 ms vs seq scan 67 ms. The API must use GiST. |
| Airflow shares the serving database | 1,714 tables in one `public` schema. Must be split for a SaaS. |
| 227 tests, **none touch Postgres**; no `.github/` at all | Every endpoint is a SQL correctness question the current suite cannot see |

---

## Endpoint inventory (60)

Legend: ✅ table exists · 🟡 planned context layer, unbuilt · 🔴 new to this design

| Group | Endpoints | # | Data |
|---|---|---|---|
| **Grid & metadata** | `/v1/grid/locate`, `/grid/cells/{id}`, `/grid/cells?bbox`, `/grid/coverage`, `/meta/variables`, `/meta/sources`, `/status` | 7 | ✅ + 🔴 |
| **Observed point** | `/point/series`, `/series/{monthly\|pentad\|annual}`, `/point/summary`, `/point/wth`, `/point/gdd`, `/point/water-balance` | 6 | ✅ (+🟡 for summary, wth) |
| **Area / aggregate** | `POST /area/series`, `/admin/{id}/series/monthly`, `POST /area/stats`, `/area/raster`, `/tiles/{var}/{z}/{x}/{y}.png` | 5 | ✅ + 🔴 |
| **Normals & anomalies** | `/point/normals`, `/point/anomaly`, `/admin/{id}/anomaly`, `/point/analogs` | 4 | 🟡 `wth_normals` |
| **Forecast** | `/forecast/seasonal`, `/daily`, `/members`, `POST /wth-ensemble`, `/forecast/skill`, `/area/forecast/map` | 6 | 🟡 — **all gated on Phase 5** |
| **Hazard / ENSO / drought** | `/enso/index`, `/plume`, `/current`, `/hazard/cyclones`, `/active`, `/point/cyclone-exposure`, `/drought/alerts`, `/point/spi`, `/ndvi-anomaly`, `/precip/compare` | 10 | 🟡 |
| **Admin** | `/admin/search`, `/{id}`, `/lookup`, `/{id}/cells` | 4 | 🟡 |
| **Bulk exports** | `POST /exports` + status / download / cancel / list | 5 | 🔴 `job` |
| **Reports** | `POST /reports` (bulletin \| farm \| admin), status, assets, templates, `/bulletins/latest` | 7 | 🔴 + 🟡 |
| **Façade** | `POST /v1/query`, `GET /v1/query/services` | 2 | — |
| **Account / admin plane** | `/account`, keys CRUD, `/usage`, `/limits`, `/internal/*` | 5 | 🔴 |

26 tables needed: **3 exist**, 12 are the planned context layer, 11 are new
(tenancy, jobs, rollups, freshness).

### The façade — the stated interaction model

```jsonc
POST /v1/query
{
  "location": { "lat": 14.10, "lon": -87.22 },   // | {"admin_id":…} | {"bbox":…} | {"child_id":…}
  "service":  "summary",                          // 28-entry catalogue
  "params":   { "start": "2020-01-01", "end": "2026-07-18", "vars": ["precip","et0"], "lang": "es" },
  "options":  { "async": "auto", "format": "json" }
}
```

`200` → `{resolved_location, data, provenance, warnings, credits_used}` for sync
services; `202 {job_id, poll_url, estimated_seconds, credits_reserved}` for reports
and exports.

`GET /v1/query/services` returns the machine-readable catalogue — per-service param
schema, credit cost, and `available: true|false` **with a reason**. Generate it from
the Pydantic models so it cannot rot. This is what lets forecast services ship dark.

**Router rule:** until `forecast_hindcast_stats` is populated and validated, all six
forecast endpoints return `503 {"error":"forecast_uncalibrated"}`. This is decision
D3 in the context plan, and it is the difference between a defensible product and a
liability.

---

## Architecture

**FastAPI + psycopg3 async pool + Redis/Valkey + ARQ workers + WeasyPrint for PDF.**
The codebase is Python 3.12 with numpy/pandas; another language forks `encoding.py`,
`et0.py`, `humidity.py` or forces an RPC hop. Pydantic v2 models the façade's
discriminated-union body natively, and `/point/summary` fans out to 6–8 independent
queries — `asyncio.gather` versus 6–8× the latency.

Keep `src/` as a shared library; the API imports it. Do not fork the domain logic.

### Database work (much smaller than at continental scale)

| Action | Cost | Effect |
|---|---|---|
| `jit_above_cost = -1` | **5 min** | **5.6–18× measured** on wide scans |
| `max_locks_per_transaction = 512`, `shared_buffers` 25% RAM, `work_mem` 32–64 MB, `random_page_cost = 1.1` | 1 h | Removes every stock-default ceiling |
| Connection pool with `max_lifetime` | in B1/B2 | Cold planning 11.96 ms → 0.109 ms |
| ⬛ **Shape-dependent** `parent_id` handling — see the measurement below | in B2 | Point queries 202× faster *with* it; wide aggregates 2.5× faster *without* |
| `mv_wth_monthly`, `mv_admin_monthly`, incrementally refreshed after `transform_silver` | A17 (**1.5 d, was 3**) | Nice-to-have at 8 M rows; keeps the bulletin renderer fast |
| **`b=4`→`b=16` re-partition** | **Skip** | Unnecessary at 28 partitions. Reconsider only if scaling past ~1,500 partitions. |
| DuckDB-over-Parquet bulk path | **Optional** | Postgres serves a full-country full-history export (8 M rows, ~100 MB) directly |

⬛ **Correction — `parent_id` pruning is not a blanket rule.** Measured on the live
DB, the right choice depends on query shape:

| Shape | No `parent_id` | With `parent_id` | |
|---|---|---|---|
| Point lookup, 1 cell | 1,916 ms | **9.5 ms** | **202× faster** — the PK leads with `parent_id` |
| Wide aggregate, 311 cells × 31 yr | **4.2 s** | 10.7 s | **2.5× slower** — pruning adds a filter and buys nothing |

The repository layer therefore needs **two paths**, not one rule: always prune point
and few-cell reads; never prune wide aggregates. Getting this backwards is what makes
`ClimateReportDataclimate` slow today, and it is why `jit=off` matters so much — see
the JIT row above.

### Other structural calls

- **Split Airflow's metadata DB from the serving DB.** `docker-compose.yml:14` points
  Airflow at the same `era5` database as silver. For a SaaS that is migration locks
  on the customer-facing DB, coupled backup/restore, connection contention, and no
  least-privilege. Cleanest path: fresh Airflow metadata DB + `airflow db migrate`,
  accepting loss of historical task runs. Move silver to a `climate` schema, app
  tables to `app`, give the API an `api_ro` role that is not superuser.
- **The Pi leaves the request path.** Three unexplained hard freezes in two days;
  swap still 199 MiB against a documented 2 GB floor; no resource limits in compose.
  Keep it as dev box and secondary ingest runner with bronze in object storage. At
  Honduras scale the ingest load is light enough that the Pi can stay the ingest
  host — the freeze risk no longer blocks a multi-week backfill, because there is no
  multi-week backfill.
- **Caching, three tiers:** CDN for tiles/rasters/PDFs; Redis for hot JSON keyed
  `v1:{service}:{child_id}:{param_hash}` with **event-driven invalidation** (DAGs POST
  `/internal/cache/invalidate`; TTL is backstop only); HTTP ETag from `ingested_at`.
  Observed series are near-immutable **except** the ERA5T 3-month window:
  `max-age=86400, immutable` for ranges ending before `now() - preliminary_months`,
  `max-age=3600` when overlapping it. Stale-after-ERA5T-correction is a correctness
  bug, not a perf bug.
- **Jobs: Redis + ARQ.** Not Celery (heavy, poor async), and **not Airflow** — coupling
  customer requests to the DAG scheduler makes report latency depend on backfill
  load. `job` table is source of truth, Redis is transport. Separate queues for
  `exports`, `reports`, `maintenance`. Idempotency via `request_hash` — report
  requests repeat heavily, and that alone is most of the saving.
- **Rendering runs in the reports worker, never the API process.** matplotlib +
  cartopy + geopandas for maps, matplotlib for the plume, Jinja2 → WeasyPrint for
  composition (the same template yields `format=html` free; CSS handles two-up map
  grids; i18n is template-level). One `mapspec` module owns colormaps, anomaly bins,
  projection, and the Honduras extent. ~20 figures × 1–3 s ≈ 30–60 s per bulletin —
  **pre-bake on a cron, serve `/bulletins/latest` from cache**, on-demand is the
  exception.
- **API keys:** `ck_live_<24 base62>`, 6-char public prefix stored clear for lookup,
  **SHA-256 + server-side pepper** of the full key stored. A 143-bit random token is
  not a password; Argon2 per request costs ~50 ms for no security gain. Prefix →
  Redis cache of `(hash, tenant, plan, limits)` keeps auth off the DB hot path.
- **Rate limiting:** Redis token bucket via Lua. Per-key RPS, daily count, and monthly
  **credits**. Credits are the hook that makes billing addable later — adding Stripe
  becomes a webhook plus a `subscription` table, with no change to the request path.
- **Usage logging must be batched** — buffer `usage_event` in-process, flush in
  batches; a per-request INSERT doubles write load. Nightly rollup, 90-day retention.
- **Observability:** structlog JSON with `request_id`/`tenant_id`/`db_ms`,
  prometheus-fastapi-instrumentator → Grafana, Sentry, `pg_stat_statements` (currently
  off), OTel sampled on fan-out endpoints. Plus **data-freshness SLIs on the same
  dashboard as p95** — for a data product, stale data *is* an outage. Surface on
  `/v1/status`.

---

## Cloud cost — Honduras

Assumptions: published list prices, no committed-use discounts, USD. Price knowledge
is May-2026 vintage — **re-verify before committing**. 130 B/row measured. Average
response 20 KB gzipped; bulletin PDF 5.9 MB (measured). Traffic tiers: dev <500/day,
10k/day ≈ 1 req/s peak, 1M/day ≈ 50 req/s peak.

**Storage (arithmetic):**

```
wth_base       283 cells × 28,126 days (1950–2026) = 7.96 M rows × 130 B =  1.03 GB
grid seed      global shipped artifact, 1,038,240 rows                   =  0.27 GB
wth_normals    283 × 85 bins × 120 B                                     =  0.003 GB
forecast SEAS5 ~28 fcells × 7 leads × 51 members × 4 vars = 40 k rows    =  0.005 GB
forecast GFS   283 × 16 d × 5 vars = 23 k rows, replaced daily           =  0.003 GB
hindcast_stats 28 × 4 × 12 × 7 = 9.4 k rows                              =  ~0
tc_track + exposure (Atlantic basin, kept global)                        =  0.03 GB
admin (GADM/geoBoundaries L0/L1/L2: 1 + 18 + 298 = 317 polygons)         =  0.02 GB
CHIRPS 77 yr   7.96 M rows × 110 B                                       =  0.88 GB
NDVI   46 yr   283 × 36 × 46 = 469 k rows × 110 B                        =  0.05 GB
                                                                 TOTAL ≈   2.3 GB
partitions ≈ 28 + context
```

Provision 40 GB and never think about it again.

**Egress:** 10k req/day × 20 KB = 6 GB/mo. 1M req/day = 600 GB/mo, plus ~6 GB of
bulletin PDFs and ~20 GB of CDN-miss tiles. All within a Hetzner server's included
20 TB.

**Monthly run-rate:**

| Provider | dev | 10k req/day | 1M req/day |
|---|---|---|---|
| **Hetzner** (self-managed PG) | CAX21 all-in-one $8 + backup $4 + object storage $6 = **$18** | CAX31 $16 + $4 + $6 = **$26** | DB CCX23 $27 + API CAX21 $8 + LB $6 + $4 + $6 = **$51** |
| **DigitalOcean** (managed PG) | PG 10 GB $15 + app $12 + Spaces $5 = **$32** | PG 2/4/38 $60 + app $12 + $5 = **$77** | $60 + 2 app $24 + Valkey $15 + LB $12 + $5 = **$116** |
| **AWS** (RDS + Fargate + CloudFront) | db.t4g.micro $12 + 20 GB $3 + Fargate $18 + ALB $17 + S3 $1 = **$51** | db.t4g.small $24 + Fargate $18 + ALB $17 + $4 = **$63** | db.t4g.large $94 + 2 Fargate $72 + ALB $27 + cache $40 + CF $53 + S3 $2 = **$288** |

**Recommendation: Hetzner.** TLS via Caddy + Let's Encrypt ($0), CDN via Cloudflare
free tier ($0), backups via pgBackRest → Storage Box ($4/TB). The price of
self-managing is that you own the restore drill — which is why E5 is a real line
item. Host in `us-east` (~50–70 ms to Tegucigalpa); `sa-east-1`/São Paulo runs
1.4–1.9× these figures for no latency win worth paying for.

**Ingest costs:** Honduras bronze is already acquired — 17.41 EECU-h, 364 MB,
**$0.044, spent**. Ongoing: GFS daily at 283 cells is negligible (run the E2 probe
to confirm, ~10 min). CDS, NOAA, IRI, IBTrACS free. **~$0.05/month.**

---

## Effort — Honduras

Velocity calibrated from this repo's own history, not a textbook rate: **30 commits
over 11 active days** (2026-06-17 → 07-31, 45 elapsed) producing 8,702 lines of
Python + 227 tests + ~5,000 lines of design docs across 8 substantial feature units
— **~1.4 active days per ~600–900 LOC unit with tests and docs**. That already
reflects working with an AI assistant; it is not discounted twice. A dev-day = 6
focused hours. Ranges are low / **likely** / high; PERT `E = (low + 4×likely + high)/6`.

### (A) Pipeline prerequisites

| ID | Work | low | **likely** | high | Δ vs continental | Top risk |
|---|---|---|---|---|---|---|
| A0 | **Step 5 `update` DAG** — daily append + 3-month ERA5T re-fetch + `is_preliminary` flip | 3 | **4** | 7 | — | A partial implementation silently leaves preliminary rows forever |
| A1 | **Load Honduras to silver** — `transform_silver` over existing bronze | 0.3 | **0.5** | 1 | **3 → 0.5; ≥2-month calendar floor eliminated** | Manifest key has no region component (known runbook gap); use `data_root` |
| A2 | Phase 0 — `context_schema.sql`, `context_load.py`, manifest key generalization, `context_pool` | 2 | **3** | 5 | — | Must not break existing ERA5 keys or the `flock` test |
| A3 | Phase 1 — `wth_normals` (monthly + pentad, `percentile_cont` terciles) | 2 | **2.5** | 4 | 3 → 2.5 | Pentad binning across leap years |
| A4 | Phase 2 — `climate_index` (ONI / Niño 3.4 / SOI + phase rule) | 1.5 | **2** | 4 | — (global data) | NOAA file-format drift |
| A5 | Phase 3 — `fspec.py`/`fencoding.py`, parameterize `encoding.py` on a spec | 1.5 | **2** | 4 | — | Refactor must leave `test_encoding.py` green or the shipped seed diverges |
| A6 | Phase 4 — SEAS5 live → `forecast_value` | 3 | **3.5** | 6 | 4 → 3.5 | CDS `seasonal-monthly-single-levels` param shape **assumed, never confirmed** (C1 probe not run) |
| A7 | **Phase 5 — hindcast calibration (HARD GATE)** | 3.5 | **5** | 10 | 6 → 5 | 1993–2016 bulk CDS (C2 not run); cross-grid join. **Skill may be near zero over Honduran terrain** |
| A8 | Phase 6 — GFS daily via GEE + **E2 probe** | 2 | **3** | 6 | 4 → 3 | E2 never run — but at 283 cells the quota exposure is negligible |
| A9 | Phase 7 — ENSO plume → `enso_forecast` | 1.5 | **2** | 5 | — (global) | IRI may only expose the chart → scraping; marked optional in the plan |
| A10 | Phase 8 — IBTrACS → `tc_track` + `tc_cell_exposure` | 2 | **3** | 5 | 4 → 3 | First real PostGIS vector work; trivial intersect at 283 cells |
| A11 | Phase 9 — CHIRPS → `wth_precip_alt` + E3 probe | 1.5 | **2** | 4 | 3 → 2 | Own export-budget probe still required (ceiling doesn't transfer) |
| A12 | Phase 10 — NMME via IRI OPeNDAP | 2 | **2.5** | 5 | 3 → 2.5 | New OPeNDAP backend; schema proven by A6 |
| A13 | Phase 11 — analog-year selection + per-pentad distributions | 2 | **3** | 6 | — | Acceptance test = reproducing the bulletin's own analogs (1982/1997/2015/2023) |
| A14 | Phase 12 — admin boundaries + `admin_cell_map` + SPI → `drought_alert` | 2 | **3** | 6 | 4 → 3 | **GADM licence blocks commercial use — switch to geoBoundaries** |
| A15 | Phase 13 — NDVI anomaly | 1 | **1.5** | 3 | 2 → 1.5 | Lowest value; least connected to crop modeling |
| A16 | **Gold `.WTH` materializer** + TAV/AMP/ELEV header (zero code today) | 2 | **3** | 5 | — | DSSAT charset question (base-36 codes may lead with a digit) unresolved |
| A17 | `mv_wth_monthly` / `mv_admin_monthly` + incremental refresh | 1 | **1.5** | 3 | 3 → 1.5 | Refresh correctness under ERA5T re-fetch |
| | **Subtotal A** | **33** | **47** | **88** | *58 → 47* | PERT **E = 51.5** |

### (B) API core

| ID | Work | low | **likely** | high | Δ | Top risk |
|---|---|---|---|---|---|---|
| B1 | FastAPI scaffold, settings, `psycopg3` async pool, lifespan, error model, OpenAPI, Docker image | 2 | **3** | 5 | — | ARM64 wheels if built on the Pi |
| B2 | Repository layer: async refactor of `src/db/query.py`, enforced `parent_id` pruning, stable query text, Pydantic models, sync wrappers for DAGs | 2 | **3** | 5 | — | Two drivers coexisting (psycopg2 for DAGs, psycopg3 for API) |
| B3 | Grid & metadata endpoints (1–7), GiST bbox path | 1.5 | **2** | 4 | — | `/grid/coverage` needs a matview or it's a full scan |
| B4 | Observed point series (8, 9, 12, 13) + json/csv/parquet + streaming | 2 | **3** | 5 | — | Full-history JSON is ~2.2 MB — stream or force parquet above a row cap |
| B5 | Area/aggregate (14–18) + rollups + sync/async policy | 2.5 | **3.5** | 6 | **6 → 3.5** | Was the DB-killer; at 283 cells it isn't |
| B6 | Normals & anomaly endpoints (19–22) | 1.5 | **2** | 4 | — | Tercile semantics must match the bulletin's |
| B7 | Forecast endpoints (23–28) + the calibration gate | 2 | **3** | 5 | — | Presenting uncalibrated numbers is what the gate prevents |
| B8 | Hazard / ENSO / drought endpoints (29–38) | 2 | **3** | 5 | — | — |
| B9 | Admin endpoints (39–42) | 1 | **1.5** | 3 | — | — |
| B10 | **`POST /v1/query` façade** + `/query/services` + credit accounting | 2 | **3** | 6 | — | Generate the catalogue from the models or it rots |
| B11 | Async job system: `job` table, ARQ workers, status/result, idempotency, cancellation | 2 | **3** | 5 | 4 → 3 | Job state split Redis/Postgres must reconcile after a Redis loss |
| B12 | Bulk exports: `.WTH` zip bundles, presigned URLs, TTL cleanup (DuckDB optional) | 1.5 | **2** | 4 | 3 → 2 | Full-country export is 100 MB — no refusal policy needed |
| B13 | Caching: Redis, key scheme, event-driven invalidation, ETag, ERA5T-aware TTLs | 2 | **2.5** | 4 | 3 → 2.5 | Stale-after-correction is a correctness bug |
| B14 | **Postgres-touching test infrastructure** — testcontainers, seeded mini-grid fixture, migration tests | 3 | **4** | 8 | — | **227 tests and not one touches Postgres.** New capability, not an extension |
| | **Subtotal B** | **27** | **38.5** | **66** | *43.5 → 38.5* | PERT **E = 41.2** |

### (C) Reports & rendering

| ID | Work | low | **likely** | high | Δ | Top risk |
|---|---|---|---|---|---|---|
⬛ **Rewritten.** `ClimateReportDataclimate` already ships C1, C3 and C5, plus the
en/es/pt i18n that was inside C3 and the SPI/GDD/water-balance logic costed elsewhere.

| ID | Work | low | **likely** | high | Δ | Top risk |
|---|---|---|---|---|---|---|
| C1 | ~~Map rendering~~ | — | **0** | — | **4.5 → 0** | **Built** — matplotlib + GeoJSON per-cell choropleths, no cartopy dependency |
| C2 | Charts: ENSO plume (~25 model series + DYN/STAT AVG), tercile bars | 1 | **1.5** | 3 | 3 → 1.5 | SPI/rainfall bars exist; only the plume is new, and it needs Phase 7 data |
| C3 | ~~PDF composition~~ | — | **0** | — | **4 → 0** | **Built** — Jinja2 → WeasyPrint, cover, sections, branding, 3 languages |
| C4 | **Seasonal bulletin composer** (~20 figures + rule-based narrative) | 3.5 | **5** | 10 | — | Still needs forecast data; narrative prose is a **liability surface** if wrong |
| C5 | Per-farm point variant of the existing regional report | 0.5 | **1** | 2 | **3 → 1** | Regional/admin report already built; this is a point-scope variant |
| C6 | Report caching, versioning, asset storage, pre-bake cron | 1 | **1.5** | 3 | — | — |
| C7 | ⬛ **Wrap the tool as a job-queue worker** behind `POST /v1/reports`, fix its unpruned queries, department boundaries | 2 | **3** | 5 | new | Today it is a CLI/MCP, not a service; its queries scan all partitions |
| | **Subtotal C** | **8** | **12** | **23** | *21 → 12* | PERT **E = 12.8** |

### (D) SaaS plumbing — geography-independent, unchanged

| ID | Work | low | **likely** | high | Top risk |
|---|---|---|---|---|---|
| D1 | API keys: `tenant`/`api_key`/`api_key_scope`, hashing + pepper, issuance, scopes, rotation, Redis lookup cache | 2 | **3** | 5 | Hash-vs-latency (don't Argon2 per request) |
| D2 | Rate limiting: Redis token bucket (Lua), per-key tiers, credits, 429 + headers | 1.5 | **2** | 4 | Credit weights need real usage data to be fair |
| D3 | Usage logging: buffered writes, nightly rollup, `/account/usage`, retention | 2 | **3** | 5 | Per-request INSERT would double write load |
| D4 | Account surface (56–60); **0 days if keys are issued manually at launch** | 3 | **4** | 8 | Scope creep into a dashboard — resist |
| D5 | Docs site: OpenAPI + guides + examples + **attribution/licence page** + changelog | 3 | **4** | 7 | The licence page is legally load-bearing |
| D6 | **CI from zero**: Actions — ruff, pytest incl. the Postgres suite, docker build, migration check, deploy | 2 | **3** | 5 | **No `.github/` exists at all** |
| D7 | DB migration tooling (Alembic/sqitch); today it's raw `.sql` applied by DAGs | 2 | **3** | 5 | Retrofitting onto an existing database |
| D8 | Licensing/attribution: `/meta/sources`, provenance block, ToS, per-source redistribution flags | 2 | **3** | 6 | **Depends on legal answers that don't exist yet** |
| | **Subtotal D** | **17.5** | **25** | **45** | PERT **E = 27.1** |

### (E) Cloud migration & ops

| ID | Work | low | **likely** | high | Δ | Top risk |
|---|---|---|---|---|---|---|
| E1 | **Split Airflow metadata DB from serving DB**; `climate` schema; least-privilege roles | 1.5 | **2** | 4 | — | Losing historical task runs (acceptable) |
| E2 | Provision cloud infra: VM, IaC, networking, firewall, TLS, secrets, DNS | 3 | **3.5** | 7 | 4 → 3.5 | First-time IaC overruns; **close 5432 on day one** |
| E3 | Data migration Pi→cloud + verification + cutover | 0.5 | **1** | 2 | **3 → 1** (2.3 GB; or just re-run the transform in cloud from bronze) | Uplink, but trivial at this size |
| E4 | Postgres tuning: `jit`, buffers, `work_mem`, locks, autovacuum, `pg_stat_statements` | 1 | **1.5** | 3 | **3 → 1.5** (no re-partition needed) | — |
| E5 | Backups + PITR + **a rehearsed restore drill** | 2 | **3** | 5 | — | An unrehearsed backup is not a backup |
| E6 | Observability: logs, Prometheus/Grafana, Sentry, uptime, **data-freshness SLIs**, alerting | 3 | **4** | 7 | — | Alert fatigue; picking freshness thresholds |
| E7 | Ingest topology: Airflow placement, bronze to object storage, credential migration | 2 | **2.5** | 5 | 3 → 2.5 | GEE key permissions (uid 50000/gid 0 gotcha) |
| E8 | Security hardening: no public 5432, TLS end-to-end, secret rotation, dep scanning | 2 | **3** | 5 | — | **5432 and 8080 are bound 0.0.0.0 with no TLS today** |
| E9 | Load test + capacity validation | 1 | **1.5** | 3 | 2 → 1.5 | — |
| | **Subtotal E** | **16** | **22** | **41** | *27 → 22* | PERT **E = 23.8** |

### Roll-up

| Bucket | low | likely | high | **PERT** | continental equivalent |
|---|---|---|---|---|---|
| A — Pipeline | 33 | 47 | 88 | **51.5** | 62.0 |
| B — API core | 27 | 38.5 | 66 | **41.2** | 47.0 |
| C — Reports | 14.5 | 21 | 40 | **23.1** | 26.4 |
| D — SaaS plumbing | 17.5 | 25 | 45 | **27.1** | 27.1 |
| E — Cloud & ops | 16 | 22 | 41 | **23.8** | 29.7 |
| **TOTAL** ⬛ | **101.5** | **144.5** | **263** | **156.4 dev-days** | 192.2 |

Independent σ = 13.6 days. These risks are strongly correlated — one developer, and
if cartopy fights ARM64 it fights the CI build too — so applying ~2.5×:

| Confidence | Dev-days |
|---|---|
| P50 | **156** |
| P80 | **~184** |
| P95 | **~210** |
| All-highs | **263** |

**Calendar:**

| Pattern | days/wk | P50 (156) | P80 (184) |
|---|---|---|---|
| Full-time (6 h/day) | 5.0 | **31 wk ≈ 7.2 mo** | 37 wk ≈ 8.5 mo |
| 3 h/day | 2.5 | 62 wk ≈ 14.4 mo | 74 wk ≈ 17.0 mo |
| **Observed (2 h/day)** | 1.7 | **92 wk ≈ 1.8 yr** | 108 wk ≈ 2.1 yr |

**Remaining calendar floors:** SEAS5 hindcast CDS queue, days-to-weeks (A7). Legal
review, weeks, and external. **The ≥2-month GEE quota floor is gone** — Honduras
bronze already exists.

---

## Slice order ⬛

Cumulative figures use the *likely* column; the P50 of 156 carries the PERT
uncertainty on top.

**M-now — The report, today (0 d).** `ClimateReportDataclimate` already generates the
branded monthly bulletin for Honduras from live silver. Two defects fixed 2026-08-05
(commit `f041de7`): `jit=off` by default (>15 min → 36.8 s) and the blank section-02
page. **This is sellable now, by email, with no API, no cloud and no auth.** It is
also the cheapest possible demand test before 20 days go into infrastructure.

**M0 — Walking skeleton (20 d).** E1 split Airflow DB → E2 provision + TLS → E3
migrate (trivial at 2.3 GB — and trivial *only now*) → E4 tune (`jit=off` alone is
5.6–18×) → B1 + B2 FastAPI + pooled repository → B4 `/point/series` → D1 API keys.
*Milestone:*
`curl -H 'Authorization: Bearer ck_live_…' '…/v1/point/series?lat=14.1&lon=-87.2&start=2020-01-01&end=2020-12-31'`
returns 366 rows over TLS in <100 ms.

**M1 — Observed data product (+40, cum. 60).** A0 update DAG → A2 + A3 normals
(materialize what the report computes live) → A16 gold `.WTH` → A17 rollups → B3, B5,
B6, B10, B11, B12, B13, B14 → D2, D3. *A1 is already done — Honduras is in silver,
311 cells, 1952–2026.*
**Sellable:** point series, monthly/pentad aggregates, normals with terciles,
anomalies, GDD, water balance, DSSAT `.WTH`, bulk exports, area aggregates. Depends
on zero external forecasts, zero new credentials, and nothing that GEE licensing,
boundary licensing, or the skill gate can block.

**M2 — Reports behind the API (+8, cum. 68).** C7 wrap the existing generator as a
job worker → C5 per-farm point variant → C6 caching and pre-bake → A4 climate_index →
department boundaries. Turns a manual monthly artifact into a self-serve product.

**M3 — Hazard & drought (+14, cum. 82).** A10 IBTrACS → A11 CHIRPS → A14 admin +
drought (**on geoBoundaries, not GADM**) → A15 NDVI → B8, B9.

**M4 — Forecast (+21, cum. 103) — the gated one.** A5 fgrid → A6 SEAS5 → **A7
hindcast (GATE)** → A8 GFS + E2 probe → A9 plume → A13 analogs → B7.
Do not start M4 before M1–M3 are earning. If A7 shows near-zero skill these endpoints
stay dark and the product is unaffected — which is precisely why it sits here.

**M5 — Full bulletin + hardening (+41.5, cum. 144.5 likely; P50 156).** A12 NMME → C2 plume, C4 bulletin
composer → D4, D5, D7, D8 → E5, E6, E7, E9.

The report earns from day zero; M0–M2 is 68 dev-days and delivers most of the
remaining commercial value on a third of the pipeline work. Everything scientifically
gated or legally uncertain is deferred behind revenue.

---

## Risk register

★★★ can stop the product.

**★★★ GEE licensing under a paid SaaS.** The fast ingest path runs on the free
**noncommercial** Contributor tier. Powering a paid public API is very likely outside
those terms. Three fallbacks, none fatal: a commercial EE licence (~$0.40/EECU-h →
the Honduras backfill already consumed 17.41 EECU-h ≈ **$7 if billed**, and ongoing
GFS at 283 cells is negligible); **CDS** (free, commercially usable, already built
and working, ~17 min/variable-year — perfectly acceptable at Honduras scale); or
**ARCO-ERA5 zarr** on public cloud buckets (free, no EE compute, mentioned nowhere
in the repo). **At Honduras scale this is a cheap problem** — the commercial-licence
price is trivial and the CDS fallback is genuinely viable. Resolve before charging.

**★★★ Boundary data licensing.** GADM explicitly forbids commercial redistribution
without prior permission. That blocks Phase 12 (`admin_boundary`, `admin_cell_map`,
municipal drought alerts) for a paid product — and admin aggregation is load-bearing
for both the bulletin and the admin report. **Switch to geoBoundaries (CC-BY,
commercial OK) now**, before A14 is written. Natural Earth (public domain, coarse)
and Honduran national INE/SINIT boundaries are the alternatives. Cost of deciding
today: zero.

**★★★ Licensing generally.** The repo contains **zero** licensing content: no
`LICENSE`, no `license` field in `pyproject.toml`, no attribution string, no grep hit
for licence/attribution/redistribution anywhere. For a service redistributing derived
data that is a gap. Copernicus (ERA5/SEAS5) permits commercial reuse **conditional on
attribution and a disclaimer**, currently absent. CHIRPS, IBTrACS, NOAA indices are
low-risk; NMME/IRI need checking. Mitigation: ship `/v1/meta/sources` and a
`provenance` block on every response **in M0** so attribution is structural; per-source
redistribution flags in config so a source can be disabled without a code change;
legal review before charging. Budget 2–5 days of your time plus counsel — not in the
dev-day total.

**★★★ Phase 5 hindcast gate.** Six endpoints, the bulletin, and the forecast half of
the farm report sit behind it; A7 is the largest single task (3.5/5/10). **The gate
can fail** — seasonal skill over Honduran complex terrain may be near zero. The honest
product then is "here is the forecast *and its measured skill, which is low*", which
is better science and a harder sell. Run the C1/C2 CDS probes before committing to
M4's schedule (~2.5 h of unattended queue, the cheapest de-risking available), and
design B7 to surface `skill_acc` as a first-class field from day one so a low value is
a feature, not a retraction.

**★★ No CI, and no test touches Postgres.** 227 tests, all offline, no `.github/`. Every
endpoint is a SQL correctness question the current suite cannot see. B14 and D6 are in
M0/M1 for that reason. Add a contract test on the shipped seed (`EU9K → 0XKE`) so a
block-size change cannot silently ship. (Symptom of no CI: the README still reports 129
tests.)

**★★ Report fidelity is a subjective acceptance criterion.** "Reproduce the CENAOS
bulletin" has no pass/fail line, and C1 + C4 are the tasks most likely to expand (9.5
likely, 19 high). Define acceptance as *"reproduces figures 1–8 with a named data source
per figure and a documented style spec"*, not *"looks like it"*. Ship JSON/HTML first,
PDF second.

**★★ Generated narrative text is a liability surface.** "Above-normal rainfall likely in
the western region" is a forecast statement; if the API generates it, the operator makes
the claim. Rule-based generation from terciles only, never free-form LLM output; every
statement carries its probability and skill score; disclaimer in the PDF and in
`/meta/sources`; keep raw numbers accessible so a customer can disagree.

**★★ Single-developer duty cycle.** 24% observed turns 7.7 months into 1.9 years. Not a
criticism of the pattern — it is the dominant term in the calendar and should be an
explicit input to the decision, not a discovered outcome.

**★ Pi hard freezes — downgraded at this scale.** Three in two days, no OOM/thermal/PCIe
trace in any journal; swap is 199 MiB against a documented 2 GB floor; no resource limits
in compose. At continental scale this blocked an 11.8-day unattended backfill. **At
Honduras scale there is no long unattended run to block** — bronze is already acquired,
and the daily update is minutes. Still: get the serving DB off the Pi in M0, raise swap
to 2 GB, add compose resource limits.

**★ Partition scaling — mostly moot.** 28 partitions, ~0.5 MB relcache per backend, 28
locks per query. `jit=off` and `max_locks_per_transaction` remain worth doing (5 minutes
for 5.6–18×). The `b=4`→`b=16` re-partition is unnecessary — but note it becomes
near-impossible after any large load, so **if Central America or LatAm scale-out is ever
likely, decide it before that load, not after**.

**★ Manifest has no region component.** Known runbook gap: a new extent over years
already downloaded is skipped as "done". `data_root` sidesteps it. Fix during A2's
manifest generalization.

---

## Critical files

- `src/db/query.py` — the read API to refactor into a pooled async repository; the
  `parent_id`-pruning contract lives here
- `src/db/silver_schema.sql` — `wth_base` DDL and index decisions
- `src/grid/encoding.py` — pure, no I/O, already vectorized; the API reuses it directly
  for arithmetic point resolution
- `docker-compose.yml:14` — the shared Airflow/serving conn string (E1); ports 5432/8080
  on `0.0.0.0` with no TLS (E8); no resource limits
- `docs/plan_climate_context_layer.md` — the 14-phase dependency graph gating every
  forecast, hazard, and report endpoint
- `docs/cost_model_climate_context.md` — measured EECU/egress/storage behind the cost
  section
- `.localdata/hn/bronze/` — the 539 existing Honduras parquet files (already loaded
  to silver: 311 cells, 1952–2026, 8.46 M rows)
- ⬛ `~/code/mr/ClimateReportDataclimate` — **the working report generator.**
  `climate_report/build.py` (orchestration), `template.html.j2` (layout),
  `figures.py` (maps/charts), `aggregate.py` (climatology — the logic A3 should
  materialize), `indices.py` (SPI/GDD/min-RH), `mcp_server.py` (3 MCP tools),
  `assets/*.geojson` (boundaries). Branch `fix/pagination-and-jit`, commit `f041de7`
- **New:** `src/api/` (app, routers, repository, models), `src/reports/` (mapspec,
  figures, templates), `src/db/app_schema.sql` (tenancy, jobs, usage),
  `.github/workflows/` (does not exist)

---

## Verification

This plan produces an estimate, not code. Its claims are checkable as follows — and
the first three are worth doing regardless:

1. **`jit` win** — `docker exec …postgres-1 psql -U era5 -d era5 -c "set jit=off;
   explain (analyze) select count(*), avg(precip) from wth_base where date='2020-06-15';"`
   versus `set jit=on`. Measured 1,550 ms vs 8,665 ms today.
2. **Honduras cardinality** — `select count(*) from era5_land_base_grid where is_land
   and lat between 12.9 and 16.6 and lon between -89.4 and -83.1` → 283; `count(distinct
   parent_id)` → 28; `count(distinct t_zone)` → 1.
3. **Bronze already exists** — `find .localdata/hn -name '*.parquet' | wc -l` → 539,
   spanning 1950–2026, 509 MB.
4. **Endpoint feasibility** — each inventory row names its tables and marks which don't
   exist; cross-check `src/db/silver_schema.sql` and `plan_climate_context_layer.md`.
5. **Effort calibration** — `git log --format=%ad --date=short | sort -u | wc -l` (11
   active days), `find src airflow tests scripts -name '*.py' | xargs wc -l` (8,702
   lines), `uv run pytest --collect-only -q` (227 tests).
6. **Cost arithmetic** — every storage figure is `cells × days × bytes/row`; every egress
   figure is `req/day × 20 KB`. Both are shown inline and recomputable. Cloud list prices
   are May-2026 vintage and must be re-verified.
