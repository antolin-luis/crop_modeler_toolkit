# CHIRPS on a Native 0.05° Fine Grid

**Status:** code complete for Phases 0–4 (branch `chirps-fine-grid`); backfill not yet run.
Supersedes §5.7 and Phase 9 of [`plan_climate_context_layer.md`](plan_climate_context_layer.md),
which assumed a 0.25°-aggregated CHIRPS.

**Why:** `wth_base` carries one precipitation estimate — ERA5 at 0.25° (~25 km). ERA5 precip
is weak over tropical complex terrain and convective regimes, exactly the regime this project
targets. CHIRPS is the standard corrective: station-blended satellite-IR rainfall at **0.05°
(~5 km)**, 25× the spatial detail, with 45 years of daily history.

Aggregating CHIRPS to 0.25° at ingest would have discarded the one thing that justifies
adding it. So it gets its own grid.

---

## 1. Facts that constrained the design

| Fact | Consequence |
|---|---|
| Both v2.0 and v3.0 begin **1981-01-01**; catalog ends 2026-06-30 | Max history is **45.5 years**, not 50+. Nothing in CHIRPS reaches further back; 1940–1980 stays ERA5-only. |
| v3.0 daily lives under **`UCSB-CHC`**, v2.0 under **`UCSB-CHG`** | A one-character trap that fails late inside EE. Both are aliased in `src/gee/chirps.py`. |
| v2.0 spans **50°S–50°N**, v3.0 spans **60°S–60°N** | The grid is defined on v3's wider span so one grid serves both. `covers_extent()` refuses an extent past a version's coverage rather than returning silent holes. |
| A global 0.05° grid is **17,280,000 cells** | Cannot be shipped as a seed the way `era5_land_base_grid` is. The grid *table* is region-scoped; the *codes* stay globally deterministic. |
| 17.28M > 36⁴ = 1,679,616 | 4-char codes cannot address it. Codes are **5 chars** (36⁵ = 60.4M). |

### Products loaded

| Silver `source` | EE collection | Note |
|---|---|---|
| `chirps_v3_rnl` (3) | `UCSB-CHC/CHIRPS/V3/DAILY_RNL` | v3 pentads disaggregated to daily via ERA5. Primary. |
| `chirps_v2` (2) | `UCSB-CHG/CHIRPS/DAILY` | v2.0 final. Comparison baseline. |
| `chirps_v3_sat` (4) | `UCSB-CHC/CHIRPS/V3/DAILY_SAT` | Registered, not loaded. NRT variant. |

---

## 2. Design decisions

### D1 — A parallel grid. `src/grid/spec.py` is untouched.

`src/grid/fine_spec.py` + `src/grid/fine_encoding.py` sit beside the 0.25° pair rather than
parameterizing them. CLAUDE.md forbids changing the canonical constants, and the 0.25° path
is load-bearing for every ERA5 row already stored. The two grids genuinely differ in binning
rule and code width, so a shared implementation would be parameter soup with a footgun in it.

**Code width is a type tag.** 4 chars ⇒ always the 0.25° ERA5 grid. 5 chars ⇒ always this
one. `fine_encoding` refuses to decode a 4-char code. The two code spaces cannot be silently
confused in a column, a join, or a log line.

### D2 — Cell **edges** on multiples of 0.05, not centres.

The one place the fine grid differs in kind, and the most expensive thing to get wrong.

```
ERA5 0.25°:   pixel CENTRES on multiples of 0.25   -> bin with round()
CHIRPS 0.05°: pixel EDGES   on multiples of 0.05   -> bin with floor()
              centre = (i + 0.5) * 0.05
crsTransform  = [0.05, 0, -180.0, 0, -0.05, 60.0]      # no half-cell offset
```

Indices are computed by multiplying by `1/RESOLUTION = 20`, never by dividing by 0.05 — 0.05
has no exact binary representation, and on a 0.05° grid most round-numbered coordinates land
*on* an edge, where a division puts them on the wrong side of a `floor` about half the time.

**Consequence: the two grids do not nest.** ERA5 cell edges land on `0.125 + k·0.25`, not a
multiple of 0.05. An ERA5 cell overlaps **36** fine cells (16 fully, 20 partially); a fine
cell touches up to **4** ERA5 cells. Moving between them is an area-weighted spatial join
(D6), never a division.

Aligning the fine grid to ERA5 instead would have forced server-side resampling of every
CHIRPS pixel — the open question flagged at `scripts/gee_cost_probe.py:30-33`. Resolved here
in favour of native values.

### D3 — Separate table, `source` in the PK, partitioned on the fine parent.

`wth_base` stays single-source and untouched. `source` is `SMALLINT`, not `TEXT`: it sits in
the PK of a ~10⁸-row table, so its width is paid in both heap and index.
Partitions are prefixed **`pcp_`**, not `wth_` — the two parent code spaces share a global
table namespace and would otherwise risk collision.

### D4 — No local-day reduction, documented rather than hidden.

CHIRPS ships already-daily, so there is nothing left to re-window and `zones = 1`. Its `date`
is the product day (UTC-anchored); `wth_base.date` is a per-cell **local** day defined by
`era5_land_base_grid.t_zone`. The two denote slightly different 24-hour windows. Daily
reduction is lossy, so this cannot be corrected after the fact.

Stated in `precip_schema.sql`, in the `precip_compare` view's SQL comment, and surfaced as an
`era5_t_zone_minutes` column. Immaterial at monthly/seasonal aggregation; **not** immaterial
on a single day.

### D5 — Unchunked, gated on a probe. **Gate passed.**

`CELL_ZONE_CEILING = 58_554` was measured at 0.25° cells, 366 bands, ERA5's hourly-reduce
compute shape — its docstring says so. Tocantins CHIRPS is 17,136 cells × 1 zone × 366 bands,
roughly 3× under the largest measured success. That was a prediction, not a measurement, so
Phase 0 probed it before anything else ran.

**Measured, 2026-08-07:** both collections exported 2020 at `attempt == 1`, `state=COMPLETED`.
No chunking is needed at this extent and `src/gee/chunks.py` stays untouched. A larger extent
needs `chunks.plan_chunks` generalized to the fine grid **and** its own probe — the ceiling
still has no measurement at 0.05°, only a pass well under it.

### D6 — Cross-grid comparison via a stored area-weight table.

`chirps_era5_map (fine_id, child_id, weight)`, computed once per extent with PostGIS. Makes
the regridding inspectable and reversible — the rule in `climate_context_layer.md` §96.
`chirps_coverage < 1.0` in the view flags an ERA5 cell the fine grid only partly covers, so
edge-of-extent averages are visible rather than an unexplained bias.

Aggregating **up** only. The other direction would manufacture detail ERA5 does not have.

---

## 3. Grid arithmetic

```
RESOLUTION 0.05   LAT_ORIGIN 60.0   NLAT 2400   NLON 7200
CODE_WIDTH 5      BLOCK_B 20 (20x20 = 400 fine cells = a 1.0° parent)
```

`BLOCK_B = 20` gives a fine parent the same 1° footprint as a b=4 ERA5 parent, so both grids
partition the world into the same boxes. It is **immutable** — baked into every stored
`fparent_id` and every partition name.

Two clean properties follow from 2400 and 7200 both being divisible by 20, neither of which
the 0.25° grid enjoys (`NLAT=721` is not divisible by 4):

- every parent holds **exactly 400** cells — no short edge blocks
- parent boundaries land on **integer degrees**, so 180° is itself a boundary and no block
  ever straddles the antimeridian

### Worked example — Palmas, Tocantins (−10.24, −48.36)

```
                    ERA5 0.25°                    CHIRPS 0.05°
lat_idx    round((90-lat)/0.25)   = 401     floor((60-lat)*20)     = 1404
lon_idx    round((lon%360)/0.25)  = 1247    floor((lon%360)*20)    = 6232
n          401*1440 + 1247 = 578,687        1404*7200 + 6232 = 10,115,032
cell       base36_4(n)  = "CEIN"            base36_5(n)  = "60ST4"
parent     base36_4(100*360+311) = "0S0N"   base36_5(70*360+311) = "00JON"
parent box [-10.125,-48.125,-9.875,-47.875] [-11.0, -49.0, -10.0, -48.0]
```

---

## 4. Query path

Mechanically identical to ERA5: a coordinate resolves arithmetically, no PostGIS on the point
path.

```sql
SELECT date, source, precip FROM wth_precip_alt
WHERE fparent_id = '00JON' AND fine_id = '60ST4' AND date BETWEEN %s AND %s
ORDER BY date, source;
```

**`fparent_id` must be in every `WHERE`** — it is the LIST partition key. Verified against
Postgres: with it, one `Index Scan` on `pcp_00jon`; without it, an `Append` across every
partition. Because it is a pure function of `fine_id`, the region path derives it in Python
(`fine_encoding.fine_parent_of`) rather than paying a second round-trip.

`geom` + GiST is for polygon work only (`cells_in_region`), never the point path.

Sizing note: a Tocantins municipality is ~2,000 fine cells against ~80 on the 0.25° grid —
25× the rows out, same number of partitions touched.

---

## 5. Sizing — Tocantins, both sources, full 1981→present

Extent `[-13.50, -50.75, -5.15, -45.70]` → **167 × 101 = 16,867 fine cells**, 54 parents.

Two counts differ and it matters which one you use:

| Count | Value | What it is |
|---|---|---|
| Export raster | 168 × 102 = 17,136 px/day | the GeoTIFF's shape |
| **Delivered cells** | **167 × 101 = 16,867** | what reaches Parquet — the box, exactly |

The raster is one row and one column larger than the box because `export._land_region`
intersects the bbox with LSIB at `max_error=1000.0` m ≈ 0.009°, and that simplification lets
the clipped geometry bulge past a bbox edge into the next pixel. **Those 269 extra pixels are
always masked**, and `dropna` removes them before Parquet: a bulge is bounded by ~0.009°
while a pixel's centre sits `RESOLUTION / 2` = 0.025° inside its edge, so the bulge can never
reach the centre that would have to be sampled. Structural, not lucky — and it holds for any
extent while `max_error < RESOLUTION / 2`.

Measured, Tocantins 1981, both sources: `raster_pixels / days = 17,136`, `cells = 16,867`,
dropped 269, `land_fraction = 0.9843`. So the exact box covers everything the export
delivers, and `chirps_base_grid` is **not** padded.

> An earlier revision padded the grid by one cell per side, on the strength of the cost
> probe's `cells = 17,136`. That figure is the probe's own artifact: `count_pixels` reports
> raster *shape*, not valid data, so it over-reports by exactly those 269 (and correspondingly
> understates `eecu_per_unit` and `bytes_per_unit` by ~1.6% for CHIRPS rows). The padding was
> reverted. What survives from it is the invariant it was reaching for — no `wth_precip_alt`
> row may reference a `fine_id` absent from `chirps_base_grid` — which nothing in the schema
> enforces, so it is a verification gate instead.

| Quantity | Value |
|---|---|
| Days 1981-01-01 → 2026-06-30 | 16,617 |
| Rows per source | ~285 M |
| **Rows total (2 sources)** | **~570 M** |
| Heap + PK index @ ~100 B/row | **~57 GB** |
| Rows per partition | ~10–13 M |
| **EECU, measured** (E3, 2020) | **0.0055** (v3_rnl) + **0.0045** (v2) EECU-h/year → **~0.46 EECU-h** total |
| Egress, measured | 21.2 + 10.5 MiB/year → **~1.5 GB** total |
| Export wall-clock, measured | 66–84 s/year-source → ~2 h over 91 tasks |

**The modelled EECU was wrong by ~143×.** The plan predicted ~0.79 EECU-h/year from
`0.0512 + 4.33e-5 × cells`, a curve fitted to ERA5. It does not transfer: ERA5 pays for an
hourly→daily reduction over 24× the input images, whereas CHIRPS arrives already daily with
`zones = 1`, so the cost is essentially just writing output pixels. At 0.05% of one month's
Contributor quota, EECU is not merely "a non-issue" here — it is noise, and no chunking or
quota scheduling is warranted.

**Storage and DB write time are the real costs**: ~11× the entire current Honduras silver,
several hours of pure database write on the Pi, days of wall-clock overall. The year-scoped
DAG makes it resumable.

Escape hatches if it gets tight, documented but **not built**: sub-partition by year range,
or store `precip` as `SMALLINT` in 0.1 mm units (lossless at CHIRPS's precision, ~1 GB).

---

## 6. What was built

| Path | Role |
|---|---|
| `src/grid/fine_spec.py` | 0.05° constants. Immutable. |
| `src/grid/fine_encoding.py` | 5-char base-36 encode/decode, parents, bboxes |
| `src/grid/encode_fine.py` | raster → `[fine_id, fparent_id, date, value]`, **raises** on misalignment |
| `src/gee/chirps.py` | source registry + daily collection builder |
| `src/gee/chirps_download.py` | export → GeoTIFF → Parquet, pinned to `FINE_CRS_TRANSFORM` |
| `src/gee/export.py` | *(modified)* additive `crs_transform` kwarg + `FINE_CRS_TRANSFORM` |
| `src/gee/metrics.py` | *(modified)* threads `crs_transform` through `run_export` |
| `src/db/fine_grid_schema.sql` | `chirps_base_grid` DDL |
| `src/db/precip_schema.sql` | `wth_precip_alt` + source lookup + QA + map DDL |
| `src/db/seed_fine_grid.py` | extent-scoped grid build; extents accumulate |
| `src/db/precip_load.py` | staging → COPY → upsert; `pcp_` partitions |
| `src/db/precip_query.py` | point / multi-cell / region reads |
| `src/db/chirps_map.py` | area-weight map + `precip_compare` view |
| `src/transform/precip_alt.py` | bronze → silver, QA, calendar report |
| `airflow/dags/chirps_grid_build.py` | per-region grid build |
| `airflow/dags/download_bronze_chirps.py` | bronze backfill, `gee_pool` |
| `airflow/dags/transform_precip_alt.py` | silver load, `silver_pool` |
| `scripts/gee_cost_probe.py` | *(modified)* `--collection`, native-grid export, `--coarse` |

Three separate DAGs, no shared task graph. Bronze auto-triggers transform; either runs alone.

**109 new tests, all offline** (mocked `ee`, synthetic rasters, fake connections). Suite: 227
→ 336.

---

## 7. Operating it

```bash
# 0. Probe the export budget first — gates everything else. DONE for Tocantins; re-run for
#    any new extent. --data-root matters: DATA_DIR defaults to /data, which exists in the
#    container but not on the host.
uv run python scripts/gee_cost_probe.py chirps --year 2020 \
    --extent -13.50 -50.75 -5.15 -45.70 --collection v3_rnl --sample E3 \
    --data-root .localdata/probe_chirps
# Gate: attempt == 1. A restart means the extent needs chunking at 0.05°.

# 1. Build the fine grid for the region (once).
#    Trigger chirps_grid_build: {"extent": [-13.50, -50.75, -5.15, -45.70]}

# 2. Backfill bronze; auto-triggers the transform.
#    Trigger download_bronze_chirps:
#    {"extent": [...], "start_year": 2020, "end_year": 2020,
#     "sources": ["chirps_v3_rnl"], "data_root": "TO"}

# 3. Cross-grid map + comparison view, after the first year lands.
uv run python -m src.db.chirps_map
```

### Verification gates

1. `chirps_base_grid` holds **16,867** rows, **54** distinct `fparent_id`.
2. Bronze export completes at `attempt == 1`. **Verified 1981, both sources:** 6,156,455 rows
   = 16,867 × 365, 54 parents, zero nulls, all code widths 5, ~0.004 EECU-h each.
3. One year of one source → **cells × days** exactly (1981: 16,867 × 365 = 6,156,455). The
   grid is the exact box, so **`wth_precip_alt` must contain no `fine_id` absent from
   `chirps_base_grid`** — check it explicitly, since no FK enforces it:
   ```sql
   SELECT count(*) FROM wth_precip_alt p
   LEFT JOIN chirps_base_grid g USING (fine_id) WHERE g.fine_id IS NULL;  -- must be 0
   ```
   Non-zero means the masked-edge reasoning in §5 has broken for this extent, and the grid —
   not the loader — is what needs widening.
4. **Spot-check one cell against the GEE Code Editor for the same date and collection.
   Values must match to float precision.** If they do not, the `crsTransform` is wrong and
   CHIRPS is being resampled. This is the single most likely defect in the whole design, and
   the reason `encode_fine_grid` raises rather than snaps.
5. A wet-season month against `wth_base.precip` via `precip_compare`. Expect disagreement —
   that is the point — but monthly totals should be the same order of magnitude.

---

## 8. Non-goals

Replacing ERA5 as the observational base; a CHIRPS `update` DAG (latency is ~5 weeks, this is
a backfill); loading `DAILY_SAT`; extending `wth_base` with a CHIRPS column; generalizing
`src/gee/chunks.py` to the fine grid unless the probe forces it; regridding ERA5 up to 0.05°.
