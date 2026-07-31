# Cost Model & Calibration Protocol — Climate Context Layer

**Status:** planning. No spend authorized by this document; it defines *how to measure* before scaling.

**Companions:** [`plan_climate_context_layer.md`](plan_climate_context_layer.md) (what to build), [`climate_context_layer.md`](climate_context_layer.md) (why), [`gee_setup.md`](gee_setup.md) §8 (existing EECU calibration method — this document extends it rather than replacing it).

**Governing principle, inherited from `gee_setup.md` §8:** *calibrate on a small real run, read the actual meter, extrapolate linearly. Don't trust a-priori estimates.* Every number below is either a formula or a measurement slot — not a claim.

> **Prices verified 2026-07-30** against GCP published pricing for the bucket's actual location, `southamerica-east1` (São Paulo, **regional** — not the `US` multi-region `gee_setup.md` §5 prescribes). Internet egress: **$0.12/GB Premium tier, $0.085/GB Standard**. Re-verify before any decision that turns on them; prices move and the bucket could be recreated elsewhere.

---

## 1. What is actually billable

Most of this plan costs nothing. Being precise about which parts *can* cost money is most of the work.

| Resource | Billable? | Binding constraint | Used by |
|---|---|---|---|
| **Earth Engine compute (EECU)** | **No** on noncommercial free tier | Monthly quota — **Contributor tier active: 1,000 EECU-h/month** | GFS, CHIRPS, NDVI |
| **GCS storage** | Yes | ~$0.020/GB-month Standard (⚠ SA-East rate unverified) | Transient GeoTIFF staging |
| **GCS Class A ops** (write/list) | Yes | ~$0.005 / 1,000 ops (⚠ unverified) | Every export shard |
| **GCS Class B ops** (read) | Yes | ~$0.0004 / 1,000 ops (⚠ unverified) | Every download |
| **GCS network egress → internet** | Yes — but **measured small**, see §2 | **$0.12/GB Premium, $0.085/GB Standard** (SA-East, verified 2026-07-30) | Every byte pulled to the Pi |
| **CDS (Copernicus)** | No | Queue position + per-request cost limit | ERA5, SEAS5, hindcast |
| **NOAA / IRI / IBTrACS / GADM** | No | Politeness / rate limits | Indices, plume, cyclones, boundaries |
| **Pi 5 + SSD** | Sunk capex | 2 TB disk | Everything |
| **Electricity** | Marginal | ~5–10 W continuous | Everything |

**Two things follow immediately:**

1. **EECU is a quota problem, not a cost problem.** Exceeding it does not bill you — tasks queue or fail. The consequence of underestimating is *delay*, not an invoice. That makes it the safe kind of unknown. It is also now the *only* open question in this document (§2).
2. **GCS egress is metered per byte** with no free ceiling, which is why it was the headline risk here. It has since been measured and is small (§2) — but it remains the only line item that could ever produce an invoice, so the budget alert stays.

---

## 2. The egress finding — MEASURED 2026-07-30, and it was wrong by ~1000×

**Original claim (retained for the record):**

```
LatAm ERA5 backfill (gee_setup §8 sizing):
  60,000 cells × 16,800 days × 7 vars × 4 B (float32)   ≈ 28 TB uncompressed
  at ~2× effective GeoTIFF compression                   ≈ 14 TB egressed
  at ~$0.12/GB                                           ≈ $1,700
```

**That arithmetic is off by three orders of magnitude — a unit error.** `60,000 × 16,800 × 7 × 4 B = 2.82e10 B` = **28 GB**, not 28 TB. Corrected, the same sketch gives ~14 GB and **~$1.70**.

Two independent measurements against real artifacts confirm it, both obtained at **zero EECU and zero egress** by reading GCS object metadata and matching it to Parquet already on disk:

| Measurement | Source | Result |
|---|---|---|
| **Honduras, complete backfill** (1950–2026, 7 vars, 539 variable-years, 341 land cells) | 539 GCS objects vs `.localdata/hn` Parquet row counts | **364 MB egressed, 66.7 M cell-days → 5.46 B/cell-day → $0.044** |
| **Latin America, 1 year × 7 vars** | `bench-latam/*.tif` object sizes (2026-06-27 run) | **371 MB** → ×46 yr = **17.1 GB → $2.05** Premium / $1.45 Standard |

So the entire continental 46-year backfill costs about **two dollars** of egress, and the Honduras archive that already exists cost four cents. The `$20` budget alert sits ~10× above the largest job in the plan.

**Bytes per useful cell-day is roughly extent-stable:** Honduras 5.46 B/cell-day, LatAm **4.77** B/cell-day (371 MB / (30,303 cells × 366 d × 7 vars)) — 13% apart. *(An earlier revision of this section claimed ~2.1 B/cell-day for LatAm and concluded the rate varied strongly with extent. That used a guessed ~60k land cells; the measured LSIB-clipped count is **30,303**.)* Egress therefore does extrapolate linearly. **EECU does not** — see §6.

### Consequences

- **The mitigation table below is moot.** `int16` scaling, `toDrive`, and direct-download were all proposed to avoid a four-figure bill that does not exist. None is worth its complexity to save a dollar. `land_only` stays because it also cuts EECU.
- **Extent is no longer an egress decision.** Choose it on EECU (§5.1) and silver disk (§10), not on transfer cost.
- **Storage hygiene still matters** — see the §7 note; the lifecycle rule currently in place does not delete anything.

| Option | Verdict after measurement |
|---|---|
| `int16` scaling | **Drop.** Saves ~$1 continent-wide; costs precision and a contract change. |
| `land_only` (implemented) | **Keep** — it cuts EECU, which is the binding constraint. |
| `Export.image.toDrive` | **Drop.** Solves a non-problem, adds a retrieval path. |
| Direct download from EE | **Drop** for cost reasons; may still be worth it to reduce moving parts. |
| Shrink the extent | Still valid — but for **EECU and disk**, not egress. |

---

## 3. Parametric cost model

Named variables so measured samples can be plugged in without re-deriving anything.

**Measured per-sample (from a calibration run):**

| Symbol | Meaning | Read from |
|---|---|---|
| `E_s` | EECU-hours consumed by sample `s` | EE task list / project quota page |
| `B_s` | Bytes egressed by sample `s` | GCS metrics, or `du` on the downloaded staging dir |
| `N_s` | Cells × days × variables in sample `s` | Known from the run's own params |
| `T_s` | Wall-clock minutes | Airflow task duration |
| `R_s` | Rows landed in silver | `SELECT count(*)` |
| `D_s` | Bytes on disk in Postgres | `pg_total_relation_size` |

**Derived unit rates:**

```
eecu_per_unit   = E_s / N_s
bytes_per_unit  = B_s / N_s
rows_per_unit   = R_s / N_s
disk_per_row    = D_s / R_s
```

**Extrapolation to any target:**

```
N_target      = cells_target × days_target × vars_target

EECU_target   = eecu_per_unit  × N_target
egress_GB     = bytes_per_unit × N_target / 1e9
egress_cost   = egress_GB × price_per_GB          ⚠ verify price
disk_GB       = disk_per_row × rows_per_unit × N_target / 1e9
months_needed = ceil(EECU_target / monthly_quota)     # the real schedule constraint
```

**Linearity assumption.** Cost scales ~linearly in `cells × days` — this is `gee_setup.md` §8's assumption and it is reasonable for pixel-wise reductions. It will **break** for anything with a spatial join or neighbourhood operation (`tc_cell_exposure`, `admin_cell_map`). Those are flagged as non-linear in §4 and must be sampled at two different sizes to get a real curve, not one.

---

## 4. Calibration protocol

One sample per cost driver. Each is small, cheap, and produces the numbers §3 needs. **Run them in this order** — the early ones gate the later ones.

**E0 is already done** — see §9.2. The Honduras backfill and the LatAm benchmark were mined from GCS object metadata, local Parquet, and `ee.data.listOperations()`, giving `B_s` and `E_s` at country scale for free. **This changed what E1 has to be:**

> Honduras's 539 tasks span **0.0170 → 0.0516 EECU-h for identical work — a 3× spread at constant workload.** At that size the meter is reading per-task overhead, not computation. A country-year sample therefore *cannot* produce a trustworthy `eecu_per_unit`, and multiplying it by ~176 to reach LatAm would be arithmetic dressed as evidence. E1 is re-scoped upward accordingly: it must run at an extent large enough that real compute dominates the fixed cost.

| ID | Sample | Measures | Extrapolates to | Est. run |
|---|---|---|---|---|
| **E0** | ✅ **Done** — mine existing Honduras backfill + LatAm benchmark from GCS/EE history | `B_s`, `E_s` at 341 land cells; egress at continental scale | Retired the §2 egress question entirely | 0 (free) |
| **E1** | **One year × 7 vars over Brazil** — extent `[-34.0, -74.0, 5.5, -34.75]`, **25,122 grid land cells → ~12,800 expected export cells, ≈38× Honduras** | `eecu_per_unit` **away from the overhead floor**; `bytes_per_unit` at mid scale; `compression_ratio` and `land_fraction` separately | The LatAm EECU question — the one real remaining unknown | ~1–2 h |
| **E2** | GFS one init, one day, full target extent | EECU + bytes per GFS run | Daily recurring burn, monthly quota fit | ~10 min |
| **E3** | CHIRPS one year, target extent | EECU + bytes per CHIRPS-year | `chirps_start_year` decision (10 yr vs full 46 yr) | ~20 min |
| **C1** | SEAS5 one live issuance, 2 vars | Queue wait, payload size, whether the splitter engages | Monthly SEAS5 cost (expected: trivial) | ~30 min |
| **C2** | SEAS5 hindcast, **one init month, all 24 years** | Request count, queue behaviour at bulk | Full hindcast (×12) | ~2 h |
| **S1** | `wth_normals` over one `parent_id` batch | CPU time, `rows_per_unit`, `disk_per_row` | Full-extent normals build | ~15 min |
| **S2** | `tc_cell_exposure` for **two storms of different track lengths** | Non-linear scaling of the PostGIS buffer/intersect | Full IBTrACS backfill | ~20 min |
| **S3** | `admin_cell_map` for **one country, then two** | Non-linear polygon×cell intersection | All target countries | ~20 min |
| **D1** | `forecast_value` GFS partition: TRUNCATE + COPY cycle ×10 | Bloat behaviour, vacuum pressure, refresh wall-clock | Whether daily GFS refresh is sustainable on a Pi | ~30 min |

**Total calibration effort: under a day of mostly-unattended runs**, and it retires nearly every unknown in this document.

### 4.1 Instrumentation (built — `src/gee/metrics.py`)

Every GEE run now appends one JSON record per line to **`<bronze_dir>/_gee_metrics.jsonl`**, beside `_manifest.json`. `src/gee/metrics.py` holds the accumulator (`RunMetrics`), the record store (`append_record` / `iter_records`), and `run_export` — the timed `start_export → wait_for_task → download_prefix_measured` triplet. `src/gee/export.py` contributes only the byte accounting it uniquely sees (`BlobStats`, `download_prefix_measured`); `download_prefix` keeps its old signature.

The record deliberately does **not** live in `_manifest.json`: that file is control state, rewritten whole on every mark and treated as empty when corrupt — a malformed metrics blob there would silently discard `done` and re-download an entire backfill. It is also keyed last-write-wins on `variable:year`, which cannot hold the same variable-year measured at two extents, and that comparison is exactly what §3's linearity assumption needs.

Fields, grouped by symbol:

| Group | Fields |
|---|---|
| `E_s` | `task_id`, `task_state`, `eecu_seconds`, `eecu_hours` |
| `B_s` | `n_blobs`, `bytes_remote`, `bytes_local`, `bytes_per_blob_max` |
| `T_s` | `t_export_s`, `t_download_s`, `t_encode_s`, `t_total_s`, `ee_queue_s`, `ee_compute_s` |
| `N_s` | `bronze_rows`, `raster_pixels`, `cells`, `cells_exact`, `days`, `n_units` |
| chunking (**v2**) | `chunk_id`, `parents`, `land_parents`, `n_zones`, `parallel`, `attempts`, `max_attempts` |
| derived | `bytes_per_unit`, `eecu_per_unit`, `compression_ratio`, `land_fraction` |
| context | `sample`, `kind`, `dataset`, `variable`, `year`, `extent`, `land_only`, `b`, `chunk_days`, `host`, `gee_project`, `run_id`, `started_at`, `finished_at`, `parquet_path`, `parquet_bytes`, `error` |

Three properties of the record worth knowing before reading one:

- **`eecu_hours` is `null`, never `0.0`, when EE omits `batch_eecu_usage_seconds`.** The field is not guaranteed on every terminal state. A silent zero would corrupt `eecu_per_unit` — the number this whole exercise exists to produce — so an honest null is recorded and `task_id` is the handle for reading the value off the EE task list by hand.
- **`bronze_rows` is not `R_s`.** Silver is wide: ~7 bronze variable-rows merge into one `wth_base` row. Using it as `rows_per_unit` inflates that rate 7×. `R_s` / `D_s` stay a Postgres query (§9).
- **A failed run still writes a record**, carrying `task_id`, `t_export_s` and `error` — which is precisely what you want when the failure *is* a quota event.
- **`schema_version` is `2` from 2026-07-31.** The added fields are the chunking group above; every v1 field kept its name and meaning, so a mixed JSONL reads fine and v1 rows simply lack the new keys. `attempts` defaults to `1` — the count EE implies when it never restarts a task — and is the field that separates "slow" from "dying repeatedly" (§9.3).
- **`compression_ratio` is measured against `raster_pixels`, not land cells.** Masked ocean pixels are stored in the GeoTIFF and egressed like any other, so they belong in the compression denominator; `land_fraction` reports separately how much of the raster was useful. An earlier version divided by land-only rows and consequently reported `0.80×` for Honduras — a codec "failure" that was really just a sea-heavy bounding box. **`bytes_per_unit` is the number to extrapolate with**, and §2 records that it is not extent-invariant.

The `download_bronze_gee` DAG takes a `sample` param (e.g. `"E1"`), stamps it on each record, logs a one-line summary per mapped task, and returns the same numbers as a dict XCom.

**Probes for the non-ERA5 samples:** `scripts/gee_cost_probe.py` (E2 GFS, E3 CHIRPS) and `scripts/cds_cost_probe.py` (C1/C2 SEAS5). The GEE probe goes through the same `run_export`, so E2/E3 are measured with the same ruler as E1; both probes append to the same JSONL. Neither writes bronze Parquet nor touches silver — the collection builders live in the scripts, not in `src/`, because the real modules are Phases 6/9 of `plan_climate_context_layer.md` and a probe must not pre-commit their design.

**What the instrumentation cannot see** — state these alongside any number derived from it:

- `bytes_remote` is a **lower bound** on billed egress: GCS bills retried chunks, and this sums each object once. A same-region transfer is not billed as internet egress at all.
- Class A/B ops are not instrumented. `n_blobs` is the Class B count and `ops ≈ n_blobs + 1`; at §1's rates that is rounding error beside egress.
- Historical runs cannot be retro-fitted — hence E1 being a *re-run*.

---

## 5. Cost profile of the context layer itself

Separating the new layer from the pre-existing ERA5 question, because they have very different shapes.

### 5.1 The important structural point: one-off → recurring

The ERA5 backfill is a **large one-off**. The context layer is **small but perpetual**:

| Source | Cadence | Nature |
|---|---|---|
| SEAS5 | monthly | ~1–2 CDS requests. Free. |
| SEAS5 hindcast | **once**, then static | ~12 CDS requests. Free. |
| GFS | **daily** (up to 4×/day) | Recurring EECU + recurring egress, forever |
| CHIRPS | once (backfill) + daily increment | Backfill is the cost; increment is trivial |
| NDVI | dekadal | Small |
| Indices / plume / IBTrACS / GADM | monthly / 6-hourly / static | Plain HTTP. Free. |

**The binding constraint changes from "total EECU" to "EECU per month".** A one-off backfill can be split across months to fit under quota (`start_year`/`end_year` already make this trivial). A daily job cannot be split — it either fits in the monthly quota every month, or it does not run.

**So the single most important number from calibration is E2**: EECU per GFS run × 30 days, compared against the monthly quota *net of whatever else is running*. The quota is **1,000 EECU-h/month (Contributor tier, already active)** — 6.7× the Community allowance the earlier draft assumed, which removes the "upgrade tier" escape hatch from the table because it has already been used. If GFS alone consumes most of 1,000, the remaining levers are a reduced variable set or a once-daily rather than 4×-daily schedule.

For scale: the *entire* Honduras 77-year × 7-variable backfill cost **17.41 EECU-h** — under 2% of one month's quota (§9.2).

### 5.2 Expected magnitudes (to be replaced by measurements)

Rough shape, pending E2/E3:

- **GFS daily, Central America extent:** 16 days × ~700 land cells × ~5 vars is a *very* small reduction — roughly 3% of a one-country-year ERA5 job, which itself is small. Egress per run likely single-digit MB. Expected verdict: comfortably inside quota, negligible egress. **Verify with E2.**
- **CHIRPS full 46-year history:** the same order of magnitude as an ERA5 variable backfill — this is the one context-layer item that is genuinely expensive in both EECU and egress. It is why `plan_climate_context_layer.md` §5.7 defaults `chirps_start_year` to a 10-year window. **E3 decides whether full history is affordable.**
- **Everything CDS or HTTP:** free, and small enough that queue latency on a monthly cadence is invisible.

---

## 6. Scaling scenarios

Partially filled from E0 (§9.2). **Honduras is measured at 341 land cells**, not the ~150 originally guessed, so every multiplier below is revised.

| Scenario | Land cells | Relative to Honduras | One-off EECU (46 yr × 7 vars) | Egress (46 yr × 7 vars) | Cost |
|---|---|---|---|---|---|
| Honduras only | **341** (measured) | 1× | **~10 EECU-h** (measured: 17.41 for 77 yr) | **~0.22 GB** (measured: 364 MB for 77 yr) | **$0.03** |
| Central America (7) | ~4–6k (est.) | ~15× | *E1 measures this* | ~1–2 GB (est.) | ~$0.2 |
| Mexico + CentAm | ~12–15k (est.) | ~40× | *unmeasured* | ~4–5 GB (est.) | ~$0.5 |
| Latin America | **30,303** (measured, LSIB-clipped) | ~89× | **~113 EECU-h** (measured: 2.45/yr × 46) | **17.1 GB** (measured) | **$2.05** |

**Both columns are now measured.** The LatAm EECU figure comes from the `bench-latam` calibration (2026-06-27): **2.45 EECU-h for one LatAm-year of all 7 variables over 30,303 land cells**, recorded in `docs/runbook.md` and confirmed by the project's own history. ×46 years → **~113 EECU-h ≈ 11% of one month's Contributor quota.** No year-splitting is needed for quota reasons.

**The overhead effect, now quantified.** `eecu_per_unit` is **3.16e-8** EECU-h/cell-day at LatAm scale vs **2.61e-7** at Honduras scale — small extents are **8.3× worse per unit**, because fixed per-task cost is amortized over far fewer values. This is why extrapolating from a country-sized sample overstates the total:

> ⚠ **Correction, 2026-07-30.** An earlier revision of this table projected **~1,840 EECU-h** for LatAm by scaling Honduras's rate up 175×, and concluded the backfill needed ~2 months of quota. That is **~16× too high**. It was written before the pre-existing 2.45 EECU-h/LatAm-year measurement was rediscovered, and it assumed ~50–65k land cells where the measured count is 30,303. The lesson is procedural, not arithmetic: **check `docs/runbook.md` and prior calibration notes before extrapolating** — the number already existed.

**Read the multiplier column before the estimates.** LatAm is ~89× Honduras — not ~400× as originally guessed, and not the ~175× of the interim correction.

---

## 7. Guardrails to put in place before scaling

Cheap, and each one caps a specific failure mode:

| Guardrail | Prevents |
|---|---|
| ✅ **GCP budget alert** at $20 (set 2026-07-30) | Any cost surprise. Now ~10× the largest job in the plan, so it functions as a tripwire for the *unexpected*, not for the backfill. |
| ⚠ **Bucket lifecycle rule** — see warning below | Storage accumulation from transient GeoTIFFs |
| **`retain_issuances` DAG param** (plan §4) | Unbounded bronze growth from 4×/day GFS |
| **`chirps_start_year` default = 10 yr** (plan §5.7) | Accidentally triggering the one expensive context-layer backfill |
| **Airflow pool caps** (`gee_pool`, `context_pool`) | Concurrent EECU burn spiking past quota |
| **EECU check before scale-up** | Discovering the quota ceiling mid-backfill |
| **`pg_total_relation_size` in the runbook** | Silent disk exhaustion on the 2 TB SSD |

**⚠ The lifecycle rule currently on the bucket does not delete anything.** As applied on 2026-07-30 it is:

```json
{"action": {"type": "AbortIncompleteMultipartUpload"}, "condition": {"age": 3}}
```

That only cleans up *failed, partial* uploads. Completed objects live forever — confirmed by `bench-latam` objects still present 33 days after the run, and 539 `bronze-gee` objects still present after 7. The rule `gee_setup.md` §5 intends is:

```json
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 3}}]}
```

Both are worth having: `Delete` for completed objects, `AbortIncompleteMultipartUpload` for interrupted ones. At 862 MB accumulated this costs ~$0.02/month, so it is hygiene rather than urgency — but it grows with every backfill, and an object that is never deleted is also never re-measured.

The budget alert deserves emphasis: it is the only guardrail that catches a cost the other mechanisms cannot see, and it takes about two minutes to set.

---

## 8. Decision gates

What each measurement would actually change — stated in advance, so the numbers are interpreted honestly rather than rationalized after the fact:

| If measurement shows... | Then... |
|---|---|
| ~~E1 egress for LatAm ERA5 > ~$100~~ | ✅ **Resolved by E0: $2.05 total.** Gate cannot fire; export path stays as-is |
| ~~E1 compression ≥ 4×~~ | ✅ **Superseded.** Egress is settled in absolute terms, so the ratio no longer drives a decision |
| ~~E1 `eecu_per_unit` vs Honduras~~ | ✅ **Resolved without E1**: the `bench-latam` calibration already measured 3.16e-8 at continental scale (8.3× better than Honduras). LatAm ≈ 113 EECU-h, comfortably inside one month. E1 is now confirmation, not discovery |
| **E1 `eecu_per_unit` materially above 3.16e-8** | The continental figure does not hold at intermediate extents — re-examine before scheduling a full backfill |
| E2 GFS > ~25 EECU-h/day (≈750/month) | Reduce GFS variables or drop to once-daily. **Contributor tier is already in use — there is no tier left to upgrade to** |
| E3 CHIRPS full history > ~500 EECU-h | Keep the 10-year default; make full backfill a documented opt-in with its own warning |
| C2 hindcast > ~24 CDS requests or heavy queueing | Split across two months; it is a one-off, so delay is free |
| S2/S3 scale worse than ~n log n | Precompute more aggressively or restrict admin levels to municipality-only |
| D1 shows table bloat despite TRUNCATE | Revisit the partition-swap approach (ATTACH/DETACH) from plan §5.3 |
| Silver disk projection > ~1.2 TB | Scope CHIRPS/NDVI down before they are built, not after |

---

## 9. Record-keeping

Results land in a committed table in this file (not a spreadsheet elsewhere) so the numbers stay next to the assumptions they inform.

### 9.1 How to run each sample

```bash
# E0 — free: no run at all. Reads GCS object sizes, matches them to bronze Parquet row
# counts on disk, and pulls EECU from EE's own operation history. Do this FIRST on any
# project with prior runs; it may answer the question without spending anything.
#   - GCS sizes:  storage.Client().list_blobs(bucket)  -> blob.size
#   - N_s:        pyarrow.parquet.ParquetFile(p).metadata.num_rows
#   - E_s:        ee.data.listOperations() -> metadata["batchEecuUsageSeconds"]
# Caveat: listOperations retains only recent tasks (~1 week observed), so mine it early.

# E1 — one year x 7 vars over BRAZIL, not a single small country. The point is to get
# eecu_per_unit away from the per-task overhead floor that makes E0-HN untrustworthy:
# ~12,800 export cells vs Honduras's 341, a ~38x step.
# data_root is REQUIRED: the manifest key is `variable:year` with no extent, so a re-run
# without it silently no-ops (docs/runbook.md §4). One JSONL line per (variable, year),
# so an E1 sample is 7 lines — aggregate by summing them.
docker compose run --rm airflow-scheduler airflow dags trigger download_bronze_gee -c \
  '{"extent":[-34.0,-74.0,5.5,-34.75],"start_year":2020,"end_year":2020,
    "data_root":"/data/calib_br","sample":"E1"}'

# Then compare eecu_per_unit against E0-HN's 2.61e-7. If it has dropped substantially,
# overhead still dominated at Honduras scale and the LatAm projection falls with it.
#
# Confounder to hold in mind when reading the result: Brazil spans 4 distinct UTC offsets,
# so build_daily_collection mosaics 4 clipped reductions per day where Honduras did 1.
# Part of any EECU difference is zone count, not cell count — so E1 bounds the LatAm
# projection from ABOVE for a same-zone-count extent, and LatAm itself spans more zones
# still. Treat the resulting rate as conservative, not exact.

# E1a / E1b — export chunk sizing (§9.3). Runs on the HOST like the other probes, so
# --data-root is a host path; it also reads era5_land_base_grid to skip ocean-only
# chunks, and Postgres is published on localhost:5432 rather than the compose hostname.
# Always --dry-run first: it prints the exact chunks and costs nothing.
POSTGRES_HOST=localhost uv run python scripts/gee_chunk_probe.py ladder --dry-run
POSTGRES_HOST=localhost uv run python scripts/gee_chunk_probe.py ladder \
    --sample E1a --data-root .localdata/probe_ladder
# --max-attempts defaults to 2: a chunk that makes EE restart twice is over the line and
# is cancelled rather than waited out. A cancelled chunk is a RESULT, not an error — it
# is how the ladder finds the ceiling, and its record carries attempts + task_id.

# Then the concurrency sweep at whichever size won. Each level gets its own data_root
# (.../p1, /p2, ...) because a manifest hit would otherwise skip the repeat measurement.
POSTGRES_HOST=localhost uv run python scripts/gee_chunk_probe.py parallel \
    --chunk-parents 100 --levels 1 2 4 8 --sample E1b --data-root .localdata/probe_par

# Aggregate + extrapolate to 1950 -> Jul 2026 (536 variable-years). No EE, no cost.
POSTGRES_HOST=localhost uv run python scripts/gee_chunk_probe.py report \
    --data-root .localdata/probe_ladder --also .localdata/probe_par

# E2 / E3 — GEE probes, same instrumented path, no bronze/silver writes.
# NOTE: these run on the HOST, not in a container, so --data-root is a HOST path
# (.localdata/...), not the container's /data/... . The DAG commands above are the
# opposite: those run inside the container and take /data/... .
#
# E2 assumptions verified live 2026-07-30: NOAA/GFS0P25 carries the band
# temperature_2m_above_ground and the properties creation_time / forecast_time /
# forecast_hours; one init = 209 images over forecast_hours 0-384 (exactly 16 days).
# GFS runs 00/06/12/18Z — pass a full timestamp to measure a non-00Z run, since the
# plan's "up to 4x/day" question is about all four.
uv run python scripts/gee_cost_probe.py gfs --init 2026-07-30T12:00:00 \
    --extent 12.0 -90.0 17.5 -83.0 --sample E2 \
    --data-root .localdata/calib_e2

# The probe exports ONE band. The real GFS layer needs ~5 variables, and per-variable
# cost is ~linear in band count — so multiply the measured EECU by the variable count,
# then by the daily (or 4x-daily) cadence, before comparing against the monthly quota.
uv run python scripts/gee_cost_probe.py chirps --year 2020 \
    --extent 12.0 -90.0 17.5 -83.0 --sample E3 \
    --data-root .localdata/calib_e3

# C1 / C2 — SEAS5. ALWAYS --dry-run first: every constant in the script's DEFAULTS block
# is an assumption until C1 confirms it against the live CDS catalogue.
uv run python scripts/cds_cost_probe.py c1 --extent 12.0 -90.0 17.5 -83.0 --dry-run
uv run python scripts/cds_cost_probe.py c1 --extent 12.0 -90.0 17.5 -83.0
uv run python scripts/cds_cost_probe.py c2 --extent 12.0 -90.0 17.5 -83.0 --init-month 8
```

Read the records back:

```bash
python -c "from src.gee.metrics import iter_records; \
  [print(r['sample'], r['variable'], r['eecu_hours'], r['bytes_remote'], r['n_units']) \
   for r in iter_records('.localdata/calib_uy/bronze/_gee_metrics.jsonl')]"
```

E1 auto-triggers `transform_silver`, so `R_s` / `D_s` come from Postgres afterwards — **not** from `bronze_rows`:

```sql
SELECT count(*) FROM wth_base;              -- R_s

-- D_s. NOT pg_total_relation_size('wth_base') — wth_base is a partitioned parent and
-- that returns 0 bytes; the data lives in the child partitions.
SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid)))
FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid
WHERE i.inhparent = 'wth_base'::regclass;
```

Measured 2026-07-30: **16.27 M rows in 2,099 MB → ~129 B/row** (`disk_per_row`), for Uruguay 1980–2026 at 7 variables. Silver disk is ~5.8× the bronze Parquet it came from.

⚠ **Bronze-only samples still write to silver.** `download_bronze_gee` auto-triggers `transform_silver` (`kick_transform`), and `data_root` scopes *bronze* only — silver is one database. A single-variable calibration run therefore lands partial rows (one variable populated, the rest NULL) in `wth_base`. Either run calibration with all 7 variables, or delete the partial rows afterwards:

```sql
DELETE FROM wth_base
 WHERE ingested_at > now() - interval '40 minutes'
   AND tmax IS NOT NULL AND tmin IS NULL AND precip IS NULL;
```

### 9.2 Results

`N_s`/`E_s`/`B_s`/`T_s` come from `_gee_metrics.jsonl`; `R_s`/`D_s` from the two queries above.

**E0 — mined from history, 2026-07-30.** No run, no EECU, no egress spent. Sources: GCS object metadata, `.localdata/hn` Parquet row counts, and `ee.data.listOperations()` (which reported `batchEecuUsageSeconds` for 549 of 554 operations).

| Sample | Date run | Extent | `N_s` (cell-days) | `E_s` (EECU-h) | `B_s` (GB) | Cost | Notes |
|---|---|---|---|---|---|---|---|
| E0-HN | 2026-07-23/24 | Honduras, 341 land cells, 1950–2026 × 7 vars (539 var-years) | 66,735,746 | **17.41** | 0.364 | $0.044 | 5.46 B/cell-day; **2.61e-7 EECU-h/cell-day**; per-task EECU 0.0170–0.0516 (**3× spread at constant work** → overhead-dominated) |
| E0-LatAm | 2026-06-27 | LatAm, **30,303 land cells**, 1 yr × 7 vars | 77,636,286 | **2.45** (from `docs/runbook.md`; no longer in `listOperations`) | 0.371 | $0.045 | ×46 yr → **113 EECU-h**, 17.1 GB, $2.05. `eecu_per_unit` **3.16e-8**, `bytes_per_unit` **4.77** — vs Honduras 2.61e-7 / 5.46 |

`R_s`/`D_s` were not captured for E0 (silver holds Uruguay, not the `hn` root). They are not on the critical path: both scale with cells × days and neither has ever been the constraint.

**Still to run:**

| Sample | Date run | Extent | `N_s` | `E_s` (EECU-h) | `B_s` (GB) | `T_s` (min) | `R_s` (rows) | `D_s` (GB) | Notes |
|---|---|---|---|---|---|---|---|---|---|
| E1 (Brazil) | **2026-07-30 — ATTEMPTED, CANCELLED** | `[-34.0,-74.0,5.5,-34.75]` | — | — | — | >20 min/var, incomplete | — | — | **Blocked: see §9.3.** All 7 exports cancelled after repeated >20-minute attempts |
| E1a (chunk ladder) | 2026-07-31 | same, tmin 2020, 25/100/400 parents | 9 chunks | 0.060 / 0.064 / 0.167 median per task | — | 213 / 218 / 304 s median | — | — | **All 9 completed, `attempts=1`.** Per-task EECU is ~fixed; see §9.4 |
| E1b (concurrency) | 2026-07-31 | same, 100 parents × 8 chunks × 4 levels | 32 chunks | — | — | 17.5 → 54.1 tasks/h (p1 → p8) | — | — | EE throttles to 4.4 in flight at level 8, net 3.1×; **set `gee_pool` = 4** |
| E1c (bigger rungs) | 2026-07-31 | same, tmin 2020, 900/1600 parents | 5 chunks | 0.327 / 0.812 median | — | 1,388 / 2,009 s median | — | — | **No ceiling found** — 19,518 cells completed at `attempts=1`, larger than the extent that failed. See §9.4 |
| SMOKE | 2026-07-30 | `[14.0,-88.0,15.0,-87.0]`, tmax 2020 | 9,150 | 0.0157 | 0.00014 | 1.4 | — | — | 25 cells. `eecu_per_unit` **1.71e-6** — 6.5× worse than Honduras, confirming the overhead floor steepens as extent shrinks. `compression_ratio` **0.25** (a 366-band COG of 25 pixels is mostly header) |

### 9.3 ⚠ E1 is blocked on export wall-clock, not on cost

The Brazil attempt (2026-07-30) was cancelled after repeated tries each exceeding ~20 minutes per variable without completing. This is a **throughput** problem, not a cost one — nothing in §2 or §6 changed — but it blocks the one measurement still outstanding.

What the surrounding data says about the cause:

| Extent | Cells | Bands | Export wall-clock |
|---|---|---|---|
| Smoke | 25 | 366 | 83 s (76 s of it EE compute) |
| Honduras | 341 | 366 | minutes (539 var-years completed in ~2 days) |
| Brazil | ~12,800 | 366 | **>20 min, did not complete** |

The export submits **one image with 366 bands** (`_to_multiband` → `toBands`), so EE computes a full year of daily reductions in a single task, and Brazil additionally mosaics **4 timezone zones per day**. Cost per unit is fine; the single-task granularity is what does not scale.

**Why one big export cannot work, stated precisely** (2026-07-31). The Brazil bbox is 39.5° × 39.25°, i.e. **158 × 157 pixels** at 0.25°. EE computes in 256 × 256 tiles, so the whole country is **smaller than one tile**: there is no spatial parallelism to be had, and one worker holds the entire year. That worker's working set is `pixels_per_band × bands × zones`, and it dies:

| Run | px/band | bands | zones | px-bands | outcome |
|---|---|---|---|---|---|
| SMOKE (1°×1°) | 25 | 366 | 1 | 9.2 k | ✅ 86 s |
| Honduras | ~375 | 366 | 1 | ~137 k | ✅ ~2 min |
| **Brazil, whole** | 24,806 | 366 | 4 | **36 M** | ❌ EE restarts (`attempt` 2, 3) |

The metrics rows prove the restarts rather than a plain failure: `attempt: 2` and `attempt: 3` with a `start_timestamp_ms` present mean the task *ran* and was restarted by EE, which is what a worker dying looks like. Splitting the extent is therefore not a nicety — it is the only way each piece gets its own worker.

Levers, with the two now built:

- **Split the extent into parent-aligned chunks** — ✅ built, `src/gee/chunks.py`. A chunk is a `k × k` block of parents (`k=10` → 10°×10°) positioned on the canonical grid, so `chunk_id` is a pure function of position and the manifest can resume. `download_variable_year(chunk=...)` writes `<var>_<year>__<chunk_id>.parquet` and marks a **spatial** manifest part, never the whole year.
- **Cap EE's restarts** — ✅ built, `wait_for_task(max_attempts=...)`. Past the cap the task is cancelled and the run recorded with `attempts`, instead of burning a queue slot for six hours. `attempts` is now folded in per *poll*, because a terminal status need not still carry the field.
- **Split the export by month** — 12 tasks of ~30 bands instead of 1 of 366. Still open, and cheaper than it looks only if the per-task overhead floor (~90 s, measured at SMOKE) is small next to the work; at 40 chunks × 12 months it would dominate. Measure before adopting.
- **`int16` with a scale factor** — dropped in §2 as a *cost* measure, but it also cuts bytes moved and may cut export time.
- **Check whether the multi-zone mosaic is the dominant term** — now measurable directly: every record carries `n_zones`, so the ladder separates "too many pixels" from "too many timezone mosaics" without a special run.

### 9.4 E1a / E1b — chunk sizing, MEASURED 2026-07-31

`scripts/gee_chunk_probe.py`, tmin 2020 over the E1 extent, `--max-attempts 2`. Both go through `download_variable_year`, so they wrote real Parquet and appended ordinary records.

**E1a (ladder, serial, 3 chunks per rung):**

| parents/chunk | box | chunks/var-yr | median task | median EECU-h | max attempt | result |
|---|---|---|---|---|---|---|
| 25 | 5°×5° | 62 | 213 s | 0.0601 | 1 | ✅ 3/3 |
| 100 | 10°×10° | 21 | 218 s | 0.0635 | 1 | ✅ 3/3 |
| 400 | 20°×20° | 8 | 304 s | 0.1672 | 1 | ✅ 3/3 |

**Nothing failed.** `attempts=1` at every rung, including 20°×20° chunks of 6,037 cells.

**E1c (2026-07-31) pushed to 900 and 1600 parents, and still found no ceiling:**

| parents/chunk | box | biggest sample | median task | median EECU-h | result |
|---|---|---|---|---|---|
| 900 | 30°×30° | 9,228 cells, 3 zones | 1,388 s | 0.327 | ✅ 3/3 |
| 1600 | 40°×40° | **19,518 cells, 3 zones** | 2,009 s | 0.812 | ✅ 2/2 (3rd interrupted by the operator, not EE) |

> ⚠ **This falsifies the raster-size diagnosis in §9.3.** A 40°×40° chunk is **160×160 = 25,600 raster px**, *larger* than the whole-Brazil bbox (158×157 = 24,806 px) that EE kept restarting — and it completed on the first attempt, as did a 4-zone sibling. Neither pixel count nor zone count alone separates the successes from the failure.

### The limit is `land_cells × zones`, and it is knowable offline

**E1d (2026-07-31)** re-ran the unchunked variable-year with `--max-attempts 2`. It **reproduced exactly**: `attempts=3`, `zones=4`, cancelled by the cap after 3,331 s. So the failure is deterministic, not transient — two independent runs, three attempts each.

Ranking all 48 records by `land_cells × zones` separates them cleanly, with no overlap:

| Export | cells | zones | cells×zones | outcome |
|---|---|---|---|---|
| `s30r003c-002` | 9,228 | 3 | 27,684 | ✅ attempt 1 |
| `s40r003c-002` | 5,585 | 4 | 22,340 | ✅ attempt 1 |
| **`s40r002c-002`** | **19,518** | **3** | **58,554** | ✅ attempt 1 — largest success |
| **whole extent** | **~22,687** | **4** | **~90,748** | ❌ restarted 3×, twice |

Every export at or below 58,554 completed first time; the only one above it failed twice. The threshold lies between them. This explains why the earlier single-variable theories failed: a 19,518-cell export succeeds at 3 zones, and a 4-zone export succeeds at 5,585 cells, but their **product** is what EE's per-worker budget is spent on — `daily.py` mosaics one clipped reduction per zone per band, so zones multiply the work each cell costs.

**Both terms are known before any EE call.** `era5_land_base_grid` carries `is_land` and `t_zone`, and `t_zone` came from the same shapefile that built `GEE_TZ_ASSET` — so `src/db/grid_query.chunk_land_stats` returns exact cell and zone counts per chunk for one SQL query. `scripts/gee_chunk_probe.py` now refuses to submit a chunk above `CELL_ZONE_CEILING = 58_554` (verified against EE: for `s40r002c-002` the grid predicted 3 zones and EE derived 3), and `--force` overrides it. **This is the guard the production DAG needs when chunking is wired in.**

**E1b (concurrency sweep, 8 chunks of 100 parents per level):**

| level | tasks/h | concurrency reached | median task | net vs serial |
|---|---|---|---|---|
| 1 | 13.8 | 0.96 | 213 s | 1.00× |
| 2 | 25.7 | 1.90 | 200 s | 1.87× |
| 4 | 46.1 | 3.07 | 238 s | 3.35× |
| 8 | 54.1 | 4.42 | 248 s | 3.93× |

EE throttles: asking for 8 in flight yields 4.4, and each task slows from 213 s to 248 s. Gains are ~flat past 4. **Set `gee_pool` to 4** — level 8 buys 17% more throughput for double the in-flight load.

**The finding that matters — a task costs a fixed amount plus a per-cell amount.** Least squares over all 47 records:

```
EECU-h  = 0.0512 + 4.33e-5 × cells     (R² 0.56)
seconds =    215 + 0.0869 × cells      (R² 0.33)
```

The fixed term is the 366-band graph over 8,760 hourly source images, paid **per submission**. So chunk size decides how many times you pay it, and the extent's cell count — which barely changes with chunking — decides the rest. Projected over 536 variable-years, using each size's *real* total cell count rather than a sampled median:

| parents/chunk | chunks/var-yr | tasks | EECU-h | quota-months | wall-clock at p8 |
|---|---|---|---|---|---|
| 25 | 62 | 33,237 | 2,167 | 2.17 | 30 d |
| 100 | 21 | 11,258 | 1,102 | 1.10 | 13 d |
| 400 | 8 | 4,289 | 778 | 0.78 | 7.6 d |
| 900 | 6 | 3,216 | 767 | 0.77 | 7.1 d |
| 1600 | 4 | 2,144 | 716 | 0.72 | 6.3 d |
| *unchunked* | *1* | *536* | *~553* | *0.55* | *untested* |

Consequences:

- **Returns flatten hard after 400.** 25 → 400 saves 64% of the EECU; 400 → 1600 saves 8% more while quadrupling per-task wall-clock (304 s → 2,009 s median) and coarsening restart granularity to 4 pieces.
- **Chunking buys resumability with EECU.** §6's 113 EECU-h for all of LatAm assumed **one task per variable-year**; it does not survive chunking. Quote it only for unchunked exports.
- **EECU is the binding constraint for this extent**, not wall-clock: even the best chunked option spends ~72% of a month's Contributor quota on Brazil alone, before LatAm.
- **Beware `median × chunk_count`.** Chunks vary hugely in land content (the 1600 rung sampled 19,518 and 5,585 cells), so a 3-sample median is not the average chunk. The table above is modelled from the fit; `scripts/gee_chunk_probe.py report` does this and prints both.

**Unchunked is off the table** — E1d reproduced the failure, so the ~553 EECU-h row above is unreachable for this extent. Chunking is mandatory, and the question is only how big.

### Decision: 400 parents (20°×20°)

| | 400 | 1600 |
|---|---|---|
| EECU-h (Brazil, full backfill) | 778 | 716 (−8%) |
| wall-clock at p8 | 7.6 d | 6.3 d |
| median task | 304 s | 2,009 s |
| restart granularity | 8 pieces | 4 pieces |
| worst chunk vs ceiling | 11,994 / 58,554 — **4.9× margin** | 57,960 / 58,554 — **1.01×** |

1600 saves 8% of the EECU and sits *one percent* under a ceiling estimated from a single data point, on an extent whose zone count rises as you move toward LatAm. 400 keeps a ~5× margin for 8% more EECU. Take the margin.

Also settled: **`gee_pool` = 4** (E1b, 2.64× net at p4 vs 3.10× at p8 for double the in-flight load).

**Remaining strategic problem — quota, not throughput.** 778 EECU-h is 78% of one month's Contributor quota for **Brazil alone**. LatAm spans more cells and more zones, so it needs either several months of quota, a smaller variable set, or a shorter start year. That is now the binding constraint on scope, and §6's continental projection needs redoing on the chunked model before any LatAm commitment.

Two smaller notes from the run: exported `cells` differs from the grid's `land_cells` by a few per chunk (LSIB polygon clip vs the ERA5-Land-derived `is_land` mask — expected, not a bug), and `n_zones` was 1–3 per chunk against 4 for whole-Brazil, so chunking also cuts the per-band mosaic term.

**This no longer blocks the cost model** — but it did change it. `eecu_per_unit` at continental scale was measured by `bench-latam` (3.16e-8 → ~113 EECU-h for the full backfill, §6) **with one task per variable-year**. §9.4 shows per-task EECU is dominated by a fixed ~0.035 EECU-h term, so splitting a variable-year into 21 chunks multiplies that term 21×: the same Brazil backfill costs ~715 EECU-h chunked. Wall-clock and EECU now trade directly against each other, and the 113 figure applies only to exports that are not split.
| E2 | | | | | | | | | |
| E3 | | | | | | | | | |
| C1 | | | | | | | | | |
| C2 | | | | | | | | | |
| S1 | | | | | | | | | |
| S2 | | | | | | | | | |
| S3 | | | | | | | | | |
| D1 | | | | | | | | | |

---

## 10. Known unknowns

Honest list of what this model cannot yet predict:

1. ✅ **CLOSED — GeoTIFF compression.** Measured indirectly at 5.46 B/cell-day (Honduras) and 4.77 B/cell-day (LatAm). The instrumentation now separates true `compression_ratio` (against *raster* pixels) from `land_fraction`, because the first version conflated them and made a sea-heavy bbox read as `0.80×`.
2. ✅ **MOOT — EE direct-download vs GCS egress billing.** Only mattered as a mitigation for a $1,700 bill that turned out to be $2.
3. ✅ **MOOT — free-tier egress allowance.** At 17 GB continent-wide the answer changes nothing.
4. **EECU accounting for GFS** — a forecast collection with more timesteps per day than ERA5 may reduce differently than the linear model assumes. → E2. **Still open.**
5. **Autovacuum behaviour on a Pi** under daily 10⁷-row TRUNCATE+COPY cycles. → D1. **Still open** (deferred).
6. **IRI Data Library rate limits** for the plume — no published figure known; treat as best-effort.
7. ✅ **CLOSED — Contributor tier.** Already granted: 1,000 EECU-h/month. This also removes it as a *remedy*, since it can no longer be traded for headroom.
8. ✅ **CLOSED — where the EECU curve flattens.** The `bench-latam` calibration measures 3.16e-8 EECU-h/cell-day at continental scale vs 2.61e-7 at Honduras scale (8.3× amortization), so LatAm is ~113 EECU-h. Only the *intermediate* shape of the curve is unsampled, and nothing depends on it. Briefly the "dominant open unknown" in this document because a pre-existing measurement was overlooked.
9. **⚠ NEW — large-extent export wall-clock.** A Brazil-sized variable-year does not complete in a workable time as a single 366-band export task (§9.3). This blocks E1 and, by extension, any continental backfill — independently of cost, which is settled. Being addressed in a dedicated session on efficient large-extent downloads.
10. **⚠ NEW — Pi hardware stability.** Three unexplained hard freezes in two days (2026-07-29 10:30, 2026-07-30 03:03, 2026-07-30 14:28), no OOM / thermal / under-voltage / PCIe trace in any journal. Unattended multi-hour runs are unreliable until this is understood. Bronze is idempotent per `(variable, year)`, so a freeze costs at most one variable-year on resume — but a *hard* freeze writes no metrics record at all, so interrupted samples must be re-run into a fresh `data_root`.

---

## 11. Bottom line

Rewritten 2026-07-30, after E0 replaced the estimates with measurements.

- **Nothing here costs meaningful money.** The full 46-year Latin America ERA5 backfill egresses **17.1 GB ≈ $2.05**. The complete Honduras archive that already exists cost **$0.044**. EECU is quota-limited, not billed; CDS and every HTTP source are free; the Pi is sunk cost.
- **§2's $1,700 egress risk was a unit error** (28 GB written as 28 TB), and the mitigations it motivated — `int16`, `toDrive`, direct-download — are all withdrawn. This document's most valuable output so far is the deletion of its own headline finding.
- **EECU is settled too.** The LatAm 46-year backfill is **~113 EECU-h** — 11% of one month's Contributor quota — measured, not extrapolated. Small extents are 8.3× worse per unit because fixed per-task cost dominates; that is a reason to prefer *fewer, larger* jobs, not a constraint on the total.
- **The one genuinely open item is export wall-clock at scale** (§9.3), which is a throughput problem, not a cost or quota one.
- **The one real scheduling exposure remains GFS's recurring monthly EECU**, because unlike a backfill it cannot be split across months. Quota is 1,000 EECU-h/month and Contributor tier is already in use, so there is no upgrade left as a remedy. → E2.
- **Method note worth carrying forward:** E0 cost nothing. Object metadata, Parquet row counts, and `ee.data.listOperations()` answered at country and continental scale what was scheduled as an hour of paid runs. On any project with history, mine it before you measure it — but mine it *early*, since EE retains operations only about a week.

Next action: run **E1 at Central America extent** (§9.1) and **E2 (GFS)**. Everything else in this document is either measured or deferred.
