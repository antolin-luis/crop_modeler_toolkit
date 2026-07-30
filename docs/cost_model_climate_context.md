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

**One caveat that matters more than the totals:** bytes per useful cell-day is **not extent-invariant**. Honduras measures 5.46 B/cell-day; LatAm implies ~2.1 B/cell-day (at ~60k land cells). A small bbox pays fixed per-shard overhead spread over few useful values, and its land fraction is worse. **Do not extrapolate a small extent's per-unit rate to a large one** — that is the error the corrected model must not repeat in the other direction.

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
| derived | `bytes_per_unit`, `eecu_per_unit`, `compression_ratio`, `land_fraction` |
| context | `sample`, `kind`, `dataset`, `variable`, `year`, `extent`, `land_only`, `b`, `chunk_days`, `host`, `gee_project`, `run_id`, `started_at`, `finished_at`, `parquet_path`, `parquet_bytes`, `error` |

Three properties of the record worth knowing before reading one:

- **`eecu_hours` is `null`, never `0.0`, when EE omits `batch_eecu_usage_seconds`.** The field is not guaranteed on every terminal state. A silent zero would corrupt `eecu_per_unit` — the number this whole exercise exists to produce — so an honest null is recorded and `task_id` is the handle for reading the value off the EE task list by hand.
- **`bronze_rows` is not `R_s`.** Silver is wide: ~7 bronze variable-rows merge into one `wth_base` row. Using it as `rows_per_unit` inflates that rate 7×. `R_s` / `D_s` stay a Postgres query (§9).
- **A failed run still writes a record**, carrying `task_id`, `t_export_s` and `error` — which is precisely what you want when the failure *is* a quota event.
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
| Latin America | ~50–65k | ~175× | **~1,840 EECU-h if linear — see warning** | **17.1 GB** (measured) | **$2.05** |

**The egress column is settled; the EECU column is not.** The LatAm egress figure is a direct measurement (`bench-latam`, ×46 years), not an extrapolation. The LatAm EECU figure *is* an extrapolation — `2.61e-7 EECU-h/cell-day` from Honduras × `7.06e9` cell-days — and it is the least trustworthy number in this document, for the reason in §4: at Honduras scale the meter reads fixed overhead, with a 3× spread across identical tasks. Treat ~1,840 EECU-h (≈2 months of quota) as an **upper bound**, because overhead-dominated samples over-state the marginal rate. E1 replaces it.

**Read the multiplier column before the estimates.** LatAm is ~175× Honduras, not ~400×.

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
| **E1 `eecu_per_unit` ≥ E0-HN's 2.61e-7** | Overhead is *not* dominating after all — LatAm really is ~1,840 EECU-h ≈ 2 months of quota. Split the backfill by year range across months |
| **E1 `eecu_per_unit` still falling at CentAm scale** | Sample once more at Mexico + CentAm before projecting; the curve has not flattened |
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
| E0-LatAm | 2026-06-27 | LatAm, 1 yr × 7 vars | — | *not retained by `listOperations`* | 0.371 | $0.045 | ×46 yr → 17.1 GB / $2.05. Implies ~2.1 B/cell-day at ~60k cells vs Honduras's 5.46 → **per-unit rate is not extent-invariant** |

`R_s`/`D_s` were not captured for E0 (silver holds Uruguay, not the `hn` root). They are not on the critical path: both scale with cells × days and neither has ever been the constraint.

**Still to run:**

| Sample | Date run | Extent | `N_s` | `E_s` (EECU-h) | `B_s` (GB) | `T_s` (min) | `R_s` (rows) | `D_s` (GB) | Notes |
|---|---|---|---|---|---|---|---|---|---|
| E1 (Brazil) | **2026-07-30 — ATTEMPTED, CANCELLED** | `[-34.0,-74.0,5.5,-34.75]` | — | — | — | >20 min/var, incomplete | — | — | **Blocked: see §9.3.** All 7 exports cancelled after repeated >20-minute attempts |
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

Levers worth evaluating before retrying E1 (**not yet investigated — deferred to a dedicated session**):

- **Split the export by month** — 12 tasks of ~30 bands instead of 1 of 366. More tasks, each far smaller, and they parallelise under `gee_pool`.
- **Split by variable-month** rather than variable-year in the DAG's `_plan`, making the manifest chunk-aware like the CDS path already is (`src/cds/manifest.py` tracks sub-chunks for exactly this reason).
- **`int16` with a scale factor** — dropped in §2 as a *cost* measure, but it also cuts bytes moved and may cut export time.
- **Check whether the 4-zone mosaic is the dominant term** by running Brazil with a single-offset extent of similar cell count.

Until this is resolved, `eecu_per_unit` at scale stays unmeasured and every LatAm EECU figure in this document remains an upper bound (§6).
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

1. ✅ **CLOSED — GeoTIFF compression.** Measured indirectly at 5.46 B/cell-day (Honduras) and ~2.1 B/cell-day (LatAm). The instrumentation now separates true `compression_ratio` (against *raster* pixels) from `land_fraction`, because the first version conflated them and made a sea-heavy bbox read as `0.80×`.
2. ✅ **MOOT — EE direct-download vs GCS egress billing.** Only mattered as a mitigation for a $1,700 bill that turned out to be $2.
3. ✅ **MOOT — free-tier egress allowance.** At 17 GB continent-wide the answer changes nothing.
4. **EECU accounting for GFS** — a forecast collection with more timesteps per day than ERA5 may reduce differently than the linear model assumes. → E2. **Still open.**
5. **Autovacuum behaviour on a Pi** under daily 10⁷-row TRUNCATE+COPY cycles. → D1. **Still open** (deferred).
6. **IRI Data Library rate limits** for the plume — no published figure known; treat as best-effort.
7. ✅ **CLOSED — Contributor tier.** Already granted: 1,000 EECU-h/month. This also removes it as a *remedy*, since it can no longer be traded for headroom.
8. **⚠ NEW — where the EECU curve flattens.** The dominant open unknown. Honduras-scale tasks show a 3× EECU spread at constant workload, so the per-cell-day rate there is overhead, not compute. Without a mid-scale sample, every LatAm EECU projection in this document is an upper bound of unknown tightness. → E1.
9. **⚠ NEW — large-extent export wall-clock.** A Brazil-sized variable-year does not complete in a workable time as a single 366-band export task (§9.3). This blocks E1 and, by extension, any continental backfill — independently of cost, which is settled. Being addressed in a dedicated session on efficient large-extent downloads.
10. **⚠ NEW — Pi hardware stability.** Three unexplained hard freezes in two days (2026-07-29 10:30, 2026-07-30 03:03, 2026-07-30 14:28), no OOM / thermal / under-voltage / PCIe trace in any journal. Unattended multi-hour runs are unreliable until this is understood. Bronze is idempotent per `(variable, year)`, so a freeze costs at most one variable-year on resume — but a *hard* freeze writes no metrics record at all, so interrupted samples must be re-run into a fresh `data_root`.

---

## 11. Bottom line

Rewritten 2026-07-30, after E0 replaced the estimates with measurements.

- **Nothing here costs meaningful money.** The full 46-year Latin America ERA5 backfill egresses **17.1 GB ≈ $2.05**. The complete Honduras archive that already exists cost **$0.044**. EECU is quota-limited, not billed; CDS and every HTTP source are free; the Pi is sunk cost.
- **§2's $1,700 egress risk was a unit error** (28 GB written as 28 TB), and the mitigations it motivated — `int16`, `toDrive`, direct-download — are all withdrawn. This document's most valuable output so far is the deletion of its own headline finding.
- **The one genuinely open question is where the EECU curve flattens.** Honduras-scale tasks vary 3× at constant workload, so they measure overhead, not compute. Every LatAm EECU projection here (~1,840 EECU-h ≈ 2 months of quota) is an upper bound until E1 runs at Central America scale.
- **The one real scheduling exposure remains GFS's recurring monthly EECU**, because unlike a backfill it cannot be split across months. Quota is 1,000 EECU-h/month and Contributor tier is already in use, so there is no upgrade left as a remedy. → E2.
- **Method note worth carrying forward:** E0 cost nothing. Object metadata, Parquet row counts, and `ee.data.listOperations()` answered at country and continental scale what was scheduled as an hour of paid runs. On any project with history, mine it before you measure it — but mine it *early*, since EE retains operations only about a week.

Next action: run **E1 at Central America extent** (§9.1) and **E2 (GFS)**. Everything else in this document is either measured or deferred.
