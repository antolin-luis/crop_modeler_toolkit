# Using Google Earth Engine in 2026 (noncommercial / student)

> Setup guide for the **GEE bronze backend** (`src/gee/`, `download_bronze_gee` DAG), an
> alternative to the CDS path that is dramatically faster for long backfills. If you last
> used Earth Engine in the "just paste an API key" era — that flow is **gone**. This page
> covers the current Cloud-project + OAuth/service-account model end to end.

## 0. Why bother (and is it free?)

The CDS is slow not because of bandwidth but because each request waits in a shared queue
and stages ERA5 from MARS **tape**. GEE serves ERA5 from hot storage and reduces
hourly→daily **server-side**, so only the small daily result leaves Google. For a multi-
decade backfill this is the difference between days and hours.

**Cost for a student: $0.** Noncommercial use (students, academic research, education,
nonprofits) runs on a free monthly quota of *Earth Engine Compute Units* (EECU-hours):

| Tier | Free EECU-hours / month |
|------|--------------------------|
| Community (default) | 150 |
| Contributor | 1,000 |

You never enter a card. Exceeding the quota does not bill you — tasks keep running in a
throttled "restricted mode". The only paid track is *commercial* use (billed at
$0.40/Batch EECU-hour), which is not what you're doing.

- Pricing & terms: <https://cloud.google.com/earth-engine/pricing>
- Noncommercial tiers: <https://developers.google.com/earth-engine/guides/noncommercial_tiers>

> Terms shift over time — confirm the current tiers when you register.

## 1. What changed since the API-key days

| Then | Now (2026) |
|------|------------|
| One API key string | A **Google Cloud project** registered for Earth Engine |
| Key in the client | **OAuth** (interactive, local dev) or a **service account** (headless) |
| — | The **Earth Engine API** must be explicitly *enabled* on the project |

Everything below is a **one-time** setup. Your code then just calls
`src.gee.client.GEEClient()`.

## 2. Create and register a Cloud project

1. Create a project: <https://console.cloud.google.com/projectcreate>. Naming convention
   is `ee-<your-username>`. You must be Owner/Editor.
2. **Register it for noncommercial Earth Engine use:**
   <https://code.earthengine.google.com/register> → choose **Unpaid / Noncommercial** →
   pick the project you just made. (If you've never used EE on this Google account, this is
   also where you accept the EE terms.)
3. **Enable the Earth Engine API** on the project:
   <https://console.cloud.google.com/apis/library/earthengine.googleapis.com> → *Enable*.
   Skipping this gives `Earth Engine API has not been used in project … or it is disabled`.
4. Confirm the project shows a Community/Contributor tier on its EE settings page.

Put the project id in `.env`:

```
GEE_PROJECT=ee-yourusername
```

## 3. Local-dev auth (interactive OAuth)

For running scripts / the calibration smoke from your own machine:

```bash
uv run earthengine authenticate     # opens a browser, pick the ee-… project
```

Credentials are saved to `~/.config/earthengine/credentials` and picked up automatically
by `ee.Initialize(project=…)`. Quick check:

```bash
uv run python -c "import ee; ee.Initialize(project='ee-yourusername'); print(ee.Number(1).getInfo())"
# -> 1
```

Leave `GEE_SERVICE_ACCOUNT_FILE` blank in `.env` to use this mode.

## 4. Headless / Airflow auth (service account)

The interactive browser flow can't run inside the Docker container or on the headless Pi,
so the DAG authenticates with a **service account** instead.

1. Create the SA:
   <https://console.cloud.google.com/iam-admin/serviceaccounts> → *Create service account*
   in your `ee-…` project. Grant roles **Earth Engine Resource Writer** and
   **Storage Object Admin** (the latter for the GCS export bucket).
2. Create a **JSON key** for the SA and download it. Store it **outside the repo**
   (e.g. `/run/secrets/ee-gee-key.json` or a path bind-mounted into the container). It is a
   credential — never commit it. `.gitignore` already blocks `*service-account*.json`,
   `*-gee-key*.json`, `*.gee.json`, but the safest habit is to keep it out of the tree.
3. **Register the SA for Earth Engine** (a separate step from project registration):
   <https://code.earthengine.google.com/register> → register the *service account email*
   under your noncommercial project. An unregistered SA gets permission errors even with
   the API enabled.
4. Point `.env` at the key:

```
GEE_SERVICE_ACCOUNT_FILE=/run/secrets/ee-gee-key.json
```

When this is set, `GEEClient` uses `ee.ServiceAccountCredentials` automatically.

## 5. GCS export bucket

The backend exports each daily raster to Google Cloud Storage, then downloads it. Create a
bucket once:

```bash
# Pick a region near the machine that downloads. This project's bucket is
# southamerica-east1 (São Paulo); `--location=US` is a multi-region and only a sensible
# default if you are in North America.
gcloud storage buckets create gs://your-era5-bronze-export --location=southamerica-east1
```

Set in `.env`:

```
GEE_GCS_BUCKET=your-era5-bronze-export
GEE_GCS_PREFIX=bronze-gee
```

Cost hygiene: add a **lifecycle rule** to auto-delete objects after a few days so exported
GeoTIFFs don't accumulate (the bronze Parquet on your SSD is the real artifact — the tiffs
are transient):

```bash
printf '{"rule":[{"action":{"type":"Delete"},"condition":{"age":3}},{"action":{"type":"AbortIncompleteMultipartUpload"},"condition":{"age":3}}]}' > /tmp/lc.json
gcloud storage buckets update gs://your-era5-bronze-export --lifecycle-file=/tmp/lc.json
```

**Verify it took** — the two actions are not interchangeable, and applying only the second
silently leaves every completed object in place forever:

```bash
gcloud storage buckets describe gs://your-era5-bronze-export --format="yaml(lifecycle_config)"
# must show action type Delete, not only AbortIncompleteMultipartUpload
```

## 6. Timezone polygon asset (required before any GEE download)

The GEE backend reduces each cell on its **own** local day (§5.3), so it has **no `timezone`
DAG param** — it derives one reduction zone per UTC offset present in the extent from a
timezone-polygon FeatureCollection asset. Build it once from the same political tz shapefile
that stamps the grid's `t_zone`, so the two agree, and point `GEE_TZ_ASSET` at it. Without it
`download_bronze_gee` raises `GEE_TZ_ASSET is unset`.

1. Download the boundary set — use the **with-oceans** build so the polygons tile the globe
   with no coastal gaps (a gap would drop boundary cells from the mosaic):

   ```bash
   curl -L -o tz-oceans.zip \
     https://github.com/evansiroky/timezone-boundary-builder/releases/latest/download/timezones-with-oceans.geojson.zip
   unzip tz-oceans.zip           # -> combined-with-oceans.json
   ```

2. Dissolve it by standard (non-DST) UTC offset into a **zipped shapefile** — EE table
   ingestion accepts `.shp`/`.zip`, not GeoJSON. `geopandas` is pulled in just for this
   maintainer script:

   ```bash
   uv run --with geopandas python scripts/build_tz_asset.py \
     --input combined-with-oceans.json --output tz_by_offset.zip
   ```

3. Ingest it as an EE table asset under your project — reuses the pipeline's service-account
   auth, so no separate `earthengine`/`gcloud` login:

   ```bash
   uv run python scripts/ingest_tz_asset.py --input tz_by_offset.zip
   ```

   It uploads the zip to your GCS bucket, starts the ingestion, polls to `SUCCEEDED`, and
   prints the asset id.

4. Set it in `.env`:

   ```
   GEE_TZ_ASSET=projects/<GEE_PROJECT>/assets/tz_by_offset
   ```

The build inputs/outputs (`combined-with-oceans.json`, `tz_by_offset.zip`, ~50–170 MB) are
git-ignored — regenerate them, never commit. This is a one-off; every GEE run reuses the
asset.

## 7. `.env` summary

```
GEE_PROJECT=ee-yourusername
GEE_SERVICE_ACCOUNT_FILE=        # blank = local OAuth; path = headless SA
GEE_GCS_BUCKET=your-era5-bronze-export
GEE_GCS_PREFIX=bronze-gee
GEE_TZ_ASSET=projects/ee-yourusername/assets/tz_by_offset   # tz zones (§6); required for GEE
```

All are optional for CDS-only users; `GEEClient` validates the ones it needs at use time, and
`download_bronze_gee` checks `GEE_TZ_ASSET` at trigger time. **Never** put key material in
`.env` itself — only the *path* to the SA JSON.

## 8. Quota, cost, and sizing a real backfill

- **Read your usage:** every run now self-reports. One JSON record per `(variable, year)`
  lands in `<bronze_dir>/_gee_metrics.jsonl` with `eecu_hours`, `bytes_remote`, wall-clock
  and `n_units` — see `docs/cost_model_climate_context.md` §4.1 for the schema and
  `src/gee/metrics.py` for the code. The EE task list
  (<https://code.earthengine.google.com/tasks>) and the project's EECU quota page remain
  the fallback: EE does not always report `batch_eecu_usage_seconds`, in which case
  `eecu_hours` is `null` (never a fake `0.0`) and the record's `task_id` is the handle for
  looking it up by hand.
- **Calibrate before scaling.** Run **one year × one country × all variables** first
  (see `docs/step3b_bronze_download_gee.md` → Verification), read the actual EECU-hours,
  and extrapolate linearly (cost scales ~ cells × days). Don't trust a-priori estimates.
- **⚠ EECU is not the only meter.** Every byte exported to GCS and pulled to the Pi is
  billed network egress, which the §5 lifecycle rule does not touch — it bounds *storage*
  only. Egress scales linearly with extent and is invisible at single-country scale;
  `bytes_remote` in the metrics JSONL is what makes it visible before a continental
  backfill. Set a GCP budget alert. See `docs/cost_model_climate_context.md` §2.
- **Measured, 2026-07-30:** the complete **Honduras** backfill (341 land cells, 1950–2026,
  7 variables — 539 export tasks) cost **17.41 EECU-h** and **364 MB** of egress (≈$0.04).
  ⚠ Do not scale that per-cell rate up naively: those tasks ranged 0.0170–0.0516 EECU-h for
  *identical* work, a 3× spread, meaning at country scale you are measuring per-task
  overhead rather than compute.
- **Latin America, full record:** ~50–65k land cells × ~16,800 days × 7 variables projects
  to **~1,840 EECU-h** by linear extrapolation from Honduras — but per the caveat above,
  treat that as an **upper bound**. Egress at the same scale is **measured** at 17.1 GB
  (≈$2). With the **Contributor tier (1,000 EECU-h/month, already active on this project)**
  that is ~2 months of quota, so **split the year range across runs/months** (the DAG's
  `start_year`/`end_year` make this trivial). See `docs/cost_model_climate_context.md` §6.
- Commercial-equivalent value, for reference only: ~$0.40 × EECU-hours (Batch rate).
- **Mine history before you measure.** `ee.data.listOperations()` reports
  `batchEecuUsageSeconds` per past task, and GCS object sizes give egress — together they
  can answer the sizing question for free. EE retains operations only ~1 week, so read them
  soon after a backfill.

## See also
- `docs/step3b_bronze_download_gee.md` — how the backend is built and how to run it.
- `docs/step3_bronze_download.md` — the original CDS backend (still supported).
