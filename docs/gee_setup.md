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
gcloud storage buckets create gs://your-era5-bronze-export --location=US
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
printf '{"rule":[{"action":{"type":"Delete"},"condition":{"age":3}}]}' > /tmp/lc.json
gcloud storage buckets update gs://your-era5-bronze-export --lifecycle-file=/tmp/lc.json
```

## 6. `.env` summary

```
GEE_PROJECT=ee-yourusername
GEE_SERVICE_ACCOUNT_FILE=        # blank = local OAuth; path = headless SA
GEE_GCS_BUCKET=your-era5-bronze-export
GEE_GCS_PREFIX=bronze-gee
```

All four are optional for CDS-only users; `GEEClient` validates the ones it needs at use
time. **Never** put key material in `.env` itself — only the *path* to the SA JSON.

## 7. Quota, cost, and sizing a real backfill

- **Read your usage:** the EE task list (<https://code.earthengine.google.com/tasks>) and
  the project's EECU quota page show EECU-hours consumed per export.
- **Calibrate before scaling.** Run **one year × one country × all variables** first
  (see `docs/step3b_bronze_download_gee.md` → Verification), read the actual EECU-hours,
  and extrapolate linearly (cost scales ~ cells × days). Don't trust a-priori estimates.
- **Latin America, full record (rough):** ~50–65k land cells × ~16,800 days × 7 variables
  is plausibly a few hundred to ~1–2k EECU-hours total. As a student that is **free**, but
  it likely exceeds the 150/month Community quota — either:
  - upgrade to the **Contributor tier** (1,000/month, still free), and/or
  - **split the year range across runs/months** (the DAG's `start_year`/`end_year` make
    this trivial), staying under the monthly quota.
- Commercial-equivalent value, for reference only: ~$0.40 × EECU-hours (Batch rate).

## See also
- `docs/step3b_bronze_download_gee.md` — how the backend is built and how to run it.
- `docs/step3_bronze_download.md` — the original CDS backend (still supported).
