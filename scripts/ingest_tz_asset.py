"""Ingest the tz-by-offset GeoJSON into an Earth Engine table asset (sets up GEE_TZ_ASSET).

**Maintainer one-off, not on the pipeline path.** Reuses the pipeline's service-account auth
(`GEEClient`) so no separate `earthengine` / `gcloud` CLI login is needed — the same creds
that already run the GEE bronze backend. It:

1. uploads the local GeoJSON (from `scripts/build_tz_asset.py`) to the configured GCS bucket,
2. starts an Earth Engine table ingestion into `projects/<GEE_PROJECT>/assets/<asset-name>`,
3. polls the ingestion task to completion.

Then set `GEE_TZ_ASSET` in `.env` to the printed asset id. Run::

    uv run python scripts/ingest_tz_asset.py --input tz_by_offset.zip

The input must be a zipped shapefile (or ``.shp``) — EE table ingestion rejects GeoJSON.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import ee

from src.config import load_config
from src.gee.client import GEEClient


def _upload_to_gcs(local_path: str, bucket: str, blob_name: str, sa_file: str | None) -> str:
    from google.cloud import storage

    client = (
        storage.Client.from_service_account_json(sa_file)
        if sa_file
        else storage.Client()
    )
    client.bucket(bucket).blob(blob_name).upload_from_filename(local_path)
    return f"gs://{bucket}/{blob_name}"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="tz_by_offset.zip")
    ap.add_argument("--asset-name", default="tz_by_offset")
    ap.add_argument("--overwrite", action="store_true", help="replace an existing asset")
    ap.add_argument("--poll", type=float, default=5.0)
    args = ap.parse_args()

    cfg = load_config()
    client = GEEClient()  # initializes ee with the service account
    bucket = client.require_bucket()
    asset_id = f"projects/{client.project}/assets/{args.asset_name}"

    # Preserve the input's extension — EE requires the GCS URI to end in .shp or .zip.
    blob_name = f"{args.asset_name}{Path(args.input).suffix}"
    gs_uri = _upload_to_gcs(
        args.input, bucket, blob_name, cfg.gee.service_account_file
    )
    print(f"uploaded -> {gs_uri}", flush=True)

    request_id = ee.data.newTaskId(1)[0]
    params = {"id": asset_id, "sources": [{"uris": [gs_uri]}]}
    op = ee.data.startTableIngestion(request_id, params, allow_overwrite=args.overwrite)
    # New-style ingestion returns a Cloud operation (name = projects/.../operations/...).
    op_name = op.get("name") if isinstance(op, dict) else None
    print(f"ingestion started -> {asset_id} (op {op_name or request_id})", flush=True)

    _DONE = {"SUCCEEDED", "FAILED", "CANCELLED", "COMPLETED"}
    while True:
        if op_name:
            state = ee.data.getOperation(op_name).get("metadata", {}).get("state", "UNKNOWN")
        else:  # fallback: legacy task view
            state = ee.data.getTaskStatus(request_id)[0].get("state", "UNKNOWN")
        print(f"  state: {state}", flush=True)
        if state in _DONE:
            if state not in ("SUCCEEDED", "COMPLETED"):
                raise SystemExit(f"ingestion {state}")
            break
        time.sleep(args.poll)

    print(f"\nDONE. Set in .env:\n  GEE_TZ_ASSET={asset_id}", flush=True)


if __name__ == "__main__":
    main()
