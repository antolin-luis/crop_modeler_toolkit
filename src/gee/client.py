"""Earth Engine auth wrapper (the GEE analogue of ``src/cds/client.py``).

Thin shim that initializes Earth Engine from ``GEEConfig`` so the rest of the toolkit never
touches credentials directly. Two auth modes (see ``docs/gee_setup.md``):

- **Service account** (headless / Airflow): ``GEE_SERVICE_ACCOUNT_FILE`` points at the SA
  JSON key. Non-interactive — the only mode that works inside the container / on the Pi.
- **User OAuth** (local dev): no SA file; uses the credentials stored by
  ``earthengine authenticate`` under ``~/.config/earthengine/credentials``.

``GEE_PROJECT`` (the registered noncommercial Cloud project) is required in both modes.
Earth Engine's auth model changed from the old "API key only" era to this Cloud-project +
OAuth/service-account flow — the tutorial covers the one-time setup.
"""

from __future__ import annotations

import ee

from src.config import load_config


class GEEClient:
    """Initializes Earth Engine on construction; validates the GEE config it needs."""

    def __init__(self) -> None:
        cfg = load_config().gee
        if not cfg.project:
            raise RuntimeError(
                "GEE_PROJECT is required for the GEE backend (see docs/gee_setup.md)"
            )
        self.project = cfg.project
        self.gcs_bucket = cfg.gcs_bucket
        self.gcs_prefix = cfg.gcs_prefix

        if cfg.service_account_file:
            # Headless: derive the SA email from the key file and init non-interactively.
            credentials = ee.ServiceAccountCredentials(
                None, key_file=cfg.service_account_file
            )
            ee.Initialize(credentials, project=cfg.project)
        else:
            # Local dev: rely on stored user OAuth creds from `earthengine authenticate`.
            ee.Initialize(project=cfg.project)

    def require_bucket(self) -> str:
        """Return the configured GCS export bucket, or raise if unset."""
        if not self.gcs_bucket:
            raise RuntimeError(
                "GEE_GCS_BUCKET is required to export bronze (see docs/gee_setup.md)"
            )
        return self.gcs_bucket
