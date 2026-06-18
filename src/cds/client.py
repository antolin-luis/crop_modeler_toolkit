"""Minimal CDS client wrapper (PLANNING.md §11.1).

A thin shim over ``cdsapi.Client`` reading credentials from ``src/config.py`` so the
rest of the toolkit never touches ``~/.cdsapirc`` directly. This is deliberately tiny:
the full adaptive splitter / manifest is roadmap Step 3. Here we only need single
static retrievals for the grid build.

Hard rule: never pass the ``grid`` parameter — server-side regridding *increases* CDS
cost and we want native 0.25° anyway (§11.1). ``retrieve`` rejects it defensively.
"""

from __future__ import annotations

from pathlib import Path

import cdsapi

from src.config import load_config


class CDSClient:
    """Thin wrapper around ``cdsapi.Client`` with credentials from config."""

    def __init__(self) -> None:
        cfg = load_config().cds
        self._client = cdsapi.Client(url=cfg.url, key=cfg.key)

    def retrieve(self, dataset: str, request: dict, target: str | Path) -> Path:
        """Retrieve ``dataset`` with ``request`` to ``target``; returns the path.

        Refuses any request carrying ``grid`` (§11.1).
        """
        if "grid" in request:
            raise ValueError(
                "CDS request must not set 'grid' — regridding increases cost (§11.1)"
            )
        target = Path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        self._client.retrieve(dataset, request, str(target))
        return target
