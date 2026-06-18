"""Fetch the two static grid-build inputs into bronze (PLANNING.md §6.4, §10 DAG 1).

Both are single-timestep, global, native-resolution fields downloaded once:

- ``geopotential.nc`` — ERA5 single-levels surface ``geopotential`` (one timestep).
  Native 0.25°. Provides the grid coordinates AND elevation = ``z / 9.80665`` (§5.4).
- ``era5_land_mask.nc`` — one ERA5-Land variable (one timestep). Native 0.1°. Land =
  non-NaN, sea = NaN — the land reference for the clip (§6.4).

Manual-drop fallback: if both files already exist, this is a no-op, so a user without
working CDS credentials can drop the two ``.nc`` files in by hand. Neither request
passes the ``grid`` parameter (§11.1).
"""

from __future__ import annotations

import shutil
import tempfile
import zipfile
from pathlib import Path

from src.config import load_config

GEOPOTENTIAL_FILE = "geopotential.nc"
LAND_MASK_FILE = "era5_land_mask.nc"

# Any concrete past timestep works for these static fields; orography and the
# land/sea pattern do not change over time.
_STATIC_DATE = {"year": "2024", "month": "01", "day": "01", "time": "00:00"}

GEOPOTENTIAL_DATASET = "reanalysis-era5-single-levels"
GEOPOTENTIAL_REQUEST = {
    "product_type": "reanalysis",
    "variable": "geopotential",
    "data_format": "netcdf",
    **_STATIC_DATE,
}

LAND_MASK_DATASET = "reanalysis-era5-land"
LAND_MASK_REQUEST = {
    "variable": "2m_temperature",  # NaN pattern over ocean is the land reference
    "data_format": "netcdf",
    **_STATIC_DATE,
}


def _normalize_netcdf(path: Path) -> None:
    """The CDS sometimes returns a zip wrapping a single ``.nc`` (seen for ERA5-Land).

    If ``path`` is a zip, replace it in place with its inner ``.nc`` so downstream
    readers always see a plain netCDF/HDF5 file.
    """
    if not zipfile.is_zipfile(path):
        return
    with zipfile.ZipFile(path) as z:
        members = [m for m in z.namelist() if m.endswith(".nc")]
        if not members:
            raise ValueError(f"{path} is a zip with no .nc member: {z.namelist()}")
        with tempfile.TemporaryDirectory() as td:
            extracted = Path(z.extract(members[0], td))
            shutil.move(str(extracted), str(path))


def ensure_static_inputs(
    dest: str | Path | None = None,
    *,
    force: bool = False,
) -> dict[str, Path]:
    """Ensure both static .nc inputs exist under ``dest``; fetch any missing.

    Returns the resolved paths. No-op when both already present (unless ``force``).
    """
    dest = Path(dest) if dest is not None else load_config().paths.bronze_static_dir
    dest.mkdir(parents=True, exist_ok=True)
    geo = dest / GEOPOTENTIAL_FILE
    mask = dest / LAND_MASK_FILE

    missing = [p for p in (geo, mask) if force or not p.exists()]
    if missing:
        # Import lazily so the manual-drop fallback path needs no cdsapi/credentials.
        from src.cds.client import CDSClient

        client = CDSClient()
        if force or not geo.exists():
            client.retrieve(GEOPOTENTIAL_DATASET, GEOPOTENTIAL_REQUEST, geo)
            _normalize_netcdf(geo)
        if force or not mask.exists():
            client.retrieve(LAND_MASK_DATASET, LAND_MASK_REQUEST, mask)
            _normalize_netcdf(mask)

    return {"geopotential": geo, "land_mask": mask}


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Fetch static grid-build inputs to bronze.")
    ap.add_argument("--dest", default=None, help="override bronze static dir")
    ap.add_argument("--force", action="store_true", help="re-fetch even if present")
    args = ap.parse_args()
    paths = ensure_static_inputs(args.dest, force=args.force)
    for name, p in paths.items():
        print(f"{name}: {p} ({'present' if p.exists() else 'MISSING'})")


if __name__ == "__main__":
    main()
