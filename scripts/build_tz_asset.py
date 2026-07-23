"""Build the timezone-by-offset FeatureCollection asset for the GEE bronze backend (§5.3).

**Maintainer one-off, not on the pipeline path** (never imported by the DAGs or tests). It
turns the timezone-boundary-builder polygons into a single dissolved-by-standard-offset
GeoJSON whose one property, ``offset``, is the standard (non-DST) UTC offset in minutes — the
exact mapping ``src/db/seed_grid.std_offset_minutes`` stamps into ``era5_land_base_grid.t_zone``,
so the GEE reduction zones and the grid's per-cell label agree by construction.

Use the **combined-with-oceans** boundary set so the polygons tile the globe with no coastal
gaps (a gap would drop boundary cells from the mosaic). Download a release from
https://github.com/evansiroky/timezone-boundary-builder (``combined-with-oceans.json``).

Output is a **zipped shapefile** — Earth Engine table ingestion only accepts ``.shp``/``.zip``
(not GeoJSON). Run (geopandas is not a project dependency — pull it in just for this script)::

    uv run --with geopandas python scripts/build_tz_asset.py \
        --input combined-with-oceans.json --output tz_by_offset.zip

Then ingest it as an EE table asset with ``scripts/ingest_tz_asset.py`` (reuses the pipeline's
service-account auth) and set ``GEE_TZ_ASSET=projects/<ee-project>/assets/tz_by_offset``.
"""

from __future__ import annotations

import argparse
import tempfile
import zipfile
from pathlib import Path
from zoneinfo import ZoneInfoNotFoundError

from src.db.seed_grid import std_offset_minutes


def _offset_for(tzid, geom) -> int:
    """Standard offset for a zone, falling back to centroid-longitude solar for a tzid the
    local tzdata lacks (e.g. America/Coyhaique) — mirrors the grid's solar fallback so the
    GEE zones and era5_land_base_grid.t_zone stay consistent."""
    try:
        return std_offset_minutes(tzid)
    except ZoneInfoNotFoundError:
        return int(round(geom.centroid.x / 15.0)) * 60


def _write_zipped_shapefile(gdf, output_path: str) -> None:
    """Write ``gdf`` as a shapefile and bundle its sidecar files into ``output_path`` (.zip)."""
    out = Path(output_path)
    stem = out.stem  # e.g. "tz_by_offset"
    with tempfile.TemporaryDirectory() as td:
        shp = Path(td) / f"{stem}.shp"
        gdf.to_file(shp, driver="ESRI Shapefile")
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
            for part in Path(td).glob(f"{stem}.*"):
                zf.write(part, part.name)


def build(input_path: str, output_path: str) -> None:
    import geopandas as gpd  # imported here so the module loads without the maintainer dep

    gdf = gpd.read_file(input_path)
    # The boundary set names its zone column "tzid" (IANA name). Map each to the standard
    # offset, then dissolve so every offset is a single (multi)polygon feature.
    gdf["offset"] = [_offset_for(t, g) for t, g in zip(gdf["tzid"], gdf.geometry)]
    dissolved = gdf.dissolve(by="offset", as_index=False)[["offset", "geometry"]]
    _write_zipped_shapefile(dissolved, output_path)
    print(f"wrote {len(dissolved)} offset zones -> {output_path}")
    print("offsets (minutes):", sorted(dissolved["offset"].tolist()))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True, help="timezone-boundary-builder GeoJSON")
    ap.add_argument("--output", default="tz_by_offset.geojson")
    args = ap.parse_args()
    build(args.input, args.output)


if __name__ == "__main__":
    main()
