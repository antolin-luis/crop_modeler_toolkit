"""Build the timezone-by-offset FeatureCollection asset for the GEE bronze backend (§5.3).

**Maintainer one-off, not on the pipeline path** (never imported by the DAGs or tests). It
turns the timezone-boundary-builder polygons into a single dissolved-by-standard-offset
GeoJSON whose one property, ``offset``, is the standard (non-DST) UTC offset in minutes — the
exact mapping ``src/db/seed_grid.std_offset_minutes`` stamps into ``era5_land_base_grid.t_zone``,
so the GEE reduction zones and the grid's per-cell label agree by construction.

Use the **combined-with-oceans** boundary set so the polygons tile the globe with no coastal
gaps (a gap would drop boundary cells from the mosaic). Download a release from
https://github.com/evansiroky/timezone-boundary-builder (``combined-with-oceans.json``).

**Coastal fill (mirrors ``seed_grid._fill_coastal_from_nearest_land``).** A land cell whose
0.25° center falls over water (estuary/lake/coast) must reduce on its country's *civil* day,
not the maritime ``Etc/GMT`` solar offset the ocean polygons carry. So we grow each land
offset zone ``COASTAL_BUFFER_DEG`` (~2°) seaward **into water only** — never over other land,
which would relabel inland border cells — and erase that strip from the ocean zones. Real land
keeps its true offset; the near-shore water strip inherits the adjacent land's offset; open
ocean (dropped by ``land_only`` exports anyway) stays maritime. Where two different land
offsets both reach the same water pixel, ``daily.build_daily_collection``'s mosaic resolves it
deterministically by sorted offset — a rare, bounded edge case at cross-offset estuaries.

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

# How far a land offset zone is grown seaward to claim coastal water pixels for the adjacent
# country's civil day. Degrees (~2° at 0.25° = 8 cells), matching seed_grid's ring cap.
COASTAL_BUFFER_DEG = 2.0


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


def _is_ocean(tzid) -> bool:
    """Maritime (solar) zone from the with-oceans set — ``Etc/GMT±N`` tiles the open sea."""
    return isinstance(tzid, str) and tzid.startswith("Etc/")


def build(input_path: str, output_path: str) -> None:
    import geopandas as gpd  # imported here so the module loads without the maintainer dep
    import pandas as pd
    from shapely.ops import unary_union

    gdf = gpd.read_file(input_path)
    # The boundary set names its zone column "tzid" (IANA name). Map each to the standard
    # offset, then dissolve so every offset is a single (multi)polygon feature.
    gdf["offset"] = [_offset_for(t, g) for t, g in zip(gdf["tzid"], gdf.geometry)]

    is_ocean = gdf["tzid"].map(_is_ocean)
    land = gdf[~is_ocean].dissolve(by="offset", as_index=False)[["offset", "geometry"]]
    ocean = gdf[is_ocean].dissolve(by="offset", as_index=False)[["offset", "geometry"]]

    # Grow each land offset seaward into water only (never over other land), so coastal water
    # pixels inherit the adjacent country's civil offset — the polygon analogue of
    # seed_grid._fill_coastal_from_nearest_land. Restrict the buffer to the ocean footprint so
    # inland border cells keep their true offset.
    ocean_union = unary_union(ocean.geometry.tolist()) if len(ocean) else None
    coastal_rows = []
    if ocean_union is not None and not ocean_union.is_empty:
        for off, geom in zip(land["offset"], land.geometry):
            strip = geom.buffer(COASTAL_BUFFER_DEG).intersection(ocean_union)
            if not strip.is_empty:
                coastal_rows.append({"offset": off, "geometry": strip})

    # Erase the claimed coastal strips from the maritime zones so the sea beyond the buffer
    # stays maritime and the globe still tiles with no gaps.
    if coastal_rows:
        claimed = unary_union([r["geometry"] for r in coastal_rows])
        ocean["geometry"] = ocean.geometry.apply(
            lambda g: g.difference(claimed)
        )
        ocean = ocean[~ocean.geometry.is_empty]

    parts = [land, gpd.GeoDataFrame(coastal_rows, crs=gdf.crs), ocean] if coastal_rows \
        else [land, ocean]
    combined = gpd.GeoDataFrame(
        pd.concat([p for p in parts if len(p)], ignore_index=True), crs=gdf.crs
    )
    dissolved = combined.dissolve(by="offset", as_index=False)[["offset", "geometry"]]
    _write_zipped_shapefile(dissolved, output_path)
    print(f"wrote {len(dissolved)} offset zones -> {output_path}")
    print("offsets (minutes):", sorted(dissolved["offset"].tolist()))
    print(f"coastal strips added for {len({r['offset'] for r in coastal_rows})} land offsets")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", required=True, help="timezone-boundary-builder GeoJSON")
    ap.add_argument("--output", default="tz_by_offset.geojson")
    args = ap.parse_args()
    build(args.input, args.output)


if __name__ == "__main__":
    main()
