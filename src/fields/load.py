"""Load a field-polygon shapefile into ``field_parcel`` and lay out its ``s2_pixel`` grid.

Both steps are SQL-heavy on purpose: PostGIS already has ring assembly (``ST_BuildArea``),
geodesic area, and UTM transforms, so Python only parses bytes and stages text.
"""

from __future__ import annotations

import io
import json
import math
from pathlib import Path

import pandas as pd

from src.db import load as db_load
from src.fields import shp
from src.grid import dbf

SCHEMA_SQL = Path(__file__).resolve().parent.parent / "db" / "fields_schema.sql"
STAGING = "field_parcel_staging"
_STAGE_COLUMNS = ["src_id", "name", "attrs", "wkt"]


def ensure_schema(conn) -> None:
    db_load.execute_script(conn, SCHEMA_SQL.read_text())
    conn.commit()


def _json_value(v):
    if v is None or v is pd.NA or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return None if pd.isna(v) else v.date().isoformat()
    if isinstance(v, str):
        # dBase pads with blanks, but some writers pad with NULs instead.
        return v.strip("\x00 ") or None
    if hasattr(v, "item"):          # numpy / pandas scalars -> plain Python
        return v.item()
    return v


def _id_text(v) -> str:
    v = _json_value(v)
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    if v is None:
        raise ValueError("id column has an empty value; every field needs an id")
    return str(v)


def build_rows(
    attrs: pd.DataFrame, shapes: list[shp.Shape], id_column: str, name_column: str | None
) -> pd.DataFrame:
    """One staging row per shape: id, name, attrs JSON, rings WKT. Pure; tested offline."""
    if id_column not in attrs.columns:
        raise KeyError(f"no id column {id_column!r}; have {list(attrs.columns)}")
    rows = []
    for s in shapes:
        rec = attrs.iloc[s.index]
        record = {c: _json_value(rec[c]) for c in attrs.columns}
        rows.append(
            {
                "src_id": _id_text(rec[id_column]),
                "name": record.get(name_column) if name_column else None,
                "attrs": json.dumps(record, ensure_ascii=False),
                "wkt": s.wkt(),
            }
        )
    out = pd.DataFrame(rows, columns=_STAGE_COLUMNS)
    dupes = out["src_id"][out["src_id"].duplicated()].unique()
    if len(dupes):
        raise ValueError(f"id column {id_column!r} is not unique: {list(dupes)[:10]}")
    return out


def load_fields(
    conn,
    shp_path: str | Path,
    field_set: str,
    *,
    id_column: str = "ID",
    name_column: str | None = "NAME",
) -> int:
    """Upsert every polygon of ``shp_path`` into ``field_parcel``. Returns rows upserted.

    A field whose geometry changed loses its pixels (and, by cascade, their NDVI and done
    marks) so ``build_pixels`` lays them out again — stale pixels outside the new boundary
    would otherwise keep their old series forever. Fields absent from the file are left
    alone; drop a set with ``DELETE FROM field_parcel WHERE field_set = ...``.
    """
    shp.check_prj(shp_path)
    shapes = shp.read_polygons(shp_path)
    with dbf.open_table(shp_path) as (header, fh):
        attrs = pd.concat(list(dbf.iter_chunks(fh, header)), ignore_index=True)
    if len(attrs) != header.n_records:
        # Deleted DBF records are dropped by the reader, which would misalign rows with
        # shapes (the .shp keeps every record). Rare enough to refuse rather than handle.
        raise ValueError(f"{Path(shp_path).name}: DBF has deleted records; repack it")
    if name_column and name_column not in attrs.columns:
        name_column = None
    rows = build_rows(attrs, shapes, id_column, name_column)

    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {STAGING} (src_id TEXT, name TEXT, attrs JSONB, wkt TEXT) "
            "ON COMMIT DROP"
        )
    buf = io.StringIO()
    rows.to_csv(buf, columns=_STAGE_COLUMNS, index=False, header=False)
    buf.seek(0)
    db_load.copy_csv(conn, STAGING, _STAGE_COLUMNS, buf)

    new_geom = "ST_Multi(ST_BuildArea(ST_GeomFromText(s.wkt, 4326)))"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT s.src_id FROM {STAGING} s "
            f"WHERE {new_geom} IS NULL OR ST_IsEmpty({new_geom})"
        )
        bad = [r[0] for r in cur.fetchall()]
        if bad:
            raise ValueError(f"no polygon could be built for field(s) {bad[:10]}")
        cur.execute(
            f"DELETE FROM s2_pixel p USING field_parcel f, {STAGING} s "
            "WHERE p.field_id = f.field_id AND f.field_set = %s AND f.src_id = s.src_id "
            f"AND NOT ST_Equals(f.geom, {new_geom})",
            (field_set,),
        )
        cur.execute(
            f"DELETE FROM s2_ndvi_chunk c USING field_parcel f, {STAGING} s "
            "WHERE c.field_id = f.field_id AND f.field_set = %s AND f.src_id = s.src_id "
            f"AND NOT ST_Equals(f.geom, {new_geom})",
            (field_set,),
        )
        cur.execute(
            "INSERT INTO field_parcel (field_set, src_id, name, attrs, area_ha, geom) "
            f"SELECT %s, s.src_id, s.name, s.attrs, "
            f"ST_Area({new_geom}::geography) / 1e4, {new_geom} FROM {STAGING} s "
            "ON CONFLICT (field_set, src_id) DO UPDATE SET name = EXCLUDED.name, "
            "attrs = EXCLUDED.attrs, area_ha = EXCLUDED.area_ha, geom = EXCLUDED.geom, "
            "loaded_at = now()",
            (field_set,),
        )
        n = cur.rowcount
    conn.commit()
    return n


# UTM zone from the field centroid; a pixel centre is kept when the field contains it.
# The bbox is widened to whole 10 m cells, so generate_series covers every candidate.
_BUILD_PIXELS_SQL = """
WITH f AS (
    SELECT field_id, geom,
           (CASE WHEN ST_Y(c) >= 0 THEN 32600 ELSE 32700 END
            + LEAST(60, floor((ST_X(c) + 180) / 6)::int + 1)) AS epsg
    FROM (SELECT field_id, geom, ST_Centroid(geom) AS c
          FROM field_parcel WHERE field_set = %s) s
), u AS (
    SELECT field_id, epsg, ST_Transform(geom, epsg) AS g FROM f
), cand AS (
    SELECT field_id, epsg, g, ix, iy
    FROM u,
         generate_series(floor(ST_XMin(g) / 10)::int, floor(ST_XMax(g) / 10)::int) ix,
         generate_series(floor(ST_YMin(g) / 10)::int, floor(ST_YMax(g) / 10)::int) iy
)
INSERT INTO s2_pixel (field_id, epsg, ix, iy, geom)
SELECT field_id, epsg, ix, iy,
       ST_Transform(ST_SetSRID(ST_MakePoint(ix * 10 + 5, iy * 10 + 5), epsg), 4326)
FROM cand
WHERE ST_Contains(g, ST_SetSRID(ST_MakePoint(ix * 10 + 5, iy * 10 + 5), epsg))
ON CONFLICT (epsg, ix, iy) DO NOTHING
"""


def build_pixels(conn, field_set: str) -> int:
    """Lay out the 10 m S2 pixels of every field in the set. Returns pixels in the set.

    Idempotent: existing pixels keep their ``pixel_id`` (and so their NDVI rows).
    """
    with conn.cursor() as cur:
        cur.execute(_BUILD_PIXELS_SQL, (field_set,))
        cur.execute(
            "SELECT count(*) FROM s2_pixel p JOIN field_parcel f USING (field_id) "
            "WHERE f.field_set = %s",
            (field_set,),
        )
        total = cur.fetchone()[0]
    conn.commit()
    return total


def field_ids(conn, field_set: str) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT field_id FROM field_parcel WHERE field_set = %s ORDER BY field_id",
            (field_set,),
        )
        return [r[0] for r in cur.fetchall()]


def validate(conn, field_set: str) -> list[dict]:
    """Per-field summary: pixels, clear observations, date span, and the ERA5 cells under it."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.field_id, f.src_id, f.name, round(f.area_ha::numeric, 2),
                   (SELECT count(*) FROM s2_pixel p WHERE p.field_id = f.field_id),
                   (SELECT count(*) FROM s2_ndvi_obs o JOIN s2_pixel p USING (pixel_id)
                     WHERE p.field_id = f.field_id),
                   (SELECT min(o.date) || '..' || max(o.date)
                      FROM s2_ndvi_obs o JOIN s2_pixel p USING (pixel_id)
                     WHERE p.field_id = f.field_id),
                   (SELECT string_agg(g.child_id, ',' ORDER BY g.child_id)
                      FROM era5_land_base_grid g WHERE ST_Intersects(g.geom, f.geom))
            FROM field_parcel f WHERE f.field_set = %s ORDER BY f.field_id
            """,
            (field_set,),
        )
        keys = ["field_id", "src_id", "name", "area_ha", "pixels", "obs", "dates",
                "era5_cells"]
        return [dict(zip(keys, r)) for r in cur.fetchall()]
