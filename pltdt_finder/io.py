"""Read NDVI series out of the database for the planting-date finder.

Each function opens its own connection unless one is passed. ``cs_min`` can only tighten
the Cloud Score+ threshold the data was extracted with (see ``s2_ndvi_chunk``), never
loosen it.
"""

from __future__ import annotations

import pandas as pd

from src.db import load as db_load

_PIXEL_SQL = """
SELECT f.field_id, f.src_id, f.name, p.pixel_id, p.epsg, p.ix, p.iy,
       ST_X(p.geom) AS lon, ST_Y(p.geom) AS lat, o.date, o.ndvi, o.cs_cdf
FROM s2_ndvi_obs o
JOIN s2_pixel p USING (pixel_id)
JOIN field_parcel f USING (field_id)
WHERE f.field_set = %(field_set)s
  AND (%(field_id)s::int IS NULL OR f.field_id = %(field_id)s::int)
  AND (%(start)s::date IS NULL OR o.date >= %(start)s::date)
  AND (%(end)s::date IS NULL OR o.date <= %(end)s::date)
  AND o.cs_cdf >= %(cs_min)s
ORDER BY f.field_id, p.pixel_id, o.date
"""

# Field-level summary per date. clear_frac is the share of the field's pixels observed
# clear that day; a low value means the median comes from a corner of the field.
_FIELD_SQL = """
SELECT f.field_id, f.src_id, f.name, o.date,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY o.ndvi) AS ndvi_median,
       avg(o.ndvi) AS ndvi_mean,
       count(*) AS n_clear,
       count(*)::real / n.n_pixels AS clear_frac
FROM s2_ndvi_obs o
JOIN s2_pixel p USING (pixel_id)
JOIN field_parcel f USING (field_id)
JOIN (SELECT field_id, count(*) AS n_pixels FROM s2_pixel GROUP BY field_id) n
  USING (field_id)
WHERE f.field_set = %(field_set)s
  AND (%(field_id)s::int IS NULL OR f.field_id = %(field_id)s::int)
  AND (%(start)s::date IS NULL OR o.date >= %(start)s::date)
  AND (%(end)s::date IS NULL OR o.date <= %(end)s::date)
  AND o.cs_cdf >= %(cs_min)s
GROUP BY f.field_id, f.src_id, f.name, o.date, n.n_pixels
ORDER BY f.field_id, o.date
"""


def _read(sql: str, params: dict, conn=None) -> pd.DataFrame:
    own = conn is None
    conn = conn or db_load.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c.name for c in cur.description]
            df = pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        if own:
            conn.close()
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def _params(field_set, field_id, start, end, cs_min) -> dict:
    return {"field_set": field_set, "field_id": field_id, "start": start, "end": end,
            "cs_min": cs_min}


def load_pixel_series(
    field_set: str,
    field_id: int | None = None,
    start: str | None = None,
    end: str | None = None,
    cs_min: float = 0.0,
    conn=None,
) -> pd.DataFrame:
    """One row per (pixel, clear date): ids, pixel centre lon/lat, ``ndvi``, ``cs_cdf``."""
    return _read(_PIXEL_SQL, _params(field_set, field_id, start, end, cs_min), conn)


def load_field_mean_series(
    field_set: str,
    field_id: int | None = None,
    start: str | None = None,
    end: str | None = None,
    cs_min: float = 0.0,
    conn=None,
) -> pd.DataFrame:
    """One row per (field, date): median/mean NDVI over clear pixels and ``clear_frac``."""
    return _read(_FIELD_SQL, _params(field_set, field_id, start, end, cs_min), conn)
