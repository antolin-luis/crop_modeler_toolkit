"""Daily mean temperature per field, from the ERA5 cell under the field's centroid.

Feeds the thermal-time lag in ``greenup.estimate_sowing``. Fields that share a cell share
one ``fetch_series`` call.
"""

from __future__ import annotations

import pandas as pd

from src.db import load as db_load
from src.db.query import fetch_series, locate

_CENTROID_SQL = """
SELECT field_id, ST_Y(ST_Centroid(geom)) AS lat, ST_X(ST_Centroid(geom)) AS lon
FROM field_parcel WHERE field_set = %s ORDER BY field_id
"""


def field_tmean(field_set: str, start: str, end: str) -> dict[int, pd.Series]:
    """``{field_id: tmean}``, tmean = (tmax + tmin) / 2 in degC, indexed by date."""
    conn = db_load.connect()
    try:
        with conn.cursor() as cur:
            cur.execute(_CENTROID_SQL, (field_set,))
            rows = cur.fetchall()
        by_cell: dict[tuple, pd.Series] = {}
        out: dict[int, pd.Series] = {}
        for fid, lat, lon in rows:
            cell = locate(lat, lon)
            if cell not in by_cell:
                w = fetch_series(lat, lon, start, end, conn=conn)
                if w.empty:
                    raise RuntimeError(f"no ERA5 weather for field {fid} at ({lat}, {lon})")
                w = w.reset_index() if "date" not in w.columns else w
                by_cell[cell] = pd.Series(
                    ((w["tmax"] + w["tmin"]) / 2).to_numpy(),
                    index=pd.to_datetime(w["date"]),
                    name="tmean",
                )
            out[fid] = by_cell[cell]
        return out
    finally:
        conn.close()
