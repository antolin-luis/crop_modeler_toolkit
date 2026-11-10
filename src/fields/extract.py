"""Pull one field's per-pixel NDVI for a date window from Earth Engine into ``s2_ndvi_obs``.

No GCS and no bronze: ``ee.data.computePixels`` returns the field's pixel block as a
numpy structured array (one field per band) synchronously. The request grid is the
field's own pixel bbox in its UTM zone at 10 m, with edges on multiples of 10 m — the
native Sentinel-2 grid — so array cell ``[i, j]`` *is* pixel ``(ix0 + j, iy1 - i)``.

A field-year is typically ~100 dates x 2 bands x a few thousand pixels: a few MB, well
under the request cap. When a request is refused for size anyway (a long thin field has a
large bbox), the date list is halved and each half fetched on its own.
"""

from __future__ import annotations

import datetime as dt
import io
import json
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.db import load as db_load
from src.fields import s2

OBS_COLUMNS = ["pixel_id", "date", "ndvi", "cs_cdf"]
STAGING = "s2_ndvi_staging"
# EE's wording for an oversized computePixels request ("Total request size ... must be
# less than or equal to ...", "... too many bands ..."). Matched loosely on purpose.
_TOO_BIG = ("request size", "less than or equal", "too many bands", "too large")


@dataclass(frozen=True)
class FieldGrid:
    field_id: int
    epsg: int
    region: dict              # field geometry, GeoJSON lon/lat
    pixel_id: np.ndarray
    ix: np.ndarray
    iy: np.ndarray

    @property
    def ix0(self) -> int:
        return int(self.ix.min())

    @property
    def iy1(self) -> int:
        return int(self.iy.max())

    @property
    def width(self) -> int:
        return int(self.ix.max()) - self.ix0 + 1

    @property
    def height(self) -> int:
        return self.iy1 - int(self.iy.min()) + 1

    def grid(self) -> dict:
        """``computePixels`` grid: top-left corner at (ix0*10, (iy1+1)*10), 10 m, north-up."""
        return {
            "dimensions": {"width": self.width, "height": self.height},
            "affineTransform": {
                "scaleX": 10, "shearX": 0, "translateX": self.ix0 * 10,
                "shearY": 0, "scaleY": -10, "translateY": (self.iy1 + 1) * 10,
            },
            "crsCode": f"EPSG:{self.epsg}",
        }


def fetch_field_grid(conn, field_id: int) -> FieldGrid:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ST_AsGeoJSON(geom) FROM field_parcel WHERE field_id = %s", (field_id,)
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(f"no field_parcel with field_id={field_id}")
        region = json.loads(row[0])
        cur.execute(
            "SELECT pixel_id, epsg, ix, iy FROM s2_pixel WHERE field_id = %s "
            "ORDER BY pixel_id",
            (field_id,),
        )
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError(
            f"field {field_id} has no s2_pixel rows — run build_pixels first (or the "
            "field is smaller than one 10 m pixel centre)"
        )
    epsgs = {r[1] for r in rows}
    if len(epsgs) != 1:
        raise RuntimeError(f"field {field_id} spans several UTM grids: {sorted(epsgs)}")
    a = np.array([(r[0], r[2], r[3]) for r in rows], dtype=np.int64)
    return FieldGrid(field_id, epsgs.pop(), region, a[:, 0], a[:, 1], a[:, 2])


def to_rows(arr: np.ndarray, g: FieldGrid, dates: list[str], cs_threshold: float) -> pd.DataFrame:
    """Structured ``computePixels`` array -> clear observations for the field's pixels."""
    i = g.iy1 - g.iy           # row index: north at the top
    j = g.ix - g.ix0
    frames = []
    for d in dates:
        tag = s2.band_tag(d)
        ndvi = arr[f"ndvi_{tag}"][i, j]
        cs = arr[f"cs_{tag}"][i, j]
        keep = (
            np.isfinite(ndvi) & np.isfinite(cs)
            & (ndvi != s2.NODATA) & (cs != s2.NODATA) & (cs >= cs_threshold)
        )
        if keep.any():
            frames.append(
                pd.DataFrame(
                    {"pixel_id": g.pixel_id[keep], "date": d,
                     "ndvi": ndvi[keep], "cs_cdf": cs[keep]}
                )
            )
    if not frames:
        return pd.DataFrame(columns=OBS_COLUMNS)
    return pd.concat(frames, ignore_index=True)


def _is_too_big(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(s in msg for s in _TOO_BIG)


def fetch_rows(
    g: FieldGrid, col, dates: list[str], cs_threshold: float, *, compute=None
) -> pd.DataFrame:
    """``computePixels`` for ``dates``, halving the date list while EE says it is too big."""
    import ee

    compute = compute or ee.data.computePixels
    try:
        arr = compute(
            {
                "expression": s2.ndvi_stack(col, dates),
                "fileFormat": "NUMPY_NDARRAY",
                "grid": g.grid(),
            }
        )
    except Exception as exc:  # ee.EEException; kept broad so tests need no real ee
        if len(dates) < 2 or not _is_too_big(exc):
            raise
        mid = len(dates) // 2
        print(f"  field {g.field_id}: {len(dates)} dates too big, splitting", flush=True)
        return pd.concat(
            [
                fetch_rows(g, col, dates[:mid], cs_threshold, compute=compute),
                fetch_rows(g, col, dates[mid:], cs_threshold, compute=compute),
            ],
            ignore_index=True,
        )
    return to_rows(arr, g, dates, cs_threshold)


def write_obs(conn, rows: pd.DataFrame) -> int:
    """Upsert observations (COPY into staging, then ``ON CONFLICT DO UPDATE``). No commit."""
    if rows.empty:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {STAGING} (pixel_id INTEGER, date DATE, ndvi REAL, "
            "cs_cdf REAL) ON COMMIT DROP"
        )
    buf = io.StringIO()
    rows.to_csv(buf, columns=OBS_COLUMNS, index=False, header=False)
    buf.seek(0)
    db_load.copy_csv(conn, STAGING, OBS_COLUMNS, buf)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO s2_ndvi_obs ({', '.join(OBS_COLUMNS)}) "
            f"SELECT {', '.join(OBS_COLUMNS)} FROM {STAGING} "
            "ON CONFLICT (pixel_id, date) DO UPDATE "
            "SET ndvi = EXCLUDED.ndvi, cs_cdf = EXCLUDED.cs_cdf"
        )
        return cur.rowcount


def mark_done(conn, *, field_id, year, start, end, cs_threshold, n_dates, n_obs) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO s2_ndvi_chunk (field_id, year, start_date, end_date, cs_threshold, "
            "n_dates, n_obs) VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (field_id, year) DO UPDATE SET start_date = EXCLUDED.start_date, "
            "end_date = EXCLUDED.end_date, cs_threshold = EXCLUDED.cs_threshold, "
            "n_dates = EXCLUDED.n_dates, n_obs = EXCLUDED.n_obs, done_at = now()",
            (field_id, year, start, end, cs_threshold, n_dates, n_obs),
        )


def extract_field_window(
    conn, field_id: int, year: int, start: str, end: str, cs_threshold: float
) -> dict:
    """Fetch, store, and mark done one (field, year) window ``[start, end)``. One commit."""
    import ee

    g = fetch_field_grid(conn, field_id)
    col = s2.collection(ee.Geometry(g.region), start, end, g.epsg)
    dates = s2.acquisition_dates(col).getInfo()
    rows = fetch_rows(g, col, dates, cs_threshold) if dates else pd.DataFrame(
        columns=OBS_COLUMNS
    )
    n_obs = write_obs(conn, rows)
    mark_done(
        conn, field_id=field_id, year=year, start=start, end=end,
        cs_threshold=cs_threshold, n_dates=len(dates), n_obs=n_obs,
    )
    conn.commit()
    return {"field_id": field_id, "year": year, "pixels": len(g.pixel_id),
            "dates": len(dates), "obs": n_obs}


def plan_windows(
    field_ids: list[int],
    start: dt.date,
    end: dt.date,
    cs_threshold: float,
    done: dict[tuple[int, int], tuple[dt.date, dt.date, float]],
    *,
    force: bool = False,
) -> list[dict]:
    """(field, calendar-year) windows covering ``[start, end]`` (``end`` inclusive).

    A window is skipped when ``done`` already covers it at a threshold no stricter than
    ``cs_threshold`` — a looser stored threshold holds a superset of the clear obs. The
    current year is never covered by yesterday's run, so it is re-pulled each time.
    """
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")
    out = []
    for year in range(start.year, end.year + 1):
        w_start = max(start, dt.date(year, 1, 1))
        w_end = min(end + dt.timedelta(days=1), dt.date(year + 1, 1, 1))  # exclusive
        for fid in field_ids:
            prev = done.get((fid, year))
            if (
                not force and prev is not None
                and prev[0] <= w_start and prev[1] >= w_end and prev[2] <= cs_threshold
            ):
                continue
            out.append({"field_id": fid, "year": year, "start": w_start.isoformat(),
                        "end": w_end.isoformat(), "cs_threshold": cs_threshold})
    return out


def fetch_done(conn, field_ids: list[int]) -> dict[tuple[int, int], tuple]:
    if not field_ids:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT field_id, year, start_date, end_date, cs_threshold FROM s2_ndvi_chunk "
            "WHERE field_id = ANY(%s)",
            (field_ids,),
        )
        # REAL round-trips 0.6 as 0.6000000238; without rounding nothing ever looks done.
        return {(r[0], r[1]): (r[2], r[3], round(float(r[4]), 4)) for r in cur.fetchall()}
