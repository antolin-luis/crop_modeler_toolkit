"""Point read API for the soil layer: a coordinate in, a DSSAT ``ID_SOIL`` out.

The soil-side counterpart of ``src/db/query.py``, and it works the same way: the source
points sit on a regular 5 arc-min grid, so the coordinate resolves to the containing
``cell5m`` **arithmetically** and the lookup is a primary-key hit — no PostGIS, no spatial
index, one row read. ``geom`` and its GiST index are only touched by the nearest-point
fallback, which is what they are for.

The encoding was verified against the loaded table, not assumed::

    cell5m = floor((90 - lat) * 12) * 4320 + floor((lon + 180) * 12)

— exact on every sampled row (row-major from the north pole, 4320 columns of 5 arc-min,
0-indexed with no offset).

Two lookups, because a coordinate can miss:

- **Exact.** The soil layer is land-only, so a coastal, lake or ice coordinate can land on
  a cell with no profile. Then there is no answer, and saying so beats inventing one.
- **Nearest.** Falls back to the closest profile within ``max_km``, reporting the distance
  so the caller can judge whether a profile 8 km away is fit for their purpose.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from src.db import load as db_load

TABLE = "soil_profile_points"

# The source grid: 5 arc-min (12 cells per degree), 4320 columns, row 0 at the north pole.
CELLS_PER_DEGREE = 12
NCOLS = 4320
NROWS = 2160

_FIELDS = ["cell5m", "soil_id", "iso2", "lat", "lon", "child_id", "parent_id"]

# Rough degrees-per-km at the equator; only used to size the search box for the nearest
# fallback, never to measure a distance (that is ST_Distance on geography).
_KM_PER_DEGREE = 111.32


@dataclass(frozen=True)
class SoilProfile:
    """One soil point. ``dist_km`` is 0.0 for an exact hit."""

    cell5m: int
    soil_id: str
    iso2: str
    lat: float
    lon: float
    child_id: str
    parent_id: str
    dist_km: float


def locate(lat: float, lon: float) -> int:
    """Return the ``cell5m`` of the 5 arc-min cell containing ``(lat, lon)``.

    Pure arithmetic and no DB round-trip, exactly like ``query.locate`` on the weather
    side. This is a **containing-cell** rule (floor), not nearest-centre (round): a
    coordinate belongs to the cell whose box it falls in, which is what the source ids mean.

    The poles and the antimeridian are clamped/wrapped so any finite input yields a valid
    id rather than an index off the end of the grid.
    """
    if not (-90.0 <= lat <= 90.0):
        raise ValueError(f"lat {lat} outside -90..90")

    row = int(math.floor((90.0 - lat) * CELLS_PER_DEGREE))
    row = min(max(row, 0), NROWS - 1)  # lat == -90 would otherwise fall one row past the end
    col = int(math.floor(((lon + 180.0) % 360.0) * CELLS_PER_DEGREE)) % NCOLS
    return row * NCOLS + col


def cell_center(cell5m: int) -> tuple[float, float]:
    """The ``(lat, lon)`` centre of a ``cell5m`` — the inverse of ``locate``."""
    row, col = divmod(int(cell5m), NCOLS)
    lat = 90.0 - (row + 0.5) / CELLS_PER_DEGREE
    lon = -180.0 + (col + 0.5) / CELLS_PER_DEGREE
    return lat, lon


def _row_to_profile(row, dist_km: float | None = None) -> SoilProfile:
    cell5m, soil_id, iso2, lat, lon, child_id, parent_id = row[:7]
    return SoilProfile(
        cell5m=int(cell5m),
        soil_id=soil_id.strip(),
        iso2=iso2.strip(),
        lat=float(lat),
        lon=float(lon),
        # child_id/parent_id are CHAR(4) and come back space-padded from psycopg2.
        child_id=child_id.strip(),
        parent_id=parent_id.strip(),
        dist_km=float(row[7]) if dist_km is None else dist_km,
    )


def profile_at(
    lat: float,
    lon: float,
    *,
    nearest: bool = True,
    max_km: float = 25.0,
    conn=None,
) -> SoilProfile | None:
    """The DSSAT soil profile for ``(lat, lon)``, or ``None``.

    Tries the containing 5 arc-min cell first — a PK lookup. With ``nearest=True`` (the
    default) a miss falls back to the closest profile within ``max_km``, measured on the
    geography (true metres), with the GiST index pre-filtering the candidates by bounding
    box. ``nearest=False`` makes a miss simply a miss.

    ``conn`` is optional; without one a connection is opened and closed around the query.
    """
    owned = conn is None
    conn = db_load.connect() if owned else conn
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(_FIELDS)} FROM {TABLE} WHERE cell5m = %s",
                (locate(lat, lon),),
            )
            row = cur.fetchone()
            if row is not None:
                return _row_to_profile(row, dist_km=0.0)
            if not nearest:
                return None

            # Longitude degrees shrink with latitude, so the search box is widened in x by
            # 1/cos(lat) — a square box in degrees would under-reach in longitude near the
            # poles and quietly return "no profile" where one is well inside max_km.
            dy = max_km / _KM_PER_DEGREE
            dx = dy / max(math.cos(math.radians(lat)), 1e-3)
            cur.execute(
                f"SELECT {', '.join('s.' + f for f in _FIELDS)}, "
                "ST_Distance(s.geom::geography, p.pt::geography) / 1000.0 AS dist_km "
                f"FROM {TABLE} s, "
                "(SELECT ST_SetSRID(ST_MakePoint(%s, %s), 4326) AS pt) p "
                "WHERE s.geom && ST_Expand(p.pt, %s, %s) "
                "  AND ST_DWithin(s.geom::geography, p.pt::geography, %s) "
                "ORDER BY s.geom <-> p.pt LIMIT 1",
                (lon, lat, dx, dy, max_km * 1000.0),
            )
            row = cur.fetchone()
            return _row_to_profile(row) if row is not None else None
    finally:
        if owned:
            conn.close()


def profiles_at(
    coords: Iterable[tuple[float, float]], *, conn=None
) -> pd.DataFrame:
    """Exact-cell lookup for many coordinates at once.

    One query for the whole batch instead of one per site. Returns a frame indexed by the
    input order with the request's ``lat``/``lon`` alongside the profile columns; a
    coordinate with no profile keeps its row with NULLs, so the output always lines up with
    the input.

    Exact only — there is no batched nearest fallback, because a per-row KNN is a per-row
    query and the caller is better off looping ``profile_at`` for the handful that missed.
    """
    requests = [(float(la), float(lo)) for la, lo in coords]
    if not requests:
        return pd.DataFrame(columns=["lat", "lon", *_FIELDS])

    wanted = [locate(la, lo) for la, lo in requests]
    owned = conn is None
    conn = db_load.connect() if owned else conn
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(_FIELDS)} FROM {TABLE} WHERE cell5m = ANY(%s)",
                (list(set(wanted)),),
            )
            rows = cur.fetchall()
    finally:
        if owned:
            conn.close()

    found = pd.DataFrame(rows, columns=_FIELDS)
    for text in ("soil_id", "iso2", "child_id", "parent_id"):
        if not found.empty:
            found[text] = found[text].str.strip()

    request_frame = pd.DataFrame(requests, columns=["lat", "lon"]).assign(cell5m=wanted)
    return request_frame.merge(
        found, on="cell5m", how="left", suffixes=("", "_cell")
    ).rename(columns={"lat_cell": "cell_lat", "lon_cell": "cell_lon"})


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="Look up the DSSAT soil profile at a point.")
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument("--max-km", type=float, default=25.0)
    ap.add_argument(
        "--exact", action="store_true", help="no nearest fallback; a miss is a miss"
    )
    args = ap.parse_args()

    profile = profile_at(
        args.lat, args.lon, nearest=not args.exact, max_km=args.max_km
    )
    if profile is None:
        raise SystemExit(
            f"no soil profile at ({args.lat}, {args.lon}); cell5m "
            f"{locate(args.lat, args.lon)} is not in {TABLE}"
        )
    print(f"soil_id   {profile.soil_id}")
    print(f"country   {profile.iso2}")
    print(f"cell5m    {profile.cell5m}  ({profile.lat}, {profile.lon})")
    print(f"distance  {profile.dist_km:.2f} km")
    print(f"era5 cell {profile.child_id} / parent {profile.parent_id}")


if __name__ == "__main__":
    main()
