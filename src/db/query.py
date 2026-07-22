"""Point read API for silver (PLANNING.md §8.1 lookup pattern).

The whole point of the grid design: a coordinate resolves to its cell **arithmetically**,
so a single-cell time series needs no PostGIS, no spatial index and no join against
``era5_land_base_grid`` — just ``child_id``/``parent_id`` computed in Python and one
partition-pruned ``SELECT``. (``geom`` + GiST is for polygon/region queries, not this.)

Consumers get a plain date-indexed DataFrame in silver units; the DSSAT ``.WTH``
materialization (gold) is a later, separate step and is not needed to use the data.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from src.db import load as db_load
from src.grid.encoding import cell_code, code_to_latlon, parent_code
from src.grid.encode_long import DEFAULT_BLOCK_SIZE_B

SERIES_COLUMNS = [
    "date", "tmax", "tmin", "precip", "srad", "wind", "tdew", "rh", "et0",
    "is_preliminary",
]


def locate(lat: float, lon: float, *, b: int = DEFAULT_BLOCK_SIZE_B) -> tuple[str, str]:
    """Return ``(child_id, parent_id)`` for an arbitrary coordinate.

    The encoders round to the nearest cell center internally, so no pre-snapping is
    needed. ``b`` must match the value the database was built with (immutable, §6.3).
    """
    return cell_code(lat, lon), parent_code(lat, lon, b)


def fetch_cell_series(
    child_id: str,
    parent_id: str,
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    conn=None,
) -> pd.DataFrame:
    """Daily series for one already-encoded cell, as a date-indexed DataFrame.

    ``conn`` is optional; without one a connection is opened and closed around the query.
    """
    clauses = ["parent_id = %s", "child_id = %s"]
    params: list[object] = [parent_id, child_id]
    if start is not None:
        clauses.append("date >= %s")
        params.append(start)
    if end is not None:
        clauses.append("date <= %s")
        params.append(end)

    sql = (
        f"SELECT {', '.join(SERIES_COLUMNS)} FROM wth_base "
        f"WHERE {' AND '.join(clauses)} ORDER BY date"
    )

    owned = conn is None
    conn = db_load.connect() if owned else conn
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        if owned:
            conn.close()

    frame = pd.DataFrame(rows, columns=SERIES_COLUMNS)
    return frame.set_index("date")


def fetch_series(
    lat: float,
    lon: float,
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    b: int = DEFAULT_BLOCK_SIZE_B,
    conn=None,
) -> pd.DataFrame:
    """Daily weather series for the grid cell containing ``(lat, lon)``.

    Columns are silver units (§8.2): ``tmax``/``tmin``/``tdew`` °C, ``precip`` mm/day,
    ``srad`` MJ/m²/day, ``wind`` m/s at 10 m, ``rh`` %, ``et0`` mm/day, plus the
    ``is_preliminary`` ERA5T provenance flag. Empty frame if the cell was never loaded
    (e.g. outside the downloaded extent, or ocean).
    """
    child_id, parent_id = locate(lat, lon, b=b)
    return fetch_cell_series(child_id, parent_id, start, end, conn=conn)


def cell_center(lat: float, lon: float) -> tuple[float, float]:
    """The cell center a coordinate snaps to — useful when reporting what was returned."""
    return code_to_latlon(cell_code(lat, lon))
