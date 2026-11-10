"""Point and region read API for ``wth_precip_alt`` (the 0.05° CHIRPS silver).

The fine-grid counterpart of ``src/db/query.py``, and mechanically identical: a coordinate
resolves to its cell **arithmetically**, so a single-cell series needs no PostGIS and no
spatial index — just ``fine_id``/``fparent_id`` computed in Python and one partition-pruned
``SELECT``.

``fparent_id`` must appear in every ``WHERE``. It is the LIST partition key, so without it
Postgres scans every partition instead of one. Because it is a pure function of ``fine_id``
(``fine_encoding.fine_parent_of``), the region path derives it locally rather than paying a
second round-trip — which is the whole reason that helper exists.

``geom`` + GiST is for polygon work only, which is what :func:`fetch_region_series` uses it
for; the point path never touches it.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

import pandas as pd

from src.db import load as db_load
from src.gee.chirps import source_code
from src.grid.fine_encoding import cell_center, fine_code, fine_parent_code, fine_parent_of

TABLE = "wth_precip_alt"
GRID = "chirps_base_grid"

SERIES_COLUMNS = ["date", "source", "precip"]


def locate_fine(lat: float, lon: float) -> tuple[str, str]:
    """``(fine_id, fparent_id)`` for an arbitrary coordinate. No database needed."""
    return fine_code(lat, lon), fine_parent_code(lat, lon)


def _source_filter(sources: Sequence[str | int] | None) -> list[int] | None:
    """Accept source names or raw SMALLINT codes; return codes."""
    if sources is None:
        return None
    return [s if isinstance(s, int) else source_code(s) for s in sources]


def _date_clauses(start, end, params: list) -> list[str]:
    clauses = []
    if start is not None:
        clauses.append("p.date >= %s")
        params.append(start)
    if end is not None:
        clauses.append("p.date <= %s")
        params.append(end)
    return clauses


def _run(sql: str, params: list, columns: list[str], conn) -> pd.DataFrame:
    owned = conn is None
    conn = db_load.connect() if owned else conn
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        if owned:
            conn.close()
    return pd.DataFrame(rows, columns=columns)


def fetch_fine_series(
    fine_id: str,
    fparent_id: str,
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    sources: Sequence[str | int] | None = None,
    conn=None,
) -> pd.DataFrame:
    """Daily precipitation for one already-encoded fine cell, as a date-indexed frame.

    Returns one row per ``(date, source)`` — loading both CHIRPS versions is the point, so
    they are not collapsed. Pass ``sources`` to restrict.
    """
    params: list[object] = [fparent_id, fine_id]
    clauses = ["p.fparent_id = %s", "p.fine_id = %s", *_date_clauses(start, end, params)]
    codes = _source_filter(sources)
    if codes is not None:
        clauses.append("p.source = ANY(%s)")
        params.append(codes)

    sql = (
        f"SELECT p.date, p.source, p.precip FROM {TABLE} p "
        f"WHERE {' AND '.join(clauses)} ORDER BY p.date, p.source"
    )
    return _run(sql, params, SERIES_COLUMNS, conn).set_index("date")


def fetch_series(
    lat: float,
    lon: float,
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    sources: Sequence[str | int] | None = None,
    conn=None,
) -> pd.DataFrame:
    """Daily CHIRPS precipitation (mm/day) for the fine cell containing ``(lat, lon)``.

    Empty frame if the cell was never loaded — outside the built extent, or masked by the
    product. ``date`` is the CHIRPS product day, **not** the local day ``wth_base`` uses;
    see ``src/db/precip_schema.sql``.
    """
    fine_id, fparent_id = locate_fine(lat, lon)
    return fetch_fine_series(
        fine_id, fparent_id, start, end, sources=sources, conn=conn
    )


def fetch_cells_series(
    fine_ids: Sequence[str],
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    sources: Sequence[str | int] | None = None,
    conn=None,
) -> pd.DataFrame:
    """Series for many fine cells at once, with partition pruning.

    ``fparent_id`` is derived from the codes in Python rather than queried, so this is one
    round-trip regardless of how many cells are asked for.
    """
    fine_ids = [f.strip() for f in fine_ids]
    if not fine_ids:
        return pd.DataFrame(columns=["fine_id", *SERIES_COLUMNS])

    fparents = sorted({fine_parent_of(f) for f in fine_ids})
    params: list[object] = [fparents, fine_ids]
    clauses = [
        "p.fparent_id = ANY(%s)",
        "p.fine_id = ANY(%s)",
        *_date_clauses(start, end, params),
    ]
    codes = _source_filter(sources)
    if codes is not None:
        clauses.append("p.source = ANY(%s)")
        params.append(codes)

    sql = (
        f"SELECT p.fine_id, p.date, p.source, p.precip FROM {TABLE} p "
        f"WHERE {' AND '.join(clauses)} ORDER BY p.fine_id, p.date, p.source"
    )
    return _run(sql, params, ["fine_id", *SERIES_COLUMNS], conn)


def cells_in_region(wkt: str, *, conn=None) -> list[str]:
    """``fine_id``s whose cell intersects a WKT polygon (EPSG:4326).

    The one place ``geom`` and its GiST index earn their keep. Feed the result to
    :func:`fetch_cells_series`, which derives the parents for pruning.
    """
    sql = (
        f"SELECT fine_id FROM {GRID} "
        "WHERE ST_Intersects(geom, ST_GeomFromText(%s, 4326)) ORDER BY fine_id"
    )
    frame = _run(sql, [wkt], ["fine_id"], conn)
    return [f.strip() for f in frame["fine_id"]]


def fetch_region_series(
    wkt: str,
    start: date | str | None = None,
    end: date | str | None = None,
    *,
    sources: Sequence[str | int] | None = None,
    conn=None,
) -> pd.DataFrame:
    """Every fine cell intersecting a polygon, as one long frame.

    Two round-trips: the GiST lookup, then the partition-pruned fact read. Sized warning —
    a Tocantins municipality is ~2,000 fine cells against ~80 on the 0.25° grid, so a
    multi-decade region pull is large. Narrow the date range, or aggregate server-side.
    """
    owned = conn is None
    conn = db_load.connect() if owned else conn
    try:
        fine_ids = cells_in_region(wkt, conn=conn)
        return fetch_cells_series(
            fine_ids, start, end, sources=sources, conn=conn
        )
    finally:
        if owned:
            conn.close()


def fine_cell_center(lat: float, lon: float) -> tuple[float, float]:
    """The fine cell centre a coordinate snaps to — for reporting what was returned."""
    return cell_center(lat, lon)
