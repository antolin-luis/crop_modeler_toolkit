"""Split an extent into parent-aligned export chunks (docs/cost_model_climate_context.md §9.3).

One EE export of a whole year over a continental extent does not complete: the export is a
single image of 366 bands (``export._to_multiband``), and at 0.25° a country-sized bbox is
*smaller* than EE's 256x256 tile, so the entire year is computed by one worker with no
spatial parallelism. Brazil additionally mosaics one clipped reduction per timezone zone
per band. The observed failure mode is EE restarting the task (``attempt`` 2, 3) until it
is cancelled — see ``.localdata/calib_br/bronze/_gee_metrics.jsonl``.

Splitting the extent gives each chunk its own task, hence its own worker and its own tile
budget. This module is only the *geometry* of that split; what size is right is an
empirical question that ``scripts/gee_chunk_probe.py`` answers.

**Chunks are aligned to the canonical grid, never to the requested extent.** A chunk is a
``k x k`` block of parents (at ``b=4`` a parent is 1° x 1°, so ``k=10`` is a 10° x 10°
box), and its position comes from ``p_row // k`` on the global grid. Consequences, all
deliberate:

- A chunk may extend past the requested extent. That is cheaper than the alternative:
  ``chunk_id`` stays a pure function of position, so the same box always carries the same
  id and the manifest can skip it on a later, differently-shaped run. A clipped chunk
  would make the id mean different boxes in different runs.
- No cell ever falls in two chunks, because parent blocks tile the grid exactly. Bronze
  rows from two chunks can therefore be concatenated without deduplication.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from src.grid.encoding import parent_code
from src.grid.spec import NLAT, NLON, RESOLUTION

# Snap tolerance for extent -> cell-index conversion: a hair under half a cell, so an
# extent given on cell edges (the usual case) includes the cells it visibly touches
# instead of losing a row to float noise.
_EPS = 1e-9


@dataclass(frozen=True)
class Chunk:
    """One export unit: a ``k x k`` block of parents, aligned to the canonical grid."""

    chunk_id: str
    extent: list[float]  # [S, W, N, E], cell edges
    parents_per_side: int
    n_parents: int  # parents actually inside the grid (edge blocks are short)
    parent_ids: tuple[str, ...]


def chunk_id(parents_per_side: int, c_row: int, c_col: int) -> str:
    """Deterministic id for a chunk position, e.g. ``s10r009c-007``.

    The size is part of the id so a ladder can put two sizes in one bronze dir without
    either overwriting the other's Parquet. Column indices are signed (west of Greenwich
    is negative) and zero-padded to a fixed width, so ids sort in geographic order.
    """
    return f"s{parents_per_side:02d}r{c_row:03d}c{c_col:+04d}"


def _cell_index_range(extent: list[float]) -> tuple[int, int, int, int]:
    """``[S, W, N, E]`` → inclusive ``(lat_idx_lo, lat_idx_hi, lon_idx_lo, lon_idx_hi)``.

    Selects every cell whose **centre** lies inside the extent.

    Longitude indices are **signed** (``lon / 0.25``, so -720..720) rather than the
    encoder's 0..1439. Both agree modulo ``NLON``, and the signed form is what makes an
    extent spanning the prime meridian (``W=-10, E=10``) a contiguous range instead of a
    range that appears to run backwards. An extent crossing the antimeridian has no such
    fix — the caller must split it — and is rejected.
    """
    south, west, north, east = extent
    if north <= south:
        raise ValueError(f"extent {extent}: N must be greater than S")
    if east <= west:
        raise NotImplementedError(
            f"extent {extent}: E must exceed W. An extent crossing the antimeridian "
            "must be split into one box each side of 180."
        )
    if west < -180.0 or east > 180.0:
        raise ValueError(f"extent {extent}: longitudes must be within -180..180")

    lat_lo = max(math.ceil((90.0 - north) / RESOLUTION - _EPS), 0)
    lat_hi = min(math.floor((90.0 - south) / RESOLUTION + _EPS), NLAT - 1)
    if lat_hi < lat_lo:
        raise ValueError(f"extent {extent} contains no cell centres")

    lon_lo = math.ceil(west / RESOLUTION - _EPS)
    lon_hi = math.floor(east / RESOLUTION + _EPS)
    if lon_hi < lon_lo:
        raise ValueError(f"extent {extent} contains no cell centres")
    # lon 180 and lon -180 are the same cell (both encode to lon index 720), so a global
    # extent must not claim both ends — that would tile the seam column twice and put the
    # same cells in two chunks.
    if lon_hi - lon_lo >= NLON:
        lon_hi = lon_lo + NLON - 1
    return lat_lo, lat_hi, lon_lo, lon_hi


def _floordiv(value: int, size: int) -> int:
    """Floor division that keeps negative longitude indices on the correct side of zero."""
    return math.floor(value / size)


def _chunk_box(c_row: int, c_col: int, side: int) -> list[float]:
    """``[S, W, N, E]`` cell-edge bounds of a chunk, from signed row/column indices.

    Computed here rather than via :func:`~src.grid.encoding.parent_bbox` because a chunk
    lives in signed longitude index space, which the encoder has no notion of. Boxes are
    clamped to ±90 / ±180: only the pole row and the seam columns are affected, and there
    is no data beyond either to lose.
    """
    lat_lo = c_row * side
    lat_hi = min((c_row + 1) * side, NLAT) - 1
    north = 90.0 - lat_lo * RESOLUTION + RESOLUTION / 2.0
    south = 90.0 - lat_hi * RESOLUTION - RESOLUTION / 2.0
    west = c_col * side * RESOLUTION - RESOLUTION / 2.0
    east = (c_col * side + side - 1) * RESOLUTION + RESOLUTION / 2.0
    return [
        max(south, -90.0),
        max(west, -180.0),
        min(north, 90.0),
        min(east, 180.0),
    ]


def _parent_ids(p_row_lo: int, p_row_hi: int, p_col_lo: int, p_col_hi: int, b: int):
    """Parent codes for a block of parent indices, via one representative cell each."""
    out = []
    for p_row in range(p_row_lo, p_row_hi + 1):
        lat = 90.0 - (p_row * b) * RESOLUTION
        for p_col in range(p_col_lo, p_col_hi + 1):
            lon = (p_col * b) * RESOLUTION
            out.append(parent_code(lat, lon, b))
    return tuple(out)


def tile_extent(
    extent: list[float], parents_per_side: int, *, b: int = 4
) -> list[Chunk]:
    """Tile ``extent`` into canonical ``k x k``-parent chunks; returns them row-major.

    Every chunk overlapping the extent is returned whole (see the module docstring on why
    they are not clipped). Chunks are returned north-to-south, west-to-east.
    """
    if parents_per_side < 1:
        raise ValueError("parents_per_side must be >= 1")
    k = parents_per_side
    lat_lo, lat_hi, lon_lo, lon_hi = _cell_index_range(extent)
    side = k * b  # cells per chunk side

    c_row_lo, c_row_hi = (lat_lo // b) // k, (lat_hi // b) // k
    # Floor division on the signed longitude index, so a chunk column west of the prime
    # meridian lands in the block below zero rather than truncating toward it.
    c_col_lo, c_col_hi = _floordiv(lon_lo, side), _floordiv(lon_hi, side)

    max_p_row = (NLAT - 1) // b
    chunks = []
    for c_row in range(c_row_lo, c_row_hi + 1):
        p_row_lo = c_row * k
        p_row_hi = min(p_row_lo + k - 1, max_p_row)
        for c_col in range(c_col_lo, c_col_hi + 1):
            p_col_lo = c_col * k
            p_col_hi = p_col_lo + k - 1
            box = _chunk_box(c_row, c_col, side)
            ids = _parent_ids(p_row_lo, p_row_hi, p_col_lo, p_col_hi, b)
            chunks.append(
                Chunk(
                    chunk_id=chunk_id(k, c_row, c_col),
                    extent=box,
                    parents_per_side=k,
                    n_parents=len(ids),
                    parent_ids=ids,
                )
            )
    return chunks
