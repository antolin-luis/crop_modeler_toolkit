"""Deterministic cell encoding for the 0.05° CHIRPS grid (``src/grid/fine_spec.py``).

The 0.05° analogue of ``src/grid/encoding.py``, kept as a separate module rather than a
parameterization of it: that module is load-bearing for every ERA5 row already in the
database, and CLAUDE.md forbids touching its constants. The duplication is deliberate and
bounded — the two grids genuinely differ in binning rule and code width, so a shared
implementation would be a parameter soup with a footgun in the middle.

Binning rule, and the one thing to get right: **cell edges** fall on multiples of 0.05, so a
coordinate is binned with ``floor``, not ``round``. Cell ``i`` spans ``[i*0.05,
(i+1)*0.05)`` and its centre is at ``(i + 0.5) * 0.05``. Compare ``encoding.py``, where a
0.25° cell *centre* is on the multiple and ``round`` finds the nearest one.

Indices are computed by multiplying by ``1/RESOLUTION = 20`` rather than dividing by 0.05.
0.05 has no exact binary representation, and dividing by it puts coordinates that sit
exactly on a cell edge (which, on a 0.05° grid, is most round-numbered coordinates) on the
wrong side of a ``floor`` about half the time. Multiplying by 20 is exact for any coordinate
that is itself a multiple of 0.05.
"""

from __future__ import annotations

import numpy as np

from src.grid.fine_spec import ALPHABET, BLOCK_B, CODE_WIDTH, LAT_ORIGIN, NLAT, NLON, RESOLUTION

ALPH = ALPHABET
RES = RESOLUTION
_INV_RES = round(1.0 / RESOLUTION)  # 20 — see module docstring on why this is a multiply

# Snaps a coordinate that is within float dust of a cell edge onto that edge rather than
# into the cell below it. Same magnitude and same purpose as src/gee/chunks.py's _EPS.
_EPS = 1e-9


def cell_indices(lat: float, lon: float) -> tuple[int, int]:
    """``(lat_idx, lon_idx)`` of the fine cell containing ``(lat, lon)``.

    Clamped to the grid, so a coordinate outside CHIRPS's 60S..60N band returns the nearest
    edge row rather than raising — the caller decides whether that is meaningful. Longitude
    wraps, so any convention (-180..180 or 0..360) is accepted.
    """
    lat_idx = int(np.floor((LAT_ORIGIN - lat) * _INV_RES + _EPS))
    lon_idx = int(np.floor((lon % 360.0) * _INV_RES + _EPS))
    return (
        min(max(lat_idx, 0), NLAT - 1),
        min(max(lon_idx, 0), NLON - 1),
    )


def fine_code(lat: float, lon: float) -> str:
    """Encode a coordinate to its 5-char base-36 fine-cell code."""
    lat_idx, lon_idx = cell_indices(lat, lon)
    return _base36(lat_idx * NLON + lon_idx)


def code_to_latlon(code: str) -> tuple[float, float]:
    """Decode a 5-char fine code back to its cell **centre** ``(lat, lon)``.

    Longitude comes back in -180..180 for storage, matching ``era5_land_base_grid``.
    """
    lat_idx, lon_idx = code_indices(code)
    lat = LAT_ORIGIN - (lat_idx + 0.5) * RES
    lon = (lon_idx + 0.5) * RES
    if lon > 180.0:
        lon -= 360.0
    return lat, lon


def code_indices(code: str) -> tuple[int, int]:
    """Decode a fine code to its ``(lat_idx, lon_idx)``."""
    _check_width(code)
    n = 0
    for c in code:
        n = n * 36 + ALPH.index(c)
    return divmod(n, NLON)


def fine_parent_code(lat: float, lon: float, b: int = BLOCK_B) -> str:
    """Encode the parent (regular ``b x b`` block) code for a coordinate.

    ``b`` is immutable for the life of the table — it is baked into every stored
    ``fparent_id`` and every partition name.
    """
    lat_idx, lon_idx = cell_indices(lat, lon)
    return _parent_from_indices(lat_idx, lon_idx, b)


def fine_parent_of(code: str, b: int = BLOCK_B) -> str:
    """Parent code straight from a fine cell code — no coordinates, no database.

    This is what keeps a region query to one round-trip: given the ``fine_id`` set a GiST
    lookup returned, the ``fparent_id`` set needed for partition pruning is derivable in
    Python. Without both in the ``WHERE`` clause Postgres scans every partition.
    """
    return _parent_from_indices(*code_indices(code), b)


def fine_parent_indices(code: str, b: int = BLOCK_B) -> tuple[int, int]:
    """Decode a parent code back to its ``(p_row, p_col)`` block indices."""
    _check_width(code)
    n = 0
    for c in code:
        n = n * 36 + ALPH.index(c)
    return divmod(n, _n_pcols(b))


def fine_parent_bbox(p_row: int, p_col: int, b: int = BLOCK_B) -> list[float]:
    """``[S, W, N, E]`` cell-edge bounds of one parent block, in -180..180 lon.

    Simpler than its 0.25° counterpart, and legitimately so. ``NLAT`` and ``NLON`` are both
    divisible by ``BLOCK_B``, so there are no short edge blocks to special-case; and because
    parent boundaries land on integer degrees, 180° is itself a boundary and no block ever
    straddles the antimeridian. The straddle check is kept anyway — it is cheap, and it is
    the guard that would fire if someone tried a ``b`` that does not divide 3600.
    """
    if not 0 <= p_row < _n_prows(b) or not 0 <= p_col < _n_pcols(b):
        raise ValueError(f"parent block ({p_row}, {p_col}) is outside the fine grid")

    north = LAT_ORIGIN - p_row * b * RES
    south = LAT_ORIGIN - min((p_row + 1) * b, NLAT) * RES
    west = p_col * b * RES
    east = min((p_col + 1) * b, NLON) * RES
    if west >= 180.0:  # wholly in the eastern-negative half
        west, east = west - 360.0, east - 360.0
    elif east > 180.0:
        raise ValueError(
            f"parent block ({p_row}, {p_col}) straddles the antimeridian; "
            f"no single [W, E] rectangle represents it (b={b} does not divide 3600)"
        )
    return [south, west, north, east]


def fine_parent_code_bbox(code: str, b: int = BLOCK_B) -> list[float]:
    """``fine_parent_bbox`` straight from a parent code — inverse of :func:`fine_parent_code`."""
    return fine_parent_bbox(*fine_parent_indices(code, b), b)


def cell_center(lat: float, lon: float) -> tuple[float, float]:
    """The fine cell centre a coordinate snaps to — for reporting what was returned."""
    return code_to_latlon(fine_code(lat, lon))


# --- Internals -------------------------------------------------------------------

def _n_pcols(b: int) -> int:
    return -(-NLON // b)  # ceil division


def _n_prows(b: int) -> int:
    return -(-NLAT // b)


def _parent_from_indices(lat_idx: int, lon_idx: int, b: int) -> str:
    return _base36((lat_idx // b) * _n_pcols(b) + (lon_idx // b))


def _base36(n: int) -> str:
    s = ""
    for _ in range(CODE_WIDTH):
        n, r = divmod(n, 36)
        s = ALPH[r] + s
    if n:
        raise ValueError(f"value does not fit in {CODE_WIDTH} base-36 chars")
    return s


def _check_width(code: str) -> None:
    """A 4-char code is the 0.25° grid and must never be decoded here (fine_spec §2)."""
    if len(code) != CODE_WIDTH:
        raise ValueError(
            f"{code!r} is {len(code)} chars; the fine grid uses {CODE_WIDTH}. "
            "A 4-char code belongs to the 0.25° ERA5 grid (src/grid/encoding.py)."
        )


# --- Vectorized variants (grid seed / raster encode) ------------------------------
# These take integer cell INDICES, not float coordinates, so no binning is involved. They
# must agree element-wise with the scalar functions above; tests lock that.

def _base36_arr(n: np.ndarray) -> np.ndarray:
    n = np.asarray(n, dtype=np.int64)
    if n.size and (n.max() >= 36**CODE_WIDTH or n.min() < 0):
        raise ValueError(f"value out of range for {CODE_WIDTH} base-36 chars")
    digits = np.empty((CODE_WIDTH, n.size), dtype=np.int64)
    rest = n.ravel().copy()
    for i in range(CODE_WIDTH - 1, -1, -1):
        rest, digits[i] = np.divmod(rest, 36)
    chars = np.array(list(ALPH))[digits]  # (CODE_WIDTH, size)
    return np.array(["".join(col) for col in chars.T.tolist()]).reshape(n.shape)


def fine_codes(lat_idx: np.ndarray, lon_idx: np.ndarray) -> np.ndarray:
    """Vectorized :func:`fine_code` over integer indices."""
    return _base36_arr(np.asarray(lat_idx, dtype=np.int64) * NLON + np.asarray(lon_idx))


def fine_parent_codes(
    lat_idx: np.ndarray, lon_idx: np.ndarray, b: int = BLOCK_B
) -> np.ndarray:
    """Vectorized :func:`fine_parent_code` over integer indices."""
    lat_idx = np.asarray(lat_idx, dtype=np.int64)
    lon_idx = np.asarray(lon_idx, dtype=np.int64)
    return _base36_arr((lat_idx // b) * _n_pcols(b) + (lon_idx // b))


def cell_indices_arr(lat: np.ndarray, lon: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Vectorized :func:`cell_indices` — bins raster coordinate axes onto fine indices."""
    lat_i = np.floor((LAT_ORIGIN - np.asarray(lat, dtype=np.float64)) * _INV_RES + _EPS)
    lon_i = np.floor((np.asarray(lon, dtype=np.float64) % 360.0) * _INV_RES + _EPS)
    lat_i = np.clip(lat_i.astype(np.int64), 0, NLAT - 1)
    lon_i = np.clip(lon_i.astype(np.int64), 0, NLON - 1)
    return lat_i, lon_i
