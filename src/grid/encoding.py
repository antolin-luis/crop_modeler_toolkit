"""Deterministic cell encoding (PLANNING.md §6.2 / §6.3).

`child_id` and `parent_id` are pure functions of coordinates — no lookup table.
The math is copied verbatim from PLANNING.md, which is the authoritative source.

Contract: inputs are cell CENTERS (exact multiples of 0.25°). Callers that hold an
arbitrary coordinate must snap to the nearest center first — ``round()`` below is
banker's rounding and ties-to-even exactly on a cell boundary.
"""

import numpy as np

from src.grid.spec import ALPHABET, NLON, RESOLUTION

ALPH = ALPHABET
RES = RESOLUTION

_ALPH_ARR = np.array(list(ALPHABET))  # '<U1' lookup table for the vectorized path


def cell_code(lat: float, lon: float) -> str:
    """Encode a cell center (lat, lon) to its 4-char base-36 child code."""
    lon = lon % 360.0                      # normalize to 0..360
    lat_idx = round((90.0 - lat) / RES)    # 0..720
    lon_idx = round(lon / RES)             # 0..1439
    n = lat_idx * NLON + lon_idx
    s = ""
    for _ in range(4):
        n, r = divmod(n, 36)
        s = ALPH[r] + s
    return s


def code_to_latlon(code: str) -> tuple[float, float]:
    """Decode a 4-char child code back to its (lat, lon) cell center."""
    n = 0
    for c in code:
        n = n * 36 + ALPH.index(c)
    lat_idx, lon_idx = divmod(n, NLON)
    lon = lon_idx * RES
    if lon > 180.0:
        lon -= 360.0                       # back to -180..180 for storage
    return 90.0 - lat_idx * RES, lon


def parent_code(lat: float, lon: float, b: int) -> str:
    """Encode the parent (regular b x b super-block) code for a cell center.

    ``b`` is the block size; ``x = b**2`` children per parent. ``b`` is immutable
    for the life of the database (PLANNING.md §6.3).
    """
    lon = lon % 360.0
    lat_idx = round((90.0 - lat) / RES)
    lon_idx = round(lon / RES)
    p_row, p_col = lat_idx // b, lon_idx // b
    n_pcols = -(-NLON // b)                 # ceil division
    n = p_row * n_pcols + p_col
    s = ""
    for _ in range(4):
        n, r = divmod(n, 36)
        s = ALPH[r] + s
    return s


# --- Vectorized variants (seed build, ~1M cells; PLANNING.md §8.1) ---------------
# These take integer cell INDICES directly (the seed already holds them from a
# meshgrid), not float coordinates — so no rounding/snap is involved. They must agree
# element-wise with the scalar functions above; tests lock that.


def _base36_4(n: np.ndarray) -> np.ndarray:
    """Encode an int array to a 4-char base-36 '<U4' array (shape preserved)."""
    n = np.asarray(n, dtype=np.int64)
    flat = n.ravel()
    digits = np.empty((4, flat.size), dtype=np.int64)
    rem = flat
    for i in range(3, -1, -1):              # least-significant digit last
        rem, digits[i] = np.divmod(rem, 36)
    chars = _ALPH_ARR[digits]               # (4, N) of single chars
    out = np.char.add(
        np.char.add(chars[0], chars[1]),
        np.char.add(chars[2], chars[3]),
    )
    return out.reshape(n.shape)


def cell_codes(lat_idx: np.ndarray, lon_idx: np.ndarray) -> np.ndarray:
    """Vectorized ``cell_code`` over integer index arrays -> '<U4' child codes."""
    lat_idx = np.asarray(lat_idx, dtype=np.int64)
    lon_idx = np.asarray(lon_idx, dtype=np.int64)
    return _base36_4(lat_idx * NLON + lon_idx)


def parent_codes(lat_idx: np.ndarray, lon_idx: np.ndarray, b: int) -> np.ndarray:
    """Vectorized ``parent_code`` over integer index arrays -> '<U4' parent codes."""
    lat_idx = np.asarray(lat_idx, dtype=np.int64)
    lon_idx = np.asarray(lon_idx, dtype=np.int64)
    n_pcols = -(-NLON // b)                  # ceil division
    n = (lat_idx // b) * n_pcols + (lon_idx // b)
    return _base36_4(n)
