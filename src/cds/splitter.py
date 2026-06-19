"""Adaptive cost-driven CDS download splitter (PLANNING.md §11.2).

The new CDS enforces a per-request **cost limit** and ``cdsapi`` raises only generic
exceptions — so we probe adaptively. ``submit`` tries a request; on a **cost** rejection
it shrinks the request **time-first** (year → semester → quarter → month → spatial tiles
as a last resort) and recurses; on auth / network / transient errors it retries with
backoff and does **not** split. The first granularity that works is cached per extent and
reused, so the ~350-request backfill does not re-probe every time.

Time-first because temporal chunks reassemble trivially (per-variable Parquet is
date-indexed → concatenate) and shrinking the period reduces cost under *any* CDS cost
model; spatial tiling only helps a volume-sensitive limit, so it is the fallback (§11.2).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Callable

# Months-per-chunk ladder: full year → semesters → quarters → single months (§11.2).
# Below a single month we fall back to spatial tiling rather than per-day requests.
_GRANULARITY_LADDER = (12, 6, 3, 1)
_MIN_TILE_DEG = 0.25  # one 0.25° cell — the spatial floor

# extent_key -> smallest months-per-chunk known to work for that extent. Lets the DAG
# start subsequent (variable, year) requests at the proven granularity (avoid re-probing
# 350+ times). Module-level / per-process; reset_cache() clears it for tests.
_granularity_cache: dict[str, int] = {}


def reset_cache() -> None:
    """Clear the per-process granularity cache (tests)."""
    _granularity_cache.clear()


def get_cached_granularity(extent_key: str) -> int | None:
    """Return the proven months-per-chunk for ``extent_key``, or ``None`` if unprobed."""
    return _granularity_cache.get(extent_key)


def _record_granularity(extent_key: str, months_per_chunk: int) -> None:
    cur = _granularity_cache.get(extent_key)
    if cur is None or months_per_chunk < cur:
        _granularity_cache[extent_key] = months_per_chunk


def is_cost_error(exc: BaseException) -> bool:
    """True if ``exc`` looks like a CDS cost / request-too-large rejection (§11.2).

    ``cdsapi`` has no typed ``CostLimitExceeded`` — every failure is a generic
    exception — so we classify by HTTP 400 plus cost wording in the message. Auth,
    network and transient errors must NOT match (they are retried, not split).
    """
    msg = str(exc).lower()
    status = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    has_400 = status == 400 or "400" in msg
    cost_words = ("cost", "too large", "too big", "request is too", "exceed")
    return has_400 and any(w in msg for w in cost_words)


def retry_with_backoff(
    fn: Callable[[], object],
    *,
    attempts: int = 4,
    base_delay: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> object:
    """Call ``fn`` with bounded exponential backoff on transient/auth/network errors.

    A cost error is re-raised immediately (the caller splits instead of retrying).
    """
    last: BaseException | None = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — cdsapi is untyped; classify by message
            if is_cost_error(e):
                raise
            last = e
            if i < attempts - 1:
                sleep(base_delay * (2**i))
    assert last is not None
    raise last


def _month_groups(months: list[str], size: int) -> list[list[str]]:
    """Partition ``months`` into consecutive groups of at most ``size``."""
    return [months[i : i + size] for i in range(0, len(months), size)]


def _next_size(current_len: int) -> int | None:
    """Next-finer months-per-chunk below ``current_len``, or ``None`` for spatial."""
    for size in _GRANULARITY_LADDER:
        if size < current_len:
            return size
    return None  # already a single month → caller falls back to spatial tiling


def _snap(x: float) -> float:
    return round(x / _MIN_TILE_DEG) * _MIN_TILE_DEG


def split_area(area: list[float]) -> list[list[float]]:
    """Halve ``area`` ([N, W, S, E]) along its longer side, snapped to 0.25° vertices.

    Raises if the area is already at the single-cell floor (cannot subdivide further).
    """
    n, w, s, e = area
    ns_span, ew_span = n - s, e - w
    if ns_span <= _MIN_TILE_DEG and ew_span <= _MIN_TILE_DEG:
        raise RuntimeError(f"cannot split area below the {_MIN_TILE_DEG}° floor: {area}")
    if ns_span >= ew_span:
        mid = _snap((n + s) / 2)
        mid = min(max(mid, s + _MIN_TILE_DEG), n - _MIN_TILE_DEG)
        return [[n, w, mid, e], [mid, w, s, e]]
    mid = _snap((w + e) / 2)
    mid = min(max(mid, w + _MIN_TILE_DEG), e - _MIN_TILE_DEG)
    return [[n, w, s, mid], [n, mid, s, e]]


def split(request: dict) -> list[dict]:
    """Yield finer sub-requests of ``request``, time-first then spatial (§11.2).

    Splits the ``month`` list down the ladder (year → semester → quarter → month); once
    at a single month, splits ``area`` instead.
    """
    months = list(request["month"])
    size = _next_size(len(months))
    if size is not None:
        return [{**request, "month": grp} for grp in _month_groups(months, size)]
    # Single month and still too large → spatial tiling (last resort).
    return [{**request, "area": tile} for tile in split_area(list(request["area"]))]


def _target_name(request: dict, depth: int, seq: int) -> str:
    months = request["month"]
    n, w, s, e = request["area"]
    return (
        f"{request['variable']}_{request['year']}"
        f"_m{months[0]}-{months[-1]}_d{depth}_{seq}.nc"
    )


def submit(
    client,
    dataset: str,
    request: dict,
    target_dir: str | Path,
    *,
    extent_key: str,
    max_depth: int = 6,
    _depth: int = 0,
    _seq: list[int] | None = None,
) -> list[Path]:
    """Retrieve ``request``, splitting time-first on a cost rejection (§11.2).

    Returns the accepted partial netCDF paths (one per sub-request that succeeded). The
    caller concatenates them. On auth/network/transient errors it retries with backoff;
    it never splits those.
    """
    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    seq = _seq if _seq is not None else [0]

    seq[0] += 1
    target = target_dir / _target_name(request, _depth, seq[0])

    def _do() -> Path:
        return client.retrieve(dataset, request, target)

    try:
        path = _do()
    except Exception as e:  # noqa: BLE001 — cdsapi is untyped; classify by message
        if not is_cost_error(e):
            # Transient / auth / network — back off and retry, do NOT split.
            return [retry_with_backoff(_do)]  # type: ignore[list-item]
        if _depth >= max_depth:
            raise RuntimeError(
                f"max split depth {max_depth} reached, still cost-rejected: {request}"
            ) from e
        paths: list[Path] = []
        for sub in split(request):
            paths += submit(
                client,
                dataset,
                sub,
                target_dir,
                extent_key=extent_key,
                max_depth=max_depth,
                _depth=_depth + 1,
                _seq=seq,
            )
        return paths

    # Leaf succeeded — remember how fine we had to go for this extent.
    _record_granularity(extent_key, len(request["month"]))
    return [path]
