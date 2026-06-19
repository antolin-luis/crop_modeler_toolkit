"""Adaptive splitter tests (PLANNING.md §11.2) — no live CDS.

A fake client raises a synthetic cost error until the request is small enough, letting
us assert the time-first split ladder, error classification, granularity caching, and the
depth / area-floor guards against runaway recursion.
"""

import pytest

from src.cds import splitter

ALL_MONTHS = [f"{m:02d}" for m in range(1, 13)]
ALL_DAYS = [f"{d:02d}" for d in range(1, 32)]


class CostError(Exception):
    status_code = 400


def _request(area=None):
    return {
        "variable": ["2m_temperature"],
        "year": "1995",
        "month": list(ALL_MONTHS),
        "day": list(ALL_DAYS),
        "area": area or [10.0, -10.0, -10.0, 10.0],
    }


class FakeClient:
    """retrieve() succeeds only when month-count <= accept_months; else cost-errors."""

    def __init__(self, accept_months: int):
        self.accept_months = accept_months
        self.attempts: list[int] = []

    def retrieve(self, dataset, request, target):
        self.attempts.append(len(request["month"]))
        if len(request["month"]) > self.accept_months:
            raise CostError("400 Client Error: cost limits exceeded")
        from pathlib import Path

        Path(target).write_text("nc")
        return Path(target)


@pytest.fixture(autouse=True)
def _clear_cache():
    splitter.reset_cache()
    yield
    splitter.reset_cache()


def test_is_cost_error_classification():
    assert splitter.is_cost_error(CostError("400: request is too large"))
    assert splitter.is_cost_error(CostError("400 cost limits exceeded"))
    assert not splitter.is_cost_error(Exception("401 invalid api key"))
    assert not splitter.is_cost_error(Exception("connection timed out"))
    assert not splitter.is_cost_error(Exception("400 bad request: unknown variable"))


def test_time_first_split_to_single_months(tmp_path):
    client = FakeClient(accept_months=1)
    paths = splitter.submit(
        client, "ds", _request(), tmp_path, extent_key="k"
    )
    assert len(paths) == 12  # one leaf per month
    # Ladder probed full year then semester then quarter before single months.
    assert client.attempts[0] == 12
    assert 6 in client.attempts and 3 in client.attempts
    assert all(p.exists() for p in paths)


def test_semester_granularity_cached(tmp_path):
    client = FakeClient(accept_months=6)
    paths = splitter.submit(client, "ds", _request(), tmp_path, extent_key="k")
    assert len(paths) == 2  # two semesters accepted
    assert splitter.get_cached_granularity("k") == 6


def test_area_floor_guard_raises(tmp_path):
    client = FakeClient(accept_months=0)  # nothing works → falls to spatial
    with pytest.raises(RuntimeError):
        splitter.submit(
            client,
            "ds",
            _request(area=[0.25, 0.0, 0.0, 0.25]),  # single cell, cannot tile
            tmp_path,
            extent_key="k",
        )


def test_max_depth_guard_raises(tmp_path):
    client = FakeClient(accept_months=0)
    with pytest.raises(RuntimeError):
        splitter.submit(
            client, "ds", _request(), tmp_path, extent_key="k", max_depth=2
        )


def test_transient_error_retries_not_splits():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise Exception("connection reset")
        return "ok"

    out = splitter.retry_with_backoff(flaky, attempts=4, sleep=lambda _s: None)
    assert out == "ok"
    assert calls["n"] == 3


def test_retry_reraises_cost_error_immediately():
    def cost():
        raise CostError("400 cost too large")

    with pytest.raises(CostError):
        splitter.retry_with_backoff(cost, attempts=4, sleep=lambda _s: None)
