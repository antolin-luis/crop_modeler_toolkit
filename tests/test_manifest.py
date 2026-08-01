"""Manifest tests (PLANNING.md §11.4) — idempotency, sub-chunk resume, atomic write."""

import json
from concurrent.futures import ProcessPoolExecutor

from src.cds.manifest import Manifest


def _mark_spatial(args):
    """Top-level so ProcessPoolExecutor can pickle it. One fresh Manifest per process,
    which is exactly what an Airflow mapped task does."""
    path, chunk_id = args
    Manifest(path).mark_spatial_chunk_done("tmin", 2020, chunk_id)


def test_var_year_idempotency(tmp_path):
    m = Manifest(tmp_path / "_manifest.json")
    assert not m.is_var_year_done("tmax", 1995)
    m.mark_var_year_done("tmax", 1995)
    assert m.is_var_year_done("tmax", 1995)
    assert not m.is_var_year_done("tmin", 1995)


def test_chunk_idempotency(tmp_path):
    m = Manifest(tmp_path / "_manifest.json")
    assert not m.is_chunk_done("tmax", 1995, "1995-01-01", "1995-06-30")
    m.mark_chunk_done("tmax", 1995, "1995-01-01", "1995-06-30")
    assert m.is_chunk_done("tmax", 1995, "1995-01-01", "1995-06-30")
    assert not m.is_chunk_done("tmax", 1995, "1995-07-01", "1995-12-31")


def test_spatial_chunk_idempotency(tmp_path):
    m = Manifest(tmp_path / "_manifest.json")
    assert not m.is_spatial_chunk_done("tmin", 2020, "s10r009c007")
    m.mark_spatial_chunk_done("tmin", 2020, "s10r009c007")
    assert m.is_spatial_chunk_done("tmin", 2020, "s10r009c007")
    assert not m.is_spatial_chunk_done("tmin", 2020, "s10r009c008")
    # A part landing never implies the year did — the caller owning the full extent
    # decides that, and a chunked backfill would otherwise stop after its first chunk.
    assert not m.is_var_year_done("tmin", 2020)


def test_spatial_and_time_chunks_share_a_list_without_colliding(tmp_path):
    m = Manifest(tmp_path / "_manifest.json")
    m.mark_chunk_done("tmax", 2020, "2020-01-01", "2020-01-31")
    m.mark_spatial_chunk_done("tmax", 2020, "2020-01-01")  # id shaped like a date
    assert m.is_chunk_done("tmax", 2020, "2020-01-01", "2020-01-31")
    assert m.is_spatial_chunk_done("tmax", 2020, "2020-01-01")
    assert not m.is_spatial_chunk_done("tmax", 2020, "2020-01-31")


def test_restart_skips_done(tmp_path):
    path = tmp_path / "_manifest.json"
    m1 = Manifest(path)
    m1.mark_chunk_done("precip", 2000, "2000-01-01", "2000-03-31")
    m1.mark_var_year_done("tmax", 2000)
    # A fresh instance reads the persisted state.
    m2 = Manifest(path)
    assert m2.is_chunk_done("precip", 2000, "2000-01-01", "2000-03-31")
    assert m2.is_var_year_done("tmax", 2000)


def test_atomic_write_leaves_no_temp(tmp_path):
    m = Manifest(tmp_path / "_manifest.json")
    m.mark_var_year_done("tmax", 1995)
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == []
    # Persisted file is valid JSON.
    json.loads((tmp_path / "_manifest.json").read_text())


def test_concurrent_processes_do_not_lose_marks(tmp_path):
    """Separate processes marking different chunks must all survive.

    Regression: a chunked GEE smoke run over Brazil marked 3 of 7 completed chunks. Each
    Airflow mapped task is its own process holding a snapshot of the manifest taken at task
    start, and the whole file is rewritten on every mark — so with ``gee_pool`` > 1 the last
    writer silently erased the others. Losing a mark means re-exporting a chunk that already
    landed, which spends the EECU quota the manifest exists to protect.
    """
    path = tmp_path / "_manifest.json"
    chunk_ids = [f"s20r00{i}c-003" for i in range(8)]

    with ProcessPoolExecutor(max_workers=8) as pool:
        list(pool.map(_mark_spatial, [(path, cid) for cid in chunk_ids]))

    done = Manifest(path)
    missing = [cid for cid in chunk_ids if not done.is_spatial_chunk_done("tmin", 2020, cid)]
    assert missing == [], f"lost {len(missing)} of {len(chunk_ids)} marks: {missing}"


def test_refresh_picks_up_another_writers_mark(tmp_path):
    """A long-lived reader (the rollup task) must be able to see later marks."""
    path = tmp_path / "_manifest.json"
    reader = Manifest(path)
    Manifest(path).mark_spatial_chunk_done("tmin", 2020, "s20r004c-003")

    assert not reader.is_spatial_chunk_done("tmin", 2020, "s20r004c-003")  # stale snapshot
    reader.refresh()
    assert reader.is_spatial_chunk_done("tmin", 2020, "s20r004c-003")


def test_missing_and_corrupt_file_tolerated(tmp_path):
    # Missing file → empty manifest.
    m = Manifest(tmp_path / "_manifest.json")
    assert not m.is_var_year_done("tmax", 1995)
    # Corrupt file → treated as empty, no raise.
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    m2 = Manifest(bad)
    assert not m2.is_var_year_done("tmax", 1995)
    m2.mark_var_year_done("tmax", 1995)  # recovers cleanly
    assert Manifest(bad).is_var_year_done("tmax", 1995)
