"""Manifest tests (PLANNING.md §11.4) — idempotency, sub-chunk resume, atomic write."""

import json

from src.cds.manifest import Manifest


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
