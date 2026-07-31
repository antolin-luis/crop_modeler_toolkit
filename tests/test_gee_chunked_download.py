"""The chunked bronze path: one Parquet and one manifest entry per chunk, not per year.

A chunk run must not look like a completed variable-year to the next run — that is what
would make a 25-chunk Brazil year stop after its first chunk and report itself done.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest

import src.gee.download as dl
import src.gee.metrics as met
from src.cds.manifest import Manifest
from src.gee.chunks import tile_extent
from src.gee.export import BlobStats
from tests.test_gee_stream import _FakeClient, _write_tiff

B = 4
# One 5-parent chunk over the Uruguay-ish box the runbook demo uses.
CHUNK = tile_extent([-35.0, -58.0, -30.0, -53.0], 5)[0]


@pytest.fixture()
def patched(tmp_path, monkeypatch):
    """Route the EE/GCS boundary at a real 3x3x3 GeoTIFF, as test_gee_stream does."""
    tiff = tmp_path / "src.tif"
    _write_tiff(tiff, ["2020-01-01", "2020-01-02", "2020-01-03"])
    monkeypatch.setattr(dl, "tz_zones", lambda *a, **k: [(-3.0, None), (-4.0, None)])
    monkeypatch.setattr(dl, "build_daily_collection", lambda *a, **k: object())
    monkeypatch.setattr(met, "start_export", lambda *a, **k: object())
    monkeypatch.setattr(met, "wait_for_task", lambda *a, **k: None)
    size = tiff.stat().st_size
    monkeypatch.setattr(
        met,
        "download_prefix_measured",
        lambda *a, **k: ([tiff], BlobStats(1, size, size, size)),
    )
    return tmp_path / "bronze"


def _run(bronze, *, chunk=CHUNK, manifest=None, **kw):
    rec: dict = {}
    out = dl.download_variable_year(
        _FakeClient(),
        "tmin",
        2020,
        [-90.0, -180.0, 90.0, 180.0],  # deliberately wrong: the chunk box must win
        tz_asset="projects/test/assets/tz",
        manifest=manifest or Manifest.for_bronze_dir(bronze),
        bronze_dir=bronze,
        b=B,
        chunk_days=2,
        metrics_out=rec,
        chunk=chunk,
        **kw,
    )
    return out, rec


def test_chunk_box_overrides_the_passed_extent(patched):
    _, rec = _run(patched)
    assert rec["extent"] == CHUNK.extent


def test_parquet_and_gcs_prefix_carry_the_chunk_id(patched):
    out, rec = _run(patched)
    assert out.name == f"tmin_2020__{CHUNK.chunk_id}.parquet"
    assert rec["name_prefix"].endswith(f"tmin_2020__{CHUNK.chunk_id}")
    assert rec["chunk_id"] == CHUNK.chunk_id
    assert pq.read_table(out).num_rows > 0


def test_a_finished_chunk_does_not_mark_the_year_done(patched):
    manifest = Manifest.for_bronze_dir(patched)
    _run(patched, manifest=manifest)
    assert manifest.is_spatial_chunk_done("tmin", 2020, CHUNK.chunk_id)
    assert not manifest.is_var_year_done("tmin", 2020)


def test_two_chunks_of_one_year_coexist(patched):
    manifest = Manifest.for_bronze_dir(patched)
    other = tile_extent([-35.0, -58.0, -30.0, -53.0], 5)[1]
    out_a, _ = _run(patched, manifest=manifest)
    out_b, _ = _run(patched, chunk=other, manifest=manifest)
    assert out_a != out_b and out_a.exists() and out_b.exists()
    assert manifest.is_spatial_chunk_done("tmin", 2020, other.chunk_id)
    # Two runs, two cost records — a size ladder needs both.
    lines = (patched / "_gee_metrics.jsonl").read_text().splitlines()
    assert len(lines) == 2


def test_a_done_chunk_is_skipped_on_re_run(patched):
    manifest = Manifest.for_bronze_dir(patched)
    out, _ = _run(patched, manifest=manifest)
    mtime = out.stat().st_mtime_ns
    out2, rec2 = _run(patched, manifest=manifest)
    assert out2 == out and out.stat().st_mtime_ns == mtime
    assert rec2 == {}, "a manifest hit measures nothing, so it records nothing"


def test_record_carries_the_chunk_context_a_ladder_reads_back(patched):
    _, rec = _run(patched, land_parents=17, parallel=4, max_attempts=2)
    assert rec["parents"] == CHUNK.n_parents == 25
    assert rec["land_parents"] == 17
    assert rec["parallel"] == 4
    assert rec["max_attempts"] == 2
    assert rec["n_zones"] == 2  # the §9.3 suspect, now recorded per run
    assert rec["schema_version"] == 2
    assert len(json.dumps(rec)) < 4096  # the append_record single-write bound


def test_unchunked_runs_keep_the_v1_shape(patched):
    rec: dict = {}
    manifest = Manifest.for_bronze_dir(patched)
    out = dl.download_variable_year(
        _FakeClient(),
        "tmin",
        2020,
        [-35.0, -58.0, -30.0, -53.0],
        tz_asset="projects/test/assets/tz",
        manifest=manifest,
        bronze_dir=patched,
        b=B,
        chunk_days=2,
        metrics_out=rec,
    )
    assert out.name == "tmin_2020.parquet"
    assert manifest.is_var_year_done("tmin", 2020)
    assert rec["chunk_id"] is None and rec["parents"] is None
