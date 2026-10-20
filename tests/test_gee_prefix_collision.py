"""An unchunked export must not adopt another export's GCS shards.

Regression for the 2026-09-24 contamination: ``list_blobs(prefix=...)`` is a string match,
so ``bronze-gee/tmin_2020`` also matched every ``bronze-gee/tmin_2020__<chunk_id>-0-0.tif``
left behind by earlier chunked runs. Nothing deletes blobs, so one unchunked var-year pulled
26 foreign shards and 17.8M duplicate rows covering lat -69.75..30 — far outside its extent.
"""

from dataclasses import dataclass

from src.gee.export import export_scope, own_shards


@dataclass
class _Blob:
    name: str


def test_own_shards_keeps_this_exports_tiles():
    prefix = "bronze-gee/abc123/tmin_2020"
    blobs = [
        _Blob(f"{prefix}.tif"),
        _Blob(f"{prefix}-0000000000-0000000000.tif"),
        _Blob(f"{prefix}-0000000000-0000002048.tif"),
    ]
    assert [b.name for b in own_shards(blobs, prefix)] == [b.name for b in blobs]


def test_own_shards_rejects_chunked_shards_of_the_same_var_year():
    prefix = "bronze-gee/abc123/tmin_2020"
    mine = _Blob(f"{prefix}-0000000000-0000000000.tif")
    foreign = [
        _Blob(f"{prefix}__s02r061c-030-0000000000-0000000000.tif"),
        _Blob(f"{prefix}__s00r041c-010.tif"),
    ]
    assert [b.name for b in own_shards([mine, *foreign], prefix)] == [mine.name]


def test_own_shards_rejects_a_longer_year_that_shares_the_prefix():
    # "tmin_202" would otherwise swallow 2020..2029; the tail guard is what stops it.
    prefix = "bronze-gee/abc123/tmin_202"
    assert own_shards([_Blob("bronze-gee/abc123/tmin_2020.tif")], prefix) == []


def test_export_scope_separates_extents_and_is_stable():
    uy = export_scope([-35.0, -59.5, -30.0, -53.0], land_only=True, chunk_id=None)
    br = export_scope([-34.0, -74.0, 5.5, -34.75], land_only=True, chunk_id=None)
    assert uy != br
    # Deterministic, so a retry lands on the same GCS path instead of orphaning a copy.
    assert uy == export_scope([-35, -59.5, -30, -53], land_only=True, chunk_id=None)
    # land_only and chunk_id are part of the request, so they must move the scope too.
    assert uy != export_scope([-35.0, -59.5, -30.0, -53.0], land_only=False, chunk_id=None)
    assert uy != export_scope([-35.0, -59.5, -30.0, -53.0], land_only=True, chunk_id="s0r0c0")
