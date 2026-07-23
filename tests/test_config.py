"""Config resolver tests — the per-run data_root override (multi-region bronze)."""

from pathlib import Path

from src.config import resolve_bronze_dir


def test_data_root_override_points_at_that_root():
    assert resolve_bronze_dir("/data/hn") == Path("/data/hn/bronze")
    assert resolve_bronze_dir(Path("/data/uy")) == Path("/data/uy/bronze")


def test_blank_or_none_falls_back_to_env(monkeypatch):
    # No data_root -> exactly load_config().paths.bronze_dir, so single-region runs are
    # unchanged regardless of what DATA_DIR is set to.
    monkeypatch.setenv("DATA_DIR", "/data")
    monkeypatch.setenv("POSTGRES_PASSWORD", "x")
    monkeypatch.setenv("CDS_KEY", "x:y")
    assert resolve_bronze_dir(None) == Path("/data/bronze")
    assert resolve_bronze_dir("") == Path("/data/bronze")


def test_two_regions_resolve_to_separate_trees():
    # The whole point: distinct roots never share a bronze tree or a manifest.
    assert resolve_bronze_dir("/data/uy") != resolve_bronze_dir("/data/hn")
