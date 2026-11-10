"""Config resolver tests — the per-run data_root override (multi-region bronze)."""

from pathlib import Path

import pytest

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


@pytest.mark.parametrize("name", ["TO", "./TO", Path("TO")])
def test_a_bare_folder_name_means_a_folder_under_data_dir(name):
    """The natural thing to type must be the correct thing to type.

    Anchoring a bare name to the process CWD instead is what silently wrote a year of
    CHIRPS bronze to ``/opt/airflow/TO/bronze`` — off the bind mount, DAG green, destroyed
    by the next container recreate.
    """
    assert resolve_bronze_dir(name) == Path("/data/TO/bronze")


def test_a_name_and_its_full_path_are_interchangeable():
    assert resolve_bronze_dir("TO") == resolve_bronze_dir("/data/TO")


def test_nested_names_are_allowed():
    assert resolve_bronze_dir("br/TO") == Path("/data/br/TO/bronze")


@pytest.mark.parametrize(
    "bad",
    [
        "/TO",              # absolute, the container root -> PermissionError from mkdir
        "/opt/airflow/TO",  # absolute and writable, but not on the mounted volume
        "/data/../TO",      # escapes DATA_DIR via ..
        "../TO",            # same escape, spelled relatively
    ],
)
def test_a_root_outside_the_data_volume_is_rejected(bad):
    """Bronze written off the mounted volume does not survive a container recreate."""
    with pytest.raises(ValueError, match="DATA_DIR"):
        resolve_bronze_dir(bad)


def test_the_rejection_names_the_folder_the_caller_probably_meant():
    with pytest.raises(ValueError, match=r"'/data/TO'"):
        resolve_bronze_dir("/TO")


def test_data_dir_itself_is_allowed():
    """DATA_DIR as the root is the single-region default, spelled explicitly."""
    assert resolve_bronze_dir("/data") == Path("/data/bronze")


def test_the_boundary_follows_data_dir(monkeypatch):
    """Point DATA_DIR at another disk and names resolve there — no hardcoded /data."""
    monkeypatch.setenv("DATA_DIR", "/mnt/ssd")
    assert resolve_bronze_dir("TO") == Path("/mnt/ssd/TO/bronze")
    with pytest.raises(ValueError, match="DATA_DIR"):
        resolve_bronze_dir("/data/TO")
