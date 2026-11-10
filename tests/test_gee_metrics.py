"""Cost-record tests: the JSONL store, EECU extraction, and unit accounting.

No live GEE / GCS / DB. These guard the two properties the calibration depends on: a
missing EECU reading stays ``None`` (never a silent 0.0 that would corrupt
``eecu_per_unit``), and the record store is append-only and tolerant of a torn tail.
"""

import json

import pytest

import pandas as pd

from src.gee.export import BlobStats
from src.gee.metrics import (
    RunMetrics,
    append_record,
    ee_timings,
    eecu_hours,
    iter_records,
    metrics_path,
)


def _metrics(**kw) -> RunMetrics:
    base = dict(
        kind="bronze_var_year",
        dataset="ECMWF/ERA5/HOURLY",
        name_prefix="bronze-gee/tmax_2020",
        extent=[-35.0, -58.5, -30.0, -53.0],
        variable="tmax",
        year=2020,
    )
    base.update(kw)
    return RunMetrics(**base)


def _frame(cells, dates, start=0.0):
    rows = [
        {"child_id": c, "parent_id": c[:2], "date": d, "value": start + i}
        for i, (c, d) in enumerate((c, d) for d in dates for c in cells)
    ]
    return pd.DataFrame(rows)


# --- record store -----------------------------------------------------------------
def test_append_and_iter_round_trip(tmp_path):
    path = metrics_path(tmp_path)
    assert path.name == "_gee_metrics.jsonl"
    append_record(path, {"run_id": "a", "bytes_remote": 10})
    append_record(path, {"run_id": "b", "bytes_remote": 20})

    recs = list(iter_records(path))
    assert [r["run_id"] for r in recs] == ["a", "b"]
    assert sum(r["bytes_remote"] for r in recs) == 30


def test_append_is_not_overwrite(tmp_path):
    """The property _manifest.json cannot provide: the same key measured twice."""
    path = metrics_path(tmp_path)
    append_record(path, {"variable": "tmax", "year": 2020, "extent": "uy"})
    append_record(path, {"variable": "tmax", "year": 2020, "extent": "hn"})
    assert [r["extent"] for r in iter_records(path)] == ["uy", "hn"]


def test_torn_tail_is_skipped_not_fatal(tmp_path):
    path = metrics_path(tmp_path)
    append_record(path, {"run_id": "good"})
    with open(path, "a") as fh:
        fh.write('{"run_id": "trunca')
    recs = list(iter_records(path))
    assert [r["run_id"] for r in recs] == ["good"]


def test_missing_file_yields_nothing(tmp_path):
    assert list(iter_records(metrics_path(tmp_path))) == []


# --- EECU -------------------------------------------------------------------------
def test_eecu_hours_present():
    assert eecu_hours({"batch_eecu_usage_seconds": 7200}) == 2.0


def test_eecu_hours_absent_is_none_never_zero():
    # A 0.0 here would silently zero eecu_per_unit — the number the calibration exists
    # to produce. None means "read it off the EE task list via task_id".
    for status in ({}, None, {"state": "COMPLETED"}, {"batch_eecu_usage_seconds": None}):
        assert eecu_hours(status) is None


def test_ee_timings_splits_queue_from_compute():
    t = ee_timings(
        {
            "creation_timestamp_ms": 1_000_000,
            "start_timestamp_ms": 1_030_000,
            "update_timestamp_ms": 1_100_000,
        }
    )
    assert t["ee_queue_s"] == 30.0
    assert t["ee_compute_s"] == 70.0


def test_ee_timings_tolerates_none():
    assert ee_timings(None)["ee_queue_s"] is None


# --- unit accounting --------------------------------------------------------------
def test_note_encode_chunk_accumulates_across_windows():
    m = _metrics()
    m.note_encode_chunk(_frame(["AAAA", "AAAB"], ["2020-01-01", "2020-01-02"]))
    m.note_encode_chunk(_frame(["AAAA", "AAAC"], ["2020-01-03"]))
    rec = m.to_record()
    assert rec["bronze_rows"] == 6
    assert rec["cells"] == 3  # AAAA seen twice, counted once
    assert rec["days"] == 3
    assert rec["n_units"] == 9
    assert rec["cells_exact"] is True


def test_fine_grid_frames_are_counted_via_cell_column():
    """The 0.05° CHIRPS path emits ``fine_id``, not ``child_id``.

    Regression: ``note_encode_chunk`` hardcoded ``child_id``, so every CHIRPS bronze task
    died with ``KeyError: 'child_id'`` *after* its export had already completed and spent
    EECU. The cost probe never caught it because it passes ``track_cells=False``.
    """
    rows = [
        {"fine_id": c, "fparent_id": c[:2], "date": d, "value": 1.0}
        for d in ("2020-01-01", "2020-01-02")
        for c in ("60ST4", "60ST5")
    ]
    m = _metrics(cell_column="fine_id")
    m.note_encode_chunk(pd.DataFrame(rows))
    rec = m.to_record()
    assert rec["cells"] == 2
    assert rec["days"] == 2
    assert rec["bronze_rows"] == 4


def test_a_frame_missing_the_cell_column_says_which_column_it_wanted():
    """Fail loudly rather than fall back — a wrong guess would corrupt the cost model."""
    m = _metrics(cell_column="fine_id")
    with pytest.raises(KeyError, match="cell_column"):
        m.note_encode_chunk(_frame(["AAAA"], ["2020-01-01"]))


def test_cells_derived_when_not_tracked():
    m = _metrics(track_cells=False)
    m.note_encode_chunk(_frame(["AAAA", "AAAB"], ["2020-01-01", "2020-01-02"]))
    rec = m.to_record()
    assert rec["bronze_rows"] == 4
    assert rec["days"] == 2
    assert rec["cells"] == 2  # bronze_rows / days
    assert rec["cells_exact"] is False  # ...and says so


def test_note_units_for_probes():
    m = _metrics(kind="probe", dataset="NOAA/GFS0P25", variable=None, year=None)
    m.note_units(cells=700, days=16)
    rec = m.to_record()
    assert rec["n_units"] == 11_200
    assert rec["bronze_rows"] == 0  # a probe encodes nothing


def test_derived_rates_and_compression():
    m = _metrics()
    m.note_export(
        task=None,
        status={"state": "COMPLETED", "id": "T1", "batch_eecu_usage_seconds": 3600},
        seconds=12.0,
    )
    m.note_download(blobs=BlobStats(2, 1000, 1000, 600), seconds=3.0)
    m.note_units(cells=10, days=50)
    m.note_raster_chunk(1000)  # 500 of 1000 pixels are land
    rec = m.to_record()

    assert rec["task_id"] == "T1" and rec["task_state"] == "COMPLETED"
    assert rec["eecu_hours"] == 1.0
    assert rec["n_units"] == 500
    assert rec["bytes_per_unit"] == 2.0
    assert rec["eecu_per_unit"] == 0.002
    assert rec["compression_ratio"] == 4.0  # 1000 raster px x 4 B / 1000 B
    assert rec["land_fraction"] == 0.5
    assert rec["n_blobs"] == 2 and rec["bytes_per_blob_max"] == 600
    assert rec["t_export_s"] == 12.0 and rec["t_download_s"] == 3.0


def test_compression_is_not_penalised_by_ocean():
    """The bug this replaced: a sea-heavy bbox read as 0.80x 'compression'.

    Same file, same codec, same bytes — only the land fraction differs. Compression must
    not move; bytes_per_unit must, because useful values are what you are paying for.
    """
    recs = []
    for land_cells in (500, 100):
        m = _metrics()
        m.note_download(blobs=BlobStats(1, 1000, 1000, 1000), seconds=1.0)
        m.note_units(cells=land_cells, days=1)
        m.note_raster_chunk(1000)
        recs.append(m.to_record())

    assert recs[0]["compression_ratio"] == recs[1]["compression_ratio"] == 4.0
    assert recs[0]["land_fraction"] == 0.5 and recs[1]["land_fraction"] == 0.1
    assert recs[0]["bytes_per_unit"] == 2.0 and recs[1]["bytes_per_unit"] == 10.0


def test_null_eecu_leaves_rate_null_not_zero():
    m = _metrics()
    m.note_export(task=None, status={"state": "COMPLETED"}, seconds=1.0)
    m.note_download(blobs=BlobStats(1, 100, 100, 100), seconds=1.0)
    m.note_units(cells=5, days=5)
    rec = m.to_record()
    assert rec["eecu_hours"] is None
    assert rec["eecu_per_unit"] is None
    assert rec["bytes_per_unit"] == 4.0  # unaffected
    assert rec["compression_ratio"] is None  # no raster counted -> not invented
    assert rec["land_fraction"] is None


def test_cancelled_export_still_records_id_eecu_and_timing(monkeypatch):
    """Regression: a real cancelled Brazil run wrote 7 all-null records.

    `note_export` used to fire only after `wait_for_task` returned, so cancelled and
    failed exports lost their task_id, their elapsed time, and the EECU they had already
    burned before dying — the exact runs whose cost you most need to account for.
    """
    from src.gee import metrics as met

    class _Task:
        id = "CANCELLED_TASK_1"

        def status(self):
            return {
                "state": "CANCELLED",
                "id": self.id,
                "batch_eecu_usage_seconds": 1800,  # burned before cancellation
            }

    monkeypatch.setattr(met, "start_export", lambda *a, **k: _Task())
    monkeypatch.setattr(
        met, "wait_for_task", lambda *a, **k: (_ for _ in ()).throw(
            RuntimeError("GEE export CANCELLED: Cancelled.")
        )
    )
    m = _metrics()
    with pytest.raises(RuntimeError, match="CANCELLED"):
        met.run_export(
            object(), [0, 0, 1, 1], bucket="b", name_prefix="p",
            description="d", dest_dir="/tmp", metrics=m,
        )
    rec = m.to_record()
    assert rec["task_id"] == "CANCELLED_TASK_1"   # recoverable on the EE task list
    assert rec["task_state"] == "CANCELLED"
    assert rec["eecu_hours"] == 0.5               # spent, not silently dropped
    assert rec["t_export_s"] is not None


def test_record_of_a_failed_run_keeps_the_export_evidence():
    m = _metrics()
    m.note_export(
        task=None, status={"state": "COMPLETED", "id": "T9"}, seconds=42.0
    )
    m.note_error(RuntimeError("quota exhausted"))
    rec = m.to_record()
    assert rec["task_id"] == "T9"
    assert rec["t_export_s"] == 42.0
    assert "quota exhausted" in rec["error"]
    assert rec["n_units"] is None  # nothing encoded, nothing invented


def test_record_is_json_serialisable_and_small(tmp_path):
    m = _metrics(sample="E1", b=4, chunk_days=30, gee_project="proj")
    m.note_export(
        task=None,
        status={"state": "COMPLETED", "id": "T1", "batch_eecu_usage_seconds": 10},
        seconds=1.0,
    )
    m.note_download(blobs=BlobStats(1, 5, 5, 5), seconds=1.0)
    m.note_encode_chunk(_frame(["AAAA"], ["2020-01-01"]))
    rec = m.to_record()

    line = json.dumps(rec, sort_keys=True, default=str)
    assert len(line) < 4096  # single-write atomicity bound under gee_pool concurrency
    path = metrics_path(tmp_path)
    append_record(path, rec)
    assert list(iter_records(path))[0]["sample"] == "E1"
