"""EE restart (``attempt``) handling: cap it, record it, and never lose it.

An export too big for one worker does not fail — EE restarts it, and the task sits there
consuming a queue slot until something cancels it. That is exactly what happened to the
Brazil whole-extent run (``attempt`` 2 and 3 in
``.localdata/calib_br/bronze/_gee_metrics.jsonl``). These tests lock the two behaviours a
chunk-size probe depends on: stop paying at a chosen attempt, and keep the count even when
the terminal status no longer carries it.
"""

from __future__ import annotations

import pytest

import src.gee.metrics as met
from src.gee.export import task_attempt, wait_for_task
from src.gee.metrics import RunMetrics


class _Task:
    """Fake EE task that walks a scripted list of status dicts."""

    def __init__(self, statuses):
        self.id = "FAKETASK"
        self._statuses = list(statuses)
        self._i = 0
        self.cancelled = False

    def status(self):
        status = self._statuses[min(self._i, len(self._statuses) - 1)]
        self._i += 1
        return status

    def cancel(self):
        self.cancelled = True

    def start(self):  # pragma: no cover — only start_export calls this
        pass


def _running(attempt: int) -> dict:
    return {"state": "RUNNING", "attempt": attempt, "id": "FAKETASK"}


def _completed() -> dict:
    return {"state": "COMPLETED", "attempt": 1, "batch_eecu_usage_seconds": 60.0}


# --- task_attempt -----------------------------------------------------------------
@pytest.mark.parametrize(
    "status,expected",
    [
        ({"attempt": 3}, 3),
        ({"attempt": "2"}, 2),
        ({}, 1),
        ({"attempt": None}, 1),
        ({"attempt": 0}, 1),
        (None, 1),
    ],
)
def test_task_attempt_defaults_to_one(status, expected):
    assert task_attempt(status) == expected


# --- the cap ----------------------------------------------------------------------
def test_wait_aborts_and_cancels_once_attempts_exceed_the_cap():
    task = _Task([_running(1), _running(2), _running(3)])
    with pytest.raises(RuntimeError, match="max_attempts=2"):
        wait_for_task(task, poll_interval=0, max_attempts=2, sleep=lambda _s: None)
    assert task.cancelled, "an over-budget task must be cancelled, not left running"


def test_wait_tolerates_a_cancel_that_itself_fails():
    class _Stubborn(_Task):
        def cancel(self):
            raise RuntimeError("EE said no")

    task = _Stubborn([_running(5)])
    # The diagnosis must survive: the raised error is about attempts, not about cancel.
    with pytest.raises(RuntimeError, match="max_attempts"):
        wait_for_task(task, poll_interval=0, max_attempts=2, sleep=lambda _s: None)


def test_attempts_within_the_cap_run_to_completion():
    task = _Task([_running(1), _running(2), _completed()])
    status = wait_for_task(task, poll_interval=0, max_attempts=2, sleep=lambda _s: None)
    assert status["state"] == "COMPLETED"
    assert not task.cancelled


def test_no_cap_means_ee_restarts_are_not_policed():
    task = _Task([_running(9), _completed()])
    status = wait_for_task(task, poll_interval=0, sleep=lambda _s: None)
    assert status["state"] == "COMPLETED"


# --- recording --------------------------------------------------------------------
def test_on_poll_sees_every_status_including_the_terminal_one():
    task = _Task([_running(1), _running(2), _completed()])
    seen = []
    wait_for_task(
        task, poll_interval=0, on_poll=seen.append, sleep=lambda _s: None
    )
    assert [s["state"] for s in seen] == ["RUNNING", "RUNNING", "COMPLETED"]


def test_metrics_keeps_the_peak_attempt_when_the_terminal_status_drops_it():
    m = RunMetrics(kind="probe", dataset="d", name_prefix="p", extent=[0, 0, 1, 1])
    m.note_poll(_running(1))
    m.note_poll(_running(3))
    m.note_poll({"state": "CANCELLED"})  # no attempt field at all
    assert m.attempts == 3
    assert m.to_record()["attempts"] == 3


def test_run_export_records_attempts_and_cap_on_a_failed_run(monkeypatch, tmp_path):
    task = _Task([_running(1), _running(2), _running(3)])
    monkeypatch.setattr(met, "start_export", lambda *a, **kw: task)
    m = RunMetrics(kind="probe", dataset="d", name_prefix="p", extent=[0, 0, 1, 1])
    with pytest.raises(RuntimeError, match="max_attempts=2"):
        met.run_export(
            None,
            [0, 0, 1, 1],
            bucket="b",
            name_prefix="p",
            description="d",
            dest_dir=tmp_path,
            metrics=m,
            max_attempts=2,
            poll_interval=0,
        )
    rec = m.to_record()
    assert rec["attempts"] == 3
    assert rec["max_attempts"] == 2
    assert rec["task_id"] == "FAKETASK"
    assert rec["schema_version"] == 2
