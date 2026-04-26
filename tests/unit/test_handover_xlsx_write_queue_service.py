from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Dict, List

import pytest

from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.service.handover_xlsx_write_queue_service import (
    HandoverXlsxWriteQueueService,
    HandoverXlsxWriteQueueTimeoutError,
)


def _config(tmp_path: Path) -> Dict[str, Any]:
    return {"_global_paths": {"runtime_state_root": str(tmp_path / ".runtime")}}


def test_xlsx_write_job_dedupes_pending_but_not_running(tmp_path: Path) -> None:
    store = ReviewBuildingDocumentStore(config=_config(tmp_path), building="A楼")

    first = store.enqueue_xlsx_write_job(
        task_type="review_excel_sync",
        dedupe_key="session-1",
        payload={"target_revision": 1},
    )
    second = store.enqueue_xlsx_write_job(
        task_type="review_excel_sync",
        dedupe_key="session-1",
        payload={"target_revision": 2},
    )

    assert second["job_id"] == first["job_id"]
    assert second["payload"]["target_revision"] == 2

    claimed = store.claim_next_xlsx_write_job()
    assert claimed is not None
    assert claimed["status"] == "running"

    third = store.enqueue_xlsx_write_job(
        task_type="review_excel_sync",
        dedupe_key="session-1",
        payload={"target_revision": 3},
    )

    assert third["job_id"] != first["job_id"]
    assert third["payload"]["target_revision"] == 3


def test_xlsx_write_queue_runs_one_building_fifo_and_continues_after_failure(tmp_path: Path) -> None:
    events: List[str] = []

    class _Queue(HandoverXlsxWriteQueueService):
        def _execute_job(self, *, building: str, job: Dict[str, Any]) -> None:
            payload = job.get("payload", {}) if isinstance(job.get("payload", {}), dict) else {}
            label = str(payload.get("label", "") or job.get("task_type", ""))
            events.append(f"start:{label}")
            if payload.get("fail"):
                raise ValueError("boom")
            events.append(f"end:{label}")

    service = _Queue(_config(tmp_path), emit_log=lambda _msg: None)
    store = ReviewBuildingDocumentStore(config=_config(tmp_path), building="A楼")
    first = store.enqueue_xlsx_write_job(task_type="test", dedupe_key="1", payload={"label": "one"})
    failed = store.enqueue_xlsx_write_job(task_type="test", dedupe_key="2", payload={"label": "two", "fail": True})
    third = store.enqueue_xlsx_write_job(task_type="test", dedupe_key="3", payload={"label": "three"})

    service.wait_for_barrier(building="A楼", timeout_sec=2.0)

    assert events == [
        "start:one",
        "end:one",
        "start:two",
        "start:three",
        "end:three",
        "start:barrier",
        "end:barrier",
    ]
    assert store.get_xlsx_write_job(first["job_id"])["status"] == "success"
    assert store.get_xlsx_write_job(failed["job_id"])["status"] == "failed"
    assert store.get_xlsx_write_job(third["job_id"])["status"] == "success"


def test_xlsx_write_queue_allows_different_buildings_to_progress_independently(tmp_path: Path) -> None:
    events: List[str] = []
    slow_started = threading.Event()
    release_slow = threading.Event()

    class _Queue(HandoverXlsxWriteQueueService):
        def _execute_job(self, *, building: str, job: Dict[str, Any]) -> None:
            payload = job.get("payload", {}) if isinstance(job.get("payload", {}), dict) else {}
            label = str(payload.get("label", "") or job.get("task_type", ""))
            events.append(f"start:{building}:{label}")
            if payload.get("block"):
                slow_started.set()
                assert release_slow.wait(2.0)
            events.append(f"end:{building}:{label}")

    service = _Queue(_config(tmp_path), emit_log=lambda _msg: None)
    store_a = ReviewBuildingDocumentStore(config=_config(tmp_path), building="A楼")
    store_b = ReviewBuildingDocumentStore(config=_config(tmp_path), building="B楼")
    store_a.enqueue_xlsx_write_job(task_type="test", dedupe_key="slow", payload={"label": "slow", "block": True})
    store_b.enqueue_xlsx_write_job(task_type="test", dedupe_key="fast", payload={"label": "fast"})

    service._start_worker(building="A楼")
    assert slow_started.wait(2.0)
    service.wait_for_barrier(building="B楼", timeout_sec=2.0)

    assert "end:B楼:fast" in events
    assert "end:A楼:slow" not in events
    release_slow.set()
    service.wait_for_barrier(building="A楼", timeout_sec=2.0)


def test_xlsx_write_queue_wait_times_out_when_worker_cannot_run(tmp_path: Path) -> None:
    class _Queue(HandoverXlsxWriteQueueService):
        def _start_worker(self, *, building: str) -> None:
            return

    service = _Queue(_config(tmp_path), emit_log=lambda _msg: None)
    store = ReviewBuildingDocumentStore(config=_config(tmp_path), building="A楼")
    job = store.enqueue_xlsx_write_job(task_type="test", dedupe_key="blocked", payload={})

    with pytest.raises(HandoverXlsxWriteQueueTimeoutError):
        service.wait_for_job(building="A楼", job_id=job["job_id"], timeout_sec=0.1)


def test_review_excel_sync_failure_marks_sync_state_failed(tmp_path: Path) -> None:
    service = HandoverXlsxWriteQueueService(_config(tmp_path), emit_log=lambda _msg: None)
    store = ReviewBuildingDocumentStore(config=_config(tmp_path), building="A楼")

    service.enqueue_review_excel_sync(
        {"building": "A楼", "session_id": "missing-session"},
        target_revision=7,
    )
    service.wait_for_barrier(building="A楼", timeout_sec=2.0)

    sync_state = store.get_sync_state("missing-session")
    assert sync_state["status"] == "failed"
    assert sync_state["pending_revision"] == 7
    assert "审核文档尚未初始化" in sync_state["error"]


def test_xlsx_write_queue_restarts_when_job_arrives_during_worker_exit(tmp_path: Path) -> None:
    processed: List[str] = []
    late_enqueued = threading.Event()

    class _Queue(HandoverXlsxWriteQueueService):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.loop_count = 0

        def _worker_loop(self, building: str) -> None:
            self.loop_count += 1
            if self.loop_count == 1:
                ReviewBuildingDocumentStore(config=self.config, building=building).enqueue_xlsx_write_job(
                    task_type="test",
                    dedupe_key="late",
                    payload={"label": "late"},
                )
                late_enqueued.set()
                return
            return super()._worker_loop(building)

        def _execute_job(self, *, building: str, job: Dict[str, Any]) -> None:
            payload = job.get("payload", {}) if isinstance(job.get("payload", {}), dict) else {}
            processed.append(str(payload.get("label", "") or job.get("task_type", "")))

    service = _Queue(_config(tmp_path), emit_log=lambda _msg: None)

    service._start_worker(building="A楼")
    assert late_enqueued.wait(2.0)
    service.wait_for_barrier(building="A楼", timeout_sec=2.0)

    assert "late" in processed


def test_xlsx_write_queue_startup_recovery_requeues_running_jobs(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = ReviewBuildingDocumentStore(config=config, building="A楼")
    job = store.enqueue_xlsx_write_job(task_type="test", dedupe_key="stale", payload={"label": "stale"})
    claimed = store.claim_next_xlsx_write_job()
    assert claimed["job_id"] == job["job_id"]
    assert claimed["status"] == "running"

    result = store.recover_xlsx_write_jobs_for_startup()

    assert result["reset_running"] == 1
    recovered = store.get_xlsx_write_job(job["job_id"])
    assert recovered["status"] == "pending"
    assert "重新排队" in recovered["error"]
