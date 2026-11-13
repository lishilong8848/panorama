import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import Mock

from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.service.handover_generation_guard import try_host_file_lock
from handover_log_module.service.handover_xlsx_write_queue_service import HandoverXlsxWriteQueueService, _output_key


def _store(tmp_path):
    return ReviewBuildingDocumentStore(config={"_global_paths": {"runtime_state_root": str(tmp_path)}}, building="A楼")


def _enqueue(store, task_type, key="", session="day"):
    return store.enqueue_xlsx_write_job(task_type=task_type, session_id=session, target_key=key)


def _finish(store, job):
    store.finish_xlsx_write_job(job_id=job["job_id"], success=True)


def test_independent_files_run_in_parallel_but_same_path_and_barrier_wait(tmp_path):
    store = _store(tmp_path)
    capacity = _enqueue(store, "capacity_overlay_sync", "capacity.xlsx")
    log = _enqueue(store, "review_excel_sync", "log.xlsx")
    same_path = _enqueue(store, "capacity_overlay_sync", "log.xlsx", session="night")
    barrier = _enqueue(store, "barrier")
    after_barrier = _enqueue(store, "review_excel_sync", "other.xlsx", session="night")

    assert store.claim_next_xlsx_write_job(worker_slot=0)["job_id"] == capacity["job_id"]
    assert store.claim_next_xlsx_write_job(worker_slot=1)["job_id"] == log["job_id"]
    assert store.claim_next_xlsx_write_job(worker_slot=1) is None
    _finish(store, capacity)
    assert store.claim_next_xlsx_write_job(worker_slot=0) is None
    _finish(store, log)
    assert store.claim_next_xlsx_write_job(worker_slot=0)["job_id"] == same_path["job_id"]
    assert store.claim_next_xlsx_write_job(worker_slot=1) is None
    _finish(store, same_path)
    assert store.claim_next_xlsx_write_job(worker_slot=1)["job_id"] == barrier["job_id"]
    _finish(store, barrier)
    assert store.claim_next_xlsx_write_job(worker_slot=0)["job_id"] == after_barrier["job_id"]


def test_slot_recovery_keeps_other_live_write_and_legacy_job_exclusive(tmp_path):
    store = _store(tmp_path)
    legacy = _enqueue(store, "review_excel_sync")
    log = _enqueue(store, "review_excel_sync", "log.xlsx")
    capacity = _enqueue(store, "capacity_overlay_sync", "capacity.xlsx")
    assert store.claim_next_xlsx_write_job(worker_slot=-1)["job_id"] == legacy["job_id"]
    assert store.claim_next_xlsx_write_job(worker_slot=1) is None
    assert store.recover_xlsx_write_jobs_for_startup(worker_slot=1) == 0
    assert store.recover_xlsx_write_jobs_for_startup(worker_slot=0) == 1
    _finish(store, store.claim_next_xlsx_write_job(worker_slot=0))
    assert store.claim_next_xlsx_write_job(worker_slot=0)["job_id"] == log["job_id"]
    assert store.claim_next_xlsx_write_job(worker_slot=1)["job_id"] == capacity["job_id"]
    assert store.recover_xlsx_write_jobs_for_startup(worker_slot=0) == 1
    assert store.get_xlsx_write_job(capacity["job_id"])["status"] == "running"


def test_migrates_existing_queue_with_pending_rows(tmp_path):
    store = _store(tmp_path)
    with sqlite3.connect(store.db_path) as conn:
        conn.executescript("""
            CREATE TABLE xlsx_write_jobs (
                job_id TEXT PRIMARY KEY, building TEXT, session_id TEXT, task_type TEXT,
                dedupe_key TEXT, payload_json TEXT, status TEXT, attempts INTEGER,
                error TEXT, created_at TEXT, updated_at TEXT, finished_at TEXT
            );
            INSERT INTO xlsx_write_jobs VALUES (
                'old', 'A', 'day', 'review_excel_sync', '', '{}', 'pending', 0, '', '', '', ''
            );
        """)
    assert store.get_xlsx_write_job("old")["target_key"] == ""
    old = store.claim_next_xlsx_write_job()
    assert old["job_id"] == "old" and old["target_key"] == ""
    _finish(store, old)


def test_live_workers_finish_log_while_capacity_is_blocked(tmp_path):
    store = _store(tmp_path)
    queue = HandoverXlsxWriteQueueService(store.config, emit_log=lambda _: None)
    capacity = _enqueue(store, "capacity_overlay_sync", "capacity.xlsx")
    log = _enqueue(store, "review_excel_sync", "log.xlsx")
    entered = threading.Event()
    unblock = threading.Event()

    def run(*, building, job):
        if job["job_id"] == capacity["job_id"]:
            entered.set()
            assert unblock.wait(10)

    queue._execute_job = run
    try:
        queue._start_worker("A楼")
        assert entered.wait(5)
        assert queue.wait_for_job(building="A楼", job_id=log["job_id"], timeout_sec=5)["status"] == "success"
        assert store.get_xlsx_write_job(capacity["job_id"])["status"] == "running"
    finally:
        unblock.set()
    assert queue.wait_for_job(building="A楼", job_id=capacity["job_id"], timeout_sec=5)["status"] == "success"


def test_target_lock_and_regenerated_path_guard(tmp_path):
    import pytest

    target = _output_key(tmp_path / "old.xlsx")
    with try_host_file_lock(target, namespace="xlsx_target") as owned:
        assert owned
        with try_host_file_lock(target, namespace="xlsx_target") as competing:
            assert not competing
        child = subprocess.run(
            [sys.executable, "-c",
             "import sys; from handover_log_module.service.handover_generation_guard import try_host_file_lock\n"
             "with try_host_file_lock(sys.argv[1], namespace='xlsx_target') as got: sys.exit(int(got))",
             target],
            capture_output=True, text=True, timeout=15,
        )
        assert child.returncode == 0, child.stderr
    review = SimpleNamespace(get_or_recover_session_by_id=lambda _: {"output_file": str(tmp_path / "new.xlsx")})
    state = SimpleNamespace(force_sync_session_dict=Mock())
    queue = HandoverXlsxWriteQueueService({}, review_service=review, document_state=state)
    job = {"task_type": "review_excel_sync", "target_key": target, "session_id": "day"}
    with pytest.raises(RuntimeError, match="重新生成"):
        queue._execute_job(building="A楼", job=job)
    state.force_sync_session_dict.assert_not_called()
