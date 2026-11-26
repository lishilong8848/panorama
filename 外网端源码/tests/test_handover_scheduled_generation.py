from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.modules.report_pipeline.service.handover_scheduled_generation import submit_handover_scheduled_generation
from app.modules.shared_bridge.service import shared_bridge_runtime_service as bridge
from app.worker import task_handlers
from handover_log_module.service import handover_orchestrator as handover
from handover_log_module.service.handover_generation_guard import generation_lock


BUILDINGS = [f"{letter}楼" for letter in "ABCDE"]
PAYLOAD = {"buildings": BUILDINGS, "duty_date": "2026-11-25", "duty_shift": "night", "scheduler_slot": "morning"}


def test_ready_floors_start_before_waiting_for_unpaired_floor():
    jobs = Mock()
    jobs.start_worker_job.return_value.job_id = "ready"
    jobs.create_waiting_worker_job.return_value.job_id = "missing"
    remote = Mock()
    remote.create_http_bridge_task.return_value = {"task_id": "bridge"}
    logs = [{"building": b, "file_path": f"{b}-log.json"} for b in BUILDINGS]
    capacities = [{"building": b, "file_path": f"{b}-capacity.json"} for b in BUILDINGS if b != "C楼"]

    result = submit_handover_scheduled_generation(
        job_service=jobs, bridge_service=remote, name="night", payload=PAYLOAD,
        log_entries=logs, capacity_entries=capacities,
    )

    assert [call[0] for call in jobs.method_calls] == ["start_worker_job", "create_waiting_worker_job", "bind_bridge_task"]
    ready_payload = jobs.start_worker_job.call_args.kwargs["worker_payload"]
    assert ready_payload["buildings"] == ["A楼", "B楼", "D楼", "E楼"]
    for key in ("building_files", "capacity_building_files"):
        assert [item["building"] for item in ready_payload[key]] == ready_payload["buildings"]
    assert jobs.start_worker_job.call_args.kwargs["resume_policy"] == "requeue"
    missing_payload = jobs.create_waiting_worker_job.call_args.kwargs["worker_payload"]
    assert missing_payload["wait_for_handover_pair"] is True
    assert missing_payload["buildings"] == ["C楼"]
    assert remote.create_http_bridge_task.call_args.kwargs["bridge_kwargs"]["buildings"] == ["C楼"]
    assert "等待补采=C楼" in result


def test_ready_counts_from_different_floors_do_not_form_a_pair():
    jobs, remote = Mock(), Mock()
    jobs.create_waiting_worker_job.return_value.job_id = "waiting"
    remote.create_http_bridge_task.return_value = {"task_id": "bridge"}
    submit_handover_scheduled_generation(
        job_service=jobs, bridge_service=remote, name="night", payload={**PAYLOAD, "buildings": ["B楼", "C楼"]},
        log_entries=[{"building": "B楼", "file_path": "B-log.json"}],
        capacity_entries=[{"building": "C楼", "file_path": "C-capacity.json"}],
    )
    jobs.start_worker_job.assert_not_called()
    assert [call.kwargs["bridge_kwargs"]["buildings"] for call in remote.create_http_bridge_task.call_args_list] == [["B楼"], ["C楼"]]
    assert len({call.kwargs["dedupe_key"] for call in jobs.create_waiting_worker_job.call_args_list}) == 2


def test_failed_single_floor_uses_new_pair_or_retries_only_same_floor(monkeypatch):
    clock = Mock(return_value=0)
    monkeypatch.setattr(bridge.time, "monotonic", clock)
    jobs = Mock()
    snapshot = {"job_id": "waiting", "bridge_task_id": "old", "feature": "handover_from_download",
                "submitted_by": "scheduler", "wait_reason": "waiting:shared_bridge"}
    jobs.list_jobs.return_value = [snapshot]
    jobs.load_job_stage_payload.return_value = {
        **PAYLOAD, "buildings": ["C楼"], "wait_for_handover_pair": True,
        "scheduler_stale_after": (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
        "_bridge_dispatch": {"get_or_create_name": "get_or_create_handover_from_download_task",
                             "bridge_kwargs": {"buildings": ["C楼"], "duty_date": PAYLOAD["duty_date"], "duty_shift": "night"}},
    }
    runtime = bridge.SharedBridgeRuntimeService.__new__(bridge.SharedBridgeRuntimeService)
    runtime._job_service = jobs
    runtime._waiting_job_cache_probe_log_markers = {}
    runtime._emit_system_log = Mock()
    runtime.get_task = Mock(return_value={"task_id": "old", "status": "failed"})
    runtime._build_handover_history_resume_binding = Mock(side_effect=RuntimeError("仍缺容量表"))
    runtime.create_http_bridge_task = Mock(return_value={"task_id": "new"})

    runtime._reconcile_waiting_jobs()
    runtime._reconcile_waiting_jobs()
    runtime.create_http_bridge_task.assert_called_once()
    assert runtime.create_http_bridge_task.call_args.kwargs["bridge_kwargs"]["buildings"] == ["C楼"]
    jobs.bind_bridge_task.assert_called_once_with("waiting", "new")
    jobs.fail_waiting_job.assert_not_called()

    clock.return_value = 301
    runtime._build_handover_history_resume_binding.side_effect = None
    worker_payload = {"building_files": [{"building": "C楼", "file_path": "C-log.json"}],
                      "capacity_building_files": [{"building": "C楼", "file_path": "C-capacity.json"}]}
    runtime._build_handover_history_resume_binding.return_value = {"worker_payload": worker_payload, "summary": "ready"}
    runtime._reconcile_waiting_jobs()
    jobs.resume_waiting_worker_job.assert_called_once_with(
        "waiting", worker_payload={**worker_payload, "buildings": ["C楼"]}, summary="ready",
    )
    assert runtime._build_handover_history_resume_binding.call_args.args[0]["request"]["duty_date"] == PAYLOAD["duty_date"]

    jobs.load_job_stage_payload.return_value["scheduler_stale_after"] = "2000-01-01 00:00:00"
    runtime._reconcile_waiting_jobs()
    jobs.cancel_job.assert_called_once_with("waiting")
    assert runtime.create_http_bridge_task.call_count == 1


def test_unbound_single_floor_checks_cache_before_redispatch():
    jobs = Mock()
    jobs.list_jobs.return_value = [
        {"job_id": "waiting", "feature": "handover_from_download", "submitted_by": "scheduler", "wait_reason": "waiting:shared_bridge"},
    ]
    jobs.load_job_stage_payload.return_value = {**PAYLOAD, "buildings": ["C楼"], "wait_for_handover_pair": True}
    item = {"building": "C楼", "file_path": "ready.json"}
    payload = {"building_files": [item], "capacity_building_files": [item]}
    runtime = bridge.SharedBridgeRuntimeService.__new__(bridge.SharedBridgeRuntimeService)
    runtime._job_service = jobs
    runtime._waiting_job_cache_probe_log_markers = {}
    runtime._build_handover_history_resume_binding = Mock(return_value={"worker_payload": payload, "summary": "ready"})
    runtime._reissue_unbound_waiting_job = Mock()
    runtime._reconcile_waiting_jobs()
    jobs.resume_waiting_worker_job.assert_called_once_with(
        "waiting", worker_payload={**payload, "buildings": ["C楼"]}, summary="ready",
    )
    runtime._reissue_unbound_waiting_job.assert_not_called()


def test_single_floor_resumes_from_cache_while_original_download_is_running():
    jobs = Mock()
    jobs.list_jobs.return_value = [
        {"job_id": "waiting", "bridge_task_id": "running", "feature": "handover_from_download",
         "submitted_by": "scheduler", "wait_reason": "waiting:shared_bridge"},
    ]
    jobs.load_job_stage_payload.return_value = {**PAYLOAD, "buildings": ["C楼"], "wait_for_handover_pair": True}
    payload = {"building_files": [{"building": "C楼", "file_path": "ready-log.json"}],
               "capacity_building_files": [{"building": "C楼", "file_path": "ready-capacity.json"}]}
    runtime = bridge.SharedBridgeRuntimeService.__new__(bridge.SharedBridgeRuntimeService)
    runtime._job_service = jobs
    runtime.get_task = Mock(return_value={"task_id": "running", "status": "running"})
    runtime._waiting_job_cache_probe_log_markers = {}
    runtime._build_handover_history_resume_binding = Mock(return_value={"worker_payload": payload, "summary": "ready"})
    runtime._reissue_unbound_waiting_job = Mock()
    runtime._reconcile_waiting_jobs()
    jobs.resume_waiting_worker_job.assert_called_once_with(
        "waiting", worker_payload={**payload, "buildings": ["C楼"]}, summary="ready",
    )
    runtime._reissue_unbound_waiting_job.assert_not_called()


def test_same_building_waits_without_blocking_another_building(tmp_path):
    first = f"{tmp_path.name}-A"
    pending = Event()

    def attempt(building):
        pending.set()
        with generation_lock(building, "2099-01-01", "night"):
            return building

    with ThreadPoolExecutor(2) as pool:
        with generation_lock(first, "2099-01-01", "night"):
            queued = pool.submit(attempt, first)
            assert pending.wait(2)
            assert pool.submit(attempt, f"{tmp_path.name}-B").result(timeout=3).endswith("-B")
            assert not queued.done()
        assert queued.result(timeout=3) == first


def test_success_receipt_for_different_floor_does_not_resume_worker():
    jobs = Mock()
    snapshot = {"job_id": "waiting", "bridge_task_id": "wrong", "feature": "handover_from_download",
                "submitted_by": "scheduler", "wait_reason": "waiting:shared_bridge"}
    jobs.list_jobs.return_value = [snapshot]
    jobs.load_job_stage_payload.return_value = {**PAYLOAD, "buildings": ["C楼"], "wait_for_handover_pair": True}
    task = {"task_id": "wrong", "status": "success"}
    item = {"building": "B楼", "file_path": "wrong.json"}
    runtime = bridge.SharedBridgeRuntimeService.__new__(bridge.SharedBridgeRuntimeService)
    runtime._job_service = jobs
    runtime.get_task = Mock(return_value=task)
    runtime._build_waiting_job_resume_binding = Mock(return_value={
        "worker_payload": {"building_files": [item], "capacity_building_files": [item]},
    })
    runtime._recover_scheduled_handover_pair = Mock()
    runtime._reconcile_waiting_jobs()
    jobs.resume_waiting_worker_job.assert_not_called()
    runtime._recover_scheduled_handover_pair.assert_called_once_with(snapshot, jobs.load_job_stage_payload.return_value, task)


def test_resumed_scheduled_worker_rechecks_existing_pair_under_generation_lock(monkeypatch):
    generate = Mock(return_value={"status": "success"})
    monkeypatch.setattr(task_handlers, "OrchestratorService", lambda _: SimpleNamespace(run_handover_from_files=generate))
    monkeypatch.setattr(task_handlers, "_stale_scheduled_handover_payload", lambda *_: {})
    task_handlers.handle_handover_from_download({}, {
        **PAYLOAD, "resume_kind": "shared_bridge_handover", "buildings": ["C楼"],
        "building_files": [{"building": "C楼", "file_path": "C-log.json"}],
        "capacity_building_files": [{"building": "C楼", "file_path": "C-capacity.json"}],
    }, Mock())
    assert generate.call_args.kwargs["only_if_missing"] is True
    assert generate.call_args.kwargs["building_files"] == [("C楼", "C-log.json")]


def test_capacity_failure_is_not_success_or_a_rewrite_of_the_existing_log(monkeypatch, tmp_path):
    log = tmp_path / "prior-log.xlsx"
    log.write_bytes(b"saved review log")
    session = {"session_id": "C楼|2099-01-01|night", "output_file": str(log), "capacity_status": "failed"}
    orchestrator = handover.HandoverOrchestrator.__new__(handover.HandoverOrchestrator)
    orchestrator.config = {}
    orchestrator._review_session_service = SimpleNamespace(
        get_session_for_building_duty_fast=Mock(return_value=session), register_generated_output=Mock(return_value=session),
    )
    orchestrator._build_shift_roster_fixed_values = Mock(return_value={})
    orchestrator._extract_service = SimpleNamespace(extract=Mock(return_value={"hits": {}, "effective_config": {}}))
    fill = Mock(return_value={"output_file": "", "fills": [], "missing_metric_to_cell": {}})
    orchestrator._fill_service = SimpleNamespace(fill=fill)
    orchestrator._build_metric_origin_context = Mock(return_value={})
    orchestrator._source_data_attachment_export_service = SimpleNamespace(build_deferred_state=Mock(return_value={}))
    orchestrator._cabinet_shift_record_export_service = SimpleNamespace(build_deferred_state=Mock(return_value={}))
    orchestrator._previous_duty_context = Mock(return_value=("2099-01-01", "day"))
    orchestrator._build_alarm_duty_window = Mock(return_value=SimpleNamespace(start_time="start", end_time="end"))
    orchestrator._build_alarm_summary_for_window = Mock(return_value={})
    orchestrator._capacity_report_service = SimpleNamespace(generate=Mock(side_effect=OSError("capacity file missing")))
    orchestrator._deployment_role_mode = Mock(return_value="external")
    orchestrator._review_link_delivery_service = SimpleNamespace(send_for_session=Mock())
    monkeypatch.setattr(handover, "ReviewDocumentStateService", Mock())
    from app.modules.handover_review.api import routes
    monkeypatch.setattr(routes, "schedule_latest_review_documents_warmup", Mock())
    result = orchestrator.run_from_existing_file(
        building="C楼", duty_date="2099-01-01", duty_shift="night", data_file="log.json",
        capacity_source_file="capacity.json", only_if_missing=True,
        fixed_cell_values={}, category_payloads={}, emit_log=lambda _: None,
    )
    assert result["failed_count"] == 1 and result["success_count"] == 0
    assert "capacity file missing" in result["errors"][0]
    assert result["results"][0]["capacity_status"] == "failed"
    assert fill.call_args.kwargs["write_output_file"] is False
    assert log.read_bytes() == b"saved review log"
    orchestrator._review_link_delivery_service.send_for_session.assert_not_called()


def test_timer_does_not_replace_a_protected_manual_session_with_missing_files():
    orchestrator = handover.HandoverOrchestrator.__new__(handover.HandoverOrchestrator)
    orchestrator._review_session_service = SimpleNamespace(get_session_for_building_duty_fast=Mock(return_value={
        "session_id": "C楼|2099-01-01|night", "manual_regenerated": True, "capacity_status": "failed",
    }))
    with pytest.raises(RuntimeError, match="不会覆盖"):
        orchestrator.run_from_existing_file(
            building="C楼", duty_date="2099-01-01", duty_shift="night",
            data_file="log.json", capacity_source_file="capacity.json", only_if_missing=True,
        )
