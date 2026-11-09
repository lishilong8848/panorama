from types import SimpleNamespace
from unittest.mock import Mock
from app.modules.report_pipeline.service.job_service import JobService, JobState, StageState

from handover_log_module.service.review_session_service import ReviewSessionService
from handover_log_module.service.station_h_duty_focus_service import StationHDutyFocusService


def test_session_lookup_and_lock_do_not_scan_other_buildings(tmp_path, monkeypatch):
    service = ReviewSessionService(
        {"_global_paths": {"runtime_state_root": str(tmp_path)}}, initialize_generated_file_index=False,
    )
    store = service._review_state_store
    batch = "2026-11-09|day"
    store.apply_changes(upsert_sessions=[
        {"session_id": f"{b}|{batch}", "building": b, "batch_key": batch,
         "duty_date": "2026-11-09", "duty_shift": "day", "revision": 1}
        for b in ("A楼", "B楼")
    ])
    monkeypatch.setattr(service, "_load_state", Mock(side_effect=AssertionError("full history scan")))
    assert len(service.list_batch_sessions(batch)) == 2
    assert service.get_cloud_batch(batch)["batch_key"] == batch
    for b in ("A楼", "B楼"):
        assert service.get_session_by_id(f"{b}|{batch}")["building"] == b
        result = service.claim_session_lock(building=b, session_id=f"{b}|{batch}", client_id=b)
        assert result["client_holds_lock"]
    conflict = service.claim_session_lock(building="A楼", session_id=f"A楼|{batch}", client_id="other")
    assert not conflict["client_holds_lock"]


def test_h_focus_status_uses_only_cached_remote_data(monkeypatch):
    no_remote = Mock(side_effect=AssertionError("remote call on status request"))
    signatures = SimpleNamespace(
        cached_directory=lambda: {"people": []},
        ensure_directory=no_remote, refresh_directory=no_remote, match_person=lambda *_: None,
    )
    chillers = SimpleNamespace(get_batch_state=lambda _: {}, ensure_batch_state=no_remote)
    review = SimpleNamespace(list_batch_sessions=lambda _: [])
    service = StationHDutyFocusService(
        {}, review_service=review, signature_service=signatures, chiller_focus_service=chillers,
    )
    monkeypatch.setattr(service, "_outdoor_temperature_text", lambda _: "")
    monkeypatch.setattr(service, "_check_labels", lambda: {})
    monkeypatch.setattr(service, "image_status", lambda **_: {})
    result = service.build_status(
        duty_date="2026-11-09", duty_shift="day", selection={}, allow_remote=False,
    )
    assert len(result["rows"]) == 5
    assert not result["print_ready"]
    no_remote.assert_not_called()


def test_inspection_progress_explains_running_without_completing_job(monkeypatch):
    service = JobService()
    stage = StageState(stage_id="main", name="inspection", worker_handler="inspection_data_upload", status="running")
    job = JobState(job_id="test", name="inspection", status="running", stages=[stage])
    monkeypatch.setattr(service, "_record_job_event", Mock())
    monkeypatch.setattr(service, "_persist_job_snapshot", Mock())
    service._handle_worker_event(job, stage, {
        "type": "progress", "stage_id": "main", "message": "等待内网接口数据: D楼/电气巡检",
    }, {})
    assert job.summary == stage.summary == "等待内网接口数据: D楼/电气巡检"
    assert job.status == "running"
