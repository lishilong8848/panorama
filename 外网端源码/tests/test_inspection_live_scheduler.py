import copy
import threading
import uuid
from types import SimpleNamespace

from app.bootstrap.container import AppContainer
from app.config.config_adapter import adapt_runtime_config, ensure_v3_config
from app.modules.report_pipeline.service.job_service import JobService


def test_linked_mode_only_suppresses_inspection_scheduler_preserving_config():
    original = ensure_v3_config({"common": {}, "features": {
        "inspection_data_upload": {"lighthouse_bridge": {"enabled": True, "token": "isolated-token"},
                                   "scheduler": {"enabled": True, "run_times": ["10:00:00"], "auto_start_in_gui": True}},
        "handover_log": {"scheduler": {"enabled": True}},
    }})
    runtime = adapt_runtime_config(original)
    before = copy.deepcopy(runtime)
    calls = []
    scheduler = SimpleNamespace(is_running=lambda: True, reconfigure=lambda cfg: calls.append(cfg))
    container = SimpleNamespace(runtime_config=runtime, inspection_data_upload_scheduler=scheduler,
        runtime_services_armed=True, _configured_deployment_snapshot=lambda: {"role_mode": "external"})
    result = AppContainer.rebuild_inspection_data_upload_scheduler(container)
    assert result["action"]["reason"] == "disabled"
    assert calls[0]["enabled"] is False
    assert runtime == before
    runtime["inspection_data_upload"]["lighthouse_bridge"]["enabled"] = False
    container.start_inspection_data_upload_scheduler = lambda **kw: {"started": True}
    result = AppContainer.rebuild_inspection_data_upload_scheduler(container)
    assert calls[-1]["enabled"] is True and result["action"]["started"]


def test_live_worker_snapshot_keeps_web_state_root(tmp_path, monkeypatch):
    web_dir, worker_dir = tmp_path/"web", tmp_path/"worker"
    web_dir.mkdir(); worker_dir.mkdir()
    config = {"paths": {"runtime_state_root": ".runtime"}}
    monkeypatch.chdir(web_dir)
    service = JobService()
    service.configure_task_engine(runtime_config=config, app_dir=worker_dir, config_snapshot_getter=lambda: config)
    monkeypatch.setattr(service, "_launch_existing_worker_job", lambda *a, **k: None)
    try:
        job = service.start_worker_job("live", worker_handler="inspection_data_live_upload", worker_payload={"live_batch_id": uuid.uuid4().hex})
        import json
        snapshot = json.loads((worker_dir/".runtime"/"task_engine"/"jobs"/job.job_id/"config_snapshot.json").read_text(encoding="utf8"))
        assert snapshot["paths"]["runtime_state_root"] == str((web_dir/".runtime").resolve())
    finally:
        service.shutdown_task_engine()
