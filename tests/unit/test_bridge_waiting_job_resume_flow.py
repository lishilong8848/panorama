from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from openpyxl import Workbook

from app.modules.report_pipeline.api import routes
from app.modules.shared_bridge.service import shared_bridge_runtime_service as runtime_module


def _write_workbook(path: Path) -> None:
    workbook = Workbook()
    workbook.active["A1"] = "ok"
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    workbook.close()


class _FakeWaitingJob:
    def __init__(self, job_id: str, *, name: str, feature: str, wait_reason: str, summary: str) -> None:
        self.job_id = job_id
        self.name = name
        self.feature = feature
        self.wait_reason = wait_reason
        self.summary = summary
        self.bridge_task_id = ""

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "name": self.name,
            "feature": self.feature,
            "status": "waiting_resource",
            "summary": self.summary,
            "wait_reason": self.wait_reason,
            "bridge_task_id": self.bridge_task_id,
            "resource_keys": ["shared_bridge:monthly_report"],
        }


class _FakeRouteJobService:
    def __init__(self) -> None:
        self.waiting_calls: list[dict] = []
        self.bind_calls: list[tuple[str, str]] = []
        self.last_job: _FakeWaitingJob | None = None

    def create_waiting_worker_job(self, **kwargs):  # noqa: ANN003
        self.waiting_calls.append(dict(kwargs))
        job = _FakeWaitingJob(
            "job-waiting-1",
            name=str(kwargs.get("name", "") or "").strip(),
            feature=str(kwargs.get("feature", "") or "").strip(),
            wait_reason=str(kwargs.get("wait_reason", "") or "").strip(),
            summary=str(kwargs.get("summary", "") or "").strip(),
        )
        self.last_job = job
        return job

    def bind_bridge_task(self, job_id: str, bridge_task_id: str):
        self.bind_calls.append((job_id, bridge_task_id))
        if self.last_job and self.last_job.job_id == job_id:
            self.last_job.bridge_task_id = bridge_task_id
        return self.last_job


class _FakeMonthlyBridgeService:
    def __init__(self) -> None:
        self.create_calls: list[dict] = []

    def get_source_cache_buildings(self):
        return ["A楼", "B楼"]

    def get_latest_source_cache_selection(self, **_kwargs):  # noqa: ANN003
        return {
            "best_bucket_key": "",
            "selected_entries": [],
            "fallback_buildings": [],
            "missing_buildings": ["A楼", "B楼"],
            "stale_buildings": [],
            "buildings": [],
            "can_proceed": False,
        }

    def create_monthly_auto_once_task(self, **kwargs):  # noqa: ANN003
        self.create_calls.append(dict(kwargs))
        return {
            "task_id": "bridge-monthly-auto-once-1",
            "feature": "monthly_report_pipeline",
            "status": "queued_for_internal",
            "request": dict(kwargs),
        }


def test_auto_once_route_creates_waiting_job_and_binds_bridge_task() -> None:
    job_service = _FakeRouteJobService()
    bridge_service = _FakeMonthlyBridgeService()
    container = SimpleNamespace(
        job_service=job_service,
        shared_bridge_service=bridge_service,
        logs=[],
        runtime_config={"paths": {}, "network": {"enable_auto_switch_wifi": False}},
        deployment_snapshot=lambda: {"role_mode": "external", "node_id": "node-ext-01", "node_label": ""},
        shared_bridge_snapshot=lambda: {"enabled": True, "root_dir": "Z:/share"},
        add_system_log=lambda *_args, **_kwargs: None,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))

    response = routes.job_auto_once(request)

    assert response["accepted"] is True
    assert response["job"]["status"] == "waiting_resource"
    assert response["job"]["wait_reason"] == "waiting:shared_bridge"
    assert response["job"]["bridge_task_id"] == "bridge-monthly-auto-once-1"
    assert job_service.bind_calls == [("job-waiting-1", "bridge-monthly-auto-once-1")]
    assert bridge_service.create_calls[0]["resume_job_id"] == "job-waiting-1"


def _runtime_config(tmp_path: Path, role_mode: str) -> dict:
    return {
        "deployment": {
            "role_mode": role_mode,
            "node_id": f"{role_mode}-node",
            "node_label": role_mode,
        },
        "shared_bridge": {
            "enabled": True,
            "root_dir": str(tmp_path),
            "poll_interval_sec": 1,
            "heartbeat_interval_sec": 1,
            "claim_lease_sec": 30,
            "stale_task_timeout_sec": 1800,
            "artifact_retention_days": 7,
            "sqlite_busy_timeout_ms": 5000,
        },
    }


class _FakeResumeJobService:
    def __init__(self) -> None:
        self.resume_calls: list[dict] = []
        self.fail_calls: list[dict] = []

    def resume_waiting_worker_job(self, job_id: str, *, worker_payload: dict | None = None, summary: str = "") -> None:
        self.resume_calls.append(
            {
                "job_id": job_id,
                "worker_payload": dict(worker_payload or {}),
                "summary": summary,
            }
        )

    def fail_waiting_job(self, job_id: str, *, error_text: str, summary: str = "") -> None:
        self.fail_calls.append(
            {
                "job_id": job_id,
                "error_text": error_text,
                "summary": summary,
            }
        )


class _FakeReconcileJobService(_FakeResumeJobService):
    def __init__(self, waiting_jobs: list[dict]) -> None:
        super().__init__()
        self._waiting_jobs = list(waiting_jobs)

    def list_jobs(self, *, limit=50, statuses=None):  # noqa: ANN001
        return list(self._waiting_jobs)


def test_handover_external_continue_resumes_bound_job_when_resume_job_id_present(monkeypatch, tmp_path: Path) -> None:
    source_file = tmp_path / "downloads" / "A楼.xlsx"
    _write_workbook(source_file)

    class _FakeDownloadService:
        def __init__(self, _cfg, **_kwargs):
            pass

        def ensure_internal_ready(self, emit_log):  # noqa: ANN001
            emit_log("internal ready")

        def run_with_capacity_report(self, **kwargs):  # noqa: ANN003
            return {
                "handover": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
                "capacity": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
            }

    class _UnexpectedOrchestratorService:
        def __init__(self, _cfg):
            pass

        def run_handover_from_files(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("存在 resume_job_id 时不应直接在桥接任务里继续执行业务")

    monkeypatch.setattr(runtime_module, "load_handover_config", lambda cfg: cfg)
    monkeypatch.setattr(runtime_module, "HandoverDownloadService", _FakeDownloadService)
    monkeypatch.setattr(runtime_module, "OrchestratorService", _UnexpectedOrchestratorService)

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = internal_service.create_handover_from_download_task(
        buildings=["A楼"],
        end_time=None,
        duty_date="2026-03-26",
        duty_shift="day",
        resume_job_id="job-origin-1",
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_handover_internal_download(claimed_internal)

    job_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=job_service,
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None

    external_service._run_handover_external_continue(claimed_external)

    updated = external_service.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "success"
    assert updated["result"]["resume_job_id"] == "job-origin-1"
    assert len(job_service.resume_calls) == 1
    assert job_service.resume_calls[0]["job_id"] == "job-origin-1"
    assert job_service.resume_calls[0]["worker_payload"]["resume_kind"] == "shared_bridge_handover"
    assert job_service.resume_calls[0]["worker_payload"]["bridge_task_id"] == task["task_id"]
    assert job_service.fail_calls == []


def test_handover_external_continue_resumes_bound_job_across_alias_shared_roots(monkeypatch, tmp_path: Path) -> None:
    canonical_root = tmp_path / "share"
    alias_root = tmp_path / "nested" / ".." / "share"
    source_file = canonical_root / "downloads" / "A楼.xlsx"
    _write_workbook(source_file)

    class _FakeDownloadService:
        def __init__(self, _cfg, **_kwargs):
            pass

        def ensure_internal_ready(self, emit_log):  # noqa: ANN001
            emit_log("internal ready")

        def run_with_capacity_report(self, **kwargs):  # noqa: ANN003
            return {
                "handover": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
                "capacity": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
            }

    class _UnexpectedOrchestratorService:
        def __init__(self, _cfg):
            pass

        def run_handover_from_files(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("存在 resume_job_id 时不应直接在桥接任务里继续执行业务")

    monkeypatch.setattr(runtime_module, "load_handover_config", lambda cfg: cfg)
    monkeypatch.setattr(runtime_module, "HandoverDownloadService", _FakeDownloadService)
    monkeypatch.setattr(runtime_module, "OrchestratorService", _UnexpectedOrchestratorService)

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(canonical_root, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = internal_service.create_handover_from_download_task(
        buildings=["A楼"],
        end_time=None,
        duty_date="2026-03-26",
        duty_shift="day",
        resume_job_id="job-origin-alias-1",
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_handover_internal_download(claimed_internal)

    job_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(alias_root, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=job_service,
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None

    external_service._run_handover_external_continue(claimed_external)

    updated = external_service.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "success"
    assert updated["result"]["resume_job_id"] == "job-origin-alias-1"
    assert len(job_service.resume_calls) == 1
    assert job_service.resume_calls[0]["job_id"] == "job-origin-alias-1"
    assert job_service.resume_calls[0]["worker_payload"]["resume_kind"] == "shared_bridge_handover"
    assert job_service.resume_calls[0]["worker_payload"]["bridge_task_id"] == task["task_id"]
    assert job_service.fail_calls == []


def test_handover_external_continue_fails_bound_job_when_artifact_missing(monkeypatch, tmp_path: Path) -> None:
    source_file = tmp_path / "downloads" / "A楼.xlsx"
    _write_workbook(source_file)

    class _FakeDownloadService:
        def __init__(self, _cfg, **_kwargs):
            pass

        def ensure_internal_ready(self, emit_log):  # noqa: ANN001
            emit_log("internal ready")

        def run_with_capacity_report(self, **kwargs):  # noqa: ANN003
            return {
                "handover": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
                "capacity": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
            }

    monkeypatch.setattr(runtime_module, "load_handover_config", lambda cfg: cfg)
    monkeypatch.setattr(runtime_module, "HandoverDownloadService", _FakeDownloadService)

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = internal_service.create_handover_from_download_task(
        buildings=["A楼"],
        end_time=None,
        duty_date="2026-03-26",
        duty_shift="day",
        resume_job_id="job-origin-2",
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_handover_internal_download(claimed_internal)

    updated_after_internal = internal_service.get_task(task["task_id"])
    assert updated_after_internal is not None
    artifact_relative_path = str(
        next(item for item in updated_after_internal["artifacts"] if item.get("artifact_kind") == "source_file")["relative_path"]
    )
    (tmp_path / artifact_relative_path).unlink()

    job_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=job_service,
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None

    external_service._run_handover_external_continue(claimed_external)

    updated = external_service.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "failed"
    assert len(job_service.fail_calls) == 1
    assert job_service.fail_calls[0]["job_id"] == "job-origin-2"
    assert "共享目录中的交接班源文件不存在或不可访问" in job_service.fail_calls[0]["error_text"]


def test_reconcile_waiting_jobs_resumes_handover_job_after_bridge_success(monkeypatch, tmp_path: Path) -> None:
    source_file = tmp_path / "downloads" / "A楼.xlsx"
    _write_workbook(source_file)

    class _FakeDownloadService:
        def __init__(self, _cfg, **_kwargs):
            pass

        def ensure_internal_ready(self, emit_log):  # noqa: ANN001
            emit_log("internal ready")

        def run_with_capacity_report(self, **kwargs):  # noqa: ANN003
            return {
                "handover": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
                "capacity": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
            }

    monkeypatch.setattr(runtime_module, "load_handover_config", lambda cfg: cfg)
    monkeypatch.setattr(runtime_module, "HandoverDownloadService", _FakeDownloadService)

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = internal_service.create_handover_from_download_task(
        buildings=["A楼"],
        end_time=None,
        duty_date="2026-03-26",
        duty_shift="day",
        resume_job_id="job-origin-reconcile-1",
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_handover_internal_download(claimed_internal)

    first_resume_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=first_resume_service,
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None
    external_service._run_handover_external_continue(claimed_external)

    reconcile_job_service = _FakeReconcileJobService(
        [
            {
                "job_id": "job-origin-reconcile-1",
                "status": "waiting_resource",
                "wait_reason": "waiting:shared_bridge",
                "bridge_task_id": task["task_id"],
            }
        ]
    )
    restarted_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=reconcile_job_service,
    )

    restarted_service._reconcile_waiting_jobs()

    assert reconcile_job_service.fail_calls == []
    assert len(reconcile_job_service.resume_calls) == 1
    assert reconcile_job_service.resume_calls[0]["job_id"] == "job-origin-reconcile-1"
    assert reconcile_job_service.resume_calls[0]["worker_payload"]["resume_kind"] == "shared_bridge_handover"
    assert reconcile_job_service.resume_calls[0]["worker_payload"]["bridge_task_id"] == task["task_id"]


def test_reconcile_waiting_jobs_fails_when_bridge_success_but_resume_files_missing(monkeypatch, tmp_path: Path) -> None:
    source_file = tmp_path / "downloads" / "A楼.xlsx"
    _write_workbook(source_file)

    class _FakeDownloadService:
        def __init__(self, _cfg, **_kwargs):
            pass

        def ensure_internal_ready(self, emit_log):  # noqa: ANN001
            emit_log("internal ready")

        def run_with_capacity_report(self, **kwargs):  # noqa: ANN003
            return {
                "handover": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
                "capacity": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
            }

    monkeypatch.setattr(runtime_module, "load_handover_config", lambda cfg: cfg)
    monkeypatch.setattr(runtime_module, "HandoverDownloadService", _FakeDownloadService)

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = internal_service.create_handover_from_download_task(
        buildings=["A楼"],
        end_time=None,
        duty_date="2026-03-26",
        duty_shift="day",
        resume_job_id="job-origin-reconcile-2",
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_handover_internal_download(claimed_internal)

    first_resume_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=first_resume_service,
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None
    external_service._run_handover_external_continue(claimed_external)

    updated_after_success = external_service.get_task(task["task_id"])
    assert updated_after_success is not None
    artifact_relative_path = str(
        next(item for item in updated_after_success["artifacts"] if item.get("artifact_kind") == "source_file")["relative_path"]
    )
    (tmp_path / artifact_relative_path).unlink()

    reconcile_job_service = _FakeReconcileJobService(
        [
            {
                "job_id": "job-origin-reconcile-2",
                "status": "waiting_resource",
                "wait_reason": "waiting:shared_bridge",
                "bridge_task_id": task["task_id"],
            }
        ]
    )
    restarted_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=reconcile_job_service,
    )

    restarted_service._reconcile_waiting_jobs()

    assert reconcile_job_service.resume_calls == []
    assert len(reconcile_job_service.fail_calls) == 1
    assert reconcile_job_service.fail_calls[0]["job_id"] == "job-origin-reconcile-2"
    assert "自动恢复原任务失败" in reconcile_job_service.fail_calls[0]["error_text"]
    assert "共享目录中没有可继续处理的交接班源文件" in reconcile_job_service.fail_calls[0]["error_text"]


def test_monthly_cache_fill_external_resumes_bound_job_when_resume_job_id_present(monkeypatch, tmp_path: Path) -> None:
    class _FakeSourceCacheService:
        def fill_monthly_history(self, *, selected_dates, emit_log):  # noqa: ANN001
            emit_log(f"fill:{','.join(selected_dates)}")
            return [
                {
                    "building": "A楼",
                    "file_path": "Z:/share/monthly/A楼.xlsx",
                    "duty_date": "2026-03-20",
                    "metadata": {"upload_date": "2026-03-20"},
                },
                {
                    "building": "B楼",
                    "file_path": "Z:/share/monthly/B楼.xlsx",
                    "duty_date": "2026-03-20",
                    "metadata": {"upload_date": "2026-03-20"},
                },
            ]

        def get_monthly_by_date_entries(self, *, selected_dates):  # noqa: ANN001
            return self.fill_monthly_history(selected_dates=selected_dates, emit_log=lambda *_args, **_kwargs: None)

        def get_enabled_buildings(self):
            return ["A楼", "B楼"]

    def _unexpected_run_monthly_from_file_items(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("存在 resume_job_id 时不应直接在桥接任务里继续处理多日期月报")

    monkeypatch.setattr(runtime_module, "run_monthly_from_file_items", _unexpected_run_monthly_from_file_items)

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    internal_service._source_cache_service = _FakeSourceCacheService()
    task = internal_service.create_monthly_cache_fill_task(
        selected_dates=["2026-03-20"],
        resume_job_id="job-origin-3",
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_monthly_cache_fill_internal(claimed_internal)

    job_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=job_service,
    )
    external_service._source_cache_service = _FakeSourceCacheService()
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None

    external_service._run_monthly_cache_fill_external(claimed_external)

    updated = external_service.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "success"
    assert updated["result"]["resume_job_id"] == "job-origin-3"
    assert len(job_service.resume_calls) == 1
    assert job_service.resume_calls[0]["job_id"] == "job-origin-3"
    assert job_service.resume_calls[0]["worker_payload"]["resume_kind"] == "shared_bridge_monthly_multi_date"
    assert job_service.resume_calls[0]["worker_payload"]["bridge_task_id"] == task["task_id"]
    assert job_service.fail_calls == []


def test_reconcile_waiting_jobs_resumes_monthly_cache_fill_job_after_bridge_success(monkeypatch, tmp_path: Path) -> None:
    class _FakeSourceCacheService:
        def fill_monthly_history(self, *, selected_dates, emit_log):  # noqa: ANN001
            emit_log(f"fill:{','.join(selected_dates)}")
            a_file = tmp_path / "monthly" / "A楼.xlsx"
            b_file = tmp_path / "monthly" / "B楼.xlsx"
            _write_workbook(a_file)
            _write_workbook(b_file)
            return [
                {
                    "building": "A楼",
                    "file_path": str(a_file),
                    "duty_date": "2026-03-20",
                    "metadata": {"upload_date": "2026-03-20"},
                },
                {
                    "building": "B楼",
                    "file_path": str(b_file),
                    "duty_date": "2026-03-20",
                    "metadata": {"upload_date": "2026-03-20"},
                },
            ]

        def get_monthly_by_date_entries(self, *, selected_dates):  # noqa: ANN001
            return self.fill_monthly_history(selected_dates=selected_dates, emit_log=lambda *_args, **_kwargs: None)

        def get_enabled_buildings(self):
            return ["A楼", "B楼"]

    monkeypatch.setattr(runtime_module, "run_monthly_from_file_items", lambda *_args, **_kwargs: {"status": "ok"})

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    internal_service._source_cache_service = _FakeSourceCacheService()
    task = internal_service.create_monthly_cache_fill_task(
        selected_dates=["2026-03-20"],
        resume_job_id="job-origin-reconcile-3",
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_monthly_cache_fill_internal(claimed_internal)

    first_resume_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=first_resume_service,
    )
    external_service._source_cache_service = _FakeSourceCacheService()
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None
    external_service._run_monthly_cache_fill_external(claimed_external)

    reconcile_job_service = _FakeReconcileJobService(
        [
            {
                "job_id": "job-origin-reconcile-3",
                "status": "waiting_resource",
                "wait_reason": "waiting:shared_bridge",
                "bridge_task_id": task["task_id"],
            }
        ]
    )
    restarted_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=reconcile_job_service,
    )
    restarted_service._source_cache_service = _FakeSourceCacheService()

    restarted_service._reconcile_waiting_jobs()

    assert reconcile_job_service.fail_calls == []
    assert len(reconcile_job_service.resume_calls) == 1
    assert reconcile_job_service.resume_calls[0]["job_id"] == "job-origin-reconcile-3"
    assert reconcile_job_service.resume_calls[0]["worker_payload"]["resume_kind"] == "shared_bridge_monthly_multi_date"
    assert reconcile_job_service.resume_calls[0]["worker_payload"]["bridge_task_id"] == task["task_id"]


def test_monthly_resume_upload_external_resumes_bound_job_when_resume_job_id_present(monkeypatch, tmp_path: Path) -> None:
    def _unexpected_resume_upload(*_args, **_kwargs):  # noqa: ANN001
        raise AssertionError("存在 resume_job_id 时不应直接在桥接任务里执行断点续传")

    monkeypatch.setattr(runtime_module, "run_bridge_resume_upload", _unexpected_resume_upload)

    job_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=job_service,
    )
    task = external_service.create_monthly_resume_upload_task(
        run_id="run-1",
        auto_trigger=False,
        resume_job_id="job-origin-4",
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None

    external_service._run_monthly_external_resume(claimed_external)

    updated = external_service.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "success"
    assert updated["result"]["resume_job_id"] == "job-origin-4"
    assert len(job_service.resume_calls) == 1
    assert job_service.resume_calls[0]["job_id"] == "job-origin-4"
    assert job_service.resume_calls[0]["worker_payload"]["run_id"] == "run-1"
    assert job_service.resume_calls[0]["worker_payload"]["bridge_task_id"] == task["task_id"]


def test_day_metric_cache_fill_external_resumes_bound_job_when_resume_job_id_present(monkeypatch, tmp_path: Path) -> None:
    source_file = tmp_path / "source_cache" / "day_metric" / "2026-03-20" / "A楼.xlsx"
    _write_workbook(source_file)

    class _FakeSourceCacheService:
        def __init__(self, *, runtime_config, store, download_browser_pool, emit_log):  # noqa: ANN001
            self.store = store

        def update_runtime_config(self, runtime_config):  # noqa: ANN001
            return None

        def stop(self):
            return None

        def get_enabled_buildings(self):
            return ["A楼"]

        def fill_day_metric_history(self, *, selected_dates, building_scope, building, emit_log):  # noqa: ANN001
            emit_log(f"fill:{','.join(selected_dates)}")
            return [
                {
                    "building": "A楼",
                    "duty_date": "2026-03-20",
                    "file_path": str(source_file),
                }
            ]

        def get_day_metric_by_date_entries(self, *, selected_dates, buildings):  # noqa: ANN001
            return [
                {
                    "building": "A楼",
                    "duty_date": "2026-03-20",
                    "file_path": str(source_file),
                }
            ]

    class _UnexpectedDayMetricService:
        def __init__(self, _cfg):
            pass

        def continue_from_source_files(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("存在 resume_job_id 时不应直接在桥接任务里继续执行业务")

    monkeypatch.setattr(runtime_module, "SharedSourceCacheService", _FakeSourceCacheService)
    monkeypatch.setattr(runtime_module, "DayMetricStandaloneUploadService", _UnexpectedDayMetricService)

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = internal_service.create_handover_cache_fill_task(
        continuation_kind="day_metric",
        buildings=None,
        duty_date=None,
        duty_shift=None,
        selected_dates=["2026-03-20"],
        building_scope="single",
        building="A楼",
        resume_job_id="job-origin-day-metric-1",
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_handover_cache_fill_internal(claimed_internal)

    job_service = _FakeResumeJobService()
    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
        job_service=job_service,
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None

    external_service._run_handover_cache_fill_external(claimed_external)

    updated = external_service.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "success"
    assert updated["result"]["resume_job_id"] == "job-origin-day-metric-1"
    assert len(job_service.resume_calls) == 1
    assert job_service.resume_calls[0]["job_id"] == "job-origin-day-metric-1"
    assert job_service.resume_calls[0]["worker_payload"]["resume_kind"] == "shared_bridge_day_metric"
    assert job_service.resume_calls[0]["worker_payload"]["bridge_task_id"] == task["task_id"]
    assert job_service.fail_calls == []
