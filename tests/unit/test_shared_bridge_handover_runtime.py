from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook

from app.modules.shared_bridge.service import shared_bridge_runtime_service as runtime_module


def _write_workbook(path: Path) -> None:
    workbook = Workbook()
    workbook.active["A1"] = "ok"
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    workbook.close()


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


def test_internal_handover_bridge_stage_moves_task_to_ready_for_external(monkeypatch, tmp_path: Path) -> None:
    source_file = tmp_path / "downloads" / "A楼.xlsx"
    _write_workbook(source_file)

    class _FakeDownloadService:
        def __init__(self, _cfg, **_kwargs):
            pass

        def ensure_internal_ready(self, emit_log):  # noqa: ANN001
            emit_log("internal ready")

        def run_with_capacity_report(self, **kwargs):  # noqa: ANN003
            emit_log = kwargs["emit_log"]
            emit_log("download ok")
            return {
                "handover": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [{"building": "B楼", "error": "下载失败"}],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
                "capacity": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [{"building": "B楼", "error": "下载失败"}],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
            }

    monkeypatch.setattr(runtime_module, "load_handover_config", lambda cfg: cfg)
    monkeypatch.setattr(runtime_module, "HandoverDownloadService", _FakeDownloadService)

    service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = service.create_handover_from_download_task(
        buildings=["A楼", "B楼"],
        end_time=None,
        duty_date="2026-03-26",
        duty_shift="day",
    )
    claimed = service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed is not None

    service._run_handover_internal_download(claimed)

    updated = service.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "ready_for_external"
    artifacts = updated["artifacts"]
    source_artifacts = [item for item in artifacts if item.get("artifact_kind") == "source_file"]
    capacity_artifacts = [item for item in artifacts if item.get("artifact_kind") == "capacity_source_file"]
    assert len(source_artifacts) == 1
    assert len(capacity_artifacts) == 1
    assert (tmp_path / source_artifacts[0]["relative_path"]).exists()
    assert (tmp_path / capacity_artifacts[0]["relative_path"]).exists()
    assert updated["result"]["internal"]["artifact_count"] == 1
    assert updated["result"]["internal"]["capacity_artifact_count"] == 1
    assert updated["result"]["internal"]["handover"]["failed"][0]["building"] == "B楼"


def test_external_handover_bridge_stage_merges_internal_and_external_results(monkeypatch, tmp_path: Path) -> None:
    source_file = tmp_path / "downloads" / "A楼.xlsx"
    _write_workbook(source_file)

    class _FakeDownloadService:
        def __init__(self, _cfg, **_kwargs):
            pass

        def ensure_internal_ready(self, emit_log):  # noqa: ANN001
            emit_log("internal ready")

        def run_with_capacity_report(self, **kwargs):  # noqa: ANN003
            emit_log = kwargs["emit_log"]
            emit_log("download ok")
            return {
                "handover": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [{"building": "B楼", "error": "下载失败"}],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
                "capacity": {
                    "success_files": [{"building": "A楼", "file_path": str(source_file)}],
                    "failed": [{"building": "B楼", "error": "下载失败"}],
                    "duty_date": "2026-03-26",
                    "duty_shift": "day",
                },
            }

    task_id = ""

    class _FakeOrchestratorService:
        def __init__(self, _cfg):
            pass

        def run_handover_from_files(self, **kwargs):  # noqa: ANN003
            building_files = kwargs["building_files"]
            capacity_building_files = kwargs["capacity_building_files"]
            expected_path = tmp_path / "artifacts" / "handover" / task_id / "source_files" / "A楼" / "A楼.xlsx"
            expected_capacity_path = tmp_path / "artifacts" / "handover" / task_id / "capacity_source_files" / "A楼" / "A楼.xlsx"
            assert building_files == [("A楼", str(expected_path))]
            assert capacity_building_files == [("A楼", str(expected_capacity_path))]
            return {
                "success_count": 1,
                "failed_count": 1,
                "status": "partial_failed",
                "results": [
                    {
                        "building": "B楼",
                        "data_file": "",
                        "output_file": "",
                        "success": False,
                        "errors": ["下载失败"],
                    },
                    {
                        "building": "A楼",
                        "data_file": building_files[0][1],
                        "output_file": "D:/outputs/A楼.docx",
                        "success": True,
                        "errors": [],
                    },
                ],
                "errors": ["下载失败"],
                "selected_buildings": ["A楼"],
                "skipped_buildings": [],
                "duty_date": "2026-03-26",
                "duty_shift": "day",
            }

    monkeypatch.setattr(runtime_module, "load_handover_config", lambda cfg: cfg)
    monkeypatch.setattr(runtime_module, "HandoverDownloadService", _FakeDownloadService)
    monkeypatch.setattr(runtime_module, "OrchestratorService", _FakeOrchestratorService)

    internal_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = internal_service.create_handover_from_download_task(
        buildings=["A楼", "B楼"],
        end_time=None,
        duty_date="2026-03-26",
        duty_shift="day",
    )
    task_id = task["task_id"]
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_handover_internal_download(claimed_internal)

    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None

    external_service._run_handover_external_continue(claimed_external)

    updated = external_service.get_task(task_id)
    assert updated is not None
    assert updated["status"] == "partial_failed"
    assert updated["result"]["success_count"] == 1
    assert updated["result"]["failed_count"] == 1
    assert updated["result"]["results"][0]["building"] == "B楼"
    assert updated["result"]["results"][0]["success"] is False
    assert updated["result"]["results"][1]["building"] == "A楼"
    assert updated["result"]["results"][1]["success"] is True


def test_external_handover_bridge_stage_fails_when_artifact_file_missing(monkeypatch, tmp_path: Path) -> None:
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
            raise AssertionError("共享文件已丢失时不应继续进入外网生成")

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
    )
    claimed_internal = internal_service._store.claim_next_task(role_target="internal", node_id="internal-node", lease_sec=30)
    assert claimed_internal is not None
    internal_service._run_handover_internal_download(claimed_internal)

    updated_after_internal = internal_service.get_task(task["task_id"])
    assert updated_after_internal is not None
    source_artifact = next(
        item
        for item in updated_after_internal["artifacts"]
        if str(item.get("artifact_kind", "")).strip() == "source_file"
    )
    artifact_relative_path = str(source_artifact["relative_path"])
    artifact_path = tmp_path / artifact_relative_path
    artifact_path.unlink()

    external_service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(tmp_path, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    claimed_external = external_service._store.claim_next_task(role_target="external", node_id="external-node", lease_sec=30)
    assert claimed_external is not None

    external_service._run_handover_external_continue(claimed_external)

    updated = external_service.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "failed"
    assert "不存在或不可访问" in str(updated.get("error", "") or updated.get("last_error", "") or updated.get("task_error", "") or "")
    repaired_artifact = next(
        item
        for item in updated["artifacts"]
        if str(item.get("artifact_kind", "")).strip() == "source_file"
    )
    assert repaired_artifact["status"] == "failed"
    assert repaired_artifact["metadata"]["error"] == "共享任务产物缺失或不可访问"
