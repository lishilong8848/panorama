from __future__ import annotations

from pathlib import Path
from typing import Any

import openpyxl

from app.modules.shared_bridge.service import shared_bridge_runtime_service as runtime_module


def _touch_file(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".xlsx":
        workbook = openpyxl.Workbook()
        workbook.active["D4"] = "市电总功率"
        workbook.active["E4"] = 123.4
        workbook.save(path)
    else:
        path.write_bytes(b"ok")
    return path


def _runtime_config(shared_root: Path, role_mode: str) -> dict[str, Any]:
    return {
        "deployment": {
            "role_mode": role_mode,
            "node_id": f"{role_mode}-node",
            "node_label": role_mode,
        },
        "shared_bridge": {
            "enabled": True,
            "root_dir": str(shared_root),
            "poll_interval_sec": 1,
            "heartbeat_interval_sec": 1,
            "claim_lease_sec": 30,
            "stale_task_timeout_sec": 1800,
            "artifact_retention_days": 7,
            "sqlite_busy_timeout_ms": 5000,
        },
    }


class _FakeSourceCacheService:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.enabled_buildings = ["A楼"]
        self.handover_ready = False
        self.day_metric_ready = False
        self.monthly_ready = False
        self.handover_file = _touch_file(root / "cache" / "handover" / "A楼.xlsx")
        self.capacity_file = _touch_file(root / "cache" / "handover_capacity" / "A楼.xlsx")
        self.day_metric_file = _touch_file(root / "cache" / "day_metric" / "A楼-20260407.xlsx")
        self.monthly_file = _touch_file(root / "cache" / "monthly" / "A楼-20260407.xlsx")

    def get_enabled_buildings(self) -> list[str]:
        return list(self.enabled_buildings)

    def fill_handover_history(
        self,
        *,
        buildings: list[str],
        duty_date: str,
        duty_shift: str,
        emit_log,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "fill_handover_history",
                {"buildings": list(buildings), "duty_date": duty_date, "duty_shift": duty_shift},
            )
        )
        emit_log("fill handover")
        return [
            {
                "building": "A楼",
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "file_path": str(self.handover_file),
            }
        ]

    def fill_handover_capacity_history(
        self,
        *,
        buildings: list[str],
        duty_date: str,
        duty_shift: str,
        emit_log,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "fill_handover_capacity_history",
                {"buildings": list(buildings), "duty_date": duty_date, "duty_shift": duty_shift},
            )
        )
        emit_log("fill capacity")
        return [
            {
                "building": "A楼",
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "file_path": str(self.capacity_file),
            }
        ]

    def get_handover_by_date_entries(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        buildings: list[str],
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "get_handover_by_date_entries",
                {"buildings": list(buildings), "duty_date": duty_date, "duty_shift": duty_shift},
            )
        )
        if not self.handover_ready:
            return []
        return [
            {
                "building": "A楼",
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "file_path": str(self.handover_file),
            }
        ]

    def get_handover_capacity_by_date_entries(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        buildings: list[str],
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "get_handover_capacity_by_date_entries",
                {"buildings": list(buildings), "duty_date": duty_date, "duty_shift": duty_shift},
            )
        )
        if not self.handover_ready:
            return []
        return [
            {
                "building": "A楼",
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "file_path": str(self.capacity_file),
            }
        ]

    def fill_day_metric_history(
        self,
        *,
        selected_dates: list[str],
        building_scope: str,
        building: str | None,
        emit_log,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "fill_day_metric_history",
                {
                    "selected_dates": list(selected_dates),
                    "building_scope": building_scope,
                    "building": building,
                },
            )
        )
        emit_log("fill day metric")
        target_buildings = [building] if building_scope == "single" and building else list(self.enabled_buildings)
        return [
            {
                "building": building_name,
                "duty_date": duty_date,
                "file_path": str(self.day_metric_file),
            }
            for duty_date in selected_dates
            for building_name in target_buildings
        ]

    def get_day_metric_by_date_entries(
        self,
        *,
        selected_dates: list[str],
        buildings: list[str],
    ) -> list[dict[str, Any]]:
        self.calls.append(
            (
                "get_day_metric_by_date_entries",
                {"selected_dates": list(selected_dates), "buildings": list(buildings)},
            )
        )
        if not self.day_metric_ready:
            return []
        return [
            {
                "building": building_name,
                "duty_date": duty_date,
                "file_path": str(self.day_metric_file),
            }
            for duty_date in selected_dates
            for building_name in buildings
        ]

    def fill_monthly_history(
        self,
        *,
        selected_dates: list[str],
        emit_log,
    ) -> list[dict[str, Any]]:
        self.calls.append(("fill_monthly_history", {"selected_dates": list(selected_dates)}))
        emit_log("fill monthly")
        return [
            {
                "building": "A楼",
                "duty_date": duty_date,
                "file_path": str(self.monthly_file),
                "metadata": {"upload_date": duty_date},
            }
            for duty_date in selected_dates
        ]

    def get_monthly_by_date_entries(self, *, selected_dates: list[str]) -> list[dict[str, Any]]:
        self.calls.append(("get_monthly_by_date_entries", {"selected_dates": list(selected_dates)}))
        if not self.monthly_ready:
            return []
        return [
            {
                "building": "A楼",
                "duty_date": duty_date,
                "file_path": str(self.monthly_file),
                "metadata": {"upload_date": duty_date},
            }
            for duty_date in selected_dates
        ]


def _build_services(shared_root: Path) -> tuple[Any, Any, _FakeSourceCacheService]:
    cache = _FakeSourceCacheService(shared_root)
    internal = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(shared_root, "internal"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    external = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(shared_root, "external"),
        app_version="test",
        emit_log=lambda *_args, **_kwargs: None,
    )
    internal._source_cache_service = cache
    external._source_cache_service = cache
    return internal, external, cache


def _event_payload(task: dict[str, Any], event_type: str) -> dict[str, Any]:
    event = next(item for item in task.get("events", []) if item.get("event_type") == event_type)
    payload = event.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def test_handover_cache_fill_waits_for_sync_then_resumes_success(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    class _FakeOrchestratorService:
        def __init__(self, _cfg):  # noqa: ANN001
            pass

        def run_handover_from_files(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            return {
                "success_count": 1,
                "failed_count": 0,
                "status": "success",
                "results": [{"building": "A楼", "success": True, "output_file": "D:/outputs/A楼.xlsx"}],
            }

    monkeypatch.setattr(runtime_module, "OrchestratorService", _FakeOrchestratorService)

    internal, external, cache = _build_services(tmp_path)
    task = external.create_handover_cache_fill_task(
        continuation_kind="handover",
        buildings=["A楼"],
        duty_date="2026-04-07",
        duty_shift="night",
        selected_dates=None,
        building_scope=None,
        building=None,
        requested_by="manual",
    )

    internal._process_one_task_if_needed()
    after_internal = external.get_task(task["task_id"])
    assert after_internal is not None
    assert after_internal["status"] == "ready_for_external"
    assert (
        "fill_handover_history",
        {"buildings": ["A楼"], "duty_date": "2026-04-07", "duty_shift": "night"},
    ) in cache.calls
    assert (
        "fill_handover_capacity_history",
        {"buildings": ["A楼"], "duty_date": "2026-04-07", "duty_shift": "night"},
    ) in cache.calls

    external._process_one_task_if_needed()
    waiting = external.get_task(task["task_id"])
    assert waiting is not None
    assert waiting["status"] == "ready_for_external"
    waiting_payload = _event_payload(waiting, "waiting_source_sync")
    assert waiting_payload["message"] == "等待内网补采同步"
    assert "日期=2026-04-07" in waiting_payload["detail"]
    assert "班次=night" in waiting_payload["detail"]

    cache.handover_ready = True
    external._process_one_task_if_needed()
    updated = external.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "success"
    assert captured["duty_date"] == "2026-04-07"
    assert captured["duty_shift"] == "night"
    assert captured["building_files"] == [("A楼", str(cache.handover_file))]
    assert captured["capacity_building_files"] == [("A楼", str(cache.capacity_file))]


def test_day_metric_cache_fill_waits_for_sync_then_resumes_success(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    class _FakeDayMetricService:
        def __init__(self, _cfg):  # noqa: ANN001
            pass

        def continue_from_source_files(self, **kwargs):  # noqa: ANN003
            captured.update(kwargs)
            return {
                "status": "ok",
                "selected_dates": list(kwargs.get("selected_dates", [])),
                "buildings": list(kwargs.get("buildings", [])),
            }

    monkeypatch.setattr(runtime_module, "DayMetricStandaloneUploadService", _FakeDayMetricService)

    internal, external, cache = _build_services(tmp_path)
    task = external.create_handover_cache_fill_task(
        continuation_kind="day_metric",
        buildings=None,
        duty_date=None,
        duty_shift=None,
        selected_dates=["2026-04-07"],
        building_scope="single",
        building="A楼",
        requested_by="manual",
    )

    internal._process_one_task_if_needed()
    after_internal = external.get_task(task["task_id"])
    assert after_internal is not None
    assert after_internal["status"] == "ready_for_external"
    assert (
        "fill_day_metric_history",
        {"selected_dates": ["2026-04-07"], "building_scope": "single", "building": "A楼"},
    ) in cache.calls

    external._process_one_task_if_needed()
    waiting = external.get_task(task["task_id"])
    assert waiting is not None
    assert waiting["status"] == "ready_for_external"
    waiting_payload = _event_payload(waiting, "waiting_source_sync")
    assert waiting_payload["message"] == "等待内网补采同步"
    assert waiting_payload["detail"] == "12项历史缓存未齐全，等待内网补采同步后自动继续。日期=2026-04-07"

    cache.day_metric_ready = True
    external._process_one_task_if_needed()
    updated = external.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "success"
    assert captured["selected_dates"] == ["2026-04-07"]
    assert captured["buildings"] == ["A楼"]
    assert captured["building_scope"] == "single"
    assert captured["building"] == "A楼"
    assert captured["source_units"] == [
        {
            "duty_date": "2026-04-07",
            "building": "A楼",
            "source_file": str(cache.day_metric_file),
        }
    ]


def test_day_metric_from_download_internal_prefers_shared_history_cache(monkeypatch, tmp_path: Path) -> None:
    class _UnexpectedDayMetricService:
        def __init__(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            pass

        def run_download_only(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("共享桥接 day_metric_from_download 内网阶段不应再重新下载 noon 源表")

    monkeypatch.setattr(runtime_module, "DayMetricStandaloneUploadService", _UnexpectedDayMetricService)

    internal, external, cache = _build_services(tmp_path)
    task = external.create_day_metric_from_download_task(
        selected_dates=["2026-04-07"],
        building_scope="single",
        building="A楼",
        requested_by="manual",
    )

    internal._process_one_task_if_needed()

    updated = external.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "ready_for_external"
    assert (
        "fill_day_metric_history",
        {"selected_dates": ["2026-04-07"], "building_scope": "single", "building": "A楼"},
    ) in cache.calls
    internal_result = updated.get("result", {}).get("internal", {})
    assert internal_result.get("downloaded_file_count") == 1
    assert internal_result.get("downloaded_files") == [
        {
            "duty_date": "2026-04-07",
            "building": "A楼",
            "source_file": str(cache.day_metric_file),
        }
    ]


def test_day_metric_from_download_internal_backfills_missing_handover_history(monkeypatch, tmp_path: Path) -> None:
    class _UnexpectedDayMetricService:
        def __init__(self, *_args, **_kwargs):  # noqa: ANN002, ANN003
            pass

        def run_download_only(self, **_kwargs):  # noqa: ANN003
            raise AssertionError("共享桥接 day_metric_from_download 内网阶段应优先走交接班历史补采，不应退回12项原下载")

    monkeypatch.setattr(runtime_module, "DayMetricStandaloneUploadService", _UnexpectedDayMetricService)

    internal, external, cache = _build_services(tmp_path)
    state = {"history_ready": False}

    def _fill_day_metric_history(*, selected_dates, building_scope, building, emit_log):  # noqa: ANN001
        cache.calls.append(
            (
                "fill_day_metric_history",
                {
                    "selected_dates": list(selected_dates),
                    "building_scope": building_scope,
                    "building": building,
                },
            )
        )
        emit_log("fill day metric")
        if not state["history_ready"]:
            raise RuntimeError("缺少可复用的交接班源文件: A楼(2026-04-16)")
        return [
            {
                "building": "A楼",
                "duty_date": "2026-04-16",
                "file_path": str(cache.day_metric_file),
            }
        ]

    def _fill_handover_history(*, buildings, duty_date, duty_shift, emit_log):  # noqa: ANN001
        cache.calls.append(
            (
                "fill_handover_history",
                {"buildings": list(buildings), "duty_date": duty_date, "duty_shift": duty_shift},
            )
        )
        emit_log("fill handover")
        state["history_ready"] = True
        return [
            {
                "building": "A楼",
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "file_path": str(cache.handover_file),
            }
        ]

    cache.fill_day_metric_history = _fill_day_metric_history  # type: ignore[method-assign]
    cache.fill_handover_history = _fill_handover_history  # type: ignore[method-assign]

    task = external.create_day_metric_from_download_task(
        selected_dates=["2026-04-16"],
        building_scope="single",
        building="A楼",
        requested_by="manual",
    )

    internal._process_one_task_if_needed()

    updated = external.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "ready_for_external"
    handover_calls = [call for call in cache.calls if call[0] == "fill_handover_history"]
    assert len(handover_calls) == 1
    assert handover_calls[0][1]["buildings"] == ["A楼"]
    assert handover_calls[0][1]["duty_date"] == "2026-04-16"
    assert handover_calls[0][1]["duty_shift"] == "day"
    assert cache.calls.count(
        (
            "fill_day_metric_history",
            {"selected_dates": ["2026-04-16"], "building_scope": "single", "building": "A楼"},
        )
    ) == 2
    internal_result = updated.get("result", {}).get("internal", {})
    assert internal_result.get("downloaded_file_count") == 1
    assert internal_result.get("downloaded_files") == [
        {
            "duty_date": "2026-04-16",
            "building": "A楼",
            "source_file": str(cache.day_metric_file),
        }
    ]


def test_monthly_cache_fill_waits_for_sync_then_resumes_success(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    def _fake_run_monthly_from_file_items(_runtime_config, *, file_items, emit_log, source_label):  # noqa: ANN001
        emit_log("monthly continue")
        captured["file_items"] = file_items
        captured["source_label"] = source_label
        return {"status": "ok", "file_count": len(file_items)}

    monkeypatch.setattr(runtime_module, "run_monthly_from_file_items", _fake_run_monthly_from_file_items)

    internal, external, cache = _build_services(tmp_path)
    task = external.create_monthly_cache_fill_task(
        selected_dates=["2026-04-07"],
        requested_by="manual",
    )

    internal._process_one_task_if_needed()
    after_internal = external.get_task(task["task_id"])
    assert after_internal is not None
    assert after_internal["status"] == "ready_for_external"
    assert ("fill_monthly_history", {"selected_dates": ["2026-04-07"]}) in cache.calls

    external._process_one_task_if_needed()
    waiting = external.get_task(task["task_id"])
    assert waiting is not None
    assert waiting["status"] == "ready_for_external"
    waiting_payload = _event_payload(waiting, "waiting_source_sync")
    assert waiting_payload["message"] == "等待内网补采同步"
    assert waiting_payload["detail"] == "月报历史缓存未齐全，等待内网补采同步后自动继续。日期=2026-04-07"

    cache.monthly_ready = True
    external._process_one_task_if_needed()
    updated = external.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "success"
    assert captured["source_label"] == "月报共享缓存"
    assert captured["file_items"] == [
        {
            "building": "A楼",
            "file_path": str(cache.monthly_file),
            "upload_date": "2026-04-07",
        }
    ]
