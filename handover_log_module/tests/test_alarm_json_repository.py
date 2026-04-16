from __future__ import annotations

from datetime import date
from pathlib import Path

from handover_log_module.repository.alarm_json_repository import AlarmJsonRepository
from handover_log_module.service.handover_orchestrator import HandoverOrchestrator, HandoverQueryContext


def test_query_alarm_summary_builds_selection_snapshot_by_window_start_date(tmp_path: Path) -> None:
    json_file = tmp_path / "alarm.json"
    json_file.write_text("{}", encoding="utf-8")
    cache_key = str(json_file.resolve())
    captured: dict[str, object] = {}

    class _Repo(AlarmJsonRepository):
        def build_selection_snapshot(self, *, buildings, reference_date=None):  # type: ignore[override]
            captured["buildings"] = list(buildings)
            captured["reference_date"] = reference_date
            return {
                "selected_by_building": {
                    "A楼": {
                        "file_path": str(json_file),
                        "entry_id": "entry-a",
                        "selection_scope": "today",
                        "source_kind": "manual",
                        "downloaded_at": "2026-04-14 23:12:03",
                    }
                }
            }

    repo = _Repo({"_shared_bridge": {"root_dir": str(tmp_path)}})
    payload = {
        "building": "A楼",
        "query_start": "2026-04-14 18:00:00",
        "query_end": "2026-04-15 09:00:00",
        "rows": [
            {
                "event_time": "2026-04-15 00:10:00",
                "recover_status": "未恢复",
                "accept_description": "未恢复事件",
            }
        ],
    }

    summary = repo.query_alarm_summary(
        building="A楼",
        start_time="2026-04-14 18:00:00",
        end_time="2026-04-15 09:00:00",
        emit_log=lambda *_args, **_kwargs: None,
        document_cache={cache_key: payload},
    )

    assert captured["buildings"] == ["A楼"]
    assert captured["reference_date"] == date(2026, 4, 14)
    assert summary.total_count == 1
    assert summary.unrecovered_count == 1


def test_orchestrator_builds_alarm_snapshot_by_duty_date(monkeypatch, tmp_path: Path) -> None:
    orchestrator = HandoverOrchestrator(
        {
            "_deployment_role_mode": "external",
            "sites": [{"building": "A楼", "enabled": True}],
        }
    )
    captured: dict[str, object] = {}

    class _FakeAlarmRepo:
        def build_selection_snapshot(self, *, buildings, reference_date=None):
            captured["buildings"] = list(buildings)
            captured["reference_date"] = reference_date
            return {"selected_by_building": {}}

    monkeypatch.setattr(orchestrator, "_alarm_json_repo", _FakeAlarmRepo())
    monkeypatch.setattr(
        orchestrator,
        "_build_query_context",
        lambda **kwargs: HandoverQueryContext(
            duty_date=str(kwargs.get("duty_date", "") or "").strip(),
            duty_shift=str(kwargs.get("duty_shift", "") or "").strip(),
            target_buildings=list(kwargs.get("buildings", []) or []),
        ),
    )
    monkeypatch.setattr(
        orchestrator,
        "run_from_existing_file",
        lambda **kwargs: {
            "results": [
                {
                    "building": kwargs["building"],
                    "data_file": kwargs["data_file"],
                    "success": True,
                }
            ]
        },
    )

    result = orchestrator.run_from_existing_files(
        building_files=[("A楼", str(tmp_path / "A.xlsx"))],
        configured_buildings=["A楼"],
        duty_date="2026-04-14",
        duty_shift="night",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert result["success_count"] == 1
    assert captured["buildings"] == ["A楼"]
    assert captured["reference_date"] == date(2026, 4, 14)


def test_query_alarm_summary_allows_partial_history_coverage_after_window_start(tmp_path: Path) -> None:
    json_file = tmp_path / "alarm.json"
    json_file.write_text("{}", encoding="utf-8")
    cache_key = str(json_file.resolve())

    class _Repo(AlarmJsonRepository):
        def build_selection_snapshot(self, *, buildings, reference_date=None):  # type: ignore[override]
            return {
                "selected_by_building": {
                    "A楼": {
                        "file_path": str(json_file),
                        "entry_id": "entry-a",
                        "selection_scope": "today",
                        "source_kind": "manual",
                        "downloaded_at": "2026-04-14 23:12:03",
                    }
                }
            }

    repo = _Repo({"_shared_bridge": {"root_dir": str(tmp_path)}})
    payload = {
        "building": "A楼",
        "query_start": "2026-02-13 23:12:09",
        "query_end": "2026-04-14 23:12:09",
        "rows": [
            {
                "event_time": "2026-04-14 18:10:00",
                "recover_status": "未恢复",
                "accept_description": "历史夜班未恢复事件",
            },
            {
                "event_time": "2026-04-15 01:10:00",
                "recover_status": "未恢复",
                "accept_description": "超出可用覆盖，不应统计",
            },
        ],
    }

    summary = repo.query_alarm_summary(
        building="A楼",
        start_time="2026-04-14 17:00:00",
        end_time="2026-04-15 08:00:00",
        emit_log=lambda *_args, **_kwargs: None,
        document_cache={cache_key: payload},
    )

    assert summary.total_count == 1
    assert summary.unrecovered_count == 1
    assert summary.accept_description == "历史夜班未恢复事件"
    assert summary.coverage_ok is False
    assert summary.fallback_used is False
