from __future__ import annotations

from handover_log_module.service.handover_orchestrator import HandoverOrchestrator, HandoverQueryContext


def _single_result(building: str, data_file: str, *, success: bool, output_file: str = "", errors=None):
    return {
        "mode": "from_existing_file",
        "success_count": 1 if success else 0,
        "failed_count": 0 if success else 1,
        "results": [
            {
                "building": building,
                "data_file": data_file,
                "output_file": output_file,
                "success": success,
                "fills": [],
                "missing_metrics": [],
                "day_metric_export": {},
                "cloud_sheet_sync": {},
                "review_session": {},
                "batch_key": "",
                "confirmed": False,
                "errors": list(errors or []),
            }
        ],
        "errors": list(errors or []),
    }


def test_run_from_existing_files_only_processes_selected_buildings():
    orchestrator = HandoverOrchestrator.__new__(HandoverOrchestrator)
    orchestrator.config = {}
    calls = []

    def _fake_run_from_existing_file(*, building, data_file, **kwargs):
        calls.append((building, data_file, kwargs.get("duty_date"), kwargs.get("duty_shift")))
        return _single_result(building, data_file, success=True, output_file=f"{building}.xlsx")

    orchestrator.run_from_existing_file = _fake_run_from_existing_file

    result = HandoverOrchestrator.run_from_existing_files(
        orchestrator,
        building_files=[("A楼", "a.xlsx"), ("C楼", "c.xlsx"), ("E楼", "e.xlsx")],
        configured_buildings=["A楼", "B楼", "C楼", "D楼", "E楼"],
        duty_date="2026-03-23",
        duty_shift="day",
        emit_log=lambda _msg: None,
    )

    assert calls == [
        ("A楼", "a.xlsx", "2026-03-23", "day"),
        ("C楼", "c.xlsx", "2026-03-23", "day"),
        ("E楼", "e.xlsx", "2026-03-23", "day"),
    ]
    assert result["selected_buildings"] == ["A楼", "C楼", "E楼"]
    assert result["skipped_buildings"] == ["B楼", "D楼"]
    assert result["success_count"] == 3
    assert result["failed_count"] == 0


def test_run_from_existing_files_continues_after_single_building_failure():
    orchestrator = HandoverOrchestrator.__new__(HandoverOrchestrator)
    orchestrator.config = {}
    calls = []

    def _fake_run_from_existing_file(*, building, data_file, **kwargs):
        calls.append(building)
        if building == "A楼":
            raise RuntimeError("broken file")
        return _single_result(building, data_file, success=True, output_file=f"{building}.xlsx")

    orchestrator.run_from_existing_file = _fake_run_from_existing_file

    result = HandoverOrchestrator.run_from_existing_files(
        orchestrator,
        building_files=[("A楼", "a.xlsx"), ("B楼", "b.xlsx"), ("C楼", "c.xlsx")],
        configured_buildings=["A楼", "B楼", "C楼", "D楼", "E楼"],
        duty_date="2026-03-23",
        duty_shift="night",
        emit_log=lambda _msg: None,
    )

    assert calls == ["A楼", "B楼", "C楼"]
    assert result["selected_buildings"] == ["A楼", "B楼", "C楼"]
    assert result["skipped_buildings"] == ["D楼", "E楼"]
    assert result["success_count"] == 2
    assert result["failed_count"] == 1
    assert len(result["results"]) == 3
    assert result["results"][0]["building"] == "A楼"
    assert result["results"][0]["success"] is False
    assert result["results"][0]["errors"] == ["broken file"]


def test_run_from_existing_files_prefetches_context_once_and_passes_grouped_results():
    orchestrator = HandoverOrchestrator.__new__(HandoverOrchestrator)
    orchestrator.config = {}
    query_calls = []
    run_calls = []

    def _fake_build_query_context(*, buildings, duty_date, duty_shift, emit_log, **kwargs):
        query_calls.append((list(buildings), duty_date, duty_shift))
        return HandoverQueryContext(
            duty_date=duty_date,
            duty_shift=duty_shift,
            target_buildings=list(buildings),
            roster_assignments={"A楼": {"leader": "张三"}, "B楼": {"leader": "李四"}},  # type: ignore[arg-type]
            event_query_by_building={"A楼": {"current_rows": []}, "B楼": {"current_rows": []}},  # type: ignore[arg-type]
            change_rows_by_building={"A楼": ["change-a"], "B楼": ["change-b"]},  # type: ignore[arg-type]
            exercise_rows_by_building={"A楼": ["exercise-a"], "B楼": ["exercise-b"]},  # type: ignore[arg-type]
            maintenance_rows_by_building={"A楼": ["maintenance-a"], "B楼": ["maintenance-b"]},  # type: ignore[arg-type]
            other_important_work_rows_by_building={"A楼": ["other-a"], "B楼": ["other-b"]},  # type: ignore[arg-type]
        )

    def _fake_run_from_existing_file(*, building, data_file, **kwargs):
        run_calls.append(
            {
                "building": building,
                "data_file": data_file,
                "roster_assignment": kwargs.get("roster_assignment"),
                "event_query_by_building": kwargs.get("event_query_by_building"),
                "change_rows_by_building": kwargs.get("change_rows_by_building"),
                "exercise_rows_by_building": kwargs.get("exercise_rows_by_building"),
                "maintenance_rows_by_building": kwargs.get("maintenance_rows_by_building"),
                "other_important_work_rows_by_building": kwargs.get("other_important_work_rows_by_building"),
            }
        )
        return _single_result(building, data_file, success=True, output_file=f"{building}.xlsx")

    orchestrator._build_query_context = _fake_build_query_context
    orchestrator.run_from_existing_file = _fake_run_from_existing_file

    result = HandoverOrchestrator.run_from_existing_files(
        orchestrator,
        building_files=[("A楼", "a.xlsx"), ("B楼", "b.xlsx")],
        configured_buildings=["A楼", "B楼", "C楼"],
        duty_date="2026-03-23",
        duty_shift="day",
        emit_log=lambda _msg: None,
    )

    assert query_calls == [(["A楼", "B楼"], "2026-03-23", "day")]
    assert [item["building"] for item in run_calls] == ["A楼", "B楼"]
    assert run_calls[0]["roster_assignment"] == {"leader": "张三"}
    assert run_calls[1]["roster_assignment"] == {"leader": "李四"}
    assert run_calls[0]["event_query_by_building"]["A楼"] == {"current_rows": []}
    assert run_calls[0]["change_rows_by_building"]["B楼"] == ["change-b"]
    assert run_calls[1]["exercise_rows_by_building"]["A楼"] == ["exercise-a"]
    assert run_calls[1]["maintenance_rows_by_building"]["B楼"] == ["maintenance-b"]
    assert run_calls[1]["other_important_work_rows_by_building"]["A楼"] == ["other-a"]
    assert result["selected_buildings"] == ["A楼", "B楼"]
    assert result["skipped_buildings"] == ["C楼"]
