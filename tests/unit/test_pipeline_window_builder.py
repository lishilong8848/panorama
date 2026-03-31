from __future__ import annotations

from app.modules.report_pipeline.service.pipeline_window_builder import build_time_window_download_tasks


def test_build_time_window_download_tasks_creates_tasks_and_runtime(tmp_path):
    logs = []
    cfg = {}
    windows = [{"date": "2026-03-01", "start_time": "2026-03-01 00:00:00", "end_time": "2026-03-02 00:00:00"}]
    sites = [{"building": "A楼", "enabled": True}, {"building": "B楼", "enabled": True}]

    def _task_factory(**kwargs):
        return kwargs

    date_result_by_date, tasks = build_time_window_download_tasks(
        time_windows=windows,
        enabled_sites=sites,
        run_save_dir=str(tmp_path),
        config=cfg,
        task_factory=_task_factory,
        emit_log=logs.append,
    )

    assert "2026-03-01" in date_result_by_date
    assert len(tasks) == 2
    assert cfg["_runtime"]["time_range_start"] == "2026-03-01 00:00:00"
    assert cfg["_runtime"]["time_range_end"] == "2026-03-02 00:00:00"
    assert any("[时间窗][2026-03-01]" in line for line in logs)
