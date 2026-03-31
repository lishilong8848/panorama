from __future__ import annotations

from dataclasses import dataclass

from app.modules.report_pipeline.service.download_result_utils import (
    collect_first_pass_results,
    merge_retry_results,
)


@dataclass
class _Task:
    date_text: str
    site: dict


@dataclass
class _Outcome:
    building: str
    success: bool


def test_collect_first_pass_results_extracts_failed_tasks():
    pairs = [
        (_Task("2026-03-01", {"building": "A楼"}), _Outcome("A楼", True)),
        (_Task("2026-03-01", {"building": "B楼"}), _Outcome("B楼", False)),
    ]
    merged, failed = collect_first_pass_results(pairs)
    assert len(merged) == 2
    assert len(failed) == 1
    assert failed[0].site["building"] == "B楼"


def test_merge_retry_results_keeps_first_pass_success():
    task_a = _Task("2026-03-01", {"building": "A楼"})
    merged = {("2026-03-01", "A楼"): (task_a, _Outcome("A楼", True))}
    retry_pairs = [(task_a, _Outcome("A楼", False))]
    out = merge_retry_results(merged, retry_pairs)
    assert out[("2026-03-01", "A楼")][1].success is True


def test_merge_retry_results_replaces_first_pass_failure():
    task_b = _Task("2026-03-01", {"building": "B楼"})
    merged = {("2026-03-01", "B楼"): (task_b, _Outcome("B楼", False))}
    retry_pairs = [(task_b, _Outcome("B楼", True))]
    out = merge_retry_results(merged, retry_pairs)
    assert out[("2026-03-01", "B楼")][1].success is True
