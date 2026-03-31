from __future__ import annotations

from dataclasses import dataclass

from app.modules.report_pipeline.service.download_runtime_utils import (
    extract_site_host,
    group_download_tasks_by_building,
    is_retryable_download_timeout,
    get_multi_date_max,
    resolve_run_save_dir,
    resolve_site_urls,
)


def test_extract_site_host_supports_ip_and_url():
    assert extract_site_host("192.168.1.10") == "192.168.1.10"
    assert extract_site_host("http://192.168.1.11/page/main/main.html") == "192.168.1.11"


def test_resolve_site_urls_always_normalizes_to_main_page():
    site = {"url": "http://192.168.1.12/page/authority/login/login.html"}
    assert resolve_site_urls(site) == ["http://192.168.1.12/page/main/main.html"]


def test_resolve_run_save_dir_supports_none_mode(tmp_path):
    out = resolve_run_save_dir(
        {"save_dir": str(tmp_path), "run_subdir_mode": "none", "run_subdir_prefix": "run_"}
    )
    assert out == str(tmp_path)


def test_get_multi_date_max_defaults_to_31():
    assert get_multi_date_max({}, default_value=31) == 31
    assert get_multi_date_max({"multi_date": {"max_dates_per_run": 12}}, default_value=31) == 12


def test_is_retryable_download_timeout_detects_timeout_error():
    assert is_retryable_download_timeout("Page.fill: Timeout 30000ms exceeded.") is True
    assert is_retryable_download_timeout("network unreachable") is False


@dataclass
class _FakeTask:
    date_text: str
    start_time: str
    end_time: str
    site: dict


def test_group_download_tasks_by_building_orders_by_building_and_date():
    tasks = [
        _FakeTask("2026-03-02", "2026-03-02 00:00:00", "2026-03-03 00:00:00", {"building": "B楼"}),
        _FakeTask("2026-03-01", "2026-03-01 00:00:00", "2026-03-02 00:00:00", {"building": "A楼"}),
        _FakeTask("2026-03-03", "2026-03-03 00:00:00", "2026-03-04 00:00:00", {"building": "A楼"}),
    ]
    grouped = group_download_tasks_by_building(tasks)
    assert [item[0] for item in grouped] == ["B楼", "A楼"]
    assert [task.date_text for task in grouped[1][1]] == ["2026-03-01", "2026-03-03"]
