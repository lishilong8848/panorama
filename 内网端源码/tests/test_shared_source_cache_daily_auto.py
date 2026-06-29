from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.modules.shared_bridge.service.shared_source_cache_service import (  # noqa: E402
    DAILY_AUTO_SOURCE_FAMILIES,
    SharedSourceCacheService,
)


def _build_service(tmp_path: Path) -> SharedSourceCacheService:
    return SharedSourceCacheService(
        runtime_config={
            "deployment": {"role_mode": "internal"},
            "shared_bridge": {"enabled": True, "root_dir": str(tmp_path)},
            "internal_source_cache": {
                "enabled": True,
                "daily_source_download": {
                    "enabled": True,
                    "run_time": "00:30:00",
                    "retry_interval_sec": 300,
                },
            },
        },
        store=None,
        emit_log=lambda _line: None,
    )


def test_daily_source_download_runs_after_configured_time_once_per_business_date(tmp_path, monkeypatch):
    service = _build_service(tmp_path)
    calls = []

    def fake_run_latest_source_steps_by_building(*, steps, force_retry_failed, force_refresh_existing):
        calls.append(
            {
                "steps": [(source_family, bucket_key) for source_family, bucket_key, _fill_func in steps],
                "force_retry_failed": force_retry_failed,
                "force_refresh_existing": force_refresh_existing,
            }
        )
        return {
            "failed_units": [],
            "blocked_units": [],
            "running_units": [],
            "completed_units": ["A楼/branch_power_family"],
        }

    monkeypatch.setattr(service, "_run_latest_source_steps_by_building", fake_run_latest_source_steps_by_building)
    monkeypatch.setattr(service, "_ensure_dirs", lambda: None)

    service._run_daily_source_files_if_due(datetime(2026, 6, 29, 0, 29, 59))
    assert calls == []

    service._run_daily_source_files_if_due(datetime(2026, 6, 29, 0, 30, 0))

    assert len(calls) == 1
    assert calls[0]["force_retry_failed"] is True
    assert calls[0]["force_refresh_existing"] is False
    assert calls[0]["steps"] == [(source_family, "2026-06-28") for source_family in DAILY_AUTO_SOURCE_FAMILIES]
    assert service._daily_source_refresh["last_success_business_date"] == "2026-06-28"

    service._run_daily_source_files_if_due(datetime(2026, 6, 29, 1, 0, 0))
    assert len(calls) == 1


def test_daily_source_download_retries_failed_business_date_after_cooldown(tmp_path, monkeypatch):
    service = _build_service(tmp_path)
    calls = []

    def fake_run_latest_source_steps_by_building(*, steps, force_retry_failed, force_refresh_existing):
        calls.append([(source_family, bucket_key) for source_family, bucket_key, _fill_func in steps])
        return {
            "failed_units": ["A楼/branch_power_family"],
            "blocked_units": [],
            "running_units": [],
            "completed_units": [],
        }

    monkeypatch.setattr(service, "_run_latest_source_steps_by_building", fake_run_latest_source_steps_by_building)
    monkeypatch.setattr(service, "_ensure_dirs", lambda: None)

    service._run_daily_source_files_if_due(datetime(2026, 6, 29, 0, 30, 0))
    assert len(calls) == 1
    assert service._daily_source_refresh["last_success_business_date"] == ""

    service._run_daily_source_files_if_due(datetime(2026, 6, 29, 0, 31, 0))
    assert len(calls) == 1

    service._last_daily_source_run_monotonic -= 301
    service._run_daily_source_files_if_due(datetime(2026, 6, 29, 0, 36, 1))
    assert len(calls) == 2
