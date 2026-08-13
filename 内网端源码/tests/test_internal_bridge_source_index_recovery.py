from __future__ import annotations

from types import SimpleNamespace

from app.modules.internal_bridge_http.service.internal_bridge_http_runner import InternalBridgeHttpTaskRunner


def test_empty_source_index_starts_background_recovery(monkeypatch) -> None:
    runner = InternalBridgeHttpTaskRunner(runtime_service=SimpleNamespace(runtime_config={}))
    monkeypatch.setattr(runner, "_get_store", lambda: object())
    monkeypatch.setattr(runner, "_list_source_cache_entries_fast", lambda *args, **kwargs: [])
    monkeypatch.setattr(runner, "_merge_main_source_cache_entries", lambda entries, **kwargs: entries)
    calls = []
    monkeypatch.setattr(runner, "_start_source_index_recovery_if_needed", lambda entries, **kwargs: calls.append(kwargs))

    assert runner.list_source_index(source_family="branch_power_family", building="A楼") == []
    assert calls == [
        {
            "source_family": "branch_power_family",
            "building": "A楼",
            "bucket_kind": "",
            "bucket_key": "",
            "duty_date": "",
            "duty_shift": "",
            "limit": 50,
        }
    ]
