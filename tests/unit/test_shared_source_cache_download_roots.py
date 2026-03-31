from __future__ import annotations

from pathlib import Path

import app.modules.shared_bridge.service.shared_source_cache_service as cache_module
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import SharedSourceCacheService


def _build_runtime_config(shared_root: Path) -> dict:
    return {
        "deployment": {"role_mode": "internal"},
        "shared_bridge": {"enabled": True, "root_dir": str(shared_root)},
        "internal_source_cache": {"enabled": True},
    }


def test_fill_handover_latest_uses_shared_tmp_root(monkeypatch, tmp_path: Path) -> None:
    shared_root = tmp_path / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    captured: dict[str, Path] = {}

    class _FakeDownloadService:
        def __init__(self, _cfg, download_browser_pool=None, *, business_root_override=None):  # noqa: ANN001
            del download_browser_pool
            captured["business_root_override"] = Path(str(business_root_override))

        def run(self, **_kwargs):  # noqa: ANN001
            download_dir = captured["business_root_override"] / "交接班共享源文件" / "download_cache"
            download_dir.mkdir(parents=True, exist_ok=True)
            source_file = download_dir / "A楼_20260330_020916.xlsx"
            source_file.write_bytes(b"test-source")
            return {
                "success_files": [
                    {
                        "building": "A楼",
                        "file_path": str(source_file),
                    }
                ],
                "duty_date": "",
                "duty_shift": "",
            }

    captured_store: dict[str, Path] = {}

    def _fake_store_entry(**kwargs):  # noqa: ANN003
        captured_store["source_path"] = Path(kwargs["source_path"])
        return {"file_path": str(kwargs["source_path"])}

    monkeypatch.setattr(cache_module, "load_handover_config", lambda _cfg: {})
    monkeypatch.setattr(cache_module, "HandoverDownloadService", _FakeDownloadService)
    monkeypatch.setattr(service, "_store_entry", _fake_store_entry)

    service.fill_handover_latest(
        building="A楼",
        bucket_key="2026-03-30 02",
        emit_log=lambda *_args, **_kwargs: None,
    )

    expected_root = shared_root / "tmp" / "source_cache" / "handover_latest" / "2026-03-30 02" / "A楼"
    assert captured["business_root_override"] == expected_root
    assert captured_store["source_path"].is_relative_to(shared_root)

