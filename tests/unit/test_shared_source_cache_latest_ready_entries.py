from __future__ import annotations

from pathlib import Path

from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import (
    FAMILY_MONTHLY_REPORT,
    SharedSourceCacheService,
)


def _build_runtime_config(shared_root: Path) -> dict:
    return {
        "deployment": {"role_mode": "external"},
        "shared_bridge": {"enabled": True, "root_dir": str(shared_root)},
        "internal_source_cache": {"enabled": True},
    }


def test_get_latest_ready_entries_uses_newest_ready_bucket_when_bucket_unspecified(tmp_path: Path) -> None:
    shared_root = tmp_path / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    old_file = shared_root / "全景平台月报源文件" / "202603" / "20260330--08" / "20260330--08--全景平台月报源文件--A楼.xlsx"
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_bytes(b"08")
    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-03-30 08",
        downloaded_at="2026-03-30 08:25:00",
        relative_path=str(old_file.relative_to(shared_root)).replace("\\", "/"),
        status="ready",
        file_hash="hash-08",
        size_bytes=2,
    )

    missing_newer_relative = "全景平台月报源文件/202603/20260330--09/20260330--09--全景平台月报源文件--A楼.xlsx"
    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-03-30 09",
        downloaded_at="2026-03-30 09:25:00",
        relative_path=missing_newer_relative,
        status="ready",
        file_hash="hash-09",
        size_bytes=2,
    )

    entries = service.get_latest_ready_entries(
        source_family=FAMILY_MONTHLY_REPORT,
        buildings=["A楼"],
    )

    assert len(entries) == 1
    assert entries[0]["bucket_key"] == "2026-03-30 08"
    assert entries[0]["file_path"] == str(old_file)


def test_get_latest_ready_entries_honors_explicit_bucket_when_provided(tmp_path: Path) -> None:
    shared_root = tmp_path / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    target_file = shared_root / "全景平台月报源文件" / "202603" / "20260330--08" / "20260330--08--全景平台月报源文件--A楼.xlsx"
    target_file.parent.mkdir(parents=True, exist_ok=True)
    target_file.write_bytes(b"08")
    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-03-30 08",
        downloaded_at="2026-03-30 08:25:00",
        relative_path=str(target_file.relative_to(shared_root)).replace("\\", "/"),
        status="ready",
        file_hash="hash-08",
        size_bytes=2,
    )

    entries = service.get_latest_ready_entries(
        source_family=FAMILY_MONTHLY_REPORT,
        buildings=["A楼"],
        bucket_key="2026-03-30 08",
    )

    assert len(entries) == 1
    assert entries[0]["bucket_key"] == "2026-03-30 08"
    assert entries[0]["file_path"] == str(target_file)
