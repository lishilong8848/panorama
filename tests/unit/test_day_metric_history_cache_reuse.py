from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import openpyxl
import pytest

from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import (
    FAMILY_HANDOVER_LOG,
    SharedSourceCacheService,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "day_metric_history_cache_reuse"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _build_runtime_config(*, role_mode: str, shared_root: Path) -> dict:
    return {
        "deployment": {"role_mode": role_mode},
        "shared_bridge": {
            "enabled": True,
            "root_dir": str(shared_root),
        },
        "internal_source_cache": {"enabled": True},
    }


def _write_handover_source_xlsx(path: Path, *, d_value: str = "市电总功率", e_value: float = 123.4) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet["D4"] = d_value
    sheet["E4"] = e_value
    workbook.save(path)


def _write_empty_xlsx(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = openpyxl.Workbook()
    workbook.active["A1"] = "empty"
    workbook.save(path)


def _register_shared_handover_entry(
    *,
    store: SharedBridgeStore,
    shared_root: Path,
    building: str,
    duty_date: str,
    downloaded_at: str,
    e_value: float = 123.4,
) -> Path:
    source_path = (
        shared_root
        / "交接班日志源文件"
        / "202604"
        / f"{duty_date.replace('-', '')}--16"
        / f"{duty_date.replace('-', '')}--16--交接班日志源文件--{building}.xlsx"
    )
    _write_handover_source_xlsx(source_path, e_value=e_value)
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building=building,
        bucket_kind="latest",
        bucket_key=f"{duty_date} 16",
        duty_date=duty_date,
        duty_shift="day",
        downloaded_at=downloaded_at,
        relative_path=source_path.relative_to(shared_root).as_posix(),
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": building, "duty_date": duty_date, "duty_shift": "day"},
    )
    return source_path


def test_fill_day_metric_history_reuses_shared_handover_sources(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )
    _register_shared_handover_entry(
        store=store,
        shared_root=shared_root,
        building="A楼",
        duty_date="2026-04-05",
        downloaded_at="2026-04-05 12:00:00",
    )

    entries = service.fill_day_metric_history(
        selected_dates=["2026-04-05"],
        building_scope="single",
        building="A楼",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert len(entries) == 1
    output_path = Path(entries[0]["file_path"])
    assert output_path.exists()
    assert entries[0]["bucket_kind"] == "latest"
    assert entries[0]["bucket_key"] == "2026-04-05 16"
    assert "20260405--16" in entries[0]["relative_path"]
    rows = store.list_source_cache_entries(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-05",
        duty_date="2026-04-05",
        duty_shift="all",
        status="ready",
    )
    assert rows == []


def test_fill_day_metric_history_raises_when_no_shared_handover_source(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    with pytest.raises(RuntimeError, match="缺少可复用的交接班源文件"):
        service.fill_day_metric_history(
            selected_dates=["2026-04-05"],
            building_scope="single",
            building="A楼",
            emit_log=lambda *_args, **_kwargs: None,
        )


def test_fill_day_metric_history_skips_empty_ready_entry_and_reuses_valid_latest(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    empty_path = shared_root / "source_cache" / "handover_log" / "date" / "202604" / "empty-A.xlsx"
    _write_empty_xlsx(empty_path)
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-05",
        duty_date="2026-04-05",
        duty_shift="all",
        downloaded_at="2026-04-05 18:00:00",
        relative_path=empty_path.relative_to(shared_root).as_posix(),
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": "A楼", "duty_date": "2026-04-05", "duty_shift": "all"},
    )

    latest_valid = _register_shared_handover_entry(
        store=store,
        shared_root=shared_root,
        building="A楼",
        duty_date="2026-04-05",
        downloaded_at="2026-04-05 16:00:00",
    )

    entries = service.fill_day_metric_history(
        selected_dates=["2026-04-05"],
        building_scope="single",
        building="A楼",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert len(entries) == 1
    output_path = Path(entries[0]["file_path"])
    assert output_path.exists()
    workbook = openpyxl.load_workbook(output_path, data_only=True)
    try:
        assert workbook.active["D4"].value == "市电总功率"
        assert workbook.active["E4"].value == 123.4
    finally:
        workbook.close()
    assert output_path.read_bytes() == latest_valid.read_bytes()
    assert entries[0]["bucket_kind"] == "latest"


def test_fill_day_metric_history_prefers_cached_entry_over_history_scan(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    cached_path = shared_root / "source_cache" / "handover_log" / "date" / "202604" / "cached-A.xlsx"
    _write_handover_source_xlsx(cached_path, e_value=111.1)
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-11",
        duty_date="2026-04-11",
        duty_shift="all",
        downloaded_at="2026-04-11 20:00:00",
        relative_path=cached_path.relative_to(shared_root).as_posix(),
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": "A楼", "duty_date": "2026-04-11", "duty_shift": "all"},
    )

    history_path = (
        shared_root
        / "交接班日志源文件"
        / "202604"
        / "20260411--11"
        / "20260411--11--交接班日志源文件--A楼.xlsx"
    )
    _write_handover_source_xlsx(history_path, e_value=222.2)

    entries = service.fill_day_metric_history(
        selected_dates=["2026-04-11"],
        building_scope="single",
        building="A楼",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert len(entries) == 1
    assert entries[0]["metadata"]["resolution_source"] == "cache_date"
    workbook = openpyxl.load_workbook(Path(entries[0]["file_path"]), data_only=True)
    try:
        assert workbook.active["E4"].value == 111.1
    finally:
        workbook.close()


def test_fill_day_metric_history_falls_back_to_valid_cache_when_history_missing(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    cached_path = shared_root / "source_cache" / "handover_log" / "date" / "202604" / "cached-A.xlsx"
    _write_handover_source_xlsx(cached_path, e_value=345.6)
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-12",
        duty_date="2026-04-12",
        duty_shift="all",
        downloaded_at="2026-04-12 20:00:00",
        relative_path=cached_path.relative_to(shared_root).as_posix(),
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": "A楼", "duty_date": "2026-04-12", "duty_shift": "all"},
    )

    entries = service.fill_day_metric_history(
        selected_dates=["2026-04-12"],
        building_scope="single",
        building="A楼",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert len(entries) == 1
    assert entries[0]["metadata"]["resolution_source"] == "cache_date"
    workbook = openpyxl.load_workbook(Path(entries[0]["file_path"]), data_only=True)
    try:
        assert workbook.active["E4"].value == 345.6
    finally:
        workbook.close()


def test_repair_day_metric_ready_entries_downgrades_empty_ready_rows(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    empty_path = shared_root / "source_cache" / "handover_log" / "date" / "202604" / "empty-A.xlsx"
    _write_empty_xlsx(empty_path)
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-05",
        duty_date="2026-04-05",
        duty_shift="all",
        downloaded_at="2026-04-05 18:00:00",
        relative_path=empty_path.relative_to(shared_root).as_posix(),
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": "A楼", "duty_date": "2026-04-05", "duty_shift": "all"},
    )
    _register_shared_handover_entry(
        store=store,
        shared_root=shared_root,
        building="A楼",
        duty_date="2026-04-05",
        downloaded_at="2026-04-05 16:00:00",
    )

    summary = service.repair_day_metric_ready_entries(
        selected_dates=["2026-04-05"],
        buildings=["A楼"],
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert summary["scanned"] >= 2
    assert summary["downgraded"] >= 1
    failed_rows = store.list_source_cache_entries(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-05",
        duty_date="2026-04-05",
        duty_shift="all",
        status="failed",
    )
    assert len(failed_rows) == 1


def test_sweep_invalid_ready_entries_only_scans_latest_and_recent_window(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    latest_empty = shared_root / "source_cache" / "handover_log" / "latest" / "20260411-16" / "A-empty.xlsx"
    _write_empty_xlsx(latest_empty)
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-04-11 16",
        duty_date="2026-04-11",
        duty_shift="day",
        downloaded_at="2026-04-12 09:00:00",
        relative_path=latest_empty.relative_to(shared_root).as_posix(),
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": "A楼", "duty_date": "2026-04-11", "duty_shift": "day"},
    )

    recent_valid = _register_shared_handover_entry(
        store=store,
        shared_root=shared_root,
        building="B楼",
        duty_date="2026-04-10",
        downloaded_at="2026-04-10 12:00:00",
    )
    assert recent_valid.exists()

    old_empty = shared_root / "source_cache" / "handover_log" / "date" / "202603" / "old-empty.xlsx"
    _write_empty_xlsx(old_empty)
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building="C楼",
        bucket_kind="date",
        bucket_key="2026-03-01",
        duty_date="2026-03-01",
        duty_shift="all",
        downloaded_at="2026-03-01 09:00:00",
        relative_path=old_empty.relative_to(shared_root).as_posix(),
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": "C楼", "duty_date": "2026-03-01", "duty_shift": "all"},
    )

    summary = service.sweep_invalid_ready_entries(
        lookback_days=7,
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert summary["scanned"] >= 2
    assert summary["downgraded"] >= 1
    assert summary["skipped"] >= 1
    failed_latest = store.list_source_cache_entries(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-04-11 16",
        status="failed",
    )
    assert len(failed_latest) == 1
    assert failed_latest[0]["metadata"]["validated_by"] == "background_sweep"
    old_ready = store.list_source_cache_entries(
        source_family=FAMILY_HANDOVER_LOG,
        building="C楼",
        bucket_kind="date",
        bucket_key="2026-03-01",
        duty_date="2026-03-01",
        duty_shift="all",
        status="ready",
    )
    assert len(old_ready) == 1


def test_sweep_invalid_ready_entries_downgrades_missing_ready_entry_to_failed(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-04-11 16",
        duty_date="2026-04-11",
        duty_shift="day",
        downloaded_at="2026-04-12 09:00:00",
        relative_path="source_cache/handover_log/latest/20260411-16/missing-A.xlsx",
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": "A楼", "duty_date": "2026-04-11", "duty_shift": "day"},
    )

    summary = service.sweep_invalid_ready_entries(
        lookback_days=7,
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert summary["scanned"] >= 1
    assert summary["downgraded"] >= 1
    assert store.list_source_cache_entries(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-04-11 16",
        status="ready",
    ) == []
    failed_rows = store.list_source_cache_entries(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="latest",
        bucket_key="2026-04-11 16",
        status="failed",
    )
    assert len(failed_rows) == 1


def test_fill_day_metric_history_falls_back_to_real_history_folder_when_store_missing_valid_entry(work_dir: Path) -> None:
    shared_root = work_dir / "shared"
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode="external", shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    empty_path = shared_root / "source_cache" / "handover_log" / "date" / "202604" / "empty-A.xlsx"
    _write_empty_xlsx(empty_path)
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-11",
        duty_date="2026-04-11",
        duty_shift="all",
        downloaded_at="2026-04-11 23:00:00",
        relative_path=empty_path.relative_to(shared_root).as_posix(),
        status="ready",
        metadata={"family": FAMILY_HANDOVER_LOG, "building": "A楼", "duty_date": "2026-04-11", "duty_shift": "all"},
    )

    history_path = (
        shared_root
        / "交接班日志源文件"
        / "202604"
        / "20260411--11"
        / "20260411--11--交接班日志源文件--A楼.xlsx"
    )
    _write_handover_source_xlsx(history_path)

    entries = service.fill_day_metric_history(
        selected_dates=["2026-04-11"],
        building_scope="single",
        building="A楼",
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert len(entries) == 1
    output_path = Path(entries[0]["file_path"])
    workbook = openpyxl.load_workbook(output_path, data_only=True)
    try:
        assert workbook.active["D4"].value == "市电总功率"
        assert workbook.active["E4"].value == 123.4
    finally:
        workbook.close()
    assert entries[0]["bucket_kind"] == "history_scan"
    assert "20260411--11" in entries[0]["relative_path"]
