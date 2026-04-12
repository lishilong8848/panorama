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


def _write_xlsx(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = openpyxl.Workbook()
    workbook.active["A1"] = "ok"
    workbook.save(path)


def _register_shared_handover_entry(
    *,
    store: SharedBridgeStore,
    shared_root: Path,
    building: str,
    duty_date: str,
    downloaded_at: str,
) -> Path:
    source_path = (
        shared_root
        / "交接班日志源文件"
        / "202604"
        / f"{duty_date.replace('-', '')}--白班"
        / f"{duty_date.replace('-', '')}--白班--交接班日志源文件--{building}.xlsx"
    )
    _write_xlsx(source_path)
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
    rows = store.list_source_cache_entries(
        source_family=FAMILY_HANDOVER_LOG,
        building="A楼",
        bucket_kind="date",
        bucket_key="2026-04-05",
        duty_date="2026-04-05",
        duty_shift="all",
        status="ready",
    )
    assert len(rows) == 1
    assert rows[0]["relative_path"].endswith(".xlsx")


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
