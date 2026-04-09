from __future__ import annotations

import json
import shutil
import uuid
from pathlib import Path

import openpyxl
import pytest

from handover_log_module.service.handover_source_file_cache_service import HandoverSourceFileCacheService


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMP_ROOT = PROJECT_ROOT / ".tmp_runtime_tests" / "handover_source_file_cache_service"


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _write_xlsx(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = openpyxl.Workbook()
    workbook.active["A1"] = "ok"
    workbook.save(path)


def test_find_latest_downloaded_source_for_date_returns_latest_valid_file(work_dir: Path) -> None:
    service = HandoverSourceFileCacheService({}, business_root_override=work_dir)
    cache_root = service.cache_root()
    older = cache_root / "download_cache" / "older.xlsx"
    newer = cache_root / "download_cache" / "newer.xlsx"
    _write_xlsx(older)
    _write_xlsx(newer)

    payload = {
        service.build_download_identity(
            building="A楼",
            template_name="交接班日志",
            duty_date="2026-04-05",
            duty_shift="day",
            start_time="2026-04-05 08:00:00",
            end_time="2026-04-05 20:00:00",
            scale_label="5分钟",
        ): {
            "file_path": str(older),
            "updated_at": "2026-04-05 08:00:00",
        },
        service.build_download_identity(
            building="A楼",
            template_name="交接班日志",
            duty_date="2026-04-05",
            duty_shift="night",
            start_time="2026-04-05 20:00:00",
            end_time="2026-04-06 08:00:00",
            scale_label="5分钟",
        ): {
            "file_path": str(newer),
            "updated_at": "2026-04-05 12:00:00",
        },
    }
    service._download_index_path().write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")  # noqa: SLF001

    resolved = service.find_latest_downloaded_source_for_date(building="A楼", duty_date="2026-04-05")

    assert resolved == str(newer)


def test_external_role_does_not_enable_local_source_cache(work_dir: Path) -> None:
    service = HandoverSourceFileCacheService({"_deployment_role_mode": "external"}, business_root_override=None)

    assert service.lookup_downloaded_source(identity="A楼|交接班日志|2026-04-05|day|x|y|5分钟") == ""
    assert service.find_latest_downloaded_source_for_date(building="A楼", duty_date="2026-04-05") == ""
    assert service.is_managed_path(work_dir / "交接班共享源文件" / "download_cache" / "a.xlsx") is False


def test_register_downloaded_source_returns_canonical_path_and_removes_download_cache_file(work_dir: Path) -> None:
    service = HandoverSourceFileCacheService({}, business_root_override=work_dir)
    source_file = service.download_cache_root() / "D楼_20260409_093721.xlsx"
    _write_xlsx(source_file)
    identity = service.build_download_identity(
        building="D楼",
        template_name="交接班日志",
        duty_date="2026-04-09",
        duty_shift="day",
        start_time="2026-04-09 08:00:00",
        end_time="2026-04-09 20:00:00",
        scale_label="5分钟",
    )

    registered_path = service.register_downloaded_source(
        identity=identity,
        file_path=str(source_file),
        emit_log=lambda *_args, **_kwargs: None,
    )

    assert registered_path
    assert registered_path != str(source_file)
    assert "交接班日志源文件" in registered_path
    assert Path(registered_path).exists() is True
    assert source_file.exists() is False
    assert service.lookup_downloaded_source(identity=identity) == registered_path
