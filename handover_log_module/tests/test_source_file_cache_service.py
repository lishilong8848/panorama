from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import os

import openpyxl

from handover_log_module.service.handover_source_file_cache_service import HandoverSourceFileCacheService


def _build_service(tmp_path: Path) -> HandoverSourceFileCacheService:
    return HandoverSourceFileCacheService({"_global_paths": {"download_save_dir": str(tmp_path)}})


def _build_workbook(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "handover"
    worksheet["A1"] = "demo"
    workbook.save(path)
    workbook.close()


def test_persist_uploaded_source_copies_to_runtime_cache_and_replaces_previous_suffix(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    source_v1 = tmp_path / "upload" / "input.xlsx"
    _build_workbook(source_v1)

    first = service.persist_uploaded_source(
        source_path=str(source_v1),
        building="A楼",
        duty_date="2026-03-24",
        duty_shift="day",
        session_id="A楼|2026-03-24|day",
        original_name="A楼.xlsx",
        emit_log=lambda *_args: None,
    )

    first_path = Path(first["stored_path"])
    assert first["managed"] is True
    assert first_path.exists()
    assert first_path.read_bytes() == source_v1.read_bytes()
    assert "|" not in str(first_path)

    source_v2 = tmp_path / "upload" / "input.xls"
    source_v2.write_bytes(b"source-v2")
    second = service.persist_uploaded_source(
        source_path=str(source_v2),
        building="A楼",
        duty_date="2026-03-24",
        duty_shift="day",
        session_id="A楼|2026-03-24|day",
        original_name="A楼.xls",
        previous_stored_path=first["stored_path"],
        emit_log=lambda *_args: None,
    )

    second_path = Path(second["stored_path"])
    assert second_path.exists()
    assert second_path.read_bytes() == source_v2.read_bytes()
    assert not first_path.exists()


def test_cleanup_orphan_sources_only_removes_unreferenced_old_files(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    cache_root = service.cache_root()
    old_orphan = cache_root / "2026-03-24_day" / "A楼" / "old" / "source.xlsx"
    old_orphan.parent.mkdir(parents=True, exist_ok=True)
    old_orphan.write_bytes(b"old")
    cutoff = datetime.now() - timedelta(days=HandoverSourceFileCacheService.CACHE_RETENTION_DAYS + 1)
    old_timestamp = cutoff.timestamp()
    os.utime(old_orphan, (old_timestamp, old_timestamp))

    referenced_source = tmp_path / "upload" / "keep.xlsx"
    _build_workbook(referenced_source)
    referenced = service.persist_uploaded_source(
        source_path=str(referenced_source),
        building="B楼",
        duty_date="2026-03-24",
        duty_shift="day",
        session_id="B楼|2026-03-24|day",
        original_name="B楼.xlsx",
        emit_log=lambda *_args: None,
    )

    removed = service.cleanup_orphan_sources(
        referenced_paths={referenced["stored_path"]},
        emit_log=lambda *_args: None,
    )

    assert removed == 1
    assert not old_orphan.exists()
    assert Path(referenced["stored_path"]).exists()


def test_register_and_lookup_downloaded_source_identity(tmp_path: Path) -> None:
    service = HandoverSourceFileCacheService({"_global_paths": {"download_save_dir": str(tmp_path)}})
    source_file = tmp_path / "downloaded.xlsx"
    _build_workbook(source_file)
    identity = service.build_download_identity(
        building="A楼",
        template_name="交接班日志（李世龙）",
        duty_date="2026-03-26",
        duty_shift="day",
        start_time="2026-03-26 12:00:00",
        end_time="2026-03-26 12:20:00",
        scale_label="5分钟",
    )

    assert service.lookup_downloaded_source(identity=identity) == ""
    service.register_downloaded_source(identity=identity, file_path=str(source_file), emit_log=lambda *_args: None)
    assert service.lookup_downloaded_source(identity=identity) == str(source_file)


def test_lookup_downloaded_source_rejects_corrupt_excel_file(tmp_path: Path) -> None:
    service = HandoverSourceFileCacheService({"_global_paths": {"download_save_dir": str(tmp_path)}})
    source_file = tmp_path / "broken.xlsx"
    source_file.parent.mkdir(parents=True, exist_ok=True)
    source_file.write_bytes(b"not-a-workbook")
    identity = service.build_download_identity(
        building="A楼",
        template_name="交接班日志（李世龙）",
        duty_date="2026-03-26",
        duty_shift="day",
        start_time="2026-03-26 12:00:00",
        end_time="2026-03-26 12:20:00",
        scale_label="5分钟",
    )
    index_path = service._download_index_path()  # noqa: SLF001
    service._save_download_index(  # noqa: SLF001
        {
            identity: {
                "file_path": str(source_file),
                "updated_at": "2026-03-26 12:30:00",
            }
        }
    )

    assert service.lookup_downloaded_source(identity=identity) == ""
    assert index_path.exists()
    assert identity not in index_path.read_text(encoding="utf-8")
