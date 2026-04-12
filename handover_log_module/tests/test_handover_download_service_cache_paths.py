from __future__ import annotations

import sys
from pathlib import Path

import openpyxl
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from handover_log_module.service import handover_download_service as module


def _write_xlsx(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = openpyxl.Workbook()
    workbook.active["A1"] = "ok"
    workbook.save(path)
    workbook.close()


def test_run_capacity_only_returns_registered_canonical_file_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config = {
        "sites": [{"enabled": True, "building": "D楼"}],
        "download": {
            "template_name": "交接班日志",
            "scale_label": "5分钟",
        },
        "capacity_report": {
            "download": {
                "template_name": "交接班容量报表",
            }
        },
    }
    captured: dict[str, Path] = {}

    def _fake_set_runtime_config(_cfg):  # noqa: ANN001
        return None

    def _fake_download_handover_xlsx_batch(**kwargs):  # noqa: ANN003
        save_dir = Path(str(kwargs["save_dir"]))
        source_file = save_dir / "D楼_20260409_093721.xlsx"
        _write_xlsx(source_file)
        captured["original_path"] = source_file
        return [
            {
                "building": "D楼",
                "success": True,
                "file_path": str(source_file),
            }
        ]

    monkeypatch.setattr(module, "set_runtime_config", _fake_set_runtime_config)
    monkeypatch.setattr(module, "download_handover_xlsx_batch", _fake_download_handover_xlsx_batch)

    service = module.HandoverDownloadService(config, business_root_override=tmp_path)

    result = service.run_capacity_only(
        buildings=["D楼"],
        duty_date="2026-04-09",
        duty_shift="day",
        switch_network=False,
        reuse_cached=False,
        emit_log=lambda *_args, **_kwargs: None,
    )

    success_files = result["success_files"]
    assert len(success_files) == 1
    final_path = Path(str(success_files[0]["file_path"]))
    assert final_path.exists() is True
    assert "交接班容量报表源文件" in str(final_path)
    assert "download_cache" not in str(final_path)
    assert captured["original_path"].exists() is False
    assert final_path != captured["original_path"]
