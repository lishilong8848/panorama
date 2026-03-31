from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import pytest

from app.modules.report_pipeline.service.calc_run_runtime import (
    run_with_config,
    run_with_explicit_file_items,
    run_with_explicit_files,
    save_results,
)


@dataclass
class _Result:
    building: str
    month: str
    payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return self.payload


def test_run_with_config_validates_excel_dir(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_with_config(
            {"input": {"excel_dir": str(tmp_path / "nope"), "buildings": ["A楼"]}},
            discover_latest_files=lambda *_args, **_kwargs: {},
            build_results_from_mapping=lambda *_args, **_kwargs: [],
            emit_log=lambda _msg: None,
        )


def test_run_with_explicit_files_upload_flow(tmp_path: Path) -> None:
    x = tmp_path / "A.xlsx"
    x.write_bytes(b"x")
    called: List[str] = []

    def _build(_cfg: Dict[str, Any], mapping: Dict[str, Path]) -> List[Any]:
        assert "A楼" in mapping
        return [_Result("A楼", "2026-03", {"a": 1})]

    run_with_explicit_files(
        config={},
        building_to_file={"A楼": str(x)},
        build_results_from_mapping=_build,
        save_results_fn=lambda *_args, **_kwargs: called.append("save"),
        upload_results_to_feishu_fn=lambda *_args, **_kwargs: called.append("upload"),
        save_json=True,
        upload=True,
    )

    assert called == ["save", "upload"]


def test_run_with_explicit_file_items_date_validation(tmp_path: Path) -> None:
    x = tmp_path / "A.xlsx"
    x.write_bytes(b"x")

    with pytest.raises(ValueError):
        run_with_explicit_file_items(
            config={},
            file_items=[{"building": "A楼", "file_path": str(x), "upload_date": "20260308"}],
            build_results_from_file_items=lambda *_args, **_kwargs: [],
            save_results_fn=lambda *_args, **_kwargs: None,
            upload_results_to_feishu_fn=lambda *_args, **_kwargs: None,
        )


def test_save_results_respects_save_json_flag(tmp_path: Path) -> None:
    output_cfg = {"output": {"save_json": False, "json_dir": str(tmp_path)}}
    save_results(
        [_Result("A楼", "2026-03", {"a": 1})],
        output_cfg,
        emit_log=lambda _msg: None,
    )
    assert not any(tmp_path.iterdir())
