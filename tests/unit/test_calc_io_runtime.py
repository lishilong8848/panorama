from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from app.modules.report_pipeline.service.calc_io_runtime import (
    build_results_from_file_items,
    build_results_from_mapping,
    discover_latest_files,
    load_config,
)


def test_discover_latest_files_pick_newest(tmp_path: Path) -> None:
    a_old = tmp_path / "A楼_1.xlsx"
    a_new = tmp_path / "A楼_2.xlsx"
    b_file = tmp_path / "B楼_1.xlsx"
    for p in (a_old, a_new, b_file):
        p.write_bytes(b"x")

    a_old.touch()
    a_new.touch()
    b_file.touch()

    result = discover_latest_files(
        excel_dir=tmp_path,
        buildings=["A楼", "B楼", "C楼"],
        file_glob_template="{building}_*.xlsx",
    )
    assert result["A楼"] == a_new
    assert result["B楼"] == b_file
    assert "C楼" not in result


def test_build_results_from_mapping_order(tmp_path: Path) -> None:
    calls: List[tuple[str, str | None]] = []

    def _calc(file_path: str, building: str | None) -> Any:
        calls.append((Path(file_path).name, building))
        return type("Result", (), {"missing_metrics": []})()

    config: Dict[str, Any] = {"input": {"buildings": ["B楼", "A楼"]}}
    mapping = {
        "A楼": tmp_path / "A.xlsx",
        "B楼": tmp_path / "B.xlsx",
    }
    for p in mapping.values():
        p.write_bytes(b"x")

    build_results_from_mapping(
        config,
        mapping,
        calculate_monthly_report=_calc,
        emit_log=lambda _msg: None,
    )
    assert calls == [("B.xlsx", "B楼"), ("A.xlsx", "A楼")]


def test_build_results_from_file_items_validate(tmp_path: Path) -> None:
    file_ok = tmp_path / "A.xlsx"
    file_ok.write_bytes(b"x")

    got = build_results_from_file_items(
        [{"building": "A楼", "file_path": str(file_ok)}],
        calculate_monthly_report=lambda *_args, **_kwargs: type("Result", (), {"missing_metrics": []})(),
        emit_log=lambda _msg: None,
    )
    assert len(got) == 1

    file_txt = tmp_path / "A.txt"
    file_txt.write_text("x", encoding="utf-8")
    with pytest.raises(ValueError):
        build_results_from_file_items(
            [{"building": "A楼", "file_path": str(file_txt)}],
            calculate_monthly_report=lambda *_args, **_kwargs: None,
            emit_log=lambda _msg: None,
        )


def test_load_config_with_bom(tmp_path: Path) -> None:
    cfg = {"x": 1}
    config_file = tmp_path / "cfg.json"
    config_file.write_text(json.dumps(cfg, ensure_ascii=False), encoding="utf-8-sig")

    loaded = load_config(
        config_file,
        resolve_pipeline_config_path=lambda p: Path(p),
    )
    assert loaded == cfg
