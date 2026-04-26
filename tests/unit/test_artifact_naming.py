from __future__ import annotations

from datetime import datetime

from app.shared.utils.artifact_naming import (
    FAMILY_HANDOVER_CAPACITY_REPORT,
    FAMILY_HANDOVER_LOG,
    FAMILY_MONTHLY_REPORT,
    build_source_artifact_path,
)


def test_latest_source_artifact_paths_use_same_hour_bucket_for_excel_families() -> None:
    for family, folder in (
        (FAMILY_HANDOVER_LOG, "交接班日志源文件"),
        (FAMILY_HANDOVER_CAPACITY_REPORT, "交接班容量报表源文件"),
        (FAMILY_MONTHLY_REPORT, "全景平台月报源文件"),
    ):
        info = build_source_artifact_path(
            source_family=family,
            building="A楼",
            suffix=".xlsx",
            bucket_kind="latest",
            bucket_key="2026-04-27 01",
            duty_date="2026-04-26",
            duty_shift="night",
        )

        assert info.bucket_segment == "20260427--01"
        assert info.relative_path.as_posix() == (
            f"{folder}/202604/20260427--01/20260427--01--{folder}--A楼.xlsx"
        )


def test_latest_source_artifact_path_falls_back_to_current_hour_when_bucket_missing() -> None:
    info = build_source_artifact_path(
        source_family=FAMILY_MONTHLY_REPORT,
        building="B楼",
        suffix=".xlsx",
        bucket_kind="latest",
        bucket_key="",
        duty_date="2026-04-26",
        now=datetime(2026, 4, 27, 2, 30, 0),
    )

    assert info.bucket_segment == "20260427--02"
    assert "20260426--月报" not in info.relative_path.as_posix()
