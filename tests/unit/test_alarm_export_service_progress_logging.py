from __future__ import annotations

from app.modules.alarm_export.service.alarm_export_service import AlarmExportService


def test_clear_feishu_table_with_progress_logs_delete_lifecycle() -> None:
    logs: list[str] = []

    class _FakeClient:
        def clear_table(
            self,
            *,
            table_id: str,
            list_page_size: int,
            delete_batch_size: int,
            progress_callback=None,
        ) -> int:
            assert table_id == "tbl-test"
            assert list_page_size == 500
            assert delete_batch_size == 200
            if progress_callback is not None:
                progress_callback(0, 3)
                progress_callback(2, 3)
                progress_callback(3, 3)
            return 3

    cleared_count = AlarmExportService._clear_feishu_table_with_progress(
        _FakeClient(),
        table_id="tbl-test",
        feishu_cfg={"list_page_size": 500, "delete_batch_size": 200},
        emit_log=logs.append,
        source="告警多维上传",
    )

    assert cleared_count == 3
    assert any("正在清空目标表旧记录" in line for line in logs)
    assert any("清空旧记录进度: 0/3" in line for line in logs)
    assert any("清空旧记录进度: 2/3" in line for line in logs)
    assert any("清空旧记录进度: 3/3" in line for line in logs)
    assert any("已清空旧记录: 3" in line for line in logs)


def test_clear_feishu_table_with_progress_keeps_legacy_client_compatible() -> None:
    logs: list[str] = []

    class _LegacyClient:
        def clear_table(
            self,
            *,
            table_id: str,
            list_page_size: int,
            delete_batch_size: int,
        ) -> int:
            assert table_id == "tbl-legacy"
            assert list_page_size == 300
            assert delete_batch_size == 150
            return 5

    cleared_count = AlarmExportService._clear_feishu_table_with_progress(
        _LegacyClient(),
        table_id="tbl-legacy",
        feishu_cfg={"list_page_size": 300, "delete_batch_size": 150},
        emit_log=logs.append,
        source="告警多维上传",
    )

    assert cleared_count == 5
    assert any("正在清空目标表旧记录" in line for line in logs)
    assert any("已清空旧记录: 5" in line for line in logs)
    assert not any("清空旧记录进度" in line for line in logs)
