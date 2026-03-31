from __future__ import annotations

from app.bootstrap.container import AppContainer


def test_runtime_action_reason_text_maps_common_codes_to_chinese() -> None:
    assert AppContainer._runtime_action_reason_text("already_running") == "已在运行"
    assert AppContainer._runtime_action_reason_text("disabled") == "未启用"
    assert AppContainer._runtime_action_reason_text("started") == "已启动"
    assert AppContainer._runtime_action_reason_text("stopped") == "已停止"
    assert AppContainer._runtime_action_reason_text("partial_started") == "部分已启动"
    assert AppContainer._runtime_action_reason_text("not_initialized") == "尚未初始化"


def test_shared_bridge_reason_text_reuses_runtime_mapping() -> None:
    assert AppContainer._shared_bridge_reason_text("disabled_or_unselected") == "当前未启用共享桥接"
    assert AppContainer._shared_bridge_reason_text("not_running") == "当前未运行"
