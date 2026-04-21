from __future__ import annotations

from pathlib import Path

from app.modules.updater.service.remote_control_store import UpdaterRemoteControlStore


def test_remote_control_store_single_slot_rejects_duplicate_pending(tmp_path: Path) -> None:
    store = UpdaterRemoteControlStore(tmp_path / "shared")

    first = store.submit_command(
        command_id="cmd-a",
        action="check",
        requested_by_node_id="external-1",
        requested_by_role="external",
    )
    second = store.submit_command(
        command_id="cmd-b",
        action="apply",
        requested_by_node_id="external-1",
        requested_by_role="external",
    )

    assert first["accepted"] is True
    assert first["command"]["message"] == "等待内网端执行检查更新"
    assert second["accepted"] is False
    assert second["already_pending"] is True
    assert second["command"]["command_id"] == "cmd-a"
    assert second["command"]["action"] == "check"


def test_remote_control_store_reuses_slot_after_terminal_state(tmp_path: Path) -> None:
    store = UpdaterRemoteControlStore(tmp_path / "shared")
    store.submit_command(
        command_id="cmd-a",
        action="check",
        requested_by_node_id="external-1",
        requested_by_role="external",
    )
    completed = store.update_command(command_id="cmd-a", status="completed", message="done")
    assert completed is not None

    next_result = store.submit_command(
        command_id="cmd-b",
        action="apply",
        requested_by_node_id="external-1",
        requested_by_role="external",
    )
    assert next_result["accepted"] is True
    assert next_result["command"]["command_id"] == "cmd-b"
    assert next_result["command"]["status"] == "pending"
    assert next_result["command"]["message"] == "等待内网端执行开始更新"


def test_remote_control_store_preserves_optional_source_commit(tmp_path: Path) -> None:
    store = UpdaterRemoteControlStore(tmp_path / "shared")

    result = store.submit_command(
        command_id="cmd-source",
        action="apply",
        requested_by_node_id="external-1",
        requested_by_role="external",
        source_commit="abcdef123456",
    )

    assert result["accepted"] is True
    command = store.load_command()
    assert command["source_commit"] == "abcdef123456"
    snapshot = store.build_internal_peer_snapshot()
    assert snapshot["command"]["source_commit"] == "abcdef123456"
