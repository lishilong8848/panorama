from __future__ import annotations

import os

import portable_launcher


def test_build_child_env_keeps_launcher_markers_and_disables_node_warnings(monkeypatch) -> None:
    monkeypatch.setenv("EXISTING_ENV", "1")

    env = portable_launcher._build_child_env()

    assert env["EXISTING_ENV"] == "1"
    assert env[portable_launcher.RESTART_EXIT_CODE_ENV] == str(portable_launcher.RESTART_EXIT_CODE)
    assert env[portable_launcher.PORTABLE_LAUNCHER_ENV] == "1"
    assert env["NODE_NO_WARNINGS"] == "1"
    assert env["PYTHONUTF8"] == "1"
    assert env["PYTHONIOENCODING"] == "utf-8"
    assert os.environ.get("NODE_NO_WARNINGS") != "1"
