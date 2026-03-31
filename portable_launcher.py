from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
MAIN_FILE = PROJECT_ROOT / "main.py"
RESTART_EXIT_CODE = 194
RESTART_EXIT_CODE_ENV = "QJPT_RESTART_EXIT_CODE"
PORTABLE_LAUNCHER_ENV = "QJPT_PORTABLE_LAUNCHER"
DISABLE_BROWSER_AUTO_OPEN_ENV = "QJPT_DISABLE_BROWSER_AUTO_OPEN"


def _build_child_env(*, disable_browser_auto_open: bool = False) -> dict[str, str]:
    env = dict(os.environ)
    env[RESTART_EXIT_CODE_ENV] = str(RESTART_EXIT_CODE)
    env[PORTABLE_LAUNCHER_ENV] = "1"
    env["NODE_NO_WARNINGS"] = "1"
    if disable_browser_auto_open:
        env[DISABLE_BROWSER_AUTO_OPEN_ENV] = "1"
    else:
        env.pop(DISABLE_BROWSER_AUTO_OPEN_ENV, None)
    return env


def _spawn_child(*, disable_browser_auto_open: bool = False) -> subprocess.Popen[str]:
    cmd = [sys.executable, str(MAIN_FILE), *sys.argv[1:]]
    popen_kwargs: dict[str, object] = {
        "cwd": str(PROJECT_ROOT),
        "env": _build_child_env(disable_browser_auto_open=disable_browser_auto_open),
    }
    if sys.platform.startswith("win") and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(cmd, **popen_kwargs)


def main() -> int:
    if not MAIN_FILE.exists():
        print(f"[ERROR] 未找到启动文件: {MAIN_FILE}", file=sys.stderr)
        return 1

    stop_requested = False
    disable_browser_auto_open = False
    child: subprocess.Popen[str] | None = None

    def _terminate_child(*_args) -> None:
        nonlocal stop_requested
        stop_requested = True
        if child and child.poll() is None:
            try:
                child.terminate()
            except Exception:
                pass

    signal.signal(signal.SIGINT, _terminate_child)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _terminate_child)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, _terminate_child)

    while True:
        child = _spawn_child(disable_browser_auto_open=disable_browser_auto_open)

        try:
            exit_code = child.wait()
        finally:
            if child.poll() is None:
                _terminate_child()

        if stop_requested:
            return int(exit_code)

        if int(exit_code) == RESTART_EXIT_CODE:
            disable_browser_auto_open = True
            print("[更新] 已完成更新，正在当前窗口内重启程序...")
            time.sleep(1)
            continue
        return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
