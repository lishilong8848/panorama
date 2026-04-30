from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
MAIN_FILE = PROJECT_ROOT / str(os.environ.get("QJPT_MAIN_FILE", "main.py") or "main.py").strip()
RESTART_EXIT_CODE = 194
RESTART_EXIT_CODE_ENV = "QJPT_RESTART_EXIT_CODE"
PORTABLE_LAUNCHER_ENV = "QJPT_PORTABLE_LAUNCHER"
DISABLE_BROWSER_AUTO_OPEN_ENV = "QJPT_DISABLE_BROWSER_AUTO_OPEN"
STARTUP_LOG_ENV = "QJPT_STARTUP_LOG"


def _configure_console_utf8() -> None:
    if not sys.platform.startswith("win"):
        return
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
        kernel32.SetConsoleOutputCP(65001)
        kernel32.SetConsoleCP(65001)
    except Exception:
        pass
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is not None and hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def _build_child_env(*, disable_browser_auto_open: bool = False) -> dict[str, str]:
    env = dict(os.environ)
    env[RESTART_EXIT_CODE_ENV] = str(RESTART_EXIT_CODE)
    env[PORTABLE_LAUNCHER_ENV] = "1"
    env["NODE_NO_WARNINGS"] = "1"
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    if disable_browser_auto_open:
        env[DISABLE_BROWSER_AUTO_OPEN_ENV] = "1"
    else:
        env.pop(DISABLE_BROWSER_AUTO_OPEN_ENV, None)
    return env


def _open_startup_log():
    raw = str(os.environ.get(STARTUP_LOG_ENV, "") or "").strip()
    if not raw:
        return None
    try:
        path = Path(raw)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open("a", encoding="utf-8", errors="replace")
    except Exception:
        return None


def _log_line(log_handle, text: str) -> None:
    if log_handle is None:
        return
    try:
        log_handle.write(text)
        if not text.endswith("\n"):
            log_handle.write("\n")
        log_handle.flush()
    except Exception:
        pass


def _spawn_child(*, disable_browser_auto_open: bool = False, capture_output: bool = False) -> subprocess.Popen[str]:
    cmd = [sys.executable, str(MAIN_FILE), *sys.argv[1:]]
    popen_kwargs: dict[str, object] = {
        "cwd": str(PROJECT_ROOT),
        "env": _build_child_env(disable_browser_auto_open=disable_browser_auto_open),
    }
    if capture_output:
        popen_kwargs.update(
            {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "text": True,
                "encoding": "utf-8",
                "errors": "replace",
                "bufsize": 1,
            }
        )
    if sys.platform.startswith("win") and hasattr(subprocess, "CREATE_NEW_PROCESS_GROUP"):
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(cmd, **popen_kwargs)


def main() -> int:
    _configure_console_utf8()
    log_handle = _open_startup_log()
    _log_line(log_handle, f"[INFO] portable launcher using main file: {MAIN_FILE}")
    if not MAIN_FILE.exists():
        message = f"[ERROR] 未找到启动文件: {MAIN_FILE}"
        print(message, file=sys.stderr)
        _log_line(log_handle, message)
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
        child = _spawn_child(
            disable_browser_auto_open=disable_browser_auto_open,
            capture_output=log_handle is not None,
        )

        try:
            if child.stdout is not None:
                for line in child.stdout:
                    print(line, end="")
                    _log_line(log_handle, line)
            exit_code = child.wait()
        finally:
            if child.poll() is None:
                _terminate_child()

        if stop_requested:
            return int(exit_code)

        if int(exit_code) == RESTART_EXIT_CODE:
            disable_browser_auto_open = True
            message = "[更新] 已完成更新，正在当前窗口内重启程序..."
            print(message)
            _log_line(log_handle, message)
            time.sleep(1)
            continue
        _log_line(log_handle, f"[INFO] child exited with code {int(exit_code)}")
        if log_handle is not None:
            try:
                log_handle.close()
            except Exception:
                pass
        return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())

