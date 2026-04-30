from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
import time
import warnings
import webbrowser
from pathlib import Path
#测试提交
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

warnings.filterwarnings(
    "ignore",
    message="Workbook contains no default style, apply openpyxl's default",
    category=UserWarning,
    module=r"openpyxl\.styles\.stylesheet",
)

from app.modules.updater.service.runtime_dependency_sync_service import (  # noqa: E402
    RuntimeDependencySyncService,
)


_SOURCE_RUN_DISABLE_UPDATER_ENV = "QJPT_DISABLE_UPDATER_IN_SOURCE_RUN"
_SOURCE_RUN_GIT_PULL_ENV = "QJPT_ENABLE_GIT_PULL_IN_SOURCE_RUN"
_PORTABLE_LAUNCHER_ENV = "QJPT_PORTABLE_LAUNCHER"


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


def _ensure_runtime_dependencies(runtime_config: dict | None = None) -> None:
    print(
        "[启动] 正在检查全部运行依赖，首次启动或版本更新后可能需要几分钟，请勿关闭此窗口。",
        flush=True,
    )
    service = RuntimeDependencySyncService(
        app_dir=PROJECT_ROOT,
        runtime_state_root=str(
            ((runtime_config or {}).get("paths", {}) or {}).get("runtime_state_root", "") or ""
        ),
        emit_log=lambda text: print(text, flush=True),
        python_executable=sys.executable,
    )
    result = service.ensure_startup_dependencies()
    checked = int(result.get("checked", 0) or 0)
    if int(result.get("installed", 0) or 0) > 0:
        print(
            f"[依赖检查] 已完成运行依赖同步: checked={checked}, installed={result.get('installed', 0)}",
            flush=True,
        )
    else:
        print(f"[依赖检查] 运行依赖已就绪: checked={checked}", flush=True)


def _stdio_is_available() -> bool:
    stdout = getattr(sys, "stdout", None)
    stderr = getattr(sys, "stderr", None)
    return stdout is not None and stderr is not None


def _schedule_open_browser(url: str, delay_sec: int) -> None:
    def _open() -> None:
        time.sleep(max(0, delay_sec))
        try:
            webbrowser.open(url, new=1)
        except Exception:  # noqa: BLE001
            pass

    threading.Thread(target=_open, daemon=True, name="open-browser").start()


def _is_private_ipv4(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    parts = raw.split(".")
    if len(parts) != 4:
        return False
    try:
        nums = [int(part) for part in parts]
    except ValueError:
        return False
    if any(num < 0 or num > 255 for num in nums):
        return False
    if nums[0] == 10:
        return True
    if nums[0] == 172 and 16 <= nums[1] <= 31:
        return True
    if nums[0] == 192 and nums[1] == 168:
        return True
    return False


def _detect_lan_ipv4s() -> list[str]:
    candidates: list[str] = []

    def _push(value: str) -> None:
        text = str(value or "").strip()
        if _is_private_ipv4(text) and text not in candidates:
            candidates.append(text)

    probe_targets = ("192.168.1.1", "10.255.255.255", "172.16.0.1", "8.8.8.8")
    for remote_host in probe_targets:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect((remote_host, 80))
            _push(str(sock.getsockname()[0] or "").strip())
        except OSError:
            pass
        finally:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    try:
        hostname = socket.gethostname()
        for family, _, _, _, sockaddr in socket.getaddrinfo(hostname, None, family=socket.AF_INET):
            if family != socket.AF_INET or not sockaddr:
                continue
            _push(str(sockaddr[0] or "").strip())
    except OSError:
        pass
    return candidates


def _resolve_browser_host(host: str, port: int) -> tuple[str, str, str]:
    local_url = f"http://127.0.0.1:{port}/"
    lan_url = ""
    browser_url = local_url
    if host == "0.0.0.0":
        lan_hosts = _detect_lan_ipv4s()
        if lan_hosts:
            lan_url = f"http://{lan_hosts[0]}:{port}/"
        return local_url, lan_url, browser_url

    if _is_private_ipv4(host):
        lan_url = f"http://{host}:{port}/"
    return local_url, lan_url, browser_url


def _port_bind_error(host: str, port: int) -> OSError | None:
    bind_host = str(host or "").strip() or "127.0.0.1"
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((bind_host, int(port)))
        return None
    except OSError as exc:
        return exc
    finally:
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass


def _normalize_role_mode(value: object) -> str:
    text = str(value or "").strip().lower()
    if text in {"internal", "external"}:
        return text
    return ""


def _is_loopback_host(value: object) -> bool:
    text = str(value or "").strip().lower()
    return text in {"127.0.0.1", "::1", "localhost"}


def _should_disable_updater_for_source_run() -> bool:
    if getattr(sys, "frozen", False):
        return False
    return True


def _apply_source_run_runtime_flags() -> bool:
    if not _should_disable_updater_for_source_run():
        return False
    os.environ.pop(_SOURCE_RUN_DISABLE_UPDATER_ENV, None)
    os.environ[_SOURCE_RUN_GIT_PULL_ENV] = "1"
    return True


def main(argv: list[str] | None = None) -> None:
    _configure_console_utf8()
    parser = argparse.ArgumentParser(description="全景平台月报控制台入口")
    parser.add_argument("--config", default="", help="覆盖默认配置文件路径")
    parser.add_argument("--host", default="", help="覆盖配置中的 common.console.host")
    parser.add_argument("--port", type=int, default=0, help="覆盖配置中的 common.console.port")
    parser.add_argument("--no-open-browser", action="store_true", help="启动后不自动打开浏览器")
    args = parser.parse_args(argv)

    config_path = str(args.config or "").strip()
    if config_path:
        os.environ["MONTHLY_REPORT_CONFIG"] = config_path

    bootstrap_settings = None
    try:
        from app.config.settings_loader import load_bootstrap_settings

        bootstrap_settings = load_bootstrap_settings(config_path or None)
    except Exception:  # noqa: BLE001
        bootstrap_settings = None

    try:
        _ensure_runtime_dependencies(bootstrap_settings)
    except Exception as exc:  # noqa: BLE001
        print(f"[启动] 运行依赖准备失败: {exc}", flush=True)
        print("[启动] 请检查当前网络、系统代理或 VPN 配置，以及 PIP 镜像连通性后重试。", flush=True)
        raise SystemExit(1) from exc
    source_run_git_pull_enabled = _apply_source_run_runtime_flags()
    if source_run_git_pull_enabled:
        print("[启动] 当前为源码直跑模式。启动阶段不会自动拉取代码；如需更新请点击页面中的“拉取代码”按钮或手动执行 git pull。", flush=True)

    import uvicorn
    from app.bootstrap.app_factory import create_app

    settings = bootstrap_settings or {}
    common_cfg = settings.get("common", {}) if isinstance(settings, dict) else {}
    console_cfg = common_cfg.get("console", {}) if isinstance(common_cfg, dict) else {}
    deployment_cfg = common_cfg.get("deployment", {}) if isinstance(common_cfg, dict) else {}
    deployment_role_mode = _normalize_role_mode(
        deployment_cfg.get("role_mode") if isinstance(deployment_cfg, dict) else ""
    )

    if not bool(console_cfg.get("enabled", True)):
        print("common.console.enabled=false，Web 控制台已禁用")
        return

    host = str(args.host).strip() or str(console_cfg.get("host", "127.0.0.1")).strip() or "127.0.0.1"
    port = int(args.port) if args.port else int(console_cfg.get("port", 18765))
    if deployment_role_mode == "internal":
        host = "127.0.0.1"
    elif deployment_role_mode == "external" and _is_loopback_host(host):
        host = "0.0.0.0"
    elif not deployment_role_mode:
        host = "127.0.0.1"

    os.environ["QJPT_CONSOLE_BIND_HOST"] = host
    os.environ["QJPT_CONSOLE_BIND_PORT"] = str(port)

    disable_browser_auto_open = bool(str(os.environ.get("QJPT_DISABLE_BROWSER_AUTO_OPEN", "") or "").strip())
    auto_open = (
        bool(console_cfg.get("auto_open_browser", True))
        and not bool(args.no_open_browser)
        and not disable_browser_auto_open
    )

    local_url, lan_url, browser_url = _resolve_browser_host(host, port)
    if deployment_role_mode == "internal":
        print(f"[内网端] 本地管理页地址: {local_url}", flush=True)
        print("[内网端] 仅监听 127.0.0.1，不提供局域网访问入口。", flush=True)
    elif deployment_role_mode == "external":
        print(f"[控制台] 本机访问地址: {local_url}", flush=True)
        if lan_url:
            print(f"[控制台] 局域网访问地址: {lan_url}", flush=True)
        elif host == "0.0.0.0":
            print("[控制台] 未检测到可用局域网 IPv4，浏览器将回退为本机地址。", flush=True)
        else:
            print("[控制台] 当前外网端未开放局域网监听，请检查监听 host 配置。", flush=True)
    else:
        print(f"[控制台] 本机访问地址: {local_url}", flush=True)
        print("[启动] 当前未确认启动角色，仅加载最小控制台壳。", flush=True)

    if auto_open:
        _schedule_open_browser(browser_url, int(console_cfg.get("open_browser_delay_sec", 1)))

    bind_error = _port_bind_error(host, port)
    if bind_error is not None:
        winerror = getattr(bind_error, "winerror", None)
        errno = getattr(bind_error, "errno", None)
        if winerror == 10048 or errno == 10048:
            print(f"[控制台] 启动失败：端口 {port} 已被占用，可能已有一个控制台实例正在运行。", flush=True)
            print("[控制台] 请先关闭旧实例，再重新启动。", flush=True)
            return
        print(f"[控制台] 启动失败：端口 {port} 绑定失败: {bind_error}", flush=True)
        return

    app = create_app()
    run_kwargs = {
        "host": host,
        "port": port,
        "log_level": "info",
        "access_log": False,
        "use_colors": False,
    }
    if not _stdio_is_available():
        run_kwargs["log_config"] = None

    uvicorn.run(app, **run_kwargs)


if __name__ == "__main__":
    main()
