from __future__ import annotations

import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple
from urllib.parse import urlparse


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:  # noqa: BLE001
        return float(default)


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return int(default)


def _parse_host_port(raw: str, default_port: int) -> Tuple[str, int]:
    text = str(raw or "").strip()
    if not text:
        return "", int(default_port)

    if "://" in text:
        parsed = urlparse(text)
        host = str(parsed.hostname or "").strip()
        port = int(parsed.port or default_port)
        return host, port

    if text.count(":") == 1:
        host, port_text = text.split(":", 1)
        host = host.strip()
        if port_text.strip().isdigit():
            return host, int(port_text.strip())
    return text, int(default_port)


def _extract_host(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    candidate = text if "://" in text else f"http://{text}"
    parsed = urlparse(candidate)
    return str(parsed.hostname or parsed.netloc or "").strip()


def _unique_hosts(hosts: Iterable[Any]) -> List[str]:
    output: List[str] = []
    for item in hosts:
        host = _extract_host(item)
        if host and host not in output:
            output.append(host)
    return output


def _probe_tcp(host: str, port: int, timeout_sec: float) -> bool:
    if not host:
        return True
    try:
        with socket.create_connection((host, int(port)), timeout=max(float(timeout_sec), 0.1)):
            return True
    except Exception:  # noqa: BLE001
        return False


def probe_ping(host: str, timeout_ms: int) -> bool:
    target = str(host or "").strip()
    if not target:
        return False
    timeout = max(100, int(timeout_ms or 1200))
    command = ["ping", "-n", "1", "-w", str(timeout), target]
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=max(1.0, timeout / 1000.0 + 1.0),
        )
        return int(completed.returncode or 1) == 0
    except Exception:  # noqa: BLE001
        return False


def _iter_internal_probe_hosts(network_cfg: Dict[str, Any], sites: Sequence[Dict[str, Any]] | None) -> List[str]:
    output: List[str] = []
    for site in list(sites or []):
        if not isinstance(site, dict):
            continue
        if not bool(site.get("enabled", True)):
            continue
        host = _extract_host(site.get("host", "")) or _extract_host(site.get("url", ""))
        if host and host not in output:
            output.append(host)
    if output:
        return output
    legacy_host = str(network_cfg.get("post_switch_probe_internal_host", "")).strip()
    if not legacy_host:
        return []
    host, _port = _parse_host_port(legacy_host, _as_int(network_cfg.get("post_switch_probe_internal_port", 80), 80))
    return [host] if host else []


def probe_internal_reachability(
    *,
    network_cfg: Dict[str, Any],
    sites: Sequence[Dict[str, Any]] | None = None,
    emit_log: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    hosts = _iter_internal_probe_hosts(network_cfg, sites)
    if not hosts:
        return {
            "reachable": True,
            "successful_host": "",
            "attempted_hosts": [],
            "error": "未配置内网探活目标，已跳过探活",
        }

    timeout_ms = max(100, _as_int(network_cfg.get("internal_probe_timeout_ms", 1200), 1200))
    parallelism = max(1, min(len(hosts), _as_int(network_cfg.get("internal_probe_parallelism", 5), 5)))
    attempted_hosts = list(hosts)
    with ThreadPoolExecutor(max_workers=parallelism, thread_name_prefix="internal-probe") as executor:
        futures = {executor.submit(probe_ping, host, timeout_ms): host for host in hosts}
        for future in as_completed(futures):
            host = futures[future]
            ok = False
            try:
                ok = bool(future.result())
            except Exception:  # noqa: BLE001
                ok = False
            if ok:
                if callable(emit_log):
                    emit_log(f"[网络] 内网探活成功: {host}")
                return {
                    "reachable": True,
                    "successful_host": host,
                    "attempted_hosts": attempted_hosts,
                    "error": "",
                }
    if callable(emit_log):
        emit_log(f"[网络] 内网探活失败: 所有站点均不可达 ({', '.join(attempted_hosts)})")
    return {
        "reachable": False,
        "successful_host": "",
        "attempted_hosts": attempted_hosts,
        "error": "5个楼站点IP均不可达" if attempted_hosts else "未配置内网探活目标",
    }


def probe_external_reachability(
    *,
    network_cfg: Dict[str, Any],
    emit_log: Callable[[str], None] | None = None,
) -> Dict[str, Any]:
    raw_host = str(network_cfg.get("post_switch_probe_external_host", "")).strip()
    default_port = _as_int(network_cfg.get("post_switch_probe_external_port", 443), 443)
    if not raw_host:
        return {
            "reachable": True,
            "host": "",
            "port": default_port,
            "error": "未配置外网探活目标，已跳过探活",
        }
    host, port = _parse_host_port(raw_host, default_port)
    if not host:
        return {
            "reachable": False,
            "host": "",
            "port": port,
            "error": f"外网探活目标无效: {raw_host}",
        }

    retries = max(1, _as_int(network_cfg.get("post_switch_probe_retries", 3), 3))
    timeout_sec = max(0.5, _as_float(network_cfg.get("post_switch_probe_timeout_sec", 2), 2))
    interval_sec = max(0.1, _as_float(network_cfg.get("post_switch_probe_interval_sec", 1), 1))
    for idx in range(1, retries + 1):
        if _probe_tcp(host, port, timeout_sec):
            if callable(emit_log):
                emit_log(f"[网络] 外网探活成功: {host}:{port} (attempt={idx}/{retries})")
            return {
                "reachable": True,
                "host": host,
                "port": port,
                "error": "",
            }
        if callable(emit_log):
            emit_log(f"[网络] 外网探活失败: {host}:{port} (attempt={idx}/{retries})")
        if idx < retries:
            time.sleep(interval_sec)
    return {
        "reachable": False,
        "host": host,
        "port": port,
        "error": f"无法访问 {host}:{port}",
    }


def get_network_reachability_state(
    *,
    network_cfg: Dict[str, Any],
    sites: Sequence[Dict[str, Any]] | None = None,
    current_ssid: str | None = None,
) -> Dict[str, Any]:
    current_ssid_text = str(current_ssid or "").strip()
    internal_ssid = str(network_cfg.get("internal_ssid", "") or "").strip()
    external_ssid = str(network_cfg.get("external_ssid", "") or "").strip()

    if current_ssid_text and internal_ssid and current_ssid_text.casefold() == internal_ssid.casefold():
        ssid_side = "internal"
    elif current_ssid_text and external_ssid and current_ssid_text.casefold() == external_ssid.casefold():
        ssid_side = "external"
    elif current_ssid_text:
        ssid_side = "other"
    else:
        ssid_side = "none"

    internal_result = probe_internal_reachability(network_cfg=network_cfg, sites=sites, emit_log=None)
    external_result = probe_external_reachability(network_cfg=network_cfg, emit_log=None)

    reachable_sides: List[str] = []
    if bool(internal_result.get("reachable", False)):
        reachable_sides.append("internal")
    if bool(external_result.get("reachable", False)):
        reachable_sides.append("external")

    if reachable_sides == ["internal", "external"]:
        if ssid_side == "internal":
            mode = "internal_only"
        elif ssid_side == "external":
            mode = "external_only"
        else:
            mode = "switching_ready"
    elif reachable_sides == ["internal"]:
        mode = "internal_only"
    elif reachable_sides == ["external"]:
        mode = "external_only"
    else:
        mode = "none_reachable"

    return {
        "current_ssid": current_ssid_text,
        "ssid_side": ssid_side,
        "internal_reachable": bool(internal_result.get("reachable", False)),
        "external_reachable": bool(external_result.get("reachable", False)),
        "reachable_sides": reachable_sides,
        "mode": mode,
        "last_checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "internal_probe": internal_result,
        "external_probe": external_result,
    }


def wait_for_network_stability(
    *,
    network_cfg: Dict[str, Any],
    target_side: str,
    sites: Sequence[Dict[str, Any]] | None = None,
    emit_log: Callable[[str], None] | None = None,
) -> Tuple[bool, str]:
    wait_sec = _as_float(network_cfg.get("post_switch_stabilize_sec", 3), 3)
    if wait_sec > 0:
        if emit_log:
            emit_log(f"[网络] 切网完成，等待网络稳定 {wait_sec:.1f}s")
        time.sleep(wait_sec)

    side = str(target_side or "").strip().lower()
    if side == "internal":
        result = probe_internal_reachability(network_cfg=network_cfg, sites=sites, emit_log=emit_log)
        if bool(result.get("reachable", False)):
            host = str(result.get("successful_host", "") or "").strip()
            return True, f"内网探活成功 {host}" if host else "内网探活成功"
        return False, str(result.get("error", "") or "内网探活失败")
    if side == "external":
        result = probe_external_reachability(network_cfg=network_cfg, emit_log=emit_log)
        if bool(result.get("reachable", False)):
            host = str(result.get("host", "") or "").strip()
            port = int(result.get("port") or 0)
            return True, f"外网探活成功 {host}:{port}" if host else "外网探活成功"
        return False, str(result.get("error", "") or "外网探活失败")
    return True, "未指定网络侧，跳过探活"
