from __future__ import annotations

import copy
import ctypes
import locale
import os
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple


class WifiSwitchError(RuntimeError):
    pass


class WifiSwitcher:
    def __init__(
        self,
        timeout_sec: int = 30,
        retry_count: int = 2,
        retry_interval_sec: int = 2,
        connect_poll_interval_sec: float = 1.0,
        fail_fast_on_netsh_error: bool = True,
        scan_before_connect: bool = True,
        scan_attempts: int = 3,
        scan_wait_sec: int = 2,
        strict_target_visible_before_connect: bool = True,
        connect_with_ssid_param: bool = True,
        preferred_interface: str = "",
        auto_disconnect_before_connect: bool = True,
        hard_recovery_enabled: bool = True,
        hard_recovery_after_scan_failures: int = 2,
        hard_recovery_steps: List[str] | None = None,
        hard_recovery_cooldown_sec: int = 20,
        require_admin_for_hard_recovery: bool = True,
        log_cb: Callable[[str], None] | None = None,
    ) -> None:
        self.timeout_sec = max(1, int(timeout_sec))
        self.retry_count = max(1, int(retry_count))
        self.retry_interval_sec = max(1, int(retry_interval_sec))
        self.connect_poll_interval_sec = max(0.2, float(connect_poll_interval_sec))
        self.fail_fast_on_netsh_error = bool(fail_fast_on_netsh_error)

        self.scan_before_connect = bool(scan_before_connect)
        self.scan_attempts = max(1, int(scan_attempts))
        self.scan_wait_sec = max(1, int(scan_wait_sec))
        self.strict_target_visible_before_connect = bool(strict_target_visible_before_connect)

        self.connect_with_ssid_param = bool(connect_with_ssid_param)
        self.preferred_interface = str(preferred_interface or "").strip()
        self.auto_disconnect_before_connect = bool(auto_disconnect_before_connect)

        self.hard_recovery_enabled = bool(hard_recovery_enabled)
        self.hard_recovery_after_scan_failures = max(1, int(hard_recovery_after_scan_failures))
        self.hard_recovery_steps = [
            step
            for step in (hard_recovery_steps or ["toggle_adapter", "restart_wlansvc"])
            if step in {"toggle_adapter", "restart_wlansvc"}
        ]
        if not self.hard_recovery_steps:
            self.hard_recovery_steps = ["toggle_adapter", "restart_wlansvc"]
        self.hard_recovery_cooldown_sec = max(0, int(hard_recovery_cooldown_sec))
        self.require_admin_for_hard_recovery = bool(require_admin_for_hard_recovery)

        self._log_cb = log_cb
        self._last_switch_report: Dict[str, Any] = {}
        self._reset_report()

    def _reset_report(self) -> None:
        self._last_switch_report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "target_ssid": "",
            "profile_name": "",
            "interface_name": "",
            "current_ssid": "",
            "result": "unknown",
            "stage": "idle",
            "error_type": "",
            "error": "",
            "visible_target": None,
            "visible_ssid_count": 0,
            "hard_recovery_attempted": False,
            "is_admin": self.is_admin(),
            "elapsed_ms": 0,
        }

    def _update_report(self, **kwargs: Any) -> None:
        self._last_switch_report.update(kwargs)
        self._last_switch_report["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def get_last_switch_report(self) -> Dict[str, Any]:
        return copy.deepcopy(self._last_switch_report)

    def _log(self, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        line = f"[WiFi] {text}"
        if self._log_cb is not None:
            try:
                self._log_cb(line)
            except Exception:  # noqa: BLE001
                pass
        else:
            print(line)

    def _run(self, *args: str, timeout: int = 10) -> subprocess.CompletedProcess:
        startupinfo = None
        creationflags = 0
        if os.name == "nt":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        return subprocess.run(
            ["netsh", *args],
            capture_output=True,
            text=False,
            timeout=timeout,
            startupinfo=startupinfo,
            creationflags=creationflags,
        )

    def _run_powershell(self, command: str, timeout: int = 20) -> subprocess.CompletedProcess:
        startupinfo = None
        creationflags = 0
        if os.name == "nt":
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        return subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
            capture_output=True,
            text=False,
            timeout=timeout,
            startupinfo=startupinfo,
            creationflags=creationflags,
        )

    @staticmethod
    def _preferred_decode_encodings() -> List[str]:
        encodings: List[str] = []
        if os.name == "nt":
            try:
                oem_cp = int(ctypes.windll.kernel32.GetOEMCP())
                if oem_cp > 0:
                    encodings.append(f"cp{oem_cp}")
            except Exception:  # noqa: BLE001
                pass
            encodings.extend(["mbcs"])
        try:
            pref = locale.getpreferredencoding(False)
            if pref:
                encodings.append(pref)
        except Exception:  # noqa: BLE001
            pass
        encodings.extend(["gbk", "cp936", "utf-8"])
        # de-dup while preserving order
        seen = set()
        result: List[str] = []
        for enc in encodings:
            key = str(enc or "").strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            result.append(enc)
        return result

    @staticmethod
    def _decode_score(text: str) -> int:
        if not text:
            return -9999
        score = 0
        # Prefer texts that contain likely netsh terms.
        positive_markers = (
            "SSID",
            "Profile",
            "配置文件",
            "连接",
            "成功",
            "已成功",
            "请求",
            "interface",
            "无线",
            "network",
        )
        for marker in positive_markers:
            if marker in text:
                score += 8
        # Penalize common mojibake fragments.
        negative_markers = (
            "\u951f",
            "�",
            "銆",
            "\u951b",
            "\u5bb8\u63d2",
            "\u93b4\u612c",
            "\u93c3",
            "\u7f03",
            "\u9352",
        )
        for marker in negative_markers:
            if marker in text:
                score -= 12
        # Slightly reward CJK and ASCII readability, but avoid length-only bias.
        cjk_count = len(re.findall(r"[\u4e00-\u9fff]", text))
        ascii_count = len(re.findall(r"[A-Za-z0-9]", text))
        score += min(cjk_count, 20)
        score += min(ascii_count // 4, 10)
        return score

    @staticmethod
    def _decode_output(raw: bytes) -> str:
        if not raw:
            return ""
        candidates: List[str] = []
        for enc in WifiSwitcher._preferred_decode_encodings():
            try:
                candidates.append(raw.decode(enc, errors="ignore"))
            except Exception:  # noqa: BLE001
                continue
        if not candidates:
            return raw.decode("latin1", errors="ignore")
        return max(candidates, key=WifiSwitcher._decode_score)

    @staticmethod
    def _extract_by_colon(line: str) -> Optional[str]:
        if ":" in line:
            return line.split(":", 1)[1].strip()
        if "：" in line:
            return line.split("：", 1)[1].strip()
        return None

    @staticmethod
    def is_admin() -> bool:
        if os.name != "nt":
            return False
        try:
            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    def _is_connect_command_error(output: str) -> bool:
        text = str(output or "").strip().lower()
        if not text:
            return False
        success_markers = [
            "completed successfully",
            "has been completed successfully",
            "已成功",
            "成功完成",
            "连接请求已经成功",
        ]
        if any(marker in text for marker in success_markers):
            return False
        error_markers = [
            "not found",
            "cannot",
            "failed",
            "error",
            "there is no wireless interface",
            "profile",
            "找不到",
            "不存在",
            "失败",
            "错误",
            "无可用",
            "未能",
            "没有无线接口",
            "无法",
        ]
        return any(marker in text for marker in error_markers)

    @staticmethod
    def _classify_connect_error(code: int, output: str) -> str:
        text = str(output or "").strip().lower()
        raw = str(output or "").strip()
        if "无法用于连接" in raw or "cannot be used to connect" in text:
            return "profile_unusable"
        if ("profile" in text and "not found" in text) or "配置文件不存在" in raw or "未找到配置文件" in raw:
            return "profile_missing"
        if "there is no wireless interface" in text or "没有无线接口" in raw or "无线接口" in raw:
            return "interface_down"
        if "parameter is incorrect" in text or "invalid" in text or "参数错误" in raw:
            return "invalid_parameter"
        if "not in range" in text or "无法连接到此网络" in raw:
            return "target_not_visible"
        if code != 0:
            return "connect_cmd_failed"
        if WifiSwitcher._is_connect_command_error(output):
            return "connect_cmd_failed"
        return ""

    def get_current_ssid(self) -> Optional[str]:
        try:
            result = self._run("wlan", "show", "interfaces", timeout=8)
            if result.returncode != 0:
                return None
            output = self._decode_output(result.stdout)
            for line in output.splitlines():
                raw = line.strip()
                if "BSSID" in raw:
                    continue
                if re.search(r"\bSSID\b", raw, flags=re.IGNORECASE):
                    value = self._extract_by_colon(raw)
                    if value:
                        return value
            return None
        except Exception:  # noqa: BLE001
            return None

    def get_interface_name(self) -> str:
        if self.preferred_interface:
            return self.preferred_interface
        try:
            result = self._run("wlan", "show", "interfaces", timeout=8)
            if result.returncode != 0:
                return ""
            output = self._decode_output(result.stdout)
            for line in output.splitlines():
                raw = line.strip()
                if not raw:
                    continue
                if raw.lower().startswith("name") or raw.startswith("名称"):
                    value = self._extract_by_colon(raw)
                    if value:
                        return value
            return ""
        except Exception:  # noqa: BLE001
            return ""

    def get_saved_profiles(self) -> List[str]:
        profiles: List[str] = []
        try:
            result = self._run("wlan", "show", "profiles", timeout=8)
            if result.returncode != 0:
                return profiles
            output = self._decode_output(result.stdout)
            for line in output.splitlines():
                text = line.strip()
                if "All User Profile" in text or "所有用户配置文件" in text:
                    value = self._extract_by_colon(text)
                    if value:
                        profiles.append(value)
            return profiles
        except Exception:  # noqa: BLE001
            return profiles

    def trigger_scan(self, interface_name: str = "") -> Tuple[bool, str]:
        args = ["wlan", "show", "networks", "mode=bssid"]
        if interface_name:
            args.append(f"interface={interface_name}")
        try:
            result = self._run(*args, timeout=12)
            output = self._decode_output(result.stdout) or self._decode_output(result.stderr)
            if result.returncode != 0:
                return False, output or f"netsh扫描失败: code={result.returncode}"
            return True, output
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)

    def list_visible_ssids(self, interface_name: str = "") -> List[str]:
        args = ["wlan", "show", "networks", "mode=bssid"]
        if interface_name:
            args.append(f"interface={interface_name}")
        ssids: List[str] = []
        try:
            result = self._run(*args, timeout=12)
            if result.returncode != 0:
                return ssids
            output = self._decode_output(result.stdout)
            for line in output.splitlines():
                raw = line.strip()
                m = re.match(r"^SSID\s+\d+\s*[:：]\s*(.*)$", raw, flags=re.IGNORECASE)
                if m:
                    value = m.group(1).strip()
                    if value:
                        ssids.append(value)
            return ssids
        except Exception:  # noqa: BLE001
            return ssids

    def _disconnect(self, interface_name: str = "") -> None:
        args = ["wlan", "disconnect"]
        if interface_name:
            args.append(f"interface={interface_name}")
        try:
            self._run(*args, timeout=8)
        except Exception:  # noqa: BLE001
            return

    def _hard_recovery(self, interface_name: str) -> Tuple[bool, str, str]:
        if not self.hard_recovery_enabled:
            return False, "硬恢复已禁用", "hard_recovery_disabled"
        if self.require_admin_for_hard_recovery and not self.is_admin():
            return False, "硬恢复需要管理员权限，请以管理员身份运行程序", "admin_required"

        self._log(f"触发硬恢复: interface={interface_name or '-'}, steps={self.hard_recovery_steps}")
        for step in self.hard_recovery_steps:
            if step == "toggle_adapter":
                if not interface_name:
                    continue
                off = self._run(
                    "interface",
                    "set",
                    "interface",
                    f'name="{interface_name}"',
                    "admin=disabled",
                    timeout=15,
                )
                on = self._run(
                    "interface",
                    "set",
                    "interface",
                    f'name="{interface_name}"',
                    "admin=enabled",
                    timeout=20,
                )
                if off.returncode != 0 or on.returncode != 0:
                    out = (self._decode_output(off.stderr) or self._decode_output(off.stdout) or "").strip()
                    out2 = (self._decode_output(on.stderr) or self._decode_output(on.stdout) or "").strip()
                    return False, f"切换网卡失败: {out or out2 or '未知错误'}", "hard_recovery_failed"
            elif step == "restart_wlansvc":
                ps = self._run_powershell("Restart-Service -Name WlanSvc -Force", timeout=20)
                if ps.returncode != 0:
                    out = (self._decode_output(ps.stderr) or self._decode_output(ps.stdout) or "").strip()
                    return False, f"重启WlanSvc失败: {out or '未知错误'}", "hard_recovery_failed"

        if self.hard_recovery_cooldown_sec > 0:
            time.sleep(self.hard_recovery_cooldown_sec)
        return True, "硬恢复完成", ""

    def _scan_target_visible(self, target_ssid: str, interface_name: str, attempt_tag: str) -> Tuple[bool, int]:
        visible = False
        visible_count = 0
        for scan_idx in range(1, self.scan_attempts + 1):
            ok, msg = self.trigger_scan(interface_name=interface_name)
            self._log(
                f"{attempt_tag} 扫描 {scan_idx}/{self.scan_attempts}: "
                f"{'ok' if ok else 'fail'} {msg[:120] if msg else '-'}"
            )
            time.sleep(self.scan_wait_sec)
            visible_ssids = self.list_visible_ssids(interface_name=interface_name)
            visible_count = len(visible_ssids)
            visible = target_ssid in visible_ssids
            self._log(
                f"{attempt_tag} 可见性: target={target_ssid}, visible={visible}, ssid_count={visible_count}"
            )
            if visible:
                break
        return visible, visible_count

    def connect(
        self,
        target_ssid: str,
        require_saved_profile: bool = True,
        profile_name: str | None = None,
    ) -> Tuple[bool, str]:
        self._reset_report()
        target_ssid = str(target_ssid or "").strip()
        profile = str(profile_name or "").strip() or target_ssid
        if not target_ssid:
            self._update_report(result="failed", error_type="invalid_target", error="目标SSID为空")
            return False, "目标SSID为空"

        start = time.perf_counter()
        interface_name = self.get_interface_name()
        current = self.get_current_ssid()
        self._update_report(
            target_ssid=target_ssid,
            profile_name=profile,
            interface_name=interface_name,
            current_ssid=current or "",
            stage="start",
            is_admin=self.is_admin(),
        )
        self._log(
            f"开始连接 target={target_ssid}, profile={profile}, interface={interface_name or '-'}, "
            f"current={current or '-'}, timeout={self.timeout_sec}s, retry={self.retry_count}"
        )

        if current == target_ssid:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            self._update_report(result="success", stage="already_connected", elapsed_ms=elapsed_ms)
            return True, f"已在目标WiFi: {target_ssid}"

        if require_saved_profile:
            saved = self.get_saved_profiles()
            if profile not in saved:
                elapsed_ms = int((time.perf_counter() - start) * 1000)
                err = f"本机未保存WiFi配置: {profile}"
                self._update_report(result="failed", stage="precheck", error_type="profile_missing", error=err, elapsed_ms=elapsed_ms)
                self._log(err)
                return False, err

        last_error = f"超时未连接到 {target_ssid}"
        last_error_type = "timeout"
        scan_failure_count = 0
        hard_recovery_attempted = False

        for attempt in range(1, self.retry_count + 1):
            tag = f"attempt={attempt}/{self.retry_count}"
            self._update_report(stage="scan")
            visible = True
            visible_count = 0
            if self.scan_before_connect:
                visible, visible_count = self._scan_target_visible(target_ssid, interface_name, tag)
                self._update_report(visible_target=visible, visible_ssid_count=visible_count)
                if self.strict_target_visible_before_connect and not visible:
                    scan_failure_count += 1
                    last_error = f"扫描未发现目标SSID: {target_ssid}"
                    last_error_type = "target_not_visible"
                    self._log(f"{tag} {last_error}")
                    if (
                        self.hard_recovery_enabled
                        and scan_failure_count >= self.hard_recovery_after_scan_failures
                    ):
                        hard_recovery_attempted = True
                        self._update_report(hard_recovery_attempted=True, stage="hard_recovery")
                        ok_hr, msg_hr, hr_error_type = self._hard_recovery(interface_name)
                        self._log(f"{tag} 硬恢复: {'成功' if ok_hr else '失败'} {msg_hr}")
                        if not ok_hr:
                            last_error = msg_hr
                            last_error_type = hr_error_type or "hard_recovery_failed"
                            if hr_error_type == "admin_required":
                                break
                        else:
                            visible, visible_count = self._scan_target_visible(
                                target_ssid,
                                interface_name,
                                f"{tag}/recheck",
                            )
                            self._update_report(visible_target=visible, visible_ssid_count=visible_count)
                            if self.strict_target_visible_before_connect and not visible:
                                if attempt < self.retry_count:
                                    time.sleep(self.retry_interval_sec)
                                continue
                    else:
                        if attempt < self.retry_count:
                            time.sleep(self.retry_interval_sec)
                        continue

            self._update_report(stage="connect_cmd")
            if self.auto_disconnect_before_connect:
                self._disconnect(interface_name)
            try:
                args = ["wlan", "connect", f"name={profile}"]
                if self.connect_with_ssid_param:
                    args.append(f"ssid={target_ssid}")
                if interface_name:
                    args.append(f"interface={interface_name}")
                result = self._run(*args, timeout=12)
                stdout_text = self._decode_output(result.stdout)
                stderr_text = self._decode_output(result.stderr)
                cmd_output = "\n".join([x for x in [stdout_text, stderr_text] if x]).strip()
                error_type = self._classify_connect_error(result.returncode, cmd_output)
                self._log(
                    f"{tag} netsh connect: code={result.returncode}, error_type={error_type or '-'}, "
                    f"output={cmd_output[:240] or '-'}"
                )

                if self.fail_fast_on_netsh_error and error_type:
                    last_error_type = error_type
                    last_error = f"netsh连接命令失败: code={result.returncode}; {cmd_output or '无输出'}"
                    if attempt < self.retry_count:
                        time.sleep(self.retry_interval_sec)
                    continue

                self._update_report(stage="poll")
                poll_start = time.perf_counter()
                while time.perf_counter() - poll_start <= self.timeout_sec:
                    current = self.get_current_ssid()
                    if current == target_ssid:
                        elapsed_ms = int((time.perf_counter() - start) * 1000)
                        self._update_report(
                            result="success",
                            stage="done",
                            current_ssid=current,
                            error_type="",
                            error="",
                            elapsed_ms=elapsed_ms,
                            hard_recovery_attempted=hard_recovery_attempted,
                        )
                        self._log(f"切换成功: target={target_ssid}, elapsed={elapsed_ms}ms")
                        return True, f"切换成功: {target_ssid}"
                    time.sleep(self.connect_poll_interval_sec)

                last_error = f"超时未连接到 {target_ssid}"
                last_error_type = "timeout"
                self._log(f"{tag} 轮询超时: {last_error}")
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                last_error_type = "exception"
                self._log(f"{tag} 异常: {last_error}")

            if attempt < self.retry_count:
                time.sleep(self.retry_interval_sec)

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        self._update_report(
            result="failed",
            stage="done",
            current_ssid=self.get_current_ssid() or "",
            error_type=last_error_type,
            error=last_error,
            elapsed_ms=elapsed_ms,
            hard_recovery_attempted=hard_recovery_attempted,
        )
        self._log(
            f"切换失败: target={target_ssid}, profile={profile}, error_type={last_error_type}, "
            f"elapsed={elapsed_ms}ms, error={last_error}"
        )
        return False, last_error

    def connect_or_raise(
        self,
        target_ssid: str,
        require_saved_profile: bool = True,
        profile_name: str | None = None,
    ) -> None:
        ok, msg = self.connect(
            target_ssid,
            require_saved_profile=require_saved_profile,
            profile_name=profile_name,
        )
        if not ok:
            raise WifiSwitchError(msg)
