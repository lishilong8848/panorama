from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.network.service.network_stability import (
    probe_external_reachability,
    probe_internal_reachability,
    wait_for_network_stability,
)
from app.modules.network.service.wifi_switch_service import WifiSwitchService
from handover_log_module.repository.download_gateway import (
    download_handover_xlsx_batch,
    set_runtime_config,
)
from handover_log_module.service.handover_source_file_cache_service import HandoverSourceFileCacheService
from wifi_switcher import WifiSwitcher


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return int(default)


class HandoverDownloadService:
    def __init__(
        self,
        config: Dict[str, Any],
        download_browser_pool: Any | None = None,
        *,
        business_root_override: str | Path | None = None,
    ) -> None:
        self.config = config
        self.did_switch_internal_this_run = False
        self._source_file_cache_service = HandoverSourceFileCacheService(
            config,
            business_root_override=business_root_override,
        )
        self._download_browser_pool = download_browser_pool

    def _enabled_buildings(self) -> List[str]:
        sites = self.config.get("sites", [])
        if not isinstance(sites, list):
            return []
        output: List[str] = []
        for site in sites:
            if not isinstance(site, dict):
                continue
            if not bool(site.get("enabled", False)):
                continue
            building = str(site.get("building", "")).strip()
            if building:
                output.append(building)
        return output

    def _download_sites(self) -> List[Dict[str, Any]]:
        download_cfg = self.config.get("download", {})
        sites = download_cfg.get("sites")
        if not isinstance(sites, list):
            sites = self.config.get("sites", [])
        return [site for site in sites if isinstance(site, dict)]

    def _build_time_range(
        self,
        *,
        start_time: str | None = None,
        end_time: str | None = None,
        duty_date: str | None = None,
        duty_shift: str | None = None,
    ) -> Dict[str, Any]:
        cfg = self.config.get("download", {})
        time_format = str(cfg.get("time_format", "%Y-%m-%d %H:%M:%S")).strip() or "%Y-%m-%d %H:%M:%S"

        if (duty_date and not duty_shift) or (duty_shift and not duty_date):
            raise ValueError("duty_date 和 duty_shift 需要同时传入")

        lookback_minutes = _as_int(cfg.get("lookback_minutes", 20), 20)
        if lookback_minutes <= 0:
            lookback_minutes = 20

        if start_time and end_time:
            start_dt = datetime.strptime(start_time, time_format)
            end_dt = datetime.strptime(end_time, time_format)
            if start_dt > end_dt:
                raise ValueError("start_time 不能晚于 end_time")
        elif start_time:
            start_dt = datetime.strptime(start_time, time_format)
            end_dt = start_dt + timedelta(minutes=lookback_minutes)
        else:
            if end_time:
                end_dt = datetime.strptime(end_time, time_format)
            else:
                end_dt = datetime.now()
            start_dt = end_dt - timedelta(minutes=lookback_minutes)
        return {
            "start_time": start_dt.strftime(time_format),
            "end_time": end_dt.strftime(time_format),
            "duty_date": str(duty_date or "").strip() or None,
            "duty_shift": str(duty_shift or "").strip() or None,
            "is_shift_window": False,
            "time_format": time_format,
        }

    def prepare_internal_for_batch_download(self, emit_log: Callable[[str], None] = print) -> bool:
        self._maybe_switch_internal(emit_log)
        return bool(self.did_switch_internal_this_run)

    def _probe_side_reachability(self, side: str, emit_log: Callable[[str], None]) -> tuple[bool, str]:
        network_cfg = self.config.get("network", {})
        if side == "internal":
            result = probe_internal_reachability(
                network_cfg=network_cfg,
                sites=self._download_sites(),
                emit_log=emit_log,
            )
            if bool(result.get("reachable", False)):
                host = str(result.get("successful_host", "") or "").strip()
                return True, f"内网探活成功: {host}" if host else "内网探活成功"
            return False, str(result.get("error", "") or "内网探活失败")
        result = probe_external_reachability(network_cfg=network_cfg, emit_log=emit_log)
        if bool(result.get("reachable", False)):
            host = str(result.get("host", "") or "").strip()
            port = int(result.get("port") or 0)
            return True, f"外网探活成功: {host}:{port}" if host else "外网探活成功"
        return False, str(result.get("error", "") or "外网探活失败")

    def _ensure_target_network_ready(self, target_side: str, emit_log: Callable[[str], None]) -> None:
        side = str(target_side or "").strip().lower()
        if side not in {"internal", "external"}:
            raise ValueError(f"unsupported target_side: {target_side}")

        network_cfg = self.config.get("network", {})
        auto_switch_enabled = bool(network_cfg.get("enable_auto_switch_wifi", True))
        if not auto_switch_enabled:
            emit_log(f"[交接班下载] 当前角色不使用单机切网，直接探测{side}网络可达性")
            ready, detail = self._probe_side_reachability(side, emit_log)
            if not ready:
                raise RuntimeError(f"{side}网络探活失败: {detail}")
            emit_log(f"[交接班下载] {detail}")
            return

        ssid_key = "internal_ssid" if side == "internal" else "external_ssid"
        profile_key = "internal_profile_name" if side == "internal" else "external_profile_name"
        target_ssid = str(network_cfg.get(ssid_key, "")).strip()
        if not target_ssid:
            raise RuntimeError(f"{side}_ssid 未配置")

        wifi = WifiSwitchService({"network": network_cfg})
        current = str(wifi.current_ssid() or "").strip()
        if current != target_ssid:
            ok, msg = wifi.connect(
                target_ssid,
                profile_name=str(network_cfg.get(profile_key, "") or "").strip() or None,
            )
            emit_log(f"[交接班下载] 确保{side}网络: {'成功' if ok else '失败'} - {msg}")
            if not ok:
                raise RuntimeError(f"确保{side}网络失败: {msg}")

        stable_ok, stable_msg = wait_for_network_stability(
            network_cfg=network_cfg,
            target_side=side,
            sites=self._download_sites(),
            emit_log=emit_log,
        )
        if not stable_ok:
            raise RuntimeError(f"{side}网络探活失败: {stable_msg}")
        emit_log(f"[交接班下载] {side}网络已就绪: {stable_msg}")

    def ensure_internal_ready(self, emit_log: Callable[[str], None] = print) -> None:
        self._ensure_target_network_ready("internal", emit_log)

    def ensure_external_ready(self, emit_log: Callable[[str], None] = print) -> None:
        self._ensure_target_network_ready("external", emit_log)

    def _build_wifi_switcher(self, network_cfg: Dict[str, Any], emit_log: Callable[[str], None]) -> WifiSwitcher:
        return WifiSwitcher(
            timeout_sec=int(network_cfg.get("switch_timeout_sec", 30)),
            retry_count=int(network_cfg.get("retry_count", 3)),
            retry_interval_sec=int(network_cfg.get("retry_interval_sec", 2)),
            connect_poll_interval_sec=float(network_cfg.get("connect_poll_interval_sec", 1)),
            fail_fast_on_netsh_error=bool(network_cfg.get("fail_fast_on_netsh_error", True)),
            scan_before_connect=bool(network_cfg.get("scan_before_connect", True)),
            scan_attempts=int(network_cfg.get("scan_attempts", 3)),
            scan_wait_sec=int(network_cfg.get("scan_wait_sec", 2)),
            strict_target_visible_before_connect=bool(network_cfg.get("strict_target_visible_before_connect", True)),
            connect_with_ssid_param=bool(network_cfg.get("connect_with_ssid_param", True)),
            preferred_interface=str(network_cfg.get("preferred_interface", "") or "").strip(),
            auto_disconnect_before_connect=bool(network_cfg.get("auto_disconnect_before_connect", True)),
            hard_recovery_enabled=bool(network_cfg.get("hard_recovery_enabled", True)),
            hard_recovery_after_scan_failures=int(network_cfg.get("hard_recovery_after_scan_failures", 2)),
            hard_recovery_steps=network_cfg.get("hard_recovery_steps", ["toggle_adapter", "restart_wlansvc"]),
            hard_recovery_cooldown_sec=int(network_cfg.get("hard_recovery_cooldown_sec", 20)),
            require_admin_for_hard_recovery=bool(network_cfg.get("require_admin_for_hard_recovery", True)),
            log_cb=emit_log,
        )

    def _maybe_switch_internal(self, emit_log: Callable[[str], None]) -> None:
        self.did_switch_internal_this_run = False
        download_cfg = self.config.get("download", {})
        if not bool(download_cfg.get("switch_to_internal_before_download", True)):
            return

        network_cfg = self.config.get("network", {})
        if not bool(network_cfg.get("enable_auto_switch_wifi", True)):
            emit_log("[交接班下载] 当前角色不使用单机切网，按当前网络继续执行内网阶段")
            return

        internal_ssid = str(network_cfg.get("internal_ssid", "")).strip()
        if not internal_ssid:
            emit_log("[交接班下载] 未配置内网 SSID，跳过切换内网")
            return

        wifi = self._build_wifi_switcher(network_cfg, emit_log)
        require_saved = bool(network_cfg.get("require_saved_profiles", True))
        ok, msg = wifi.connect(
            internal_ssid,
            require_saved_profile=require_saved,
            profile_name=str(network_cfg.get("internal_profile_name", "") or "").strip() or None,
        )
        emit_log(f"[交接班下载] 切换内网: {'成功' if ok else '失败'} - {msg}")
        if not ok:
            raise RuntimeError(f"切换内网失败: {msg}")

        stable_ok, stable_msg = wait_for_network_stability(
            network_cfg=network_cfg,
            target_side="internal",
            sites=self._download_sites(),
            emit_log=emit_log,
        )
        if not stable_ok:
            raise RuntimeError(f"切网后网络未稳定: {stable_msg}")
        emit_log(f"[交接班下载] 内网稳定检查: {stable_msg}")
        self.did_switch_internal_this_run = True

    def switch_external_after_download(self, emit_log: Callable[[str], None]) -> None:
        network_cfg = self.config.get("network", {})
        if not bool(network_cfg.get("enable_auto_switch_wifi", True)):
            emit_log("[交接班下载] 当前角色不使用单机切网，按当前网络继续执行外网阶段")
            return

        external_ssid = str(network_cfg.get("external_ssid", "")).strip()
        if not external_ssid:
            emit_log("[交接班下载] 未配置外网 SSID，跳过切换外网")
            return

        wifi = self._build_wifi_switcher(network_cfg, emit_log)
        require_saved = bool(network_cfg.get("require_saved_profiles", True))
        ok, msg = wifi.connect(
            external_ssid,
            require_saved_profile=require_saved,
            profile_name=str(network_cfg.get("external_profile_name", "") or "").strip() or None,
        )
        emit_log(f"[交接班下载] 切换外网: {'成功' if ok else '失败'} - {msg}")
        if not ok:
            return

        stable_ok, stable_msg = wait_for_network_stability(
            network_cfg=network_cfg,
            target_side="external",
            sites=self._download_sites(),
            emit_log=emit_log,
        )
        if stable_ok:
            emit_log(f"[交接班下载] 外网稳定检查: {stable_msg}")
        else:
            emit_log(f"[交接班下载] 外网稳定检查失败: {stable_msg}")

    def run(
        self,
        buildings: List[str] | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        duty_date: str | None = None,
        duty_shift: str | None = None,
        switch_network: bool = True,
        reuse_cached: bool = True,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        self.did_switch_internal_this_run = False
        target_buildings = buildings[:] if buildings else self._enabled_buildings()
        if not target_buildings:
            raise ValueError("没有可下载的楼栋，请检查 handover_log.sites 或传入 buildings 参数")

        explicit_time_window = bool(start_time or end_time)
        time_range = self._build_time_range(
            start_time=start_time,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
        )
        start_time = str(time_range.get("start_time", "")).strip()
        end_time_text = str(time_range.get("end_time", "")).strip()
        duty_date_text = str(time_range.get("duty_date", "")).strip() or None
        duty_shift_text = str(time_range.get("duty_shift", "")).strip() or None
        is_shift_window = bool(time_range.get("is_shift_window", False))

        download_cfg = self.config.get("download", {})
        template_name = str(download_cfg.get("template_name", "")).strip()
        scale_label = str(download_cfg.get("scale_label", "")).strip()
        if not template_name:
            raise ValueError("配置错误: handover_log.download.template_name 不能为空")
        if not scale_label:
            raise ValueError("配置错误: handover_log.download.scale_label 不能为空")

        query_result_timeout_ms = _as_int(download_cfg.get("query_result_timeout_ms", 20000), 20000)
        download_event_timeout_ms = _as_int(download_cfg.get("download_event_timeout_ms", 120000), 120000)
        login_fill_timeout_ms = _as_int(download_cfg.get("login_fill_timeout_ms", 5000), 5000)
        menu_visible_timeout_ms = _as_int(download_cfg.get("menu_visible_timeout_ms", 20000), 20000)
        iframe_timeout_ms = _as_int(download_cfg.get("iframe_timeout_ms", 15000), 15000)
        start_end_visible_timeout_ms = _as_int(download_cfg.get("start_end_visible_timeout_ms", 5000), 5000)
        page_refresh_retry_count = _as_int(download_cfg.get("page_refresh_retry_count", 1), 1)
        force_iframe_reopen_each_task = bool(download_cfg.get("force_iframe_reopen_each_task", True))
        parallel_by_building = bool(download_cfg.get("parallel_by_building", False))
        site_start_delay_sec = max(0, _as_int(download_cfg.get("site_start_delay_sec", 1), 1))
        debug_step_log = bool(download_cfg.get("debug_step_log", True))
        export_button_text = str(download_cfg.get("export_button_text", "原样导出")).strip() or "原样导出"
        menu_path_raw = download_cfg.get("menu_path", ["报表报告", "数据查询", "即时报表"])
        menu_path = menu_path_raw if isinstance(menu_path_raw, list) else ["报表报告", "数据查询", "即时报表"]

        global_download_cfg = self.config.get("_global_download", {})
        max_retries = _as_int(download_cfg.get("max_retries", global_download_cfg.get("max_retries", 2)), 2)
        retry_wait_sec = _as_int(download_cfg.get("retry_wait_sec", global_download_cfg.get("retry_wait_sec", 2)), 2)

        download_dir = self._source_file_cache_service.download_cache_root()
        download_dir.mkdir(parents=True, exist_ok=True)

        pending_buildings: List[str] = []
        reused_results: List[Dict[str, Any]] = []
        if reuse_cached:
            for building in target_buildings:
                identity = self._source_file_cache_service.build_download_identity(
                    building=building,
                    template_name=template_name,
                    duty_date=duty_date_text or "",
                    duty_shift=duty_shift_text or "",
                    start_time=start_time,
                    end_time=end_time_text,
                    scale_label=scale_label,
                )
                cached_path = self._source_file_cache_service.lookup_downloaded_source(identity=identity)
                if cached_path:
                    emit_log(f"[交接班下载][{building}] 复用共享源文件: {cached_path}")
                    reused_results.append(
                        {
                            "building": building,
                            "success": True,
                            "file_path": cached_path,
                            "reused_cached_source": True,
                        }
                    )
                    continue
                pending_buildings.append(building)
        else:
            pending_buildings = list(target_buildings)

        if switch_network and pending_buildings:
            self._maybe_switch_internal(emit_log)

        downloaded_results: List[Dict[str, Any]] = []
        if pending_buildings:
            set_runtime_config(
                {
                    "handover_log": self.config,
                    "download": self.config.get("_global_download", {}),
                    "network": self.config.get("network", {}),
                }
            )
            emit_log(f"[交接班下载] 执行模式: {'并发' if parallel_by_building else '串行'}")
            emit_log(
                "[交接班下载] 下载参数: "
                f"parallel_by_building={str(parallel_by_building).lower()}, "
                f"menu_visible_timeout_ms={menu_visible_timeout_ms}, "
                f"iframe_timeout_ms={iframe_timeout_ms}, "
                f"query_result_timeout_ms={query_result_timeout_ms}, "
                f"start_end_visible_timeout_ms={start_end_visible_timeout_ms}"
            )
            if explicit_time_window:
                emit_log(f"[交接班下载] 页面查询时间窗: start={start_time}, end={end_time_text}")
            else:
                emit_log(
                    f"[交接班下载] 页面查询时间窗: 最近 {_as_int(download_cfg.get('lookback_minutes', 20), 20)} 分钟, "
                    f"start={start_time}, end={end_time_text}"
                )

            for building in pending_buildings:
                if is_shift_window and duty_date_text and duty_shift_text:
                    emit_log(
                        f"[交接班下载][{building}] 已入队: duty_date={duty_date_text}, duty_shift={duty_shift_text}, "
                        f"start={start_time}, end={end_time_text}, 刻度={scale_label}"
                    )
                else:
                    emit_log(f"[交接班下载][{building}] 已入队: start={start_time}, end={end_time_text}, 刻度={scale_label}")

            downloaded_results = download_handover_xlsx_batch(
                buildings=pending_buildings,
                start_time=start_time,
                end_time=end_time_text,
                scale_label=scale_label,
                template_name=template_name,
                save_dir=str(download_dir),
                query_result_timeout_ms=query_result_timeout_ms,
                download_event_timeout_ms=download_event_timeout_ms,
                login_fill_timeout_ms=login_fill_timeout_ms,
                menu_visible_timeout_ms=menu_visible_timeout_ms,
                iframe_timeout_ms=iframe_timeout_ms,
                start_end_visible_timeout_ms=start_end_visible_timeout_ms,
                page_refresh_retry_count=page_refresh_retry_count,
                max_retries=max_retries,
                retry_wait_sec=retry_wait_sec,
                force_iframe_reopen_each_task=force_iframe_reopen_each_task,
                export_button_text=export_button_text,
                menu_path=menu_path,
                parallel_by_building=parallel_by_building,
                site_start_delay_sec=site_start_delay_sec,
                debug_step_log=debug_step_log,
                browser_pool=self._download_browser_pool,
            )

            for item in downloaded_results:
                if not bool(item.get("success")):
                    continue
                building = str(item.get("building", "")).strip()
                file_path = str(item.get("file_path", "")).strip()
                if not building or not file_path:
                    continue
                identity = self._source_file_cache_service.build_download_identity(
                    building=building,
                    template_name=template_name,
                    duty_date=duty_date_text or "",
                    duty_shift=duty_shift_text or "",
                    start_time=start_time,
                    end_time=end_time_text,
                    scale_label=scale_label,
                )
                self._source_file_cache_service.register_downloaded_source(
                    identity=identity,
                    file_path=file_path,
                    emit_log=emit_log,
                )
        else:
            emit_log("[交接班下载] 全部楼栋命中共享源文件，跳过下载")

        results = list(reused_results) + list(downloaded_results)

        for result in results:
            building = str(result.get("building", "")).strip() or "-"
            if result.get("success"):
                emit_log(f"[交接班下载][{building}] 下载成功: {result.get('file_path')}")
            else:
                emit_log(f"[交接班下载][{building}] 下载失败: {result.get('error')}")

        success_files = [x for x in results if x.get("success")]
        failed = [x for x in results if not x.get("success")]
        return {
            "start_time": start_time,
            "end_time": end_time_text,
            "duty_date": duty_date_text,
            "duty_shift": duty_shift_text,
            "is_shift_window": is_shift_window,
            "results": results,
            "success_files": success_files,
            "failed": failed,
        }
