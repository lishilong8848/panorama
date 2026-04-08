from __future__ import annotations

import copy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List

from handover_log_module.repository.download_gateway import (
    download_handover_xlsx_batch,
    set_runtime_config,
)
from handover_log_module.service.handover_source_file_cache_service import HandoverSourceFileCacheService


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
        self._business_root_override = Path(business_root_override) if business_root_override else None
        self._source_file_cache_service = HandoverSourceFileCacheService(
            config,
            business_root_override=business_root_override,
        )
        self._download_browser_pool = download_browser_pool

    def _clone_with_download_config(self, download_cfg: Dict[str, Any]) -> "HandoverDownloadService":
        cloned_config = copy.deepcopy(self.config if isinstance(self.config, dict) else {})
        cloned_config["download"] = copy.deepcopy(download_cfg if isinstance(download_cfg, dict) else {})
        return HandoverDownloadService(
            cloned_config,
            self._download_browser_pool,
            business_root_override=self._business_root_override,
        )

    def _capacity_download_config(self, *, template_name_override: str | None = None) -> Dict[str, Any]:
        base_cfg = copy.deepcopy(
            self.config.get("download", {}) if isinstance(self.config.get("download", {}), dict) else {}
        )
        capacity_root_cfg = (
            self.config.get("capacity_report", {})
            if isinstance(self.config.get("capacity_report", {}), dict)
            else {}
        )
        capacity_download_cfg = (
            capacity_root_cfg.get("download", {})
            if isinstance(capacity_root_cfg.get("download", {}), dict)
            else {}
        )
        base_cfg.update(copy.deepcopy(capacity_download_cfg))
        template_name = str(template_name_override or base_cfg.get("template_name", "")).strip()
        if template_name:
            base_cfg["template_name"] = template_name
        e_template_name = str(capacity_download_cfg.get("e_template_name", "") or "").strip()
        if e_template_name and template_name == e_template_name:
            default_e_query_timeout_ms = 90000
            query_timeout_ms = _as_int(base_cfg.get("query_result_timeout_ms", 20000), 20000)
            e_query_timeout_ms = _as_int(
                capacity_download_cfg.get("e_query_result_timeout_ms", default_e_query_timeout_ms),
                default_e_query_timeout_ms,
            )
            base_cfg["query_result_timeout_ms"] = max(query_timeout_ms, e_query_timeout_ms)
        return base_cfg

    @staticmethod
    def _merge_multi_download_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
        merged_results: List[Dict[str, Any]] = []
        merged_success_files: List[Dict[str, Any]] = []
        merged_failed: List[Dict[str, Any]] = []
        start_time = ""
        end_time = ""
        duty_date = ""
        duty_shift = ""
        is_shift_window = False
        for item in results:
            if not isinstance(item, dict):
                continue
            if not start_time:
                start_time = str(item.get("start_time", "") or "").strip()
            if not end_time:
                end_time = str(item.get("end_time", "") or "").strip()
            if not duty_date:
                duty_date = str(item.get("duty_date", "") or "").strip()
            if not duty_shift:
                duty_shift = str(item.get("duty_shift", "") or "").strip()
            is_shift_window = bool(is_shift_window or item.get("is_shift_window", False))
            merged_results.extend([row for row in item.get("results", []) if isinstance(row, dict)])
            merged_success_files.extend([row for row in item.get("success_files", []) if isinstance(row, dict)])
            merged_failed.extend([row for row in item.get("failed", []) if isinstance(row, dict)])
        return {
            "start_time": start_time,
            "end_time": end_time,
            "duty_date": duty_date,
            "duty_shift": duty_shift,
            "is_shift_window": is_shift_window,
            "results": merged_results,
            "success_files": merged_success_files,
            "failed": merged_failed,
        }

    def run_capacity_only(
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
        target_buildings = buildings[:] if buildings else self._enabled_buildings()
        target_buildings = [str(item or "").strip() for item in target_buildings if str(item or "").strip()]
        if not target_buildings:
            raise ValueError("没有可下载的楼栋，请检查 handover_log.sites 或传入 buildings 参数")

        capacity_root_cfg = (
            self.config.get("capacity_report", {})
            if isinstance(self.config.get("capacity_report", {}), dict)
            else {}
        )
        capacity_download_cfg = (
            capacity_root_cfg.get("download", {})
            if isinstance(capacity_root_cfg.get("download", {}), dict)
            else {}
        )
        normal_template_name = str(capacity_download_cfg.get("template_name", "") or "").strip()
        e_template_name = str(capacity_download_cfg.get("e_template_name", "") or "").strip() or normal_template_name
        if not normal_template_name:
            raise ValueError("配置错误: handover_log.capacity_report.download.template_name 不能为空")

        grouped_buildings: List[tuple[List[str], str]] = []
        normal_buildings = [building for building in target_buildings if building != "E楼"]
        e_buildings = [building for building in target_buildings if building == "E楼"]
        if normal_buildings:
            grouped_buildings.append((normal_buildings, normal_template_name))
        if e_buildings:
            grouped_buildings.append((e_buildings, e_template_name))

        results: List[Dict[str, Any]] = []
        switched = False
        for group_buildings, template_name in grouped_buildings:
            cloned_service = self._clone_with_download_config(
                self._capacity_download_config(template_name_override=template_name)
            )
            results.append(
                cloned_service.run(
                    buildings=group_buildings,
                    start_time=start_time,
                    end_time=end_time,
                    duty_date=duty_date,
                    duty_shift=duty_shift,
                    switch_network=bool(switch_network and not switched),
                    reuse_cached=reuse_cached,
                    emit_log=emit_log,
                )
            )
            switched = True
        merged = self._merge_multi_download_results(results)
        merged["report_kind"] = "capacity"
        return merged

    def run_with_capacity_report(
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
        handover_result = self.run(
            buildings=buildings,
            start_time=start_time,
            end_time=end_time,
            duty_date=duty_date,
            duty_shift=duty_shift,
            switch_network=switch_network,
            reuse_cached=reuse_cached,
            emit_log=emit_log,
        )
        capacity_result = self.run_capacity_only(
            buildings=buildings,
            start_time=str(handover_result.get("start_time", "") or "").strip() or start_time,
            end_time=str(handover_result.get("end_time", "") or "").strip() or end_time,
            duty_date=str(handover_result.get("duty_date", "") or "").strip() or duty_date,
            duty_shift=str(handover_result.get("duty_shift", "") or "").strip() or duty_shift,
            switch_network=False,
            reuse_cached=reuse_cached,
            emit_log=emit_log,
        )
        return {
            "start_time": str(handover_result.get("start_time", "") or "").strip(),
            "end_time": str(handover_result.get("end_time", "") or "").strip(),
            "duty_date": str(handover_result.get("duty_date", "") or "").strip(),
            "duty_shift": str(handover_result.get("duty_shift", "") or "").strip(),
            "handover": handover_result,
            "capacity": capacity_result,
        }

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

    def _ensure_target_network_ready(self, target_side: str, emit_log: Callable[[str], None]) -> None:
        side = str(target_side or "").strip().lower()
        if side not in {"internal", "external"}:
            raise ValueError(f"unsupported target_side: {target_side}")
        emit_log(f"[交接班下载] 网络切换功能已移除，按当前网络继续执行{side}阶段")

    def ensure_internal_ready(self, emit_log: Callable[[str], None] = print) -> None:
        self._ensure_target_network_ready("internal", emit_log)

    def ensure_external_ready(self, emit_log: Callable[[str], None] = print) -> None:
        self._ensure_target_network_ready("external", emit_log)

    def _maybe_switch_internal(self, emit_log: Callable[[str], None]) -> None:
        self.did_switch_internal_this_run = False
        emit_log("[交接班下载] 网络切换功能已移除，下载前不再切换到内网")

    def switch_external_after_download(self, emit_log: Callable[[str], None]) -> None:
        emit_log("[交接班下载] 网络切换功能已移除，下载后不再切回外网")

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
