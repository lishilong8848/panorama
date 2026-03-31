from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

from PySide6.QtCore import QDate, QObject, Qt, QTimer, Signal
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QDateEdit,
    QLabel,
    QLineEdit,
    QListWidget,
    QListView,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from pipeline_utils import (
    build_event_text,
    get_app_dir,
    load_calc_module,
    load_download_module,
    load_pipeline_config,
    resolve_config_path,
    send_feishu_webhook,
)
from wifi_switcher import WifiSwitcher


def _deepcopy_cfg(data: Dict[str, Any]) -> Dict[str, Any]:
    return json.loads(json.dumps(data, ensure_ascii=False))


def _valid_time(value: str) -> bool:
    return bool(re.fullmatch(r"\d{2}:\d{2}:\d{2}", value.strip()))


def _get_nested_value(data: Dict[str, Any], path: str, default: Any = None) -> Any:
    current: Any = data
    for key in path.split("."):
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _set_nested_value(data: Dict[str, Any], path: str, value: Any) -> None:
    current: Dict[str, Any] = data
    keys = path.split(".")
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[keys[-1]] = value


def _ensure_config_defaults(cfg: Dict[str, Any]) -> Dict[str, Any]:
    download_cfg = cfg.get("download")
    if isinstance(download_cfg, dict):
        multi_cfg = download_cfg.get("multi_date")
        if not isinstance(multi_cfg, dict):
            multi_cfg = {}
            download_cfg["multi_date"] = multi_cfg
        if "max_dates_per_run" not in multi_cfg:
            multi_cfg["max_dates_per_run"] = 31
    return cfg


def _normalize_sheet_rules_config(raw_rules: Any) -> list[dict[str, Any]]:
    if isinstance(raw_rules, dict):
        normalized_input: list[Any] = []
        for sheet_name, rule in raw_rules.items():
            if isinstance(rule, dict):
                normalized_input.append(
                    {
                        "sheet_name": str(sheet_name).strip(),
                        "table_id": rule.get("table_id", ""),
                        "header_row": rule.get("header_row", 1),
                    }
                )
            elif isinstance(rule, str):
                parts = [x.strip() for x in rule.split("|")]
                if not parts or not parts[0]:
                    raise ValueError(f"feishu_sheet_import.sheet_rules[{sheet_name}] 字符串格式错误，应为 table_id|header_row")
                normalized_input.append(
                    {
                        "sheet_name": str(sheet_name).strip(),
                        "table_id": parts[0],
                        "header_row": parts[1] if len(parts) >= 2 and parts[1] else 1,
                    }
                )
            else:
                raise ValueError(f"feishu_sheet_import.sheet_rules[{sheet_name}] 必须是对象或字符串")
    elif isinstance(raw_rules, list):
        normalized_input = list(raw_rules)
    else:
        raise ValueError("feishu_sheet_import.sheet_rules 必须是数组或对象")

    if not normalized_input:
        raise ValueError("feishu_sheet_import.sheet_rules 不能为空")

    rules: list[dict[str, Any]] = []
    seen_sheet: set[str] = set()
    for idx, item in enumerate(normalized_input, 1):
        if isinstance(item, dict):
            sheet_name = str(item.get("sheet_name", "")).strip()
            table_id = str(item.get("table_id", "")).strip()
            header_row_raw = item.get("header_row", 1)
        elif isinstance(item, str):
            parts = [x.strip() for x in item.split("|")]
            if len(parts) != 3:
                raise ValueError(f"feishu_sheet_import.sheet_rules 第{idx}项格式错误，应为 sheet_name|table_id|header_row")
            sheet_name, table_id, header_row_raw = parts
        else:
            raise ValueError(f"feishu_sheet_import.sheet_rules 第{idx}项必须是对象或字符串")

        if not sheet_name:
            raise ValueError(f"feishu_sheet_import.sheet_rules 第{idx}项 sheet_name 不能为空")
        if not table_id:
            raise ValueError(f"feishu_sheet_import.sheet_rules 第{idx}项 table_id 不能为空")
        try:
            header_row = int(header_row_raw)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"feishu_sheet_import.sheet_rules 第{idx}项 header_row 必须是整数") from exc
        if header_row < 1:
            raise ValueError(f"feishu_sheet_import.sheet_rules 第{idx}项 header_row 必须大于等于1")

        sheet_key = sheet_name.casefold()
        if sheet_key in seen_sheet:
            raise ValueError(f"feishu_sheet_import.sheet_rules 存在重复 sheet_name: {sheet_name}")
        seen_sheet.add(sheet_key)

        rules.append(
            {
                "sheet_name": sheet_name,
                "table_id": table_id,
                "header_row": header_row,
            }
        )
    return rules


class _LineEmitter:
    def __init__(self, emit_line: Callable[[str], None]) -> None:
        self.emit_line = emit_line
        self._buffer = ""

    def write(self, text: str) -> int:
        if not text:
            return 0
        self._buffer += text.replace("\r\n", "\n").replace("\r", "\n")
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if line:
                self.emit_line(line)
        return len(text)

    def flush(self) -> None:
        last = self._buffer.strip()
        if last:
            self.emit_line(last)
        self._buffer = ""


class UiBus(QObject):
    log = Signal(str)
    info = Signal(str, str)
    error = Signal(str, str)
    task_state = Signal(bool)


def _send_failure_webhook(
    config: Dict[str, Any],
    stage: str,
    detail: str,
    building: str | None = None,
    emit_log: Callable[[str], None] | None = None,
) -> None:
    notify_cfg = config["notify"]
    if not bool(notify_cfg["enable_webhook"]):
        return

    webhook_url = str(notify_cfg["feishu_webhook_url"]).strip()
    keyword = str(notify_cfg["keyword"]).strip()
    timeout = int(notify_cfg["timeout"])
    if not webhook_url:
        return

    network_cfg = config["network"]
    external_ssid = str(network_cfg["external_ssid"]).strip()
    if external_ssid:
        wifi = WifiSwitcher(
            timeout_sec=int(network_cfg["switch_timeout_sec"]),
            retry_count=int(network_cfg["retry_count"]),
            retry_interval_sec=int(network_cfg["retry_interval_sec"]),
        )
        current = wifi.get_current_ssid()
        if current != external_ssid:
            ok, msg = wifi.connect(
                external_ssid,
                require_saved_profile=bool(network_cfg["require_saved_profiles"]),
            )
            if not ok:
                if emit_log:
                    emit_log(f"[Webhook] 切换外网失败，本次不发送: {msg}")
                return
            if emit_log:
                emit_log(f"[Webhook] 为发送告警已切换外网: {msg}")

    text = build_event_text(stage=stage, detail=detail, building=building)
    ok, msg = send_feishu_webhook(webhook_url, text, keyword=keyword, timeout=timeout)
    if emit_log:
        emit_log(f"[Webhook] {'发送成功' if ok else '发送失败'}: {msg}")


class DailyAutoScheduler:
    def __init__(
        self,
        config: Dict[str, Any],
        emit_log: Callable[[str], None],
        run_callback: Callable[[str], Tuple[bool, str]],
        is_busy: Callable[[], bool],
    ) -> None:
        if "scheduler" not in config or not isinstance(config["scheduler"], dict):
            raise ValueError("配置错误: scheduler 缺失或格式错误")
        raw = config["scheduler"]

        required = [
            "enabled",
            "auto_start_in_gui",
            "run_time",
            "check_interval_sec",
            "catch_up_if_missed",
            "retry_failed_in_same_period",
            "state_file",
        ]
        missing = [k for k in required if k not in raw]
        if missing:
            raise ValueError(f"配置错误: scheduler 缺少字段 {missing}")

        run_time = str(raw["run_time"]).strip()
        if not _valid_time(run_time):
            raise ValueError("配置错误: scheduler.run_time 必须是 HH:MM:SS")
        check_interval_sec = int(raw["check_interval_sec"])
        if check_interval_sec <= 0:
            raise ValueError("配置错误: scheduler.check_interval_sec 必须大于0")
        state_file = str(raw["state_file"]).strip()
        if not state_file:
            raise ValueError("配置错误: scheduler.state_file 不能为空")

        self.cfg = {
            "enabled": bool(raw["enabled"]),
            "auto_start_in_gui": bool(raw["auto_start_in_gui"]),
            "run_time": run_time,
            "check_interval_sec": check_interval_sec,
            "catch_up_if_missed": bool(raw["catch_up_if_missed"]),
            "retry_failed_in_same_period": bool(raw["retry_failed_in_same_period"]),
            "state_file": state_file,
        }

        self.emit_log = emit_log
        self.run_callback = run_callback
        self.is_busy = is_busy
        self.enabled = bool(self.cfg["enabled"])
        self.auto_start_in_gui = bool(self.cfg["auto_start_in_gui"])

        self.started_at = datetime.now()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        self.state_path = self._resolve_state_path(str(self.cfg["state_file"]))
        self.state = self._load_state()

    @staticmethod
    def _resolve_state_path(state_file: str) -> Path:
        p = Path(state_file)
        if p.is_absolute():
            return p
        local = os.getenv("LOCALAPPDATA", "").strip()
        root = Path(local) / "monthly_report_gui" if local else get_app_dir()
        root.mkdir(parents=True, exist_ok=True)
        return root / p

    def _load_state(self) -> Dict[str, str]:
        default = {
            "last_success_period": "",
            "last_attempt_period": "",
            "last_run_at": "",
            "last_status": "",
            "last_error": "",
            "retry_done_period": "",
        }
        if not self.state_path.exists():
            return default
        try:
            obj = json.loads(self.state_path.read_text(encoding="utf-8"))
            if not isinstance(obj, dict):
                return default
            out = dict(default)
            for k in out:
                out[k] = str(obj.get(k, "") or "")
            return out
        except Exception:
            return default

    def _save_state(self) -> None:
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            self.emit_log(f"[调度] 保存状态失败: {exc}")

    def _schedule_for_day(self, day: datetime) -> datetime:
        h, m, s = [int(x) for x in self.cfg["run_time"].split(":")]
        h, m, s = max(0, min(h, 23)), max(0, min(m, 59)), max(0, min(s, 59))
        return datetime(day.year, day.month, day.day, h, m, s)

    def _period(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    def _should_trigger(self, now: datetime) -> tuple[bool, str]:
        if not self.enabled:
            return False, ""
        period = self._period(now)
        if self.state.get("last_success_period", "") == period:
            return False, period

        scheduled = self._schedule_for_day(now)
        if now < scheduled:
            return False, period

        if (
            not bool(self.cfg["catch_up_if_missed"])
            and self.started_at > scheduled
            and self.state.get("last_attempt_period", "") != period
        ):
            return False, period

        if self.state.get("last_attempt_period", "") != period:
            return True, period

        if not bool(self.cfg["retry_failed_in_same_period"]):
            return False, period
        if self.state.get("last_status", "") != "failed":
            return False, period
        if self.state.get("retry_done_period", "") == period:
            return False, period
        return True, period

    def next_run_time(self, now: datetime | None = None) -> datetime:
        now = now or datetime.now()
        scheduled = self._schedule_for_day(now)
        if now < scheduled:
            return scheduled
        should_run, _ = self._should_trigger(now)
        if should_run:
            return now
        return self._schedule_for_day(now + timedelta(days=1))

    def next_run_text(self) -> str:
        return self.next_run_time().strftime("%Y-%m-%d %H:%M:%S")

    def status_text(self) -> str:
        if not self.enabled:
            return "已禁用"
        return "运行中" if self.is_running() else "未启动"

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if not self.enabled:
            self.emit_log("[调度] scheduler.enabled=false，不启动")
            return
        if self.is_running():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="daily-auto-scheduler")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    def _loop(self) -> None:
        self.emit_log(f"[调度] 已启动，每天 {self.cfg['run_time']} 自动执行")
        interval = int(self.cfg["check_interval_sec"])
        last_next = ""
        while not self._stop.is_set():
            now = datetime.now()
            next_text = self.next_run_text()
            if next_text != last_next:
                self.emit_log(f"[调度] 下次执行: {next_text}")
                last_next = next_text

            should_run, period = self._should_trigger(now)
            if should_run:
                if self.is_busy():
                    self.emit_log("[调度] 当前有任务运行，稍后重试")
                else:
                    is_retry = (
                        self.state.get("last_attempt_period", "") == period
                        and self.state.get("last_status", "") == "failed"
                    )
                    source = "内置每日调度补跑" if is_retry else "内置每日调度"
                    ok, detail = self.run_callback(source)
                    self.state["last_attempt_period"] = period
                    self.state["last_run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.state["last_status"] = "success" if ok else "failed"
                    self.state["last_error"] = "" if ok else detail
                    if ok:
                        self.state["last_success_period"] = period
                    if is_retry:
                        self.state["retry_done_period"] = period
                    self._save_state()
                    self.emit_log(f"[调度] {'成功' if ok else '失败'}: {'ok' if ok else detail}")
            self._stop.wait(interval)


class ConfigEditorDialog(QDialog):
    def __init__(self, config: Dict[str, Any], config_path: Path, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("configDialog")
        self.setWindowTitle("配置设置")
        self.resize(980, 760)
        self.config_path = config_path
        self._config = _ensure_config_defaults(_deepcopy_cfg(config))

        self.fields: Dict[str, tuple[str, QWidget]] = {}
        self.site_rows: list[dict[str, QWidget]] = []

        root = QVBoxLayout(self)
        root.setContentsMargins(14, 14, 14, 14)
        root.setSpacing(10)

        self.lbl_config_path = QLabel(f"配置文件: {self.config_path}")
        self.lbl_config_path.setObjectName("mutedNote")
        self.lbl_config_hint = QLabel("说明：仅通过中文表单编辑配置，保存后会回写到 JSON 文件。")
        self.lbl_config_hint.setObjectName("mutedNote")
        root.addWidget(self.lbl_config_path)
        root.addWidget(self.lbl_config_hint)

        self.tabs = QTabWidget()
        self.tabs.setObjectName("configTabs")
        root.addWidget(self.tabs, 1)

        self.basic_tab = QWidget()
        self.basic_tab.setObjectName("configBasicTab")
        self.tabs.addTab(self.basic_tab, "基础设置（中文）")

        self._build_basic_tab()
        self._load_form_from_config()

        row = QHBoxLayout()
        self.btn_reload = QPushButton("从文件重载")
        self.btn_save_form = QPushButton("保存基础设置")
        self.btn_close = QPushButton("关闭")
        self.btn_reload.setProperty("kind", "secondary")
        self.btn_save_form.setProperty("kind", "primary")
        self.btn_close.setProperty("kind", "ghost")
        row.addWidget(self.btn_reload)
        row.addWidget(self.btn_save_form)
        row.addStretch(1)
        row.addWidget(self.btn_close)
        root.addLayout(row)

        self.btn_reload.clicked.connect(self._reload_from_file)
        self.btn_save_form.clicked.connect(self._save_from_form)
        self.btn_close.clicked.connect(self.accept)
        self._apply_dialog_style()

    def _build_basic_tab(self) -> None:
        outer = QVBoxLayout(self.basic_tab)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)
        scroll = QScrollArea()
        scroll.setObjectName("configScroll")
        scroll.setWidgetResizable(True)
        scroll.viewport().setObjectName("configScrollViewport")
        outer.addWidget(scroll)

        container = QWidget()
        container.setObjectName("configScrollContent")
        scroll.setWidget(container)
        layout = QGridLayout(container)
        layout.setContentsMargins(4, 4, 4, 4)
        layout.setHorizontalSpacing(12)
        layout.setVerticalSpacing(12)

        sec_input = QGroupBox("输入配置")
        sec_output = QGroupBox("输出配置")
        sec_download = QGroupBox("下载配置")
        sec_sites = QGroupBox("站点配置（download.sites）")
        sec_network = QGroupBox("网络配置")
        sec_notify = QGroupBox("告警配置")
        sec_gui = QGroupBox("GUI开关")
        sec_scheduler = QGroupBox("调度配置")
        sec_feishu = QGroupBox("飞书配置")
        sec_sheet_import = QGroupBox("5Sheet导表配置")

        layout.addWidget(sec_input, 0, 0)
        layout.addWidget(sec_output, 0, 1)
        layout.addWidget(sec_download, 1, 0)
        layout.addWidget(sec_network, 1, 1)
        layout.addWidget(sec_sites, 2, 0, 1, 2)
        layout.addWidget(sec_notify, 3, 0)
        layout.addWidget(sec_feishu, 3, 1)
        layout.addWidget(sec_gui, 4, 0)
        layout.addWidget(sec_scheduler, 4, 1)
        layout.addWidget(sec_sheet_import, 5, 0, 1, 2)
        layout.setColumnStretch(0, 1)
        layout.setColumnStretch(1, 1)
        layout.setRowStretch(6, 1)

        input_form = QFormLayout(sec_input)
        output_form = QFormLayout(sec_output)
        download_form = QFormLayout(sec_download)
        network_form = QFormLayout(sec_network)
        notify_form = QFormLayout(sec_notify)
        gui_form = QFormLayout(sec_gui)
        scheduler_form = QFormLayout(sec_scheduler)
        feishu_form = QFormLayout(sec_feishu)
        sheet_import_form = QFormLayout(sec_sheet_import)

        self._add_line(input_form, "Excel目录", "input.excel_dir", r"例如：D:\QLDownload")
        self._add_line(input_form, "楼栋列表（逗号分隔）", "input.buildings", "例如：A楼,B楼,C楼,D楼,E楼", value_kind="list_csv")
        self._add_line(input_form, "文件匹配模板", "input.file_glob_template", "例如：{building}_*.xlsx")

        self._add_bool(output_form, "保存计算结果JSON", "output.save_json")
        self._add_line(output_form, "结果输出目录", "output.json_dir", r"例如：D:\QLDownload\全景月报计算结果")

        self._add_line(download_form, "下载目录", "download.save_dir", r"例如：D:\QLDownload")
        self._add_choice(
            download_form,
            "每次运行子目录模式",
            "download.run_subdir_mode",
            [
                ("timestamp（推荐）", "timestamp"),
                ("none（不创建子目录）", "none"),
            ],
        )
        self._add_line(download_form, "运行子目录前缀", "download.run_subdir_prefix", "例如：run_")
        self._add_choice(
            download_form,
            "时间区间模式",
            "download.time_range_mode",
            [
                ("昨天00:00:00 到今天00:00:00", "yesterday_to_today_start"),
                ("上月1号到本月1号", "last_month_to_this_month_start"),
                ("自定义", "custom"),
            ],
        )
        self._add_line(download_form, "自定义开始时间", "download.start_time", "YYYY-MM-DD HH:MM:SS")
        self._add_line(download_form, "自定义结束时间", "download.end_time", "YYYY-MM-DD HH:MM:SS")
        self._add_int(download_form, "下载重试次数", "download.max_retries", 0, 100)
        self._add_int(download_form, "下载重试等待(秒)", "download.retry_wait_sec", 0, 3600)
        self._add_int(download_form, "站点启动间隔(秒)", "download.site_start_delay_sec", 0, 3600)
        self._add_int(download_form, "多日期单次最大天数", "download.multi_date.max_dates_per_run", 1, 365)
        self._add_bool(download_form, "仅处理本次下载文件", "download.only_process_downloaded_this_run")
        self._add_bool(download_form, "浏览器无头模式", "download.browser_headless")
        self._add_line(download_form, "浏览器通道", "download.browser_channel", "例如：chrome")
        self._add_line(download_form, "Playwright浏览器目录", "download.playwright_browsers_path", "可留空")

        self._build_sites_editor(sec_sites)

        self._add_line(network_form, "内网SSID", "network.internal_ssid", "例如：e-donghuan")
        self._add_line(network_form, "外网SSID", "network.external_ssid", "例如：EL-BG")
        self._add_int(network_form, "切换超时(秒)", "network.switch_timeout_sec", 1, 600)
        self._add_int(network_form, "切网重试次数", "network.retry_count", 0, 100)
        self._add_int(network_form, "切网重试间隔(秒)", "network.retry_interval_sec", 0, 3600)
        self._add_bool(network_form, "要求已保存WiFi配置", "network.require_saved_profiles")
        self._add_bool(network_form, "流程后切回原网络", "network.switch_back_to_original")

        self._add_bool(notify_form, "启用Webhook告警", "notify.enable_webhook")
        self._add_line(notify_form, "Webhook地址", "notify.feishu_webhook_url", "https://open.feishu.cn/open-apis/bot/v2/hook/...")
        self._add_line(notify_form, "关键词", "notify.keyword", "事件")
        self._add_int(notify_form, "Webhook超时(秒)", "notify.timeout", 1, 600)
        self._add_bool(notify_form, "下载失败告警", "notify.on_download_failure")
        self._add_bool(notify_form, "切网失败告警", "notify.on_wifi_failure")
        self._add_bool(notify_form, "上传失败告警", "notify.on_upload_failure")

        self._add_bool(gui_form, "启用GUI入口", "manual_upload_gui.enabled")

        self._add_bool(scheduler_form, "启用调度", "scheduler.enabled")
        self._add_bool(scheduler_form, "GUI启动时自动启动调度", "scheduler.auto_start_in_gui")
        self._add_line(scheduler_form, "调度时间", "scheduler.run_time", "HH:MM:SS")
        self._add_int(scheduler_form, "检查间隔(秒)", "scheduler.check_interval_sec", 5, 3600)
        self._add_bool(scheduler_form, "允许补跑错过任务", "scheduler.catch_up_if_missed")
        self._add_bool(scheduler_form, "同周期失败后重试", "scheduler.retry_failed_in_same_period")
        self._add_line(scheduler_form, "调度状态文件", "scheduler.state_file", "例如：daily_scheduler_state.json")

        self._add_bool(feishu_form, "启用飞书上传", "feishu.enable_upload")
        self._add_line(feishu_form, "App ID", "feishu.app_id")
        self._add_line(feishu_form, "App Secret", "feishu.app_secret")
        self._add_line(feishu_form, "App Token", "feishu.app_token")
        self._add_line(feishu_form, "数据表ID", "feishu.calc_table_id")
        self._add_line(feishu_form, "附件表ID", "feishu.attachment_table_id")
        self._add_line(feishu_form, "报表类型", "feishu.report_type")
        self._add_bool(feishu_form, "跳过0值记录", "feishu.skip_zero_records")
        self._add_choice(feishu_form, "日期字段模式", "feishu.date_field_mode", [("timestamp", "timestamp"), ("text", "text")])
        self._add_int(feishu_form, "日期字段默认日", "feishu.date_field_day", 1, 31)
        self._add_int(feishu_form, "时区偏移小时", "feishu.date_tz_offset_hours", -12, 14)
        self._add_int(feishu_form, "飞书接口超时(秒)", "feishu.timeout", 1, 600)
        self._add_int(feishu_form, "飞书请求重试次数", "feishu.request_retry_count", 0, 20)
        self._add_int(feishu_form, "飞书请求重试间隔(秒)", "feishu.request_retry_interval_sec", 0, 60)

        self._add_bool(sheet_import_form, "启用5Sheet导表", "feishu_sheet_import.enabled")
        self._add_line(sheet_import_form, "导表App Token", "feishu_sheet_import.app_token")
        self._add_bool(sheet_import_form, "上传前先清空目标表", "feishu_sheet_import.clear_before_upload")
        self._add_bool(sheet_import_form, "单Sheet失败后继续", "feishu_sheet_import.continue_on_sheet_error")
        self._add_int(sheet_import_form, "导表接口超时(秒)", "feishu_sheet_import.timeout", 1, 600)
        self._add_int(sheet_import_form, "导表请求重试次数", "feishu_sheet_import.request_retry_count", 0, 20)
        self._add_int(sheet_import_form, "导表请求重试间隔(秒)", "feishu_sheet_import.request_retry_interval_sec", 0, 60)
        self._add_int(sheet_import_form, "读取分页大小", "feishu_sheet_import.list_page_size", 1, 500)
        self._add_int(sheet_import_form, "删除批量大小", "feishu_sheet_import.delete_batch_size", 1, 500)
        self._add_int(sheet_import_form, "写入批量大小", "feishu_sheet_import.create_batch_size", 1, 500)
        self._add_sheet_rules(
            sheet_import_form,
            "Sheet映射规则",
            "feishu_sheet_import.sheet_rules",
            "每行格式：Sheet名|table_id|header_row\n"
            "例：0.重点推动|tblpQHfmC556F0rV|1\n"
            "保存后会自动转换为按Sheet名分组的映射格式",
        )

    def _build_sites_editor(self, parent: QWidget) -> None:
        layout = QVBoxLayout(parent)
        layout.setSpacing(8)
        header = QGridLayout()
        header.addWidget(QLabel("楼栋"), 0, 0)
        header.addWidget(QLabel("启用"), 0, 1)
        header.addWidget(QLabel("URL"), 0, 2)
        header.addWidget(QLabel("用户名"), 0, 3)
        header.addWidget(QLabel("密码"), 0, 4)
        layout.addLayout(header)

        self.sites_container = QWidget()
        self.sites_grid = QGridLayout(self.sites_container)
        self.sites_grid.setContentsMargins(0, 0, 0, 0)
        self.sites_grid.setHorizontalSpacing(8)
        self.sites_grid.setVerticalSpacing(6)
        layout.addWidget(self.sites_container)

        btn_row = QHBoxLayout()
        self.btn_add_site = QPushButton("新增站点")
        self.btn_add_site.setProperty("kind", "secondary")
        btn_row.addWidget(self.btn_add_site)
        btn_row.addStretch(1)
        layout.addLayout(btn_row)
        self.btn_add_site.clicked.connect(self._add_site_row)

    def _add_line(self, form: QFormLayout, label: str, key: str, placeholder: str = "", value_kind: str = "str") -> None:
        edit = QLineEdit()
        edit.setPlaceholderText(placeholder)
        edit.setMinimumHeight(32)
        form.addRow(label, edit)
        self.fields[key] = (value_kind, edit)

    def _add_bool(self, form: QFormLayout, label: str, key: str) -> None:
        box = QCheckBox("开启")
        form.addRow(label, box)
        self.fields[key] = ("bool", box)

    def _add_int(self, form: QFormLayout, label: str, key: str, min_value: int, max_value: int) -> None:
        spin = QSpinBox()
        spin.setRange(min_value, max_value)
        spin.setMinimumHeight(32)
        form.addRow(label, spin)
        self.fields[key] = ("int", spin)

    def _add_choice(self, form: QFormLayout, label: str, key: str, options: list[tuple[str, str]]) -> None:
        combo = QComboBox()
        for text, value in options:
            combo.addItem(text, value)
        combo.setMinimumHeight(32)
        form.addRow(label, combo)
        self.fields[key] = ("choice", combo)

    def _add_sheet_rules(self, form: QFormLayout, label: str, key: str, placeholder: str) -> None:
        editor = QPlainTextEdit()
        editor.setPlaceholderText(placeholder)
        editor.setMinimumHeight(120)
        form.addRow(label, editor)
        self.fields[key] = ("sheet_rules", editor)

    @staticmethod
    def _sheet_rules_to_text(value: Any) -> str:
        try:
            rules = _normalize_sheet_rules_config(value)
        except Exception:
            return ""
        lines: list[str] = []
        for item in rules:
            sheet_name = str(item.get("sheet_name", "")).strip()
            table_id = str(item.get("table_id", "")).strip()
            header_row = item.get("header_row", "")
            if not sheet_name and not table_id and header_row == "":
                continue
            lines.append(f"{sheet_name}|{table_id}|{header_row}")
        return "\n".join(lines)

    @staticmethod
    def _text_to_sheet_rules(text: str) -> dict[str, dict[str, Any]]:
        raw_lines = [line.strip() for line in text.replace("\r\n", "\n").split("\n") if line.strip()]
        if not raw_lines:
            raise ValueError("feishu_sheet_import.sheet_rules 不能为空")

        rules: dict[str, dict[str, Any]] = {}
        for idx, line in enumerate(raw_lines, 1):
            parts: list[str]
            if "|" in line:
                parts = [p.strip() for p in line.split("|")]
            elif "\t" in line:
                parts = [p.strip() for p in line.split("\t")]
            elif "," in line:
                parts = [p.strip() for p in line.split(",")]
            else:
                raise ValueError(f"sheet_rules 第{idx}行格式错误，应为 Sheet名|table_id|header_row")

            if len(parts) != 3:
                raise ValueError(f"sheet_rules 第{idx}行字段数量错误，应为3段")

            sheet_name = parts[0]
            table_id = parts[1]
            header_row_text = parts[2]
            if not sheet_name:
                raise ValueError(f"sheet_rules 第{idx}行 sheet_name 不能为空")
            if not table_id:
                raise ValueError(f"sheet_rules 第{idx}行 table_id 不能为空")
            try:
                header_row = int(header_row_text)
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"sheet_rules 第{idx}行 header_row 不是整数: {header_row_text}") from exc
            if header_row < 1:
                raise ValueError(f"sheet_rules 第{idx}行 header_row 必须大于等于1")
            if sheet_name in rules:
                raise ValueError(f"sheet_rules 第{idx}行 sheet_name 重复: {sheet_name}")
            rules[sheet_name] = {
                "table_id": table_id,
                "header_row": header_row,
            }
        return rules

    def _clear_site_rows(self) -> None:
        for row in self.site_rows:
            for widget in row.values():
                widget.deleteLater()
        self.site_rows.clear()

    def _add_site_row(self, site: Dict[str, Any] | None = None) -> None:
        site = site or {
            "building": "",
            "enabled": True,
            "url": "",
            "username": "",
            "password": "",
        }
        row_index = len(self.site_rows)
        building = QLineEdit(str(site.get("building", "")))
        enabled = QCheckBox()
        enabled.setChecked(bool(site.get("enabled", True)))
        url = QLineEdit(str(site.get("url", "")))
        username = QLineEdit(str(site.get("username", "")))
        password = QLineEdit(str(site.get("password", "")))
        password.setEchoMode(QLineEdit.Password)
        building.setPlaceholderText("如：A楼")
        url.setPlaceholderText("如：http://192.168.x.x/...")
        username.setPlaceholderText("登录账号")
        password.setPlaceholderText("登录密码")

        self.sites_grid.addWidget(building, row_index, 0)
        self.sites_grid.addWidget(enabled, row_index, 1)
        self.sites_grid.addWidget(url, row_index, 2)
        self.sites_grid.addWidget(username, row_index, 3)
        self.sites_grid.addWidget(password, row_index, 4)

        self.site_rows.append(
            {
                "building": building,
                "enabled": enabled,
                "url": url,
                "username": username,
                "password": password,
            }
        )

    def _load_form_from_config(self) -> None:
        for key, (kind, widget) in self.fields.items():
            value = _get_nested_value(self._config, key)
            if kind == "bool":
                widget.setChecked(bool(value))  # type: ignore[attr-defined]
            elif kind == "int":
                try:
                    widget.setValue(int(value))  # type: ignore[attr-defined]
                except Exception:
                    widget.setValue(0)  # type: ignore[attr-defined]
            elif kind == "choice":
                idx = widget.findData(value)  # type: ignore[attr-defined]
                widget.setCurrentIndex(idx if idx >= 0 else 0)  # type: ignore[attr-defined]
            elif kind == "list_csv":
                text = ",".join([str(item).strip() for item in (value or []) if str(item).strip()])
                widget.setText(text)  # type: ignore[attr-defined]
            elif kind == "sheet_rules":
                widget.setPlainText(self._sheet_rules_to_text(value))  # type: ignore[attr-defined]
            else:
                widget.setText("" if value is None else str(value))  # type: ignore[attr-defined]

        self._clear_site_rows()
        sites = _get_nested_value(self._config, "download.sites", [])
        if isinstance(sites, list):
            for site in sites:
                if isinstance(site, dict):
                    self._add_site_row(site)
        if not self.site_rows:
            self._add_site_row()

    def _collect_sites(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for row in self.site_rows:
            building = row["building"].text().strip()  # type: ignore[attr-defined]
            if not building:
                continue
            result.append(
                {
                    "building": building,
                    "enabled": bool(row["enabled"].isChecked()),  # type: ignore[attr-defined]
                    "url": row["url"].text().strip(),  # type: ignore[attr-defined]
                    "username": row["username"].text().strip(),  # type: ignore[attr-defined]
                    "password": row["password"].text().strip(),  # type: ignore[attr-defined]
                }
            )
        return result

    def _collect_form_config(self) -> Dict[str, Any]:
        merged = _deepcopy_cfg(self._config)
        for key, (kind, widget) in self.fields.items():
            if kind == "bool":
                value = bool(widget.isChecked())  # type: ignore[attr-defined]
            elif kind == "int":
                value = int(widget.value())  # type: ignore[attr-defined]
            elif kind == "choice":
                value = widget.currentData()  # type: ignore[attr-defined]
            elif kind == "list_csv":
                raw = widget.text().strip()  # type: ignore[attr-defined]
                values = [item.strip() for item in raw.replace("，", ",").split(",") if item.strip()]
                value = values
            elif kind == "sheet_rules":
                raw = widget.toPlainText().strip()  # type: ignore[attr-defined]
                value = self._text_to_sheet_rules(raw)
            else:
                value = widget.text().strip()  # type: ignore[attr-defined]
            _set_nested_value(merged, key, value)

        _set_nested_value(merged, "download.sites", self._collect_sites())
        # URL 字段不再使用，保存时自动移除，避免配置歧义。
        feishu_cfg = merged.get("feishu")
        if isinstance(feishu_cfg, dict):
            feishu_cfg.pop("calc_table_url", None)
            feishu_cfg.pop("attachment_table_url", None)
        return merged

    def _validate_form_config(self, cfg: Dict[str, Any]) -> list[str]:
        errors: list[str] = []

        run_time = str(_get_nested_value(cfg, "scheduler.run_time", "")).strip()
        if not _valid_time(run_time):
            errors.append("scheduler.run_time 格式错误，应为 HH:MM:SS")

        mode = str(_get_nested_value(cfg, "download.time_range_mode", "")).strip()
        if mode not in {"yesterday_to_today_start", "last_month_to_this_month_start", "custom"}:
            errors.append("download.time_range_mode 取值无效")
        if mode == "custom":
            fmt = "%Y-%m-%d %H:%M:%S"
            start_time = str(_get_nested_value(cfg, "download.start_time", "")).strip()
            end_time = str(_get_nested_value(cfg, "download.end_time", "")).strip()
            start_dt: datetime | None = None
            end_dt: datetime | None = None
            try:
                start_dt = datetime.strptime(start_time, fmt)
            except Exception:
                errors.append(f"download.start_time 格式错误，应为 {fmt}")
            try:
                end_dt = datetime.strptime(end_time, fmt)
            except Exception:
                errors.append(f"download.end_time 格式错误，应为 {fmt}")
            if start_dt and end_dt and start_dt >= end_dt:
                errors.append("自定义时间区间错误：开始时间必须早于结束时间")
            now = datetime.now()
            if start_dt and start_dt > now:
                errors.append("自定义时间区间错误：开始时间不能超过当前时间")
            if end_dt and end_dt > now:
                errors.append("自定义时间区间错误：结束时间不能超过当前时间")

        try:
            max_dates = int(_get_nested_value(cfg, "download.multi_date.max_dates_per_run", 31))
            if max_dates <= 0:
                errors.append("download.multi_date.max_dates_per_run 必须大于0")
        except Exception:
            errors.append("download.multi_date.max_dates_per_run 必须是整数")

        buildings = _get_nested_value(cfg, "input.buildings", [])
        if not isinstance(buildings, list) or not buildings:
            errors.append("input.buildings 不能为空")

        sites = _get_nested_value(cfg, "download.sites", [])
        if not isinstance(sites, list) or not sites:
            errors.append("download.sites 不能为空")
        else:
            for site in sites:
                if not isinstance(site, dict):
                    errors.append("download.sites 项必须是对象")
                    continue
                required_site_keys = ["building", "enabled", "url", "username", "password"]
                missing_site_keys = [k for k in required_site_keys if k not in site]
                if missing_site_keys:
                    errors.append(f"download.sites 项缺少字段: {missing_site_keys}")
                    continue

                building = str(site["building"]).strip()
                if not building:
                    errors.append("download.sites 存在空 building")
                if bool(site["enabled"]):
                    if not str(site["url"]).strip():
                        errors.append(f"{building or '(未命名楼栋)'} 已启用但 url 为空")
                    if not str(site["username"]).strip():
                        errors.append(f"{building or '(未命名楼栋)'} 已启用但 username 为空")
                    if not str(site["password"]).strip():
                        errors.append(f"{building or '(未命名楼栋)'} 已启用但 password 为空")

        notify_keyword = str(_get_nested_value(cfg, "notify.keyword", "")).strip()
        if not notify_keyword:
            errors.append("notify.keyword 不能为空")

        for key in ("feishu.request_retry_count", "feishu.request_retry_interval_sec"):
            try:
                value = int(_get_nested_value(cfg, key, 0))
                if value < 0:
                    errors.append(f"{key} 必须大于等于0")
            except Exception:
                errors.append(f"{key} 必须是整数")

        sheet_cfg = _get_nested_value(cfg, "feishu_sheet_import", {})
        if not isinstance(sheet_cfg, dict):
            errors.append("feishu_sheet_import 配置缺失或格式错误")
            return errors
        if not str(sheet_cfg.get("app_token", "")).strip():
            errors.append("feishu_sheet_import.app_token 不能为空")
        for key in (
            "timeout",
            "list_page_size",
            "delete_batch_size",
            "create_batch_size",
        ):
            try:
                value = int(sheet_cfg.get(key))
                if value <= 0:
                    errors.append(f"feishu_sheet_import.{key} 必须大于0")
            except Exception:
                errors.append(f"feishu_sheet_import.{key} 必须是整数")
        for key in ("request_retry_count", "request_retry_interval_sec"):
            try:
                value = int(sheet_cfg.get(key, 0))
                if value < 0:
                    errors.append(f"feishu_sheet_import.{key} 必须大于等于0")
            except Exception:
                pass

        rules = sheet_cfg.get("sheet_rules")
        try:
            _normalize_sheet_rules_config(rules)
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))

        return errors

    def _persist(self, parsed: Dict[str, Any]) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
        self._config = parsed
        self._load_form_from_config()

    def _reload_from_file(self) -> None:
        try:
            cfg = load_pipeline_config(self.config_path)
            self._config = _ensure_config_defaults(cfg)
            self._load_form_from_config()
            QMessageBox.information(self, "完成", "已从文件重载配置")
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "失败", str(exc))

    def _save_from_form(self) -> None:
        try:
            parsed = self._collect_form_config()
        except Exception as exc:  # noqa: BLE001
            QMessageBox.critical(self, "保存失败", str(exc))
            return
        errors = self._validate_form_config(parsed)
        if errors:
            QMessageBox.critical(self, "保存失败", "\n".join([f"- {e}" for e in errors]))
            return
        self._persist(parsed)
        QMessageBox.information(self, "完成", "基础配置已保存")

    def get_config(self) -> Dict[str, Any]:
        return _deepcopy_cfg(self._config)

    def _apply_dialog_style(self) -> None:
        # Dialog follows the same visual language as the main window.
        self.setStyleSheet(
            """
            QDialog#configDialog { background: #eef2f8; }
            QTabWidget#configTabs::pane {
                border: 1px solid #d6e0ef;
                border-radius: 10px;
                background: #f8fafc;
                top: -1px;
            }
            QTabBar::tab {
                background: #e2e8f0;
                color: #334155;
                border: 1px solid #cbd5e1;
                border-bottom: 0;
                padding: 8px 14px;
                border-top-left-radius: 8px;
                border-top-right-radius: 8px;
                min-width: 140px;
            }
            QTabBar::tab:selected {
                background: #ffffff;
                color: #1e3a8a;
                border-color: #bfdbfe;
            }
            QWidget#configBasicTab,
            QWidget#configScrollViewport,
            QWidget#configScrollContent {
                background: #f8fafc;
            }
            QScrollArea#configScroll {
                border: none;
                background: #f8fafc;
            }
            QGroupBox {
                border: 1px solid #d6e0ef;
                border-radius: 10px;
                margin-top: 10px;
                padding: 12px 10px 10px 10px;
                background: #ffffff;
                font-weight: 700;
                color: #1e3a8a;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                top: 2px;
                padding: 0 4px;
            }
            QLabel, QCheckBox {
                background: transparent;
            }
            QLineEdit, QComboBox, QSpinBox, QPlainTextEdit {
                border: 1px solid #cbd5e1;
                border-radius: 8px;
                padding: 6px 8px;
                background: #ffffff;
                min-height: 32px;
            }
            QLineEdit:focus, QComboBox:focus, QSpinBox:focus, QPlainTextEdit:focus {
                border: 1px solid #2563eb;
            }
            QLabel#mutedNote { color: #64748b; }
            QMessageBox, QMessageBox QWidget {
                background: #ffffff;
                color: #0f172a;
            }
            QMessageBox QLabel {
                background: #ffffff;
                color: #0f172a;
                min-width: 280px;
            }
            """
        )

class UnifiedReportWindow(QMainWindow):
    def __init__(self, config: Dict[str, Any], auto_start_scheduler: bool = True) -> None:
        super().__init__()
        self.setWindowTitle("全景月报自动与补传控制台 (PySide6)")
        self.resize(1160, 860)

        self.bus = UiBus()
        self.bus.log.connect(self._append_log)
        self.bus.info.connect(lambda t, m: QMessageBox.information(self, t, m))
        self.bus.error.connect(lambda t, m: QMessageBox.critical(self, t, m))
        self.bus.task_state.connect(self._apply_running_state)

        self.task_lock = threading.Lock()

        self.config = _ensure_config_defaults(config)
        self.config_path = self._resolve_config_path()
        self.calc_module = load_calc_module()
        self.pipeline_module = load_download_module()

        if not hasattr(self.calc_module, "run_with_explicit_files"):
            raise RuntimeError("计算脚本缺少 run_with_explicit_files 入口")
        if not hasattr(self.calc_module, "import_workbook_sheets_to_feishu"):
            raise RuntimeError("计算脚本缺少 import_workbook_sheets_to_feishu 入口")
        if not hasattr(self.pipeline_module, "main"):
            raise RuntimeError("下载脚本缺少 main 入口")

        self.scheduler = DailyAutoScheduler(
            config=self.config,
            emit_log=self.log_async,
            run_callback=self._run_pipeline_from_scheduler,
            is_busy=lambda: self.task_lock.locked(),
        )

        self._build_ui()
        self._apply_style()
        self._apply_config_to_ui()

        if self.scheduler.enabled and self.scheduler.auto_start_in_gui and auto_start_scheduler:
            self.scheduler.start()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self._refresh_scheduler_labels)
        self.timer.start(5000)
        self._refresh_scheduler_labels()

    def _resolve_config_path(self) -> Path:
        try:
            return resolve_config_path()
        except Exception:
            return Path(__file__).with_name("表格计算配置.json")

    def _build_ui(self) -> None:
        root = QWidget()
        root.setObjectName("mainRoot")
        self.setCentralWidget(root)
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(0)

        self.main_scroll = QScrollArea()
        self.main_scroll.setObjectName("mainScroll")
        self.main_scroll.setWidgetResizable(True)
        root_layout.addWidget(self.main_scroll)

        content = QWidget()
        content.setObjectName("mainContent")
        self.main_scroll.setWidget(content)

        main = QVBoxLayout(content)
        main.setContentsMargins(14, 14, 14, 14)
        main.setSpacing(12)

        title = QLabel("全景月报自动与补传控制台")
        title.setObjectName("title")
        subtitle = QLabel("内网下载 -> 外网上传飞书多维，支持每日调度、手动补传、5Sheet导表、配置编辑")
        subtitle.setObjectName("subtitle")
        main.addWidget(title)
        main.addWidget(subtitle)

        auto_box = QGroupBox("自动流程")
        auto_box.setObjectName("panel")
        auto_layout = QVBoxLayout(auto_box)
        auto_layout.setSpacing(10)
        auto_hint = QLabel("点击“立即执行自动流程”会按配置自动切网下载并上传。")
        auto_hint.setObjectName("mutedNote")
        auto_layout.addWidget(auto_hint)
        row = QHBoxLayout()
        row.setSpacing(8)
        self.btn_run_auto = QPushButton("立即执行自动流程")
        self.btn_toggle_scheduler = QPushButton("启动调度")
        self.btn_config = QPushButton("配置设置")
        self.btn_run_auto.setProperty("kind", "primary")
        self.btn_toggle_scheduler.setProperty("kind", "secondary")
        self.btn_config.setProperty("kind", "ghost")
        row.addWidget(self.btn_run_auto)
        row.addWidget(self.btn_toggle_scheduler)
        row.addWidget(self.btn_config)
        row.addStretch(1)
        auto_layout.addLayout(row)
        self.lbl_scheduler = QLabel("调度状态: 初始化中")
        self.lbl_next = QLabel("下次执行: -")
        self.lbl_scheduler.setObjectName("statusChip")
        self.lbl_next.setObjectName("statusChip")
        auto_layout.addWidget(self.lbl_scheduler)
        auto_layout.addWidget(self.lbl_next)
        main.addWidget(auto_box)

        multi_date_box = QGroupBox("多日期自动流程")
        multi_date_box.setObjectName("panel")
        multi_grid = QGridLayout(multi_date_box)
        multi_grid.setHorizontalSpacing(10)
        multi_grid.setVerticalSpacing(10)

        multi_grid.addWidget(QLabel("选择日期"), 0, 0)
        self.date_picker = QDateEdit()
        self.date_picker.setCalendarPopup(True)
        self.date_picker.setDisplayFormat("yyyy-MM-dd")
        self.date_picker.setDate(QDate.currentDate())
        multi_grid.addWidget(self.date_picker, 0, 1)

        self.btn_date_add = QPushButton("添加日期")
        self.btn_date_add.setProperty("kind", "secondary")
        multi_grid.addWidget(self.btn_date_add, 0, 2)

        multi_grid.addWidget(QLabel("已选日期"), 1, 0, Qt.AlignTop)
        self.list_dates = QListWidget()
        self.list_dates.setFlow(QListView.LeftToRight)
        self.list_dates.setViewMode(QListView.IconMode)
        self.list_dates.setResizeMode(QListView.Adjust)
        self.list_dates.setMovement(QListView.Static)
        self.list_dates.setWrapping(True)
        self.list_dates.setSpacing(6)
        self.list_dates.setSelectionMode(QListWidget.ExtendedSelection)
        self.list_dates.setMinimumHeight(120)
        multi_grid.addWidget(self.list_dates, 1, 1, 1, 2)

        self.lbl_date_count = QLabel("已选0天")
        self.lbl_date_count.setObjectName("statusChip")
        multi_grid.addWidget(self.lbl_date_count, 2, 1)

        multi_btn_row = QHBoxLayout()
        self.btn_date_remove = QPushButton("移除选中")
        self.btn_date_remove.setProperty("kind", "ghost")
        self.btn_date_clear = QPushButton("清空日期")
        self.btn_date_clear.setProperty("kind", "ghost")
        self.btn_run_multi_date = QPushButton("执行多日期自动流程")
        self.btn_run_multi_date.setProperty("kind", "primary")
        multi_btn_row.addWidget(self.btn_date_remove)
        multi_btn_row.addWidget(self.btn_date_clear)
        multi_btn_row.addStretch(1)
        multi_btn_row.addWidget(self.btn_run_multi_date)
        multi_grid.addLayout(multi_btn_row, 3, 1, 1, 2)
        multi_grid.setColumnStretch(1, 1)
        main.addWidget(multi_date_box)

        manual_box = QGroupBox("手动补传")
        manual_box.setObjectName("panel")
        grid = QGridLayout(manual_box)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(10)
        grid.addWidget(QLabel("楼栋"), 0, 0)
        self.cmb_building = QComboBox()
        grid.addWidget(self.cmb_building, 0, 1)

        grid.addWidget(QLabel("xlsx文件"), 1, 0)
        self.edit_file = QLineEdit()
        self.edit_file.setPlaceholderText("请选择要补传的 xlsx 文件")
        self.btn_choose = QPushButton("选择文件")
        self.btn_choose.setProperty("kind", "secondary")
        grid.addWidget(self.edit_file, 1, 1)
        grid.addWidget(self.btn_choose, 1, 2)

        self.chk_switch = QCheckBox("补传前先切换到外网")
        self.chk_switch.setChecked(True)
        grid.addWidget(self.chk_switch, 2, 1)

        self.btn_manual = QPushButton("执行手动补传")
        self.btn_manual.setProperty("kind", "primary")
        grid.addWidget(self.btn_manual, 3, 1)
        grid.setColumnStretch(1, 1)
        main.addWidget(manual_box)

        sheet_box = QGroupBox("5Sheet导表（清空后导入）")
        sheet_box.setObjectName("panel")
        sheet_grid = QGridLayout(sheet_box)
        sheet_grid.setHorizontalSpacing(10)
        sheet_grid.setVerticalSpacing(10)
        sheet_grid.addWidget(QLabel("xlsx文件"), 0, 0)
        self.edit_sheet_file = QLineEdit()
        self.edit_sheet_file.setPlaceholderText("请选择包含5个Sheet的 xlsx 文件")
        self.btn_choose_sheet = QPushButton("选择文件")
        self.btn_choose_sheet.setProperty("kind", "secondary")
        sheet_grid.addWidget(self.edit_sheet_file, 0, 1)
        sheet_grid.addWidget(self.btn_choose_sheet, 0, 2)
        self.chk_sheet_switch = QCheckBox("导表前先切换到外网")
        self.chk_sheet_switch.setChecked(True)
        sheet_grid.addWidget(self.chk_sheet_switch, 1, 1)
        self.btn_sheet_import = QPushButton("清空并上传5个Sheet")
        self.btn_sheet_import.setProperty("kind", "primary")
        sheet_grid.addWidget(self.btn_sheet_import, 2, 1)
        sheet_grid.setColumnStretch(1, 1)
        main.addWidget(sheet_box)

        log_box = QGroupBox("日志")
        log_box.setObjectName("panel")
        log_box.setMinimumHeight(430)
        log_layout = QVBoxLayout(log_box)
        clear_row = QHBoxLayout()
        self.btn_clear = QPushButton("清空日志")
        self.btn_clear.setProperty("kind", "ghost")
        clear_row.addStretch(1)
        clear_row.addWidget(self.btn_clear)
        log_layout.addLayout(clear_row)

        self.log_edit = QTextEdit()
        self.log_edit.setObjectName("logPanel")
        self.log_edit.setReadOnly(True)
        self.log_edit.setFont(QFont("Consolas", 10))
        self.log_edit.setMinimumHeight(360)
        log_layout.addWidget(self.log_edit)
        main.addWidget(log_box)

        self.btn_run_auto.clicked.connect(self.start_auto_pipeline)
        self.btn_toggle_scheduler.clicked.connect(self.toggle_scheduler)
        self.btn_config.clicked.connect(self.open_config_dialog)
        self.btn_date_add.clicked.connect(self.add_date_selection)
        self.btn_date_remove.clicked.connect(self.remove_selected_dates)
        self.btn_date_clear.clicked.connect(self.clear_selected_dates)
        self.btn_run_multi_date.clicked.connect(self.start_multi_date_pipeline)
        self.btn_choose.clicked.connect(self.choose_file)
        self.btn_manual.clicked.connect(self.start_manual_upload)
        self.btn_choose_sheet.clicked.connect(self.choose_sheet_file)
        self.btn_sheet_import.clicked.connect(self.start_sheet_import)
        self.btn_clear.clicked.connect(lambda: self.log_edit.clear())

    def _apply_style(self) -> None:
        self.setStyleSheet(
            """
            QWidget {
                color: #0f172a;
                font-family: 'Microsoft YaHei UI';
                font-size: 13px;
            }
            QWidget#mainRoot {
                background: #eef2f8;
            }
            QWidget#mainContent {
                background: #eef2f8;
            }
            QLabel, QCheckBox {
                background: transparent;
            }
            QLabel#title {
                font-size: 24px;
                font-weight: 700;
                color: #0b1f44;
            }
            QLabel#subtitle {
                color: #475569;
                font-size: 13px;
            }
            QLabel#mutedNote {
                color: #64748b;
                font-size: 12px;
            }
            QLabel#statusChip {
                color: #1e3a8a;
                background: #eff6ff;
                border: 1px solid #bfdbfe;
                border-radius: 8px;
                padding: 4px 8px;
            }
            QGroupBox {
                border: 1px solid #d6e0ef;
                border-radius: 12px;
                margin-top: 12px;
                padding: 14px 12px 12px 12px;
                background: #ffffff;
                font-weight: 700;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                top: 2px;
                padding: 0 4px;
                color: #1e3a8a;
            }
            QPushButton {
                background: #2563eb;
                color: #ffffff;
                border: 1px solid #1d4ed8;
                border-radius: 8px;
                padding: 0 14px;
                min-height: 34px;
                font-weight: 600;
            }
            QPushButton:hover { background: #1d4ed8; }
            QPushButton:pressed { background: #1e40af; }
            QPushButton:disabled {
                background: #94a3b8;
                border-color: #94a3b8;
                color: #e2e8f0;
            }
            QPushButton[kind="secondary"] {
                background: #ffffff;
                color: #1d4ed8;
                border: 1px solid #9fb9e6;
            }
            QPushButton[kind="secondary"]:hover { background: #eff6ff; }
            QPushButton[kind="ghost"] {
                background: #f8fafc;
                color: #334155;
                border: 1px solid #cbd5e1;
            }
            QPushButton[kind="ghost"]:hover { background: #f1f5f9; }
            QLineEdit, QComboBox, QTextEdit, QSpinBox, QListWidget, QDateEdit {
                border: 1px solid #cbd5e1;
                border-radius: 8px;
                padding: 6px 8px;
                background: #ffffff;
                min-height: 32px;
            }
            QLineEdit:focus, QComboBox:focus, QTextEdit:focus, QSpinBox:focus, QListWidget:focus, QDateEdit:focus {
                border: 1px solid #2563eb;
            }
            QDateEdit {
                background: #ffffff;
                color: #0f172a;
            }
            QDateEdit::drop-down {
                background: #ffffff;
                border-left: 1px solid #cbd5e1;
                width: 24px;
            }
            QCalendarWidget QWidget {
                background: #ffffff;
                color: #0f172a;
            }
            QCalendarWidget QAbstractItemView {
                background: #ffffff;
                selection-background-color: #2563eb;
                selection-color: #ffffff;
            }
            QListWidget {
                background: #ffffff;
            }
            QListWidget::item {
                background: #eff6ff;
                border: 1px solid #bfdbfe;
                border-radius: 6px;
                padding: 4px 10px;
                margin: 2px;
            }
            QListWidget::item:selected {
                background: #2563eb;
                color: #ffffff;
                border: 1px solid #1d4ed8;
            }
            QCheckBox { spacing: 8px; }
            QCheckBox::indicator {
                width: 16px;
                height: 16px;
                border-radius: 3px;
                border: 1px solid #94a3b8;
                background: #ffffff;
            }
            QCheckBox::indicator:checked {
                background: #2563eb;
                border-color: #2563eb;
            }
            QTextEdit#logPanel {
                background: #0f172a;
                color: #dbeafe;
                border: 1px solid #1e293b;
                border-radius: 10px;
            }
            QScrollArea#mainScroll {
                border: none;
                background: #eef2f8;
            }
            QScrollArea { border: none; background: transparent; }
            QMessageBox, QMessageBox QWidget {
                background: #ffffff;
                color: #0f172a;
            }
            QMessageBox QLabel {
                background: #ffffff;
                color: #0f172a;
                min-width: 280px;
            }
            """
        )

    def _append_log(self, text: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_edit.append(f"[{ts}] {text}")

    def log_async(self, text: str) -> None:
        self.bus.log.emit(text)

    def _apply_running_state(self, running: bool) -> None:
        enabled = not running
        for btn in (
            self.btn_run_auto,
            self.btn_toggle_scheduler,
            self.btn_config,
            self.btn_date_add,
            self.btn_date_remove,
            self.btn_date_clear,
            self.btn_run_multi_date,
            self.btn_choose,
            self.btn_manual,
            self.btn_choose_sheet,
            self.btn_sheet_import,
        ):
            btn.setEnabled(enabled)
        self.date_picker.setEnabled(enabled)
        self.list_dates.setEnabled(enabled)

    def _acquire_task(self) -> bool:
        ok = self.task_lock.acquire(blocking=False)
        if ok:
            self.bus.task_state.emit(True)
        return ok

    def _release_task(self) -> None:
        if self.task_lock.locked():
            self.task_lock.release()
        self.bus.task_state.emit(False)

    def _refresh_scheduler_labels(self) -> None:
        self.lbl_scheduler.setText(f"调度状态: {self.scheduler.status_text()}")
        self.lbl_next.setText(f"下次执行: {self.scheduler.next_run_text()}")
        if self.scheduler.enabled:
            self.btn_toggle_scheduler.setText("停止调度" if self.scheduler.is_running() else "启动调度")
            self.btn_toggle_scheduler.setEnabled(not self.task_lock.locked())
        else:
            self.btn_toggle_scheduler.setText("调度已禁用")
            self.btn_toggle_scheduler.setEnabled(False)

    def _execute_pipeline_once(self, source: str, show_popup: bool, notify_on_failure: bool) -> tuple[bool, str]:
        writer = _LineEmitter(self.log_async)
        try:
            self.log_async(f"[{source}] 开始执行自动流程")
            with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                self.pipeline_module.main()
            writer.flush()
            self.log_async(f"[{source}] 自动流程执行完成")
            if show_popup:
                self.bus.info.emit("完成", "自动流程执行完成")
            return True, "ok"
        except Exception as exc:  # noqa: BLE001
            detail = str(exc)
            self.log_async(f"[{source}] 失败: {detail}")
            if notify_on_failure:
                _send_failure_webhook(self.config, stage=source, detail=detail, emit_log=self.log_async)
            if show_popup:
                self.bus.error.emit("失败", detail)
            return False, detail

    def _run_pipeline_from_scheduler(self, source: str) -> tuple[bool, str]:
        if not self._acquire_task():
            return False, "当前有任务运行"
        try:
            return self._execute_pipeline_once(source, False, notify_on_failure=("补跑" in source))
        finally:
            self._release_task()

    def start_auto_pipeline(self) -> None:
        if self.task_lock.locked():
            QMessageBox.warning(self, "提示", "当前有任务正在执行，请稍后")
            return
        threading.Thread(target=self._auto_worker, daemon=True).start()

    def _auto_worker(self) -> None:
        if not self._acquire_task():
            self.log_async("[自动] 当前有任务运行，取消本次触发")
            return
        try:
            self._execute_pipeline_once("手动触发自动流程", True, True)
        finally:
            self._release_task()

    def _get_selected_dates(self) -> list[str]:
        values: list[str] = []
        for i in range(self.list_dates.count()):
            text = self.list_dates.item(i).text().strip()
            if text:
                values.append(text)
        return values

    def _set_selected_dates(self, values: list[str]) -> None:
        self.list_dates.clear()
        for text in sorted(set(values)):
            self.list_dates.addItem(text)
        self.lbl_date_count.setText(f"已选{self.list_dates.count()}天")

    def add_date_selection(self) -> None:
        day = self.date_picker.date()
        today = QDate.currentDate()
        if day > today:
            QMessageBox.warning(self, "提示", "不允许选择未来日期")
            return
        values = self._get_selected_dates()
        values.append(day.toString("yyyy-MM-dd"))
        self._set_selected_dates(values)

    def remove_selected_dates(self) -> None:
        selected = self.list_dates.selectedItems()
        if not selected:
            return
        for item in selected:
            row = self.list_dates.row(item)
            self.list_dates.takeItem(row)
        self.lbl_date_count.setText(f"已选{self.list_dates.count()}天")

    def clear_selected_dates(self) -> None:
        self.list_dates.clear()
        self.lbl_date_count.setText("已选0天")

    def start_multi_date_pipeline(self) -> None:
        if self.task_lock.locked():
            QMessageBox.warning(self, "提示", "当前有任务正在执行，请稍后")
            return
        if self.list_dates.count() <= 0:
            QMessageBox.warning(self, "提示", "请至少添加一个日期")
            return
        threading.Thread(target=self._multi_date_worker, daemon=True).start()

    def _multi_date_worker(self) -> None:
        writer = _LineEmitter(self.log_async)
        if not self._acquire_task():
            self.log_async("[多日期自动流程] 当前有任务运行，取消本次触发")
            return
        try:
            selected_dates = self._get_selected_dates()
            if not selected_dates:
                raise ValueError("未选择任何日期")
            for day_text in selected_dates:
                day = datetime.strptime(day_text, "%Y-%m-%d").date()
                if day > datetime.now().date():
                    raise ValueError(f"不允许选择未来日期: {day_text}")
            self.log_async(f"[多日期自动流程] 开始执行，日期={','.join(sorted(selected_dates))}")
            if not hasattr(self.pipeline_module, "run_with_selected_dates"):
                raise RuntimeError("下载脚本缺少 run_with_selected_dates 入口")
            with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                result = self.pipeline_module.run_with_selected_dates(config=self.config, selected_dates=selected_dates)
            writer.flush()
            total_files = int(result.get("total_files", 0))
            success_dates = result.get("success_dates", [])
            failed_dates = result.get("failed_dates", [])
            error_text = str(result.get("error", "")).strip()
            if error_text:
                self.log_async(
                    f"[多日期自动流程] 失败：成功日期={len(success_dates)}，失败日期={len(failed_dates)}，下载文件={total_files}"
                )
                self.bus.error.emit("失败", error_text)
                return
            self.log_async(
                f"[多日期自动流程] 完成：成功日期={len(success_dates)}，失败日期={len(failed_dates)}，下载文件={total_files}"
            )
            self.bus.info.emit(
                "完成",
                f"多日期流程执行完成\n成功日期: {len(success_dates)}\n失败日期: {len(failed_dates)}\n下载文件: {total_files}",
            )
        except Exception as exc:  # noqa: BLE001
            detail = str(exc)
            self.log_async(f"[多日期自动流程] 失败: {detail}")
            _send_failure_webhook(self.config, stage="多日期自动流程", detail=detail, emit_log=self.log_async)
            self.bus.error.emit("失败", detail)
        finally:
            self._release_task()

    def choose_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "选择xlsx文件", "", "Excel文件 (*.xlsx);;所有文件 (*.*)")
        if path:
            self.edit_file.setText(path)

    def choose_sheet_file(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "选择5Sheet导表文件", "", "Excel文件 (*.xlsx);;所有文件 (*.*)")
        if path:
            self.edit_sheet_file.setText(path)

    def _switch_to_external_network(self, scene: str) -> None:
        net_cfg = self.config["network"]
        ssid = str(net_cfg["external_ssid"]).strip()
        if not ssid:
            raise ValueError("network.external_ssid 为空")
        wifi = WifiSwitcher(
            timeout_sec=int(net_cfg["switch_timeout_sec"]),
            retry_count=int(net_cfg["retry_count"]),
            retry_interval_sec=int(net_cfg["retry_interval_sec"]),
        )
        self.log_async(f"[网络][{scene}] 尝试切换到外网: {ssid}")
        ok, msg = wifi.connect(ssid, require_saved_profile=bool(net_cfg["require_saved_profiles"]))
        if not ok:
            raise RuntimeError(f"切换外网失败: {msg}")
        self.log_async(f"[网络][{scene}] {msg}")

    def start_manual_upload(self) -> None:
        if self.task_lock.locked():
            QMessageBox.warning(self, "提示", "当前有任务正在执行，请稍后")
            return
        if not self.edit_file.text().strip():
            QMessageBox.warning(self, "提示", "请先选择xlsx文件")
            return
        threading.Thread(target=self._manual_worker, daemon=True).start()

    def start_sheet_import(self) -> None:
        if self.task_lock.locked():
            QMessageBox.warning(self, "提示", "当前有任务正在执行，请稍后")
            return
        if not bool(_get_nested_value(self.config, "feishu_sheet_import.enabled", False)):
            QMessageBox.warning(self, "提示", "feishu_sheet_import.enabled=false，5Sheet导表功能已禁用")
            return
        if not self.edit_sheet_file.text().strip():
            QMessageBox.warning(self, "提示", "请先选择xlsx文件")
            return
        threading.Thread(target=self._sheet_import_worker, daemon=True).start()

    def _manual_worker(self) -> None:
        building = self.cmb_building.currentText().strip()
        file_path = self.edit_file.text().strip()
        writer = _LineEmitter(self.log_async)

        if not self._acquire_task():
            self.log_async("[补传] 当前有任务运行，取消本次补传")
            return
        try:
            file_obj = Path(file_path)
            if not file_obj.exists():
                raise FileNotFoundError(f"文件不存在: {file_path}")
            if file_obj.suffix.lower() != ".xlsx":
                raise ValueError("请选择xlsx文件")

            if self.chk_switch.isChecked():
                self._switch_to_external_network("手动补传")

            self.log_async(f"[补传] 开始处理 building={building}, file={file_path}")
            with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                self.calc_module.run_with_explicit_files(
                    config=self.config,
                    building_to_file={building: file_path},
                    upload=True,
                    save_json=True,
                )
            writer.flush()
            self.log_async("[补传] 处理完成")
            self.bus.info.emit("完成", "单楼补传完成")
        except Exception as exc:  # noqa: BLE001
            detail = str(exc)
            self.log_async(f"[补传] 失败: {detail}")
            _send_failure_webhook(self.config, stage="手动补传", detail=detail, building=building or None, emit_log=self.log_async)
            self.bus.error.emit("失败", detail)
        finally:
            self._release_task()

    @staticmethod
    def _build_sheet_import_failure_detail(file_path: str, result: Dict[str, Any]) -> str:
        failed_items = [item for item in result.get("sheet_results", []) if not bool(item.get("success"))]
        if not failed_items:
            return f"文件: {file_path}，导表结果返回失败但未附带失败详情。"
        fragments: list[str] = []
        for item in failed_items:
            sheet_name = str(item.get("sheet_name", "")).strip() or "(未知Sheet)"
            error = str(item.get("error", "")).strip() or "未知错误"
            fragments.append(f"{sheet_name}: {error}")
        return f"文件: {file_path}；失败Sheet({len(fragments)}): " + " | ".join(fragments)

    def _sheet_import_worker(self) -> None:
        file_path = self.edit_sheet_file.text().strip()
        writer = _LineEmitter(self.log_async)
        if not self._acquire_task():
            self.log_async("[5Sheet导表] 当前有任务运行，取消本次导表")
            return
        try:
            file_obj = Path(file_path)
            if not file_obj.exists():
                raise FileNotFoundError(f"文件不存在: {file_path}")
            if file_obj.suffix.lower() != ".xlsx":
                raise ValueError("请选择xlsx文件")

            if self.chk_sheet_switch.isChecked():
                self._switch_to_external_network("5Sheet导表")

            self.log_async(f"[5Sheet导表] 开始处理 file={file_path}")
            with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                result = self.calc_module.import_workbook_sheets_to_feishu(config=self.config, xlsx_path=file_path)
            writer.flush()

            success_count = int(result.get("success_count", 0))
            failed_count = int(result.get("failed_count", 0))
            self.log_async(f"[5Sheet导表] 完成: 成功Sheet={success_count}, 失败Sheet={failed_count}")
            if failed_count > 0:
                detail = self._build_sheet_import_failure_detail(file_path=file_path, result=result)
                _send_failure_webhook(self.config, stage="5Sheet导表", detail=detail, emit_log=self.log_async)
                self.bus.error.emit("部分失败", detail)
            else:
                self.bus.info.emit("完成", "5个Sheet均已清空并导入完成")
        except Exception as exc:  # noqa: BLE001
            detail = str(exc)
            self.log_async(f"[5Sheet导表] 失败: {detail}")
            _send_failure_webhook(self.config, stage="5Sheet导表", detail=detail, emit_log=self.log_async)
            self.bus.error.emit("失败", detail)
        finally:
            self._release_task()

    def toggle_scheduler(self) -> None:
        if not self.scheduler.enabled:
            QMessageBox.warning(self, "提示", "scheduler.enabled=false，调度已禁用")
            return
        if self.scheduler.is_running():
            self.scheduler.stop()
            self.log_async("[调度] 已手动停止")
        else:
            self.scheduler.start()
            self.log_async("[调度] 已手动启动")
        self._refresh_scheduler_labels()

    def open_config_dialog(self) -> None:
        dlg = ConfigEditorDialog(self.config, self.config_path, self)
        if dlg.exec() != QDialog.Accepted:
            return
        was_running = self.scheduler.is_running()
        self.scheduler.stop()
        self.config = _ensure_config_defaults(dlg.get_config())
        self.scheduler = DailyAutoScheduler(
            config=self.config,
            emit_log=self.log_async,
            run_callback=self._run_pipeline_from_scheduler,
            is_busy=lambda: self.task_lock.locked(),
        )
        self._apply_config_to_ui()
        if was_running and self.scheduler.enabled:
            self.scheduler.start()
        self._refresh_scheduler_labels()
        self.log_async(f"[配置] 已保存并重新加载: {self.config_path}")

    def _apply_config_to_ui(self) -> None:
        input_cfg = self.config["input"]
        buildings = input_cfg["buildings"]
        values = [str(x).strip() for x in buildings if str(x).strip()]
        if not values:
            raise ValueError("配置错误: input.buildings 缺失或为空")
        curr = self.cmb_building.currentText().strip()
        self.cmb_building.clear()
        self.cmb_building.addItems(values)
        if curr in values:
            self.cmb_building.setCurrentText(curr)

        ext = str(self.config["network"]["external_ssid"]).strip()
        self.chk_switch.setText(f"补传前先切换到外网({ext})")
        self.chk_sheet_switch.setText(f"导表前先切换到外网({ext})")

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if self.task_lock.locked():
            answer = QMessageBox.question(
                self,
                "确认退出",
                "当前有任务正在执行，确定退出吗？",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No,
            )
            if answer != QMessageBox.Yes:
                event.ignore()
                return
        self.scheduler.stop()
        event.accept()


def _run_auto_mode(config: Dict[str, Any]) -> None:
    module = load_download_module()
    try:
        module.main()
    except Exception as exc:  # noqa: BLE001
        detail = str(exc)
        _send_failure_webhook(config, stage="自动流程启动", detail=detail)
        raise


def _run_daemon_mode(config: Dict[str, Any]) -> None:
    module = load_download_module()
    lock = threading.Lock()

    def run_once(source: str) -> tuple[bool, str]:
        if not lock.acquire(blocking=False):
            return False, "当前有任务运行"
        try:
            print(f"[{source}] 开始执行自动流程")
            module.main()
            print(f"[{source}] 自动流程执行完成")
            return True, "ok"
        except Exception as exc:  # noqa: BLE001
            detail = str(exc)
            print(f"[{source}] 失败: {detail}")
            if "补跑" in source:
                _send_failure_webhook(config, stage=source, detail=detail, emit_log=print)
            return False, detail
        finally:
            lock.release()

    scheduler = DailyAutoScheduler(config=config, emit_log=print, run_callback=run_once, is_busy=lambda: lock.locked())
    if not scheduler.enabled:
        print("[调度] scheduler.enabled=false，守护模式不启动")
        return

    scheduler.start()
    print("[守护] 已进入内置调度守护模式，按 Ctrl+C 退出")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[守护] 收到退出信号，停止调度")
    finally:
        scheduler.stop()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="月报统一GUI/内置每日调度入口 (PySide6)")
    parser.add_argument("--auto", action="store_true", help="不打开GUI，立即执行一次自动流程")
    parser.add_argument("--daemon", action="store_true", help="不打开GUI，进入内置每日调度守护模式")
    parser.add_argument("--no-scheduler", action="store_true", help="GUI模式下不自动启动调度")
    args = parser.parse_args(argv)

    if args.auto and args.daemon:
        parser.error("--auto 和 --daemon 不能同时使用")

    config = load_pipeline_config()
    if args.auto:
        _run_auto_mode(config)
        return
    if args.daemon:
        _run_daemon_mode(config)
        return

    if "manual_upload_gui" not in config or "enabled" not in config["manual_upload_gui"]:
        raise ValueError("配置错误: manual_upload_gui.enabled 缺失，请在JSON中配置。")
    if not config["manual_upload_gui"]["enabled"]:
        print("manual_upload_gui.enabled=false，GUI入口已禁用")
        return

    app = QApplication([])
    app.setApplicationName("全景月报自动与补传控制台")
    window = UnifiedReportWindow(config=config, auto_start_scheduler=not args.no_scheduler)
    window.show()
    window.log_async("统一GUI已就绪，可手动执行或由内置调度自动执行")
    app.exec()


if __name__ == "__main__":
    main()
