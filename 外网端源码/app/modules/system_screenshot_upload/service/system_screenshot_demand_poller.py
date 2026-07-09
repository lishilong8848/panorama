from __future__ import annotations

import copy
import threading
import time
from datetime import datetime
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient


DEFAULT_DEMAND_POLL_CONFIG: Dict[str, Any] = {
    "enabled": True,
    "interval_sec": 30,
    "app_token": "ASLxbfESPahdTKs0A9NccgbrnXc",
    "table_id": "tblQlmBM3vgoN7fq",
    "request_field": "同步需求",
    "completed_field": "上传完成",
    "page_size": 100,
    "max_records": 100,
}


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _deep_merge(raw: Any, defaults: Any) -> Any:
    if isinstance(defaults, dict):
        src = raw if isinstance(raw, dict) else {}
        out: Dict[str, Any] = {}
        for key, default_value in defaults.items():
            out[key] = _deep_merge(src.get(key), default_value)
        for key, value in src.items():
            if key not in out:
                out[key] = copy.deepcopy(value)
        return out
    if isinstance(defaults, list):
        return copy.deepcopy(raw) if isinstance(raw, list) else copy.deepcopy(defaults)
    return copy.deepcopy(defaults if raw is None else raw)


def _positive_int(value: Any, default: int, *, min_value: int = 1, max_value: int | None = None) -> int:
    try:
        number = int(float(str(value).strip()))
    except Exception:
        number = int(default)
    if number < min_value:
        number = min_value
    if max_value is not None and number > max_value:
        number = max_value
    return number


def _truthy_checkbox(value: Any) -> bool:
    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on", "是", "勾选", "已勾选"}
    if isinstance(value, list):
        return any(_truthy_checkbox(item) for item in value)
    if isinstance(value, dict):
        for key in ("checked", "value", "text", "name"):
            if key in value and _truthy_checkbox(value.get(key)):
                return True
    return False


def normalize_demand_poll_config(
    runtime_config: Dict[str, Any],
    overrides: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    screenshot_cfg = _dict(_dict(runtime_config).get("system_screenshot_upload"))
    poll_cfg = _deep_merge(screenshot_cfg.get("demand_poll"), DEFAULT_DEMAND_POLL_CONFIG)
    for key, value in _dict(overrides).items():
        if key in poll_cfg and value not in (None, ""):
            poll_cfg[key] = value
    poll_cfg["interval_sec"] = _positive_int(poll_cfg.get("interval_sec"), 30, min_value=5, max_value=3600)
    poll_cfg["page_size"] = _positive_int(poll_cfg.get("page_size"), 100, min_value=1, max_value=500)
    poll_cfg["max_records"] = _positive_int(poll_cfg.get("max_records"), 100, min_value=1, max_value=5000)
    for key in ("app_token", "table_id", "request_field", "completed_field"):
        poll_cfg[key] = str(poll_cfg.get(key, "") or "").strip()
    return poll_cfg


def mark_demand_record_completed(
    runtime_config: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    client_factory: Callable[[str], FeishuBitableClient] | None = None,
) -> Dict[str, Any]:
    cfg = normalize_demand_poll_config(
        runtime_config,
        {
            "app_token": payload.get("demand_app_token"),
            "table_id": payload.get("demand_table_id"),
            "request_field": payload.get("demand_request_field"),
            "completed_field": payload.get("demand_completed_field"),
        },
    )
    record_id = str(payload.get("demand_record_id", "") or "").strip()
    if not record_id:
        raise ValueError("缺少同步需求记录 record_id")
    if not cfg["app_token"] or not cfg["table_id"]:
        raise ValueError("系统截图同步需求表配置缺失")
    client = client_factory(cfg["app_token"]) if callable(client_factory) else FeishuBitableClient(cfg["app_token"])
    return client.update_record(
        cfg["table_id"],
        record_id,
        {
            cfg["request_field"]: False,
            cfg["completed_field"]: True,
        },
    )


class SystemScreenshotDemandPoller:
    def __init__(
        self,
        *,
        runtime_config_getter: Callable[[], Dict[str, Any]],
        job_service: Any,  # noqa: ANN401
        emit_log: Callable[[str], None] | None = None,
        role_mode_getter: Callable[[], str] | None = None,
        client_factory: Callable[[str], FeishuBitableClient] | None = None,
    ) -> None:
        self._runtime_config_getter = runtime_config_getter
        self._job_service = job_service
        self._emit_log = emit_log
        self._role_mode_getter = role_mode_getter
        self._client_factory = client_factory
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._snapshot: Dict[str, Any] = {
            "enabled": False,
            "running": False,
            "last_poll_at": "",
            "last_hit_count": 0,
            "last_submitted_job_id": "",
            "last_error": "",
            "last_decision": "未启动",
        }
        self._last_error_log_key = ""
        self._last_error_log_at = 0.0

    def _runtime_config(self) -> Dict[str, Any]:
        try:
            cfg = self._runtime_config_getter()
        except Exception:
            return {}
        return cfg if isinstance(cfg, dict) else {}

    def _config(self) -> Dict[str, Any]:
        return normalize_demand_poll_config(self._runtime_config())

    def _role_mode(self) -> str:
        if not callable(self._role_mode_getter):
            return ""
        try:
            return str(self._role_mode_getter() or "").strip().lower()
        except Exception:
            return ""

    def _client(self, app_token: str) -> FeishuBitableClient:
        if callable(self._client_factory):
            return self._client_factory(app_token)
        return FeishuBitableClient(app_token)

    def _set_snapshot(self, **updates: Any) -> None:
        with self._lock:
            self._snapshot.update(updates)

    def _log(self, text: str) -> None:
        if callable(self._emit_log):
            try:
                self._emit_log(str(text or "").strip())
            except Exception:
                pass

    def _log_error_throttled(self, text: str) -> None:
        now = time.monotonic()
        key = str(text or "").strip()
        if key != self._last_error_log_key or now - self._last_error_log_at >= 60:
            self._last_error_log_key = key
            self._last_error_log_at = now
            self._log(f"[系统截图上传][同步需求] 轮询失败: {key}")

    def start(self) -> Dict[str, Any]:
        cfg = self._config()
        if not bool(cfg.get("enabled", True)):
            self._set_snapshot(
                enabled=False,
                running=False,
                last_decision="同步需求轮询已禁用",
            )
            return {"ok": True, "running": False, "reason": "disabled"}
        if self._thread and self._thread.is_alive():
            self._set_snapshot(enabled=True, running=True)
            return {"ok": True, "running": True, "reason": "already_running"}
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="system-screenshot-demand-poller",
            daemon=True,
        )
        self._thread.start()
        self._set_snapshot(enabled=True, running=True, last_decision="同步需求轮询已启动")
        return {"ok": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        self._stop_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=5)
        running = bool(self._thread and self._thread.is_alive())
        self._set_snapshot(running=running, last_decision="同步需求轮询已停止" if not running else "同步需求轮询停止超时")
        return {"ok": not running, "running": running, "reason": "stopped" if not running else "stop_timeout"}

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def status_snapshot(self) -> Dict[str, Any]:
        cfg = self._config()
        with self._lock:
            payload = copy.deepcopy(self._snapshot)
        payload.update(
            {
                "enabled": bool(cfg.get("enabled", True)),
                "running": self.is_running(),
                "interval_sec": int(cfg.get("interval_sec", 30) or 30),
                "table_id": cfg.get("table_id", ""),
                "request_field": cfg.get("request_field", ""),
                "completed_field": cfg.get("completed_field", ""),
            }
        )
        return payload

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            cfg = self._config()
            if bool(cfg.get("enabled", True)):
                try:
                    self.poll_once()
                except Exception as exc:  # noqa: BLE001
                    error_text = str(exc)
                    self._set_snapshot(
                        last_poll_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        last_error=error_text,
                        last_decision="轮询失败",
                    )
                    self._log_error_throttled(error_text)
            else:
                self._set_snapshot(enabled=False, running=True, last_decision="同步需求轮询已禁用")
            interval = int(cfg.get("interval_sec", 30) or 30)
            self._stop_event.wait(max(5, interval))
        self._set_snapshot(running=False)

    def poll_once(self) -> Dict[str, Any]:
        cfg = self._config()
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not bool(cfg.get("enabled", True)):
            result = {"ok": True, "skipped": True, "reason": "disabled"}
            self._set_snapshot(last_poll_at=now_text, last_decision="同步需求轮询已禁用", last_error="")
            return result
        role_mode = self._role_mode()
        if callable(self._role_mode_getter) and role_mode != "external":
            result = {"ok": True, "skipped": True, "reason": f"role={role_mode or '-'}"}
            self._set_snapshot(last_poll_at=now_text, last_decision="非外网端，跳过同步需求轮询", last_error="")
            return result
        if not cfg["app_token"] or not cfg["table_id"]:
            raise ValueError("系统截图同步需求表配置缺失")
        client = self._client(cfg["app_token"])
        records = client.list_records(
            table_id=cfg["table_id"],
            page_size=int(cfg.get("page_size", 100) or 100),
            max_records=int(cfg.get("max_records", 100) or 100),
            field_names=[cfg["request_field"], cfg["completed_field"]],
        )
        pending: List[Dict[str, Any]] = []
        for item in records:
            fields = _dict(item.get("fields"))
            if _truthy_checkbox(fields.get(cfg["request_field"])):
                pending.append(item)
        hit_count = len(pending)
        if hit_count <= 0:
            self._set_snapshot(
                last_poll_at=now_text,
                last_hit_count=0,
                last_error="",
                last_decision="未发现同步需求",
            )
            return {"ok": True, "submitted": False, "hit_count": 0}
        if self._job_service.has_active_jobs_for_feature_prefixes(["system_screenshot_upload"]):
            self._set_snapshot(
                last_poll_at=now_text,
                last_hit_count=hit_count,
                last_error="",
                last_decision="已有系统截图上传任务运行，跳过本轮",
            )
            return {"ok": True, "submitted": False, "hit_count": hit_count, "reason": "job_active"}
        record = pending[0]
        record_id = str(record.get("record_id", "") or "").strip()
        if not record_id:
            raise ValueError("同步需求记录缺少 record_id")
        capture_date = datetime.now().strftime("%Y-%m-%d")
        job = self._job_service.start_worker_job(
            name=f"系统截图上传-同步需求 {capture_date}",
            worker_handler="system_screenshot_demand_upload",
            worker_payload={
                "capture_date": capture_date,
                "trigger_internal_capture": True,
                "internal_capture_force": True,
                "demand_record_id": record_id,
                "demand_app_token": cfg["app_token"],
                "demand_table_id": cfg["table_id"],
                "demand_request_field": cfg["request_field"],
                "demand_completed_field": cfg["completed_field"],
            },
            resource_keys=[f"system_screenshot_upload:{capture_date}"],
            priority="scheduler",
            feature="system_screenshot_upload",
            dedupe_key=f"system_screenshot_upload:demand:{capture_date}:{record_id}",
            submitted_by="demand_poll",
        )
        job_id = str(getattr(job, "job_id", "") or "")
        self._set_snapshot(
            last_poll_at=now_text,
            last_hit_count=hit_count,
            last_submitted_job_id=job_id,
            last_error="",
            last_decision=f"已提交同步需求任务 job_id={job_id}",
        )
        self._log(
            f"[系统截图上传][同步需求] 已提交任务 job_id={job_id}, record_id={record_id}, date={capture_date}"
        )
        return {"ok": True, "submitted": True, "hit_count": hit_count, "job_id": job_id}
