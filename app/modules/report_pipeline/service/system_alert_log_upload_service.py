from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from pipeline_utils import get_app_dir


class SystemAlertLogUploadService:
    TARGET_APP_TOKEN = "HScCwZt9QiqPCUkSrHjcbBL1ngb"
    TARGET_TABLE_ID = "tblGy6Z1GTxbY1EQ"
    TARGET_FIELD_NAME = "日志信息"
    IDLE_SECONDS = 30.0
    POLL_INTERVAL_SECONDS = 1.0
    BATCH_SIZE = 100
    FORCE_FLUSH_PENDING_LINES = 1000
    FORCE_FLUSH_AGE_SECONDS = 600.0
    COMPACT_UPLOADED_LINES_THRESHOLD = 5000
    COMPACT_FILE_SIZE_THRESHOLD_BYTES = 5 * 1024 * 1024

    def __init__(
        self,
        *,
        config_getter: Callable[[], Dict[str, Any]],
        active_job_id_getter: Callable[[], str],
        emit_log: Callable[[str], None],
        runtime_state_root: str = "",
        mark_uploaded: Callable[[List[int]], None] | None = None,
    ) -> None:
        self._config_getter = config_getter
        self._active_job_id_getter = active_job_id_getter
        self._emit_log = emit_log
        self._mark_uploaded = mark_uploaded
        self._runtime_root = self._resolve_runtime_root(runtime_state_root)
        self._queue_dir = self._runtime_root / "system_alert_logs"
        self._queue_path = self._queue_dir / "queue.jsonl"
        self._state_path = self._queue_dir / "upload_state.json"
        self._queue_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_active_at = time.monotonic()
        self._state = self._load_state()
        self._last_flush_at = ""
        self._last_error = ""

    @staticmethod
    def _resolve_runtime_root(runtime_state_root: str) -> Path:
        app_dir = get_app_dir()
        text = str(runtime_state_root or "").strip()
        path = Path(text) if text else app_dir / ".runtime"
        if not path.is_absolute():
            path = app_dir / path
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _load_state(self) -> Dict[str, Any]:
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            payload = {}
        uploaded_line_count = int(payload.get("uploaded_line_count", 0) or 0)
        return {"uploaded_line_count": max(0, uploaded_line_count)}

    def _save_state(self) -> None:
        self._state_path.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> Dict[str, Any]:
        if self.is_running():
            return {"started": False, "running": True, "reason": "already_running"}
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="system-alert-log-uploader",
        )
        self._thread.start()
        return {"started": True, "running": True, "reason": "started"}

    def stop(self) -> Dict[str, Any]:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None
        return {"stopped": True, "running": False, "reason": "stopped"}

    def enqueue_entry(self, entry: Dict[str, Any]) -> None:
        payload = entry if isinstance(entry, dict) else {}
        level = str(payload.get("level", "")).strip().lower()
        if level not in {"warning", "error"}:
            return
        with self._lock:
            with self._queue_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _loop(self) -> None:
        while not self._stop.wait(self.POLL_INTERVAL_SECONDS):
            active_job_id = str(self._active_job_id_getter() or "").strip()
            if active_job_id:
                self._last_active_at = time.monotonic()
                if self._should_force_flush():
                    self._flush_pending()
                continue
            if time.monotonic() - self._last_active_at < self.IDLE_SECONDS and not self._should_force_flush():
                continue
            self._flush_pending()

    def _pending_queue_stats(self) -> Dict[str, Any]:
        with self._lock:
            if not self._queue_path.exists():
                return {
                    "pending_lines": 0,
                    "queue_file_size_bytes": 0,
                    "oldest_pending_at": "",
                    "oldest_pending_age_sec": 0.0,
                }
            try:
                raw_lines = self._queue_path.read_text(encoding="utf-8").splitlines()
            except Exception:  # noqa: BLE001
                return {
                    "pending_lines": 0,
                    "queue_file_size_bytes": int(self._queue_path.stat().st_size) if self._queue_path.exists() else 0,
                    "oldest_pending_at": "",
                    "oldest_pending_age_sec": 0.0,
                }
            start = int(self._state.get("uploaded_line_count", 0) or 0)
            pending_lines = max(0, len(raw_lines) - start)
            oldest_pending_at = ""
            oldest_pending_age_sec = 0.0
            for raw_line in raw_lines[start:]:
                text = str(raw_line or "").strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except Exception:  # noqa: BLE001
                    continue
                if not isinstance(payload, dict):
                    continue
                timestamp_text = str(payload.get("timestamp", "") or "").strip()
                if not timestamp_text:
                    continue
                oldest_pending_at = timestamp_text
                try:
                    oldest_pending_age_sec = max(
                        0.0,
                        time.time() - time.mktime(time.strptime(timestamp_text, "%Y-%m-%d %H:%M:%S")),
                    )
                except Exception:  # noqa: BLE001
                    oldest_pending_age_sec = 0.0
                break
            return {
                "pending_lines": pending_lines,
                "queue_file_size_bytes": int(self._queue_path.stat().st_size) if self._queue_path.exists() else 0,
                "oldest_pending_at": oldest_pending_at,
                "oldest_pending_age_sec": oldest_pending_age_sec,
            }

    def _should_force_flush(self) -> bool:
        stats = self._pending_queue_stats()
        if int(stats.get("pending_lines", 0) or 0) > self.FORCE_FLUSH_PENDING_LINES:
            return True
        return float(stats.get("oldest_pending_age_sec", 0.0) or 0.0) > self.FORCE_FLUSH_AGE_SECONDS

    def _load_pending_batch(self, limit: int) -> tuple[List[Dict[str, Any]], int]:
        with self._lock:
            if not self._queue_path.exists():
                return [], 0
            try:
                lines = self._queue_path.read_text(encoding="utf-8").splitlines()
            except Exception:  # noqa: BLE001
                return [], 0
            start = int(self._state.get("uploaded_line_count", 0) or 0)
            if start >= len(lines):
                return [], 0
            entries: List[Dict[str, Any]] = []
            consumed = 0
            for raw_line in lines[start:]:
                consumed += 1
                text = str(raw_line or "").strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except Exception:  # noqa: BLE001
                    continue
                if not isinstance(payload, dict):
                    continue
                entries.append(payload)
                if len(entries) >= limit:
                    break
            return entries, consumed

    def _advance_uploaded_cursor(self, consumed: int, uploaded_ids: List[int]) -> None:
        if consumed <= 0:
            return
        with self._lock:
            current = int(self._state.get("uploaded_line_count", 0) or 0)
            self._state["uploaded_line_count"] = current + consumed
            self._save_state()
        self._compact_queue_file_if_needed()
        if uploaded_ids and callable(self._mark_uploaded):
            try:
                self._mark_uploaded(uploaded_ids)
            except Exception:  # noqa: BLE001
                pass

    def _compact_queue_file_if_needed(self) -> None:
        with self._lock:
            if not self._queue_path.exists():
                return
            uploaded_line_count = int(self._state.get("uploaded_line_count", 0) or 0)
            try:
                file_size = int(self._queue_path.stat().st_size)
            except Exception:  # noqa: BLE001
                file_size = 0
            if (
                uploaded_line_count < self.COMPACT_UPLOADED_LINES_THRESHOLD
                and file_size < self.COMPACT_FILE_SIZE_THRESHOLD_BYTES
            ):
                return
            try:
                lines = self._queue_path.read_text(encoding="utf-8").splitlines()
            except Exception:  # noqa: BLE001
                return
            remaining = lines[uploaded_line_count:] if uploaded_line_count < len(lines) else []
            payload = ("\n".join(remaining) + ("\n" if remaining else ""))
            self._queue_path.write_text(payload, encoding="utf-8")
            self._state["uploaded_line_count"] = 0
            self._save_state()

    def _build_client(self) -> FeishuBitableClient:
        config = self._config_getter() if callable(self._config_getter) else {}
        common = config.get("common", {}) if isinstance(config, dict) else {}
        feishu_auth = common.get("feishu_auth", {}) if isinstance(common, dict) else {}
        if not isinstance(feishu_auth, dict):
            feishu_auth = {}
        app_id = str(feishu_auth.get("app_id", "") or "").strip()
        app_secret = str(feishu_auth.get("app_secret", "") or "").strip()
        if not app_id or not app_secret:
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        return FeishuBitableClient(
            app_id=app_id,
            app_secret=app_secret,
            app_token=self.TARGET_APP_TOKEN,
            calc_table_id=self.TARGET_TABLE_ID,
            attachment_table_id=self.TARGET_TABLE_ID,
            timeout=int(feishu_auth.get("timeout", 30) or 30),
            request_retry_count=int(feishu_auth.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(feishu_auth.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=lambda **_: 0,
            canonical_metric_name_fn=lambda value: str(value or "").strip(),
            dimension_mapping={},
        )

    def _upload_entries(self, entries: List[Dict[str, Any]]) -> None:
        client = self._build_client()
        fields_list = [
            {self.TARGET_FIELD_NAME: str(item.get("line", "") or "").strip()}
            for item in entries
            if str(item.get("line", "") or "").strip()
        ]
        if not fields_list:
            return
        client.batch_create_records(
            table_id=self.TARGET_TABLE_ID,
            fields_list=fields_list,
            batch_size=self.BATCH_SIZE,
        )

    def _flush_pending(self) -> None:
        while not self._stop.is_set():
            entries, consumed = self._load_pending_batch(self.BATCH_SIZE)
            if not entries and consumed <= 0:
                return
            if not entries and consumed > 0:
                self._advance_uploaded_cursor(consumed, [])
                continue
            try:
                self._upload_entries(entries)
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                self._emit_log(f"[系统告警上报] 上传失败: {exc}")
                return
            self._advance_uploaded_cursor(
                consumed,
                [int(item.get("id", 0) or 0) for item in entries if int(item.get("id", 0) or 0) > 0],
            )
            self._last_error = ""
            self._last_flush_at = time.strftime("%Y-%m-%d %H:%M:%S")
            self._emit_log(f"[系统告警上报] 上传完成 count={len(entries)}")

    def runtime_snapshot(self) -> Dict[str, Any]:
        stats = self._pending_queue_stats()
        return {
            "running": bool(self.is_running()),
            "pending_lines": int(stats.get("pending_lines", 0) or 0),
            "queue_file_size_bytes": int(stats.get("queue_file_size_bytes", 0) or 0),
            "oldest_pending_at": str(stats.get("oldest_pending_at", "") or "").strip(),
            "last_flush_at": str(self._last_flush_at or "").strip(),
            "last_error": str(self._last_error or "").strip(),
        }
