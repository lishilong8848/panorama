from __future__ import annotations

import argparse
import json
import sqlite3
import socket
import sys
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict

from app.worker.task_handlers import HANDLER_REGISTRY


class WorkerCancelledError(RuntimeError):
    pass


class CancellationContext:
    def __init__(self) -> None:
        self._cancelled = threading.Event()
        self._cleanup_hooks: list[Callable[[], None]] = []
        self._lock = threading.Lock()

    def request_cancel(self) -> None:
        self._cancelled.set()

    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()

    def raise_if_cancelled(self) -> None:
        if self.is_cancelled():
            raise WorkerCancelledError("cancelled")

    def register_cleanup_hook(self, fn: Callable[[], None]) -> None:
        with self._lock:
            self._cleanup_hooks.append(fn)

    def run_cleanup(self) -> None:
        with self._lock:
            hooks = list(reversed(self._cleanup_hooks))
            self._cleanup_hooks.clear()
        for fn in hooks:
            try:
                fn()
            except Exception:
                pass


class WorkerRuntime:
    def __init__(self, *, stage_id: str, cancel_context: CancellationContext) -> None:
        self.stage_id = stage_id
        self.cancel_context = cancel_context

    def emit_log(self, text: str, *, level: str = "info") -> None:
        self.cancel_context.raise_if_cancelled()
        _emit_log(self.stage_id, text, level=level)

    def emit_event(self, payload: Dict[str, Any]) -> None:
        _emit_event(payload)

    def is_cancelled(self) -> bool:
        return self.cancel_context.is_cancelled()

    def raise_if_cancelled(self) -> None:
        self.cancel_context.raise_if_cancelled()

    def register_cleanup_hook(self, fn: Callable[[], None]) -> None:
        self.cancel_context.register_cleanup_hook(fn)


def _json_ready(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _load_config_snapshot(job_dir: Path) -> Dict[str, Any]:
    job_id = str(job_dir.name or "").strip()
    if not job_id:
        return {}
    db_path = job_dir.parent.parent / "task_engine.db"
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(str(db_path), timeout=5.0, isolation_level=None, check_same_thread=False)
    try:
        row = conn.execute("SELECT config_snapshot_json FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        return {}
    raw = str(row[0] or "").strip()
    if not raw or raw == "null":
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _emit_event(payload: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(_json_ready(payload), ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _emit_log(stage_id: str, text: str, *, level: str = "info") -> None:
    line = str(text or "").strip()
    if not line:
        return
    _emit_event(
        {
            "type": "log",
            "level": str(level or "info").strip() or "info",
            "message": line,
            "stage_id": stage_id,
            "ts": _now_text(),
        }
    )


def _run_control_server(control_port: int, runtime: WorkerRuntime) -> None:
    if int(control_port or 0) <= 0:
        return
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", int(control_port)))
        server.listen(1)
        server.settimeout(0.5)
        while not runtime.is_cancelled():
            try:
                client, _ = server.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with client:
                client.settimeout(1.0)
                raw = b""
                while True:
                    try:
                        chunk = client.recv(4096)
                    except TimeoutError:
                        break
                    if not chunk:
                        break
                    raw += chunk
                try:
                    payload = json.loads(raw.decode("utf-8").strip() or "{}")
                except Exception:
                    payload = {}
                if str(payload.get("type", "") or "").strip().lower() == "cancel":
                    runtime.cancel_context.request_cancel()
                    runtime.emit_event(
                        {
                            "type": "stage_status",
                            "stage_id": runtime.stage_id,
                            "status": "cancelling",
                            "summary": str(payload.get("reason", "") or "cancel_requested"),
                            "metadata": {"reason": str(payload.get("reason", "") or "cancel_requested")},
                            "ts": _now_text(),
                        }
                    )
                    break


def _run_heartbeat(runtime: WorkerRuntime, interval_sec: float) -> None:
    wait_interval = max(1.0, float(interval_sec or 5.0))
    while not runtime.is_cancelled():
        runtime.emit_event(
            {
                "type": "heartbeat",
                "stage_id": runtime.stage_id,
                "ts": _now_text(),
            }
        )
        if runtime.cancel_context._cancelled.wait(timeout=wait_interval):
            break


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="QJPT task worker entry")
    parser.add_argument("--job-dir", required=True)
    parser.add_argument("--stage-id", required=True)
    parser.add_argument("--handler", required=True)
    parser.add_argument("--payload-file", required=True)
    parser.add_argument("--control-port", type=int, default=0)
    parser.add_argument("--heartbeat-interval", type=float, default=5.0)
    args = parser.parse_args(argv)

    job_dir = Path(args.job_dir).resolve()
    payload_file = Path(args.payload_file).resolve()
    stage_id = str(args.stage_id or "").strip() or "main"
    config_snapshot = _load_config_snapshot(job_dir)
    payload = _load_json(payload_file)

    handler_name = str(args.handler or "").strip()
    handler = HANDLER_REGISTRY.get(handler_name)
    if handler is None:
        _emit_log(stage_id, f"[worker] unknown handler: {handler_name}", level="error")
        _emit_event(
            {
                "type": "result",
                "stage_id": stage_id,
                "ok": False,
                "handler": handler_name,
                "error": f"unknown_worker_handler:{handler_name}",
                "ts": _now_text(),
            }
        )
        return 2

    cancel_context = CancellationContext()
    runtime = WorkerRuntime(stage_id=stage_id, cancel_context=cancel_context)
    control_thread = threading.Thread(
        target=_run_control_server,
        args=(int(args.control_port or 0), runtime),
        daemon=True,
        name=f"worker-control-{stage_id}",
    )
    heartbeat_thread = threading.Thread(
        target=_run_heartbeat,
        args=(runtime, float(args.heartbeat_interval or 5.0)),
        daemon=True,
        name=f"worker-heartbeat-{stage_id}",
    )
    control_thread.start()
    heartbeat_thread.start()

    _emit_event(
        {
            "type": "stage_status",
            "stage_id": stage_id,
            "status": "running",
            "summary": "worker_started",
            "metadata": {"handler": handler_name},
            "ts": _now_text(),
        }
    )

    def emit_log(text: str) -> None:
        runtime.emit_log(text)

    try:
        runtime.raise_if_cancelled()
        try:
            result = handler(config_snapshot, payload, emit_log, runtime)
        except TypeError:
            result = handler(config_snapshot, payload, emit_log)
        runtime.raise_if_cancelled()
        _emit_event(
            {
                "type": "result",
                "stage_id": stage_id,
                "ok": True,
                "handler": handler_name,
                "payload": _json_ready(result),
                "ts": _now_text(),
            }
        )
        cancel_context.run_cleanup()
        return 0
    except WorkerCancelledError:
        cancel_context.run_cleanup()
        _emit_event(
            {
                "type": "stage_status",
                "stage_id": stage_id,
                "status": "cancelled",
                "summary": "cancelled",
                "metadata": {"reason": "cancel_requested"},
                "ts": _now_text(),
            }
        )
        _emit_event(
            {
                "type": "result",
                "stage_id": stage_id,
                "ok": False,
                "cancelled": True,
                "handler": handler_name,
                "error": "cancelled",
                "ts": _now_text(),
            }
        )
        return 130
    except Exception as exc:  # noqa: BLE001
        detail = str(exc)
        cancel_context.run_cleanup()
        _emit_log(stage_id, f"[worker] failed: {detail}", level="error")
        _emit_event(
            {
                "type": "result",
                "stage_id": stage_id,
                "ok": False,
                "handler": handler_name,
                "error": detail,
                "ts": _now_text(),
            }
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
