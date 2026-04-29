from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root


def _json_ready(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


class TaskEngineStore:
    def __init__(
        self,
        *,
        runtime_config: dict[str, Any] | None = None,
        app_dir: Path | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self.runtime_root = resolve_runtime_state_root(runtime_config=runtime_config, app_dir=app_dir)
        self.root = self.runtime_root / "task_engine"
        self.jobs_root = self.root / "jobs"
        self.resources_root = self.root / "resources"
        self.workers_root = self.root / "workers"
        self.jobs_root.mkdir(parents=True, exist_ok=True)
        self.resources_root.mkdir(parents=True, exist_ok=True)
        self.workers_root.mkdir(parents=True, exist_ok=True)
        (self.resources_root / "queues").mkdir(parents=True, exist_ok=True)

    def _job_dir(self, job_id: str) -> Path:
        path = self.jobs_root / str(job_id or "").strip()
        path.mkdir(parents=True, exist_ok=True)
        (path / "stages").mkdir(parents=True, exist_ok=True)
        return path

    def resolve_job_dir(self, job_id: str) -> Path:
        return self._job_dir(job_id)

    def resolve_stage_payload_path(self, job_id: str, stage_id: str) -> Path:
        return self._job_dir(job_id) / "stages" / f"{str(stage_id or '').strip()}.input.json"

    def resolve_stage_result_path(self, job_id: str, stage_id: str) -> Path:
        return self._job_dir(job_id) / "stages" / f"{str(stage_id or '').strip()}.result.json"

    @staticmethod
    def _write_json(path: Path, payload: Any) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(f"{path.suffix}.tmp")
        tmp_path.write_text(
            json.dumps(_json_ready(payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(path)

    def persist_job(self, job_payload: dict[str, Any]) -> None:
        job_id = str(job_payload.get("job_id", "")).strip()
        if not job_id:
            return
        with self._lock:
            self._write_json(self._job_dir(job_id) / "job.json", job_payload)

    def persist_stage(self, job_id: str, stage_payload: dict[str, Any]) -> None:
        target_job_id = str(job_id or "").strip()
        stage_id = str(stage_payload.get("stage_id", "")).strip()
        if not target_job_id or not stage_id:
            return
        with self._lock:
            self._write_json(self._job_dir(target_job_id) / "stages" / f"{stage_id}.json", stage_payload)

    def persist_stage_payload(self, job_id: str, stage_id: str, payload: dict[str, Any]) -> Path:
        target_job_id = str(job_id or "").strip()
        target_stage_id = str(stage_id or "").strip()
        if not target_job_id or not target_stage_id:
            raise ValueError("job_id and stage_id are required")
        path = self.resolve_stage_payload_path(target_job_id, target_stage_id)
        with self._lock:
            self._write_json(path, payload)
        return path

    def persist_config_snapshot(self, job_id: str, config_snapshot: dict[str, Any] | None) -> None:
        target_job_id = str(job_id or "").strip()
        if not target_job_id or not isinstance(config_snapshot, dict):
            return
        with self._lock:
            self._write_json(self._job_dir(target_job_id) / "config_snapshot.json", config_snapshot)

    def append_log(self, job_id: str, entry: dict[str, Any]) -> None:
        target_job_id = str(job_id or "").strip()
        if not target_job_id:
            return
        with self._lock:
            log_path = self._job_dir(target_job_id) / "logs.ndjson"
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(_json_ready(entry), ensure_ascii=False))
                handle.write("\n")

    def persist_resource_snapshot(self, snapshot: dict[str, Any]) -> None:
        with self._lock:
            self._write_json(self.resources_root / "resources.json", snapshot)

    def persist_worker_snapshot(self, job_id: str, stage_id: str, snapshot: dict[str, Any]) -> None:
        target_job_id = str(job_id or "").strip()
        target_stage_id = str(stage_id or "").strip()
        if not target_job_id or not target_stage_id:
            return
        worker_path = self.workers_root / f"{target_job_id}-{target_stage_id}.json"
        with self._lock:
            self._write_json(worker_path, snapshot)
