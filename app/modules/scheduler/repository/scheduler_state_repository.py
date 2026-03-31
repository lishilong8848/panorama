from __future__ import annotations

import json
from pathlib import Path
from typing import Dict


class SchedulerStateRepository:
    def load(self, path: Path) -> Dict[str, str]:
        default = {
            "last_success_period": "",
            "last_attempt_period": "",
            "last_run_at": "",
            "last_status": "",
            "last_error": "",
            "retry_done_period": "",
        }
        if not path.exists():
            return default
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(obj, dict):
                return default
            out = dict(default)
            for key in out:
                out[key] = str(obj.get(key, "") or "")
            return out
        except Exception:  # noqa: BLE001
            return default

    def save(self, path: Path, state: Dict[str, str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
