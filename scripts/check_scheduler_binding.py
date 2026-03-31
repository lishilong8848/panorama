from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict


ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.bootstrap.app_factory import create_app  # noqa: E402
from app.config.settings_loader import load_settings  # noqa: E402


def run_check() -> Dict[str, Any]:
    app = create_app()
    container = app.state.container

    report: Dict[str, Any] = {
        "config_path": str(container.config_path),
        "frontend_mode": str(getattr(container, "frontend_mode", "")),
        "scheduler_enabled": bool(getattr(container.scheduler, "enabled", False)),
        "scheduler_running": bool(container.scheduler.is_running()) if container.scheduler else False,
        "executor_bound_initial": bool(container.is_scheduler_executor_bound()),
        "callback_name_initial": container.scheduler_executor_name(),
        "reload_check": {},
    }

    cfg = load_settings(container.config_path)
    try:
        cfg["common"]["scheduler"]["auto_start_in_gui"] = False
    except Exception:  # noqa: BLE001
        pass
    container.reload_config(cfg)

    report["reload_check"] = {
        "executor_bound_after_reload": bool(container.is_scheduler_executor_bound()),
        "callback_name_after_reload": container.scheduler_executor_name(),
        "scheduler_running_after_reload": bool(container.scheduler.is_running()) if container.scheduler else False,
    }
    report["scheduler_start_probe"] = {"skipped": True, "reason": "no_thread_start_in_diagnostic"}
    report["scheduler_stop_probe"] = {"skipped": True, "reason": "no_thread_start_in_diagnostic"}
    report["ok"] = bool(report["executor_bound_initial"]) and bool(
        report["reload_check"].get("executor_bound_after_reload", False)
    )
    return report


def main() -> int:
    report = run_check()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not bool(report.get("ok", False)):
        print("[调度体检] 失败: 调度执行器绑定异常，请检查 app_factory/container 回调绑定逻辑。")
        return 2
    print("[调度体检] 通过: 调度执行器绑定正常（含 reload 后）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
