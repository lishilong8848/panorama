from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INTERNAL_SRC = ROOT / "内网端源码"


def _run(args: list[str], *, env: dict[str, str] | None = None) -> None:
    print("+ " + " ".join(args), flush=True)
    subprocess.run(args, cwd=str(ROOT), env=env, check=True)


def main() -> int:
    compile_targets = [
        INTERNAL_SRC / "app" / "modules" / "internal_bridge_http" / "api" / "routes.py",
        INTERNAL_SRC / "app" / "modules" / "internal_bridge_http" / "service" / "internal_bridge_http_runner.py",
        INTERNAL_SRC / "app" / "modules" / "shared_bridge" / "service" / "internal_download_browser_pool.py",
        INTERNAL_SRC / "app" / "modules" / "alarm_rule_export" / "service" / "alarm_rule_export_service.py",
        INTERNAL_SRC / "app" / "modules" / "scheduler" / "service" / "monthly_scheduler_service.py",
        INTERNAL_SRC / "app" / "modules" / "scheduler" / "service" / "daily_scheduler_service.py",
        INTERNAL_SRC / "app" / "modules" / "scheduler" / "service" / "interval_scheduler_service.py",
        INTERNAL_SRC / "app" / "modules" / "scheduler" / "service" / "handover_scheduler_manager.py",
        INTERNAL_SRC / "app" / "bootstrap" / "container.py",
        INTERNAL_SRC / "app" / "bootstrap" / "app_factory.py",
    ]
    _run([sys.executable, "-m", "py_compile", *[str(path) for path in compile_targets]])
    tests_dir = INTERNAL_SRC / "tests"
    if tests_dir.exists():
        env = dict(os.environ)
        env["PYTHONPATH"] = str(INTERNAL_SRC)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        _run([sys.executable, "-m", "pytest", str(tests_dir)], env=env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
