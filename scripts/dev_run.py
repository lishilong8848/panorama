from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Sequence, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run_step(name: str, cmd: Sequence[str], cwd: Path) -> Tuple[bool, int]:
    print(f"\n[DEV-RUN] 开始: {name}")
    print(f"[DEV-RUN] 命令: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(cwd), check=False)
    ok = proc.returncode == 0
    print(f"[DEV-RUN] {'通过' if ok else '失败'}: {name} (code={proc.returncode})")
    return ok, int(proc.returncode)


def main() -> int:
    checks: List[Tuple[str, List[str], Path]] = []
    checks.append(
        (
            "Python 语法检查 (compileall)",
            [
                sys.executable,
                "-m",
                "compileall",
                "-q",
                "app",
                "scripts",
                "handover_log_module",
                "main.py",
                "下载动环表格.py",
                "表格计算部分代码.py",
                "pipeline_utils.py",
                "wifi_switcher.py",
            ],
            PROJECT_ROOT,
        )
    )

    if importlib.util.find_spec("pytest") is not None:
        checks.append(("Pytest 单元/集成测试", [sys.executable, "-m", "pytest", "-q"], PROJECT_ROOT))
    else:
        print("[DEV-RUN] 当前环境未安装 pytest，跳过单元/集成测试。")

    checks.append(("调度执行器绑定体检", [sys.executable, "scripts/check_scheduler_binding.py"], PROJECT_ROOT))
    checks.append(("文本编码可疑片段检查", [sys.executable, "scripts/check_text_encoding.py"], PROJECT_ROOT))

    node_bin = shutil.which("node")
    if node_bin:
        checks.append(("前端语法检查", [node_bin, "--check", "web/frontend/src/app.js"], PROJECT_ROOT))
    else:
        print("[DEV-RUN] 未检测到 node，跳过前端语法检查。")

    failed: List[Tuple[str, int]] = []
    for name, cmd, cwd in checks:
        ok, code = _run_step(name, cmd, cwd)
        if not ok:
            failed.append((name, code))

    if failed:
        print("\n[DEV-RUN] 失败项:")
        for name, code in failed:
            print(f"- {name}: code={code}")
        return 1

    print("\n[DEV-RUN] 所有检查通过。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
