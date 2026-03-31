from __future__ import annotations

import argparse
import copy
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.settings_loader import load_settings, validate_settings, write_settings_atomically


ROLE_PORTS = {
    "switching": 18765,
    "internal": 18766,
    "external": 18767,
}


def normalize_role_mode(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text == "hybrid":
        return "switching"
    if text in {"switching", "internal", "external"}:
        return text
    raise ValueError(f"不支持的角色: {value}")


def default_instance_root(role_mode: str) -> Path:
    return PROJECT_ROOT / ".dev_instances" / role_mode


def default_shared_root() -> Path:
    return PROJECT_ROOT / ".dev_instances" / "shared_bridge"


def instance_config_path(instance_root: Path) -> Path:
    return instance_root / "表格计算配置.json"


def build_instance_config(
    base_cfg: Dict[str, Any],
    *,
    role_mode: str,
    port: int,
    instance_root: Path,
    shared_root: Path,
) -> Dict[str, Any]:
    cfg = copy.deepcopy(base_cfg if isinstance(base_cfg, dict) else DEFAULT_CONFIG_V3)
    common = cfg.setdefault("common", {})
    paths = common.setdefault("paths", {})
    console = common.setdefault("console", {})
    deployment = common.setdefault("deployment", {})
    shared_bridge = common.setdefault("shared_bridge", {})

    instance_root = Path(instance_root)
    business_root = instance_root / "business"
    runtime_root = instance_root / ".runtime"

    paths["business_root_dir"] = str(business_root)
    paths["runtime_state_root"] = str(runtime_root)
    console["host"] = "127.0.0.1"
    console["port"] = int(port)
    console["auto_open_browser"] = False

    deployment["role_mode"] = role_mode
    deployment["node_id"] = ""
    deployment["node_label"] = ""

    if role_mode in {"internal", "external"}:
        shared_bridge["enabled"] = True
        shared_bridge["root_dir"] = str(shared_root)
    else:
        shared_bridge["enabled"] = False
        shared_bridge.setdefault("root_dir", "")

    return cfg


def prepare_instance(
    *,
    role_mode: str,
    port: int,
    base_config_path: str | None,
    instance_root: Path,
    shared_root: Path,
) -> Path:
    base_cfg = load_settings(base_config_path or None)
    cfg = build_instance_config(
        base_cfg,
        role_mode=role_mode,
        port=port,
        instance_root=instance_root,
        shared_root=shared_root,
    )
    target = instance_config_path(instance_root)
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized = validate_settings(cfg)
    write_settings_atomically(normalized, path=target)
    return target


def build_launch_command(
    *,
    config_path: Path,
    port: int,
    open_browser: bool,
) -> list[str]:
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "main.py"),
        "--config",
        str(config_path),
        "--port",
        str(int(port)),
    ]
    if not open_browser:
        cmd.append("--no-open-browser")
    return cmd


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="开发机角色实例启动器")
    parser.add_argument("--role", required=True, choices=["switching", "internal", "external"], help="目标角色")
    parser.add_argument("--port", type=int, default=0, help="覆盖实例端口")
    parser.add_argument("--base-config", default="", help="作为模板的基础配置文件路径")
    parser.add_argument("--instance-root", default="", help="实例工作目录")
    parser.add_argument("--shared-root", default="", help="共享桥接目录，internal/external 默认共用 .dev_instances/shared_bridge")
    parser.add_argument("--prepare-only", action="store_true", help="只生成实例配置，不启动程序")
    parser.add_argument("--open-browser", action="store_true", help="启动后自动打开浏览器")
    args = parser.parse_args(argv)

    role_mode = normalize_role_mode(args.role)
    port = int(args.port or ROLE_PORTS[role_mode])
    instance_root = Path(str(args.instance_root or "").strip() or default_instance_root(role_mode))
    shared_root = Path(str(args.shared_root or "").strip() or default_shared_root())
    config_path = prepare_instance(
        role_mode=role_mode,
        port=port,
        base_config_path=str(args.base_config or "").strip() or None,
        instance_root=instance_root,
        shared_root=shared_root,
    )

    print(f"[开发实例] 角色: {role_mode}")
    print(f"[开发实例] 端口: {port}")
    print(f"[开发实例] 配置: {config_path}")
    print(f"[开发实例] 业务目录: {instance_root / 'business'}")
    print(f"[开发实例] 运行时目录: {instance_root / '.runtime'}")
    if role_mode in {"internal", "external"}:
        print(f"[开发实例] 共享目录: {shared_root}")

    if args.prepare_only:
        return 0

    cmd = build_launch_command(
        config_path=config_path,
        port=port,
        open_browser=bool(args.open_browser),
    )
    print(f"[开发实例] 启动命令: {' '.join(cmd)}")
    return subprocess.call(cmd, cwd=str(PROJECT_ROOT))


if __name__ == "__main__":
    raise SystemExit(main())
