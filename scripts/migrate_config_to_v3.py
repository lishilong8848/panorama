from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config.config_adapter import ensure_v3_config  # noqa: E402
from pipeline_utils import resolve_config_path  # noqa: E402


def migrate(path: Path, write: bool = True) -> Path:
    with path.open("r", encoding="utf-8-sig") as f:
        raw = json.load(f)

    migrated = ensure_v3_config(raw)
    migrated["version"] = 3

    if write:
        backup = path.with_name(f"{path.name}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak")
        backup.write_text(path.read_text(encoding="utf-8-sig"), encoding="utf-8-sig")
        path.write_text(json.dumps(migrated, ensure_ascii=False, indent=2), encoding="utf-8-sig")
        return backup
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="将旧配置迁移为 v3（common + features）")
    parser.add_argument("--config", default="", help="配置文件路径，默认自动定位 表格计算配置.json")
    parser.add_argument("--dry-run", action="store_true", help="仅校验迁移，不写回文件")
    args = parser.parse_args()

    cfg_path = resolve_config_path(args.config or None)
    backup = migrate(cfg_path, write=not args.dry_run)

    if args.dry_run:
        print(f"[迁移预检] 成功: {cfg_path}")
    else:
        print(f"[迁移完成] 配置已升级为v3: {cfg_path}")
        print(f"[迁移完成] 旧配置备份: {backup}")
        print("[迁移完成] 告警数据库配置已公共化到 common.alarm_db")


if __name__ == "__main__":
    main()
