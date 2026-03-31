from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

from app.config.config_merge_guard import build_repaired_user_config
from app.config.settings_loader import save_settings


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _summarize_changes(old_cfg: dict, repaired_cfg: dict) -> dict:
    summary = {
        "monthly_buildings": len(repaired_cfg.get("features", {}).get("monthly_report", {}).get("buildings", []) or []),
        "monthly_sites": len(repaired_cfg.get("features", {}).get("monthly_report", {}).get("sites", []) or []),
        "sheet_rules": len(repaired_cfg.get("features", {}).get("sheet_import", {}).get("sheet_rules", []) or []),
        "business_root_dir": str(
            repaired_cfg.get("common", {}).get("paths", {}).get("business_root_dir", "") or ""
        ).strip(),
        "source_business_root_dir": str(
            old_cfg.get("common", {}).get("paths", {}).get("business_root_dir", "")
            or old_cfg.get("common", {}).get("paths", {}).get("download_save_dir", "")
            or old_cfg.get("common", {}).get("paths", {}).get("excel_dir", "")
            or ""
        ).strip(),
    }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="安全修复用户配置到最新结构")
    parser.add_argument("--source-old-config", required=True, help="旧配置文件路径（值来源）")
    parser.add_argument("--target-config", required=True, help="待修复配置文件路径")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不写入")
    parser.add_argument("--write", action="store_true", help="执行写入")
    args = parser.parse_args()

    source_path = Path(args.source_old_config).resolve()
    target_path = Path(args.target_config).resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"旧配置不存在: {source_path}")
    if not target_path.exists():
        raise FileNotFoundError(f"目标配置不存在: {target_path}")

    source_cfg = _read_json(source_path)
    target_cfg = _read_json(target_path)
    result = build_repaired_user_config(source_cfg, target_cfg)
    summary = _summarize_changes(source_cfg, result.merged)

    print("[Repair] 旧配置来源:", source_path)
    print("[Repair] 当前目标:", target_path)
    print("[Repair] 恢复候选路径数:", len(result.suspicious_loss_paths))
    if result.suspicious_loss_paths:
        print("[Repair] 检测到的配置丢失路径:", "、".join(result.suspicious_loss_paths[:12]))
    print(
        "[Repair] 摘要:"
        f" business_root_dir={summary['business_root_dir']},"
        f" monthly_buildings={summary['monthly_buildings']},"
        f" monthly_sites={summary['monthly_sites']},"
        f" sheet_rules={summary['sheet_rules']}"
    )

    if args.dry_run or not args.write:
        print("[Repair] dry-run 模式，未写入。")
        return

    repair_backup = target_path.with_name(
        f"{target_path.stem}.repair_backup.{time.strftime('%Y%m%d-%H%M%S')}{target_path.suffix}"
    )
    shutil.copy2(target_path, repair_backup)
    saved = save_settings(result.merged, target_path)
    print("[Repair] 已写入修复后的配置。")
    print("[Repair] repair 备份文件:", repair_backup)
    print(
        "[Repair] 最终摘要:"
        f" business_root_dir={saved.get('common', {}).get('paths', {}).get('business_root_dir', '')},"
        f" monthly_buildings={len(saved.get('features', {}).get('monthly_report', {}).get('buildings', []) or [])},"
        f" monthly_sites={len(saved.get('features', {}).get('monthly_report', {}).get('sites', []) or [])},"
        f" sheet_rules={len(saved.get('features', {}).get('sheet_import', {}).get('sheet_rules', []) or [])}"
    )


if __name__ == "__main__":
    main()
