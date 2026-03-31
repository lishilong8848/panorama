from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config.config_schema_v3 import DEFAULT_CONFIG_V3  # noqa: E402
from app.config.settings_loader import save_settings  # noqa: E402


def test_save_settings_writes_atomically_and_keeps_backup(tmp_path: Path) -> None:
    config_path = tmp_path / "表格计算配置.json"
    initial = copy.deepcopy(DEFAULT_CONFIG_V3)
    initial["common"]["paths"]["business_root_dir"] = r"D:\FirstRoot"
    config_path.write_text(json.dumps(initial, ensure_ascii=False, indent=2), encoding="utf-8-sig")

    updated = copy.deepcopy(DEFAULT_CONFIG_V3)
    updated["common"]["paths"]["business_root_dir"] = r"D:\SecondRoot"

    saved = save_settings(updated, config_path)

    assert saved["common"]["paths"]["business_root_dir"] == r"D:\SecondRoot"
    reloaded = json.loads(config_path.read_text(encoding="utf-8-sig"))
    assert reloaded["common"]["paths"]["business_root_dir"] == r"D:\SecondRoot"
    backups = list(tmp_path.glob("表格计算配置.backup.*.json"))
    assert backups, "expected at least one backup file"
    assert not (tmp_path / "表格计算配置.json.tmp").exists()
