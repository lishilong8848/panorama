from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from handover_log_module.api.facade import run_from_download, run_from_existing_file


def _load_json(path: str) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def main() -> None:
    parser = argparse.ArgumentParser(description="交接班日志模块本地调试入口")
    parser.add_argument("--mode", choices=["from-file", "download"], required=True, help="执行模式")
    parser.add_argument("--config", type=str, default="", help="配置文件路径，可传主配置JSON")
    parser.add_argument("--building", type=str, default="", help="楼栋，例如C楼")
    parser.add_argument("--data-file", type=str, default="", help="from-file模式使用的数据表xlsx")
    parser.add_argument("--buildings", type=str, default="", help="download模式可选，逗号分隔楼栋")
    parser.add_argument("--end-time", type=str, default="", help="结束时间 YYYY-MM-DD HH:MM:SS，可选")
    parser.add_argument("--duty-date", type=str, default="", help="班次日期 YYYY-MM-DD")
    parser.add_argument("--duty-shift", type=str, default="", help="班次 day/night")
    args = parser.parse_args()

    cfg: Dict[str, Any] = {}
    if args.config:
        cfg = _load_json(args.config)

    if args.mode == "from-file":
        if not args.building:
            raise SystemExit("--building 不能为空")
        if not args.data_file:
            raise SystemExit("--data-file 不能为空")
        result = run_from_existing_file(
            config=cfg,
            building=args.building,
            data_file=args.data_file,
            end_time=args.end_time or None,
            emit_log=print,
        )
    else:
        buildings = [x.strip() for x in str(args.buildings).split(",") if x.strip()] if args.buildings else None
        result = run_from_download(
            config=cfg,
            buildings=buildings,
            end_time=args.end_time or None,
            duty_date=args.duty_date or None,
            duty_shift=args.duty_shift or None,
            emit_log=print,
        )

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
