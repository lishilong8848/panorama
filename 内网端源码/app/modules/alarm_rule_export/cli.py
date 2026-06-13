from __future__ import annotations

import asyncio
import sys

from app.modules.alarm_rule_export.service.alarm_rule_export_service import (
    _run,
    build_parser,
)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\n已退出。")
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"运行失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
