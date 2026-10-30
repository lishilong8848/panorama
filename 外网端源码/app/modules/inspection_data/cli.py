"""Inspect/create the inspection schema without changing business records."""
from __future__ import annotations

import argparse
import json

from app.config.config_adapter import adapt_runtime_config
from app.config.settings_loader import load_pipeline_config
from app.modules.inspection_data.service.inspection_data_upload_service import InspectionDataUploadService


def main():
    parser = argparse.ArgumentParser(description="巡检多维表字段预检/初始化（不修改业务记录）")
    parser.add_argument("--config", default="表格计算配置.json")
    parser.add_argument("--apply-schema", action="store_true", help="仅创建缺少的字段，不删除字段或记录")
    args = parser.parse_args()
    service = InspectionDataUploadService(adapt_runtime_config(load_pipeline_config(args.config)))
    print(json.dumps(service.ensure_schema(apply=args.apply_schema), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
