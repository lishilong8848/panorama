from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


def _text(value: Any) -> str:
    return str(value or "").strip()


def _date_value_for_compare(client: Any, date_text: str) -> Any:
    converter = getattr(client, "_to_feishu_date", None)
    if callable(converter):
        try:
            return converter(date_text)
        except Exception:  # noqa: BLE001
            return date_text
    return date_text


def _date_field_matches(value: Any, *, date_text: str, target_value: Any) -> bool:
    if value is None:
        return False
    value_text = _text(value)
    target_text = _text(target_value)
    if value_text and target_text and value_text == target_text:
        return True
    if value_text and value_text == _text(date_text):
        return True
    if isinstance(value, (int, float)) and isinstance(target_value, (int, float)):
        return int(value) == int(target_value)
    # 日期字符串容错: 允许 "YYYY-MM-DD HH:MM:SS" 或 ISO 字符串前缀匹配
    if len(value_text) >= 10 and value_text[:10] == _text(date_text):
        return True
    return False


def _collect_calc_record_ids_for_replace(
    *,
    client: Any,
    calc_table_id: str,
    building: str,
    date_text: str,
) -> List[str]:
    records = client.list_records(table_id=calc_table_id, page_size=500, max_records=0)
    target_date_value = _date_value_for_compare(client, date_text)
    record_ids: List[str] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        record_id = _text(item.get("record_id"))
        fields = item.get("fields", {})
        if not record_id or not isinstance(fields, dict):
            continue
        if _text(fields.get("楼栋")) != _text(building):
            continue
        if not _date_field_matches(fields.get("日期"), date_text=date_text, target_value=target_date_value):
            continue
        record_ids.append(record_id)
    return record_ids


def _collect_attachment_record_ids_for_replace(
    *,
    client: Any,
    attachment_table_id: str,
    report_type: str,
    building: str,
    date_text: str,
) -> List[str]:
    records = client.list_records(table_id=attachment_table_id, page_size=500, max_records=0)
    target_date_value = _date_value_for_compare(client, date_text)
    record_ids: List[str] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        record_id = _text(item.get("record_id"))
        fields = item.get("fields", {})
        if not record_id or not isinstance(fields, dict):
            continue
        if _text(fields.get("类型")) != _text(report_type):
            continue
        if _text(fields.get("楼栋")) != _text(building):
            continue
        if not _date_field_matches(fields.get("日期"), date_text=date_text, target_value=target_date_value):
            continue
        record_ids.append(record_id)
    return record_ids


def upload_results_to_feishu(
    results: List[Any],
    config: Dict[str, Any],
    *,
    resolve_upload_date_from_runtime: Callable[[Dict[str, Any]], str | None],
    client_factory: Callable[..., Any],
    date_override_by_source: Optional[Dict[str, str]] = None,
    log_feature: str = "月报上传",
    emit_log: Callable[[str], None] = print,
) -> None:
    if "feishu" not in config or not isinstance(config["feishu"], dict):
        raise ValueError("配置错误: feishu 缺失，请在JSON中配置。")
    feishu_cfg = config["feishu"]
    if "enable_upload" not in feishu_cfg:
        raise ValueError("配置错误: feishu.enable_upload 缺失，请在JSON中配置。")
    if not feishu_cfg["enable_upload"]:
        emit_log("[飞书] 已关闭上传。")
        return

    app_id = str(feishu_cfg["app_id"]).strip()
    app_secret = str(feishu_cfg["app_secret"]).strip()
    app_token = str(feishu_cfg["app_token"]).strip()
    calc_table_id = str(feishu_cfg["calc_table_id"]).strip()
    attachment_table_id = str(feishu_cfg["attachment_table_id"]).strip()

    required_values = {
        "app_id": app_id,
        "app_secret": app_secret,
        "app_token": app_token,
        "calc_table_id": calc_table_id,
        "attachment_table_id": attachment_table_id,
    }
    missing_keys = [k for k, v in required_values.items() if not v]
    if missing_keys:
        raise ValueError(f"飞书配置缺失: {missing_keys}")

    request_retry_count = int(feishu_cfg.get("request_retry_count", 3))
    request_retry_interval_sec = float(feishu_cfg.get("request_retry_interval_sec", 1))
    if request_retry_count < 0:
        raise ValueError("配置错误: feishu.request_retry_count 必须大于等于0")
    if request_retry_interval_sec < 0:
        raise ValueError("配置错误: feishu.request_retry_interval_sec 必须大于等于0")

    client = client_factory(
        app_id=app_id,
        app_secret=app_secret,
        app_token=app_token,
        calc_table_id=calc_table_id,
        attachment_table_id=attachment_table_id,
        date_field_mode=str(feishu_cfg["date_field_mode"]).strip(),
        date_field_day=int(feishu_cfg["date_field_day"]),
        date_tz_offset_hours=int(feishu_cfg["date_tz_offset_hours"]),
        timeout=int(feishu_cfg["timeout"]),
        request_retry_count=request_retry_count,
        request_retry_interval_sec=request_retry_interval_sec,
    )

    report_type = feishu_cfg["report_type"]
    skip_zero_records = bool(feishu_cfg["skip_zero_records"])
    date_override = resolve_upload_date_from_runtime(config)

    normalized_source_dates: Dict[str, str] = {}
    if date_override_by_source:
        for source, day_text in date_override_by_source.items():
            source_path = str(Path(source).resolve())
            normalized_source_dates[source_path] = str(day_text).strip()

    for result in results:
        source_key = str(Path(result.source_file).resolve())
        upload_date_text = normalized_source_dates.get(source_key, "") or date_override or result.month
        building_text = str(result.building or "-").strip() or "-"
        file_text = str(result.source_file or "-").strip() or "-"
        date_text = str(upload_date_text or "-").strip() or "-"
        pue_value = result.values.get("PUE")
        pue_text = "-" if pue_value is None else f"{float(pue_value):.3f}"
        emit_log(f"[飞书上传] 楼栋={building_text} 日期={date_text} PUE={pue_text}")

        try:
            calc_delete_ids = _collect_calc_record_ids_for_replace(
                client=client,
                calc_table_id=calc_table_id,
                building=result.building,
                date_text=upload_date_text,
            )
            emit_log(
                f"[飞书上传][覆盖] 已读取旧计算记录: 楼栋={building_text}, 日期={date_text}, count={len(calc_delete_ids)}"
            )
            if calc_delete_ids:
                deleted_calc = client.batch_delete_records(
                    table_id=calc_table_id,
                    record_ids=calc_delete_ids,
                    batch_size=500,
                )
                emit_log(
                    f"[飞书上传][覆盖] 已删除旧计算记录: 楼栋={building_text}, 日期={date_text}, count={int(deleted_calc or 0)}"
                )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书旧计算记录覆盖删除 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            attachment_delete_ids = _collect_attachment_record_ids_for_replace(
                client=client,
                attachment_table_id=attachment_table_id,
                report_type=report_type,
                building=result.building,
                date_text=upload_date_text,
            )
            emit_log(
                f"[飞书上传][覆盖] 已读取旧附件记录: 楼栋={building_text}, 日期={date_text}, count={len(attachment_delete_ids)}"
            )
            if attachment_delete_ids:
                deleted_attachment = client.batch_delete_records(
                    table_id=attachment_table_id,
                    record_ids=attachment_delete_ids,
                    batch_size=500,
                )
                emit_log(
                    f"[飞书上传][覆盖] 已删除旧附件记录: 楼栋={building_text}, 日期={date_text}, count={int(deleted_attachment or 0)}"
                )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书旧附件记录覆盖删除 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            client.upload_calc_records(
                result.records,
                skip_zero_records=skip_zero_records,
                date_override=upload_date_text,
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书计算记录上传 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            file_token = client.upload_attachment(result.source_file)
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书附件上传 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        try:
            client.upload_attachment_record(
                report_type=report_type,
                building=result.building,
                date_text=upload_date_text,
                attachment_tokens=[file_token],
            )
        except Exception as exc:  # noqa: BLE001
            emit_log(
                f"[文件流程失败] 功能={log_feature} 阶段=飞书附件记录写入 楼栋={building_text} "
                f"文件={file_text} 日期={date_text} 错误={exc}"
            )
            raise

        emit_log(
            f"[文件上传成功] 功能={log_feature} 阶段=飞书上传完成 楼栋={building_text} "
            f"文件={file_text} 日期={date_text} 详情=已按覆盖策略写入新记录"
        )
