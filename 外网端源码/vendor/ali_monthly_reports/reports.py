from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .config import ReportConfig
from .feishu import build_feishu_session, download_record_attachments, fetch_bitable_records, request_tenant_token
from .staff import write_staff_roster_workbook
from .utils import (
    filter_records_by_year_month,
    generated_file_payload,
    make_unique_path,
    remove_url_query_param,
    require_config_values,
)


LogFunc = Callable[[str, str], None]


def default_log(message: str, level: str = "info") -> None:
    pass


def generate_alarm_analysis_report(
    year: str,
    month_number: int,
    config: ReportConfig,
    *,
    token_provider: Callable[[ReportConfig], str] = request_tenant_token,
    session_builder: Callable[[str], Any] = build_feishu_session,
    generate_alarm_report_func: Callable[..., dict[str, Any]] | None = None,
    log: LogFunc | None = None,
) -> dict[str, Any]:
    require_config_values(config, ["feishu_app_id", "feishu_app_secret", "ali_alarm_source_url", "ali_alarm_template_path"])
    logger = log or default_log
    download_folder = Path(config.download_folder).expanduser()
    download_folder.mkdir(parents=True, exist_ok=True)
    template_path = Path(config.ali_alarm_template_path).expanduser()
    if not template_path.exists():
        raise FileNotFoundError(f"告警分析模板不存在: {template_path}")

    output_path = make_unique_path(download_folder, f"EA118机房{year}年{month_number}月告警分析表.xlsx")
    logger(f"开始生成月度告警分析: {year}年{month_number}月", "info")
    token = token_provider(config)
    session = session_builder(token)
    if generate_alarm_report_func is None:
        from .alarm_report_generator import generate_alarm_report as generate_alarm_report_func

    result = generate_alarm_report_func(
        template=template_path,
        month=f"{year}-{month_number:02d}",
        out=output_path,
        feishu_url=config.ali_alarm_source_url,
        session=session,
    )
    logger(f"告警分析生成完成: {output_path.name}", "success")
    return {
        "action": "generated_alarm_analysis",
        "files": [generated_file_payload(output_path)],
        "message": f"月度告警分析已生成: {output_path.name}",
        **result,
    }


def generate_over_power_report(
    year: str,
    month_number: int,
    config: ReportConfig,
    *,
    token_provider: Callable[[ReportConfig], str] = request_tenant_token,
    table_records_fetcher: Callable[[ReportConfig, dict[str, str], str, LogFunc | None], list[dict[str, Any]]] = fetch_bitable_records,
    attachment_downloader: Callable[[str, Path, str], None] | None = None,
    log: LogFunc | None = None,
) -> dict[str, Any]:
    require_config_values(config, ["feishu_app_id", "feishu_app_secret", "feishu_app_token"])
    logger = log or default_log
    token = token_provider(config)
    records = table_records_fetcher(config, config.high_power_table_config, token, logger)
    filtered_records = filter_records_by_year_month(records, year, month_number)
    if not filtered_records:
        raise ValueError(f"没有找到 {year}年{month_number}月 的超功率记录")

    def attachment_matches(file_name: str) -> bool:
        upper_name = file_name.upper()
        return ("超功耗" in file_name or "超功率" in file_name) and "TOP5" not in upper_name

    download_folder = Path(config.download_folder).expanduser()
    download_folder.mkdir(parents=True, exist_ok=True)
    kwargs: dict[str, Any] = {}
    if attachment_downloader is not None:
        kwargs["attachment_downloader"] = attachment_downloader
    downloaded, errors, paths = download_record_attachments(
        filtered_records,
        token,
        download_folder,
        attachment_filter=attachment_matches,
        log=logger,
        **kwargs,
    )
    if not paths:
        raise ValueError("未匹配到月度超功率附件")
    return {
        "action": "downloaded_existing_attachment",
        "files": [generated_file_payload(path) for path in paths],
        "downloaded": downloaded,
        "errors": errors,
        "message": "月度超功率已获取完成",
    }


def generate_staff_roster_report(
    year: str,
    month_number: int,
    config: ReportConfig,
    *,
    token_provider: Callable[[ReportConfig], str] = request_tenant_token,
    session_builder: Callable[[str], Any] = build_feishu_session,
    fetch_records: Callable[[str, Any], list[dict[str, Any]]] | None = None,
    log: LogFunc | None = None,
) -> dict[str, Any]:
    require_config_values(config, ["feishu_app_id", "feishu_app_secret", "ali_staff_source_url"])
    logger = log or default_log
    token = token_provider(config)
    session = session_builder(token)
    if fetch_records is None:
        from .alarm_report_generator import fetch_feishu_records as fetch_records

    active_records = fetch_records(config.ali_staff_source_url, session=session)
    all_records = fetch_records(remove_url_query_param(config.ali_staff_source_url, "view"), session=session)
    download_folder = Path(config.download_folder).expanduser()
    download_folder.mkdir(parents=True, exist_ok=True)
    output_path = make_unique_path(download_folder, f"南通机房{year}年{month_number}月在职人员导出.xlsx")
    counts = write_staff_roster_workbook(active_records, all_records, f"{year}-{month_number:02d}", output_path)
    logger(f"在职人员导出完成: {output_path.name}", "success")
    return {
        "action": "generated_staff_roster",
        "files": [generated_file_payload(output_path)],
        "record_count": len(all_records),
        **counts,
        "message": f"在职人员导出已生成: {output_path.name}",
    }


def generate_monthly_report(report_type: str, year: str, month_number: int, config: ReportConfig, **kwargs: Any) -> dict[str, Any]:
    if report_type == "alarm_analysis":
        return generate_alarm_analysis_report(year, month_number, config, **kwargs)
    if report_type == "over_power":
        return generate_over_power_report(year, month_number, config, **kwargs)
    if report_type == "staff_roster":
        return generate_staff_roster_report(year, month_number, config, **kwargs)
    raise ValueError(f"不支持的月报类型: {report_type}")
