from __future__ import annotations

import json
import math
import re
import time
import warnings
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import openpyxl
import requests
from openpyxl.utils.datetime import from_excel
from app.modules.report_pipeline.service.calc_io_runtime import (
    build_results_from_file_items as build_results_from_file_items_runtime,
    build_results_from_mapping as build_results_from_mapping_runtime,
    discover_latest_files as discover_latest_files_runtime,
    load_config as load_config_runtime,
    resolve_config_path as resolve_config_path_runtime,
)
from app.modules.report_pipeline.service.calc_run_runtime import (
    run_with_config as run_with_config_runtime,
    run_with_explicit_file_items as run_with_explicit_file_items_runtime,
    run_with_explicit_files as run_with_explicit_files_runtime,
    save_results as save_results_runtime,
)
from app.modules.report_pipeline.service.calc_source_runtime import (
    apply_building_source_overrides as apply_building_source_overrides_runtime,
    extract_building as extract_building_runtime,
    extract_month as extract_month_runtime,
    extract_row_sources as extract_row_sources_runtime,
    locate_stat_columns as locate_stat_columns_runtime,
)
from app.modules.sheet_import.core.field_value_converter import (
    convert_value_for_field as convert_value_for_field_runtime,
    lookup_option_name as lookup_option_name_runtime,
    normalize_field_name as normalize_field_name_runtime,
    parse_timestamp_ms as parse_timestamp_ms_runtime,
    split_multi_values as split_multi_values_runtime,
)
from app.modules.sheet_import.core.image_anchor_mapper import (
    auto_pick_attachment_field as auto_pick_attachment_field_runtime,
    build_explicit_image_mapping as build_explicit_image_mapping_runtime,
    resolve_attachment_target_field as resolve_attachment_target_field_runtime,
    select_tokens_by_strategy as select_tokens_by_strategy_runtime,
)
from app.modules.sheet_import.core.sheet_rules import (
    normalize_sheet_rules as normalize_sheet_rules_runtime,
)
from app.modules.sheet_import.service.image_upload_runtime import (
    apply_sheet_images_to_row_payloads as apply_sheet_images_to_row_payloads_runtime,
    parse_image_import_config as parse_image_import_config_runtime,
)
from app.modules.sheet_import.service.row_payload_runtime import (
    prepare_row_payloads_for_table as prepare_row_payloads_for_table_runtime,
    prepare_rows_for_table as prepare_rows_for_table_runtime,
)
from app.modules.sheet_import.service.workbook_import_runtime import (
    import_workbook_sheets_to_feishu as import_workbook_sheets_to_feishu_runtime,
)
from app.modules.sheet_import.repository.workbook_repository import (
    build_raw_header_name_by_column as build_raw_header_name_by_column_runtime,
    extract_header_pairs as extract_header_pairs_runtime,
    extract_rows_with_row_index as extract_rows_with_row_index_runtime,
    extract_sheet_images_by_anchor as extract_sheet_images_by_anchor_runtime,
    image_extension_and_mime as image_extension_and_mime_runtime,
    safe_file_token as safe_file_token_runtime,
)
from app.modules.report_pipeline.core.metrics_math import (
    date_from_datetime_text as date_from_datetime_text_runtime,
    date_text_to_timestamp_ms as date_text_to_timestamp_ms_runtime,
    month_from_datetime_text as month_from_datetime_text_runtime,
    month_to_timestamp_ms as month_to_timestamp_ms_runtime,
    norm_text as norm_text_runtime,
    resolve_upload_date_from_runtime as resolve_upload_date_from_runtime_runtime,
    round6 as round6_runtime,
    round_metric_value as round_metric_value_runtime,
    safe_div as safe_div_runtime,
    to_float as to_float_runtime,
)
from app.modules.report_pipeline.core.metrics_rules import (
    BASE_RULES,
    DERIVED_CATEGORY_HINT,
    DERIVED_RULES,
    FEISHU_DIMENSION_MAPPING,
    canonical_metric_name,
)
from app.modules.report_pipeline.service.feishu_upload_runtime import (
    upload_results_to_feishu as upload_results_to_feishu_runtime,
)
from app.modules.feishu.service.bitable_client_runtime import (
    FeishuBitableClient as FeishuBitableClientRuntime,
)
from pipeline_utils import resolve_config_path as resolve_pipeline_config_path


HEADER_ROW = 2
DATA_START_ROW = 4
TYPE_COL = 2      # B
CATEGORY_COL = 3  # C
ITEM_COL = 4      # D
DATA_START_COL = 5  # E

DEFAULT_CONFIG_FILENAME = "表格计算配置.json"
PUE_FORMULA_DEFAULT = "(市电总用电量+柴发总发电量-光伏发电量)/IT总用电量"
PUE_FORMULA_B_BUILDING = "(变压器总进线+柴发总发电量-光伏发电量)/IT总用电量"


def _norm_text(value: Any) -> str:
    return norm_text_runtime(value)


def _to_float(value: Any) -> Optional[float]:
    return to_float_runtime(value)


def _round6(value: float) -> float:
    return round6_runtime(value)


def _round_metric_value(metric: str, value: float) -> float:
    return round_metric_value_runtime(metric, value)


def _safe_div(numerator: float, denominator: float) -> float:
    return safe_div_runtime(numerator, denominator)


def date_text_to_timestamp_ms(date_text: str, default_day: int = 1, tz_offset_hours: int = 8) -> int:
    return date_text_to_timestamp_ms_runtime(date_text, default_day=default_day, tz_offset_hours=tz_offset_hours)


def month_to_timestamp_ms(month_text: str, day: int = 1, tz_offset_hours: int = 8) -> int:
    return month_to_timestamp_ms_runtime(month_text, day=day, tz_offset_hours=tz_offset_hours)


def _month_from_datetime_text(value: Any) -> str:
    return month_from_datetime_text_runtime(value)


def _date_from_datetime_text(value: Any) -> str:
    return date_from_datetime_text_runtime(value)


def _resolve_upload_date_from_runtime(config: Dict[str, Any]) -> str:
    return resolve_upload_date_from_runtime_runtime(config)


@dataclass
class RowSource:
    row_index: int
    type_name: str
    category_name: str
    item_name: str
    max_value: Optional[float]
    min_value: Optional[float]
    avg_value: Optional[float]
    data_values: List[float]

    @property
    def data_sum(self) -> Optional[float]:
        if not self.data_values:
            return None
        return float(sum(self.data_values))


@dataclass
class CalculatedRecord:
    type_name: str
    category_name: str
    item_name: str
    building: str
    month: str
    calc_method: str
    value: float

    def to_feishu_fields(
        self,
        date_value: Optional[Any] = None,
        type_name: Optional[str] = None,
        category_name: Optional[str] = None,
        item_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        if date_value is None:
            date_value = self.month
        return {
            "类型": type_name or self.type_name,
            "分类": category_name or self.category_name,
            "项目": item_name or self.item_name,
            "楼栋": self.building,
            "日期": date_value,
            "计算方式": self.calc_method,
            "用电量": self.value,
        }


@dataclass
class CalculationResult:
    source_file: str
    building: str
    month: str
    values: Dict[str, float]
    records: List[CalculatedRecord]
    missing_metrics: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_file": self.source_file,
            "building": self.building,
            "month": self.month,
            "values": self.values,
            "records": [asdict(record) for record in self.records],
            "missing_metrics": self.missing_metrics,
        }

def _locate_stat_columns(ws: openpyxl.worksheet.worksheet.Worksheet) -> Dict[str, int]:
    return locate_stat_columns_runtime(
        ws,
        header_row=HEADER_ROW,
        data_start_col=DATA_START_COL,
        max_label=_norm_text("最大值"),
        min_label=_norm_text("最小值"),
        avg_label=_norm_text("平均值"),
        norm_text=_norm_text,
    )


def _extract_month(ws: openpyxl.worksheet.worksheet.Worksheet, max_col: int) -> str:
    return extract_month_runtime(
        ws,
        max_col,
        header_row=HEADER_ROW,
        data_start_col=DATA_START_COL,
        month_pattern=re.compile(r"(\d{4})[-/年](\d{1,2})"),
    )


def _extract_building(file_path: str) -> str:
    return extract_building_runtime(file_path)


def _extract_row_sources(
    ws: openpyxl.worksheet.worksheet.Worksheet,
    stat_cols: Dict[str, int],
) -> Tuple[Dict[str, RowSource], str, str]:
    return extract_row_sources_runtime(
        ws,
        stat_cols,
        data_start_col=DATA_START_COL,
        data_start_row=DATA_START_ROW,
        type_col=TYPE_COL,
        category_col=CATEGORY_COL,
        item_col=ITEM_COL,
        to_float=_to_float,
        canonical_metric_name=canonical_metric_name,
        row_source_factory=RowSource,
    )


def _calc_base_value(source: Optional[RowSource], method: str) -> Optional[float]:
    if source is None:
        return None
    if method == "平均值":
        return source.avg_value
    if method == "最大值-最小值":
        if source.max_value is None or source.min_value is None:
            return None
        return source.max_value - source.min_value
    if method == "所有数据求和":
        return source.data_sum
    raise ValueError(f"未知计算方式: {method}")


def _sum_values(values: Dict[str, float], metrics: Iterable[str]) -> float:
    return sum(values.get(metric, 0.0) for metric in metrics)


def _is_b_building(building: Any) -> bool:
    text = str(building or "").strip().upper()
    if not text:
        return False
    return text.startswith("B")


def _resolve_derived_calc_method(metric: str, default_method: str, building: Any) -> str:
    if metric == "PUE" and _is_b_building(building):
        return PUE_FORMULA_B_BUILDING
    return default_method


def _compute_derived(values: Dict[str, float], building: Any = "") -> Dict[str, float]:
    derived: Dict[str, float] = {}

    derived["制冷单元辅助设备用电"] = _round6(
        values.get("冷却单元总用电量", 0.0)
        - values.get("冷却塔功率", 0.0)
        - values.get("一次泵功率", 0.0)
        - values.get("冷却泵功率", 0.0)
    )

    it_total = values.get("IT总用电量", 0.0)
    if _is_b_building(building):
        pue_numerator = (
            values.get("变压器总进线", 0.0)
            + values.get("柴发总发电量", 0.0)
            - values.get("光伏发电量", 0.0)
        )
    else:
        pue_numerator = (
            values.get("市电总用电量", 0.0)
            + values.get("柴发总发电量", 0.0)
            - values.get("光伏发电量", 0.0)
        )
    derived["PUE"] = _round6(_safe_div(pue_numerator, it_total))

    plf_numerator = _sum_values(values, ["变压器损耗", "UPS损耗", "HVDC损耗"])
    derived["PLF"] = _round6(_safe_div(plf_numerator, it_total))

    clf_numerator = _sum_values(
        values,
        [
            "冷水机组总用电量",
            "冷却单元总用电量",
            "二次泵总用电量",
            "包间精密空调总用电量",
            "包间恒湿机总用电量",
            "其他精密空调总用电量",
        ],
    )
    derived["CLF"] = _round6(_safe_div(clf_numerator, it_total))

    wclf_numerator = _sum_values(values, ["冷水机组总用电量", "冷却单元总用电量", "二次泵总用电量"])
    derived["WCLF（冷源）"] = _round6(_safe_div(wclf_numerator, it_total))

    aclf_numerator = _sum_values(values, ["包间精密空调总用电量", "包间恒湿机总用电量", "其他精密空调总用电量"])
    derived["ACLF（末端）"] = _round6(_safe_div(aclf_numerator, it_total))

    return derived


def _pick_context(
    metric: str,
    source_map: Dict[str, RowSource],
    default_type: str,
    default_category: str,
) -> Tuple[str, str]:
    source = source_map.get(metric)
    if source is not None:
        type_name = source.type_name or default_type
        category_name = source.category_name or default_category
        return type_name, category_name

    if metric in DERIVED_CATEGORY_HINT:
        return default_type, DERIVED_CATEGORY_HINT[metric]
    return default_type, default_category


def calculate_monthly_report(file_path: str, building_override: Optional[str] = None) -> CalculationResult:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Workbook contains no default style, apply openpyxl's default",
            category=UserWarning,
            module=r"openpyxl\.styles\.stylesheet",
        )
        wb = openpyxl.load_workbook(file_path, data_only=True)
    ws = wb.active

    stat_cols = _locate_stat_columns(ws)
    month = _extract_month(ws, stat_cols["max"])
    building = building_override or _extract_building(file_path)

    source_map, default_type, default_category = _extract_row_sources(ws, stat_cols)
    source_map = apply_building_source_overrides_runtime(
        building=building,
        source_map=source_map,
        canonical_metric_name=canonical_metric_name,
    )
    it_source = source_map.get("IT负载率")
    if it_source is not None:
        if it_source.type_name:
            default_type = it_source.type_name
        if it_source.category_name:
            default_category = it_source.category_name
    if not default_type:
        default_type = "数据计算"
    if not default_category:
        default_category = "未分类"

    values: Dict[str, float] = {}
    records: List[CalculatedRecord] = []
    missing_metrics: List[str] = []

    for metric, method in BASE_RULES:
        source = source_map.get(metric)
        raw_value = _calc_base_value(source, method)
        if raw_value is None:
            missing_metrics.append(metric)
            raw_value = 0.0
        value = _round6(raw_value)
        values[metric] = value
        type_name, category_name = _pick_context(metric, source_map, default_type, default_category)
        records.append(
            CalculatedRecord(
                type_name=type_name,
                category_name=category_name,
                item_name=metric,
                building=building,
                month=month,
                calc_method=method,
                value=value,
            )
        )

    derived_values = _compute_derived(values, building=building)
    for metric, method in DERIVED_RULES:
        method = _resolve_derived_calc_method(metric, method, building)
        value = _round_metric_value(metric, derived_values.get(metric, 0.0))
        values[metric] = value
        type_name, category_name = _pick_context(metric, source_map, default_type, default_category)
        records.append(
            CalculatedRecord(
                type_name=type_name,
                category_name=category_name,
                item_name=metric,
                building=building,
                month=month,
                calc_method=method,
                value=value,
            )
        )

    result = CalculationResult(
        source_file=file_path,
        building=building,
        month=month,
        values=values,
        records=records,
        missing_metrics=missing_metrics,
    )
    wb.close()
    return result


class FeishuBitableClient(FeishuBitableClientRuntime):
    def __init__(
        self,
        app_id: str,
        app_secret: str,
        app_token: str,
        calc_table_id: str,
        attachment_table_id: str,
        date_field_mode: str = "timestamp",
        date_field_day: int = 1,
        date_tz_offset_hours: int = 8,
        timeout: int = 30,
        request_retry_count: int = 3,
        request_retry_interval_sec: float = 1.0,
    ) -> None:
        super().__init__(
            app_id=app_id,
            app_secret=app_secret,
            app_token=app_token,
            calc_table_id=calc_table_id,
            attachment_table_id=attachment_table_id,
            date_field_mode=date_field_mode,
            date_field_day=date_field_day,
            date_tz_offset_hours=date_tz_offset_hours,
            timeout=timeout,
            request_retry_count=request_retry_count,
            request_retry_interval_sec=request_retry_interval_sec,
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=canonical_metric_name,
            dimension_mapping=FEISHU_DIMENSION_MAPPING,
        )


@dataclass
class RowPayload:
    row_index: int
    fields: Dict[str, Any]


@dataclass
class ImagePlacement:
    row_index: int
    column_index: int
    image_index: int
    file_name: str
    mime_type: str
    content: bytes


def _cell_to_feishu_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    return value


def _extract_header_pairs(ws: openpyxl.worksheet.worksheet.Worksheet, header_row: int) -> List[Tuple[int, str]]:
    return extract_header_pairs_runtime(ws, header_row)


def _build_raw_header_name_by_column(
    ws: openpyxl.worksheet.worksheet.Worksheet,
    header_row: int,
) -> Dict[int, str]:
    return build_raw_header_name_by_column_runtime(ws, header_row)


def extract_rows_with_row_index(
    ws: openpyxl.worksheet.worksheet.Worksheet,
    header_row: int,
) -> List[RowPayload]:
    return extract_rows_with_row_index_runtime(
        ws=ws,
        header_row=header_row,
        row_payload_factory=lambda row_index, fields: RowPayload(row_index=row_index, fields=fields),
    )


def _extract_sheet_fields_and_rows(ws: openpyxl.worksheet.worksheet.Worksheet, header_row: int) -> List[Dict[str, Any]]:
    payloads = extract_rows_with_row_index(ws=ws, header_row=header_row)
    return [payload.fields for payload in payloads]


def _safe_file_token(text: str) -> str:
    return safe_file_token_runtime(text)


def _image_extension_and_mime(image_obj: Any) -> Tuple[str, str]:
    return image_extension_and_mime_runtime(image_obj)


def extract_sheet_images_by_anchor(
    ws: openpyxl.worksheet.worksheet.Worksheet,
    header_row: int,
) -> List[ImagePlacement]:
    return extract_sheet_images_by_anchor_runtime(
        ws=ws,
        header_row=header_row,
        image_placement_factory=lambda **kwargs: ImagePlacement(**kwargs),
    )


def _normalize_field_name(name: str) -> str:
    return normalize_field_name_runtime(name)


def _parse_timestamp_ms(value: Any, tz_offset_hours: int = 8) -> Optional[int]:
    return parse_timestamp_ms_runtime(value, tz_offset_hours=tz_offset_hours)


def _split_multi_values(value: Any) -> List[str]:
    return split_multi_values_runtime(value)


def _lookup_option_name(raw: str, option_names: List[str], option_norm_map: Dict[str, str]) -> Optional[str]:
    return lookup_option_name_runtime(raw, option_names, option_norm_map)


def _convert_value_for_field(value: Any, field_meta: Dict[str, Any], tz_offset_hours: int) -> Tuple[Any, bool]:
    return convert_value_for_field_runtime(value, field_meta, tz_offset_hours)


def _prepare_row_payloads_for_table(
    raw_rows: List[RowPayload],
    table_fields: List[Dict[str, Any]],
    tz_offset_hours: int,
) -> Tuple[List[RowPayload], Dict[str, int]]:
    return prepare_row_payloads_for_table_runtime(
        raw_rows=raw_rows,
        table_fields=table_fields,
        tz_offset_hours=tz_offset_hours,
        normalize_field_name=_normalize_field_name,
        convert_value_for_field=_convert_value_for_field,
        row_payload_factory=lambda row_index, fields: RowPayload(row_index=row_index, fields=fields),
    )


def _prepare_rows_for_table(
    raw_rows: List[Dict[str, Any]],
    table_fields: List[Dict[str, Any]],
    tz_offset_hours: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    return prepare_rows_for_table_runtime(
        raw_rows=raw_rows,
        table_fields=table_fields,
        tz_offset_hours=tz_offset_hours,
        prepare_row_payloads_for_table=_prepare_row_payloads_for_table,
        row_payload_factory=lambda row_index, fields: RowPayload(row_index=row_index, fields=fields),
    )


def _parse_image_import_config(import_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return parse_image_import_config_runtime(import_cfg)


def _normalize_sheet_rules(raw_rules: Any) -> List[Dict[str, Any]]:
    return normalize_sheet_rules_runtime(raw_rules)


def _build_explicit_image_mapping(sheet_image_rules: Any) -> Dict[str, Dict[str, Dict[str, str]]]:
    return build_explicit_image_mapping_runtime(sheet_image_rules)


def _auto_pick_attachment_field(source_column: str, attachment_field_names: List[str]) -> Tuple[Optional[str], str]:
    return auto_pick_attachment_field_runtime(source_column, attachment_field_names)


def _resolve_attachment_target_field(
    sheet_name: str,
    source_column: str,
    attachment_field_names: List[str],
    explicit_map_by_sheet: Dict[str, Dict[str, Dict[str, str]]],
    mapping_mode: str,
) -> Tuple[Optional[str], str]:
    return resolve_attachment_target_field_runtime(
        sheet_name=sheet_name,
        source_column=source_column,
        attachment_field_names=attachment_field_names,
        explicit_map_by_sheet=explicit_map_by_sheet,
        mapping_mode=mapping_mode,
    )


def _select_tokens_by_strategy(tokens: List[str], strategy: str) -> List[str]:
    return select_tokens_by_strategy_runtime(tokens, strategy)


def _apply_sheet_images_to_row_payloads(
    ws: openpyxl.worksheet.worksheet.Worksheet,
    sheet_name: str,
    header_row: int,
    row_payloads: List[RowPayload],
    table_fields: List[Dict[str, Any]],
    image_cfg: Dict[str, Any],
    explicit_map_by_sheet: Dict[str, Dict[str, Dict[str, str]]],
    client: FeishuBitableClient,
    log_func: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    return apply_sheet_images_to_row_payloads_runtime(
        ws=ws,
        sheet_name=sheet_name,
        header_row=header_row,
        row_payloads=row_payloads,
        table_fields=table_fields,
        image_cfg=image_cfg,
        explicit_map_by_sheet=explicit_map_by_sheet,
        client=client,
        build_raw_header_name_by_column=_build_raw_header_name_by_column,
        extract_sheet_images_by_anchor=extract_sheet_images_by_anchor,
        resolve_attachment_target_field=_resolve_attachment_target_field,
        select_tokens_by_strategy=_select_tokens_by_strategy,
        log_func=log_func,
    )


def import_workbook_sheets_to_feishu(config: Dict[str, Any], xlsx_path: str) -> Dict[str, Any]:
    return import_workbook_sheets_to_feishu_runtime(
        config=config,
        xlsx_path=xlsx_path,
        client_factory=FeishuBitableClient,
        normalize_sheet_rules=_normalize_sheet_rules,
        parse_image_import_config=_parse_image_import_config,
        build_explicit_image_mapping=_build_explicit_image_mapping,
        extract_rows_with_row_index=extract_rows_with_row_index,
        prepare_row_payloads_for_table=_prepare_row_payloads_for_table,
        apply_sheet_images_to_row_payloads=_apply_sheet_images_to_row_payloads,
        emit_log=print,
    )


def _resolve_config_path(config_path: Path | str | None = None) -> Path:
    return resolve_config_path_runtime(
        config_path,
        resolve_pipeline_config_path=resolve_pipeline_config_path,
    )


def load_config(config_path: Path | str | None = None) -> Dict[str, Any]:
    return load_config_runtime(
        config_path,
        resolve_pipeline_config_path=resolve_pipeline_config_path,
    )


def _discover_latest_files(excel_dir: Path, buildings: List[str], file_glob_template: str) -> Dict[str, Path]:
    return discover_latest_files_runtime(
        excel_dir,
        buildings,
        file_glob_template,
    )


def _build_results_from_mapping(
    config: Dict[str, Any],
    building_to_file: Dict[str, Path],
) -> List[CalculationResult]:
    return build_results_from_mapping_runtime(
        config,
        building_to_file,
        calculate_monthly_report=lambda file_path, building: calculate_monthly_report(
            file_path,
            building_override=building,
        ),
        emit_log=print,
    )


def _build_results_from_file_items(file_items: List[Dict[str, str]]) -> List[CalculationResult]:
    return build_results_from_file_items_runtime(
        file_items,
        calculate_monthly_report=lambda file_path, building: calculate_monthly_report(
            file_path,
            building_override=building,
        ),
        emit_log=print,
    )


def run_with_config(config: Dict[str, Any]) -> List[CalculationResult]:
    return run_with_config_runtime(
        config,
        discover_latest_files=_discover_latest_files,
        build_results_from_mapping=_build_results_from_mapping,
        emit_log=print,
    )


def run_with_explicit_files(
    config: Dict[str, Any],
    building_to_file: Dict[str, str],
    upload: bool = True,
    save_json: bool = False,
    upload_log_feature: str = "月报上传",
) -> List[CalculationResult]:
    """
    按显式给定的楼栋->文件路径执行计算与上传。
    仅处理传入文件，不会回退扫描目录中的历史文件。
    """
    return run_with_explicit_files_runtime(
        config,
        building_to_file,
        build_results_from_mapping=_build_results_from_mapping,
        save_results_fn=save_results,
        upload_results_to_feishu_fn=upload_results_to_feishu,
        upload=upload,
        save_json=save_json,
        upload_log_feature=upload_log_feature,
    )


def run_with_explicit_file_items(
    config: Dict[str, Any],
    file_items: List[Dict[str, str]],
    upload: bool = True,
    save_json: bool = False,
    upload_log_feature: str = "月报上传",
) -> List[CalculationResult]:
    """
    按显式给定的文件项执行计算与上传。
    每项必须包含 building/file_path，可选 upload_date(YYYY-MM-DD)。
    支持同一楼栋跨多个日期重复上传。
    """
    return run_with_explicit_file_items_runtime(
        config,
        file_items,
        build_results_from_file_items=_build_results_from_file_items,
        save_results_fn=save_results,
        upload_results_to_feishu_fn=upload_results_to_feishu,
        upload=upload,
        save_json=save_json,
        upload_log_feature=upload_log_feature,
    )


def save_results(results: List[CalculationResult], config: Dict[str, Any]) -> None:
    save_results_runtime(
        results,
        config,
        emit_log=print,
    )


def upload_results_to_feishu(
    results: List[CalculationResult],
    config: Dict[str, Any],
    date_override_by_source: Optional[Dict[str, str]] = None,
    log_feature: str = "月报上传",
) -> None:
    upload_results_to_feishu_runtime(
        results=results,
        config=config,
        resolve_upload_date_from_runtime=_resolve_upload_date_from_runtime,
        client_factory=FeishuBitableClient,
        date_override_by_source=date_override_by_source,
        log_feature=log_feature,
        emit_log=print,
    )

def main() -> None:
    config = load_config()
    results = run_with_config(config)
    if not results:
        print("未计算任何楼栋：请检查配置中的路径和文件命名。")
        return
    upload_results_to_feishu(results, config)
    print("全部处理完成。")


if __name__ == "__main__":
    main()

