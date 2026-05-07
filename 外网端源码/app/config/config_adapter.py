from __future__ import annotations

import copy
from typing import Any, Dict, List

from app.config.config_compat_cleanup import (
    sanitize_alarm_export_config,
    sanitize_day_metric_upload_config,
    sanitize_wet_bulb_collection_config,
)
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3, deep_merge_defaults
from app.shared.utils.file_utils import fallback_missing_windows_drive_path
from handover_log_module.core.cell_rule_compiler import migrate_legacy_rule_structures, normalize_cell_rules
from pipeline_utils import get_app_dir


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


MONTHLY_DOWNLOAD_SUBDIR = "月报下载"
HANDOVER_OUTPUT_SUBDIR = "交接班日志输出"
HANDOVER_SHARED_SOURCE_SUBDIR = "交接班共享源文件"


def _normalize_role_mode(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"hybrid", "switching", "dual", "dual_reachable"}:
        return ""
    if text in {"internal", "external"}:
        return text
    return ""


def normalize_role_mode(value: Any) -> str:
    return _normalize_role_mode(value)


def resolve_shared_bridge_paths(shared_bridge_cfg: Dict[str, Any] | None, role_mode: Any) -> Dict[str, Any]:
    shared_bridge = deep_merge_defaults(
        _dict(shared_bridge_cfg),
        _dict(DEFAULT_CONFIG_V3["common"].get("shared_bridge")),
    )
    role_mode = _normalize_role_mode(role_mode)
    legacy_root = str(shared_bridge.get("root_dir", "") or "").strip()
    internal_root = str(shared_bridge.get("internal_root_dir", "") or "").strip() or legacy_root
    external_root = str(shared_bridge.get("external_root_dir", "") or "").strip() or legacy_root
    resolved_root = legacy_root
    if role_mode == "internal":
        resolved_root = internal_root
    elif role_mode == "external":
        resolved_root = external_root
    shared_bridge["internal_root_dir"] = internal_root
    shared_bridge["external_root_dir"] = external_root
    shared_bridge["root_dir"] = resolved_root
    return shared_bridge


def _resolve_shared_bridge_paths(common: Dict[str, Any], deployment: Dict[str, Any]) -> Dict[str, Any]:
    return resolve_shared_bridge_paths(_dict(common.get("shared_bridge")), _dict(deployment).get("role_mode"))


def _join_path_text(base: str, child: str) -> str:
    base_text = str(base or "").strip().rstrip("\\/")
    child_text = str(child or "").strip().strip("\\/")
    if not base_text:
        return child_text
    if not child_text:
        return base_text
    separator = "\\" if ("\\" in base_text or ":" in base_text) else "/"
    return f"{base_text}{separator}{child_text}"


def _resolve_business_root(common_paths: Dict[str, Any]) -> str:
    defaults = _dict(_dict(DEFAULT_CONFIG_V3.get("common")).get("paths"))
    default_root = str(defaults.get("business_root_dir", "") or "").strip() or "D:\\QLDownload"
    explicit_root = str(common_paths.get("business_root_dir", "") or "").strip()
    legacy_download_root = str(common_paths.get("download_save_dir", "") or "").strip()
    legacy_excel_root = str(common_paths.get("excel_dir", "") or "").strip()
    if explicit_root and (explicit_root != default_root or (not legacy_download_root and not legacy_excel_root)):
        return str(
            fallback_missing_windows_drive_path(
                explicit_root,
                app_dir=get_app_dir(),
                label="业务根目录",
            )
        )
    root = legacy_download_root or legacy_excel_root or explicit_root or default_root
    return str(
        fallback_missing_windows_drive_path(
            root,
            app_dir=get_app_dir(),
            label="业务根目录",
        )
    )


def _resolve_runtime_state_root(common_paths: Dict[str, Any]) -> str:
    defaults = _dict(_dict(DEFAULT_CONFIG_V3.get("common")).get("paths"))
    default_root = str(defaults.get("runtime_state_root", "") or "").strip() or ".runtime"
    explicit_root = str(common_paths.get("runtime_state_root", "") or "").strip()
    return explicit_root or default_root


def _backfill_common_feishu_auth_from_legacy(cfg: Dict[str, Any]) -> None:
    common = _dict(cfg.get("common"))
    current_auth = _dict(common.get("feishu_auth"))
    current_app_id = str(current_auth.get("app_id", "") or "").strip()
    current_app_secret = str(current_auth.get("app_secret", "") or "").strip()
    if current_app_id and current_app_secret:
        cfg["common"] = common
        return

    candidates: List[Dict[str, Any]] = []
    common_legacy = _dict(common.get("feishu"))
    if common_legacy:
        candidates.append(common_legacy)
    top_level_legacy = _dict(cfg.get("feishu"))
    if top_level_legacy:
        candidates.append(top_level_legacy)

    for candidate in candidates:
        app_id = str(candidate.get("app_id", "") or "").strip()
        app_secret = str(candidate.get("app_secret", "") or "").strip()
        if not app_id or not app_secret:
            continue
        current_auth["app_id"] = app_id
        current_auth["app_secret"] = app_secret
        for key in ("request_retry_count", "request_retry_interval_sec", "timeout"):
            if key in candidate:
                current_auth[key] = candidate.get(key)
        common["feishu_auth"] = current_auth
        break
    cfg["common"] = common


def _apply_single_root_paths(common: Dict[str, Any], features: Dict[str, Any]) -> None:
    common_paths = _dict(common.get("paths"))
    business_root = _resolve_business_root(common_paths)
    runtime_state_root = _resolve_runtime_state_root(common_paths)
    common_paths = {
        "business_root_dir": business_root,
        "runtime_state_root": runtime_state_root,
    }
    common["paths"] = common_paths

    handover = _dict(features.get("handover_log"))
    template = _dict(handover.get("template"))
    template.pop("output_dir", None)
    handover["template"] = template
    features["handover_log"] = handover


_METRICS_SUMMARY_DEFAULT_ENTRIES: List[Any] = [
    {"label_cell": "A6", "value_cell": "B6"},
    {"label_cell": "C6", "value_cell": "D6"},
    {"label_cell": "E6", "value_cell": "F6"},
    {"label_cell": "G6", "value_cell": "H6"},
    {"label_cell": "A7", "value_cell": "B7"},
    {"label_cell": "C7", "value_cell": "D7"},
    {"label_cell": "E7", "value_cell": "F7"},
    {"label_cell": "G7", "value_cell": "H7"},
    {"label_cell": "A8", "value_cell": "B8"},
    {"label_cell": "C8", "value_cell": "D8"},
    {"label_cell": "E8", "value_cell": "F8"},
    {"label_cell": "A9", "value_cell": "B9"},
    {"label_cell": "C9", "value_cell": "D9"},
    {"label_cell": "E9", "value_cell": "F9"},
    {"label_cell": "G9", "value_cell": "H9"},
    {"label_cell": "A10", "value_cell": "B10"},
    {"label_cell": "C10", "value_cell": "D10"},
    {"label_cell": "E10", "value_cell": "F10"},
    "B15",
    "D15",
    "F15",
    "H52",
    "H53",
    "H54",
    "H55",
]


def _normalize_review_fixed_cell_entry(raw_entry: Any) -> Dict[str, str] | str | None:
    if isinstance(raw_entry, str):
        cell_name = str(raw_entry or "").strip().upper()
        return cell_name or None
    if not isinstance(raw_entry, dict):
        return None
    label_cell = str(
        raw_entry.get("label_cell", raw_entry.get("LABEL_CELL", "")) or ""
    ).strip().upper()
    value_cell = str(
        raw_entry.get("value_cell", raw_entry.get("VALUE_CELL", "")) or ""
    ).strip().upper()
    if not value_cell:
        return None
    if label_cell:
        return {"label_cell": label_cell, "value_cell": value_cell}
    return value_cell


def _review_fixed_cell_key(entry: Dict[str, str] | str) -> str:
    if isinstance(entry, dict):
        return str(entry.get("value_cell", "")).strip().upper()
    return str(entry or "").strip().upper()


def _normalize_metrics_summary_entries(raw_entries: Any) -> List[Any]:
    existing_entries: List[Any] = []
    existing_by_key: Dict[str, Any] = {}
    for raw_entry in _list(raw_entries):
        normalized_entry = _normalize_review_fixed_cell_entry(raw_entry)
        if normalized_entry is None:
            continue
        key = _review_fixed_cell_key(normalized_entry)
        if not key:
            continue
        existing_entries.append(normalized_entry)
        existing_by_key[key] = normalized_entry

    normalized_defaults = []
    for raw_entry in _METRICS_SUMMARY_DEFAULT_ENTRIES:
        normalized_entry = _normalize_review_fixed_cell_entry(raw_entry)
        if normalized_entry is not None:
            normalized_defaults.append(normalized_entry)

    output: List[Any] = []
    used_keys: set[str] = set()
    for default_entry in normalized_defaults:
        key = _review_fixed_cell_key(default_entry)
        existing_entry = existing_by_key.get(key)
        if isinstance(default_entry, dict) and isinstance(existing_entry, str):
            chosen_entry = copy.deepcopy(default_entry)
        elif existing_entry is not None:
            chosen_entry = copy.deepcopy(existing_entry)
        else:
            chosen_entry = copy.deepcopy(default_entry)
        output.append(chosen_entry)
        used_keys.add(key)

    for existing_entry in existing_entries:
        key = _review_fixed_cell_key(existing_entry)
        if not key or key in used_keys:
            continue
        output.append(copy.deepcopy(existing_entry))
        used_keys.add(key)
    return output


def _normalize_review_ui_fixed_cells(features: Dict[str, Any]) -> None:
    handover = _dict(features.get("handover_log"))
    review_ui = _dict(handover.get("review_ui"))
    fixed_cells = _dict(review_ui.get("fixed_cells"))
    fixed_cells["metrics_summary"] = _normalize_metrics_summary_entries(fixed_cells.get("metrics_summary"))
    review_ui["fixed_cells"] = fixed_cells
    handover["review_ui"] = review_ui
    features["handover_log"] = handover


def _discard_deprecated_alarm_db(
    common: Dict[str, Any],
    features: Dict[str, Any],
) -> None:
    common.pop("alarm_db", None)
    handover = _dict(features.get("handover_log"))
    handover.pop("alarm_db", None)
    features["handover_log"] = handover


def _extract_buildings_for_handover(features: Dict[str, Any]) -> list[str]:
    output: list[str] = []
    monthly = _dict(features.get("monthly_report"))
    for item in _list(monthly.get("buildings")):
        building = str(item or "").strip()
        if building and building not in output:
            output.append(building)
    for site in _list(monthly.get("sites")):
        if not isinstance(site, dict):
            continue
        building = str(site.get("building", "")).strip()
        if building and building not in output:
            output.append(building)
    return output


def _normalize_handover_rules(features: Dict[str, Any]) -> None:
    handover = _dict(features.get("handover_log"))
    handover = migrate_legacy_rule_structures(handover)
    buildings = _extract_buildings_for_handover(features)
    normalized_rules = normalize_cell_rules(handover, buildings)
    default_rows = _list(normalized_rules.get("default_rows"))
    existing_ids = {
        str(_dict(row).get("id", "")).strip()
        for row in default_rows
        if isinstance(row, dict) and str(_dict(row).get("id", "")).strip()
    }
    default_template_rows = (
        _dict(_dict(_dict(DEFAULT_CONFIG_V3.get("features")).get("handover_log")).get("cell_rules")).get("default_rows")
    )
    if isinstance(default_template_rows, list):
        for row in default_template_rows:
            if not isinstance(row, dict):
                continue
            row_id = str(row.get("id", "")).strip()
            if not row_id or row_id in existing_ids:
                continue
            default_rows.append(copy.deepcopy(row))
            existing_ids.add(row_id)
    normalized_rules["default_rows"] = default_rows
    handover["cell_rules"] = normalized_rules
    handover.pop("rules", None)
    handover.pop("cell_mapping", None)
    handover.pop("format_templates", None)
    handover.pop("building_overrides", None)
    features["handover_log"] = handover
    _normalize_review_ui_fixed_cells(features)


def is_v3_config(cfg: Dict[str, Any] | None) -> bool:
    if not isinstance(cfg, dict):
        return False
    common = cfg.get("common")
    features = cfg.get("features")
    return isinstance(common, dict) and isinstance(features, dict)


def _legacy_to_v3(legacy_cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = deep_merge_defaults({}, DEFAULT_CONFIG_V3)

    legacy_input = _dict(legacy_cfg.get("input"))
    legacy_output = _dict(legacy_cfg.get("output"))
    legacy_download = _dict(legacy_cfg.get("download"))
    legacy_scheduler = _dict(legacy_cfg.get("scheduler"))
    legacy_notify = _dict(legacy_cfg.get("notify"))
    legacy_feishu = _dict(legacy_cfg.get("feishu"))
    legacy_web = _dict(legacy_cfg.get("web"))
    common = cfg["common"]
    common_paths = _dict(common.get("paths"))
    business_root = (
        str(legacy_download.get("save_dir", "") or "").strip()
        or str(legacy_input.get("excel_dir", "") or "").strip()
        or _resolve_business_root(common_paths)
    )
    common_paths["business_root_dir"] = business_root
    common["paths"] = common_paths

    common["scheduler"] = deep_merge_defaults(legacy_scheduler, common.get("scheduler", {}))
    common["notify"] = deep_merge_defaults(legacy_notify, common.get("notify", {}))
    common["console"] = deep_merge_defaults(legacy_web, common.get("console", {}))
    auth = _dict(common.get("feishu_auth"))
    auth["app_id"] = str(legacy_feishu.get("app_id", auth.get("app_id", "")) or "").strip()
    auth["app_secret"] = str(legacy_feishu.get("app_secret", auth.get("app_secret", "")) or "").strip()
    if "request_retry_count" in legacy_feishu:
        auth["request_retry_count"] = legacy_feishu.get("request_retry_count")
    if "request_retry_interval_sec" in legacy_feishu:
        auth["request_retry_interval_sec"] = legacy_feishu.get("request_retry_interval_sec")
    if "timeout" in legacy_feishu:
        auth["timeout"] = legacy_feishu.get("timeout")
    common["feishu_auth"] = deep_merge_defaults(auth, common.get("feishu_auth", {}))

    features = cfg["features"]
    monthly = _dict(features.get("monthly_report"))
    monthly["buildings"] = copy.deepcopy(_list(legacy_input.get("buildings")))
    monthly["file_glob_template"] = str(legacy_input.get("file_glob_template", monthly.get("file_glob_template", "{building}_*.xlsx")) or "{building}_*.xlsx")

    for key in (
        "time_range_mode",
        "custom_window_mode",
        "start_time",
        "end_time",
        "run_subdir_mode",
        "run_subdir_prefix",
        "max_retries",
        "retry_wait_sec",
        "site_start_delay_sec",
        "only_process_downloaded_this_run",
        "browser_headless",
        "browser_channel",
        "playwright_browsers_path",
    ):
        if key in legacy_download:
            monthly[key] = copy.deepcopy(legacy_download[key])

    monthly["daily_custom_window"] = deep_merge_defaults(
        _dict(legacy_download.get("daily_custom_window")),
        _dict(monthly.get("daily_custom_window")),
    )
    monthly["sites"] = copy.deepcopy(_list(legacy_download.get("sites")))
    monthly["multi_date"] = deep_merge_defaults(
        _dict(legacy_download.get("multi_date")),
        _dict(monthly.get("multi_date")),
    )
    monthly["resume"] = deep_merge_defaults(
        _dict(legacy_download.get("resume")),
        _dict(monthly.get("resume")),
    )
    monthly["performance"] = deep_merge_defaults(
        _dict(legacy_download.get("performance")),
        _dict(monthly.get("performance")),
    )

    monthly_upload = _dict(monthly.get("upload"))
    for key in (
        "enable_upload",
        "skip_zero_records",
        "date_field_mode",
        "date_field_day",
        "date_tz_offset_hours",
        "app_token",
        "calc_table_id",
        "attachment_table_id",
        "report_type",
    ):
        if key in legacy_feishu:
            monthly_upload[key] = copy.deepcopy(legacy_feishu[key])
    monthly["upload"] = deep_merge_defaults(monthly_upload, _dict(monthly.get("upload")))
    features["monthly_report"] = deep_merge_defaults(monthly, _dict(features.get("monthly_report")))

    features["sheet_import"] = deep_merge_defaults(_dict(legacy_cfg.get("feishu_sheet_import")), _dict(features.get("sheet_import")))
    features["handover_log"] = deep_merge_defaults(_dict(legacy_cfg.get("handover_log")), _dict(features.get("handover_log")))
    features["manual_upload_gui"] = deep_merge_defaults(_dict(legacy_cfg.get("manual_upload_gui")), _dict(features.get("manual_upload_gui")))
    features["day_metric_upload"] = sanitize_day_metric_upload_config(_dict(features.get("day_metric_upload")))
    features["alarm_export"] = sanitize_alarm_export_config(_dict(features.get("alarm_export")))
    features["wet_bulb_collection"] = sanitize_wet_bulb_collection_config(
        _dict(features.get("wet_bulb_collection"))
    )
    if _is_external_deployment(common):
        _strip_external_source_site_config(common, features)
    else:
        common["internal_source_sites"] = _resolve_internal_source_sites(common, features)

    _discard_deprecated_alarm_db(common, features)
    _normalize_handover_rules(features)
    _apply_single_root_paths(common, features)

    # output.save_json/json_dir are intentionally no longer exposed in v3 UI,
    # but keep compatibility in runtime adapter only.
    if legacy_output:
        monthly.setdefault("_legacy_output", {})
        monthly["_legacy_output"] = {
            "save_json": bool(legacy_output.get("save_json", False)),
            "json_dir": str(legacy_output.get("json_dir", "") or "").strip(),
        }

    cfg["common"] = deep_merge_defaults(common, cfg.get("common", {}))
    cfg["features"] = deep_merge_defaults(features, cfg.get("features", {}))
    cfg["version"] = 3
    return cfg


def ensure_v3_config(raw_cfg: Dict[str, Any] | None) -> Dict[str, Any]:
    raw = copy.deepcopy(raw_cfg) if isinstance(raw_cfg, dict) else {}
    if is_v3_config(raw):
        cfg = deep_merge_defaults(raw, DEFAULT_CONFIG_V3)
        _backfill_common_feishu_auth_from_legacy(cfg)
        common = _dict(cfg.get("common"))
        features = _dict(cfg.get("features"))
        deployment = deep_merge_defaults(_dict(common.get("deployment")), _dict(DEFAULT_CONFIG_V3["common"].get("deployment")))
        deployment["role_mode"] = normalize_role_mode(deployment.get("role_mode"))
        deployment["last_started_role_mode"] = normalize_role_mode(deployment.get("last_started_role_mode"))
        common["deployment"] = deployment
        common["shared_bridge"] = _resolve_shared_bridge_paths(common, deployment)
        if _is_external_deployment(common):
            _strip_external_source_site_config(common, features)
        else:
            common["internal_source_sites"] = _resolve_internal_source_sites(common, features)
        features["day_metric_upload"] = sanitize_day_metric_upload_config(_dict(features.get("day_metric_upload")))
        features["alarm_export"] = sanitize_alarm_export_config(_dict(features.get("alarm_export")))
        features["wet_bulb_collection"] = sanitize_wet_bulb_collection_config(
            _dict(features.get("wet_bulb_collection"))
        )
        _discard_deprecated_alarm_db(common, features)
        _normalize_handover_rules(features)
        _apply_single_root_paths(common, features)
        cfg["common"] = common
        cfg["features"] = features
        cfg["version"] = 3
        return cfg
    return _legacy_to_v3(raw)


def _clean_sites(sites: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in _list(sites):
        if not isinstance(item, dict):
            continue
        host = str(item.get("host", "") or "").strip()
        username = str(item.get("username", "") or "").strip()
        password = str(item.get("password", "") or "")
        url = item.get("url")
        enabled = bool(item.get("enabled", False))
        has_complete_credentials = bool((host or str(url or "").strip()) and username and password)
        out.append(
            {
                "building": str(item.get("building", "") or "").strip(),
                "enabled": enabled and has_complete_credentials,
                "host": host,
                "username": username,
                "password": password,
                "url": url,
            }
        )
    return out


def _default_internal_source_sites() -> List[Dict[str, Any]]:
    return [
        {"building": "A楼", "enabled": False, "host": "", "username": "", "password": "", "url": ""},
        {"building": "B楼", "enabled": False, "host": "", "username": "", "password": "", "url": ""},
        {"building": "C楼", "enabled": False, "host": "", "username": "", "password": "", "url": ""},
        {"building": "D楼", "enabled": False, "host": "", "username": "", "password": "", "url": ""},
        {"building": "E楼", "enabled": False, "host": "", "username": "", "password": "", "url": ""},
    ]


def _has_meaningful_site_config(sites: List[Dict[str, Any]]) -> bool:
    for site in sites:
        if not isinstance(site, dict):
            continue
        if bool(site.get("enabled", False)):
            return True
        if str(site.get("host", "") or site.get("url", "") or "").strip():
            return True
        if str(site.get("username", "") or "").strip():
            return True
        if str(site.get("password", "") or ""):
            return True
    return False


def _resolve_internal_source_sites(common: Dict[str, Any], features: Dict[str, Any]) -> List[Dict[str, Any]]:
    common_sites = _clean_sites(_dict(common).get("internal_source_sites"))
    if common_sites and _has_meaningful_site_config(common_sites):
        return common_sites
    monthly_sites = _clean_sites(_dict(_dict(features).get("monthly_report")).get("sites"))
    if monthly_sites:
        return monthly_sites
    handover_sites = _clean_sites(_dict(_dict(features).get("handover_log")).get("sites"))
    if handover_sites:
        return handover_sites
    return _default_internal_source_sites()


def _is_external_deployment(common: Dict[str, Any]) -> bool:
    return _normalize_role_mode(_dict(common.get("deployment")).get("role_mode")) == "external"


def _strip_external_source_site_config(common: Dict[str, Any], features: Dict[str, Any]) -> None:
    common.pop("internal_source_sites", None)
    monthly = _dict(features.get("monthly_report"))
    monthly["sites"] = []
    features["monthly_report"] = monthly
    handover = _dict(features.get("handover_log"))
    handover["sites"] = []
    handover["download"] = _dict(handover.get("download"))
    handover["download"]["sites"] = []
    features["handover_log"] = handover


def _has_day_metric_target_config(target: Dict[str, Any]) -> bool:
    source = _dict(target.get("source"))
    return bool(
        str(source.get("app_token", "") or "").strip()
        or str(source.get("table_id", "") or "").strip()
    )


def _migrate_legacy_day_metric_upload_config(
    handover_log: Dict[str, Any],
    day_metric_upload: Dict[str, Any],
) -> Dict[str, Any]:
    legacy_export = _dict(handover_log.get("day_metric_export"))
    if not legacy_export:
        return day_metric_upload

    migrated = copy.deepcopy(day_metric_upload)
    target = _dict(migrated.get("target"))
    if not _has_day_metric_target_config(target):
        migrated["target"] = {
            "source": copy.deepcopy(_dict(legacy_export.get("source"))),
            "fields": copy.deepcopy(_dict(legacy_export.get("fields"))),
            "missing_value_policy": str(legacy_export.get("missing_value_policy", "") or "").strip() or "zero",
        }
    return migrated


def adapt_runtime_config(v3_cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = ensure_v3_config(v3_cfg)
    common = _dict(cfg.get("common"))
    features = _dict(cfg.get("features"))

    common_paths = _dict(common.get("paths"))
    business_root = _resolve_business_root(common_paths)
    runtime_state_root = _resolve_runtime_state_root(common_paths)
    monthly_download_dir = _join_path_text(business_root, MONTHLY_DOWNLOAD_SUBDIR)
    handover_output_dir = _join_path_text(business_root, HANDOVER_OUTPUT_SUBDIR)
    handover_shared_source_dir = _join_path_text(business_root, HANDOVER_SHARED_SOURCE_SUBDIR)
    deployment = deep_merge_defaults(_dict(common.get("deployment")), _dict(DEFAULT_CONFIG_V3["common"].get("deployment")))
    deployment["role_mode"] = normalize_role_mode(deployment.get("role_mode"))
    deployment["last_started_role_mode"] = normalize_role_mode(deployment.get("last_started_role_mode"))
    shared_bridge = _resolve_shared_bridge_paths(common, deployment)
    internal_source_cache = deep_merge_defaults(
        _dict(common.get("internal_source_cache")),
        _dict(DEFAULT_CONFIG_V3["common"].get("internal_source_cache")),
    )
    internal_source_sites = [] if _is_external_deployment(common) else _resolve_internal_source_sites(common, features)
    scheduler = _dict(common.get("scheduler"))
    updater = _dict(common.get("updater"))
    notify = _dict(common.get("notify"))
    feishu_auth = _dict(common.get("feishu_auth"))
    console = _dict(common.get("console"))

    monthly = _dict(features.get("monthly_report"))
    sheet_import = _dict(features.get("sheet_import"))
    handover_log = _dict(features.get("handover_log"))
    day_metric_upload = sanitize_day_metric_upload_config(
        _migrate_legacy_day_metric_upload_config(
            handover_log,
            _dict(features.get("day_metric_upload")),
        )
    )
    alarm_export = sanitize_alarm_export_config(_dict(features.get("alarm_export")))
    wet_bulb_collection = sanitize_wet_bulb_collection_config(_dict(features.get("wet_bulb_collection")))
    manual_upload_gui = _dict(features.get("manual_upload_gui"))
    runtime_download = {
        "save_dir": monthly_download_dir,
        "time_range_mode": monthly.get("time_range_mode"),
        "custom_window_mode": monthly.get("custom_window_mode"),
        "start_time": monthly.get("start_time"),
        "end_time": monthly.get("end_time"),
        "daily_custom_window": copy.deepcopy(_dict(monthly.get("daily_custom_window"))),
        "run_subdir_mode": monthly.get("run_subdir_mode"),
        "run_subdir_prefix": monthly.get("run_subdir_prefix"),
        "max_retries": monthly.get("max_retries"),
        "retry_wait_sec": monthly.get("retry_wait_sec"),
        "site_start_delay_sec": monthly.get("site_start_delay_sec"),
        "only_process_downloaded_this_run": monthly.get("only_process_downloaded_this_run"),
        "browser_headless": monthly.get("browser_headless"),
        "browser_channel": monthly.get("browser_channel"),
        "playwright_browsers_path": monthly.get("playwright_browsers_path"),
        "sites": copy.deepcopy(internal_source_sites),
        "multi_date": copy.deepcopy(_dict(monthly.get("multi_date"))),
        "resume": copy.deepcopy(_dict(monthly.get("resume"))),
        "performance": copy.deepcopy(_dict(monthly.get("performance"))),
    }

    monthly_upload = _dict(monthly.get("upload"))
    runtime_feishu = {
        "app_id": feishu_auth.get("app_id"),
        "app_secret": feishu_auth.get("app_secret"),
        "request_retry_count": feishu_auth.get("request_retry_count"),
        "request_retry_interval_sec": feishu_auth.get("request_retry_interval_sec"),
        "timeout": feishu_auth.get("timeout"),
        "enable_upload": monthly_upload.get("enable_upload"),
        "skip_zero_records": monthly_upload.get("skip_zero_records"),
        "date_field_mode": monthly_upload.get("date_field_mode"),
        "date_field_day": monthly_upload.get("date_field_day"),
        "date_tz_offset_hours": monthly_upload.get("date_tz_offset_hours"),
        "app_token": monthly_upload.get("app_token"),
        "calc_table_id": monthly_upload.get("calc_table_id"),
        "attachment_table_id": monthly_upload.get("attachment_table_id"),
        "report_type": monthly_upload.get("report_type"),
    }

    legacy_output = _dict(monthly.get("_legacy_output"))
    runtime_output = {
        "save_json": bool(legacy_output.get("save_json", False)),
        "json_dir": str(legacy_output.get("json_dir", "") or "").strip(),
    }
    runtime_handover_log = copy.deepcopy(handover_log)
    runtime_handover_log["template"] = _dict(runtime_handover_log.get("template"))
    runtime_handover_log["template"]["output_dir"] = handover_output_dir
    runtime_handover_log["sites"] = copy.deepcopy(internal_source_sites)
    runtime_handover_log["download"] = _dict(runtime_handover_log.get("download"))
    runtime_handover_log["download"]["sites"] = copy.deepcopy(internal_source_sites)

    runtime = {
        "version": 3,
        "paths": {
            "runtime_state_root": runtime_state_root,
            "download_save_dir": business_root,
            "excel_dir": business_root,
            "business_root_dir": business_root,
            "handover_output_dir": handover_output_dir,
            "handover_shared_source_dir": handover_shared_source_dir,
        },
        "input": {
            "excel_dir": business_root,
            "buildings": copy.deepcopy(_list(monthly.get("buildings"))),
            "file_glob_template": str(monthly.get("file_glob_template", "{building}_*.xlsx") or "{building}_*.xlsx"),
        },
        "output": runtime_output,
        "download": runtime_download,
        "deployment": copy.deepcopy(deployment),
        "shared_bridge": copy.deepcopy(shared_bridge),
        "internal_source_cache": copy.deepcopy(internal_source_cache),
        "internal_source_sites": copy.deepcopy(internal_source_sites),
        "network": {
            "connect_poll_interval_sec": 1,
            "enable_auto_switch_wifi": False,
            "role_mode": normalize_role_mode(deployment.get("role_mode")),
            "fail_fast_on_netsh_error": True,
            "scan_before_connect": True,
            "scan_attempts": 3,
            "scan_wait_sec": 2,
            "strict_target_visible_before_connect": True,
            "connect_with_ssid_param": True,
            "preferred_interface": "",
            "auto_disconnect_before_connect": True,
            "hard_recovery_enabled": True,
            "hard_recovery_after_scan_failures": 2,
            "hard_recovery_steps": ["toggle_adapter", "restart_wlansvc"],
            "hard_recovery_cooldown_sec": 20,
            "require_admin_for_hard_recovery": True,
            "internal_profile_name": "",
            "external_profile_name": "",
            "post_switch_stabilize_sec": 3,
            "post_switch_probe_enabled": False,
            "post_switch_probe_internal_host": "",
            "post_switch_probe_internal_port": 80,
            "post_switch_probe_external_host": "open.feishu.cn",
            "post_switch_probe_external_port": 443,
            "post_switch_probe_timeout_sec": 2,
            "post_switch_probe_retries": 3,
            "post_switch_probe_interval_sec": 1,
            "internal_probe_timeout_ms": 1200,
            "internal_probe_count": 1,
            "internal_probe_parallelism": 5,
            "internal_probe_cache_ttl_sec": 2,
            "external_probe_cache_ttl_sec": 2,
        },
        "scheduler": copy.deepcopy(scheduler),
        "updater": copy.deepcopy(updater),
        "notify": copy.deepcopy(notify),
        "feishu": runtime_feishu,
        "feishu_sheet_import": copy.deepcopy(sheet_import),
        "handover_log": runtime_handover_log,
        "day_metric_upload": copy.deepcopy(day_metric_upload),
        "alarm_export": copy.deepcopy(alarm_export),
        "wet_bulb_collection": copy.deepcopy(wet_bulb_collection),
        "manual_upload_gui": copy.deepcopy(manual_upload_gui),
        "web": copy.deepcopy(console),
    }
    return runtime


def sync_runtime_back_to_v3(v3_cfg: Dict[str, Any], runtime_cfg: Dict[str, Any]) -> Dict[str, Any]:
    cfg = ensure_v3_config(v3_cfg)
    runtime = copy.deepcopy(runtime_cfg if isinstance(runtime_cfg, dict) else {})
    common = _dict(cfg.get("common"))
    features = _dict(cfg.get("features"))
    runtime_paths = _dict(runtime.get("paths"))

    deployment = deep_merge_defaults(_dict(runtime.get("deployment")), _dict(common.get("deployment")))
    deployment["role_mode"] = _normalize_role_mode(deployment.get("role_mode"))
    deployment["last_started_role_mode"] = _normalize_role_mode(deployment.get("last_started_role_mode"))
    common["deployment"] = deployment
    external_deployment = deployment["role_mode"] == "external"
    runtime_internal_source_sites = [] if external_deployment else _clean_sites(runtime.get("internal_source_sites"))

    monthly = _dict(features.get("monthly_report"))
    if not external_deployment:
        if not runtime_internal_source_sites:
            runtime_internal_source_sites = _clean_sites(_dict(runtime.get("download", {})).get("sites", []))
        if not runtime_internal_source_sites:
            runtime_internal_source_sites = _clean_sites(_dict(_dict(runtime.get("handover_log")).get("download")).get("sites", []))
        if not runtime_internal_source_sites:
            runtime_internal_source_sites = _default_internal_source_sites()
    monthly["sites"] = copy.deepcopy(runtime_internal_source_sites)
    features["monthly_report"] = monthly
    shared_bridge = deep_merge_defaults(_dict(runtime.get("shared_bridge")), _dict(common.get("shared_bridge")))
    active_root = str(shared_bridge.get("root_dir", "") or "").strip()
    internal_root = str(shared_bridge.get("internal_root_dir", "") or "").strip()
    external_root = str(shared_bridge.get("external_root_dir", "") or "").strip()
    if deployment["role_mode"] == "internal" and active_root:
        internal_root = active_root
    elif deployment["role_mode"] == "external" and active_root:
        external_root = active_root
    if not internal_root:
        internal_root = active_root
    if not external_root:
        external_root = active_root
    shared_bridge["internal_root_dir"] = internal_root
    shared_bridge["external_root_dir"] = external_root
    common["shared_bridge"] = resolve_shared_bridge_paths(shared_bridge, deployment.get("role_mode"))
    common["internal_source_cache"] = deep_merge_defaults(
        _dict(runtime.get("internal_source_cache")),
        _dict(common.get("internal_source_cache")),
    )
    if external_deployment:
        common.pop("internal_source_sites", None)
    else:
        common["internal_source_sites"] = copy.deepcopy(runtime_internal_source_sites)
    common["scheduler"] = deep_merge_defaults(_dict(runtime.get("scheduler")), _dict(common.get("scheduler")))
    common["updater"] = deep_merge_defaults(_dict(runtime.get("updater")), _dict(common.get("updater")))
    common["notify"] = deep_merge_defaults(_dict(runtime.get("notify")), _dict(common.get("notify")))
    common["console"] = deep_merge_defaults(_dict(runtime.get("web")), _dict(common.get("console")))
    common_paths = _dict(common.get("paths"))
    runtime_download = _dict(runtime.get("download"))
    runtime_input = _dict(runtime.get("input"))
    if runtime_paths:
        runtime_root_candidate = str(runtime_paths.get("runtime_state_root", "") or "").strip()
        if runtime_root_candidate:
            common_paths["runtime_state_root"] = runtime_root_candidate
        root_candidate = str(runtime_paths.get("business_root_dir", "") or "").strip()
        if not root_candidate:
            root_candidate = str(runtime_paths.get("download_save_dir", "") or "").strip()
        if root_candidate:
            common_paths["business_root_dir"] = root_candidate
    if not str(common_paths.get("runtime_state_root", "") or "").strip():
        common_paths["runtime_state_root"] = _resolve_runtime_state_root(_dict(common.get("paths")))
    if not str(common_paths.get("business_root_dir", "") or "").strip():
        root_candidate = str(runtime_input.get("excel_dir", "") or "").strip()
        if root_candidate:
            common_paths["business_root_dir"] = root_candidate
    if not str(common_paths.get("business_root_dir", "") or "").strip():
        monthly_save_dir = str(runtime_download.get("save_dir", "") or "").strip()
        if monthly_save_dir:
            common_paths["business_root_dir"] = monthly_save_dir
    common["paths"] = common_paths
    feishu_auth = _dict(common.get("feishu_auth"))
    runtime_feishu = _dict(runtime.get("feishu"))
    for key in ("app_id", "app_secret", "request_retry_count", "request_retry_interval_sec", "timeout"):
        if key in runtime_feishu:
            feishu_auth[key] = runtime_feishu.get(key)
    common["feishu_auth"] = feishu_auth

    handover = _dict(features.get("handover_log"))
    day_metric_upload = sanitize_day_metric_upload_config(
        _migrate_legacy_day_metric_upload_config(
            handover,
            _dict(features.get("day_metric_upload")),
        )
    )
    handover.pop("alarm_db", None)
    handover.pop("day_metric_export", None)
    handover["sites"] = copy.deepcopy(runtime_internal_source_sites)
    handover["download"] = _dict(handover.get("download"))
    handover["download"]["sites"] = copy.deepcopy(runtime_internal_source_sites)
    features["handover_log"] = handover
    features["day_metric_upload"] = sanitize_day_metric_upload_config(
        deep_merge_defaults(
            _dict(runtime.get("day_metric_upload")),
            day_metric_upload,
        )
    )
    features["alarm_export"] = sanitize_alarm_export_config(deep_merge_defaults(
        _dict(runtime.get("alarm_export")),
        _dict(features.get("alarm_export")),
    ))
    features["wet_bulb_collection"] = sanitize_wet_bulb_collection_config(
        deep_merge_defaults(
            _dict(runtime.get("wet_bulb_collection")),
            _dict(features.get("wet_bulb_collection")),
        )
    )
    _apply_single_root_paths(common, features)

    cfg["common"] = common
    cfg["features"] = features
    cfg["version"] = 3
    return cfg
