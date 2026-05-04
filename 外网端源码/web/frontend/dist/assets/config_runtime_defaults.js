import { clone } from "./config_common_utils.js";
import {
  cleanupAlarmExportCompat,
  cleanupDayMetricUploadCompat,
  cleanupWetBulbCollectionCompat,
} from "./config_compat_cleanup.js";

function setStringDefault(obj, key, value) {
  if (!String(obj?.[key] || "").trim()) obj[key] = value;
}

function setNumberDefault(obj, key, value) {
  if (typeof obj?.[key] !== "number") obj[key] = value;
}

function setBooleanDefault(obj, key, value) {
  if (typeof obj?.[key] !== "boolean") obj[key] = value;
}

function joinPathText(base, child) {
  const baseText = String(base || "").trim().replace(/[\\/]+$/, "");
  const childText = String(child || "").trim().replace(/^[\\/]+/, "");
  if (!baseText) return childText;
  if (!childText) return baseText;
  const separator = baseText.includes("\\") || baseText.includes(":") ? "\\" : "/";
  return `${baseText}${separator}${childText}`;
}

const METRICS_SUMMARY_DEFAULT_ENTRIES = [
  { label_cell: "A6", value_cell: "B6" },
  { label_cell: "C6", value_cell: "D6" },
  { label_cell: "E6", value_cell: "F6" },
  { label_cell: "G6", value_cell: "H6" },
  { label_cell: "A7", value_cell: "B7" },
  { label_cell: "C7", value_cell: "D7" },
  { label_cell: "E7", value_cell: "F7" },
  { label_cell: "G7", value_cell: "H7" },
  { label_cell: "A8", value_cell: "B8" },
  { label_cell: "C8", value_cell: "D8" },
  { label_cell: "E8", value_cell: "F8" },
  { label_cell: "A9", value_cell: "B9" },
  { label_cell: "C9", value_cell: "D9" },
  { label_cell: "E9", value_cell: "F9" },
  { label_cell: "G9", value_cell: "H9" },
  { label_cell: "A10", value_cell: "B10" },
  { label_cell: "C10", value_cell: "D10" },
  { label_cell: "E10", value_cell: "F10" },
  "B15",
  "D15",
  "F15",
  "H52",
  "H53",
  "H54",
  "H55",
];

function normalizeReviewFixedCellEntry(rawEntry) {
  if (typeof rawEntry === "string") {
    const cellName = String(rawEntry || "").trim().toUpperCase();
    return cellName || null;
  }
  if (!rawEntry || typeof rawEntry !== "object" || Array.isArray(rawEntry)) {
    return null;
  }
  const labelCell = String(rawEntry.label_cell || "").trim().toUpperCase();
  const valueCell = String(rawEntry.value_cell || "").trim().toUpperCase();
  if (!valueCell) {
    return null;
  }
  if (labelCell) {
    return { label_cell: labelCell, value_cell: valueCell };
  }
  return valueCell;
}

function reviewFixedCellKey(entry) {
  if (entry && typeof entry === "object" && !Array.isArray(entry)) {
    return String(entry.value_cell || "").trim().toUpperCase();
  }
  return String(entry || "").trim().toUpperCase();
}

function normalizeMetricsSummaryEntries(rawEntries) {
  const existingEntries = [];
  const existingByKey = new Map();
  for (const rawEntry of Array.isArray(rawEntries) ? rawEntries : []) {
    const normalizedEntry = normalizeReviewFixedCellEntry(rawEntry);
    if (!normalizedEntry) continue;
    const key = reviewFixedCellKey(normalizedEntry);
    if (!key) continue;
    existingEntries.push(normalizedEntry);
    existingByKey.set(key, normalizedEntry);
  }

  const defaults = METRICS_SUMMARY_DEFAULT_ENTRIES.map((entry) => normalizeReviewFixedCellEntry(entry)).filter(Boolean);
  const output = [];
  const usedKeys = new Set();
  for (const defaultEntry of defaults) {
    const key = reviewFixedCellKey(defaultEntry);
    const existingEntry = existingByKey.get(key);
    let chosenEntry = defaultEntry;
    if (typeof defaultEntry === "object" && typeof existingEntry === "string") {
      chosenEntry = defaultEntry;
    } else if (existingEntry) {
      chosenEntry = existingEntry;
    }
    output.push(clone(chosenEntry));
    usedKeys.add(key);
  }

  for (const existingEntry of existingEntries) {
    const key = reviewFixedCellKey(existingEntry);
    if (!key || usedKeys.has(key)) continue;
    output.push(clone(existingEntry));
    usedKeys.add(key);
  }
  return output;
}

function ensureRoot(cfg) {
  cfg.paths = cfg.paths || {};
  cfg.deployment = cfg.deployment || {};
  cfg.shared_bridge = cfg.shared_bridge || {};
  cfg.internal_source_sites = Array.isArray(cfg.internal_source_sites) ? cfg.internal_source_sites : [];
  cfg.input = cfg.input || {};
  cfg.output = cfg.output || {};
  cfg.download = cfg.download || {};
  cfg.network = cfg.network || {};
  cfg.notify = cfg.notify || {};
  cfg.scheduler = cfg.scheduler || {};
  cfg.updater = cfg.updater || {};
  cfg.feishu = cfg.feishu || {};
  cfg.alarm_export = cfg.alarm_export || {};
  cfg.feishu_sheet_import = cfg.feishu_sheet_import || {};
  cfg.manual_upload_gui = cfg.manual_upload_gui || {};
  cfg.handover_log = cfg.handover_log || {};
  cfg.day_metric_upload = cfg.day_metric_upload || {};
  cfg.branch_power_upload = cfg.branch_power_upload || {};
  cfg.wet_bulb_collection = cfg.wet_bulb_collection || {};
  cfg.handover_log.template = cfg.handover_log.template || {};
  cfg.handover_log.review_ui = cfg.handover_log.review_ui || {};
  cfg.web = cfg.web || {};

  cfg.download.multi_date = cfg.download.multi_date || {};
  cfg.download.resume = cfg.download.resume || {};
  cfg.download.performance = cfg.download.performance || {};
  cfg.download.daily_custom_window = cfg.download.daily_custom_window || {};


  cfg.handover_log.download = cfg.handover_log.download || {};
  cfg.handover_log.shift_roster = cfg.handover_log.shift_roster || {};
  cfg.handover_log.event_sections = cfg.handover_log.event_sections || {};
  cfg.handover_log.monthly_event_report = cfg.handover_log.monthly_event_report || {};
  cfg.handover_log.monthly_change_report = cfg.handover_log.monthly_change_report || {};
  cfg.handover_log.source_data_attachment_export = cfg.handover_log.source_data_attachment_export || {};
  cfg.handover_log.cloud_sheet_sync = cfg.handover_log.cloud_sheet_sync || {};
  cfg.handover_log.daily_report_bitable_export = cfg.handover_log.daily_report_bitable_export || {};
  cfg.handover_log.review_ui.fixed_cells = cfg.handover_log.review_ui.fixed_cells || {};
  cfg.handover_log.shift_roster.source = cfg.handover_log.shift_roster.source || {};
  cfg.handover_log.shift_roster.fields = cfg.handover_log.shift_roster.fields || {};
  cfg.handover_log.shift_roster.cells = cfg.handover_log.shift_roster.cells || {};
  cfg.handover_log.shift_roster.match = cfg.handover_log.shift_roster.match || {};
  cfg.handover_log.shift_roster.shift_alias = cfg.handover_log.shift_roster.shift_alias || {};
  cfg.handover_log.event_sections.source = cfg.handover_log.event_sections.source || {};
  cfg.handover_log.event_sections.duty_window = cfg.handover_log.event_sections.duty_window || {};
  cfg.handover_log.event_sections.fields = cfg.handover_log.event_sections.fields || {};
  cfg.handover_log.event_sections.sections = cfg.handover_log.event_sections.sections || {};
  cfg.handover_log.event_sections.column_mapping = cfg.handover_log.event_sections.column_mapping || {};
  cfg.handover_log.event_sections.column_mapping.header_alias = cfg.handover_log.event_sections.column_mapping.header_alias || {};
  cfg.handover_log.event_sections.column_mapping.fallback_cols = cfg.handover_log.event_sections.column_mapping.fallback_cols || {};
  cfg.handover_log.event_sections.progress_text = cfg.handover_log.event_sections.progress_text || {};
  cfg.handover_log.event_sections.cache = cfg.handover_log.event_sections.cache || {};
  cfg.handover_log.monthly_event_report.template = cfg.handover_log.monthly_event_report.template || {};
  cfg.handover_log.monthly_event_report.scheduler = cfg.handover_log.monthly_event_report.scheduler || {};
  cfg.handover_log.monthly_event_report.test_delivery = cfg.handover_log.monthly_event_report.test_delivery || {};
  cfg.handover_log.monthly_change_report.template = cfg.handover_log.monthly_change_report.template || {};
  cfg.handover_log.monthly_change_report.scheduler = cfg.handover_log.monthly_change_report.scheduler || {};
  cfg.handover_log.source_data_attachment_export.source = cfg.handover_log.source_data_attachment_export.source || {};
  cfg.handover_log.source_data_attachment_export.fields = cfg.handover_log.source_data_attachment_export.fields || {};
  cfg.handover_log.source_data_attachment_export.fixed_values = cfg.handover_log.source_data_attachment_export.fixed_values || {};
  cfg.handover_log.source_data_attachment_export.fixed_values.shift_text =
    cfg.handover_log.source_data_attachment_export.fixed_values.shift_text || {};
  cfg.handover_log.cloud_sheet_sync.sheet_names = cfg.handover_log.cloud_sheet_sync.sheet_names || {};
  cfg.handover_log.cloud_sheet_sync.copy = cfg.handover_log.cloud_sheet_sync.copy || {};
  cfg.handover_log.cloud_sheet_sync.request = cfg.handover_log.cloud_sheet_sync.request || {};
  cfg.handover_log.daily_report_bitable_export.target = cfg.handover_log.daily_report_bitable_export.target || {};
  cfg.handover_log.daily_report_bitable_export.fields = cfg.handover_log.daily_report_bitable_export.fields || {};
  setStringDefault(cfg.handover_log.daily_report_bitable_export, "browser_profile_directory", "");
  cfg.handover_log.template_fixed_fill = cfg.handover_log.template_fixed_fill || {};
  cfg.handover_log.template_fixed_fill.shift_text = cfg.handover_log.template_fixed_fill.shift_text || {};
  cfg.handover_log.template_fixed_fill.on_alarm_query_fail = cfg.handover_log.template_fixed_fill.on_alarm_query_fail || {};
  cfg.handover_log.cell_rules = cfg.handover_log.cell_rules || {};
  cfg.wet_bulb_collection.scheduler = cfg.wet_bulb_collection.scheduler || {};
  cfg.wet_bulb_collection.source = cfg.wet_bulb_collection.source || {};
  cfg.wet_bulb_collection.target = cfg.wet_bulb_collection.target || {};
  cfg.wet_bulb_collection.fields = cfg.wet_bulb_collection.fields || {};
  cfg.wet_bulb_collection.cooling_mode = cfg.wet_bulb_collection.cooling_mode || {};
  cfg.alarm_export.scheduler = cfg.alarm_export.scheduler || {};
  cfg.alarm_export.feishu = cfg.alarm_export.feishu || {};
  cfg.alarm_export.shared_source_upload =
    cfg.alarm_export.shared_source_upload && typeof cfg.alarm_export.shared_source_upload === "object"
      ? cfg.alarm_export.shared_source_upload
      : {};
  cfg.day_metric_upload.scheduler = cfg.day_metric_upload.scheduler || {};
  cfg.day_metric_upload.behavior = cfg.day_metric_upload.behavior || {};
  cfg.day_metric_upload.target = cfg.day_metric_upload.target || {};
  cfg.day_metric_upload.target.source = cfg.day_metric_upload.target.source || {};
  cfg.day_metric_upload.target.fields = cfg.day_metric_upload.target.fields || {};
  cfg.branch_power_upload.scheduler = cfg.branch_power_upload.scheduler || {};
}

function defaultInternalSourceSites() {
  return ["A楼", "B楼", "C楼", "D楼", "E楼"].map((building) => ({
    building,
    enabled: false,
    host: "",
    username: "",
    password: "",
  }));
}

function normalizeInternalSourceSites(rawSites) {
  const defaultsByBuilding = new Map(defaultInternalSourceSites().map((row) => [row.building, row]));
  const sourceRows = Array.isArray(rawSites) ? rawSites : [];
  for (const rawSite of sourceRows) {
    if (!rawSite || typeof rawSite !== "object" || Array.isArray(rawSite)) continue;
    const building = String(rawSite.building || "").trim();
    if (!defaultsByBuilding.has(building)) continue;
    const host = String(rawSite.host || rawSite.ip || rawSite.base_url || "").trim();
    const username = String(rawSite.username || rawSite.user || "").trim();
    const password = String(rawSite.password || "").trim();
    const enabled = typeof rawSite.enabled === "boolean" ? rawSite.enabled : true;
    defaultsByBuilding.set(building, {
      building,
      enabled: enabled && Boolean(host && username && password),
      host,
      username,
      password,
    });
  }
  return Array.from(defaultsByBuilding.values());
}

function applyDeploymentDefaults(cfg) {
  const roleText = String(cfg.deployment.role_mode || "").trim().toLowerCase();
  cfg.deployment.role_mode =
    ["internal", "external"].includes(roleText)
        ? roleText
        : "";
  cfg.deployment.node_id = String(cfg.deployment.node_id || "").trim();
  cfg.deployment.node_label = String(cfg.deployment.node_label || "").trim();
  setBooleanDefault(cfg.shared_bridge, "enabled", false);
  cfg.shared_bridge.root_dir = String(cfg.shared_bridge.root_dir || "").trim();
  cfg.shared_bridge.internal_root_dir = String(cfg.shared_bridge.internal_root_dir || cfg.shared_bridge.root_dir || "").trim();
  cfg.shared_bridge.external_root_dir = String(cfg.shared_bridge.external_root_dir || cfg.shared_bridge.root_dir || "").trim();
  setNumberDefault(cfg.shared_bridge, "poll_interval_sec", 2);
  setNumberDefault(cfg.shared_bridge, "heartbeat_interval_sec", 5);
  setNumberDefault(cfg.shared_bridge, "claim_lease_sec", 30);
  setNumberDefault(cfg.shared_bridge, "stale_task_timeout_sec", 1800);
  setNumberDefault(cfg.shared_bridge, "artifact_retention_days", 7);
  setNumberDefault(cfg.shared_bridge, "sqlite_busy_timeout_ms", 15000);
  if (cfg.deployment.role_mode === "internal") {
    cfg.shared_bridge.root_dir = cfg.shared_bridge.internal_root_dir;
  } else if (cfg.deployment.role_mode === "external") {
    cfg.shared_bridge.root_dir = cfg.shared_bridge.external_root_dir;
  }
}

function applyDownloadDefaults(cfg) {
  const rootPath = String(cfg.download.save_dir || cfg.input.excel_dir || "").trim() || "D:\\QLDownload";
  cfg.download.save_dir = rootPath;
  cfg.input.excel_dir = rootPath;
  cfg.input.file_glob_template = "{building}_*.xlsx";
  if (!["absolute", "daily_relative"].includes(String(cfg.download.custom_window_mode || "").trim())) {
    cfg.download.custom_window_mode = "absolute";
  }
  setStringDefault(cfg.download.daily_custom_window, "start_time", "08:00:00");
  setStringDefault(cfg.download.daily_custom_window, "end_time", "17:00:00");
  setBooleanDefault(cfg.download.daily_custom_window, "cross_day", false);

  setBooleanDefault(cfg.download.resume, "enabled", true);
  setNumberDefault(cfg.download.resume, "retention_days", 7);
  setBooleanDefault(cfg.download.resume, "auto_continue_when_external", true);
  setNumberDefault(cfg.download.resume, "auto_continue_poll_sec", 5);
  setNumberDefault(cfg.download.resume, "gc_every_n_items", 5);
  setNumberDefault(cfg.download.resume, "upload_chunk_threshold", 20);
  setNumberDefault(cfg.download.resume, "upload_chunk_size", 5);
  setStringDefault(cfg.download.resume, "root_dir", "pipeline_resume");
  setStringDefault(cfg.download.resume, "index_file", "index.json");

  setNumberDefault(cfg.download.performance, "query_result_timeout_ms", 10000);
  setNumberDefault(cfg.download.performance, "login_fill_timeout_ms", 5000);
  setNumberDefault(cfg.download.performance, "start_end_visible_timeout_ms", 3000);
  setBooleanDefault(cfg.download.performance, "force_iframe_reopen_each_task", true);
  setNumberDefault(cfg.download.performance, "page_refresh_retry_count", 1);
  setBooleanDefault(cfg.download.performance, "retry_failed_after_all_done", true);
  setNumberDefault(cfg.download.performance, "retry_failed_max_rounds", 1);
}

function applyInternalSourceSiteDefaults(cfg) {
  const fallbackSites =
    cfg.internal_source_sites.length
      ? cfg.internal_source_sites
      : Array.isArray(cfg.download?.sites) && cfg.download.sites.length
        ? cfg.download.sites
        : Array.isArray(cfg.handover_log?.sites) && cfg.handover_log.sites.length
          ? cfg.handover_log.sites
          : cfg.handover_log?.download?.sites;
  cfg.internal_source_sites = normalizeInternalSourceSites(fallbackSites);
}

function buildDefaultHandoverRows() {
  return [
    { id: "cold_temp_max", enabled: true, target_cell: "B9", rule_type: "aggregate", d_keywords: ["冷通道温度"], match_mode: "contains_casefold", agg: "max", template: "{value}℃/{b_norm} {c_norm}", computed_op: "", params: {} },
    { id: "cold_humi_max", enabled: true, target_cell: "D9", rule_type: "aggregate", d_keywords: ["冷通道湿度"], match_mode: "contains_casefold", agg: "max", template: "{value}%/{b_norm} {c_norm}", computed_op: "", params: {} },
    { id: "cold_temp_min", enabled: true, target_cell: "F9", rule_type: "aggregate", d_keywords: ["冷通道温度"], match_mode: "contains_casefold", agg: "min", template: "{value}℃/{b_norm} {c_norm}", computed_op: "", params: {} },
    { id: "cold_humi_min", enabled: true, target_cell: "H9", rule_type: "aggregate", d_keywords: ["冷通道湿度"], match_mode: "contains_casefold", agg: "min", template: "{value}%/{b_norm} {c_norm}", computed_op: "", params: {} },
    { id: "tr_load_max", enabled: true, target_cell: "B10", rule_type: "aggregate", d_keywords: ["负载率"], d_regex: ".*负载率", group_contains: "TR", match_mode: "contains_casefold", agg: "max", template: "{value}%/{d_name}", computed_op: "", params: {} },
    { id: "ups_load_max", enabled: true, target_cell: "D10", rule_type: "aggregate", d_keywords: ["负载率"], d_regex: ".*负载率", group_contains: "UPS", match_mode: "contains_casefold", agg: "max", template: "{value}%/{d_name}", computed_op: "", params: {} },
    { id: "hvdc_load_max", enabled: true, target_cell: "", rule_type: "aggregate", d_keywords: ["HVDC", "负载率"], d_regex: ".*(HVDC.*负载率|负载率.*HVDC).*", group_contains: "HVDC", match_mode: "contains_casefold", agg: "max", template: "{value}", computed_op: "", params: {} },
    { id: "battery_backup_min", enabled: true, target_cell: "F10", rule_type: "aggregate", d_keywords: ["电池放电后备时间"], match_mode: "contains_casefold", agg: "min", template: "{value}", computed_op: "", params: {} },
    { id: "oil_backup_time", enabled: true, target_cell: "H6", rule_type: "direct", d_keywords: ["油量后备时间", "燃油后备时间"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "chiller_mode_1", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["1号冷机模式"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "chiller_mode_2", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["2号冷机模式"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "chiller_mode_3", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["3号冷机模式"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "chiller_mode_4", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["4号冷机模式"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "chiller_mode_5", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["5号冷机模式"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "chiller_mode_6", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["6号冷机模式"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "west_tank_time", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["西区蓄冷罐放冷时间"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "east_tank_time", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["东区蓄冷罐放冷时间"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "ring_124", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["124-冷冻水供水环管温度", "124-冷冻水供水温度", "西区冷冻水供水环管温度"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "ring_150", enabled: true, target_cell: "", rule_type: "direct", d_keywords: ["150-冷冻水供水环管温度", "150-冷冻水供水温度", "东区冷冻水供水环管温度"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "chilled_supply_temp_max", enabled: true, target_cell: "", rule_type: "aggregate", d_keywords: ["冷冻水供水温度"], match_mode: "contains_casefold", agg: "max", template: "{value}", computed_op: "", params: {} },
    { id: "tank_backup", enabled: true, target_cell: "F8", rule_type: "computed", d_keywords: [], match_mode: "contains_casefold", agg: "first", template: "西区{west}/东区{east}", computed_op: "tank_backup", params: {} },
    { id: "outdoor_temp", enabled: true, target_cell: "B7", rule_type: "direct", d_keywords: ["室外温度", "室外干球温度"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "wet_bulb", enabled: true, target_cell: "D7", rule_type: "direct", d_keywords: ["室外湿球温度"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "ring_supply_temp", enabled: true, target_cell: "H7", rule_type: "computed", d_keywords: [], match_mode: "contains_casefold", agg: "first", template: "西区{west}℃/东区{east}℃", computed_op: "ring_supply_temp", params: {} },
    { id: "chiller_mode_summary", enabled: true, target_cell: "F7", rule_type: "computed", d_keywords: [], match_mode: "contains_casefold", agg: "first", template: "西区{west_mode}/东区{east_mode}", computed_op: "chiller_mode_summary", params: {} },
    { id: "water_pool_backup_time", enabled: true, target_cell: "D8", rule_type: "direct", d_keywords: ["水池后备时间"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "city_water_pressure", enabled: true, target_cell: "B8", rule_type: "direct", d_keywords: ["市政补水管压力", "市政压力"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "pue", enabled: true, target_cell: "B6", rule_type: "direct", d_keywords: ["PUE", "实时PUE", "实时pue"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "city_power", enabled: true, target_cell: "D6", rule_type: "direct", d_keywords: ["市电进线总功率", "市电总功率", "D楼总功率"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
    { id: "it_power", enabled: true, target_cell: "F6", rule_type: "direct", d_keywords: ["IT总功率", "IT功率", "二三四层功率和", "IT功率和"], match_mode: "contains_casefold", agg: "first", template: "{value}", computed_op: "", params: {} },
  ];
}

function applyHandoverDefaults(cfg) {
  cfg.handover_log.chiller_mode = cfg.handover_log.chiller_mode || {};
  cfg.handover_log.scheduler = cfg.handover_log.scheduler || {};
  cfg.handover_log.capacity_report = cfg.handover_log.capacity_report || {};
  cfg.handover_log.change_management_section = cfg.handover_log.change_management_section || {};
  cfg.handover_log.exercise_management_section = cfg.handover_log.exercise_management_section || {};
  cfg.handover_log.maintenance_management_section = cfg.handover_log.maintenance_management_section || {};
  cfg.handover_log.other_important_work_section = cfg.handover_log.other_important_work_section || {};
  const chillerMode = cfg.handover_log.chiller_mode;
  const scheduler = cfg.handover_log.scheduler;
  const capacityReport = cfg.handover_log.capacity_report;
  const download = cfg.handover_log.download;
  const template = cfg.handover_log.template;
  const businessRoot = String(cfg.download.save_dir || cfg.input.excel_dir || "").trim() || "D:\\QLDownload";
  const shiftWindows = download.shift_windows || {};
  shiftWindows.day = shiftWindows.day || {};
  shiftWindows.night = shiftWindows.night || {};
  download.shift_windows = shiftWindows;

  setStringDefault(download, "template_name", "交接班日志（李世龙）");
  setNumberDefault(download, "lookback_minutes", 20);
  setBooleanDefault(download, "use_shift_window_when_provided", false);
  setStringDefault(shiftWindows.day, "start", "08:00:00");
  setStringDefault(shiftWindows.day, "end", "17:00:00");
  setStringDefault(shiftWindows.night, "start", "17:00:00");
  setStringDefault(shiftWindows.night, "end_next_day", "08:00:00");
  setStringDefault(download, "scale_label", "5分钟");
  setNumberDefault(download, "query_result_timeout_ms", 20000);
  setNumberDefault(download, "download_event_timeout_ms", 120000);
  setNumberDefault(download, "login_fill_timeout_ms", 5000);
  setNumberDefault(download, "menu_visible_timeout_ms", 20000);
  setNumberDefault(download, "iframe_timeout_ms", 15000);
  setNumberDefault(download, "start_end_visible_timeout_ms", 5000);
  setNumberDefault(download, "page_refresh_retry_count", 1);
  setNumberDefault(download, "max_retries", 2);
  setNumberDefault(download, "retry_wait_sec", 2);
  setBooleanDefault(download, "force_iframe_reopen_each_task", true);
  setBooleanDefault(download, "parallel_by_building", false);
  setNumberDefault(download, "site_start_delay_sec", 1);
  setBooleanDefault(download, "debug_step_log", true);
  setStringDefault(download, "export_button_text", "原样导出");
  capacityReport.weather = capacityReport.weather || {};
  setStringDefault(capacityReport.weather, "provider", "seniverse");
  setStringDefault(capacityReport.weather, "location", "崇川区");
  capacityReport.weather.fallback_locations = Array.isArray(capacityReport.weather.fallback_locations)
    ? capacityReport.weather.fallback_locations.filter((item) => String(item || "").trim())
    : ["南通"];
  if (!capacityReport.weather.fallback_locations.length) {
    capacityReport.weather.fallback_locations = ["南通"];
  }
  setStringDefault(capacityReport.weather, "language", "zh-Hans");
  setStringDefault(capacityReport.weather, "unit", "c");
  setNumberDefault(capacityReport.weather, "timeout_sec", 8);
  setStringDefault(capacityReport.weather, "auth_mode", "signed");
  setStringDefault(capacityReport.weather, "seniverse_public_key", "");
  setStringDefault(capacityReport.weather, "seniverse_private_key", "");
  const shiftRoster = cfg.handover_log.shift_roster;
  const eventSections = cfg.handover_log.event_sections;
  const changeManagement = cfg.handover_log.change_management_section;
  changeManagement.monthly_report_fields = changeManagement.monthly_report_fields || {};
  const exerciseManagement = cfg.handover_log.exercise_management_section;
  const maintenanceManagement = cfg.handover_log.maintenance_management_section;
  const otherImportantWork = cfg.handover_log.other_important_work_section;
  shiftRoster.long_day = shiftRoster.long_day || {};
  shiftRoster.long_day.source = shiftRoster.long_day.source || {};
  shiftRoster.long_day.fields = shiftRoster.long_day.fields || {};
  shiftRoster.long_day.match = shiftRoster.long_day.match || {};
  shiftRoster.engineer_directory = shiftRoster.engineer_directory || {};
  shiftRoster.engineer_directory.source = shiftRoster.engineer_directory.source || {};
  shiftRoster.engineer_directory.fields = shiftRoster.engineer_directory.fields || {};
  shiftRoster.engineer_directory.delivery = shiftRoster.engineer_directory.delivery || {};
  shiftRoster.engineer_directory.match = shiftRoster.engineer_directory.match || {};
  changeManagement.source = changeManagement.source || {};
  changeManagement.fields = changeManagement.fields || {};
  changeManagement.sections = changeManagement.sections || {};
  changeManagement.column_mapping = changeManagement.column_mapping || {};
  changeManagement.column_mapping.header_alias = changeManagement.column_mapping.header_alias || {};
  changeManagement.column_mapping.fallback_cols = changeManagement.column_mapping.fallback_cols || {};
  changeManagement.work_window_text = changeManagement.work_window_text || {};
  exerciseManagement.source = exerciseManagement.source || {};
  exerciseManagement.fields = exerciseManagement.fields || {};
  exerciseManagement.sections = exerciseManagement.sections || {};
  exerciseManagement.fixed_values = exerciseManagement.fixed_values || {};
  exerciseManagement.column_mapping = exerciseManagement.column_mapping || {};
  exerciseManagement.column_mapping.header_alias = exerciseManagement.column_mapping.header_alias || {};
  exerciseManagement.column_mapping.fallback_cols = exerciseManagement.column_mapping.fallback_cols || {};
  maintenanceManagement.source = maintenanceManagement.source || {};
  maintenanceManagement.fields = maintenanceManagement.fields || {};
  maintenanceManagement.sections = maintenanceManagement.sections || {};
  maintenanceManagement.fixed_values = maintenanceManagement.fixed_values || {};
  maintenanceManagement.column_mapping = maintenanceManagement.column_mapping || {};
  maintenanceManagement.column_mapping.header_alias = maintenanceManagement.column_mapping.header_alias || {};
  maintenanceManagement.column_mapping.fallback_cols = maintenanceManagement.column_mapping.fallback_cols || {};
  otherImportantWork.source = otherImportantWork.source || {};
  otherImportantWork.sections = otherImportantWork.sections || {};
  otherImportantWork.column_mapping = otherImportantWork.column_mapping || {};
  otherImportantWork.column_mapping.header_alias = otherImportantWork.column_mapping.header_alias || {};
  otherImportantWork.column_mapping.fallback_cols = otherImportantWork.column_mapping.fallback_cols || {};
  otherImportantWork.sources = otherImportantWork.sources && typeof otherImportantWork.sources === "object" ? otherImportantWork.sources : {};
  setBooleanDefault(shiftRoster, "enabled", true);
  setStringDefault(shiftRoster, "people_split_regex", "[、,/，；;\\s]+");
  setStringDefault(shiftRoster.source, "app_token", "G7oUwGdwaiTmimk8i2ecGTWOn4d");
  setStringDefault(shiftRoster.source, "table_id", "tblRV9KeWFh9xCkm");
  setNumberDefault(shiftRoster.source, "page_size", 500);
  setNumberDefault(shiftRoster.source, "max_records", 5000);
  setStringDefault(shiftRoster.fields, "duty_date", "排班日期");
  setStringDefault(shiftRoster.fields, "building", "机楼");
  setStringDefault(shiftRoster.fields, "team", "班组");
  setStringDefault(shiftRoster.fields, "shift", "班次");
  if (!String(shiftRoster.fields.people_text || "").trim() || String(shiftRoster.fields.people_text || "").trim() === "人员（文本）") {
    shiftRoster.fields.people_text = "值班人员（实际）";
  }
  setStringDefault(shiftRoster.cells, "current_people", "C3");
  setStringDefault(shiftRoster.cells, "next_people", "G3");
  if (!Array.isArray(shiftRoster.cells.next_first_person_cells) || !shiftRoster.cells.next_first_person_cells.length) {
    shiftRoster.cells.next_first_person_cells = ["H52", "H53", "H54", "H55"];
  }
  setStringDefault(shiftRoster.match, "building_mode", "exact_then_code");
  if (!Array.isArray(shiftRoster.shift_alias.day) || !shiftRoster.shift_alias.day.length) {
    shiftRoster.shift_alias.day = ["白班", "day", "DAY"];
  }
  if (!Array.isArray(shiftRoster.shift_alias.night) || !shiftRoster.shift_alias.night.length) {
    shiftRoster.shift_alias.night = ["夜班", "night", "NIGHT"];
  }

  const longDay = shiftRoster.long_day;
  setBooleanDefault(longDay, "enabled", true);
  setStringDefault(longDay.source, "app_token", "");
  setStringDefault(longDay.source, "table_id", "tblyyU7BbO4vB1oO");
  setNumberDefault(longDay.source, "page_size", 500);
  setNumberDefault(longDay.source, "max_records", 5000);
  setStringDefault(longDay.fields, "duty_date", "排班日期");
  setStringDefault(longDay.fields, "building", "机楼");
  setStringDefault(longDay.fields, "shift", "班次");
  if (!String(longDay.fields.people_text || "").trim() || String(longDay.fields.people_text || "").trim() === "人员（文本）") {
    longDay.fields.people_text = "值班人员（实际）";
  }
  setStringDefault(longDay, "shift_value", "长白");
  setStringDefault(longDay, "day_cell", "B4");
  setStringDefault(longDay, "night_cell", "F4");
  setStringDefault(longDay, "prefix", "长白岗：");
  setStringDefault(longDay, "rest_text", "/");
  setStringDefault(longDay.match, "building_mode", "exact_then_code");

  const engineerDirectory = shiftRoster.engineer_directory;
  setBooleanDefault(engineerDirectory, "enabled", true);
  setStringDefault(engineerDirectory.source, "app_token", "");
  setStringDefault(engineerDirectory.source, "table_id", "tblZsQ6UmLdg9a2m");
  setNumberDefault(engineerDirectory.source, "page_size", 500);
  setNumberDefault(engineerDirectory.source, "max_records", 5000);
  setStringDefault(engineerDirectory.fields, "building", "楼栋/专业");
  setStringDefault(engineerDirectory.fields, "specialty", "专业");
  setStringDefault(engineerDirectory.fields, "supervisor_text", "主管（文本）");
  setStringDefault(engineerDirectory.fields, "supervisor_person", "主管");
  setStringDefault(engineerDirectory.fields, "position", "职位");
  setStringDefault(engineerDirectory.fields, "recipient_id", "");
  setStringDefault(engineerDirectory.delivery, "receive_id_type", "user_id");
  setStringDefault(engineerDirectory.delivery, "position_keyword", "设施运维主管");
  setStringDefault(engineerDirectory.match, "building_mode", "exact_then_code");

  setBooleanDefault(eventSections, "enabled", true);
  setStringDefault(eventSections.source, "app_token", "D01TwFPyXiJBY6kCBDZcMCGLnSe");
  setStringDefault(eventSections.source, "table_id", "tblj9XJLq5QzTAqX");
  setNumberDefault(eventSections.source, "page_size", 500);
  setNumberDefault(eventSections.source, "max_records", 5000);
  setStringDefault(eventSections.duty_window, "day_start", "09:00:00");
  setStringDefault(eventSections.duty_window, "day_end", "18:00:00");
  setStringDefault(eventSections.duty_window, "night_start", "18:00:00");
  setStringDefault(eventSections.duty_window, "night_end_next_day", "09:00:00");
  setStringDefault(eventSections.duty_window, "boundary_mode", "left_closed_right_open");
  setStringDefault(eventSections.fields, "event_time", "事件发生时间");
  setStringDefault(eventSections.fields, "building", "机楼");
  setStringDefault(eventSections.fields, "event_level", "事件等级");
  setStringDefault(eventSections.fields, "description", "告警描述");
  setStringDefault(eventSections.fields, "exclude_checked", "不计入事件");
  setStringDefault(eventSections.fields, "final_status", "最终状态");
  setStringDefault(eventSections.fields, "exclude_duration", "事件结束处理时长");
  setStringDefault(eventSections.fields, "exclude_duration_value", "不计入事件");
  setStringDefault(eventSections.fields, "to_maint", "是否转检修");
  setStringDefault(eventSections.fields, "maint_done_time", "检修完成时间");
  setStringDefault(eventSections.fields, "event_done_time", "事件结束时间");
  setStringDefault(eventSections.sections, "new_event", "新事件处理");
  setStringDefault(eventSections.sections, "history_followup", "历史事件跟进");
  setBooleanDefault(eventSections.column_mapping, "resolve_by_header", true);
  if (!Array.isArray(eventSections.column_mapping.header_alias.event_level) || !eventSections.column_mapping.header_alias.event_level.length) {
    eventSections.column_mapping.header_alias.event_level = ["事件等级"];
  }
  if (!Array.isArray(eventSections.column_mapping.header_alias.event_time) || !eventSections.column_mapping.header_alias.event_time.length) {
    eventSections.column_mapping.header_alias.event_time = ["发生时间"];
  }
  if (!Array.isArray(eventSections.column_mapping.header_alias.description) || !eventSections.column_mapping.header_alias.description.length) {
    eventSections.column_mapping.header_alias.description = ["描述", "告警描述"];
  }
  if (!Array.isArray(eventSections.column_mapping.header_alias.work_window) || !eventSections.column_mapping.header_alias.work_window.length) {
    eventSections.column_mapping.header_alias.work_window = ["作业时间段"];
  }
  if (!Array.isArray(eventSections.column_mapping.header_alias.progress) || !eventSections.column_mapping.header_alias.progress.length) {
    eventSections.column_mapping.header_alias.progress = ["事件处理进展"];
  }
  if (!Array.isArray(eventSections.column_mapping.header_alias.follower) || !eventSections.column_mapping.header_alias.follower.length) {
    eventSections.column_mapping.header_alias.follower = ["跟进人"];
  }
  setStringDefault(eventSections.column_mapping.fallback_cols, "event_level", "B");
  setStringDefault(eventSections.column_mapping.fallback_cols, "event_time", "C");
  setStringDefault(eventSections.column_mapping.fallback_cols, "description", "D");
  setStringDefault(eventSections.column_mapping.fallback_cols, "work_window", "E");
  setStringDefault(eventSections.column_mapping.fallback_cols, "progress", "F");
  setStringDefault(eventSections.column_mapping.fallback_cols, "follower", "G");
  setStringDefault(eventSections.progress_text, "done", "已完成");
  setStringDefault(eventSections.progress_text, "todo", "未完成");
  setBooleanDefault(eventSections.cache, "enabled", true);
  setStringDefault(eventSections.cache, "state_file", "handover_shared_cache.json");
  setNumberDefault(eventSections.cache, "max_pending", 20000);
  setNumberDefault(eventSections.cache, "max_last_query_ids", 5000);

  const monthlyEventReport = cfg.handover_log.monthly_event_report;
  const monthlyChangeReport = cfg.handover_log.monthly_change_report;
  setBooleanDefault(monthlyEventReport, "enabled", true);
  setStringDefault(monthlyEventReport.template, "source_path", "月度事件统计表空模板.xlsx");
  setStringDefault(monthlyEventReport.template, "change_source_path", "月度变更统计表空模板.xlsx");
  setStringDefault(monthlyEventReport.template, "output_dir", "D:\\QLDownload\\月度统计表输出\\事件月度统计表");
  setStringDefault(
    monthlyEventReport.template,
    "file_name_pattern",
    "{building}_{month}_事件月度统计表.xlsx",
  );
  setBooleanDefault(monthlyEventReport.scheduler, "enabled", true);
  setBooleanDefault(monthlyEventReport.scheduler, "auto_start_in_gui", false);
  setNumberDefault(monthlyEventReport.scheduler, "day_of_month", 1);
  setStringDefault(monthlyEventReport.scheduler, "run_time", "01:00:00");
  setNumberDefault(monthlyEventReport.scheduler, "check_interval_sec", 30);
  setStringDefault(
    monthlyEventReport.scheduler,
    "state_file",
    "monthly_event_report_scheduler_state.json",
  );
  setStringDefault(monthlyEventReport.test_delivery, "receive_id_type", "open_id");
  monthlyEventReport.test_delivery.receive_ids = Array.isArray(monthlyEventReport.test_delivery.receive_ids)
    ? monthlyEventReport.test_delivery.receive_ids
        .map((item) => String(item || "").trim())
        .filter(Boolean)
        .filter((item, index, list) => list.indexOf(item) === index)
    : ["ou_902e364a6c2c6c20893c02abe505a7b2"];
  if (!monthlyEventReport.test_delivery.receive_ids.length) {
    monthlyEventReport.test_delivery.receive_ids = ["ou_902e364a6c2c6c20893c02abe505a7b2"];
  }

  setBooleanDefault(monthlyChangeReport, "enabled", true);
  setStringDefault(
    monthlyChangeReport.template,
    "source_path",
    String(monthlyChangeReport.template.source_path || "").trim()
      || String(monthlyEventReport.template.change_source_path || "").trim()
      || "月度变更统计表空模板.xlsx",
  );
  setStringDefault(monthlyChangeReport.template, "output_dir", "D:\\QLDownload\\月度统计表输出\\变更月度统计表");
  setStringDefault(
    monthlyChangeReport.template,
    "file_name_pattern",
    "{building}_{month}_变更月度统计表.xlsx",
  );
  setBooleanDefault(monthlyChangeReport.scheduler, "enabled", true);
  setBooleanDefault(monthlyChangeReport.scheduler, "auto_start_in_gui", false);
  setNumberDefault(monthlyChangeReport.scheduler, "day_of_month", 1);
  setStringDefault(monthlyChangeReport.scheduler, "run_time", "01:00:00");
  setNumberDefault(monthlyChangeReport.scheduler, "check_interval_sec", 30);
  setStringDefault(
    monthlyChangeReport.scheduler,
    "state_file",
    "monthly_change_report_scheduler_state.json",
  );

  setBooleanDefault(changeManagement, "enabled", true);
  setStringDefault(changeManagement.source, "app_token", "D01TwFPyXiJBY6kCBDZcMCGLnSe");
  setStringDefault(changeManagement.source, "table_id", "tblYodlEKeWzqogu");
  setNumberDefault(changeManagement.source, "page_size", 500);
  setNumberDefault(changeManagement.source, "max_records", 5000);
  setStringDefault(changeManagement.fields, "building", "楼栋");
  setStringDefault(changeManagement.fields, "start_time", "变更开始时间");
  setStringDefault(changeManagement.fields, "end_time", "变更结束时间");
  setStringDefault(changeManagement.fields, "updated_time", "更新最新的时间");
  setStringDefault(changeManagement.fields, "change_level", "阿里-变更等级");
  setStringDefault(changeManagement.fields, "process_updates", "过程更新时间");
  setStringDefault(changeManagement.fields, "description", "名称");
  setStringDefault(changeManagement.fields, "specialty", "专业");
  setStringDefault(changeManagement.monthly_report_fields, "building", String(changeManagement.fields.building || "").trim() || "楼栋");
  setStringDefault(changeManagement.monthly_report_fields, "change_code", "变更编码");
  setStringDefault(changeManagement.monthly_report_fields, "name", "名称");
  setStringDefault(changeManagement.monthly_report_fields, "location", "位置");
  setStringDefault(changeManagement.monthly_report_fields, "change_level", "智航-变更等级");
  setStringDefault(changeManagement.monthly_report_fields, "status", "变更状态");
  setStringDefault(changeManagement.monthly_report_fields, "start_time", "变更开始时间");
  setStringDefault(changeManagement.monthly_report_fields, "end_time", "变更结束时间");
  setStringDefault(changeManagement.sections, "change_management", "变更管理");
  setBooleanDefault(changeManagement.column_mapping, "resolve_by_header", true);
  if (!Array.isArray(changeManagement.column_mapping.header_alias.change_level) || !changeManagement.column_mapping.header_alias.change_level.length) {
    changeManagement.column_mapping.header_alias.change_level = ["变更等级", "事件等级"];
  }
  if (!Array.isArray(changeManagement.column_mapping.header_alias.work_window) || !changeManagement.column_mapping.header_alias.work_window.length) {
    changeManagement.column_mapping.header_alias.work_window = ["作业时间段"];
  }
  if (!Array.isArray(changeManagement.column_mapping.header_alias.description) || !changeManagement.column_mapping.header_alias.description.length) {
    changeManagement.column_mapping.header_alias.description = ["描述", "告警描述"];
  }
  if (!Array.isArray(changeManagement.column_mapping.header_alias.executor) || !changeManagement.column_mapping.header_alias.executor.length) {
    changeManagement.column_mapping.header_alias.executor = ["执行人", "跟进人"];
  }
  setStringDefault(changeManagement.column_mapping.fallback_cols, "change_level", "B");
  setStringDefault(changeManagement.column_mapping.fallback_cols, "work_window", "E");
  setStringDefault(changeManagement.column_mapping.fallback_cols, "description", "D");
  setStringDefault(changeManagement.column_mapping.fallback_cols, "executor", "H");
  setStringDefault(changeManagement.work_window_text, "day_anchor", "08:00:00");
  setStringDefault(changeManagement.work_window_text, "day_default_end", "18:30:00");
  setStringDefault(changeManagement.work_window_text, "night_anchor", "18:00:00");
  setStringDefault(changeManagement.work_window_text, "night_default_end_next_day", "08:00:00");

  setBooleanDefault(exerciseManagement, "enabled", true);
  setStringDefault(exerciseManagement.source, "app_token", "D01TwFPyXiJBY6kCBDZcMCGLnSe");
  setStringDefault(exerciseManagement.source, "table_id", "tblBrALE11XCicNN");
  setNumberDefault(exerciseManagement.source, "page_size", 500);
  setNumberDefault(exerciseManagement.source, "max_records", 5000);
  setStringDefault(exerciseManagement.fields, "building", "机楼");
  setStringDefault(exerciseManagement.fields, "start_time", "演练开始时间");
  setStringDefault(exerciseManagement.fields, "project", "告警描述");
  setStringDefault(exerciseManagement.sections, "exercise_management", "演练管理");
  setStringDefault(exerciseManagement.fixed_values, "exercise_type", "计划性演练");
  setStringDefault(exerciseManagement.fixed_values, "completion", "已完成");
  setBooleanDefault(exerciseManagement.column_mapping, "resolve_by_header", true);
  if (!Array.isArray(exerciseManagement.column_mapping.header_alias.exercise_type) || !exerciseManagement.column_mapping.header_alias.exercise_type.length) {
    exerciseManagement.column_mapping.header_alias.exercise_type = ["演练类型"];
  }
  if (!Array.isArray(exerciseManagement.column_mapping.header_alias.exercise_item) || !exerciseManagement.column_mapping.header_alias.exercise_item.length) {
    exerciseManagement.column_mapping.header_alias.exercise_item = ["演练项目"];
  }
  if (!Array.isArray(exerciseManagement.column_mapping.header_alias.completion) || !exerciseManagement.column_mapping.header_alias.completion.length) {
    exerciseManagement.column_mapping.header_alias.completion = ["演练完成情况", "完成情况"];
  }
  if (!Array.isArray(exerciseManagement.column_mapping.header_alias.executor) || !exerciseManagement.column_mapping.header_alias.executor.length) {
    exerciseManagement.column_mapping.header_alias.executor = ["执行人", "跟进人"];
  }
  setStringDefault(exerciseManagement.column_mapping.fallback_cols, "exercise_type", "B");
  setStringDefault(exerciseManagement.column_mapping.fallback_cols, "exercise_item", "C");
  setStringDefault(exerciseManagement.column_mapping.fallback_cols, "completion", "D");
  setStringDefault(exerciseManagement.column_mapping.fallback_cols, "executor", "H");

  setBooleanDefault(maintenanceManagement, "enabled", true);
  setStringDefault(maintenanceManagement.source, "app_token", "D01TwFPyXiJBY6kCBDZcMCGLnSe");
  setStringDefault(maintenanceManagement.source, "table_id", "tblk7QuEsiE4p3nZ");
  setNumberDefault(maintenanceManagement.source, "page_size", 500);
  setNumberDefault(maintenanceManagement.source, "max_records", 5000);
  setStringDefault(maintenanceManagement.fields, "building", "楼栋");
  setStringDefault(maintenanceManagement.fields, "start_time", "实际开始时间");
  setStringDefault(maintenanceManagement.fields, "updated_time", "最新更新时间");
  setStringDefault(maintenanceManagement.fields, "actual_end_time", "实际结束时间");
  setStringDefault(maintenanceManagement.fields, "item", "名称");
  setStringDefault(maintenanceManagement.fields, "specialty", "专业");
  setStringDefault(maintenanceManagement.sections, "maintenance_management", "维护管理");
  setStringDefault(maintenanceManagement.fixed_values, "vendor_internal", "自维");
  setStringDefault(maintenanceManagement.fixed_values, "vendor_external", "厂维");
  setStringDefault(maintenanceManagement.fixed_values, "completion", "已完成");
  setBooleanDefault(maintenanceManagement.column_mapping, "resolve_by_header", true);
  if (!Array.isArray(maintenanceManagement.column_mapping.header_alias.maintenance_item) || !maintenanceManagement.column_mapping.header_alias.maintenance_item.length) {
    maintenanceManagement.column_mapping.header_alias.maintenance_item = ["维护总项"];
  }
  if (!Array.isArray(maintenanceManagement.column_mapping.header_alias.maintenance_party) || !maintenanceManagement.column_mapping.header_alias.maintenance_party.length) {
    maintenanceManagement.column_mapping.header_alias.maintenance_party = ["维护执行方"];
  }
  if (!Array.isArray(maintenanceManagement.column_mapping.header_alias.completion) || !maintenanceManagement.column_mapping.header_alias.completion.length) {
    maintenanceManagement.column_mapping.header_alias.completion = ["维护完成情况", "完成情况"];
  }
  if (!Array.isArray(maintenanceManagement.column_mapping.header_alias.executor) || !maintenanceManagement.column_mapping.header_alias.executor.length) {
    maintenanceManagement.column_mapping.header_alias.executor = ["执行人", "跟进人"];
  }
  setStringDefault(maintenanceManagement.column_mapping.fallback_cols, "maintenance_item", "B");
  setStringDefault(maintenanceManagement.column_mapping.fallback_cols, "maintenance_party", "C");
  setStringDefault(maintenanceManagement.column_mapping.fallback_cols, "completion", "D");
  setStringDefault(maintenanceManagement.column_mapping.fallback_cols, "executor", "H");

  setBooleanDefault(otherImportantWork, "enabled", true);
  setStringDefault(otherImportantWork.source, "app_token", "D01TwFPyXiJBY6kCBDZcMCGLnSe");
  setNumberDefault(otherImportantWork.source, "page_size", 500);
  setNumberDefault(otherImportantWork.source, "max_records", 5000);
  setStringDefault(otherImportantWork.sections, "other_important_work", "其他重要工作记录");
  if (!Array.isArray(otherImportantWork.order) || !otherImportantWork.order.length) {
    otherImportantWork.order = ["power_notice", "device_adjustment", "device_patrol", "device_repair"];
  }
  setBooleanDefault(otherImportantWork.column_mapping, "resolve_by_header", true);
  if (!Array.isArray(otherImportantWork.column_mapping.header_alias.description) || !otherImportantWork.column_mapping.header_alias.description.length) {
    otherImportantWork.column_mapping.header_alias.description = ["描述"];
  }
  if (!Array.isArray(otherImportantWork.column_mapping.header_alias.completion) || !otherImportantWork.column_mapping.header_alias.completion.length) {
    otherImportantWork.column_mapping.header_alias.completion = ["完成情况"];
  }
  if (!Array.isArray(otherImportantWork.column_mapping.header_alias.executor) || !otherImportantWork.column_mapping.header_alias.executor.length) {
    otherImportantWork.column_mapping.header_alias.executor = ["执行人", "跟进人"];
  }
  setStringDefault(otherImportantWork.column_mapping.fallback_cols, "description", "B");
  setStringDefault(otherImportantWork.column_mapping.fallback_cols, "completion", "F");
  setStringDefault(otherImportantWork.column_mapping.fallback_cols, "executor", "H");
  const ensureOtherSource = (key, label, tableId, descriptionField, completionField) => {
    otherImportantWork.sources[key] =
      otherImportantWork.sources[key] && typeof otherImportantWork.sources[key] === "object"
        ? otherImportantWork.sources[key]
        : {};
    const current = otherImportantWork.sources[key];
    current.fields = current.fields && typeof current.fields === "object" ? current.fields : {};
    setStringDefault(current, "label", label);
    setStringDefault(current, "table_id", tableId);
    setStringDefault(current.fields, "building", "楼栋");
    setStringDefault(current.fields, "actual_start_time", "实际开始时间");
    setStringDefault(current.fields, "actual_end_time", "实际结束时间");
    setStringDefault(current.fields, "description", descriptionField);
    setStringDefault(current.fields, "completion", completionField);
    setStringDefault(current.fields, "specialty", "专业");
  };
  ensureOtherSource("power_notice", "上电通告", "tblf2uQrzCWw5eIV", "名称", "进度");
  ensureOtherSource("device_adjustment", "设备调整", "tbleqBZdQu1n8qqK", "内容", "进度");
  ensureOtherSource("device_patrol", "设备轮巡", "tbl0XK1iQ1P6VY5Y", "内容", "进度");
  ensureOtherSource("device_repair", "设备检修", "tblpaHktT0mn0hwg", "维修故障", "进度（完成情况）");

  const legacyDayMetricExport = cfg.handover_log.day_metric_export || {};
  const dayMetricTarget = cfg.day_metric_upload.target;
  const sourceDataAttachmentExport = cfg.handover_log.source_data_attachment_export;
  const dailyReportExport = cfg.handover_log.daily_report_bitable_export;
  const reviewUi = cfg.handover_log.review_ui;
  reviewUi.cabinet_power_defaults_by_building =
    reviewUi.cabinet_power_defaults_by_building &&
    typeof reviewUi.cabinet_power_defaults_by_building === "object" &&
    !Array.isArray(reviewUi.cabinet_power_defaults_by_building)
      ? reviewUi.cabinet_power_defaults_by_building
      : {};
  reviewUi.footer_inventory_defaults_by_building =
    reviewUi.footer_inventory_defaults_by_building &&
    typeof reviewUi.footer_inventory_defaults_by_building === "object" &&
    !Array.isArray(reviewUi.footer_inventory_defaults_by_building)
      ? reviewUi.footer_inventory_defaults_by_building
      : {};
  if (
    !(dayMetricTarget.source && (dayMetricTarget.source.app_token || dayMetricTarget.source.table_id))
    && legacyDayMetricExport
    && typeof legacyDayMetricExport === "object"
    && legacyDayMetricExport.source
    && typeof legacyDayMetricExport.source === "object"
  ) {
    dayMetricTarget.source = { ...legacyDayMetricExport.source };
  }
  if (
    !(dayMetricTarget.fields && Object.keys(dayMetricTarget.fields).length)
    && legacyDayMetricExport
    && typeof legacyDayMetricExport === "object"
    && legacyDayMetricExport.fields
    && typeof legacyDayMetricExport.fields === "object"
  ) {
    dayMetricTarget.fields = { ...legacyDayMetricExport.fields };
  }
  if (!dayMetricTarget.missing_value_policy && legacyDayMetricExport?.missing_value_policy) {
    dayMetricTarget.missing_value_policy = legacyDayMetricExport.missing_value_policy;
  }

  setStringDefault(dayMetricTarget, "missing_value_policy", "zero");
  setStringDefault(dayMetricTarget.source, "app_token", "ASLxbfESPahdTKs0A9NccgbrnXc");
  setStringDefault(dayMetricTarget.source, "table_id", "tblAHGF8mV6U9jid");
  setNumberDefault(dayMetricTarget.source, "create_batch_size", 200);
  setStringDefault(dayMetricTarget.fields, "type", "类型");
  setStringDefault(dayMetricTarget.fields, "building", "楼栋");
  setStringDefault(dayMetricTarget.fields, "date", "日期");
  setStringDefault(dayMetricTarget.fields, "value", "数值");
  setStringDefault(dayMetricTarget.fields, "position_code", "位置/编号");
  delete dayMetricTarget.types;

  setBooleanDefault(sourceDataAttachmentExport, "enabled", true);
  setBooleanDefault(sourceDataAttachmentExport, "upload_night_shift", true);
  setBooleanDefault(sourceDataAttachmentExport, "replace_existing", true);
  setStringDefault(sourceDataAttachmentExport.source, "app_token", "ASLxbfESPahdTKs0A9NccgbrnXc");
  setStringDefault(sourceDataAttachmentExport.source, "table_id", "tblF13MQ10PslIdI");
  setNumberDefault(sourceDataAttachmentExport.source, "page_size", 500);
  setNumberDefault(sourceDataAttachmentExport.source, "max_records", 5000);
  setNumberDefault(sourceDataAttachmentExport.source, "delete_batch_size", 200);
  setStringDefault(sourceDataAttachmentExport.fields, "type", "类型");
  setStringDefault(sourceDataAttachmentExport.fields, "building", "楼栋");
  setStringDefault(sourceDataAttachmentExport.fields, "date", "日期");
  setStringDefault(sourceDataAttachmentExport.fields, "shift", "班次");
  setStringDefault(sourceDataAttachmentExport.fields, "attachment", "附件");
  setStringDefault(sourceDataAttachmentExport.fixed_values, "type", "动环数据");
  setStringDefault(sourceDataAttachmentExport.fixed_values.shift_text, "day", "白班");
  setStringDefault(sourceDataAttachmentExport.fixed_values.shift_text, "night", "夜班");

  const cloudSheetSync = cfg.handover_log.cloud_sheet_sync;
  cloudSheetSync.sheet_names = cloudSheetSync.sheet_names || {};
  cloudSheetSync.copy = cloudSheetSync.copy || {};
  cloudSheetSync.request = cloudSheetSync.request || {};
  setBooleanDefault(cloudSheetSync, "enabled", true);
  setStringDefault(cloudSheetSync, "root_wiki_url", "https://vnet.feishu.cn/wiki/WlpWwkhQGi46pEkYbMTcNnOzntb");
  setStringDefault(cloudSheetSync, "template_node_token", "QxeYwGTHbiyz9bk2gRAca4nonod");
  setStringDefault(cloudSheetSync, "spreadsheet_name_pattern", "南通园区交接班日志-{date_text}{shift_text}");
  setStringDefault(cloudSheetSync, "source_sheet_name", "交接班日志");
  setStringDefault(cloudSheetSync, "sync_mode", "overwrite_named_sheet");
  setStringDefault(cloudSheetSync.sheet_names, "A楼", "A楼");
  setStringDefault(cloudSheetSync.sheet_names, "B楼", "B楼");
  setStringDefault(cloudSheetSync.sheet_names, "C楼", "C楼");
  setStringDefault(cloudSheetSync.sheet_names, "D楼", "D楼");
  setStringDefault(cloudSheetSync.sheet_names, "E楼", "E楼");
  setBooleanDefault(cloudSheetSync.copy, "values", true);
  setBooleanDefault(cloudSheetSync.copy, "formulas", true);
  setBooleanDefault(cloudSheetSync.copy, "styles", true);
  setBooleanDefault(cloudSheetSync.copy, "merges", true);
  setBooleanDefault(cloudSheetSync.copy, "row_heights", true);
  setBooleanDefault(cloudSheetSync.copy, "column_widths", true);
  setNumberDefault(cloudSheetSync.request, "timeout_sec", 20);
  setNumberDefault(cloudSheetSync.request, "max_retries", 3);
  setNumberDefault(cloudSheetSync.request, "retry_backoff_sec", 2);

  setBooleanDefault(dailyReportExport, "enabled", true);
  setStringDefault(dailyReportExport.target, "app_token", "MliKbC3fXa8PXrsndKscmxjdn1g");
  setStringDefault(dailyReportExport.target, "table_id", "tblxfd0HA9kDZQ3w");
  setBooleanDefault(dailyReportExport.target, "replace_existing", true);
  setNumberDefault(dailyReportExport.target, "page_size", 500);
  setNumberDefault(dailyReportExport.target, "max_records", 5000);
  setNumberDefault(dailyReportExport.target, "delete_batch_size", 200);
  setStringDefault(dailyReportExport.fields, "year", "年度");
  setStringDefault(dailyReportExport.fields, "date", "日期");
  setStringDefault(dailyReportExport.fields, "shift", "班次");
  setStringDefault(dailyReportExport.fields, "report_link", "交接班日报");
  setStringDefault(dailyReportExport.fields, "screenshots", "日报截图");
  setStringDefault(
    dailyReportExport,
    "summary_page_url",
    "https://vnet.feishu.cn/app/LTjUbmZsTaTFIVsuQSLcUi4Onf4?pageId=pgeZUMIpMDuIIfLA",
  );
  setStringDefault(
    dailyReportExport,
    "external_page_url",
    "https://vnet.feishu.cn/app/LTjUbmZsTaTFIVsuQSLcUi4Onf4?pageId=pgecZCUXaEtvP9Yl",
  );

  setBooleanDefault(reviewUi, "enabled", true);
  if (!Array.isArray(reviewUi.buildings) || reviewUi.buildings.length !== 5) {
    reviewUi.buildings = [
      { code: "a", name: "A楼" },
      { code: "b", name: "B楼" },
      { code: "c", name: "C楼" },
      { code: "d", name: "D楼" },
      { code: "e", name: "E楼" },
    ];
  }
  if (!Array.isArray(reviewUi.fixed_cells.header_basic) || !reviewUi.fixed_cells.header_basic.length) {
    reviewUi.fixed_cells.header_basic = ["A1", "B2", "F2", "C3", "G3", "B4", "F4"];
  }
  reviewUi.fixed_cells.metrics_summary = normalizeMetricsSummaryEntries(reviewUi.fixed_cells.metrics_summary);
  if (
    !Array.isArray(reviewUi.fixed_cells.cabinet_power_info) ||
    !reviewUi.fixed_cells.cabinet_power_info.length
  ) {
    reviewUi.fixed_cells.cabinet_power_info = ["B13", "D13", "F13", "H13"];
  }
  setNumberDefault(reviewUi, "autosave_debounce_ms", 800);
  setNumberDefault(reviewUi, "poll_interval_sec", 5);
  setBooleanDefault(reviewUi, "bind_latest_session", true);
  setStringDefault(reviewUi, "public_base_url", "");
  reviewUi.review_link_recipients_by_building =
    reviewUi.review_link_recipients_by_building &&
    typeof reviewUi.review_link_recipients_by_building === "object" &&
    !Array.isArray(reviewUi.review_link_recipients_by_building)
      ? reviewUi.review_link_recipients_by_building
      : {};
  if (!Array.isArray(reviewUi.section_hidden_columns) || !reviewUi.section_hidden_columns.length) {
    reviewUi.section_hidden_columns = ["I"];
  } else {
    reviewUi.section_hidden_columns = reviewUi.section_hidden_columns
      .map((value) => String(value || "").trim().toUpperCase())
      .filter((value, index, array) => /^[B-I]$/.test(value) && array.indexOf(value) === index);
    if (!reviewUi.section_hidden_columns.length) {
      reviewUi.section_hidden_columns = ["I"];
    }
  }

  setBooleanDefault(scheduler, "enabled", true);
  setBooleanDefault(scheduler, "auto_start_in_gui", false);
  setStringDefault(scheduler, "morning_time", "08:00:00");
  setStringDefault(scheduler, "afternoon_time", "17:00:00");
  setNumberDefault(scheduler, "check_interval_sec", 30);
  setBooleanDefault(scheduler, "catch_up_if_missed", false);
  setBooleanDefault(scheduler, "retry_failed_in_same_period", false);
  setStringDefault(scheduler, "morning_state_file", "handover_scheduler_morning_state.json");
  setStringDefault(scheduler, "afternoon_state_file", "handover_scheduler_afternoon_state.json");
  template.apply_building_title = true;
  setStringDefault(template, "source_path", "交接班日志空模板.xlsx");
  template.title_cell = "A1";
  template.building_title_pattern = "EA118机房{building_code}栋数据中心交接班日志";
  template.building_title_map = {
    "A楼": "EA118机房A栋数据中心交接班日志",
    "B楼": "EA118机房B栋数据中心交接班日志",
    "C楼": "EA118机房C栋数据中心交接班日志",
    "D楼": "EA118机房D栋数据中心交接班日志",
    "E楼": "EA118机房E栋数据中心交接班日志",
  };

  const fixed = cfg.handover_log.template_fixed_fill;
  setStringDefault(fixed, "date_cell", "B2");
  setStringDefault(fixed, "shift_cell", "F2");
  setStringDefault(fixed, "alarm_total_cell", "B15");
  setStringDefault(fixed, "alarm_unrecovered_cell", "D15");
  setStringDefault(fixed, "alarm_accept_desc_cell", "F15");
  setStringDefault(fixed, "date_text_format", "{year}年{month}月{day}日");
  setStringDefault(fixed.shift_text, "day", "白班");
  setStringDefault(fixed.shift_text, "night", "夜班");
  setStringDefault(fixed.on_alarm_query_fail, "total", "0");
  setStringDefault(fixed.on_alarm_query_fail, "unrecovered", "0");
  setStringDefault(fixed.on_alarm_query_fail, "accept_desc", "/");

  if (!Array.isArray(chillerMode.west_keys) || !chillerMode.west_keys.length) {
    chillerMode.west_keys = ["chiller_mode_1", "chiller_mode_2", "chiller_mode_3"];
  }
  if (!Array.isArray(chillerMode.east_keys) || !chillerMode.east_keys.length) {
    chillerMode.east_keys = ["chiller_mode_4", "chiller_mode_5", "chiller_mode_6"];
  }
  if (!Array.isArray(chillerMode.priority_order) || !chillerMode.priority_order.length) {
    chillerMode.priority_order = ["1", "2", "3", "4"];
  }
  if (!chillerMode.value_map || typeof chillerMode.value_map !== "object") {
    chillerMode.value_map = { "1": "制冷", "2": "预冷", "3": "板换", "4": "停机" };
  }
  setStringDefault(chillerMode, "fallback_mode_text", "停机");

  const cellRules = cfg.handover_log.cell_rules;
  const defaultRowsTemplate = clone(buildDefaultHandoverRows());
  if (!Array.isArray(cellRules.default_rows) || !cellRules.default_rows.length) {
    cellRules.default_rows = defaultRowsTemplate;
  } else {
    const existingIds = new Set(
      cellRules.default_rows
        .map((row) => String(row?.id || "").trim())
        .filter(Boolean),
    );
    for (const row of defaultRowsTemplate) {
      const rowId = String(row?.id || "").trim();
      if (!rowId || existingIds.has(rowId)) continue;
      cellRules.default_rows.push(clone(row));
      existingIds.add(rowId);
    }
  }
  if (!cellRules.building_rows || typeof cellRules.building_rows !== "object") {
    cellRules.building_rows = {};
  }
  for (const building of ["A楼", "B楼", "C楼", "D楼", "E楼"]) {
    if (!Array.isArray(cellRules.building_rows[building])) {
      cellRules.building_rows[building] = [];
    }
    if (!Array.isArray(reviewUi.review_link_recipients_by_building[building])) {
      reviewUi.review_link_recipients_by_building[building] = [];
    }
  }

  delete cfg.handover_log.rules;
  delete cfg.handover_log.cell_mapping;
  delete cfg.handover_log.format_templates;
  delete cfg.handover_log.building_overrides;
}

function applyAlarmExportDefaults(cfg) {
  const alarmExport = cfg.alarm_export;
  const scheduler = alarmExport.scheduler;
  const feishu = alarmExport.feishu;
  const sharedSourceUpload = alarmExport.shared_source_upload;
  const legacyTarget =
    sharedSourceUpload.target && typeof sharedSourceUpload.target === "object"
      ? sharedSourceUpload.target
      : {};

  if (!String(feishu.app_token || "").trim() && String(legacyTarget.app_token || "").trim()) {
    feishu.app_token = String(legacyTarget.app_token || "").trim();
  }
  if (!String(feishu.table_id || "").trim() && String(legacyTarget.table_id || "").trim()) {
    feishu.table_id = String(legacyTarget.table_id || "").trim();
  }
  if ((!Number.isInteger(feishu.page_size) || feishu.page_size <= 0) && Number.isInteger(legacyTarget.page_size) && legacyTarget.page_size > 0) {
    feishu.page_size = legacyTarget.page_size;
  }
  if ((!Number.isInteger(feishu.delete_batch_size) || feishu.delete_batch_size <= 0) && Number.isInteger(legacyTarget.delete_batch_size) && legacyTarget.delete_batch_size > 0) {
    feishu.delete_batch_size = legacyTarget.delete_batch_size;
  }
  if ((!Number.isInteger(feishu.create_batch_size) || feishu.create_batch_size <= 0) && Number.isInteger(legacyTarget.create_batch_size) && legacyTarget.create_batch_size > 0) {
    feishu.create_batch_size = legacyTarget.create_batch_size;
  }

  setStringDefault(feishu, "app_token", "");
  setStringDefault(feishu, "table_id", "");
  setNumberDefault(feishu, "page_size", 500);
  setNumberDefault(feishu, "delete_batch_size", 500);
  setNumberDefault(feishu, "create_batch_size", 200);
  cleanupAlarmExportCompat(alarmExport);
  setBooleanDefault(scheduler, "enabled", true);
  setBooleanDefault(scheduler, "auto_start_in_gui", false);
  setStringDefault(scheduler, "run_time", "08:10:00");
  setNumberDefault(scheduler, "check_interval_sec", 30);
  setBooleanDefault(scheduler, "catch_up_if_missed", false);
  setBooleanDefault(scheduler, "retry_failed_in_same_period", true);
  setStringDefault(scheduler, "state_file", "alarm_event_upload_scheduler_state.json");
  setBooleanDefault(sharedSourceUpload, "replace_existing_on_full", true);
}

function applyNetworkDefaults(cfg) {
  setNumberDefault(cfg.network, "connect_poll_interval_sec", 1);
  delete cfg.network.enable_auto_switch_wifi;
  setBooleanDefault(cfg.network, "fail_fast_on_netsh_error", true);
  setBooleanDefault(cfg.network, "scan_before_connect", true);
  setNumberDefault(cfg.network, "scan_attempts", 3);
  setNumberDefault(cfg.network, "scan_wait_sec", 2);
  setBooleanDefault(cfg.network, "strict_target_visible_before_connect", true);
  setBooleanDefault(cfg.network, "connect_with_ssid_param", true);
  setStringDefault(cfg.network, "preferred_interface", "");
  setBooleanDefault(cfg.network, "auto_disconnect_before_connect", true);
  setBooleanDefault(cfg.network, "hard_recovery_enabled", true);
  setNumberDefault(cfg.network, "hard_recovery_after_scan_failures", 2);
  if (!Array.isArray(cfg.network.hard_recovery_steps) || !cfg.network.hard_recovery_steps.length) {
    cfg.network.hard_recovery_steps = ["toggle_adapter", "restart_wlansvc"];
  }
  setNumberDefault(cfg.network, "hard_recovery_cooldown_sec", 20);
  setBooleanDefault(cfg.network, "require_admin_for_hard_recovery", true);
  setStringDefault(cfg.network, "internal_profile_name", "");
  setStringDefault(cfg.network, "external_profile_name", "");
  setNumberDefault(cfg.network, "post_switch_stabilize_sec", 3);
  setBooleanDefault(cfg.network, "post_switch_probe_enabled", false);
  setStringDefault(cfg.network, "post_switch_probe_internal_host", "");
  setNumberDefault(cfg.network, "post_switch_probe_internal_port", 80);
  setStringDefault(cfg.network, "post_switch_probe_external_host", "open.feishu.cn");
  setNumberDefault(cfg.network, "post_switch_probe_external_port", 443);
  setNumberDefault(cfg.network, "post_switch_probe_timeout_sec", 2);
  setNumberDefault(cfg.network, "post_switch_probe_retries", 3);
  setNumberDefault(cfg.network, "post_switch_probe_interval_sec", 1);
}

function applyWetBulbCollectionDefaults(cfg) {
  const wet = cleanupWetBulbCollectionCompat(cfg.wet_bulb_collection);
  const scheduler = wet.scheduler;
  const source = wet.source;
  const target = wet.target;
  const fields = wet.fields;
  const coolingMode = wet.cooling_mode;

  setBooleanDefault(wet, "enabled", true);

  setBooleanDefault(scheduler, "enabled", true);
  setBooleanDefault(scheduler, "auto_start_in_gui", false);
  setNumberDefault(scheduler, "interval_minutes", 60);
  setNumberDefault(scheduler, "check_interval_sec", 30);
  setBooleanDefault(scheduler, "retry_failed_on_next_tick", true);
  setStringDefault(scheduler, "state_file", "wet_bulb_collection_scheduler_state.json");

  setBooleanDefault(source, "reuse_handover_download", true);
  setBooleanDefault(source, "reuse_handover_rule_engine", true);

  setStringDefault(target, "app_token", "JbZywYBfgiltYpksj2bc1HvCnPd");
  setStringDefault(target, "table_id", "tblm3MOOxKCW3ZPd");
  setNumberDefault(target, "page_size", 500);
  setNumberDefault(target, "max_records", 5000);
  setNumberDefault(target, "delete_batch_size", 200);
  setNumberDefault(target, "create_batch_size", 200);
  setBooleanDefault(target, "replace_existing", true);

  setStringDefault(fields, "date", "日期");
  setStringDefault(fields, "building", "楼栋");
  setStringDefault(fields, "wet_bulb_temp", "天气湿球温度");
  setStringDefault(fields, "cooling_mode", "冷源运行模式");
  setStringDefault(fields, "sequence", "序号");

  if (!Array.isArray(coolingMode.priority_order) || !coolingMode.priority_order.length) {
    coolingMode.priority_order = ["1", "2", "3", "4"];
  }
  if (!coolingMode.source_value_map || typeof coolingMode.source_value_map !== "object") {
    coolingMode.source_value_map = { "1": "制冷", "2": "预冷", "3": "板换", "4": "停机" };
  }
  if (!coolingMode.upload_value_map || typeof coolingMode.upload_value_map !== "object") {
    coolingMode.upload_value_map = {
      "制冷": "机械制冷",
      "预冷": "预冷模式",
      "板换": "自然冷模式",
    };
  }
  if (!Array.isArray(coolingMode.skip_modes) || !coolingMode.skip_modes.length) {
    coolingMode.skip_modes = ["停机"];
  }
}

function applyDayMetricUploadDefaults(cfg) {
  const upload = cleanupDayMetricUploadCompat(cfg.day_metric_upload);
  const scheduler = upload.scheduler;
  const behavior = upload.behavior;
  const target = upload.target;
  const targetSource = target.source;
  const targetFields = target.fields;

  setBooleanDefault(scheduler, "enabled", true);
  setBooleanDefault(scheduler, "auto_start_in_gui", false);
  setNumberDefault(scheduler, "interval_minutes", 60);
  setNumberDefault(scheduler, "check_interval_sec", 30);
  setBooleanDefault(scheduler, "retry_failed_on_next_tick", true);
  setStringDefault(scheduler, "state_file", "day_metric_upload_scheduler_state.json");

  setNumberDefault(behavior, "basic_retry_attempts", 3);
  setNumberDefault(behavior, "basic_retry_backoff_sec", 2);
  setNumberDefault(behavior, "network_retry_attempts", 5);
  setNumberDefault(behavior, "network_retry_backoff_sec", 2);
  setNumberDefault(behavior, "alert_after_attempts", 5);

  setStringDefault(target, "missing_value_policy", "zero");
  setStringDefault(targetSource, "app_token", "ASLxbfESPahdTKs0A9NccgbrnXc");
  setStringDefault(targetSource, "table_id", "tblAHGF8mV6U9jid");
  setNumberDefault(targetSource, "create_batch_size", 200);

  setStringDefault(targetFields, "type", "类型");
  setStringDefault(targetFields, "building", "楼栋");
  setStringDefault(targetFields, "date", "日期");
  setStringDefault(targetFields, "value", "数值");
  setStringDefault(targetFields, "position_code", "位置/编号");
  delete target.types;
}

function applyBranchPowerUploadDefaults(cfg) {
  const upload = cfg.branch_power_upload;
  const scheduler = upload.scheduler;
  setBooleanDefault(scheduler, "enabled", true);
  setBooleanDefault(scheduler, "auto_start_in_gui", false);
  setNumberDefault(scheduler, "interval_minutes", 60);
  setNumberDefault(scheduler, "check_interval_sec", 30);
  setBooleanDefault(scheduler, "retry_failed_on_next_tick", true);
  setStringDefault(scheduler, "state_file", "branch_power_upload_scheduler_state.json");
}

function applySchedulerDefaults(cfg) {
  setBooleanDefault(cfg.scheduler, "enabled", true);
  setBooleanDefault(cfg.scheduler, "auto_start_in_gui", false);
  setNumberDefault(cfg.scheduler, "interval_minutes", 60);
  setNumberDefault(cfg.scheduler, "check_interval_sec", 30);
  setBooleanDefault(cfg.scheduler, "retry_failed_on_next_tick", true);
  setStringDefault(cfg.scheduler, "state_file", "daily_scheduler_state.json");
}

function applyUpdaterDefaults(cfg) {
  setBooleanDefault(cfg.updater, "enabled", true);
  setNumberDefault(cfg.updater, "check_interval_sec", 3600);
  setBooleanDefault(cfg.updater, "auto_apply", false);
  setBooleanDefault(cfg.updater, "auto_restart", true);
  setBooleanDefault(cfg.updater, "allow_downgrade", false);
  setStringDefault(cfg.updater, "gitee_repo", "https://gitee.com/myligitt/qjpt.git");
  setStringDefault(cfg.updater, "gitee_branch", "master");
  setStringDefault(cfg.updater, "gitee_subdir", "updates/patches");
  setStringDefault(cfg.updater, "gitee_manifest_path", "updates/latest_patch.json");
  setNumberDefault(cfg.updater, "request_timeout_sec", 20);
  setNumberDefault(cfg.updater, "download_retry_count", 3);
  setStringDefault(cfg.updater, "state_file", "updater_state.json");
  setStringDefault(cfg.updater, "download_dir", "runtime_state/updater/downloads");
  setStringDefault(cfg.updater, "backup_dir", "runtime_state/updater/backups");
  setNumberDefault(cfg.updater, "max_backups", 3);
}

export function ensureConfigShape(raw) {
  const cfg = clone(raw || {});
  ensureRoot(cfg);
  applyDeploymentDefaults(cfg);
  setBooleanDefault(cfg.output, "save_json", false);
  applyDownloadDefaults(cfg);
  applyInternalSourceSiteDefaults(cfg);
  applyHandoverDefaults(cfg);
  applyAlarmExportDefaults(cfg);
  applyNetworkDefaults(cfg);
  applyWetBulbCollectionDefaults(cfg);
  applyDayMetricUploadDefaults(cfg);
  applyBranchPowerUploadDefaults(cfg);
  setBooleanDefault(cfg.manual_upload_gui, "enabled", true);
  applySchedulerDefaults(cfg);
  applyUpdaterDefaults(cfg);
  cfg.download.sites = Array.isArray(cfg.download.sites) ? cfg.download.sites : [];
  cfg.input.buildings = Array.isArray(cfg.input.buildings) ? cfg.input.buildings : [];
  return cfg;
}
