import {
  buildSheetRulesObject,
  clone,
  isValidHms,
  normalizeDatetimeLocalToApi,
  normalizeRunTimeText,
  normalizeSheetRules,
} from "./config_helpers.js";
import {
  cleanupAlarmExportCompat,
  cleanupDayMetricUploadCompat,
  cleanupWetBulbCollectionCompat,
} from "./config_compat_cleanup.js";

function joinPathText(base, child) {
  const baseText = String(base || "").trim().replace(/[\\/]+$/, "");
  const childText = String(child || "").trim().replace(/^[\\/]+/, "");
  if (!baseText) return childText;
  if (!childText) return baseText;
  const separator = baseText.includes("\\") || baseText.includes(":") ? "\\" : "/";
  return `${baseText}${separator}${childText}`;
}

function hasMeaningfulSheetRuleRows(rows) {
  return (Array.isArray(rows) ? rows : []).some((row) => {
    const sheetName = String(row?.sheet_name || "").trim();
    const tableId = String(row?.table_id || "").trim();
    return Boolean(sheetName || tableId);
  });
}

function normalizeHandoverRuleRow(raw) {
  const row = { ...(raw || {}) };
  row.id = String(row.id || "").trim();
  row.enabled = Boolean(row.enabled);
  row.target_cell = String(row.target_cell || "").trim().toUpperCase();
  row.rule_type = String(row.rule_type || "direct").trim().toLowerCase();
  row.match_mode = String(row.match_mode || "contains_casefold").trim().toLowerCase();
  row.agg = String(row.agg || "first").trim().toLowerCase();
  row.template = String(row.template || "{value}").trim() || "{value}";
  row.computed_op = String(row.computed_op || "").trim();
  row.params = row.params && typeof row.params === "object" ? { ...row.params } : {};
  row.d_keywords = Array.isArray(row.d_keywords)
    ? row.d_keywords.map((x) => String(x || "").trim()).filter(Boolean)
    : [];
  return row;
}

function validateAndNormalizeHandoverCellRules(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.cell_rules = payload.handover_log.cell_rules || {};
  const cellRules = payload.handover_log.cell_rules;

  const normalizeRows = (rows) =>
    (Array.isArray(rows) ? rows : [])
      .map(normalizeHandoverRuleRow)
      .filter((row) => row.id || row.target_cell || row.d_keywords.length || row.computed_op);

  cellRules.default_rows = normalizeRows(cellRules.default_rows);
  if (!cellRules.default_rows.length) {
    return { ok: false, error: "交接班默认对应关系不能为空" };
  }

  cellRules.building_rows =
    cellRules.building_rows && typeof cellRules.building_rows === "object" ? cellRules.building_rows : {};
  Object.keys(cellRules.building_rows).forEach((building) => {
    cellRules.building_rows[building] = normalizeRows(cellRules.building_rows[building]);
  });

  const validRuleType = new Set(["direct", "aggregate", "computed"]);
  const validAgg = new Set(["first", "max", "min"]);
  const cellPattern = /^[A-Z]+[1-9]\d*$/;

  const validateScope = (scopeRows, scopeName) => {
    const seenId = new Set();
    for (let i = 0; i < scopeRows.length; i += 1) {
      const row = scopeRows[i];
      if (!row.id) return `${scopeName} 第${i + 1}行规则ID不能为空`;
      if (seenId.has(row.id)) return `${scopeName} 存在重复规则ID: ${row.id}`;
      seenId.add(row.id);
      if (row.target_cell && !cellPattern.test(row.target_cell)) {
        return `${scopeName} 第${i + 1}行单元格格式错误: ${row.target_cell}`;
      }
      if (!validRuleType.has(row.rule_type)) return `${scopeName} 第${i + 1}行规则类型错误: ${row.rule_type}`;
      if (!validAgg.has(row.agg)) return `${scopeName} 第${i + 1}行聚合方式错误: ${row.agg}`;
      if (row.rule_type === "computed" && !row.computed_op) return `${scopeName} 第${i + 1}行计算类型不能为空`;
      if ((row.rule_type === "direct" || row.rule_type === "aggregate") && !row.d_keywords.length) {
        return `${scopeName} 第${i + 1}行关键词不能为空`;
      }
    }
    return "";
  };

  let err = validateScope(cellRules.default_rows, "全局默认");
  if (err) return { ok: false, error: err };
  for (const [building, rows] of Object.entries(cellRules.building_rows)) {
    err = validateScope(rows, `${building}覆盖`);
    if (err) return { ok: false, error: err };
  }

  delete payload.handover_log.rules;
  delete payload.handover_log.cell_mapping;
  delete payload.handover_log.format_templates;
  delete payload.handover_log.building_overrides;
  return { ok: true };
}

function validateAndNormalizeHandoverTemplate(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.template = payload.handover_log.template || {};
  const template = payload.handover_log.template;

  template.apply_building_title = true;
  template.title_cell = "A1";
  template.building_title_pattern = "EA118机房{building_code}栋数据中心交接班日志";
  template.building_title_map = {
    "A楼": "EA118机房A栋数据中心交接班日志",
    "B楼": "EA118机房B栋数据中心交接班日志",
    "C楼": "EA118机房C栋数据中心交接班日志",
    "D楼": "EA118机房D栋数据中心交接班日志",
    "E楼": "EA118机房E栋数据中心交接班日志",
  };
  return { ok: true };
}

function validateAndNormalizeHandoverDownload(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.download = payload.handover_log.download || {};
  const download = payload.handover_log.download;
  download.shift_windows = download.shift_windows && typeof download.shift_windows === "object" ? download.shift_windows : {};
  download.shift_windows.day =
    download.shift_windows.day && typeof download.shift_windows.day === "object" ? download.shift_windows.day : {};
  download.shift_windows.night =
    download.shift_windows.night && typeof download.shift_windows.night === "object" ? download.shift_windows.night : {};

  download.template_name = String(download.template_name || "").trim();
  download.scale_label = String(download.scale_label || "").trim();
  download.export_button_text = String(download.export_button_text || "原样导出").trim() || "原样导出";
  download.query_result_timeout_ms = Number.parseInt(download.query_result_timeout_ms ?? 20000, 10);
  download.download_event_timeout_ms = Number.parseInt(download.download_event_timeout_ms ?? 120000, 10);
  download.login_fill_timeout_ms = Number.parseInt(download.login_fill_timeout_ms ?? 5000, 10);
  download.menu_visible_timeout_ms = Number.parseInt(download.menu_visible_timeout_ms ?? 20000, 10);
  download.iframe_timeout_ms = Number.parseInt(download.iframe_timeout_ms ?? 15000, 10);
  download.start_end_visible_timeout_ms = Number.parseInt(download.start_end_visible_timeout_ms ?? 5000, 10);
  download.page_refresh_retry_count = Number.parseInt(download.page_refresh_retry_count ?? 1, 10);
  download.max_retries = Number.parseInt(download.max_retries ?? 2, 10);
  download.retry_wait_sec = Number.parseInt(download.retry_wait_sec ?? 2, 10);
  download.site_start_delay_sec = Number.parseInt(download.site_start_delay_sec ?? 1, 10);
  download.parallel_by_building = Boolean(download.parallel_by_building);

  if (!download.template_name) return { ok: false, error: "交接班报表模板名称不能为空" };
  if (!download.scale_label) return { ok: false, error: "交接班查询刻度不能为空" };

  if (!isValidHms(download.shift_windows.day.start)) {
    return { ok: false, error: "交接班白班开始时间格式错误，必须是 HH:MM:SS" };
  }
  if (!isValidHms(download.shift_windows.day.end)) {
    return { ok: false, error: "交接班白班结束时间格式错误，必须是 HH:MM:SS" };
  }
  if (!isValidHms(download.shift_windows.night.start)) {
    return { ok: false, error: "交接班夜班开始时间格式错误，必须是 HH:MM:SS" };
  }
  if (!isValidHms(download.shift_windows.night.end_next_day)) {
    return { ok: false, error: "交接班夜班结束时间格式错误，必须是 HH:MM:SS" };
  }

  const positiveFields = [
    ["query_result_timeout_ms", "查询结果等待超时"],
    ["download_event_timeout_ms", "导出下载等待超时"],
    ["login_fill_timeout_ms", "登录识别等待超时"],
    ["menu_visible_timeout_ms", "菜单等待超时"],
    ["iframe_timeout_ms", "iframe等待超时"],
    ["start_end_visible_timeout_ms", "时间输入框等待超时"],
    ["max_retries", "楼栋下载重试次数"],
  ];
  for (const [key, label] of positiveFields) {
    if (!Number.isInteger(download[key]) || download[key] <= 0) {
      return { ok: false, error: `交接班${label}必须大于0` };
    }
  }

  const nonNegativeFields = [
    ["lookback_minutes", "默认回看分钟数"],
    ["page_refresh_retry_count", "页面重试次数"],
    ["retry_wait_sec", "重试等待秒数"],
    ["site_start_delay_sec", "并发启动间隔秒数"],
  ];
  for (const [key, label] of nonNegativeFields) {
    download[key] = Number.parseInt(download[key] ?? 0, 10);
    if (!Number.isInteger(download[key]) || download[key] < 0) {
      return { ok: false, error: `交接班${label}必须大于等于0` };
    }
  }

  return { ok: true };
}

function validateAndNormalizeHandoverShiftRoster(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.shift_roster = payload.handover_log.shift_roster || {};
  const roster = payload.handover_log.shift_roster;

  roster.enabled = Boolean(roster.enabled);
  roster.source = roster.source && typeof roster.source === "object" ? roster.source : {};
  roster.fields = roster.fields && typeof roster.fields === "object" ? roster.fields : {};
  roster.cells = roster.cells && typeof roster.cells === "object" ? roster.cells : {};
  roster.match = roster.match && typeof roster.match === "object" ? roster.match : {};
  roster.shift_alias = roster.shift_alias && typeof roster.shift_alias === "object" ? roster.shift_alias : {};

  roster.source.app_token = String(roster.source.app_token || "").trim();
  roster.source.table_id = String(roster.source.table_id || "").trim();
  roster.source.page_size = Number.parseInt(roster.source.page_size ?? 500, 10);
  roster.source.max_records = Number.parseInt(roster.source.max_records ?? 5000, 10);

  roster.fields.duty_date = String(roster.fields.duty_date || "").trim();
  roster.fields.building = String(roster.fields.building || "").trim();
  roster.fields.team = String(roster.fields.team || "").trim();
  roster.fields.shift = String(roster.fields.shift || "").trim();
  roster.fields.people_text = String(roster.fields.people_text || "").trim();

  roster.cells.current_people = String(roster.cells.current_people || "").trim().toUpperCase();
  roster.cells.next_people = String(roster.cells.next_people || "").trim().toUpperCase();
  if (typeof roster.cells.next_first_person_cells === "string") {
    roster.cells.next_first_person_cells = roster.cells.next_first_person_cells
      .split(/[，,;\s\r\n\t]+/)
      .map((x) => String(x || "").trim().toUpperCase())
      .filter(Boolean);
  }
  if (!Array.isArray(roster.cells.next_first_person_cells)) {
    roster.cells.next_first_person_cells = [];
  }
  roster.cells.next_first_person_cells = roster.cells.next_first_person_cells
    .map((x) => String(x || "").trim().toUpperCase())
    .filter(Boolean);

  roster.match.building_mode = String(roster.match.building_mode || "exact_then_code").trim().toLowerCase();

  const normalizeAlias = (raw, fallback) => {
    if (typeof raw === "string") {
      return raw
        .split(/[，,;\s\r\n\t]+/)
        .map((x) => String(x || "").trim())
        .filter(Boolean);
    }
    if (Array.isArray(raw)) {
      const out = raw.map((x) => String(x || "").trim()).filter(Boolean);
      return out.length ? out : fallback;
    }
    return fallback;
  };
  roster.shift_alias.day = normalizeAlias(roster.shift_alias.day, ["白班", "day", "DAY"]);
  roster.shift_alias.night = normalizeAlias(roster.shift_alias.night, ["夜班", "night", "NIGHT"]);
  roster.people_split_regex = String(roster.people_split_regex || "").trim() || "[、,/，；;\\s]+";

  if (!Number.isInteger(roster.source.page_size) || roster.source.page_size <= 0) {
    return { ok: false, error: "交接班排班读取 page_size 必须大于0" };
  }
  if (!Number.isInteger(roster.source.max_records) || roster.source.max_records <= 0) {
    return { ok: false, error: "交接班排班读取 max_records 必须大于0" };
  }

  if (!roster.enabled) {
    return { ok: true };
  }

  if (!roster.source.app_token) return { ok: false, error: "交接班排班多维 app_token 不能为空" };
  if (!roster.source.table_id) return { ok: false, error: "交接班排班多维 table_id 不能为空" };
  if (!roster.fields.duty_date) return { ok: false, error: "交接班排班字段“排班日期”不能为空" };
  if (!roster.fields.building) return { ok: false, error: "交接班排班字段“机楼”不能为空" };
  if (!roster.fields.team) return { ok: false, error: "交接班排班字段“班组”不能为空" };
  if (!roster.fields.shift) return { ok: false, error: "交接班排班字段“班次”不能为空" };
  if (!roster.fields.people_text) return { ok: false, error: "交接班排班字段“值班人员（实际）”不能为空" };

  const cellPattern = /^[A-Z]+[1-9]\d*$/;
  if (!cellPattern.test(roster.cells.current_people)) {
    return { ok: false, error: "交接班排班 current_people 单元格格式错误（示例 C3）" };
  }
  if (!cellPattern.test(roster.cells.next_people)) {
    return { ok: false, error: "交接班排班 next_people 单元格格式错误（示例 G3）" };
  }
  if (!roster.cells.next_first_person_cells.length) {
    return { ok: false, error: "交接班排班 next_first_person_cells 至少要有一个单元格" };
  }
  for (const cell of roster.cells.next_first_person_cells) {
    if (!cellPattern.test(cell)) {
      return { ok: false, error: `交接班排班 next_first_person_cells 存在非法单元格: ${cell}` };
    }
  }
  if (roster.match.building_mode !== "exact_then_code") {
    return { ok: false, error: "交接班排班 building_mode 仅支持 exact_then_code" };
  }
  if (!roster.shift_alias.day.length || !roster.shift_alias.night.length) {
    return { ok: false, error: "交接班排班班次别名 day/night 不能为空" };
  }
  try {
    // eslint-disable-next-line no-new
    new RegExp(roster.people_split_regex);
  } catch (_err) {
    return { ok: false, error: "交接班排班人员分隔正则非法" };
  }

  roster.long_day = roster.long_day && typeof roster.long_day === "object" ? roster.long_day : {};
  roster.long_day.source =
    roster.long_day.source && typeof roster.long_day.source === "object" ? roster.long_day.source : {};
  roster.long_day.fields =
    roster.long_day.fields && typeof roster.long_day.fields === "object" ? roster.long_day.fields : {};
  roster.long_day.match =
    roster.long_day.match && typeof roster.long_day.match === "object" ? roster.long_day.match : {};
  roster.long_day.enabled = Boolean(roster.long_day.enabled);
  roster.long_day.source.app_token = String(roster.long_day.source.app_token || "").trim();
  roster.long_day.source.table_id = String(roster.long_day.source.table_id || "").trim();
  roster.long_day.source.page_size = Number.parseInt(roster.long_day.source.page_size ?? 500, 10);
  roster.long_day.source.max_records = Number.parseInt(roster.long_day.source.max_records ?? 5000, 10);
  roster.long_day.fields.duty_date = String(roster.long_day.fields.duty_date || "").trim();
  roster.long_day.fields.building = String(roster.long_day.fields.building || "").trim();
  roster.long_day.fields.shift = String(roster.long_day.fields.shift || "").trim();
  roster.long_day.fields.people_text = String(roster.long_day.fields.people_text || "").trim();
  roster.long_day.shift_value = String(roster.long_day.shift_value || "").trim();
  roster.long_day.day_cell = String(roster.long_day.day_cell || "").trim().toUpperCase();
  roster.long_day.night_cell = String(roster.long_day.night_cell || "").trim().toUpperCase();
  roster.long_day.prefix = String(roster.long_day.prefix || "").trim();
  roster.long_day.rest_text = String(roster.long_day.rest_text || "").trim();
  roster.long_day.match.building_mode = String(roster.long_day.match.building_mode || "exact_then_code")
    .trim()
    .toLowerCase();

  if (roster.long_day.enabled) {
    if (!roster.long_day.source.table_id) return { ok: false, error: "长白岗排班多维 table_id 不能为空" };
    if (!Number.isInteger(roster.long_day.source.page_size) || roster.long_day.source.page_size <= 0) {
      return { ok: false, error: "长白岗排班读取 page_size 必须大于0" };
    }
    if (!Number.isInteger(roster.long_day.source.max_records) || roster.long_day.source.max_records <= 0) {
      return { ok: false, error: "长白岗排班读取 max_records 必须大于0" };
    }
    if (!roster.long_day.fields.duty_date || !roster.long_day.fields.building || !roster.long_day.fields.people_text) {
      return { ok: false, error: "长白岗排班字段映射不能为空" };
    }
    if (!cellPattern.test(roster.long_day.day_cell) || !cellPattern.test(roster.long_day.night_cell)) {
      return { ok: false, error: "长白岗填充单元格格式错误（示例 B4/F4）" };
    }
  }

  roster.engineer_directory =
    roster.engineer_directory && typeof roster.engineer_directory === "object" ? roster.engineer_directory : {};
  roster.engineer_directory.source =
    roster.engineer_directory.source && typeof roster.engineer_directory.source === "object"
      ? roster.engineer_directory.source
      : {};
  roster.engineer_directory.fields =
    roster.engineer_directory.fields && typeof roster.engineer_directory.fields === "object"
      ? roster.engineer_directory.fields
      : {};
  roster.engineer_directory.delivery =
    roster.engineer_directory.delivery && typeof roster.engineer_directory.delivery === "object"
      ? roster.engineer_directory.delivery
      : {};
  roster.engineer_directory.match =
    roster.engineer_directory.match && typeof roster.engineer_directory.match === "object"
      ? roster.engineer_directory.match
      : {};
  roster.engineer_directory.enabled = Boolean(roster.engineer_directory.enabled);
  roster.engineer_directory.source.app_token = String(roster.engineer_directory.source.app_token || "").trim();
  roster.engineer_directory.source.table_id = String(roster.engineer_directory.source.table_id || "").trim();
  roster.engineer_directory.source.page_size = Number.parseInt(roster.engineer_directory.source.page_size ?? 500, 10);
  roster.engineer_directory.source.max_records = Number.parseInt(roster.engineer_directory.source.max_records ?? 5000, 10);
  roster.engineer_directory.fields.building = String(roster.engineer_directory.fields.building || "").trim();
  roster.engineer_directory.fields.specialty = String(roster.engineer_directory.fields.specialty || "").trim();
  roster.engineer_directory.fields.supervisor_text = String(roster.engineer_directory.fields.supervisor_text || "").trim();
  roster.engineer_directory.fields.supervisor_person = String(roster.engineer_directory.fields.supervisor_person || "").trim();
  roster.engineer_directory.fields.position = String(roster.engineer_directory.fields.position || "").trim();
  roster.engineer_directory.fields.recipient_id = String(roster.engineer_directory.fields.recipient_id || "").trim();
  roster.engineer_directory.delivery.receive_id_type = String(roster.engineer_directory.delivery.receive_id_type || "user_id")
    .trim()
    .toLowerCase();
  roster.engineer_directory.delivery.position_keyword = String(roster.engineer_directory.delivery.position_keyword || "").trim();
  roster.engineer_directory.match.building_mode = String(roster.engineer_directory.match.building_mode || "exact_then_code")
    .trim()
    .toLowerCase();

  if (roster.engineer_directory.enabled) {
    if (!roster.engineer_directory.source.table_id) return { ok: false, error: "工程师目录多维 table_id 不能为空" };
    if (!Number.isInteger(roster.engineer_directory.source.page_size) || roster.engineer_directory.source.page_size <= 0) {
      return { ok: false, error: "工程师目录读取 page_size 必须大于0" };
    }
    if (!Number.isInteger(roster.engineer_directory.source.max_records) || roster.engineer_directory.source.max_records <= 0) {
      return { ok: false, error: "工程师目录读取 max_records 必须大于0" };
    }
    if (
      !roster.engineer_directory.fields.building
      || !roster.engineer_directory.fields.specialty
      || !roster.engineer_directory.fields.supervisor_text
    ) {
      return { ok: false, error: "工程师目录字段映射不能为空" };
    }
    if (!roster.engineer_directory.delivery.receive_id_type) {
      return { ok: false, error: "工程师目录发送 receive_id_type 不能为空" };
    }
    if (!roster.engineer_directory.delivery.position_keyword) {
      return { ok: false, error: "工程师目录发送职位关键字不能为空" };
    }
  }

  return { ok: true };
}

function validateAndNormalizeHandoverEventSections(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.event_sections = payload.handover_log.event_sections || {};
  const eventSections = payload.handover_log.event_sections;

  eventSections.enabled = Boolean(eventSections.enabled);
  eventSections.source = eventSections.source && typeof eventSections.source === "object" ? eventSections.source : {};
  eventSections.duty_window =
    eventSections.duty_window && typeof eventSections.duty_window === "object" ? eventSections.duty_window : {};
  eventSections.fields = eventSections.fields && typeof eventSections.fields === "object" ? eventSections.fields : {};
  eventSections.sections = eventSections.sections && typeof eventSections.sections === "object" ? eventSections.sections : {};
  eventSections.column_mapping =
    eventSections.column_mapping && typeof eventSections.column_mapping === "object" ? eventSections.column_mapping : {};
  eventSections.column_mapping.header_alias =
    eventSections.column_mapping.header_alias && typeof eventSections.column_mapping.header_alias === "object"
      ? eventSections.column_mapping.header_alias
      : {};
  eventSections.column_mapping.fallback_cols =
    eventSections.column_mapping.fallback_cols && typeof eventSections.column_mapping.fallback_cols === "object"
      ? eventSections.column_mapping.fallback_cols
      : {};
  eventSections.progress_text =
    eventSections.progress_text && typeof eventSections.progress_text === "object" ? eventSections.progress_text : {};
  eventSections.cache = eventSections.cache && typeof eventSections.cache === "object" ? eventSections.cache : {};

  eventSections.source.app_token = String(eventSections.source.app_token || "").trim();
  eventSections.source.table_id = String(eventSections.source.table_id || "").trim();
  eventSections.source.page_size = Number.parseInt(eventSections.source.page_size ?? 500, 10);
  eventSections.source.max_records = Number.parseInt(eventSections.source.max_records ?? 5000, 10);

  eventSections.duty_window.day_start = normalizeRunTimeText(eventSections.duty_window.day_start);
  eventSections.duty_window.day_end = normalizeRunTimeText(eventSections.duty_window.day_end);
  eventSections.duty_window.night_start = normalizeRunTimeText(eventSections.duty_window.night_start);
  eventSections.duty_window.night_end_next_day = normalizeRunTimeText(eventSections.duty_window.night_end_next_day);
  eventSections.duty_window.boundary_mode = String(
    eventSections.duty_window.boundary_mode || "left_closed_right_open",
  )
    .trim()
    .toLowerCase();

  eventSections.fields.event_time = String(eventSections.fields.event_time || "").trim();
  eventSections.fields.building = String(eventSections.fields.building || "").trim();
  eventSections.fields.event_level = String(eventSections.fields.event_level || "").trim();
  eventSections.fields.description = String(eventSections.fields.description || "").trim();
  eventSections.fields.exclude_checked = String(eventSections.fields.exclude_checked || "").trim();
  eventSections.fields.final_status = String(eventSections.fields.final_status || "").trim();
  eventSections.fields.exclude_duration = String(eventSections.fields.exclude_duration || "").trim();
  eventSections.fields.exclude_duration_value = String(eventSections.fields.exclude_duration_value || "").trim();
  eventSections.fields.to_maint = String(eventSections.fields.to_maint || "").trim();
  eventSections.fields.maint_done_time = String(eventSections.fields.maint_done_time || "").trim();
  eventSections.fields.event_done_time = String(eventSections.fields.event_done_time || "").trim();

  eventSections.sections.new_event = String(eventSections.sections.new_event || "").trim();
  eventSections.sections.history_followup = String(eventSections.sections.history_followup || "").trim();

  eventSections.column_mapping.resolve_by_header = Boolean(eventSections.column_mapping.resolve_by_header);
  const normalizeAlias = (raw) => {
    if (typeof raw === "string") {
      return raw.split(/[，,;\s\r\n\t]+/).map((x) => String(x || "").trim()).filter(Boolean);
    }
    if (Array.isArray(raw)) {
      return raw.map((x) => String(x || "").trim()).filter(Boolean);
    }
    return [];
  };
  const aliasKeys = ["event_level", "event_time", "description", "work_window", "progress", "follower"];
  for (const key of aliasKeys) {
    eventSections.column_mapping.header_alias[key] = normalizeAlias(eventSections.column_mapping.header_alias[key]);
    eventSections.column_mapping.fallback_cols[key] = String(
      eventSections.column_mapping.fallback_cols[key] || "",
    )
      .trim()
      .toUpperCase();
  }

  eventSections.progress_text.done = String(eventSections.progress_text.done || "").trim();
  eventSections.progress_text.todo = String(eventSections.progress_text.todo || "").trim();
  eventSections.cache.enabled = Boolean(eventSections.cache.enabled);
  eventSections.cache.state_file = String(eventSections.cache.state_file || "").trim();
  eventSections.cache.max_pending = Number.parseInt(eventSections.cache.max_pending ?? 20000, 10);
  eventSections.cache.max_last_query_ids = Number.parseInt(eventSections.cache.max_last_query_ids ?? 5000, 10);

  if (!eventSections.enabled) {
    return { ok: true };
  }
  if (!eventSections.source.app_token) return { ok: false, error: "事件分类多维 app_token 不能为空" };
  if (!eventSections.source.table_id) return { ok: false, error: "事件分类多维 table_id 不能为空" };
  if (!Number.isInteger(eventSections.source.page_size) || eventSections.source.page_size <= 0) {
    return { ok: false, error: "事件分类 page_size 必须大于0" };
  }
  if (!Number.isInteger(eventSections.source.max_records) || eventSections.source.max_records <= 0) {
    return { ok: false, error: "事件分类 max_records 必须大于0" };
  }
  if (!isValidHms(eventSections.duty_window.day_start)) return { ok: false, error: "事件分类白班开始时间格式错误（HH:MM:SS）" };
  if (!isValidHms(eventSections.duty_window.day_end)) return { ok: false, error: "事件分类白班结束时间格式错误（HH:MM:SS）" };
  if (!isValidHms(eventSections.duty_window.night_start)) return { ok: false, error: "事件分类夜班开始时间格式错误（HH:MM:SS）" };
  if (!isValidHms(eventSections.duty_window.night_end_next_day)) {
    return { ok: false, error: "事件分类夜班结束（次日）时间格式错误（HH:MM:SS）" };
  }
  if (eventSections.duty_window.boundary_mode !== "left_closed_right_open") {
    return { ok: false, error: "事件分类 boundary_mode 仅支持 left_closed_right_open" };
  }
  if (
    !eventSections.fields.event_time ||
    !eventSections.fields.building ||
    !eventSections.fields.event_level ||
    !eventSections.fields.description
  ) {
    return { ok: false, error: "事件分类字段映射（时间/机楼/等级/描述）不能为空" };
  }
  if (!eventSections.fields.exclude_checked || !eventSections.fields.final_status) {
    return { ok: false, error: "事件分类字段映射（不计入事件/最终状态）不能为空" };
  }
  if (!eventSections.fields.to_maint || !eventSections.fields.maint_done_time || !eventSections.fields.event_done_time) {
    return { ok: false, error: "事件分类进展字段映射不能为空" };
  }
  if (!eventSections.sections.new_event || !eventSections.sections.history_followup) {
    return { ok: false, error: "事件分类名称（新事件/历史跟进）不能为空" };
  }
  for (const key of aliasKeys) {
    if (!eventSections.column_mapping.header_alias[key].length) {
      return { ok: false, error: `事件分类表头别名不能为空: ${key}` };
    }
    if (!/^[A-Z]+$/.test(eventSections.column_mapping.fallback_cols[key])) {
      return { ok: false, error: `事件分类回退列名非法: ${key}` };
    }
  }
  if (!eventSections.progress_text.done || !eventSections.progress_text.todo) {
    return { ok: false, error: "事件分类进展文案（已完成/未完成）不能为空" };
  }
  if (!eventSections.cache.state_file) return { ok: false, error: "事件分类缓存状态文件不能为空" };
  if (!Number.isInteger(eventSections.cache.max_pending) || eventSections.cache.max_pending <= 0) {
    return { ok: false, error: "事件分类缓存 max_pending 必须大于0" };
  }
  if (!Number.isInteger(eventSections.cache.max_last_query_ids) || eventSections.cache.max_last_query_ids <= 0) {
    return { ok: false, error: "事件分类缓存 max_last_query_ids 必须大于0" };
  }
  return { ok: true };
}

function validateAndNormalizeHandoverChangeManagementSection(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.change_management_section = payload.handover_log.change_management_section || {};
  const sectionCfg = payload.handover_log.change_management_section;

  sectionCfg.enabled = Boolean(sectionCfg.enabled);
  sectionCfg.source = sectionCfg.source && typeof sectionCfg.source === "object" ? sectionCfg.source : {};
  sectionCfg.fields = sectionCfg.fields && typeof sectionCfg.fields === "object" ? sectionCfg.fields : {};
  sectionCfg.monthly_report_fields =
    sectionCfg.monthly_report_fields && typeof sectionCfg.monthly_report_fields === "object"
      ? sectionCfg.monthly_report_fields
      : {};
  sectionCfg.sections = sectionCfg.sections && typeof sectionCfg.sections === "object" ? sectionCfg.sections : {};
  sectionCfg.column_mapping =
    sectionCfg.column_mapping && typeof sectionCfg.column_mapping === "object" ? sectionCfg.column_mapping : {};
  sectionCfg.column_mapping.header_alias =
    sectionCfg.column_mapping.header_alias && typeof sectionCfg.column_mapping.header_alias === "object"
      ? sectionCfg.column_mapping.header_alias
      : {};
  sectionCfg.column_mapping.fallback_cols =
    sectionCfg.column_mapping.fallback_cols && typeof sectionCfg.column_mapping.fallback_cols === "object"
      ? sectionCfg.column_mapping.fallback_cols
      : {};
  sectionCfg.work_window_text =
    sectionCfg.work_window_text && typeof sectionCfg.work_window_text === "object" ? sectionCfg.work_window_text : {};

  sectionCfg.source.app_token = String(sectionCfg.source.app_token || "").trim();
  sectionCfg.source.table_id = String(sectionCfg.source.table_id || "").trim();
  sectionCfg.source.page_size = Number.parseInt(sectionCfg.source.page_size ?? 500, 10);
  sectionCfg.source.max_records = Number.parseInt(sectionCfg.source.max_records ?? 5000, 10);

  sectionCfg.fields.building = String(sectionCfg.fields.building || "").trim();
  sectionCfg.fields.start_time = String(sectionCfg.fields.start_time || "").trim();
  sectionCfg.fields.end_time = String(sectionCfg.fields.end_time || "").trim();
  sectionCfg.fields.updated_time = String(sectionCfg.fields.updated_time || "").trim();
  sectionCfg.fields.change_level = String(sectionCfg.fields.change_level || "").trim();
  sectionCfg.fields.process_updates = String(sectionCfg.fields.process_updates || "").trim();
  sectionCfg.fields.description = String(sectionCfg.fields.description || "").trim();
  sectionCfg.fields.specialty = String(sectionCfg.fields.specialty || "").trim();
  sectionCfg.monthly_report_fields.building = String(sectionCfg.monthly_report_fields.building || "").trim();
  sectionCfg.monthly_report_fields.change_code = String(sectionCfg.monthly_report_fields.change_code || "").trim();
  sectionCfg.monthly_report_fields.name = String(sectionCfg.monthly_report_fields.name || "").trim();
  sectionCfg.monthly_report_fields.location = String(sectionCfg.monthly_report_fields.location || "").trim();
  sectionCfg.monthly_report_fields.change_level = String(sectionCfg.monthly_report_fields.change_level || "").trim();
  sectionCfg.monthly_report_fields.status = String(sectionCfg.monthly_report_fields.status || "").trim();
  sectionCfg.monthly_report_fields.start_time = String(sectionCfg.monthly_report_fields.start_time || "").trim();
  sectionCfg.monthly_report_fields.end_time = String(sectionCfg.monthly_report_fields.end_time || "").trim();

  sectionCfg.sections.change_management = String(sectionCfg.sections.change_management || "").trim();
  sectionCfg.column_mapping.resolve_by_header = Boolean(sectionCfg.column_mapping.resolve_by_header);

  const normalizeAlias = (raw) => {
    if (typeof raw === "string") {
      return raw.split(/[，,;\s\r\n\t]+/).map((x) => String(x || "").trim()).filter(Boolean);
    }
    if (Array.isArray(raw)) {
      return raw.map((x) => String(x || "").trim()).filter(Boolean);
    }
    return [];
  };
  const aliasKeys = ["change_level", "work_window", "description", "executor"];
  for (const key of aliasKeys) {
    sectionCfg.column_mapping.header_alias[key] = normalizeAlias(sectionCfg.column_mapping.header_alias[key]);
    sectionCfg.column_mapping.fallback_cols[key] = String(sectionCfg.column_mapping.fallback_cols[key] || "")
      .trim()
      .toUpperCase();
  }

  sectionCfg.work_window_text.day_anchor = normalizeRunTimeText(sectionCfg.work_window_text.day_anchor);
  sectionCfg.work_window_text.day_default_end = normalizeRunTimeText(sectionCfg.work_window_text.day_default_end);
  sectionCfg.work_window_text.night_anchor = normalizeRunTimeText(sectionCfg.work_window_text.night_anchor);
  sectionCfg.work_window_text.night_default_end_next_day = normalizeRunTimeText(
    sectionCfg.work_window_text.night_default_end_next_day,
  );

  if (!sectionCfg.enabled) {
    return { ok: true };
  }
  if (!sectionCfg.source.app_token) return { ok: false, error: "变更管理多维 app_token 不能为空" };
  if (!sectionCfg.source.table_id) return { ok: false, error: "变更管理多维 table_id 不能为空" };
  if (!Number.isInteger(sectionCfg.source.page_size) || sectionCfg.source.page_size <= 0) {
    return { ok: false, error: "变更管理 page_size 必须大于0" };
  }
  if (!Number.isInteger(sectionCfg.source.max_records) || sectionCfg.source.max_records <= 0) {
    return { ok: false, error: "变更管理 max_records 必须大于0" };
  }
  if (
    !sectionCfg.fields.building ||
    !sectionCfg.fields.start_time ||
    !sectionCfg.fields.end_time ||
    !sectionCfg.fields.updated_time ||
    !sectionCfg.fields.change_level ||
    !sectionCfg.fields.process_updates ||
    !sectionCfg.fields.description ||
    !sectionCfg.fields.specialty
  ) {
    return { ok: false, error: "变更管理字段映射（楼栋/变更开始时间/变更结束时间/更新时间回退/变更等级/过程更新时间/名称/专业）不能为空" };
  }
  if (
    !sectionCfg.monthly_report_fields.building ||
    !sectionCfg.monthly_report_fields.change_code ||
    !sectionCfg.monthly_report_fields.name ||
    !sectionCfg.monthly_report_fields.location ||
    !sectionCfg.monthly_report_fields.change_level ||
    !sectionCfg.monthly_report_fields.status ||
    !sectionCfg.monthly_report_fields.start_time ||
    !sectionCfg.monthly_report_fields.end_time
  ) {
    return { ok: false, error: "变更月报字段映射（楼栋/变更编码/名称/位置/智航-变更等级/变更状态/变更开始时间/变更结束时间）不能为空" };
  }
  if (!sectionCfg.sections.change_management) {
    return { ok: false, error: "变更管理分类名不能为空" };
  }
  for (const key of aliasKeys) {
    if (!sectionCfg.column_mapping.header_alias[key].length) {
      return { ok: false, error: `变更管理表头别名不能为空: ${key}` };
    }
    if (!/^[A-Z]+$/.test(sectionCfg.column_mapping.fallback_cols[key])) {
      return { ok: false, error: `变更管理回退列名非法: ${key}` };
    }
  }
  if (!isValidHms(sectionCfg.work_window_text.day_anchor)) return { ok: false, error: "变更管理白班锚点时间格式错误（HH:MM:SS）" };
  if (!isValidHms(sectionCfg.work_window_text.day_default_end)) return { ok: false, error: "变更管理白班默认结束时间格式错误（HH:MM:SS）" };
  if (!isValidHms(sectionCfg.work_window_text.night_anchor)) return { ok: false, error: "变更管理夜班锚点时间格式错误（HH:MM:SS）" };
  if (!isValidHms(sectionCfg.work_window_text.night_default_end_next_day)) {
    return { ok: false, error: "变更管理夜班默认结束（次日）时间格式错误（HH:MM:SS）" };
  }
  return { ok: true };
}

function validateAndNormalizeHandoverExerciseManagementSection(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.exercise_management_section = payload.handover_log.exercise_management_section || {};
  const sectionCfg = payload.handover_log.exercise_management_section;

  sectionCfg.enabled = Boolean(sectionCfg.enabled);
  sectionCfg.source = sectionCfg.source && typeof sectionCfg.source === "object" ? sectionCfg.source : {};
  sectionCfg.fields = sectionCfg.fields && typeof sectionCfg.fields === "object" ? sectionCfg.fields : {};
  sectionCfg.sections = sectionCfg.sections && typeof sectionCfg.sections === "object" ? sectionCfg.sections : {};
  sectionCfg.fixed_values = sectionCfg.fixed_values && typeof sectionCfg.fixed_values === "object" ? sectionCfg.fixed_values : {};
  sectionCfg.column_mapping =
    sectionCfg.column_mapping && typeof sectionCfg.column_mapping === "object" ? sectionCfg.column_mapping : {};
  sectionCfg.column_mapping.header_alias =
    sectionCfg.column_mapping.header_alias && typeof sectionCfg.column_mapping.header_alias === "object"
      ? sectionCfg.column_mapping.header_alias
      : {};
  sectionCfg.column_mapping.fallback_cols =
    sectionCfg.column_mapping.fallback_cols && typeof sectionCfg.column_mapping.fallback_cols === "object"
      ? sectionCfg.column_mapping.fallback_cols
      : {};

  sectionCfg.source.app_token = String(sectionCfg.source.app_token || "").trim();
  sectionCfg.source.table_id = String(sectionCfg.source.table_id || "").trim();
  sectionCfg.source.page_size = Number.parseInt(sectionCfg.source.page_size ?? 500, 10);
  sectionCfg.source.max_records = Number.parseInt(sectionCfg.source.max_records ?? 5000, 10);

  sectionCfg.fields.building = String(sectionCfg.fields.building || "").trim();
  sectionCfg.fields.start_time = String(sectionCfg.fields.start_time || "").trim();
  sectionCfg.fields.project = String(sectionCfg.fields.project || "").trim();

  sectionCfg.sections.exercise_management = String(sectionCfg.sections.exercise_management || "").trim();
  sectionCfg.fixed_values.exercise_type = String(sectionCfg.fixed_values.exercise_type || "").trim();
  sectionCfg.fixed_values.completion = String(sectionCfg.fixed_values.completion || "").trim();
  sectionCfg.column_mapping.resolve_by_header = Boolean(sectionCfg.column_mapping.resolve_by_header);

  const normalizeAlias = (raw) => {
    if (typeof raw === "string") {
      return raw.split(/[，,;\s\r\n\t]+/).map((x) => String(x || "").trim()).filter(Boolean);
    }
    if (Array.isArray(raw)) {
      return raw.map((x) => String(x || "").trim()).filter(Boolean);
    }
    return [];
  };
  const aliasKeys = ["exercise_type", "exercise_item", "completion", "executor"];
  for (const key of aliasKeys) {
    sectionCfg.column_mapping.header_alias[key] = normalizeAlias(sectionCfg.column_mapping.header_alias[key]);
    sectionCfg.column_mapping.fallback_cols[key] = String(sectionCfg.column_mapping.fallback_cols[key] || "")
      .trim()
      .toUpperCase();
  }

  if (!sectionCfg.enabled) {
    return { ok: true };
  }
  if (!sectionCfg.source.app_token) return { ok: false, error: "演练管理多维 app_token 不能为空" };
  if (!sectionCfg.source.table_id) return { ok: false, error: "演练管理多维 table_id 不能为空" };
  if (!Number.isInteger(sectionCfg.source.page_size) || sectionCfg.source.page_size <= 0) {
    return { ok: false, error: "演练管理 page_size 必须大于0" };
  }
  if (!Number.isInteger(sectionCfg.source.max_records) || sectionCfg.source.max_records <= 0) {
    return { ok: false, error: "演练管理 max_records 必须大于0" };
  }
  if (!sectionCfg.fields.building || !sectionCfg.fields.start_time || !sectionCfg.fields.project) {
    return { ok: false, error: "演练管理字段映射（机楼/演练开始时间/告警描述）不能为空" };
  }
  if (!sectionCfg.sections.exercise_management) {
    return { ok: false, error: "演练管理分类名不能为空" };
  }
  if (!sectionCfg.fixed_values.exercise_type || !sectionCfg.fixed_values.completion) {
    return { ok: false, error: "演练管理固定文案（演练类型/演练完成情况）不能为空" };
  }
  for (const key of aliasKeys) {
    if (!sectionCfg.column_mapping.header_alias[key].length) {
      return { ok: false, error: `演练管理表头别名不能为空: ${key}` };
    }
    if (!/^[A-Z]+$/.test(sectionCfg.column_mapping.fallback_cols[key])) {
      return { ok: false, error: `演练管理回退列名非法: ${key}` };
    }
  }
  return { ok: true };
}

function validateAndNormalizeHandoverMaintenanceManagementSection(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.maintenance_management_section = payload.handover_log.maintenance_management_section || {};
  const sectionCfg = payload.handover_log.maintenance_management_section;

  sectionCfg.enabled = Boolean(sectionCfg.enabled);
  sectionCfg.source = sectionCfg.source && typeof sectionCfg.source === "object" ? sectionCfg.source : {};
  sectionCfg.fields = sectionCfg.fields && typeof sectionCfg.fields === "object" ? sectionCfg.fields : {};
  sectionCfg.sections = sectionCfg.sections && typeof sectionCfg.sections === "object" ? sectionCfg.sections : {};
  sectionCfg.fixed_values = sectionCfg.fixed_values && typeof sectionCfg.fixed_values === "object" ? sectionCfg.fixed_values : {};
  sectionCfg.column_mapping =
    sectionCfg.column_mapping && typeof sectionCfg.column_mapping === "object" ? sectionCfg.column_mapping : {};
  sectionCfg.column_mapping.header_alias =
    sectionCfg.column_mapping.header_alias && typeof sectionCfg.column_mapping.header_alias === "object"
      ? sectionCfg.column_mapping.header_alias
      : {};
  sectionCfg.column_mapping.fallback_cols =
    sectionCfg.column_mapping.fallback_cols && typeof sectionCfg.column_mapping.fallback_cols === "object"
      ? sectionCfg.column_mapping.fallback_cols
      : {};

  sectionCfg.source.app_token = String(sectionCfg.source.app_token || "").trim();
  sectionCfg.source.table_id = String(sectionCfg.source.table_id || "").trim();
  sectionCfg.source.page_size = Number.parseInt(sectionCfg.source.page_size ?? 500, 10);
  sectionCfg.source.max_records = Number.parseInt(sectionCfg.source.max_records ?? 5000, 10);

  sectionCfg.fields.building = String(sectionCfg.fields.building || "").trim();
  sectionCfg.fields.start_time = String(sectionCfg.fields.start_time || "").trim();
  sectionCfg.fields.updated_time = String(sectionCfg.fields.updated_time || "").trim();
  sectionCfg.fields.actual_end_time = String(sectionCfg.fields.actual_end_time || "").trim();
  sectionCfg.fields.item = String(sectionCfg.fields.item || "").trim();
  sectionCfg.fields.specialty = String(sectionCfg.fields.specialty || "").trim();

  sectionCfg.sections.maintenance_management = String(sectionCfg.sections.maintenance_management || "").trim();
  sectionCfg.fixed_values.vendor_internal = String(sectionCfg.fixed_values.vendor_internal || "").trim();
  sectionCfg.fixed_values.vendor_external = String(sectionCfg.fixed_values.vendor_external || "").trim();
  sectionCfg.fixed_values.completion = String(sectionCfg.fixed_values.completion || "").trim();
  sectionCfg.column_mapping.resolve_by_header = Boolean(sectionCfg.column_mapping.resolve_by_header);

  const normalizeAlias = (raw) => {
    if (typeof raw === "string") {
      return raw.split(/[，,;\s\r\n\t]+/).map((x) => String(x || "").trim()).filter(Boolean);
    }
    if (Array.isArray(raw)) {
      return raw.map((x) => String(x || "").trim()).filter(Boolean);
    }
    return [];
  };
  const aliasKeys = ["maintenance_item", "maintenance_party", "completion", "executor"];
  for (const key of aliasKeys) {
    sectionCfg.column_mapping.header_alias[key] = normalizeAlias(sectionCfg.column_mapping.header_alias[key]);
    sectionCfg.column_mapping.fallback_cols[key] = String(sectionCfg.column_mapping.fallback_cols[key] || "")
      .trim()
      .toUpperCase();
  }

  if (!sectionCfg.enabled) {
    return { ok: true };
  }
  if (!sectionCfg.source.app_token) return { ok: false, error: "维护管理多维 app_token 不能为空" };
  if (!sectionCfg.source.table_id) return { ok: false, error: "维护管理多维 table_id 不能为空" };
  if (!Number.isInteger(sectionCfg.source.page_size) || sectionCfg.source.page_size <= 0) {
    return { ok: false, error: "维护管理 page_size 必须大于0" };
  }
  if (!Number.isInteger(sectionCfg.source.max_records) || sectionCfg.source.max_records <= 0) {
    return { ok: false, error: "维护管理 max_records 必须大于0" };
  }
  if (!sectionCfg.fields.building || !sectionCfg.fields.start_time || !sectionCfg.fields.updated_time || !sectionCfg.fields.actual_end_time || !sectionCfg.fields.item || !sectionCfg.fields.specialty) {
    return { ok: false, error: "维护管理字段映射（楼栋/实际开始时间/最新更新时间回退/实际结束时间/名称/专业）不能为空" };
  }
  if (!sectionCfg.sections.maintenance_management) {
    return { ok: false, error: "维护管理分类名不能为空" };
  }
  if (!sectionCfg.fixed_values.vendor_internal || !sectionCfg.fixed_values.vendor_external || !sectionCfg.fixed_values.completion) {
    return { ok: false, error: "维护管理固定文案（自维/厂维/维护完成情况）不能为空" };
  }
  for (const key of aliasKeys) {
    if (!sectionCfg.column_mapping.header_alias[key].length) {
      return { ok: false, error: `维护管理表头别名不能为空: ${key}` };
    }
    if (!/^[A-Z]+$/.test(sectionCfg.column_mapping.fallback_cols[key])) {
      return { ok: false, error: `维护管理回退列名非法: ${key}` };
    }
  }
  return { ok: true };
}

function validateAndNormalizeHandoverOtherImportantWorkSection(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.other_important_work_section = payload.handover_log.other_important_work_section || {};
  const sectionCfg = payload.handover_log.other_important_work_section;

  sectionCfg.enabled = Boolean(sectionCfg.enabled);
  sectionCfg.source = sectionCfg.source && typeof sectionCfg.source === "object" ? sectionCfg.source : {};
  sectionCfg.sections = sectionCfg.sections && typeof sectionCfg.sections === "object" ? sectionCfg.sections : {};
  sectionCfg.column_mapping =
    sectionCfg.column_mapping && typeof sectionCfg.column_mapping === "object" ? sectionCfg.column_mapping : {};
  sectionCfg.column_mapping.header_alias =
    sectionCfg.column_mapping.header_alias && typeof sectionCfg.column_mapping.header_alias === "object"
      ? sectionCfg.column_mapping.header_alias
      : {};
  sectionCfg.column_mapping.fallback_cols =
    sectionCfg.column_mapping.fallback_cols && typeof sectionCfg.column_mapping.fallback_cols === "object"
      ? sectionCfg.column_mapping.fallback_cols
      : {};
  sectionCfg.sources = sectionCfg.sources && typeof sectionCfg.sources === "object" ? sectionCfg.sources : {};

  sectionCfg.source.app_token = String(sectionCfg.source.app_token || "").trim();
  sectionCfg.source.page_size = Number.parseInt(sectionCfg.source.page_size ?? 500, 10);
  sectionCfg.source.max_records = Number.parseInt(sectionCfg.source.max_records ?? 5000, 10);
  sectionCfg.sections.other_important_work = String(sectionCfg.sections.other_important_work || "").trim();
  sectionCfg.order = Array.isArray(sectionCfg.order)
    ? sectionCfg.order.map((item) => String(item || "").trim()).filter(Boolean)
    : [];
  sectionCfg.column_mapping.resolve_by_header = Boolean(sectionCfg.column_mapping.resolve_by_header);

  const normalizeAlias = (raw) => {
    if (typeof raw === "string") {
      return raw.split(/[，,;\s\r\n\t]+/).map((x) => String(x || "").trim()).filter(Boolean);
    }
    if (Array.isArray(raw)) {
      return raw.map((x) => String(x || "").trim()).filter(Boolean);
    }
    return [];
  };
  const aliasKeys = ["description", "completion", "executor"];
  for (const key of aliasKeys) {
    sectionCfg.column_mapping.header_alias[key] = normalizeAlias(sectionCfg.column_mapping.header_alias[key]);
    sectionCfg.column_mapping.fallback_cols[key] = String(sectionCfg.column_mapping.fallback_cols[key] || "")
      .trim()
      .toUpperCase();
  }

  const sourceKeys = ["power_notice", "device_adjustment", "device_patrol", "device_repair"];
  for (const sourceKey of sourceKeys) {
    sectionCfg.sources[sourceKey] =
      sectionCfg.sources[sourceKey] && typeof sectionCfg.sources[sourceKey] === "object"
        ? sectionCfg.sources[sourceKey]
        : {};
    const current = sectionCfg.sources[sourceKey];
    current.label = String(current.label || "").trim();
    current.table_id = String(current.table_id || "").trim();
    current.fields = current.fields && typeof current.fields === "object" ? current.fields : {};
    current.fields.building = String(current.fields.building || "").trim();
    current.fields.actual_start_time = String(current.fields.actual_start_time || "").trim();
    current.fields.actual_end_time = String(current.fields.actual_end_time || "").trim();
    current.fields.description = String(current.fields.description || "").trim();
    current.fields.completion = String(current.fields.completion || "").trim();
    current.fields.specialty = String(current.fields.specialty || "").trim();
  }

  if (!sectionCfg.enabled) {
    return { ok: true };
  }
  if (!sectionCfg.source.app_token) return { ok: false, error: "其他重要工作记录多维 app_token 不能为空" };
  if (!Number.isInteger(sectionCfg.source.page_size) || sectionCfg.source.page_size <= 0) {
    return { ok: false, error: "其他重要工作记录 page_size 必须大于0" };
  }
  if (!Number.isInteger(sectionCfg.source.max_records) || sectionCfg.source.max_records <= 0) {
    return { ok: false, error: "其他重要工作记录 max_records 必须大于0" };
  }
  if (!sectionCfg.sections.other_important_work) {
    return { ok: false, error: "其他重要工作记录分类名不能为空" };
  }
  const expectedOrder = ["power_notice", "device_adjustment", "device_patrol", "device_repair"];
  if (sectionCfg.order.join(",") !== expectedOrder.join(",")) {
    return { ok: false, error: "其他重要工作记录顺序必须为：上电通告、设备调整、设备轮巡、设备检修" };
  }
  for (const key of aliasKeys) {
    if (!sectionCfg.column_mapping.header_alias[key].length) {
      return { ok: false, error: `其他重要工作记录表头别名不能为空: ${key}` };
    }
    if (!/^[A-Z]+$/.test(sectionCfg.column_mapping.fallback_cols[key])) {
      return { ok: false, error: `其他重要工作记录回退列名非法: ${key}` };
    }
  }
  for (const sourceKey of sourceKeys) {
    const current = sectionCfg.sources[sourceKey];
    if (!current.table_id) {
      return { ok: false, error: `其他重要工作记录来源 table_id 不能为空: ${sourceKey}` };
    }
    if (
      !current.fields.building ||
      !current.fields.actual_start_time ||
      !current.fields.actual_end_time ||
      !current.fields.description ||
      !current.fields.completion ||
      !current.fields.specialty
    ) {
      return { ok: false, error: `其他重要工作记录来源字段映射不能为空: ${sourceKey}` };
    }
  }
  return { ok: true };
}

function validateAndNormalizeDayMetricUploadTarget(payload) {
  payload.day_metric_upload = payload.day_metric_upload || {};
  payload.day_metric_upload.target =
    payload.day_metric_upload.target && typeof payload.day_metric_upload.target === "object"
      ? payload.day_metric_upload.target
      : {};
  payload.day_metric_upload.behavior =
    payload.day_metric_upload.behavior && typeof payload.day_metric_upload.behavior === "object"
      ? payload.day_metric_upload.behavior
      : {};
  const exportCfg = payload.day_metric_upload.target;
  exportCfg.source = exportCfg.source && typeof exportCfg.source === "object" ? exportCfg.source : {};
  exportCfg.fields = exportCfg.fields && typeof exportCfg.fields === "object" ? exportCfg.fields : {};
  exportCfg.missing_value_policy = String(exportCfg.missing_value_policy || "zero").trim().toLowerCase() || "zero";

  cleanupDayMetricUploadCompat(payload.day_metric_upload);
  delete exportCfg.types;

  exportCfg.source.app_token = String(exportCfg.source.app_token || "").trim();
  exportCfg.source.table_id = String(exportCfg.source.table_id || "").trim();
  exportCfg.source.create_batch_size = Number.parseInt(exportCfg.source.create_batch_size ?? 200, 10);

  exportCfg.fields.type = String(exportCfg.fields.type || "").trim();
  exportCfg.fields.building = String(exportCfg.fields.building || "").trim();
  exportCfg.fields.date = String(exportCfg.fields.date || "").trim();
  exportCfg.fields.value = String(exportCfg.fields.value || "").trim();
  exportCfg.fields.position_code = String(exportCfg.fields.position_code || "").trim();

  if (exportCfg.missing_value_policy !== "zero") {
    return { ok: false, error: "12项目标缺失值策略仅支持 zero" };
  }
  if (!exportCfg.source.app_token || !exportCfg.source.table_id) {
    return { ok: false, error: "12项目标配置不能为空，请填写 app_token 和 table_id" };
  }
  if (!Number.isInteger(exportCfg.source.create_batch_size) || exportCfg.source.create_batch_size <= 0) {
    return { ok: false, error: "12项目标批次大小必须大于0" };
  }
  if (
    !exportCfg.fields.type
    || !exportCfg.fields.building
    || !exportCfg.fields.date
    || !exportCfg.fields.value
    || !exportCfg.fields.position_code
  ) {
    return { ok: false, error: "12项目标字段映射（类型/楼栋/日期/数值/位置编号）不能为空" };
  }
  return { ok: true };
}

function validateAndNormalizeHandoverSourceDataAttachmentExport(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.source_data_attachment_export = payload.handover_log.source_data_attachment_export || {};
  const exportCfg = payload.handover_log.source_data_attachment_export;
  exportCfg.enabled = Boolean(exportCfg.enabled);
  exportCfg.upload_night_shift = Boolean(exportCfg.upload_night_shift);
  exportCfg.replace_existing = Boolean(exportCfg.replace_existing);
  exportCfg.source = exportCfg.source && typeof exportCfg.source === "object" ? exportCfg.source : {};
  exportCfg.fields = exportCfg.fields && typeof exportCfg.fields === "object" ? exportCfg.fields : {};
  exportCfg.fixed_values = exportCfg.fixed_values && typeof exportCfg.fixed_values === "object" ? exportCfg.fixed_values : {};
  exportCfg.fixed_values.shift_text =
    exportCfg.fixed_values.shift_text && typeof exportCfg.fixed_values.shift_text === "object"
      ? exportCfg.fixed_values.shift_text
      : {};

  exportCfg.source.app_token = String(exportCfg.source.app_token || "").trim();
  exportCfg.source.table_id = String(exportCfg.source.table_id || "").trim();
  exportCfg.source.page_size = Number.parseInt(exportCfg.source.page_size ?? 500, 10);
  exportCfg.source.max_records = Number.parseInt(exportCfg.source.max_records ?? 5000, 10);
  exportCfg.source.delete_batch_size = Number.parseInt(exportCfg.source.delete_batch_size ?? 200, 10);

  exportCfg.fields.type = String(exportCfg.fields.type || "").trim();
  exportCfg.fields.building = String(exportCfg.fields.building || "").trim();
  exportCfg.fields.date = String(exportCfg.fields.date || "").trim();
  exportCfg.fields.shift = String(exportCfg.fields.shift || "").trim();
  exportCfg.fields.attachment = String(exportCfg.fields.attachment || "").trim();

  exportCfg.fixed_values.type = String(exportCfg.fixed_values.type || "").trim();
  exportCfg.fixed_values.shift_text.day = String(exportCfg.fixed_values.shift_text.day || "").trim();
  exportCfg.fixed_values.shift_text.night = String(exportCfg.fixed_values.shift_text.night || "").trim();

  if (!exportCfg.enabled) return { ok: true };
  if (!exportCfg.source.app_token) return { ok: false, error: "源数据附件上报多维 app_token 不能为空" };
  if (!exportCfg.source.table_id) return { ok: false, error: "源数据附件上报多维 table_id 不能为空" };
  if (!Number.isInteger(exportCfg.source.page_size) || exportCfg.source.page_size <= 0) {
    return { ok: false, error: "源数据附件上报 page_size 必须大于0" };
  }
  if (!Number.isInteger(exportCfg.source.max_records) || exportCfg.source.max_records <= 0) {
    return { ok: false, error: "源数据附件上报 max_records 必须大于0" };
  }
  if (!Number.isInteger(exportCfg.source.delete_batch_size) || exportCfg.source.delete_batch_size <= 0) {
    return { ok: false, error: "源数据附件上报 delete_batch_size 必须大于0" };
  }
  if (
    !exportCfg.fields.type ||
    !exportCfg.fields.building ||
    !exportCfg.fields.date ||
    !exportCfg.fields.shift ||
    !exportCfg.fields.attachment
  ) {
    return { ok: false, error: "源数据附件上报字段映射（类型/楼栋/日期/班次/附件）不能为空" };
  }
  if (!exportCfg.fixed_values.type) return { ok: false, error: "源数据附件上报固定类型文案不能为空" };
  if (!exportCfg.fixed_values.shift_text.day || !exportCfg.fixed_values.shift_text.night) {
    return { ok: false, error: "源数据附件上报班次文案（白班/夜班）不能为空" };
  }
  return { ok: true };
}

function validateAndNormalizeHandoverCloudSheetSync(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.cloud_sheet_sync = payload.handover_log.cloud_sheet_sync || {};
  const syncCfg = payload.handover_log.cloud_sheet_sync;
  syncCfg.enabled = Boolean(syncCfg.enabled);
  syncCfg.root_wiki_url = String(syncCfg.root_wiki_url || "").trim();
  syncCfg.template_node_token = String(syncCfg.template_node_token || "").trim();
  syncCfg.spreadsheet_name_pattern = String(syncCfg.spreadsheet_name_pattern || "").trim();
  syncCfg.source_sheet_name = String(syncCfg.source_sheet_name || "").trim();
  syncCfg.sync_mode =
    String(syncCfg.sync_mode || "overwrite_named_sheet").trim().toLowerCase() || "overwrite_named_sheet";
  if (syncCfg.sync_mode === "rebuild_sheet") {
    syncCfg.sync_mode = "overwrite_named_sheet";
  }
  syncCfg.sheet_names =
    syncCfg.sheet_names && typeof syncCfg.sheet_names === "object" && !Array.isArray(syncCfg.sheet_names)
      ? syncCfg.sheet_names
      : {};
  syncCfg.copy =
    syncCfg.copy && typeof syncCfg.copy === "object" && !Array.isArray(syncCfg.copy)
      ? syncCfg.copy
      : {};
  syncCfg.request =
    syncCfg.request && typeof syncCfg.request === "object" && !Array.isArray(syncCfg.request)
      ? syncCfg.request
      : {};

  for (const building of ["A楼", "B楼", "C楼", "D楼", "E楼"]) {
    syncCfg.sheet_names[building] = String(syncCfg.sheet_names[building] || "").trim();
  }
  for (const key of ["values", "formulas", "styles", "merges", "row_heights", "column_widths"]) {
    syncCfg.copy[key] = Boolean(syncCfg.copy[key]);
  }
  syncCfg.request.timeout_sec = Number.parseInt(syncCfg.request.timeout_sec ?? 20, 10);
  syncCfg.request.max_retries = Number.parseInt(syncCfg.request.max_retries ?? 3, 10);
  syncCfg.request.retry_backoff_sec = Number.parseFloat(syncCfg.request.retry_backoff_sec ?? 2);

  if (!syncCfg.root_wiki_url) {
    return { ok: false, error: "交接班云文档同步根 wiki URL 不能为空" };
  }
  try {
    const parsed = new URL(syncCfg.root_wiki_url);
    if (!/^https?:$/i.test(parsed.protocol) || !String(parsed.pathname || "").includes("/wiki/")) {
      return { ok: false, error: "交接班云文档同步根 wiki URL 格式错误" };
    }
  } catch (_err) {
    return { ok: false, error: "交接班云文档同步根 wiki URL 格式错误" };
  }
  if (!syncCfg.template_node_token) {
    return { ok: false, error: "交接班云文档同步模板 node token 不能为空" };
  }
  if (!syncCfg.spreadsheet_name_pattern) {
    return { ok: false, error: "交接班云文档同步云表名称模板不能为空" };
  }
  if (!syncCfg.source_sheet_name) {
    return { ok: false, error: "交接班云文档同步源 Sheet 名不能为空" };
  }
  if (syncCfg.sync_mode !== "overwrite_named_sheet") {
    return { ok: false, error: "交接班云文档同步模式仅支持 overwrite_named_sheet" };
  }
  for (const building of ["A楼", "B楼", "C楼", "D楼", "E楼"]) {
    if (!syncCfg.sheet_names[building]) {
      return { ok: false, error: `交接班云文档同步 ${building} Sheet 名不能为空` };
    }
  }
  if (!Number.isInteger(syncCfg.request.timeout_sec) || syncCfg.request.timeout_sec <= 0) {
    return { ok: false, error: "交接班云文档同步超时时间必须大于0" };
  }
  if (!Number.isInteger(syncCfg.request.max_retries) || syncCfg.request.max_retries < 0) {
    return { ok: false, error: "交接班云文档同步重试次数必须大于等于0" };
  }
  if (!Number.isFinite(syncCfg.request.retry_backoff_sec) || syncCfg.request.retry_backoff_sec < 0) {
    return { ok: false, error: "交接班云文档同步重试退避秒数必须大于等于0" };
  }
  return { ok: true };
}

function validateAndNormalizeHandoverReviewUi(payload) {
  payload.handover_log = payload.handover_log || {};
  payload.handover_log.review_ui = payload.handover_log.review_ui || {};
  const reviewUi = payload.handover_log.review_ui;
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
  reviewUi.review_link_recipients_by_building =
    reviewUi.review_link_recipients_by_building &&
    typeof reviewUi.review_link_recipients_by_building === "object" &&
    !Array.isArray(reviewUi.review_link_recipients_by_building)
      ? reviewUi.review_link_recipients_by_building
      : {};
  reviewUi.public_base_url = String(reviewUi.public_base_url || "").trim();
  if (reviewUi.public_base_url && !/^https?:\/\//i.test(reviewUi.public_base_url)) {
    reviewUi.public_base_url = `http://${reviewUi.public_base_url}`;
  }

  reviewUi.section_hidden_columns = Array.isArray(reviewUi.section_hidden_columns)
    ? reviewUi.section_hidden_columns
        .map((value) => String(value || "").trim().toUpperCase())
        .filter((value, index, array) => value && array.indexOf(value) === index)
    : ["I"];

  for (const column of reviewUi.section_hidden_columns) {
    if (!/^[B-I]$/.test(column)) {
      return { ok: false, error: "审核页隐藏列仅支持 B-I 单列字母" };
    }
  }
  if (reviewUi.public_base_url) {
    try {
      const parsed = new URL(reviewUi.public_base_url);
      if (!/^https?:$/i.test(parsed.protocol) || !String(parsed.host || "").trim()) {
        return { ok: false, error: "审核页外部访问基地址必须是合法的 http/https 地址" };
      }
      reviewUi.public_base_url = `${parsed.protocol}//${parsed.host}`;
    } catch (_err) {
      return { ok: false, error: "审核页外部访问基地址格式错误，应类似 http://192.168.220.160:18765" };
    }
  }

  const allowedFooterColumns = ["B", "C", "E", "F", "G", "H"];
  const blankFooterCells = () =>
    Object.fromEntries(allowedFooterColumns.map((column) => [column, ""]));
  const normalizedFooterDefaults = {};
  for (const [rawBuilding, rawPayload] of Object.entries(reviewUi.footer_inventory_defaults_by_building)) {
    const building = String(rawBuilding || "").trim();
    if (!building) continue;
    const payloadRows =
      rawPayload && typeof rawPayload === "object" && Array.isArray(rawPayload.rows) ? rawPayload.rows : [];
    const rows = payloadRows.map((rawRow) => {
      const rawCells = rawRow && typeof rawRow === "object" && rawRow.cells && typeof rawRow.cells === "object"
        ? rawRow.cells
        : {};
      const cells = blankFooterCells();
      for (const column of allowedFooterColumns) {
        cells[column] = String(rawCells[column] ?? "").trim();
      }
      return { cells };
    });
    normalizedFooterDefaults[building] = {
      rows: rows.length ? rows : [{ cells: blankFooterCells() }],
    };
  }
  reviewUi.footer_inventory_defaults_by_building = normalizedFooterDefaults;

  const normalizedReviewRecipients = {};
  for (const [rawBuilding, rawItems] of Object.entries(reviewUi.review_link_recipients_by_building)) {
    const building = String(rawBuilding || "").trim();
    if (!building) continue;
    const rows = [];
    const seenOpenIds = new Set();
    for (const rawItem of Array.isArray(rawItems) ? rawItems : []) {
      if (!rawItem || typeof rawItem !== "object") continue;
      const note = String(rawItem.note || "").trim();
      const openId = String(rawItem.open_id || "").trim();
      if (!openId || seenOpenIds.has(openId)) continue;
      seenOpenIds.add(openId);
      rows.push({
        note,
        open_id: openId,
        enabled: rawItem.enabled === false ? false : true,
      });
    }
    normalizedReviewRecipients[building] = rows;
  }
  reviewUi.review_link_recipients_by_building = normalizedReviewRecipients;

  const allowedCabinetCells = ["B13", "D13", "F13", "H13"];
  const normalizedCabinetDefaults = {};
  for (const [rawBuilding, rawPayload] of Object.entries(reviewUi.cabinet_power_defaults_by_building)) {
    const building = String(rawBuilding || "").trim();
    if (!building) continue;
    const sourceCells =
      rawPayload && typeof rawPayload === "object" && rawPayload.cells && typeof rawPayload.cells === "object"
        ? rawPayload.cells
        : {};
    const cells = {};
    for (const cell of allowedCabinetCells) {
      cells[cell] = String(sourceCells[cell] ?? "").trim();
    }
    normalizedCabinetDefaults[building] = { cells };
  }
  reviewUi.cabinet_power_defaults_by_building = normalizedCabinetDefaults;

  return { ok: true };
}

function validateAndNormalizeAlarmExport(payload) {
  payload.alarm_export = payload.alarm_export || {};
  const alarmExport = payload.alarm_export;
  alarmExport.scheduler = alarmExport.scheduler && typeof alarmExport.scheduler === "object" ? alarmExport.scheduler : {};
  alarmExport.feishu = alarmExport.feishu && typeof alarmExport.feishu === "object" ? alarmExport.feishu : {};
  alarmExport.shared_source_upload =
    alarmExport.shared_source_upload && typeof alarmExport.shared_source_upload === "object"
      ? alarmExport.shared_source_upload
      : {};
  const legacyTarget =
    alarmExport.shared_source_upload.target && typeof alarmExport.shared_source_upload.target === "object"
      ? alarmExport.shared_source_upload.target
      : {};

  if (!String(alarmExport.feishu.app_token || "").trim() && String(legacyTarget.app_token || "").trim()) {
    alarmExport.feishu.app_token = String(legacyTarget.app_token || "").trim();
  }
  if (!String(alarmExport.feishu.table_id || "").trim() && String(legacyTarget.table_id || "").trim()) {
    alarmExport.feishu.table_id = String(legacyTarget.table_id || "").trim();
  }

  cleanupAlarmExportCompat(alarmExport);
  alarmExport.feishu.app_token = String(alarmExport.feishu.app_token || "").trim();
  alarmExport.feishu.table_id = String(alarmExport.feishu.table_id || "").trim();
  alarmExport.scheduler.enabled = true;
  alarmExport.scheduler.auto_start_in_gui = Boolean(alarmExport.scheduler.auto_start_in_gui);
  alarmExport.scheduler.run_time = normalizeRunTimeText(alarmExport.scheduler.run_time) || "08:10:00";
  alarmExport.scheduler.check_interval_sec = Number.parseInt(alarmExport.scheduler.check_interval_sec ?? 30, 10);
  alarmExport.scheduler.catch_up_if_missed = Boolean(alarmExport.scheduler.catch_up_if_missed);
  alarmExport.scheduler.retry_failed_in_same_period =
    alarmExport.scheduler.retry_failed_in_same_period !== false;
  alarmExport.scheduler.state_file = String(
    alarmExport.scheduler.state_file || "alarm_event_upload_scheduler_state.json",
  ).trim();
  alarmExport.feishu.page_size = Number.parseInt(alarmExport.feishu.page_size ?? legacyTarget.page_size ?? 500, 10);
  alarmExport.feishu.delete_batch_size = Number.parseInt(
    alarmExport.feishu.delete_batch_size ?? legacyTarget.delete_batch_size ?? 500,
    10,
  );
  alarmExport.feishu.create_batch_size = Number.parseInt(
    alarmExport.feishu.create_batch_size ?? legacyTarget.create_batch_size ?? 200,
    10,
  );
  alarmExport.shared_source_upload.replace_existing_on_full =
    alarmExport.shared_source_upload.replace_existing_on_full !== false;

  if (Boolean(alarmExport.feishu.app_token) !== Boolean(alarmExport.feishu.table_id)) {
    return { ok: false, error: "告警信息上传配置错误：App Token 与 Table ID 必须同时填写或同时留空" };
  }
  if (!alarmExport.scheduler.run_time) {
    return { ok: false, error: "告警信息上传调度时间格式错误，必须是 HH:MM 或 HH:MM:SS" };
  }
  if (!Number.isInteger(alarmExport.scheduler.check_interval_sec) || alarmExport.scheduler.check_interval_sec <= 0) {
    return { ok: false, error: "告警信息上传调度检查间隔必须大于0" };
  }
  if (!alarmExport.scheduler.state_file) {
    return { ok: false, error: "告警信息上传调度状态文件不能为空" };
  }
  if (!Number.isInteger(alarmExport.feishu.page_size) || alarmExport.feishu.page_size <= 0) {
    return { ok: false, error: "告警信息上传配置错误：清表分页大小必须大于0" };
  }
  if (!Number.isInteger(alarmExport.feishu.delete_batch_size) || alarmExport.feishu.delete_batch_size <= 0) {
    return { ok: false, error: "告警信息上传配置错误：清表删除批次必须大于0" };
  }
  if (!Number.isInteger(alarmExport.feishu.create_batch_size) || alarmExport.feishu.create_batch_size <= 0) {
    return { ok: false, error: "告警信息上传配置错误：写入批次大小必须大于0" };
  }
  return { ok: true };
}

function validateAndNormalizeMonthlyEventReport(payload) {
  payload.handover_log = payload.handover_log || {};
  const handover = payload.handover_log;
  handover.monthly_event_report =
    handover.monthly_event_report && typeof handover.monthly_event_report === "object"
      ? handover.monthly_event_report
      : {};
  const monthly = handover.monthly_event_report;
  monthly.template = monthly.template && typeof monthly.template === "object" ? monthly.template : {};
  monthly.scheduler = monthly.scheduler && typeof monthly.scheduler === "object" ? monthly.scheduler : {};
  monthly.test_delivery = monthly.test_delivery && typeof monthly.test_delivery === "object" ? monthly.test_delivery : {};

  monthly.enabled = monthly.enabled !== false;
  monthly.template.source_path = String(monthly.template.source_path || "").trim();
  monthly.template.change_source_path = String(monthly.template.change_source_path || "").trim();
  monthly.template.output_dir = String(monthly.template.output_dir || "").trim();
  monthly.template.file_name_pattern = String(monthly.template.file_name_pattern || "").trim();
  monthly.scheduler.enabled = true;
  monthly.scheduler.auto_start_in_gui = Boolean(monthly.scheduler.auto_start_in_gui);
  monthly.scheduler.day_of_month = Number.parseInt(monthly.scheduler.day_of_month ?? 1, 10);
  monthly.scheduler.run_time = normalizeRunTimeText(monthly.scheduler.run_time);
  monthly.scheduler.check_interval_sec = Number.parseInt(monthly.scheduler.check_interval_sec ?? 30, 10);
  monthly.scheduler.state_file = String(monthly.scheduler.state_file || "").trim();
  monthly.test_delivery.receive_id_type = String(monthly.test_delivery.receive_id_type || "open_id").trim() || "open_id";
  monthly.test_delivery.receive_ids = Array.isArray(monthly.test_delivery.receive_ids)
    ? monthly.test_delivery.receive_ids
        .map((item) => String(item || "").trim())
        .filter(Boolean)
        .filter((item, index, list) => list.indexOf(item) === index)
    : [];

  if (!monthly.enabled) return { ok: true };
  if (!monthly.template.source_path) {
    return { ok: false, error: "月度事件统计表模板路径不能为空" };
  }
  if (!monthly.template.output_dir) {
    return { ok: false, error: "月度事件统计表输出目录不能为空" };
  }
  if (!monthly.template.file_name_pattern) {
    return { ok: false, error: "月度事件统计表文件命名规则不能为空" };
  }
  if (!Number.isInteger(monthly.scheduler.day_of_month) || monthly.scheduler.day_of_month < 1 || monthly.scheduler.day_of_month > 31) {
    return { ok: false, error: "月度事件统计表调度日期必须在 1 到 31 之间" };
  }
  if (!monthly.scheduler.run_time) {
    return { ok: false, error: "月度事件统计表调度时间必须是 HH:MM 或 HH:MM:SS" };
  }
  if (!Number.isInteger(monthly.scheduler.check_interval_sec) || monthly.scheduler.check_interval_sec <= 0) {
    return { ok: false, error: "月度事件统计表调度检查间隔必须大于 0 秒" };
  }
  if (!monthly.scheduler.state_file) {
    return { ok: false, error: "月度事件统计表调度状态文件名不能为空" };
  }
  if (!monthly.test_delivery.receive_id_type) {
    return { ok: false, error: "月度事件统计表测试发送 receive_id_type 不能为空" };
  }
  return { ok: true };
}

function validateAndNormalizeMonthlyChangeReport(payload) {
  payload.handover_log = payload.handover_log || {};
  const handover = payload.handover_log;
  handover.monthly_change_report =
    handover.monthly_change_report && typeof handover.monthly_change_report === "object"
      ? handover.monthly_change_report
      : {};
  const monthly = handover.monthly_change_report;
  monthly.template = monthly.template && typeof monthly.template === "object" ? monthly.template : {};
  monthly.scheduler = monthly.scheduler && typeof monthly.scheduler === "object" ? monthly.scheduler : {};

  monthly.enabled = monthly.enabled !== false;
  monthly.template.source_path = String(monthly.template.source_path || "").trim();
  monthly.template.output_dir = String(monthly.template.output_dir || "").trim();
  monthly.template.file_name_pattern = String(monthly.template.file_name_pattern || "").trim();
  monthly.scheduler.enabled = true;
  monthly.scheduler.auto_start_in_gui = Boolean(monthly.scheduler.auto_start_in_gui);
  monthly.scheduler.day_of_month = Number.parseInt(monthly.scheduler.day_of_month ?? 1, 10);
  monthly.scheduler.run_time = normalizeRunTimeText(monthly.scheduler.run_time);
  monthly.scheduler.check_interval_sec = Number.parseInt(monthly.scheduler.check_interval_sec ?? 30, 10);
  monthly.scheduler.state_file = String(monthly.scheduler.state_file || "").trim();

  if (!monthly.enabled) return { ok: true };
  if (!monthly.template.source_path) {
    return { ok: false, error: "月度变更统计表模板路径不能为空" };
  }
  if (!monthly.template.output_dir) {
    return { ok: false, error: "月度变更统计表输出目录不能为空" };
  }
  if (!monthly.template.file_name_pattern) {
    return { ok: false, error: "月度变更统计表文件命名规则不能为空" };
  }
  if (!Number.isInteger(monthly.scheduler.day_of_month) || monthly.scheduler.day_of_month < 1 || monthly.scheduler.day_of_month > 31) {
    return { ok: false, error: "月度变更统计表调度日期必须在 1 到 31 之间" };
  }
  if (!monthly.scheduler.run_time) {
    return { ok: false, error: "月度变更统计表调度时间必须是 HH:MM 或 HH:MM:SS" };
  }
  if (!Number.isInteger(monthly.scheduler.check_interval_sec) || monthly.scheduler.check_interval_sec <= 0) {
    return { ok: false, error: "月度变更统计表调度检查间隔必须大于 0 秒" };
  }
  if (!monthly.scheduler.state_file) {
    return { ok: false, error: "月度变更统计表调度状态文件名不能为空" };
  }
  return { ok: true };
}

function validateAndNormalizeWetBulbCollection(payload) {
  payload.wet_bulb_collection = payload.wet_bulb_collection || {};
  const wet = cleanupWetBulbCollectionCompat(payload.wet_bulb_collection);
  wet.scheduler = wet.scheduler && typeof wet.scheduler === "object" ? wet.scheduler : {};
  wet.source = wet.source && typeof wet.source === "object" ? wet.source : {};
  wet.target = wet.target && typeof wet.target === "object" ? wet.target : {};
  wet.fields = wet.fields && typeof wet.fields === "object" ? wet.fields : {};
  wet.cooling_mode = wet.cooling_mode && typeof wet.cooling_mode === "object" ? wet.cooling_mode : {};

  wet.enabled = Boolean(wet.enabled);

  wet.scheduler.enabled = true;
  wet.scheduler.auto_start_in_gui = Boolean(wet.scheduler.auto_start_in_gui);
  wet.scheduler.interval_minutes = Number.parseInt(wet.scheduler.interval_minutes ?? 60, 10);
  wet.scheduler.check_interval_sec = Number.parseInt(wet.scheduler.check_interval_sec ?? 30, 10);
  wet.scheduler.retry_failed_on_next_tick = Boolean(wet.scheduler.retry_failed_on_next_tick);
  wet.scheduler.state_file = String(wet.scheduler.state_file || "").trim();

  wet.source.reuse_handover_download = Boolean(wet.source.reuse_handover_download ?? true);
  wet.source.reuse_handover_rule_engine = Boolean(wet.source.reuse_handover_rule_engine ?? true);

  wet.target.app_token = String(wet.target.app_token || "").trim();
  wet.target.table_id = String(wet.target.table_id || "").trim();
  wet.target.page_size = Number.parseInt(wet.target.page_size ?? 500, 10);
  wet.target.max_records = Number.parseInt(wet.target.max_records ?? 5000, 10);
  wet.target.delete_batch_size = Number.parseInt(wet.target.delete_batch_size ?? 200, 10);
  wet.target.create_batch_size = Number.parseInt(wet.target.create_batch_size ?? 200, 10);
  wet.target.replace_existing = Boolean(wet.target.replace_existing ?? true);

  wet.fields.date = String(wet.fields.date || "").trim();
  wet.fields.building = String(wet.fields.building || "").trim();
  wet.fields.wet_bulb_temp = String(wet.fields.wet_bulb_temp || "").trim();
  wet.fields.cooling_mode = String(wet.fields.cooling_mode || "").trim();
  wet.fields.sequence = String(wet.fields.sequence || "").trim();

  if (!Array.isArray(wet.cooling_mode.priority_order)) {
    wet.cooling_mode.priority_order = ["1", "2", "3", "4"];
  }
  wet.cooling_mode.priority_order = wet.cooling_mode.priority_order
    .map((item) => String(item || "").trim())
    .filter(Boolean);
  wet.cooling_mode.source_value_map =
    wet.cooling_mode.source_value_map && typeof wet.cooling_mode.source_value_map === "object"
      ? wet.cooling_mode.source_value_map
      : {};
  wet.cooling_mode.upload_value_map =
    wet.cooling_mode.upload_value_map && typeof wet.cooling_mode.upload_value_map === "object"
      ? wet.cooling_mode.upload_value_map
      : {};
  wet.cooling_mode.skip_modes = Array.isArray(wet.cooling_mode.skip_modes)
    ? wet.cooling_mode.skip_modes.map((item) => String(item || "").trim()).filter(Boolean)
    : ["停机"];

  if (!Number.isInteger(wet.scheduler.interval_minutes) || wet.scheduler.interval_minutes < 1) {
    return { ok: false, error: "湿球温度定时采集执行间隔必须大于等于1分钟" };
  }
  if (!Number.isInteger(wet.scheduler.check_interval_sec) || wet.scheduler.check_interval_sec <= 0) {
    return { ok: false, error: "湿球温度定时采集检查间隔必须大于0秒" };
  }
  if (!wet.scheduler.state_file) {
    return { ok: false, error: "湿球温度定时采集状态文件名不能为空" };
  }
  if (!wet.target.app_token || !wet.target.table_id) {
    return { ok: false, error: "湿球温度定时采集目标配置不能为空，请填写 app_token 和 table_id" };
  }
  for (const key of ["page_size", "max_records", "delete_batch_size", "create_batch_size"]) {
    if (!Number.isInteger(wet.target[key]) || wet.target[key] <= 0) {
      return { ok: false, error: `湿球温度定时采集 ${key} 必须大于0` };
    }
  }
  if (!wet.fields.date || !wet.fields.building || !wet.fields.wet_bulb_temp || !wet.fields.cooling_mode || !wet.fields.sequence) {
    return { ok: false, error: "湿球温度定时采集字段映射（日期/楼栋/天气湿球温度/冷源运行模式/序号）不能为空" };
  }
  if (!wet.cooling_mode.priority_order.length) {
    return { ok: false, error: "湿球温度定时采集冷源模式优先级不能为空" };
  }
  for (const key of ["制冷", "预冷", "板换"]) {
    if (!String(wet.cooling_mode.upload_value_map[key] || "").trim()) {
      return { ok: false, error: `湿球温度定时采集模式映射缺少 ${key}` };
    }
  }
  return { ok: true };
}

export function prepareConfigPayloadForSave({
  config,
  buildingsText,
  customAbsoluteStartLocal,
  customAbsoluteEndLocal,
  sheetRuleRows,
}) {
  if (!config) {
    return { ok: false, error: "配置未加载，无法保存" };
  }

  const payload = clone(config);
  payload.input = payload.input || {};
  payload.output = payload.output || {};
  payload.download = payload.download || {};
  payload.download.multi_date = payload.download.multi_date || {};
  payload.download.resume = payload.download.resume || {};
  payload.download.performance = payload.download.performance || {};
  payload.feishu_sheet_import = payload.feishu_sheet_import || {};
  payload.paths = payload.paths || {};

  payload.input.buildings = String(buildingsText || "")
    .split(/[，,\s\r\n\t]+/)
    .map((x) => x.trim())
    .filter((x) => x);
  payload.input.file_glob_template = "{building}_*.xlsx";
  payload.output.save_json = false;
  const deploymentRoleMode = String(payload.deployment?.role_mode || "").trim().toLowerCase();
  payload.shared_bridge = payload.shared_bridge || {};
  payload.shared_bridge.internal_root_dir = String(
    payload.shared_bridge.internal_root_dir || payload.shared_bridge.root_dir || "",
  ).trim();
  payload.shared_bridge.external_root_dir = String(
    payload.shared_bridge.external_root_dir || payload.shared_bridge.root_dir || "",
  ).trim();
  payload.shared_bridge.root_dir = deploymentRoleMode === "internal"
    ? payload.shared_bridge.internal_root_dir
    : deploymentRoleMode === "external"
      ? payload.shared_bridge.external_root_dir
      : "";
  const fallbackBusinessRoot = deploymentRoleMode === "internal"
    ? String(payload.shared_bridge?.internal_root_dir || "").trim()
    : "";
  const businessRoot = String(payload.download.save_dir || payload.input.excel_dir || fallbackBusinessRoot).trim();
  if (!businessRoot) {
    return { ok: false, error: "业务根目录不能为空" };
  }
  payload.input.excel_dir = businessRoot;
  payload.download.save_dir = businessRoot;

  payload.scheduler = payload.scheduler || {};
  payload.scheduler.run_time = normalizeRunTimeText(payload.scheduler.run_time) || "00:10:00";
  payload.scheduler.enabled = true;
  payload.scheduler.auto_start_in_gui = Boolean(payload.scheduler.auto_start_in_gui);
  payload.scheduler.check_interval_sec = Number.parseInt(payload.scheduler.check_interval_sec ?? 30, 10);
  payload.scheduler.catch_up_if_missed = Boolean(payload.scheduler.catch_up_if_missed);
  payload.scheduler.retry_failed_in_same_period = payload.scheduler.retry_failed_in_same_period !== false;

  // 路径收敛：调度状态/续传/告警恢复统一使用内部固定 .runtime 子路径
  payload.scheduler.state_file = "daily_scheduler_state.json";
  payload.download.resume.root_dir = "pipeline_resume";
  payload.download.resume.index_file = "index.json";

  payload.day_metric_upload = payload.day_metric_upload || {};
  payload.day_metric_upload.scheduler =
    payload.day_metric_upload.scheduler && typeof payload.day_metric_upload.scheduler === "object"
      ? payload.day_metric_upload.scheduler
      : {};
  payload.day_metric_upload.scheduler.enabled = true;
  payload.day_metric_upload.scheduler.auto_start_in_gui = Boolean(payload.day_metric_upload.scheduler.auto_start_in_gui);
  payload.day_metric_upload.scheduler.interval_minutes = Number.parseInt(
    payload.day_metric_upload.scheduler.interval_minutes ?? 60,
    10,
  );
  payload.day_metric_upload.scheduler.check_interval_sec = Number.parseInt(
    payload.day_metric_upload.scheduler.check_interval_sec ?? 30,
    10,
  );
  payload.day_metric_upload.scheduler.retry_failed_on_next_tick =
    payload.day_metric_upload.scheduler.retry_failed_on_next_tick !== false;
  payload.day_metric_upload.scheduler.state_file = String(
    payload.day_metric_upload.scheduler.state_file || "day_metric_upload_scheduler_state.json",
  ).trim();
  if (!payload.scheduler.run_time) {
    return { ok: false, error: "自动流程每日执行时间必须是 HH:MM 或 HH:MM:SS" };
  }
  if (!Number.isInteger(payload.scheduler.check_interval_sec) || payload.scheduler.check_interval_sec <= 0) {
    return { ok: false, error: "自动流程调度检查间隔必须大于0秒" };
  }
  if (!payload.scheduler.state_file) {
    return { ok: false, error: "自动流程调度状态文件不能为空" };
  }
  if (!Number.isInteger(payload.day_metric_upload.scheduler.interval_minutes) || payload.day_metric_upload.scheduler.interval_minutes < 1) {
    return { ok: false, error: "12项独立上传调度间隔必须大于0分钟" };
  }
  if (
    !Number.isInteger(payload.day_metric_upload.scheduler.check_interval_sec) ||
    payload.day_metric_upload.scheduler.check_interval_sec <= 0
  ) {
    return { ok: false, error: "12项独立上传调度检查间隔必须大于0" };
  }
  if (!payload.day_metric_upload.scheduler.state_file) {
    return { ok: false, error: "12项独立上传调度状态文件不能为空" };
  }

  payload.internal_source_sites = [];
  if (Array.isArray(payload.download?.sites)) payload.download.sites = [];
  if (Array.isArray(payload.handover_log?.sites)) payload.handover_log.sites = [];
  if (Array.isArray(payload.handover_log?.download?.sites)) payload.handover_log.download.sites = [];

  payload.network = payload.network || {};
  delete payload.network.enable_auto_switch_wifi;
  payload.network.internal_profile_name = String(payload.network.internal_profile_name || "").trim();
  payload.network.external_profile_name = String(payload.network.external_profile_name || "").trim();
  payload.network.preferred_interface = String(payload.network.preferred_interface || "").trim();
  payload.network.scan_attempts = Number.parseInt(payload.network.scan_attempts ?? 3, 10);
  payload.network.scan_wait_sec = Number.parseInt(payload.network.scan_wait_sec ?? 2, 10);
  payload.network.hard_recovery_after_scan_failures = Number.parseInt(
    payload.network.hard_recovery_after_scan_failures ?? 2,
    10,
  );
  payload.network.hard_recovery_cooldown_sec = Number.parseInt(
    payload.network.hard_recovery_cooldown_sec ?? 20,
    10,
  );
  payload.network.post_switch_stabilize_sec = Number.parseFloat(payload.network.post_switch_stabilize_sec ?? 3);
  payload.network.post_switch_probe_enabled = Boolean(payload.network.post_switch_probe_enabled);
  payload.network.post_switch_probe_internal_host = String(payload.network.post_switch_probe_internal_host || "").trim();
  payload.network.post_switch_probe_internal_port = Number.parseInt(payload.network.post_switch_probe_internal_port ?? 80, 10);
  payload.network.post_switch_probe_external_host = String(payload.network.post_switch_probe_external_host || "").trim();
  payload.network.post_switch_probe_external_port = Number.parseInt(payload.network.post_switch_probe_external_port ?? 443, 10);
  payload.network.post_switch_probe_timeout_sec = Number.parseFloat(payload.network.post_switch_probe_timeout_sec ?? 2);
  payload.network.post_switch_probe_retries = Number.parseInt(payload.network.post_switch_probe_retries ?? 3, 10);
  payload.network.post_switch_probe_interval_sec = Number.parseFloat(payload.network.post_switch_probe_interval_sec ?? 1);
  payload.network.hard_recovery_steps = Array.isArray(payload.network.hard_recovery_steps)
    ? payload.network.hard_recovery_steps.filter((x) => ["toggle_adapter", "restart_wlansvc"].includes(String(x)))
    : [];
  if (!payload.network.hard_recovery_steps.length) {
    payload.network.hard_recovery_steps = ["toggle_adapter", "restart_wlansvc"];
  }
  if (!Number.isInteger(payload.network.scan_attempts) || payload.network.scan_attempts <= 0) {
    return { ok: false, error: "扫描次数必须大于0" };
  }
  if (!Number.isInteger(payload.network.scan_wait_sec) || payload.network.scan_wait_sec <= 0) {
    return { ok: false, error: "扫描等待秒数必须大于0" };
  }
  if (
    !Number.isInteger(payload.network.hard_recovery_after_scan_failures) ||
    payload.network.hard_recovery_after_scan_failures <= 0
  ) {
    return { ok: false, error: "连续扫描失败触发阈值必须大于0" };
  }
  if (!Number.isInteger(payload.network.hard_recovery_cooldown_sec) || payload.network.hard_recovery_cooldown_sec < 0) {
    return { ok: false, error: "硬恢复冷却时间必须大于等于0" };
  }
  if (!Number.isFinite(payload.network.post_switch_stabilize_sec) || payload.network.post_switch_stabilize_sec < 0) {
    return { ok: false, error: "网络稳定等待秒数必须大于等于0" };
  }
  if (!Number.isFinite(payload.network.post_switch_probe_timeout_sec) || payload.network.post_switch_probe_timeout_sec <= 0) {
    return { ok: false, error: "稳定探测超时时间必须大于0" };
  }
  if (!Number.isInteger(payload.network.post_switch_probe_retries) || payload.network.post_switch_probe_retries <= 0) {
    return { ok: false, error: "稳定探测重试次数必须大于0" };
  }
  if (!Number.isFinite(payload.network.post_switch_probe_interval_sec) || payload.network.post_switch_probe_interval_sec <= 0) {
    return { ok: false, error: "稳定探测重试间隔必须大于0" };
  }
  if (!Number.isInteger(payload.network.post_switch_probe_internal_port) || payload.network.post_switch_probe_internal_port <= 0) {
    return { ok: false, error: "采集端探测端口必须大于0" };
  }
  if (!Number.isInteger(payload.network.post_switch_probe_external_port) || payload.network.post_switch_probe_external_port <= 0) {
    return { ok: false, error: "外网探测端口必须大于0" };
  }

  delete payload.alarm_common_db;

  payload.handover_log = payload.handover_log || {};
  payload.handover_log.template = payload.handover_log.template || {};
  payload.handover_log.scheduler = payload.handover_log.scheduler || {};
  payload.handover_log.capacity_report = payload.handover_log.capacity_report || {};
  payload.handover_log.capacity_report.weather = payload.handover_log.capacity_report.weather || {};
  payload.handover_log.template.source_path = String(payload.handover_log.template.source_path || "").trim();
  if (!payload.handover_log.template.source_path) {
    return { ok: false, error: "交接班模板文件不能为空" };
  }
  payload.handover_log.scheduler.enabled = true;
  payload.handover_log.scheduler.auto_start_in_gui = Boolean(payload.handover_log.scheduler.auto_start_in_gui);
  payload.handover_log.scheduler.catch_up_if_missed = Boolean(payload.handover_log.scheduler.catch_up_if_missed);
  payload.handover_log.scheduler.retry_failed_in_same_period = Boolean(
    payload.handover_log.scheduler.retry_failed_in_same_period,
  );
  payload.handover_log.scheduler.morning_time = normalizeRunTimeText(payload.handover_log.scheduler.morning_time);
  payload.handover_log.scheduler.afternoon_time = normalizeRunTimeText(payload.handover_log.scheduler.afternoon_time);
  payload.handover_log.scheduler.check_interval_sec = Number.parseInt(
    payload.handover_log.scheduler.check_interval_sec ?? 30,
    10,
  );
  payload.handover_log.scheduler.morning_state_file = String(
    payload.handover_log.scheduler.morning_state_file || "",
  ).trim();
  payload.handover_log.scheduler.afternoon_state_file = String(
    payload.handover_log.scheduler.afternoon_state_file || "",
  ).trim();
  if (!payload.handover_log.scheduler.morning_time) {
    return { ok: false, error: "交接班上午调度时间格式错误，必须是 HH:MM 或 HH:MM:SS" };
  }
  if (!payload.handover_log.scheduler.afternoon_time) {
    return { ok: false, error: "交接班下午调度时间格式错误，必须是 HH:MM 或 HH:MM:SS" };
  }
  if (
    !Number.isInteger(payload.handover_log.scheduler.check_interval_sec) ||
    payload.handover_log.scheduler.check_interval_sec <= 0
  ) {
    return { ok: false, error: "交接班调度检查间隔必须大于0" };
  }
  if (!payload.handover_log.scheduler.morning_state_file) {
    return { ok: false, error: "交接班上午状态文件不能为空" };
  }
  if (!payload.handover_log.scheduler.afternoon_state_file) {
    return { ok: false, error: "交接班下午状态文件不能为空" };
  }
  payload.handover_log.capacity_report.weather.provider = String(
    payload.handover_log.capacity_report.weather.provider || "seniverse",
  ).trim() || "seniverse";
  payload.handover_log.capacity_report.weather.location = String(
    payload.handover_log.capacity_report.weather.location || "崇川区",
  ).trim() || "崇川区";
  payload.handover_log.capacity_report.weather.fallback_locations = Array.isArray(
    payload.handover_log.capacity_report.weather.fallback_locations,
  )
    ? payload.handover_log.capacity_report.weather.fallback_locations
      .map((item) => String(item || "").trim())
      .filter(Boolean)
    : String(payload.handover_log.capacity_report.weather.fallback_locations || "")
      .split(/[，,]/)
      .map((item) => String(item || "").trim())
      .filter(Boolean);
  if (!payload.handover_log.capacity_report.weather.fallback_locations.length) {
    payload.handover_log.capacity_report.weather.fallback_locations = ["南通"];
  }
  payload.handover_log.capacity_report.weather.language = String(
    payload.handover_log.capacity_report.weather.language || "zh-Hans",
  ).trim() || "zh-Hans";
  payload.handover_log.capacity_report.weather.unit = String(
    payload.handover_log.capacity_report.weather.unit || "c",
  ).trim() || "c";
  payload.handover_log.capacity_report.weather.auth_mode = String(
    payload.handover_log.capacity_report.weather.auth_mode || "signed",
  ).trim() || "signed";
  payload.handover_log.capacity_report.weather.timeout_sec = Number.parseInt(
    payload.handover_log.capacity_report.weather.timeout_sec ?? 8,
    10,
  );
  payload.handover_log.capacity_report.weather.seniverse_public_key = String(
    payload.handover_log.capacity_report.weather.seniverse_public_key || "",
  ).trim();
  payload.handover_log.capacity_report.weather.seniverse_private_key = String(
    payload.handover_log.capacity_report.weather.seniverse_private_key || "",
  ).trim();
  if (
    payload.handover_log.capacity_report.weather.provider === "seniverse"
    && !payload.handover_log.capacity_report.weather.seniverse_public_key
  ) {
    return { ok: false, error: "交接班容量报表天气配置缺少心知公钥" };
  }
  if (
    payload.handover_log.capacity_report.weather.provider === "seniverse"
    && !payload.handover_log.capacity_report.weather.seniverse_private_key
  ) {
    return { ok: false, error: "交接班容量报表天气配置缺少心知私钥" };
  }
  if (
    !Number.isInteger(payload.handover_log.capacity_report.weather.timeout_sec)
    || payload.handover_log.capacity_report.weather.timeout_sec <= 0
  ) {
    return { ok: false, error: "交接班容量报表天气请求超时必须大于0秒" };
  }

  payload.download.custom_window_mode = String(payload.download.custom_window_mode || "absolute").trim();
  if (!["absolute", "daily_relative"].includes(payload.download.custom_window_mode)) {
    return { ok: false, error: "自定义时间模式错误，只能是 absolute 或 daily_relative" };
  }
  payload.download.daily_custom_window = payload.download.daily_custom_window || {};
  payload.download.daily_custom_window.start_time = String(payload.download.daily_custom_window.start_time || "").trim();
  payload.download.daily_custom_window.end_time = String(payload.download.daily_custom_window.end_time || "").trim();
  payload.download.daily_custom_window.cross_day = Boolean(payload.download.daily_custom_window.cross_day);

  if (String(payload.download.time_range_mode || "").trim() === "custom") {
    if (payload.download.custom_window_mode === "absolute") {
      const startText = normalizeDatetimeLocalToApi(customAbsoluteStartLocal);
      const endText = normalizeDatetimeLocalToApi(customAbsoluteEndLocal);
      if (!startText || !endText) {
        return { ok: false, error: "自定义绝对时间段格式错误，请选择开始和结束时间" };
      }
      payload.download.start_time = startText;
      payload.download.end_time = endText;
    } else {
      if (!isValidHms(payload.download.daily_custom_window.start_time)) {
        return { ok: false, error: "每日相对时间段开始时间格式错误，必须是 HH:MM:SS" };
      }
      if (!isValidHms(payload.download.daily_custom_window.end_time)) {
        return { ok: false, error: "每日相对时间段结束时间格式错误，必须是 HH:MM:SS" };
      }
    }
  }

  const existingSheetRuleRows = normalizeSheetRules(payload?.feishu_sheet_import?.sheet_rules);
  const activeSheetRuleRows = hasMeaningfulSheetRuleRows(sheetRuleRows || [])
    ? (sheetRuleRows || [])
    : existingSheetRuleRows;
  if (hasMeaningfulSheetRuleRows(activeSheetRuleRows)) {
    try {
      payload.feishu_sheet_import.sheet_rules = buildSheetRulesObject(activeSheetRuleRows);
    } catch (err) {
      return { ok: false, error: `映射规则配置错误（sheet_rules）: ${err}` };
    }
  } else if (payload.feishu_sheet_import && typeof payload.feishu_sheet_import === "object") {
    delete payload.feishu_sheet_import.sheet_rules;
  }

  const handoverValidation = validateAndNormalizeHandoverCellRules(payload);
  if (!handoverValidation.ok) {
    return handoverValidation;
  }
  const handoverTemplateValidation = validateAndNormalizeHandoverTemplate(payload);
  if (!handoverTemplateValidation.ok) {
    return handoverTemplateValidation;
  }
  const handoverDownloadValidation = validateAndNormalizeHandoverDownload(payload);
  if (!handoverDownloadValidation.ok) {
    return handoverDownloadValidation;
  }
  const handoverShiftRosterValidation = validateAndNormalizeHandoverShiftRoster(payload);
  if (!handoverShiftRosterValidation.ok) {
    return handoverShiftRosterValidation;
  }
  const handoverEventSectionsValidation = validateAndNormalizeHandoverEventSections(payload);
  if (!handoverEventSectionsValidation.ok) {
    return handoverEventSectionsValidation;
  }
  const monthlyEventReportValidation = validateAndNormalizeMonthlyEventReport(payload);
  if (!monthlyEventReportValidation.ok) {
    return monthlyEventReportValidation;
  }
  const monthlyChangeReportValidation = validateAndNormalizeMonthlyChangeReport(payload);
  if (!monthlyChangeReportValidation.ok) {
    return monthlyChangeReportValidation;
  }
  const handoverChangeManagementValidation = validateAndNormalizeHandoverChangeManagementSection(payload);
  if (!handoverChangeManagementValidation.ok) {
    return handoverChangeManagementValidation;
  }
  const handoverExerciseManagementValidation = validateAndNormalizeHandoverExerciseManagementSection(payload);
  if (!handoverExerciseManagementValidation.ok) {
    return handoverExerciseManagementValidation;
  }
  const handoverMaintenanceManagementValidation = validateAndNormalizeHandoverMaintenanceManagementSection(payload);
  if (!handoverMaintenanceManagementValidation.ok) {
    return handoverMaintenanceManagementValidation;
  }
  const handoverOtherImportantWorkValidation = validateAndNormalizeHandoverOtherImportantWorkSection(payload);
  if (!handoverOtherImportantWorkValidation.ok) {
    return handoverOtherImportantWorkValidation;
  }
  const dayMetricTargetValidation = validateAndNormalizeDayMetricUploadTarget(payload);
  if (!dayMetricTargetValidation.ok) {
    return dayMetricTargetValidation;
  }
  const handoverSourceDataAttachmentValidation = validateAndNormalizeHandoverSourceDataAttachmentExport(payload);
  if (!handoverSourceDataAttachmentValidation.ok) {
    return handoverSourceDataAttachmentValidation;
  }
  const handoverCloudSheetSyncValidation = validateAndNormalizeHandoverCloudSheetSync(payload);
  if (!handoverCloudSheetSyncValidation.ok) {
    return handoverCloudSheetSyncValidation;
  }
  const handoverReviewUiValidation = validateAndNormalizeHandoverReviewUi(payload);
  if (!handoverReviewUiValidation.ok) {
    return handoverReviewUiValidation;
  }
  const alarmExportValidation = validateAndNormalizeAlarmExport(payload);
  if (!alarmExportValidation.ok) {
    return alarmExportValidation;
  }
  const wetBulbCollectionValidation = validateAndNormalizeWetBulbCollection(payload);
  if (!wetBulbCollectionValidation.ok) {
    return wetBulbCollectionValidation;
  }

  return { ok: true, payload };
}
