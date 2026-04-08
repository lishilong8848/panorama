import { normalizeSiteHost } from "./config_helpers.js";

const DASHBOARD_MODULE_STORAGE_KEY = "dashboard_active_module";

function normalizeLocalRoleMode(value) {
  const text = String(value || "").trim().toLowerCase();
  if (["internal", "external"].includes(text)) return text;
  return "";
}

function setBodyScrollLock(locked) {
  if (typeof document === "undefined" || !document.body) return;
  document.body.style.overflow = locked ? "hidden" : "";
}

export function createUiLocalActions(ctx) {
  const {
    currentView,
    activeConfigTab,
    config,
    manualFile,
    sheetFile,
    dayMetricLocalFile,
    handoverFilesByBuilding,
    sheetRuleRows,
    logs,
    logFilter,
    message,
    dashboardModules,
    dashboardActiveModule,
    dashboardModuleMenuOpen,
    handoverRuleScope,
  } = ctx;

  function openDashboardPage() {
    const roleMode = normalizeLocalRoleMode(config.value?.deployment?.role_mode || "");
    currentView.value = roleMode === "internal" ? "status" : "dashboard";
    closeDashboardMenuDrawer();
  }

  function openStatusPage() {
    currentView.value = "status";
    closeDashboardMenuDrawer();
  }

  function openConfigPage() {
    currentView.value = "config";
    closeDashboardMenuDrawer();
  }

  function switchConfigTab(tabKey) {
    const nextTab = String(tabKey || "").trim() || "common_paths";
    const roleMode = normalizeLocalRoleMode(config.value?.deployment?.role_mode || "");
    const hiddenCommonTabs = roleMode === "internal"
      ? new Set(["common_scheduler", "common_notify", "common_feishu_auth"])
      : new Set(roleMode ? ["common_alarm_db"] : []);
    const hiddenFeatureTabs = new Set(["feature_alarm"]);
    if (roleMode === "internal") {
      hiddenFeatureTabs.add("feature_monthly");
      hiddenFeatureTabs.add("feature_handover");
      hiddenFeatureTabs.add("feature_wet_bulb_collection");
      hiddenFeatureTabs.add("feature_alarm_export");
      hiddenFeatureTabs.add("feature_sheet");
      hiddenFeatureTabs.add("feature_manual");
    }
    if (hiddenCommonTabs.has(nextTab)) {
      activeConfigTab.value = "common_deployment";
      return;
    }
    if (hiddenFeatureTabs.has(nextTab)) {
      activeConfigTab.value = "common_deployment";
      return;
    }
    activeConfigTab.value = nextTab;
  }

  function setDashboardActiveModule(moduleId) {
    const id = String(moduleId || "").trim();
    if (!id) return;
    const found = (dashboardModules.value || []).some((item) => item.id === id);
    if (!found) return;
    dashboardActiveModule.value = id;
    closeDashboardMenuDrawer();
    if (typeof window !== "undefined" && window.localStorage) {
      try {
        window.localStorage.setItem(DASHBOARD_MODULE_STORAGE_KEY, id);
      } catch (_) {
        // ignore localStorage errors
      }
    }
  }

  function openDashboardMenuDrawer() {
    dashboardModuleMenuOpen.value = true;
    setBodyScrollLock(true);
  }

  function closeDashboardMenuDrawer() {
    dashboardModuleMenuOpen.value = false;
    setBodyScrollLock(false);
  }

  function onManualFileChange(e) {
    manualFile.value = e?.target?.files && e.target.files[0] ? e.target.files[0] : null;
  }

  function onSheetFileChange(e) {
    sheetFile.value = e?.target?.files && e.target.files[0] ? e.target.files[0] : null;
  }

  function onDayMetricLocalFileChange(e) {
    dayMetricLocalFile.value = e?.target?.files && e.target.files[0] ? e.target.files[0] : null;
  }

  function onHandoverBuildingFileChange(building, e) {
    const key = String(building || "").trim();
    if (!key) return;
    handoverFilesByBuilding[key] = e?.target?.files && e.target.files[0] ? e.target.files[0] : null;
  }

  function addSiteRow() {
    if (!config.value) return;
    config.value.download.sites.push({
      building: "",
      enabled: false,
      host: "",
      username: "",
      password: "",
    });
  }

  function removeSiteRow(index) {
    if (!config.value) return;
    config.value.download.sites.splice(index, 1);
  }

  function addSheetRuleRow() {
    sheetRuleRows.value.push({ sheet_name: "", table_id: "", header_row: 1 });
  }

  function removeSheetRuleRow(index) {
    if (sheetRuleRows.value.length <= 1) {
      sheetRuleRows.value = [{ sheet_name: "", table_id: "", header_row: 1 }];
      return;
    }
    sheetRuleRows.value.splice(index, 1);
  }

  function previewSiteUrl(site) {
    const host = normalizeSiteHost(site?.host || "");
    return host ? `http://${host}/page/main/main.html` : "-";
  }

  function clearLogs() {
    logs.value = [];
    logFilter.value = "";
    message.value = "日志已清空";
  }

  function ensureHandoverCellRulesShape() {
    if (!config.value) return null;
    config.value.handover_log = config.value.handover_log || {};
    const handover = config.value.handover_log;
    handover.cell_rules = handover.cell_rules || {};
    if (!Array.isArray(handover.cell_rules.default_rows)) {
      handover.cell_rules.default_rows = [];
    }
    if (!handover.cell_rules.building_rows || typeof handover.cell_rules.building_rows !== "object") {
      handover.cell_rules.building_rows = {};
    }
    return handover.cell_rules;
  }

  function getActiveHandoverRuleRows() {
    const cellRules = ensureHandoverCellRulesShape();
    if (!cellRules) return [];
    const scope = String(handoverRuleScope?.value || "default").trim() || "default";
    if (scope === "default") {
      return cellRules.default_rows;
    }
    if (!Array.isArray(cellRules.building_rows[scope])) {
      cellRules.building_rows[scope] = [];
    }
    return cellRules.building_rows[scope];
  }

  function addHandoverRuleRow() {
    const rows = getActiveHandoverRuleRows();
    rows.push({
      id: "",
      enabled: true,
      target_cell: "",
      rule_type: "direct",
      d_keywords: [],
      match_mode: "contains_casefold",
      agg: "first",
      template: "{value}",
      computed_op: "",
      params: {},
    });
  }

  function removeHandoverRuleRow(index) {
    const rows = getActiveHandoverRuleRows();
    if (!Array.isArray(rows)) return;
    if (index < 0 || index >= rows.length) return;
    rows.splice(index, 1);
  }

  function updateHandoverRuleKeywords(row, text) {
    if (!row || typeof row !== "object") return;
    row.d_keywords = String(text || "")
      .split(/[，,]/)
      .map((x) => x.trim())
      .filter((x) => x);
  }

  function getHandoverComputedPreset(row) {
    const op = String(row?.computed_op || "").trim();
    if (!op) return "__expr__";
    if (op === "chiller_mode_summary" || op === "ring_supply_temp" || op === "tank_backup") {
      return op;
    }
    return "__expr__";
  }

  function onHandoverComputedPresetChange(row, preset) {
    if (!row || typeof row !== "object") return;
    const value = String(preset || "").trim();
    if (value === "chiller_mode_summary" || value === "ring_supply_temp" || value === "tank_backup") {
      row.computed_op = value;
      return;
    }
    if (value === "__expr__" && (row.computed_op === "chiller_mode_summary" || row.computed_op === "ring_supply_temp" || row.computed_op === "tank_backup")) {
      row.computed_op = "";
    }
  }

  function copyAllDefaultRulesToCurrentBuilding() {
    const cellRules = ensureHandoverCellRulesShape();
    if (!cellRules) return;
    const scope = String(handoverRuleScope?.value || "default").trim() || "default";
    if (scope === "default") return;
    cellRules.building_rows[scope] = (Array.isArray(cellRules.default_rows) ? cellRules.default_rows : []).map((row) => ({
      ...row,
      d_keywords: Array.isArray(row.d_keywords) ? [...row.d_keywords] : [],
      params: row.params && typeof row.params === "object" ? { ...row.params } : {},
    }));
    message.value = `已复制全局默认规则到 ${scope} 覆盖`;
  }

  function clearCurrentBuildingOverrides() {
    const cellRules = ensureHandoverCellRulesShape();
    if (!cellRules) return;
    const scope = String(handoverRuleScope?.value || "default").trim() || "default";
    if (scope === "default") return;
    cellRules.building_rows[scope] = [];
    message.value = `已清空 ${scope} 覆盖规则`;
  }

  function restoreDefaultRuleForCurrentBuilding(ruleId) {
    const cellRules = ensureHandoverCellRulesShape();
    if (!cellRules) return;
    const scope = String(handoverRuleScope?.value || "default").trim() || "default";
    if (scope === "default") return;
    const rows = Array.isArray(cellRules.building_rows[scope]) ? cellRules.building_rows[scope] : [];
    const targetId = String(ruleId || "").trim();
    if (!targetId) return;
    const next = rows.filter((row) => String(row?.id || "").trim() !== targetId);
    cellRules.building_rows[scope] = next;
    message.value = `已删除 ${scope} 对规则 ${targetId} 的覆盖，将回退到全局默认`;
  }

  return {
    openStatusPage,
    openDashboardPage,
    openConfigPage,
    switchConfigTab,
    setDashboardActiveModule,
    openDashboardMenuDrawer,
    closeDashboardMenuDrawer,
      onManualFileChange,
      onSheetFileChange,
      onDayMetricLocalFileChange,
      onHandoverBuildingFileChange,
    addSiteRow,
    removeSiteRow,
    addSheetRuleRow,
    removeSheetRuleRow,
    previewSiteUrl,
    clearLogs,
    getActiveHandoverRuleRows,
    addHandoverRuleRow,
    removeHandoverRuleRow,
    updateHandoverRuleKeywords,
    getHandoverComputedPreset,
    onHandoverComputedPresetChange,
    copyAllDefaultRulesToCurrentBuilding,
    clearCurrentBuildingOverrides,
    restoreDefaultRuleForCurrentBuilding,
  };
}
