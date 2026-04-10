import { apiDatetimeToLocal, ensureConfigShape, todayText } from "./config_helpers.js";
import { mapSchedulerDecisionText, mapSchedulerTriggerText } from "./scheduler_text.js";
import { createActionGuard } from "./action_guard.js";
import { mapUpdaterResultText } from "./updater_text.js";
import { getDashboardMenuGroupsForRole } from "./dashboard_menu_config.js";

function buildDashboardModules(menuGroups) {
  return menuGroups.flatMap((group) =>
    (Array.isArray(group.items) ? group.items : []).map((item) => ({
      ...item,
      group_id: group.id,
      group_title: group.title,
    })),
  );
}

function resolveDeploymentRoleMode(roleMode) {
  const text = String(roleMode || "").trim().toLowerCase();
  return ["internal", "external"].includes(text) ? text : "";
}

function normalizeDashboardRoleMode(roleMode) {
  return resolveDeploymentRoleMode(roleMode) || "external";
}

function filterDashboardMenuGroupsByRole(roleMode) {
  const normalized = normalizeDashboardRoleMode(roleMode);
  const groups = getDashboardMenuGroupsForRole(normalized);
  return groups.map((group) => ({
    id: group.id,
    title: group.title,
    items: (Array.isArray(group.items) ? group.items : []).map((item) => ({
      ...item,
      group_id: group.id,
      group_title: group.title,
    })),
  }));
}

function buildRoleDashboardState(roleMode, preferredId = "") {
  const normalized = normalizeDashboardRoleMode(roleMode);
  const menuGroups = filterDashboardMenuGroupsByRole(normalized);
  const modules = buildDashboardModules(menuGroups);
  const defaultId = normalized === "internal" ? "runtime_logs" : "auto_flow";
  const activeModule = modules.some((item) => item.id === preferredId)
    ? preferredId
    : (modules.some((item) => item.id === defaultId) ? defaultId : (modules[0]?.id || "auto_flow"));
  return { menuGroups, modules, activeModule };
}

const DASHBOARD_MODULE_STORAGE_KEY = "dashboard_active_module";
const INTERNAL_BUILDINGS = Object.freeze(["A楼", "B楼", "C楼", "D楼", "E楼"]);

function basenameFromPath(input) {
  const text = String(input || "").trim();
  if (!text) return "";
  const parts = text.split(/[\\/]/).filter(Boolean);
  return parts.length ? parts[parts.length - 1] : text;
}

function shiftTextFromCode(shift) {
  const text = String(shift || "").trim().toLowerCase();
  if (text === "day") return "白班";
  if (text === "night") return "夜班";
  return String(shift || "").trim() || "-";
}

function normalizeSchedulerText(value, fallback = "-") {
  const text = String(value || "").trim();
  return text || fallback;
}

function normalizeSchedulerDateText(value, fallback = "未安排") {
  const text = String(value || "").trim();
  return text || fallback;
}

function hasSchedulerFailureMarker(...values) {
  return values.some((value) => {
    const text = String(value || "").trim().toLowerCase();
    if (!text) return false;
    return ["fail", "failed", "error", "exception", "异常", "失败"].some((marker) => text.includes(marker));
  });
}

function normalizeSchedulerStatusVm({ running = false, configured = false, status = "", lastTriggerResult = "", lastDecision = "" }) {
  if (hasSchedulerFailureMarker(status, lastTriggerResult, lastDecision)) {
    return { statusText: "异常", tone: "danger" };
  }
  if (running) {
    return { statusText: "已启动", tone: "success" };
  }
  if (!configured) {
    return { statusText: "未配置", tone: "warning" };
  }
  return { statusText: "未启动", tone: "neutral" };
}

function buildMonthlySchedulerRunText(dayOfMonth, runTime) {
  const day = Number.parseInt(String(dayOfMonth || "").trim(), 10);
  const time = String(runTime || "").trim();
  if (Number.isFinite(day) && day > 0 && time) {
    return `每月${day}号 ${time}`;
  }
  if (Number.isFinite(day) && day > 0) {
    return `每月${day}号`;
  }
  if (time) {
    return time;
  }
  return "未设置";
}

function toComparableSchedulerDateText(value) {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?$/.test(text) ? text : "";
}

function compareSchedulerDateText(left, right) {
  const leftText = toComparableSchedulerDateText(left);
  const rightText = toComparableSchedulerDateText(right);
  if (!leftText && !rightText) return 0;
  if (!leftText) return 1;
  if (!rightText) return -1;
  return leftText.localeCompare(rightText);
}

function getDailyReportBrowserLabel(rawAuth, fallback = "系统浏览器") {
  const label = String(rawAuth?.browser_label || "").trim();
  return label || fallback;
}

function mapCloudSheetSyncVm(raw) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const attempted = Boolean(raw?.attempted);
  const url = String(raw?.spreadsheet_url || "").trim();
  const error = String(raw?.error || "").trim();
  if (status === "success") {
    return { text: "云表已同步", tone: "success", url, error };
  }
  if (status === "pending_upload") {
    return { text: "云表待最终上传", tone: "warning", url, error };
  }
  if (status === "prepare_failed") {
    return { text: "云表预建失败", tone: "danger", url, error };
  }
  if (status === "failed") {
    return { text: "云表最终上传失败", tone: "danger", url, error };
  }
  if (status === "disabled") {
    return { text: "云表未启用", tone: "neutral", url, error };
  }
  if (status === "skipped") {
    return { text: "云表未执行", tone: "neutral", url, error };
  }
  if (attempted) {
    return { text: "云表已尝试同步", tone: "info", url, error };
  }
  return { text: "云表未执行", tone: "neutral", url, error };
}

function buildHandoverReviewRowSnapshot(reviewRows, reviewLinks) {
  const normalizedReviewRows = Array.isArray(reviewRows) ? reviewRows : [];
  const normalizedReviewLinks = Array.isArray(reviewLinks) ? reviewLinks : [];
  const reviewMap = new Map();
  const linkMap = new Map();
  const orderedBuildings = [...INTERNAL_BUILDINGS];

  normalizedReviewRows.forEach((row) => {
    const building = String(row?.building || "").trim();
    if (!building) return;
    if (!reviewMap.has(building)) {
      reviewMap.set(building, row || {});
    }
    if (!orderedBuildings.includes(building)) {
      orderedBuildings.push(building);
    }
  });

  normalizedReviewLinks.forEach((row) => {
    const building = String(row?.building || "").trim();
    const url = String(row?.url || "").trim();
    if (!building || !url) return;
    if (!linkMap.has(building)) {
      linkMap.set(building, {
        building,
        code: String(row?.code || "").trim().toLowerCase(),
        url,
      });
    }
    if (!orderedBuildings.includes(building)) {
      orderedBuildings.push(building);
    }
  });

  return orderedBuildings.map((building) => {
    const reviewRow = reviewMap.get(building) || null;
    const link = linkMap.get(building) || null;
    let status = "missing";
    let text = "未生成";
    let tone = "neutral";

    if (reviewRow) {
      if (reviewRow?.has_session) {
        if (reviewRow?.confirmed) {
          status = "confirmed";
          text = "已确认";
          tone = "success";
        } else {
          status = "pending";
          text = "待确认";
          tone = "warning";
        }
      }
    } else if (link?.url) {
      status = "reachable";
      text = "可访问";
      tone = "info";
    }

    const cloudSheetSyncVm = mapCloudSheetSyncVm(reviewRow?.cloud_sheet_sync || {});
    return {
      building,
      reviewRow,
      link,
      status,
      text,
      tone,
      url: link?.url || "",
      hasUrl: Boolean(String(link?.url || "").trim()),
      cloudSheetSyncText: cloudSheetSyncVm.text,
      cloudSheetSyncTone: cloudSheetSyncVm.tone,
      cloudSheetUrl: cloudSheetSyncVm.url,
      hasCloudSheetUrl: Boolean(cloudSheetSyncVm.url),
      cloudSheetError: cloudSheetSyncVm.error,
    };
  });
}

function mapDailyReportAuthVm(raw) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const error = String(raw?.error || "").trim();
  const browserLabel = getDailyReportBrowserLabel(raw);
  if (status === "ready") {
    return { text: "已登录", tone: "success", error: "" };
  }
  if (status === "ready_without_target_page") {
    return {
      text: "已登录，待打开目标页",
      tone: "info",
      error: `当前已接管${browserLabel}，但尚未定位到飞书目标页；执行截图测试时会自动尝试补开。`,
    };
  }
  if (status === "missing_login") {
    if (error === "browser_not_started") {
      return {
        text: "待登录",
        tone: "warning",
        error: `${browserLabel} 登录页尚未打开，请点击“初始化飞书截图登录态”。`,
      };
    }
    if (error === "browser_started_without_pages") {
      return {
        text: "待打开飞书页",
        tone: "warning",
        error: `${browserLabel} 登录页已被关闭，请点击“初始化飞书截图登录态”重新打开。`,
      };
    }
    if (error === "feishu_page_not_open") {
      return {
        text: "已登录，待打开目标页",
        tone: "info",
        error: `当前${browserLabel}中未检测到飞书目标页；执行截图测试时会自动尝试补开。`,
      };
    }
    return {
      text: "待登录",
      tone: "warning",
      error: error === "login_required" || !error ? `当前${browserLabel}中的飞书登录态未就绪，请完成扫码登录。` : error,
    };
  }
  if (status === "expired") {
    return {
      text: "已失效",
      tone: "warning",
      error: error || "当前飞书截图登录态已失效，请重新初始化并扫码登录。",
    };
  }
  if (status === "browser_unavailable") {
    if (error.includes("未找到系统 Edge 或 Chrome")) {
      return {
        text: "浏览器不可用",
        tone: "danger",
        error: "未找到可用系统浏览器（Edge/Chrome），请安装 Microsoft Edge 或 Google Chrome。",
      };
    }
    if (error.includes("browser_debug_port_unavailable")) {
      return {
        text: "浏览器不可用",
        tone: "danger",
        error: `请先关闭所有 ${browserLabel} 窗口后，再点击“初始化飞书截图登录态”。`,
      };
    }
    return {
      text: "浏览器不可用",
      tone: "danger",
      error: error || `当前无法接管${browserLabel}，请检查其是否已安装并可正常启动。`,
    };
  }
  return { text: "未初始化", tone: "neutral", error: error || "截图登录态尚未初始化。" };
}

function mapDailyReportExportVm(raw, authRaw = {}) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const errorCode = String(raw?.error_code || "").trim();
  const rawError = String(raw?.error || "").trim();
  const rawDetail = String(raw?.error_detail || "").trim();
  const browserLabel = getDailyReportBrowserLabel(authRaw);
  const error =
    rawError === "login_required"
      ? `当前${browserLabel}中的飞书登录态未就绪，请完成扫码登录。`
      : rawError ||
        (errorCode === "daily_report_url_field_invalid"
          ? "日报链接字段写入失败，请检查飞书多维表“交接班日报”字段类型。"
          : errorCode === "missing_spreadsheet_url"
            ? "当前批次缺少云文档链接，无法重写日报记录。"
            : errorCode === "missing_effective_asset"
              ? "当前最终生效截图不完整，无法重写日报记录。"
              : rawDetail);
  if (status === "success") {
    return { text: "日报多维记录已写入", tone: "success", error };
  }
  if (status === "pending") {
    return { text: "日报多维待写入", tone: "warning", error };
  }
  if (status === "skipped_due_to_cloud_sync_not_ok") {
    return { text: "等待本批次云文档全部成功", tone: "neutral", error };
  }
  if (status === "login_required") {
    return { text: "需要登录飞书后才能自动截图", tone: "warning", error };
  }
  if (status === "capture_failed") {
    return { text: "截图失败，日报记录未写入", tone: "danger", error };
  }
  if (status === "pending_asset_rewrite") {
    return { text: "截图已更新，待重写日报记录", tone: "warning", error };
  }
  if (status === "failed") {
    return { text: "日报多维写入失败", tone: "danger", error };
  }
  if (status === "skipped") {
    return { text: "日报多维已跳过", tone: "neutral", error };
  }
  return { text: "日报多维未执行", tone: "neutral", error };
}

function formatSharedBridgeRuntimeError(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const normalized = text.toLowerCase();
  if (normalized === "shared_bridge_disabled") return "共享桥接未启用";
  if (normalized === "shared_bridge_service_unavailable") return "共享桥接服务不可用";
  if (normalized === "disabled_or_switching" || normalized === "disabled_or_unselected") return "当前未启用共享桥接";
  if (normalized === "misconfigured") return "共享桥接目录未配置";
  if (normalized === "database is locked") return "共享桥接数据库正忙，请稍后重试";
  if (normalized === "unable to open database file") return "无法打开共享桥接数据库文件";
  if (normalized === "cannot operate on a closed database" || normalized === "cannot operate on a closed database.") {
    return "共享桥接数据库连接已关闭";
  }
  if (normalized.includes("permissionerror") || normalized.includes("winerror 5")) {
    return "共享桥接目录无写入权限";
  }
  if (normalized.includes("no such table")) {
    return "共享桥接数据库结构未初始化";
  }
  return text;
}

function formatInternalDownloadPoolError(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const normalized = text.toLowerCase();
  if (normalized.includes("net::err_empty_response")) {
    return "页面无响应，请检查楼栋页面服务或网络";
  }
  if (normalized.includes("net::err_connection_refused")) {
    return "页面拒绝连接，请检查楼栋页面服务是否启动";
  }
  if (normalized.includes("net::err_connection_timed_out") || normalized.includes("net::err_timed_out")) {
    return "页面访问超时，请检查楼栋网络或站点状态";
  }
  if (normalized.includes("net::err_name_not_resolved")) {
    return "页面地址无法解析，请检查楼栋地址配置";
  }
  if (normalized.includes("net::err_internet_disconnected")) {
    return "网络未连接，请检查当前网络";
  }
  if (normalized.includes("browser_context_missing")) {
    return "浏览器上下文不可用，系统会自动重建";
  }
  if (normalized.includes("browser_unavailable")) {
    return "浏览器不可用，请检查系统浏览器和 Playwright";
  }
  if (normalized.includes("login_required")) {
    return "登录态已失效，任务开始前会自动重登";
  }
  if (normalized.includes("page.goto:")) {
    return "页面访问失败，请检查楼栋页面是否可达";
  }
  return formatSharedBridgeRuntimeError(text);
}

function mapDailyReportScreenshotTestVm(raw, options = {}) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const rawError = String(raw?.error || "").trim();
  const rawErrorMessage = String(raw?.error_message || "").trim();
  const browserLabel = String(options.browserLabel || "").trim() || "系统浏览器";
  let error = rawErrorMessage || rawError;
  if (rawError === "target_page_not_open") {
    error = `目标网页当前没有在${browserLabel}中打开，请先打开对应页面后再测试截图。`;
  } else if (rawError === "target_page_mismatch") {
    error = "当前打开页面与目标页面不一致，请重新打开对应飞书页面后重试。";
  } else if (rawError === "login_required") {
    error = `当前${browserLabel}中的飞书登录态未就绪，请先完成扫码登录。`;
  } else if (rawError === "timeout") {
    error = "截图操作超时，请查看系统错误日志后重试。";
  }
  const path = String(raw?.path || raw?.stored_path || "").trim();
  const capturedAt = String(raw?.captured_at || "").trim();
  const fallbackExists = Boolean(options.fallbackExists);
  const fallbackPath = String(options.fallbackPath || "").trim();
  const fallbackCapturedAt = String(options.fallbackCapturedAt || "").trim();
  const skippedText = String(options.skippedText || "本次测试已跳过").trim() || "本次测试已跳过";
  if (status === "ok") {
    return {
      text: "最近测试成功",
      tone: "success",
      error: "",
      detail: capturedAt || basenameFromPath(path) || "",
    };
  }
  if (status === "skipped") {
    return {
      text: skippedText,
      tone: "neutral",
      error,
      detail: "",
    };
  }
  if (status === "failed" || status === "error") {
    return {
      text: "最近测试失败",
      tone: "danger",
      error: error || "截图测试失败",
      detail: "",
    };
  }
  if (fallbackExists) {
    return {
      text: "已有截图文件",
      tone: "info",
      error: "",
      detail: fallbackCapturedAt || basenameFromPath(fallbackPath) || "",
    };
  }
  return { text: "尚未测试", tone: "neutral", error: "", detail: "" };
}

function emptyDailyReportAssetVariant() {
  return {
    exists: false,
    stored_path: "",
    captured_at: "",
    preview_url: "",
    thumbnail_url: "",
    full_image_url: "",
  };
}

function buildDailyReportAssetDownloadName(dutyDate, dutyShift, title) {
  const dutyDateText = String(dutyDate || "").trim() || "unknown-date";
  const dutyShiftText = String(dutyShift || "").trim().toLowerCase() || "unknown-shift";
  const titleText = String(title || "").trim() || "日报截图";
  return `${dutyDateText}_${dutyShiftText}_${titleText}.png`;
}

function mapDailyReportLastWrittenSourceVm(source) {
  const text = String(source || "").trim().toLowerCase();
  if (text === "manual") {
    return { text: "上次入库：手工图", tone: "warning", exists: true };
  }
  if (text === "auto") {
    return { text: "上次入库：自动图", tone: "info", exists: true };
  }
  return { text: "尚未写入日报", tone: "neutral", exists: false };
}

function normalizeDayMetricUnitTone(status) {
  const text = String(status || "").trim().toLowerCase();
  if (text === "ok" || text === "success") return "success";
  if (text === "failed") return "danger";
  if (text === "skipped") return "neutral";
  return "warning";
}

function normalizeDayMetricUnitStatusText(status) {
  const text = String(status || "").trim().toLowerCase();
  if (text === "ok" || text === "success") return "成功";
  if (text === "failed") return "失败";
  if (text === "skipped") return "跳过";
  return text || "-";
}

function normalizeDayMetricUnitStageText(stage) {
  const text = String(stage || "").trim().toLowerCase();
  if (text === "download") return "下载";
  if (text === "attachment") return "附件上传";
  if (text === "extract") return "提取";
  if (text === "rewrite") return "重写";
  if (text === "upload") return "上传";
  return text || "-";
}

function normalizeDayMetricNetworkModeText(mode) {
  const text = String(mode || "").trim().toLowerCase();
  if (text === "auto_switch") return "当前角色网络";
  if (text === "current_network") return "当前角色网络";
  return text || "-";
}

function normalizeDailyReportAssetCard(raw, title, options = {}) {
  const asset = raw && typeof raw === "object" ? raw : {};
  const auto = asset.auto && typeof asset.auto === "object" ? asset.auto : emptyDailyReportAssetVariant();
  const manual = asset.manual && typeof asset.manual === "object" ? asset.manual : emptyDailyReportAssetVariant();
  const source = String(asset.source || "").trim().toLowerCase();
  const effectiveExists = Boolean(asset.exists);
  const effectiveThumbnailUrl = String(asset.thumbnail_url || asset.preview_url || "").trim();
  const effectiveFullImageUrl = String(asset.full_image_url || asset.preview_url || "").trim();
  const effectiveCapturedAt = String(asset.captured_at || "").trim();
  const dutyDate = String(options.dutyDate || "").trim();
  const dutyShift = String(options.dutyShift || "").trim().toLowerCase();
  const lastWrittenSourceVm = mapDailyReportLastWrittenSourceVm(options.lastWrittenSource || "");
  const sourceText =
    source === "manual" ? "手工" : source === "auto" ? "自动" : "未生成";
  return {
    title,
    exists: effectiveExists,
    source,
    sourceText,
    stored_path: String(asset.stored_path || "").trim(),
    captured_at: effectiveCapturedAt,
    preview_url: effectiveThumbnailUrl,
    thumbnail_url: effectiveThumbnailUrl,
    full_image_url: effectiveFullImageUrl,
    auto,
    manual,
    hasManual: Boolean(manual.exists),
    hasAuto: Boolean(auto.exists),
    downloadName: buildDailyReportAssetDownloadName(dutyDate, dutyShift, title),
    lastWrittenSource: String(options.lastWrittenSource || "").trim().toLowerCase(),
    lastWrittenSourceText: lastWrittenSourceVm.text,
    lastWrittenSourceTone: lastWrittenSourceVm.tone,
    hasLastWrittenSource: lastWrittenSourceVm.exists,
  };
}

function isBridgeTerminalStatus(status) {
  const text = String(status || "").trim().toLowerCase();
  return text === "success" || text === "failed" || text === "partial_failed" || text === "cancelled" || text === "stale";
}

function resolveInitialDashboardModule() {
  const defaultId = buildRoleDashboardState("external").activeModule;
  if (typeof window === "undefined" || !window.localStorage) {
    return defaultId;
  }
  try {
    const value = String(window.localStorage.getItem(DASHBOARD_MODULE_STORAGE_KEY) || "").trim();
    const externalModules = buildRoleDashboardState("external").modules;
    if (value && externalModules.some((item) => item.id === value)) {
      return value;
    }
  } catch (_) {
    // ignore localStorage errors
  }
  return defaultId;
}

export function createAppState(vueApi) {
  const { reactive, ref, computed } = vueApi;
  const actionGuard = createActionGuard(vueApi);

  const health = reactive({
    version: "",
    startup_time: "",
    startup_role_confirmed: false,
    role_selection_required: false,
    startup_handoff: {
      active: false,
      mode: "",
      target_role_mode: "",
      requested_at: "",
      reason: "",
      nonce: "",
    },
    runtime_activated: false,
    activation_phase: "",
    activation_error: "",
    active_job_id: "",
    active_job_ids: [],
    job_counts: {},
    scheduler: {
      status: "-",
      next_run_time: "-",
      enabled: false,
      running: false,
      started_at: "",
      last_check_at: "",
      last_decision: "",
      last_trigger_at: "",
      last_trigger_result: "",
      state_path: "",
      state_exists: false,
    },
    handover_scheduler: {
      enabled: false,
      running: false,
      status: "-",
      executor_bound: false,
      callback_name: "-",
      morning: {
        next_run_time: "",
        last_decision: "",
        last_trigger_at: "",
        last_trigger_result: "",
        state_path: "",
        state_exists: false,
      },
      afternoon: {
        next_run_time: "",
        last_decision: "",
        last_trigger_at: "",
        last_trigger_result: "",
        state_path: "",
        state_exists: false,
      },
      state_paths: {},
    },
    handover: {
      engineer_directory: {
        target_preview: {
          configured_app_token: "",
          operation_app_token: "",
          table_id: "",
          target_kind: "",
          display_url: "",
          bitable_url: "",
          wiki_node_token: "",
          message: "",
          resolved_at: "",
        },
      },
      review_status: {
        batch_key: "",
        duty_date: "",
        duty_shift: "",
        has_any_session: false,
        confirmed_count: 0,
        required_count: 5,
        all_confirmed: false,
        ready_for_followup_upload: false,
        buildings: [],
        followup_progress: {
          status: "idle",
          can_resume_followup: false,
          pending_count: 0,
          failed_count: 0,
          attachment_pending_count: 0,
          cloud_pending_count: 0,
          daily_report_status: "idle",
        },
      },
      review_links: [],
      review_base_url: "",
      review_base_url_effective: "",
      review_base_url_effective_source: "",
      review_base_url_candidates: [],
      review_base_url_status: "",
      review_base_url_error: "",
      review_base_url_validated_candidates: [],
      review_base_url_candidate_results: [],
      review_base_url_manual_available: true,
      configured: false,
      review_base_url_configured_at: "",
      review_base_url_last_probe_at: "",
    },
    wet_bulb_collection: {
      enabled: false,
      scheduler: {
        running: false,
        status: "-",
        next_run_time: "",
        last_check_at: "",
        last_decision: "",
        last_trigger_at: "",
        last_trigger_result: "",
        state_path: "",
        state_exists: false,
        executor_bound: false,
        callback_name: "-",
      },
      target_preview: {
        configured_app_token: "",
        operation_app_token: "",
        table_id: "",
        target_kind: "",
        display_url: "",
        bitable_url: "",
        wiki_node_token: "",
        message: "",
        resolved_at: "",
      },
    },
    monthly_event_report: {
      enabled: false,
      scheduler: {
        running: false,
        status: "-",
        next_run_time: "",
        last_check_at: "",
        last_decision: "",
        last_trigger_at: "",
        last_trigger_result: "",
        state_path: "",
        state_exists: false,
        executor_bound: false,
        callback_name: "-",
      },
      last_run: {
        started_at: "",
        finished_at: "",
        status: "",
        report_type: "",
        scope: "",
        building: "",
        target_month: "",
        generated_files: 0,
        successful_buildings: [],
        failed_buildings: [],
        output_dir: "",
        files_by_building: {},
        error: "",
      },
      delivery: {
        error: "",
        last_run: {
          started_at: "",
          finished_at: "",
          status: "",
          report_type: "",
          scope: "",
          building: "",
          target_month: "",
          successful_buildings: [],
          failed_buildings: [],
          sent_count: 0,
          message_ids: {},
          error: "",
          test_mode: false,
          test_receive_id: "",
          test_receive_id_type: "",
          test_receive_ids: [],
          test_successful_receivers: [],
          test_failed_receivers: [],
          test_file_building: "",
          test_file_name: "",
        },
        recipient_status_by_building: [],
      },
    },
    monthly_change_report: {
      enabled: false,
      scheduler: {
        running: false,
        status: "-",
        next_run_time: "",
        last_check_at: "",
        last_decision: "",
        last_trigger_at: "",
        last_trigger_result: "",
        state_path: "",
        state_exists: false,
        executor_bound: false,
        callback_name: "-",
      },
      last_run: {
        started_at: "",
        finished_at: "",
        status: "",
        report_type: "",
        scope: "",
        building: "",
        target_month: "",
        generated_files: 0,
        successful_buildings: [],
        failed_buildings: [],
        output_dir: "",
        files_by_building: {},
        error: "",
      },
      delivery: {
        error: "",
        last_run: {
          started_at: "",
          finished_at: "",
          status: "",
          report_type: "",
          scope: "",
          building: "",
          target_month: "",
          successful_buildings: [],
          failed_buildings: [],
          sent_count: 0,
          message_ids: {},
          error: "",
          test_mode: false,
          test_receive_id: "",
          test_receive_id_type: "",
          test_receive_ids: [],
          test_successful_receivers: [],
          test_failed_receivers: [],
          test_file_building: "",
          test_file_name: "",
        },
        recipient_status_by_building: [],
      },
    },
    day_metric_upload: {
      scheduler: {
        enabled: false,
        running: false,
        status: "未初始化",
        next_run_time: "",
        last_check_at: "",
        last_decision: "",
        last_trigger_at: "",
        last_trigger_result: "",
        state_path: "",
        state_exists: false,
        executor_bound: false,
        callback_name: "",
      },
      target_preview: {
        configured_app_token: "",
        operation_app_token: "",
        table_id: "",
        target_kind: "",
        display_url: "",
        bitable_url: "",
        wiki_node_token: "",
        message: "",
        resolved_at: "",
      },
    },
    alarm_event_upload: {
      enabled: false,
      scheduler: {
        enabled: false,
        running: false,
        status: "未初始化",
        next_run_time: "",
        last_check_at: "",
        last_decision: "",
        last_trigger_at: "",
        last_trigger_result: "",
        state_path: "",
        state_exists: false,
        executor_bound: false,
        callback_name: "",
      },
      target_preview: {
        configured_app_token: "",
        operation_app_token: "",
        table_id: "",
        target_kind: "",
        display_url: "",
        bitable_url: "",
        wiki_node_token: "",
        message: "",
        resolved_at: "",
      },
    },
    deployment: {
      role_mode: "",
      node_id: "",
      node_label: "",
    },
    shared_bridge: {
      enabled: false,
      role_mode: "",
      root_dir: "",
      db_status: "disabled",
      last_error: "",
      last_poll_at: "",
      pending_internal: 0,
      pending_external: 0,
      problematic: 0,
      task_count: 0,
      node_count: 0,
      node_heartbeat_ok: false,
      agent_status: "disabled",
      heartbeat_interval_sec: 5,
      poll_interval_sec: 2,
      internal_download_pool: {
        enabled: false,
        browser_ready: false,
        page_slots: [],
        active_buildings: [],
        last_error: "",
      },
      internal_source_cache: {
        enabled: false,
        scheduler_running: false,
        current_hour_bucket: "",
        last_run_at: "",
        last_success_at: "",
        last_error: "",
        cache_root: "",
        current_hour_refresh: {
          running: false,
          last_run_at: "",
          last_success_at: "",
          last_error: "",
          failed_buildings: [],
          blocked_buildings: [],
          running_buildings: [],
          completed_buildings: [],
          scope_text: "当前小时",
        },
        handover_log_family: {
          ready_count: 0,
          failed_buildings: [],
          last_success_at: "",
          current_bucket: "",
          buildings: [],
        },
        handover_capacity_report_family: {
          ready_count: 0,
          failed_buildings: [],
          last_success_at: "",
          current_bucket: "",
          buildings: [],
        },
        monthly_report_family: {
          ready_count: 0,
          failed_buildings: [],
          last_success_at: "",
          current_bucket: "",
          buildings: [],
        },
        alarm_event_family: {
          ready_count: 0,
          failed_buildings: [],
          last_success_at: "",
          current_bucket: "",
          buildings: [],
        },
      },
      internal_alert_status: {
        buildings: [],
        active_count: 0,
        last_notified_at: "",
      },
    },
    network: { current_ssid: "-" },
    updater: {
      enabled: true,
      disabled_reason: "",
      running: false,
      last_check_at: "",
      last_result: "",
      last_error: "",
      local_version: "",
      remote_version: "",
      source_kind: "remote",
      source_label: "远端正式更新源",
      local_release_revision: 0,
      remote_release_revision: 0,
      state_path: "",
      update_available: false,
      force_apply_available: false,
      restart_required: false,
      dependency_sync_status: "idle",
      dependency_sync_error: "",
      dependency_sync_at: "",
      queued_apply: {
        queued: false,
        mode: "",
        queued_at: "",
        reason: "",
      },
      mirror_ready: false,
      mirror_version: "",
      mirror_manifest_path: "",
      last_publish_at: "",
      last_publish_error: "",
    },
    system_logs: [],
  });

  const config = ref(ensureConfigShape({}));
  const currentView = ref("dashboard");
  const activeConfigTab = ref("common_paths");

  const initialDashboardState = buildRoleDashboardState("external", resolveInitialDashboardModule());
  const dashboardMenuGroups = ref(initialDashboardState.menuGroups);
  const dashboardModules = ref(initialDashboardState.modules);
  const dashboardActiveModule = ref(initialDashboardState.activeModule);
  const dashboardModuleMenuOpen = ref(false);

  function applyDashboardRoleMode(roleMode) {
    const next = buildRoleDashboardState(roleMode, dashboardActiveModule.value);
    dashboardMenuGroups.value = next.menuGroups;
    dashboardModules.value = next.modules;
    dashboardActiveModule.value = next.activeModule;
  }

  const selectedDate = ref(todayText());
  const rangeStartDate = ref(todayText());
  const rangeEndDate = ref(todayText());
  const selectedDates = ref([]);
  const logs = ref([]);
  const logFilter = ref("");
  const currentJob = ref(null);
  const jobsList = ref([]);
  const selectedJobId = ref("");
  const bridgeTasks = ref([]);
  const selectedBridgeTaskId = ref("");
  const bridgeTaskDetail = ref(null);
  const resourceSnapshot = ref({
    network: {},
    controlled_browser: { holder_job_id: "", queue_length: 0 },
    batch_locks: [],
    resources: [],
  });
  const busy = ref(false);
  const message = ref("");
  const bootstrapReady = ref(false);
  const fullHealthLoaded = ref(false);
  const configLoaded = ref(false);
  const healthLoadError = ref("");
  const configLoadError = ref("");
  const engineerDirectoryLoaded = ref(false);
  const pendingResumeRuns = ref([]);
  const schedulerQuickSaving = ref(false);
  const handoverSchedulerQuickSaving = ref(false);
  const wetBulbSchedulerQuickSaving = ref(false);
  const dayMetricUploadSchedulerQuickSaving = ref(false);
  const alarmEventUploadSchedulerQuickSaving = ref(false);
  const monthlyEventReportSchedulerQuickSaving = ref(false);
  const monthlyChangeReportSchedulerQuickSaving = ref(false);
  const configAutoSaveSuspendDepth = ref(0);
  const autoResumeState = reactive({
    inProgress: false,
    lastRunId: "",
    lastTryTs: 0,
  });

  const buildingsText = ref("");
  const sheetRuleRows = ref([]);

  const manualBuilding = ref("");
  const manualFile = ref(null);
  const manualUploadDate = ref(todayText());
  const sheetFile = ref(null);
  const dayMetricUploadScope = ref("single");
  const dayMetricUploadBuilding = ref("");
  const dayMetricSelectedDate = ref(todayText());
  const dayMetricRangeStartDate = ref(todayText());
  const dayMetricRangeEndDate = ref(todayText());
  const dayMetricSelectedDates = ref([]);
  const dayMetricLocalBuilding = ref("");
  const dayMetricLocalDate = ref(todayText());
  const dayMetricLocalFile = ref(null);
  const handoverFile = ref(null);
  const handoverFilesByBuilding = reactive({});
  const handoverDutyDate = ref(todayText());
  const handoverDutyShift = ref("day");
  const handoverDownloadScope = ref("single");
  const handoverEngineerDirectory = ref([]);
  const handoverEngineerLoading = ref(false);
  const handoverDailyReportContext = ref({
    ok: true,
    batch_key: "",
    duty_date: "",
    duty_shift: "",
    daily_report_record_export: {
      status: "idle",
      updated_at: "",
      record_id: "",
      record_url: "",
      spreadsheet_url: "",
      error: "",
      summary_screenshot_path: "",
      external_screenshot_path: "",
      summary_screenshot_source_used: "",
      external_screenshot_source_used: "",
    },
    screenshot_auth: {
      status: "missing_login",
      profile_dir: "",
      last_checked_at: "",
      error: "",
      browser_kind: "",
      browser_label: "",
      browser_executable: "",
    },
    capture_assets: {
      summary_sheet_image: {
        exists: false,
        source: "none",
        stored_path: "",
        captured_at: "",
        preview_url: "",
        thumbnail_url: "",
        full_image_url: "",
        auto: emptyDailyReportAssetVariant(),
        manual: emptyDailyReportAssetVariant(),
      },
      external_page_image: {
        exists: false,
        source: "none",
        stored_path: "",
        captured_at: "",
        preview_url: "",
        thumbnail_url: "",
        full_image_url: "",
        auto: emptyDailyReportAssetVariant(),
        manual: emptyDailyReportAssetVariant(),
      },
    },
  });
  const handoverDailyReportLastScreenshotTest = ref({
    batch_key: "",
    status: "",
    tested_at: "",
    summary_sheet_image: { status: "", error: "", path: "" },
    external_page_image: { status: "", error: "", path: "" },
  });
  const handoverDailyReportPreviewModal = ref({
    open: false,
    title: "",
    imageUrl: "",
    downloadName: "",
  });
  const handoverDailyReportUploadModal = ref({
    open: false,
    target: "",
    title: "",
    hint: "",
  });
  const handoverRuleScope = ref("default");
  const handoverDutyAutoFollow = ref(true);
  const handoverDutyLastAutoAt = ref(0);
  const customAbsoluteStartLocal = ref("");
  const customAbsoluteEndLocal = ref("");

  const systemLogOffset = ref(0);
  const timers = {
    pollTimer: null,
    healthTimer: null,
    jobsTimer: null,
    bridgeTasksTimer: null,
    dailyReportContextTimer: null,
    handoverDutyTimer: null,
  };
  const streamController = {
    attachJobStream() {},
    attachSystemStream() {},
    closeJobStream() {},
    closeSystemStream() {},
    pauseAll() {},
    resumeAll() {},
    dispose() {},
  };

  const filteredLogs = computed(() => {
    const keyword = logFilter.value.trim();
    const filteredEntries = !keyword
      ? logs.value
      : logs.value.filter((entry) => String(entry?.line || "").includes(keyword));
    return filteredEntries.map((entry) => String(entry?.line || "").trim()).filter(Boolean);
  });
  const internalOpsLogs = computed(() => {
    const keyword = logFilter.value.trim();
    if (keyword) {
      return filteredLogs.value;
    }
    return logs.value
      .map((entry) => String(entry?.line || "").trim())
      .filter(Boolean)
      .filter((line) => (
        line.includes("[共享桥接]")
        || line.includes("内网下载")
        || line.includes("浏览器池")
        || line.includes("页池")
        || line.includes("共享目录更新")
        || line.includes("更新镜像")
      ));
  });

  const canRun = computed(() => {
    const updater = health.updater || {};
    const dependencyStatus = String(updater?.dependency_sync_status || "").trim().toLowerCase();
    const lastResult = String(updater?.last_result || "").trim().toLowerCase();
    const queuedApply = Boolean(updater?.queued_apply?.queued);
    const activeUpdaterResults = new Set([
      "downloading_patch",
      "applying_patch",
      "dependency_checking",
      "dependency_syncing",
      "dependency_rollback",
      "updated_restart_scheduled",
      "restart_pending",
    ]);
    return !(
      queuedApply ||
      Boolean(updater?.restart_required) ||
      dependencyStatus === "running" ||
      activeUpdaterResults.has(lastResult)
    );
  });
  const isStatusView = computed(() => currentView.value === "status");
  const isDashboardView = computed(() => currentView.value === "dashboard");
  const isConfigView = computed(() => currentView.value === "config");
  const initialLoadingPhase = computed(() => {
    if (!bootstrapReady.value) return "bootstrapping";
    if (!configLoaded.value || !fullHealthLoaded.value) return "background_loading";
    return "ready";
  });
  const initialLoadingStatusText = computed(() => {
    const loadingErrors = [];
    if (!fullHealthLoaded.value && String(healthLoadError.value || "").trim()) {
      loadingErrors.push(`运行状态加载失败：${String(healthLoadError.value || "").trim()}`);
    }
    if (!configLoaded.value && String(configLoadError.value || "").trim()) {
      loadingErrors.push(`配置加载失败：${String(configLoadError.value || "").trim()}`);
    }
    if (!bootstrapReady.value) return "页面正在启动...";
    if (!configLoaded.value && !fullHealthLoaded.value) {
      return loadingErrors.length
        ? `页面已打开，但初始化加载失败：${loadingErrors.join("；")}`
        : "页面已打开，正在加载运行状态和配置...";
    }
    if (!fullHealthLoaded.value) {
      return loadingErrors.length ? `页面已打开，但${loadingErrors[0]}` : "页面已打开，正在加载运行状态...";
    }
    if (!configLoaded.value) {
      return loadingErrors.length ? `页面已打开，但${loadingErrors[0]}` : "页面已打开，正在加载配置...";
    }
    return "";
  });
  const selectedDateCount = computed(() => selectedDates.value.length);
  const dayMetricSelectedDateCount = computed(() => dayMetricSelectedDates.value.length);
  const pendingResumeCount = computed(() => pendingResumeRuns.value.length);
  const dayMetricCurrentPayload = computed(() => {
    const payload = currentJob.value?.payload;
    const mode = String(payload?.mode || "").trim().toLowerCase();
    if (mode === "from_download" || mode === "from_file") {
      return payload;
    }
    return null;
  });
  const dayMetricCurrentResultRows = computed(() => {
    const payload = dayMetricCurrentPayload.value;
    const payloadMode = String(payload?.mode || "from_download").trim().toLowerCase() || "from_download";
    const rows = Array.isArray(payload?.results) ? payload.results : [];
    const output = [];
    for (const dateRow of rows) {
      const dutyDate = String(dateRow?.duty_date || "").trim();
      const buildings = Array.isArray(dateRow?.buildings) ? dateRow.buildings : [];
      for (const row of buildings) {
        const rawStatus = String(row?.status || "").trim().toLowerCase();
        const rawStage = String(row?.stage || "").trim().toLowerCase();
        const sourceFile = String(row?.source_file || "").trim();
        const retryable = rawStatus === "failed" && Boolean(row?.retryable);
        let retryHint = "";
        if (rawStatus !== "failed") {
          retryHint = "仅失败单元可重试";
        } else if (!retryable) {
          retryHint = String(row?.error || "").trim()
            || (payloadMode === "from_file"
              ? "本地补录原始文件已失效，请重新选择文件后再执行。"
              : "当前失败单元暂不支持重试");
        }
        output.push({
          mode: String(row?.mode || payloadMode).trim().toLowerCase() || payloadMode,
          duty_date: dutyDate,
          building: String(row?.building || "").trim() || "-",
          status_key: rawStatus,
          stage_key: rawStage,
          status: normalizeDayMetricUnitStatusText(rawStatus),
          stage: normalizeDayMetricUnitStageText(rawStage),
          network_mode: normalizeDayMetricNetworkModeText(row?.network_mode),
          deleted_records: Number(row?.deleted_records || 0),
          created_records: Number(row?.created_records || 0),
          source_file: sourceFile,
          error: String(row?.error || "").trim(),
          attempts: Number(row?.attempts || 0),
          retryable,
          retry_source: String(row?.retry_source || "").trim(),
          failed_at: String(row?.failed_at || "").trim(),
          retry_hint: retryHint,
          tone: normalizeDayMetricUnitTone(row?.status),
        });
      }
    }
    return output;
  });
  const dayMetricRetryableRows = computed(() =>
    dayMetricCurrentResultRows.value.filter((row) => row.status_key === "failed" && row.retryable),
  );
  const dayMetricRetryableFailedCount = computed(() => dayMetricRetryableRows.value.length);
  const activeJobStatuses = new Set(["queued", "planning", "waiting_resource", "dispatching", "running"]);
  const handoverGenerationFeatures = new Set(["handover_from_download", "handover_from_file", "handover_from_files"]);
  const handoverGenerationBusy = computed(() => {
    const candidates = [];
    if (currentJob.value && typeof currentJob.value === "object") {
      candidates.push(currentJob.value);
    }
    if (Array.isArray(jobsList.value)) {
      candidates.push(...jobsList.value);
    }
    const seen = new Set();
    for (const item of candidates) {
      if (!item || typeof item !== "object") continue;
      const jobId = String(item?.job_id || "").trim();
      const feature = String(item?.feature || "").trim().toLowerCase();
      const status = String(item?.status || "").trim().toLowerCase();
      const dedupeKey = jobId || `${feature}:${status}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      if (handoverGenerationFeatures.has(feature) && activeJobStatuses.has(status)) {
        return true;
      }
    }
    return false;
  });
  const runningJobs = computed(() =>
    jobsList.value.filter((item) => String(item?.status || "").trim().toLowerCase() === "running"),
  );
  const waitingResourceJobs = computed(() =>
    jobsList.value.filter((item) => {
      const status = String(item?.status || "").trim().toLowerCase();
      return status === "queued" || status === "waiting_resource";
    }),
  );
  const recentFinishedJobs = computed(() =>
    jobsList.value.filter((item) => {
      const status = String(item?.status || "").trim().toLowerCase();
      return (
        status === "success"
        || status === "failed"
        || status === "cancelled"
        || status === "interrupted"
        || status === "partial_failed"
        || status === "blocked_precondition"
      );
    }).slice(0, 6),
  );
  const bridgeTasksEnabled = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    return Boolean(health.shared_bridge?.enabled) && (roleMode === "internal" || roleMode === "external");
  });
  const isInternalRole = computed(() => resolveDeploymentRoleMode(health.deployment?.role_mode || "") === "internal");
  const BRIDGE_HISTORY_DISPLAY_LIMIT = 30;
  const activeBridgeTasks = computed(() =>
    bridgeTasks.value.filter((item) => !isBridgeTerminalStatus(item?.status)),
  );
  const totalBridgeHistoryCount = computed(() =>
    bridgeTasks.value.filter((item) => isBridgeTerminalStatus(item?.status)).length,
  );
  const displayedBridgeTasks = computed(() => activeBridgeTasks.value);
  const hiddenBridgeHistoryCount = computed(() =>
    Math.max(0, totalBridgeHistoryCount.value - BRIDGE_HISTORY_DISPLAY_LIMIT),
  );
  const recentFinishedBridgeTasks = computed(() =>
    bridgeTasks.value.filter((item) => isBridgeTerminalStatus(item?.status)).slice(0, 8),
  );
  function formatSharedBridgeDbStatus(dbStatus) {
    const normalized = String(dbStatus || "").trim().toLowerCase();
    if (normalized === "ok") return "正常";
    if (normalized === "disabled") return "未启用";
    if (normalized === "misconfigured") return "共享目录未配置";
    if (normalized === "busy") return "数据库正忙";
    if (normalized === "unavailable") return "数据库暂不可用";
    if (normalized === "error") return "异常";
    return String(dbStatus || "").trim() || "未启用";
  }
  function formatSharedBridgeAgentStatus(agentStatus, heartbeatOk) {
    const normalized = String(agentStatus || "").trim().toLowerCase();
    if (normalized === "running") {
      return heartbeatOk ? "运行中" : "运行中/心跳异常";
    }
    if (normalized === "disabled") return "未启用";
    if (normalized === "stopped") return "已停止";
    return String(agentStatus || "").trim() || "-";
  }
  function formatUpdaterSourceLabel(updater) {
    const kind = String(updater?.source_kind || "").trim().toLowerCase();
    if (kind === "shared_mirror") return "共享目录更新源（不访问互联网）";
    const label = String(updater?.source_label || "").trim();
    return label || "远端正式更新源";
  }
  function formatUpdaterMirrorStatus(updater) {
    const sourceKind = String(updater?.source_kind || "").trim().toLowerCase();
    const mirrorReady = Boolean(updater?.mirror_ready);
    const mirrorVersion = String(updater?.mirror_version || "").trim();
    const lastPublishAt = String(updater?.last_publish_at || "").trim();
    const lastPublishError = String(updater?.last_publish_error || "").trim();
    if (sourceKind === "shared_mirror") {
      if (mirrorReady) {
        return mirrorVersion ? `已发布 ${mirrorVersion}` : "已发布批准版本";
      }
      if (lastPublishError) return "共享目录镜像异常";
      return "等待外网端发布批准版本";
    }
    if (mirrorReady) {
      if (mirrorVersion && lastPublishAt) return `已发布 ${mirrorVersion} / ${lastPublishAt}`;
      if (mirrorVersion) return `已发布 ${mirrorVersion}`;
      return "已发布批准版本";
    }
    if (lastPublishError) return "共享目录发布失败";
    return "尚未发布到共享目录";
  }
function normalizeInternalDownloadPoolSlot(slot) {
  const building = String(slot?.building || "").trim() || "-";
  const pageReady = Boolean(slot?.page_ready);
  const inUse = Boolean(slot?.in_use);
  const suspended = Boolean(slot?.suspended);
  const suspendReason = formatInternalDownloadPoolError(slot?.suspend_reason || slot?.pending_issue_summary);
  const failureKind = String(slot?.failure_kind || "").trim().toLowerCase();
  const recoveryAttempts = Number.parseInt(String(slot?.recovery_attempts || 0), 10) || 0;
  const nextProbeAt = String(slot?.next_probe_at || "").trim();
  const lastUsedAt = String(slot?.last_used_at || "").trim();
  const lastLoginAt = String(slot?.last_login_at || "").trim();
  const lastResult = String(slot?.last_result || "").trim().toLowerCase();
  const lastError = formatInternalDownloadPoolError(slot?.last_error);
  const loginError = formatInternalDownloadPoolError(slot?.login_error);
  const loginState = String(slot?.login_state || "").trim().toLowerCase();
  let tone = "neutral";
  let stateText = "未建页";
  let loginTone = "warning";
  let loginText = "待登录";
  let detailText = pageReady ? "页签已就绪，等待下载任务" : "页签尚未初始化";
  if (pageReady) {
    tone = "success";
    stateText = "待命";
  }
  if (suspended) {
    tone = "danger";
    stateText = "已暂停等待恢复";
  }
  if (!suspended) {
    if (inUse) {
      tone = "warning";
      stateText = "使用中";
    } else if (lastResult === "failed" || lastResult === "error") {
      tone = "danger";
      stateText = "最近失败";
    } else if (lastResult === "success") {
      tone = "success";
      stateText = "最近成功";
    } else if (lastResult === "running") {
      tone = "warning";
      stateText = "使用中";
    } else if (lastResult === "ready" && pageReady) {
      tone = "success";
      stateText = "待命";
    }
  }
    if (suspended) {
      loginTone = "danger";
      loginText = failureKind === "login_failed" || failureKind === "login_expired" ? "登录失败" : "页面异常";
    } else if (loginState === "ready") {
      loginTone = "success";
      loginText = "已登录";
    } else if (loginState === "logging_in") {
      loginTone = "info";
      loginText = "登录中";
    } else if (loginState === "expired") {
      loginTone = "warning";
      loginText = "登录已失效";
    } else if (loginState === "failed") {
      loginTone = "danger";
      loginText = "登录失败";
  } else if (!pageReady) {
    loginTone = "neutral";
    loginText = "待初始化";
  }
  if (suspended) {
    detailText = suspendReason || "该楼已暂停等待恢复";
    if (nextProbeAt) {
      detailText += ` / 下次自动检测：${nextProbeAt}`;
    }
  } else if (loginState === "failed") {
    detailText = loginError || lastError || "登录失败，请检查楼栋地址、网络和登录页状态";
  } else if (loginState === "expired") {
    detailText = "登录态已失效，任务开始前会自动重登";
  } else if (loginState === "logging_in") {
    detailText = "正在检查登录态并准备进入目标页面";
  } else if (lastError) {
    detailText = lastError;
  } else if (lastUsedAt) {
    detailText = `最近使用：${lastUsedAt}`;
  }
  return {
    building,
    pageReady,
      inUse,
      lastUsedAt,
      lastLoginAt,
      lastResult,
      lastError,
      suspended,
      suspendReason,
      failureKind,
      recoveryAttempts,
      nextProbeAt,
      loginState,
      loginTone,
      loginText,
      loginError,
      tone,
      stateText,
      detailText,
    };
  }
  function normalizeSourceCacheBuildingStatus(raw, fallbackBucket) {
    const building = String(raw?.building || "").trim() || "-";
    const bucketKey = String(raw?.bucket_key || "").trim() || String(fallbackBucket || "").trim() || "-";
    const downloadedAt = String(raw?.downloaded_at || "").trim();
    const lastError = formatSharedBridgeRuntimeError(raw?.last_error);
    const blocked = Boolean(raw?.blocked);
    const blockedReason = formatInternalDownloadPoolError(raw?.blocked_reason || raw?.last_error);
    const nextProbeAt = String(raw?.next_probe_at || "").trim();
    const relativePath = String(raw?.relative_path || "").trim();
    const resolvedFilePath = String(raw?.resolved_file_path || "").trim();
    const rawStatus = String(raw?.status || "").trim().toLowerCase();
    const statusKey = ["ready", "failed", "downloading", "consumed"].includes(rawStatus) ? rawStatus : "waiting";
    const ready = statusKey === "ready" && Boolean(raw?.ready);
    let tone = "warning";
    let stateText = "等待中";
    if (statusKey === "ready" && ready) {
      tone = "success";
      stateText = "已就绪";
    } else if (statusKey === "consumed") {
      tone = "info";
      stateText = "已消费";
    } else if (statusKey === "downloading") {
      tone = "info";
      stateText = "下载中";
    } else if (statusKey === "failed") {
      tone = "danger";
      stateText = "失败";
    } else if (blocked) {
      tone = "warning";
      stateText = "等待内网恢复";
    }
    const detailText = blocked
      ? `${blockedReason || "楼栋页面异常，等待内网恢复"}${nextProbeAt ? ` / 下次自动检测：${nextProbeAt}` : ""}`
      : statusKey === "consumed"
        ? (downloadedAt ? `外网已消费并删除 / ${downloadedAt}` : "外网已消费并删除")
      : (lastError || downloadedAt || (resolvedFilePath ? resolvedFilePath : "等待共享文件就绪"));
    return {
      building,
      bucketKey,
      statusKey,
      ready,
      downloadedAt,
      lastError,
      blocked,
      blockedReason,
      nextProbeAt,
      relativePath,
      resolvedFilePath,
      tone,
      stateText,
      detailText,
    };
  }
  function normalizeSourceCacheFamilyOverview({ key, title, payload, fallbackBucket }) {
    const familyPayload = payload && typeof payload === "object" ? payload : {};
    const readyCount = Number.parseInt(String(familyPayload.ready_count || 0), 10) || 0;
    const failedBuildings = Array.isArray(familyPayload.failed_buildings)
      ? familyPayload.failed_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const blockedBuildings = Array.isArray(familyPayload.blocked_buildings)
      ? familyPayload.blocked_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const normalizedBuildings = Array.isArray(familyPayload.buildings)
      ? familyPayload.buildings.map((item) => normalizeSourceCacheBuildingStatus(item, fallbackBucket))
      : [];
    const buildingMap = new Map(
      normalizedBuildings.map((item) => [String(item?.building || "").trim(), item]),
    );
    const buildings = INTERNAL_BUILDINGS.map(
      (building) => buildingMap.get(building) || normalizeSourceCacheBuildingStatus({ building }, fallbackBucket),
    );
    const currentBucket = String(familyPayload.current_bucket || "").trim() || String(fallbackBucket || "").trim();
    const lastSuccessAt = String(familyPayload.last_success_at || "").trim();
    const rawManualRefresh = familyPayload.manual_refresh && typeof familyPayload.manual_refresh === "object"
      ? familyPayload.manual_refresh
      : {};
    const manualRefresh = {
      running: Boolean(rawManualRefresh.running),
      lastRunAt: String(rawManualRefresh.last_run_at || "").trim(),
      lastSuccessAt: String(rawManualRefresh.last_success_at || "").trim(),
      lastError: formatSharedBridgeRuntimeError(rawManualRefresh.last_error),
      bucketKey: String(rawManualRefresh.bucket_key || "").trim(),
      successfulBuildings: Array.isArray(rawManualRefresh.successful_buildings)
        ? rawManualRefresh.successful_buildings.map((item) => String(item || "").trim()).filter(Boolean)
        : [],
      failedBuildings: Array.isArray(rawManualRefresh.failed_buildings)
        ? rawManualRefresh.failed_buildings.map((item) => String(item || "").trim()).filter(Boolean)
        : [],
      blockedBuildings: Array.isArray(rawManualRefresh.blocked_buildings)
        ? rawManualRefresh.blocked_buildings.map((item) => String(item || "").trim()).filter(Boolean)
        : [],
      totalRowCount: Number.parseInt(String(rawManualRefresh.total_row_count || 0), 10) || 0,
      buildingRowCounts: rawManualRefresh.building_row_counts && typeof rawManualRefresh.building_row_counts === "object"
        ? Object.fromEntries(
          Object.entries(rawManualRefresh.building_row_counts)
            .map(([name, value]) => [String(name || "").trim(), Number.parseInt(String(value || 0), 10) || 0]),
        )
        : {},
      queryStart: String(rawManualRefresh.query_start || "").trim(),
      queryEnd: String(rawManualRefresh.query_end || "").trim(),
    };
    const hasFailures = buildings.length
      ? buildings.some((item) => item.statusKey === "failed")
      : failedBuildings.length > 0;
    const allReady = buildings.length
      ? buildings.every((item) => item.statusKey === "ready")
      : readyCount > 0 && !hasFailures;
    const hasBlocked = buildings.length
      ? buildings.some((item) => item.blocked)
      : blockedBuildings.length > 0;
    const bucketScopeText = key === "alarm_event_family" ? "本次定时" : "本小时";
    const tone = hasFailures ? "danger" : allReady ? "success" : "warning";
    const statusText = hasFailures
      ? `${bucketScopeText}存在失败楼栋`
      : hasBlocked
        ? `${bucketScopeText}存在等待恢复楼栋`
      : allReady
        ? `${bucketScopeText}全部就绪`
        : `${bucketScopeText}仍有楼栋等待中`;
    return {
      key,
      title,
      readyCount,
      failedBuildings,
      blockedBuildings,
      lastSuccessAt,
      currentBucket,
      buildings,
      hasFailures,
      hasBlocked,
      allReady,
      tone,
      statusText,
      manualRefresh,
    };
  }
  function normalizeLatestSelectionBuildingStatus(raw, fallbackBucket) {
    const building = String(raw?.building || "").trim() || "-";
    const bucketKey = String(raw?.bucket_key || "").trim() || String(fallbackBucket || "").trim() || "-";
    const downloadedAt = String(raw?.downloaded_at || "").trim();
    const lastError = formatSharedBridgeRuntimeError(raw?.last_error);
    const relativePath = String(raw?.relative_path || "").trim();
    const resolvedFilePath = String(raw?.resolved_file_path || "").trim();
    const usingFallback = Boolean(raw?.using_fallback);
    const versionGap = Number.isInteger(raw?.version_gap)
      ? raw.version_gap
      : Number.parseInt(String(raw?.version_gap ?? ""), 10);
    const rawStatus = String(raw?.status || "").trim().toLowerCase();
    const statusKey = ["ready", "stale"].includes(rawStatus) ? rawStatus : "waiting";
    let tone = "warning";
    let stateText = "等待共享文件就绪";
    if (statusKey === "ready" && usingFallback) {
      tone = "warning";
      stateText = "使用上一版共享文件";
    } else if (statusKey === "ready") {
      tone = "success";
      stateText = "已就绪";
    } else if (statusKey === "stale") {
      tone = "danger";
      stateText = "版本过旧，等待更新";
    }
    return {
      building,
      bucketKey,
      statusKey,
      usingFallback,
      versionGap: Number.isFinite(versionGap) ? Math.max(0, versionGap) : null,
      downloadedAt,
      lastError,
      relativePath,
      resolvedFilePath,
      tone,
      stateText,
      detailText: lastError || downloadedAt || (resolvedFilePath ? resolvedFilePath : "等待共享文件就绪"),
    };
  }
  function formatLatestSelectionAgeText(value) {
    const ageHours = Number.parseFloat(String(value ?? ""));
    if (!Number.isFinite(ageHours) || ageHours < 0) return "";
    const rounded = Math.round(ageHours * 10) / 10;
    if (Number.isInteger(rounded)) return `${rounded} 小时`;
    return `${rounded.toFixed(1)} 小时`;
  }
  function normalizeLatestSelectionOverview({ key, title, payload }) {
    const selectionPayload = payload && typeof payload === "object" ? payload : {};
    const bestBucketKey = String(selectionPayload.best_bucket_key || "").trim();
    const bestBucketAgeHours = Number.parseFloat(String(selectionPayload.best_bucket_age_hours ?? ""));
    const isBestBucketTooOld = Boolean(selectionPayload.is_best_bucket_too_old);
    const bestBucketAgeText = formatLatestSelectionAgeText(bestBucketAgeHours);
    const fallbackBuildings = Array.isArray(selectionPayload.fallback_buildings)
      ? selectionPayload.fallback_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const missingBuildings = Array.isArray(selectionPayload.missing_buildings)
      ? selectionPayload.missing_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const staleBuildings = Array.isArray(selectionPayload.stale_buildings)
      ? selectionPayload.stale_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const buildings = Array.isArray(selectionPayload.buildings)
      ? selectionPayload.buildings.map((item) => normalizeLatestSelectionBuildingStatus(item, bestBucketKey))
      : [];
    const canProceed = Boolean(selectionPayload.can_proceed)
      && !missingBuildings.length
      && !staleBuildings.length
      && !isBestBucketTooOld;
    let tone = "warning";
    let statusText = "等待共享文件就绪";
    let summaryText = "共享文件尚未齐套。";
    if (isBestBucketTooOld) {
      tone = "danger";
      statusText = "最新时间桶已过旧";
      summaryText = bestBucketKey
        ? `当前最新时间桶 ${bestBucketKey}${bestBucketAgeText ? ` 距现在约 ${bestBucketAgeText}` : ""}，已超过 3 小时，等待内网更新后会自动重试。`
        : "当前最新共享文件已超过 3 小时，等待内网更新后会自动重试。";
    } else if (staleBuildings.length) {
      tone = "danger";
      statusText = "存在过旧楼栋";
      summaryText = `以下楼栋相对最新时间桶落后超过 3 桶：${staleBuildings.join(" / ")}`;
    } else if (missingBuildings.length) {
      tone = "warning";
      statusText = "存在缺失楼栋";
      summaryText = `以下楼栋尚未登记共享文件：${missingBuildings.join(" / ")}`;
    } else if (fallbackBuildings.length) {
      tone = "warning";
      statusText = "已允许回退";
      summaryText = `以下楼栋正在使用上一版共享文件：${fallbackBuildings.join(" / ")}`;
    } else if (canProceed && buildings.length) {
      tone = "success";
      statusText = "最新桶已齐套";
      summaryText = "当前 5 楼都已命中最新共享文件，可直接继续外网处理。";
    }
    return {
      key,
      title,
      bestBucketKey,
      bestBucketAgeHours: Number.isFinite(bestBucketAgeHours) ? Math.max(0, bestBucketAgeHours) : null,
      bestBucketAgeText,
      isBestBucketTooOld,
      fallbackBuildings,
      missingBuildings,
      staleBuildings,
      buildings,
      canProceed,
      tone,
      statusText,
      summaryText,
    };
  }
  function getLatestSharedSourceCacheTaskEvent(task) {
    const events = Array.isArray(task?.events) ? task.events : [];
    return events.length ? events[0] : null;
  }
  function isSharedSourceCacheBackfillWaitingSync(task) {
    if (!task || typeof task !== "object") return false;
    const normalizedStatus = String(task?.status || "").trim().toLowerCase();
    if (!["ready_for_external", "waiting_next_side"].includes(normalizedStatus)) {
      return false;
    }
    const latestEvent = getLatestSharedSourceCacheTaskEvent(task);
    const latestEventType = String(latestEvent?.event_type || "").trim().toLowerCase();
    if (latestEventType === "waiting_source_sync") return true;
    const latestEventText = String(latestEvent?.event_text || latestEvent?.payload?.message || "").trim();
    return latestEventText.includes("等待内网补采同步");
  }
  function formatBridgeProgressStatusText(statusOrTask, taskLike = null) {
    const task = statusOrTask && typeof statusOrTask === "object"
      ? statusOrTask
      : (taskLike && typeof taskLike === "object" ? taskLike : null);
    const normalized = String(task ? task.status : statusOrTask || "").trim().toLowerCase();
    if (!normalized) return "执行中";
    if (["internal_running", "external_running", "running", "claimed", "internal_claimed", "external_claimed"].includes(normalized)) {
      return "执行中";
    }
    if (["queued_for_internal", "pending"].includes(normalized)) {
      return "等待执行";
    }
    if (["ready_for_external", "waiting_next_side"].includes(normalized)) {
      return isSharedSourceCacheBackfillWaitingSync(task) ? "等待内网补采同步" : "等待接续";
    }
    if (normalized === "success") return "已完成";
    if (normalized === "failed") return "失败";
    if (normalized === "partial_failed") return "部分失败";
    return String(task ? task.status : statusOrTask || "").trim() || "执行中";
  }
  function formatSharedSourceCacheBackfillStageText(task) {
    const stageName = String(task?.current_stage_name || "").trim();
    const featureLabel = String(task?.feature_label || "").trim() || "共享桥接任务";
    const statusText = formatBridgeProgressStatusText(task?.current_stage_status || task?.status || "", task);
    return stageName ? `${stageName} / ${statusText}` : `${featureLabel} / ${statusText}`;
  }
  function normalizeSharedSourceCacheTaskBuildings(requestPayload) {
    const request = requestPayload && typeof requestPayload === "object" ? requestPayload : {};
    const singleBuilding = String(request.building || "").trim();
    if (singleBuilding) return [singleBuilding];
    const multipleBuildings = Array.isArray(request.buildings)
      ? request.buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    return multipleBuildings;
  }
  function formatSharedSourceCacheBackfillScopeText(task) {
    const feature = String(task?.feature || "").trim().toLowerCase();
    const request = task?.request && typeof task.request === "object" ? task.request : {};
    if (feature === "handover_cache_fill") {
      const dutyDate = String(request.duty_date || "").trim();
      const dutyShiftText = shiftTextFromCode(request.duty_shift || "");
      if (dutyDate && dutyShiftText !== "-") {
        return `${dutyDate} / ${dutyShiftText}`;
      }
      const selectedDates = Array.isArray(request.selected_dates)
        ? request.selected_dates.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      if (selectedDates.length) {
        return `日期 ${selectedDates.join(" / ")}`;
      }
      return "";
    }
    if (feature === "monthly_cache_fill") {
      const selectedDates = Array.isArray(request.selected_dates)
        ? request.selected_dates.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      return selectedDates.length ? selectedDates.join(" / ") : "";
    }
    return "";
  }
  function buildSharedSourceCacheBackfillOverlays(tasks) {
    const normalizedTasks = Array.isArray(tasks) ? tasks : [];
    return normalizedTasks.flatMap((task) => {
      const feature = String(task?.feature || "").trim().toLowerCase();
      const request = task?.request && typeof task.request === "object" ? task.request : {};
      const requestedBuildings = normalizeSharedSourceCacheTaskBuildings(request);
      const baseOverlay = {
        taskId: String(task?.task_id || "").trim(),
        requestedBuildings,
        stageText: formatSharedSourceCacheBackfillStageText(task),
        scopeText: formatSharedSourceCacheBackfillScopeText(task),
      };
      if (feature === "handover_cache_fill" && String(request.continuation_kind || "").trim().toLowerCase() === "handover") {
        return [
          {
            ...baseOverlay,
            familyKey: "handover_log_family",
          },
          {
            ...baseOverlay,
            familyKey: "handover_capacity_report_family",
          },
        ];
      }
      if (feature === "monthly_cache_fill") {
        return [
          {
            ...baseOverlay,
            familyKey: "monthly_report_family",
          },
        ];
      }
      return [];
    });
  }
  function applySharedSourceCacheBackfillOverlay(family, overlays) {
    const familyOverlays = (Array.isArray(overlays) ? overlays : []).filter((item) => item.familyKey === family.key);
    if (!familyOverlays.length) {
      return {
        ...family,
        backfillRunning: false,
        backfillText: "",
        backfillScopeText: "",
        backfillTaskId: "",
      };
    }
    const isMonthlyDateFileFamily = String(family?.key || "").trim().toLowerCase() === "monthly_report_family";
    const familyBuildings = Array.isArray(family.buildings) ? family.buildings : [];
    let runningBuildingCount = 0;
    const buildings = familyBuildings.map((building) => {
      const buildingName = String(building?.building || "").trim();
      if (!buildingName || String(building?.statusKey || "").trim().toLowerCase() === "ready") {
        return {
          ...building,
          backfillRunning: false,
          backfillText: "",
          backfillScopeText: "",
          backfillTaskId: "",
        };
      }
      const overlay = familyOverlays.find((item) => (
        !item.requestedBuildings.length || item.requestedBuildings.includes(buildingName)
      ));
      if (!overlay) {
        return {
          ...building,
          backfillRunning: false,
          backfillText: "",
          backfillScopeText: "",
          backfillTaskId: "",
        };
      }
      runningBuildingCount += 1;
      return {
        ...building,
        tone: "warning",
        stateText: "补采中",
        backfillRunning: true,
        backfillText: overlay.stageText,
        backfillScopeText: overlay.scopeText,
        backfillTaskId: overlay.taskId,
      };
    });
    if (!familyBuildings.length && familyOverlays.length) {
      return {
        ...family,
        backfillRunning: true,
        backfillText: familyOverlays[0].stageText,
        backfillScopeText: familyOverlays[0].scopeText,
        backfillTaskId: familyOverlays[0].taskId,
        tone: "warning",
        statusText: "补采中",
      };
    }
    if (!runningBuildingCount) {
      return {
        ...family,
        buildings,
        backfillRunning: false,
        backfillText: "",
        backfillScopeText: "",
        backfillTaskId: "",
      };
    }
    return {
      ...family,
      buildings,
      backfillRunning: true,
      backfillText: familyOverlays[0].stageText,
      backfillScopeText: familyOverlays[0].scopeText,
      backfillTaskId: familyOverlays[0].taskId,
      tone: "warning",
      statusText: isMonthlyDateFileFamily ? "同步中" : "补采中",
      summaryText: isMonthlyDateFileFamily
        ? "月报日期文件同步中；如外网任务显示“等待内网补采同步”，文件到位后会自动继续并回切为已就绪。"
        : "历史共享文件补采中；如外网任务显示“等待内网补采同步”，文件到位后会自动继续并回切为已就绪。",
    };
  }
  function normalizeMonthlyDateFileFamilyOverview(family) {
    const payload = family && typeof family === "object" ? family : {};
    const buildings = Array.isArray(payload.buildings) ? payload.buildings : [];
    let summaryText = String(payload.summaryText || "").trim();
    if (payload.isBestBucketTooOld) {
      summaryText = payload.bestBucketKey
        ? `当前月报日期文件 ${payload.bestBucketKey} 距现在较久，等待内网同步到目标日期后会自动重试。`
        : "当前月报日期文件尚未同步到目标日期，等待内网更新后会自动重试。";
    } else if (payload.staleBuildings && payload.staleBuildings.length) {
      summaryText = `以下楼栋月报日期文件相对目标日期落后：${payload.staleBuildings.join(" / ")}`;
    } else if (payload.missingBuildings && payload.missingBuildings.length) {
      summaryText = `以下楼栋尚未登记对应日期的月报文件：${payload.missingBuildings.join(" / ")}`;
    } else if (payload.fallbackBuildings && payload.fallbackBuildings.length) {
      summaryText = `以下楼栋正在使用上一版日期文件：${payload.fallbackBuildings.join(" / ")}`;
    } else if (payload.canProceed && buildings.length) {
      summaryText = "当前月报日期文件已齐套，外网按业务日期处理时会直接读取对应日期文件。";
    } else if (!summaryText) {
      summaryText = "月报源文件统一按业务日期展示；外网处理哪一天，就读取哪一天的日期文件。";
    }
    return {
      ...payload,
      dateSemantic: true,
      referenceLabel: "当前日期文件",
      ageLabel: "距当前约",
      backfillLabel: "当前同步",
      backfillScopeLabel: "同步日期",
      buildingReferenceLabel: "日期文件",
      summaryText,
      statusText: payload.canProceed && buildings.length
        ? "日期文件已齐套"
        : String(payload.statusText || "").trim() || "等待日期文件就绪",
    };
  }
  function normalizeAlarmEventReadinessBuilding(raw, fallbackBucket) {
    const building = String(raw?.building || "").trim() || "-";
    const bucketKey = String(raw?.bucket_key || "").trim() || String(fallbackBucket || "").trim() || "-";
    const downloadedAt = String(raw?.downloaded_at || "").trim();
    const selectedDownloadedAt = String(raw?.selected_downloaded_at || downloadedAt || "").trim();
    const lastError = formatSharedBridgeRuntimeError(raw?.last_error);
    const relativePath = String(raw?.relative_path || "").trim();
    const resolvedFilePath = String(raw?.resolved_file_path || "").trim();
    const blocked = Boolean(raw?.blocked);
    const blockedReason = formatInternalDownloadPoolError(raw?.blocked_reason || raw?.last_error);
    const sourceKind = String(raw?.source_kind || "").trim().toLowerCase();
    const selectionScope = String(raw?.selection_scope || "").trim().toLowerCase();
    const rawStatus = String(raw?.status || "").trim().toLowerCase();
    const statusKey = ["ready", "failed"].includes(rawStatus) ? rawStatus : "waiting";
    let tone = "warning";
    let stateText = "今天和昨天都缺文件";
    if (statusKey === "ready") {
      tone = "success";
      stateText = "已就绪";
    } else if (statusKey === "failed") {
      tone = "danger";
      stateText = "失败";
    } else if (blocked) {
      tone = "warning";
      stateText = "等待内网恢复";
    }
    const sourceKindText = sourceKind === "manual" ? "手动" : sourceKind === "latest" ? "定时" : "";
    const selectionScopeText = selectionScope === "today"
      ? "今天最新"
      : selectionScope === "yesterday_fallback"
        ? "昨天回退"
        : selectionScope === "missing"
          ? "今天和昨天都缺文件"
          : "";
    return {
      building,
      bucketKey,
      statusKey,
      usingFallback: false,
      versionGap: null,
      downloadedAt,
      lastError,
      relativePath,
      resolvedFilePath,
      tone,
      stateText,
      sourceKind,
      sourceKindText,
      selectionScope,
      selectionScopeText,
      selectedDownloadedAt,
      detailText: blocked
        ? (blockedReason || "等待内网恢复")
        : (lastError || selectedDownloadedAt || (resolvedFilePath ? resolvedFilePath : "今天和昨天都没有可用告警文件")),
    };
  }
  function normalizeAlarmEventReadinessOverview({ key, title, payload }) {
    const familyPayload = payload && typeof payload === "object" ? payload : {};
    const currentBucket = String(familyPayload.current_bucket || "").trim();
    const selectionPolicy = String(familyPayload.selection_policy || "").trim();
    const selectionReferenceDate = String(familyPayload.selection_reference_date || "").trim();
    const usedPreviousDayFallback = Array.isArray(familyPayload.used_previous_day_fallback)
      ? familyPayload.used_previous_day_fallback.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const missingTodayBuildings = Array.isArray(familyPayload.missing_today_buildings)
      ? familyPayload.missing_today_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const missingBothDaysBuildings = Array.isArray(familyPayload.missing_both_days_buildings)
      ? familyPayload.missing_both_days_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const uploadState = familyPayload.external_upload && typeof familyPayload.external_upload === "object"
      ? familyPayload.external_upload
      : {};
    const buildings = Array.isArray(familyPayload.buildings)
      ? familyPayload.buildings.map((item) => normalizeAlarmEventReadinessBuilding(item, currentBucket))
      : [];
    const buildingMap = new Map(buildings.map((item) => [String(item.building || "").trim(), item]));
    const normalizedBuildings = INTERNAL_BUILDINGS.map(
      (building) => buildingMap.get(building) || normalizeAlarmEventReadinessBuilding({ building }, currentBucket),
    );
    const readyCount = normalizedBuildings.filter((item) => item.statusKey === "ready").length;
    const todaySelectedCount = normalizedBuildings.filter((item) =>
      item.selectionScope === "today" && item.statusKey === "ready",
    ).length;
    const failedBuildings = normalizedBuildings.filter((item) => item.statusKey === "failed").map((item) => item.building);
    const blockedBuildings = normalizedBuildings.filter((item) => item.stateText === "等待内网恢复").map((item) => item.building);
    const uploadLastRunAt = String(uploadState.last_run_at || "").trim();
    const uploadLastSuccessAt = String(uploadState.last_success_at || "").trim();
    const uploadLastError = formatSharedBridgeRuntimeError(uploadState.last_error);
    const uploadRecordCount = Number.parseInt(String(uploadState.uploaded_record_count || 0), 10) || 0;
    const uploadFileCount = Number.parseInt(String(uploadState.uploaded_file_count || 0), 10) || 0;
    const uploadRunning = Boolean(uploadState.running);
    const uploadStartedAt = String(uploadState.started_at || "").trim();
    const uploadCurrentMode = String(uploadState.current_mode || "").trim();
    const uploadCurrentScope = String(uploadState.current_scope || "").trim();
    const uploadRunningText = uploadRunning
      ? `正在上传${uploadCurrentMode === "single_building" ? `（${uploadCurrentScope || "单楼"}）` : "（全量）"}${uploadStartedAt ? `，开始于 ${uploadStartedAt}` : ""}`
      : "";
    let tone = "warning";
    let statusText = "等待当天最新文件";
    let summaryText = "当前策略：当天最新一份，缺失则回退昨天最新。";
    if (failedBuildings.length) {
      tone = "danger";
      statusText = "存在失败楼栋";
      summaryText = `以下楼栋告警信息文件处理失败：${failedBuildings.join(" / ")}`;
    } else if (blockedBuildings.length) {
      tone = "warning";
      statusText = "等待内网恢复";
      summaryText = `以下楼栋正在等待内网恢复：${blockedBuildings.join(" / ")}`;
    } else if (missingBothDaysBuildings.length) {
      tone = readyCount > 0 ? "warning" : "danger";
      statusText = "存在缺失楼栋";
      summaryText = `当前策略：当天最新一份，缺失则回退昨天最新。今天最新 ${todaySelectedCount}/5 楼；昨天回退 ${usedPreviousDayFallback.length}/5 楼；今天和昨天都缺文件 ${missingBothDaysBuildings.length}/5 楼。`;
    } else if (usedPreviousDayFallback.length) {
      tone = "warning";
      statusText = "存在昨天回退";
      summaryText = `当前策略：当天最新一份，缺失则回退昨天最新。今天最新 ${todaySelectedCount}/5 楼；昨天回退 ${usedPreviousDayFallback.length}/5 楼。`;
    } else if (readyCount > 0) {
      tone = "success";
      statusText = "当天最新已就绪";
      summaryText = `当前策略：当天最新一份，缺失则回退昨天最新。今天已有 ${todaySelectedCount || readyCount}/5 个楼栋告警文件可供外网消费。`;
    }
    if (uploadLastRunAt) {
      summaryText += ` 最近上传：${uploadLastRunAt}（记录 ${uploadRecordCount} 条，文件 ${uploadFileCount} 份，源文件保留）。`;
    }
    return {
      key,
      title,
      bestBucketKey: currentBucket,
      bestBucketAgeHours: null,
      bestBucketAgeText: "",
      isBestBucketTooOld: false,
      fallbackBuildings: [],
      missingBuildings: [],
      staleBuildings: [],
      buildings: normalizedBuildings,
      canProceed: readyCount > 0,
      tone,
      statusText,
      summaryText,
      participatesInAutoRetry: false,
      uploadLastRunAt,
      uploadLastSuccessAt,
      uploadLastError,
      uploadRecordCount,
      uploadFileCount,
      uploadRunning,
      uploadStartedAt,
      uploadCurrentMode,
      uploadCurrentScope,
      uploadRunningText,
      selectionPolicy,
      selectionReferenceDate,
      usedPreviousDayFallback,
      missingTodayBuildings,
      missingBothDaysBuildings,
      todaySelectedCount,
    };
  }
  const internalDownloadPoolOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    if (roleMode !== "internal") {
      return {
        tone: "neutral",
        statusText: "仅内网端启用",
        summaryText: "常驻 5 个楼栋页签的内网下载页池只在内网端运行。",
        errorText: "",
        items: [
          { label: "页池状态", value: "当前角色未启用", tone: "neutral" },
          { label: "固定楼栋页签", value: "A楼 / B楼 / C楼 / D楼 / E楼", tone: "neutral" },
        ],
        slots: [],
      };
    }
    const rawPool = health.shared_bridge?.internal_download_pool || {};
    const enabled = Boolean(rawPool.enabled);
    const browserReady = Boolean(rawPool.browser_ready);
    const lastError = formatInternalDownloadPoolError(rawPool.last_error);
    const activeBuildings = Array.isArray(rawPool.active_buildings)
      ? rawPool.active_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const normalizedSlots = Array.isArray(rawPool.page_slots)
      ? rawPool.page_slots.map((slot) => normalizeInternalDownloadPoolSlot(slot))
      : [];
    const slotMap = new Map(
      normalizedSlots.map((slot) => [String(slot?.building || "").trim(), slot]),
    );
    const slots = INTERNAL_BUILDINGS.map((building) => slotMap.get(building) || normalizeInternalDownloadPoolSlot({ building }));
    const readyLoginCount = slots.filter((slot) => slot.loginState === "ready").length;
    let tone = "warning";
    let statusText = "启动中";
    let summaryText = "内网下载页池正在准备浏览器和固定楼栋页签。";
    if (!enabled) {
      tone = "warning";
      statusText = "未启用";
      summaryText = "当前内网端尚未启用常驻下载页池。";
    } else if (browserReady) {
      tone = activeBuildings.length ? "warning" : readyLoginCount === slots.length && slots.length ? "success" : "warning";
      statusText = activeBuildings.length
        ? "运行中 / 有页签占用"
        : readyLoginCount === slots.length && slots.length
          ? "运行中 / 5个楼已登录"
          : "运行中 / 预登录进行中";
      summaryText = activeBuildings.length
        ? `当前占用楼栋：${activeBuildings.join(" / ")}`
        : readyLoginCount === slots.length && slots.length
          ? "5个楼状态实时展示，下载前会先刷新，只有登录失效时才重新登录。"
          : `5个楼状态实时展示中，已登录 ${readyLoginCount}/${slots.length || 5}，页面每5秒自动刷新。`;
    } else if (lastError) {
      tone = "danger";
      statusText = "页池异常";
      summaryText = "内网下载页池启动失败或最近一次重建异常，请检查 Playwright 环境和登录页。";
    }
    return {
      tone,
      statusText,
      summaryText,
      errorText: lastError,
      items: [
        {
          label: "页池状态",
          value: browserReady ? "浏览器已就绪" : enabled ? "浏览器未就绪" : "未启用",
          tone: browserReady ? "success" : enabled ? "warning" : "neutral",
        },
        {
          label: "当前占用",
          value: activeBuildings.length ? activeBuildings.join(" / ") : "无",
          tone: activeBuildings.length ? "warning" : "neutral",
        },
        {
          label: "已登录楼栋",
          value: `${readyLoginCount}/${slots.length || 5}`,
          tone: readyLoginCount === slots.length && slots.length ? "success" : readyLoginCount > 0 ? "warning" : "neutral",
        },
      ],
      slots,
    };
  });
  const internalSourceCacheOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    if (roleMode !== "internal") {
      return {
        tone: "neutral",
        statusText: "仅内网端启用",
        summaryText: "共享缓存仓只在内网端运行，外网端默认只消费共享目录中的最新有效文件。",
        currentHourBucket: "-",
        lastRunAt: "",
        lastSuccessAt: "",
        errorText: "",
        cacheRoot: "",
        items: [],
        families: [],
      };
    }
    const rawCache = health.shared_bridge?.internal_source_cache || {};
    const enabled = Boolean(rawCache.enabled);
    const running = Boolean(rawCache.scheduler_running);
    const currentHourBucket = String(rawCache.current_hour_bucket || "").trim();
    const lastRunAt = String(rawCache.last_run_at || "").trim();
    const lastSuccessAt = String(rawCache.last_success_at || "").trim();
    const lastError = formatSharedBridgeRuntimeError(rawCache.last_error);
    const cacheRoot = String(rawCache.cache_root || "").trim();
    const handoverFamily = rawCache.handover_log_family && typeof rawCache.handover_log_family === "object"
      ? rawCache.handover_log_family
      : rawCache.handover_family && typeof rawCache.handover_family === "object"
        ? rawCache.handover_family
        : {};
    const handoverCapacityFamily = rawCache.handover_capacity_report_family && typeof rawCache.handover_capacity_report_family === "object"
      ? rawCache.handover_capacity_report_family
      : {};
    const monthlyFamily = rawCache.monthly_report_family && typeof rawCache.monthly_report_family === "object"
      ? rawCache.monthly_report_family
      : rawCache.monthly_family && typeof rawCache.monthly_family === "object"
        ? rawCache.monthly_family
        : {};
    const alarmFamily = rawCache.alarm_event_family && typeof rawCache.alarm_event_family === "object"
      ? rawCache.alarm_event_family
      : {};
    const families = [
      normalizeSourceCacheFamilyOverview({
        key: "handover_log_family",
        title: "交接班日志源文件",
        payload: handoverFamily,
        fallbackBucket: currentHourBucket,
      }),
      normalizeSourceCacheFamilyOverview({
        key: "handover_capacity_report_family",
        title: "交接班容量报表源文件",
        payload: handoverCapacityFamily,
        fallbackBucket: currentHourBucket,
      }),
      normalizeSourceCacheFamilyOverview({
        key: "monthly_report_family",
        title: "全景平台月报源文件",
        payload: monthlyFamily,
        fallbackBucket: currentHourBucket,
      }),
      normalizeSourceCacheFamilyOverview({
        key: "alarm_event_family",
        title: "告警信息源文件",
        payload: alarmFamily,
        fallbackBucket: String(alarmFamily.current_bucket || "").trim() || currentHourBucket,
      }),
    ];
    let tone = "warning";
    let statusText = "准备中";
    let summaryText = "内网端会维护四组共享源文件：交接班日志源文件、交接班容量报表源文件、全景平台月报源文件，以及按策略拉取的告警信息源文件。";
    if (!enabled) {
      tone = "warning";
      statusText = "未启用";
      summaryText = "当前未启用共享缓存仓。";
    } else if (families.some((family) => family.hasFailures || family.hasBlocked) || (!lastSuccessAt && lastError)) {
      tone = "danger";
      statusText = "最近一轮存在失败";
      summaryText = "最近一轮共享文件同步存在失败楼栋，请检查共享目录权限和内网页面登录状态。";
    } else if (families.every((family) => family.allReady && family.buildings.length > 0)) {
      tone = "success";
      statusText = "本轮共享文件已全部就绪";
      summaryText = "交接班、容量报表、月报和告警信息四组共享文件都已就绪。";
    } else if (running) {
      tone = "warning";
      statusText = "运行中";
      summaryText = "共享缓存仓正在维护交接班、容量报表、月报和最近应执行的告警信息文件。";
    }
    return {
      tone,
      statusText,
      summaryText,
      currentHourBucket: currentHourBucket || "-",
      lastRunAt,
      lastSuccessAt,
      errorText: lastError,
      cacheRoot,
      items: [
        {
          label: "当前小时桶",
          value: currentHourBucket || "-",
          tone: currentHourBucket ? "info" : "neutral",
        },
        {
          label: "最近成功时间",
          value: lastSuccessAt || "-",
          tone: lastSuccessAt ? "success" : "neutral",
        },
        {
          label: "最近调度时间",
          value: lastRunAt || "-",
          tone: lastRunAt ? "info" : "neutral",
        },
      ],
      families,
    };
  });
  const internalRealtimeSourceFamilies = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    if (roleMode !== "internal") {
      return [];
    }
    const families = Array.isArray(internalSourceCacheOverview.value?.families)
      ? internalSourceCacheOverview.value.families
      : [];
    return families.map((family) => ({
      key: family.key,
      title: family.title,
      tone: family.tone,
      statusText: family.statusText,
      currentBucket: family.currentBucket || internalSourceCacheOverview.value?.currentHourBucket || "-",
      lastSuccessAt: family.lastSuccessAt || "",
      buildings: Array.isArray(family.buildings) ? family.buildings : [],
    }));
  });
  function normalizeExternalInternalAlertBuilding(raw, fallbackBuilding) {
    const building = String(raw?.building || fallbackBuilding || "").trim() || "-";
    const status = String(raw?.status || "").trim().toLowerCase();
    const lastProblemAt = String(raw?.last_problem_at || "").trim();
    const lastRecoveredAt = String(raw?.last_recovered_at || "").trim();
    const activeCount = Number.parseInt(String(raw?.active_count || 0), 10) || 0;
    if (status === "problem") {
      return {
        building,
        tone: "danger",
        statusText: "异常",
        summaryText: String(raw?.summary || "").trim() || "存在内网异常告警",
        detailText: String(raw?.detail || "").trim(),
        timeText: lastProblemAt ? `最近告警：${lastProblemAt}` : "",
        activeCount,
      };
    }
    return {
      building,
      tone: "success",
      statusText: "正常",
      summaryText: lastRecoveredAt ? "已恢复正常" : "正常",
      detailText: "",
      timeText: lastRecoveredAt ? `最近恢复：${lastRecoveredAt}` : "",
      activeCount: 0,
    };
  }
  const externalInternalAlertOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    if (roleMode !== "external") {
      return {
        tone: "neutral",
        statusText: "仅外网端展示",
        summaryText: "外网端通过内网环境告警状态展示 5 楼状态。",
        items: [],
        buildings: [],
      };
    }
    const rawStatus = health.shared_bridge?.internal_alert_status || {};
    const rawBuildings = Array.isArray(rawStatus.buildings) ? rawStatus.buildings : [];
    const buildingMap = new Map(
      rawBuildings
        .filter((item) => item && typeof item === "object")
        .map((item) => [String(item.building || "").trim(), item]),
    );
    const buildings = INTERNAL_BUILDINGS.map((building) =>
      normalizeExternalInternalAlertBuilding(buildingMap.get(building), building),
    );
    const activeCount = Number.parseInt(String(rawStatus.active_count || 0), 10) || 0;
    const lastNotifiedAt = String(rawStatus.last_notified_at || "").trim();
    return {
      tone: activeCount > 0 ? "danger" : "success",
      statusText: activeCount > 0 ? "存在异常楼栋" : "5楼均正常",
      summaryText: activeCount > 0
        ? `当前有 ${activeCount} 个楼栋存在未恢复的内网告警。`
        : "当前未收到内网异常告警，5 个楼均显示正常。",
      items: [
        {
          label: "异常楼栋",
          value: `${activeCount}/5`,
          tone: activeCount > 0 ? "danger" : "success",
        },
        {
          label: "最近告警同步",
          value: lastNotifiedAt || "-",
          tone: lastNotifiedAt ? "info" : "neutral",
        },
      ],
      buildings,
    };
  });
  const currentHourRefreshOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    const rawCache = health.shared_bridge?.internal_source_cache || {};
    const payload = rawCache.current_hour_refresh && typeof rawCache.current_hour_refresh === "object"
      ? rawCache.current_hour_refresh
      : {};
    const running = Boolean(payload.running);
    const lastRunAt = String(payload.last_run_at || "").trim();
    const lastSuccessAt = String(payload.last_success_at || "").trim();
    const lastError = formatSharedBridgeRuntimeError(payload.last_error);
    const failedBuildings = Array.isArray(payload.failed_buildings)
      ? payload.failed_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const blockedBuildings = Array.isArray(payload.blocked_buildings)
      ? payload.blocked_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const runningBuildings = Array.isArray(payload.running_buildings)
      ? payload.running_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const completedBuildings = Array.isArray(payload.completed_buildings)
      ? payload.completed_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    if (roleMode !== "internal") {
      return {
        tone: "neutral",
        statusText: "当前角色未启用",
        summaryText: "",
        lastRunAt: "",
        lastSuccessAt: "",
        lastError: "",
        failedBuildings: [],
        blockedBuildings: [],
        runningBuildings: [],
        completedBuildings: [],
      };
    }
    if (running) {
      const runningSummary = runningBuildings.length
        ? `当前并行处理中：${runningBuildings.join(" / ")}`
        : "正在立即补下当前小时的交接班日志源文件、全景平台月报源文件，以及最近应执行的告警信息源文件。";
      return {
        tone: "warning",
        statusText: "当前小时下载中",
        summaryText: runningSummary,
        lastRunAt,
        lastSuccessAt,
        lastError,
        failedBuildings,
        blockedBuildings,
        runningBuildings,
        completedBuildings,
      };
    }
    if (failedBuildings.length || blockedBuildings.length || (!lastSuccessAt && lastError)) {
      let summaryText = "当前小时下载最近一轮存在失败项，请检查对应楼栋的登录态、共享目录权限和下载页面可用性。";
      if (!failedBuildings.length && blockedBuildings.length) {
        summaryText = `以下楼栋正在等待恢复后再继续下载：${blockedBuildings.join(" / ")}`;
      } else if (failedBuildings.length && blockedBuildings.length) {
        summaryText = `失败项：${failedBuildings.join(" / ")}；等待恢复：${blockedBuildings.join(" / ")}`;
      }
      return {
        tone: "danger",
        statusText: "最近一轮存在失败",
        summaryText,
        lastRunAt,
        lastSuccessAt,
        lastError: failedBuildings.length || blockedBuildings.length ? lastError : "",
        failedBuildings,
        blockedBuildings,
        runningBuildings,
        completedBuildings,
      };
    }
    if (lastSuccessAt) {
      const summaryText = completedBuildings.length
        ? `最近一轮已完成：${completedBuildings.join(" / ")}`
        : "当前小时共享文件和最近应执行的告警信息源文件已完成一轮立即补下。";
      return {
        tone: "success",
        statusText: "最近一轮已完成",
        summaryText,
        lastRunAt,
        lastSuccessAt,
        lastError: "",
        failedBuildings: [],
        blockedBuildings: [],
        runningBuildings,
        completedBuildings,
      };
    }
    return {
      tone: "neutral",
      statusText: "尚未手动执行",
      summaryText: "可手动触发“立即下载当前小时全部文件”，同时补下当前小时共享文件和最近应执行的告警信息文件。",
      lastRunAt,
      lastSuccessAt,
      lastError,
      failedBuildings,
      blockedBuildings,
      runningBuildings,
      completedBuildings,
    };
  });
  const internalRuntimeOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    if (roleMode !== "internal") {
      return {
        tone: "neutral",
        statusText: "仅内网端启用",
        summaryText: "",
        items: [],
        cacheRoot: "",
        errorText: "",
        poolStatusText: "",
        poolSummaryText: "",
        poolItems: [],
        poolErrorText: "",
        slots: [],
        currentHourRefresh: {
          tone: "neutral",
          statusText: "",
          summaryText: "",
          lastRunAt: "",
          lastSuccessAt: "",
          lastError: "",
          failedBuildings: [],
          blockedBuildings: [],
          runningBuildings: [],
          completedBuildings: [],
        },
        families: [],
      };
    }
    const sourceCache = internalSourceCacheOverview.value;
    const downloadPool = internalDownloadPoolOverview.value;
    return {
      tone: sourceCache.tone,
      statusText: sourceCache.statusText,
      summaryText: sourceCache.summaryText,
      items: Array.isArray(sourceCache.items) ? sourceCache.items : [],
      cacheRoot: sourceCache.cacheRoot || "",
      errorText: sourceCache.errorText || "",
      poolStatusText: downloadPool.statusText || "",
      poolSummaryText: downloadPool.summaryText || "",
      poolItems: Array.isArray(downloadPool.items) ? downloadPool.items : [],
      poolErrorText: downloadPool.errorText || "",
      slots: Array.isArray(downloadPool.slots) ? downloadPool.slots : [],
      currentHourRefresh: currentHourRefreshOverview.value,
      families: Array.isArray(sourceCache.families) ? sourceCache.families : [],
    };
  });
  const internalSourceCacheHistoryOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    if (roleMode !== "internal") {
      return {
        tone: "neutral",
        statusText: "",
        summaryText: "",
        items: [],
        recentLogs: [],
      };
    }
    const sourceCache = internalSourceCacheOverview.value;
    const currentHourRefresh = currentHourRefreshOverview.value;
    const recentLogs = internalOpsLogs.value
      .filter((line) => line.includes("[共享缓存]"))
      .slice(0, 8);
    let tone = "neutral";
    let statusText = "暂无历史";
    let summaryText = "这里只保留最近调度、最近成功、最近错误和共享缓存日志，不再重复展示当前实时状态。";
    if (currentHourRefresh.tone === "danger" || sourceCache.tone === "danger") {
      tone = "danger";
      statusText = "最近存在失败";
      summaryText = "最近一轮小时下载或手动补下存在失败，请检查对应楼栋登录态、共享目录权限和下载页面可用性。";
    } else if (sourceCache.lastSuccessAt || currentHourRefresh.lastSuccessAt) {
      tone = "success";
      statusText = "最近调度正常";
      summaryText = "最近一次共享缓存调度和手动补下已完成，可在这里查看历史时间点和最近日志。";
    } else if (sourceCache.lastRunAt || currentHourRefresh.lastRunAt || recentLogs.length) {
      tone = "warning";
      statusText = "已有历史记录";
      summaryText = "最近已有共享缓存调度记录，当前卡片只保留历史摘要和最近日志。";
    }
    return {
      tone,
      statusText,
      summaryText,
      items: [
        {
          label: "当前小时桶",
          value: sourceCache.currentHourBucket || "-",
          tone: sourceCache.currentHourBucket ? "info" : "neutral",
        },
        {
          label: "最近小时调度",
          value: sourceCache.lastRunAt || "-",
          tone: sourceCache.lastRunAt ? "info" : "neutral",
        },
        {
          label: "最近小时成功",
          value: sourceCache.lastSuccessAt || "-",
          tone: sourceCache.lastSuccessAt ? "success" : "neutral",
        },
        {
          label: "当前小时最近触发",
          value: currentHourRefresh.lastRunAt || "-",
          tone: currentHourRefresh.lastRunAt ? "warning" : "neutral",
        },
        {
          label: "当前小时最近完成",
          value: currentHourRefresh.lastSuccessAt || "-",
          tone: currentHourRefresh.lastSuccessAt ? "success" : "neutral",
        },
        {
          label: "最近错误",
          value: currentHourRefresh.lastError || sourceCache.errorText || "-",
          tone: currentHourRefresh.lastError || sourceCache.errorText ? "danger" : "neutral",
        },
      ],
      recentLogs,
      lastError: currentHourRefresh.lastError || sourceCache.errorText || "",
    };
  });
  const sharedSourceCacheReadinessOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    const rawCache = health.shared_bridge?.internal_source_cache || {};
    const lastError = formatSharedBridgeRuntimeError(rawCache.last_error);
    const gatingFamilies = [
      normalizeLatestSelectionOverview({
        key: "handover_log_family",
        title: "交接班日志源文件",
        payload: (rawCache.handover_log_family || rawCache.handover_family || {}).latest_selection || {},
      }),
      normalizeLatestSelectionOverview({
        key: "monthly_report_family",
        title: "全景平台月报源文件",
        payload: (rawCache.monthly_report_family || rawCache.monthly_family || {}).latest_selection || {},
      }),
    ];
    const displayLatestFamilies = [
      normalizeLatestSelectionOverview({
        key: "handover_log_family",
        title: "交接班日志源文件",
        payload: (rawCache.handover_log_family || rawCache.handover_family || {}).latest_selection || {},
      }),
      normalizeLatestSelectionOverview({
        key: "handover_capacity_report_family",
        title: "交接班容量报表源文件",
        payload: (rawCache.handover_capacity_report_family || {}).latest_selection || {},
      }),
      normalizeLatestSelectionOverview({
        key: "monthly_report_family",
        title: "全景平台月报源文件",
        payload: (rawCache.monthly_report_family || rawCache.monthly_family || {}).latest_selection || {},
      }),
    ];
    const displayFamilies = displayLatestFamilies.map((family) => {
      if (family.key === "handover_capacity_report_family") {
        return {
          ...family,
          summaryText: family.canProceed && family.buildings.length
            ? "当前容量共享源文件已齐套，仅同步展示供外网状态页与容量报表链路参考。"
            : family.summaryText || "容量共享源文件状态仅用于同步展示。",
        };
      }
      if (family.key === "monthly_report_family") {
        return normalizeMonthlyDateFileFamilyOverview(family);
      }
      return family;
    });
    const alarmFamily = normalizeAlarmEventReadinessOverview({
      key: "alarm_event_family",
      title: "告警信息源文件",
      payload: rawCache.alarm_event_family || {},
    });
    if (roleMode !== "external") {
      return {
        tone: "neutral",
        statusText: "当前角色未使用共享缓存",
        summaryText: "",
        displayNoteText: "",
        referenceBucketKey: "-",
        errorText: "",
        families: [],
        canProceedLatest: false,
        autoRetrySignature: "",
        familyCanProceed: {},
        familyRetrySignatures: {},
      };
    }
    const hasStale = gatingFamilies.some((family) => family.staleBuildings.length > 0);
    const hasTooOld = gatingFamilies.some((family) => family.isBestBucketTooOld);
    const hasMissing = gatingFamilies.some((family) => family.missingBuildings.length > 0);
    const hasFallback = gatingFamilies.some((family) => family.fallbackBuildings.length > 0);
    const allReady = gatingFamilies.every((family) => family.canProceed && family.buildings.length > 0);
    const referenceBucketKey = gatingFamilies
      .map((family) => family.bestBucketKey)
      .filter(Boolean)
      .sort()
      .slice(-1)[0] || "-";
    const autoRetrySignature = gatingFamilies.map((family) => [
      family.key,
      family.bestBucketKey,
      family.buildings.map((building) =>
        [
          building.building,
          building.statusKey,
          building.bucketKey,
          building.versionGap ?? "",
          building.usingFallback ? "1" : "0",
        ].join(":"),
      ).join("|"),
      family.isBestBucketTooOld ? "1" : "0",
      family.bestBucketAgeText,
    ].join("=")).join("||");
    const familyCanProceed = Object.fromEntries(
      gatingFamilies.map((family) => [family.key, Boolean(family.canProceed)]),
    );
    const familyRetrySignatures = Object.fromEntries(
      gatingFamilies.map((family) => [
        family.key,
        [
          family.bestBucketKey,
          family.buildings.map((building) =>
            [
              building.building,
              building.statusKey,
              building.bucketKey,
              building.versionGap ?? "",
              building.usingFallback ? "1" : "0",
            ].join(":"),
          ).join("|"),
          family.isBestBucketTooOld ? "1" : "0",
          family.bestBucketAgeText,
        ].join("="),
      ]),
    );
    const overlayTasks = buildSharedSourceCacheBackfillOverlays(activeBridgeTasks.value);
    const families = [...displayFamilies, alarmFamily].map((family) => {
      if (family.key === "alarm_event_family") return family;
      return applySharedSourceCacheBackfillOverlay(family, overlayTasks);
    });
    return {
      tone: hasTooOld || hasStale ? "danger" : allReady ? "success" : "warning",
      statusText: hasTooOld ? "等待共享文件更新" : hasStale ? "等待共享文件就绪" : allReady ? "共享文件已就绪" : "等待共享文件就绪",
      summaryText: hasTooOld
        ? "当前共享参考文件整体已超过 3 小时，等待内网更新后会自动重试默认入口。"
        : hasStale
          ? "部分楼栋共享文件版本过旧，等待更新后会自动重试默认入口。"
          : hasMissing
            ? "部分楼栋共享文件缺失，等待补齐后会自动重试默认入口。"
          : hasFallback
            ? "当前允许部分楼栋回退到不超过 3 桶的上一版共享文件。"
            : "外网默认入口继续只依赖交接班日志源文件与全景平台月报源文件。",
      displayNoteText: "交接班容量报表源文件仅在状态页同步展示，不单独阻断外网默认流程。",
      referenceBucketKey,
      errorText: lastError,
      families,
      canProceedLatest: allReady,
      autoRetrySignature,
      familyCanProceed,
      familyRetrySignatures,
    };
  });
  const updaterMirrorOverview = computed(() => {
    const updater = health.updater || {};
    const updaterEnabled = updater?.enabled !== false;
    const disabledReason = String(updater?.disabled_reason || "").trim().toLowerCase();
    const sourceKind = String(updater.source_kind || "").trim().toLowerCase();
    const sourceLabel = formatUpdaterSourceLabel(updater);
    const mirrorReady = Boolean(updater.mirror_ready);
    const mirrorVersion = String(updater.mirror_version || "").trim();
    const localVersion = String(updater.local_version || "").trim() || "-";
    const localRevision = Number.parseInt(String(updater.local_release_revision || 0), 10) || 0;
    const lastPublishAt = String(updater.last_publish_at || "").trim();
    const manifestPath = String(updater.mirror_manifest_path || "").trim();
    const errorText = String(updater.last_publish_error || "").trim();
    if (!updaterEnabled && disabledReason === "source_python_run") {
      return {
        tone: "info",
        kicker: "调试模式",
        title: "本地运行模式",
        statusText: "本地调试模式",
        summaryText: "当前为 Python 本地源码运行，已跳过自动更新与共享目录镜像检查。",
        manifestPath: "",
        errorText: "",
        items: [
          {
            label: "运行方式",
            value: "Python 本地源码运行",
            tone: "info",
          },
          {
            label: "当前版本",
            value: localRevision > 0 ? `${localVersion} / r${localRevision}` : localVersion,
            tone: "neutral",
          },
          {
            label: "更新行为",
            value: "不自动更新",
            tone: "neutral",
          },
          {
            label: "共享镜像",
            value: "不检查",
            tone: "neutral",
          },
        ],
      };
    }
    let tone = "neutral";
    let statusText = "尚未发布到共享目录";
    let summaryText = "当前更新链路尚未生成可供内网跟随的批准版本。";
    if (sourceKind === "shared_mirror") {
      if (mirrorReady) {
        tone = "success";
        statusText = "已检测到共享目录批准版本";
        summaryText = "当前使用共享目录更新源，不访问互联网；检测到新批准版本后会自动跟随。";
      } else if (errorText) {
        tone = "danger";
        statusText = "共享目录更新源异常";
        summaryText = "当前使用共享目录更新源，但镜像读取失败，请先检查共享目录可访问性。";
      } else {
        tone = "warning";
        statusText = "等待外网端发布批准版本";
        summaryText = "当前使用共享目录更新源，不访问互联网；外网端发布批准版本后会自动跟随。";
      }
    } else if (mirrorReady) {
      tone = "success";
      statusText = "已发布批准版本到共享目录";
      summaryText = "外网端已把当前已验证版本发布到共享目录，内网端可自动跟随。";
    } else if (errorText) {
      tone = "danger";
      statusText = "共享目录发布失败";
      summaryText = "当前仍使用远端正式更新源，但最近一次共享目录镜像发布失败。";
    } else {
      tone = "warning";
      statusText = "尚未发布批准版本";
      summaryText = "当前仍使用远端正式更新源；完成验证后会把批准版本发布到共享目录。";
    }
    return {
      tone,
      kicker: "更新镜像",
      title: "共享目录批准版本",
      statusText,
      summaryText,
      manifestPath,
      errorText,
      items: [
        {
          label: "更新源",
          value: sourceLabel,
          tone: sourceKind === "shared_mirror" ? "warning" : "info",
        },
        {
          label: "本机版本",
          value: localRevision > 0 ? `${localVersion} / r${localRevision}` : localVersion,
          tone: "neutral",
        },
        {
          label: "批准版本号",
          value: mirrorVersion || (sourceKind === "shared_mirror" ? "待外网端发布" : "尚未发布"),
          tone: mirrorReady ? "success" : "neutral",
        },
        {
          label: "共享目录更新时间",
          value: lastPublishAt || "-",
          tone: lastPublishAt ? "info" : "neutral",
        },
      ],
    };
  });
  const currentBridgeTask = computed(() => {
    const selectedTaskId = String(selectedBridgeTaskId.value || "").trim();
    if (bridgeTaskDetail.value && String(bridgeTaskDetail.value?.task_id || "").trim() === selectedTaskId) {
      return bridgeTaskDetail.value;
    }
    if (selectedTaskId) {
      const matched = bridgeTasks.value.find((item) => String(item?.task_id || "").trim() === selectedTaskId);
      if (matched) return matched;
    }
    return bridgeTaskDetail.value || bridgeTasks.value[0] || null;
  });
  const dayMetricRetryAllMode = computed(() => {
    const payload = dayMetricCurrentPayload.value;
    const mode = String(payload?.mode || "from_download").trim().toLowerCase();
    return mode === "from_file" ? "from_file" : "from_download";
  });
  const handoverDutyAutoLabel = computed(() => (handoverDutyAutoFollow.value ? "当前自动" : "手动覆盖"));
  const schedulerDecisionText = computed(() => mapSchedulerDecisionText(health.scheduler.last_decision));
  const schedulerTriggerText = computed(() => mapSchedulerTriggerText(health.scheduler.last_trigger_result));
  const wetBulbSchedulerDecisionText = computed(() =>
    mapSchedulerDecisionText(health.wet_bulb_collection?.scheduler?.last_decision),
  );
  const wetBulbSchedulerTriggerText = computed(() =>
    mapSchedulerTriggerText(health.wet_bulb_collection?.scheduler?.last_trigger_result),
  );
  const monthlyEventReportSchedulerDecisionText = computed(() =>
    mapSchedulerDecisionText(health.monthly_event_report?.scheduler?.last_decision),
  );
  const monthlyEventReportSchedulerTriggerText = computed(() =>
    mapSchedulerTriggerText(health.monthly_event_report?.scheduler?.last_trigger_result),
  );
  const monthlyChangeReportSchedulerDecisionText = computed(() =>
    mapSchedulerDecisionText(health.monthly_change_report?.scheduler?.last_decision),
  );
  const monthlyChangeReportSchedulerTriggerText = computed(() =>
    mapSchedulerTriggerText(health.monthly_change_report?.scheduler?.last_trigger_result),
  );
  const dayMetricUploadSchedulerDecisionText = computed(() =>
    mapSchedulerDecisionText(health.day_metric_upload?.scheduler?.last_decision),
  );
  const dayMetricUploadSchedulerTriggerText = computed(() =>
    mapSchedulerTriggerText(health.day_metric_upload?.scheduler?.last_trigger_result),
  );
  const alarmEventUploadSchedulerDecisionText = computed(() =>
    mapSchedulerDecisionText(health.alarm_event_upload?.scheduler?.last_decision),
  );
  const alarmEventUploadSchedulerTriggerText = computed(() =>
    mapSchedulerTriggerText(health.alarm_event_upload?.scheduler?.last_trigger_result),
  );
  const handoverMorningDecisionText = computed(() =>
    mapSchedulerDecisionText(health.handover_scheduler?.morning?.last_decision),
  );
  const handoverAfternoonDecisionText = computed(() =>
    mapSchedulerDecisionText(health.handover_scheduler?.afternoon?.last_decision),
  );
  const handoverReviewRows = computed(() =>
    buildHandoverReviewRowSnapshot(
      Array.isArray(health.handover?.review_status?.buildings) ? health.handover.review_status.buildings : [],
      Array.isArray(health.handover?.review_links) ? health.handover.review_links : [],
    ),
  );
  const handoverReviewStatusItems = computed(() => {
    return handoverReviewRows.value.map((row) => `${row.building} ${row.text}`).filter(Boolean);
  });
  const handoverReviewLinks = computed(() => {
    return handoverReviewRows.value
      .filter((row) => row.hasUrl)
      .map((row) => ({
        building: row.building,
        code: String(row?.link?.code || "").trim().toLowerCase(),
        url: row.url,
      }));
  });
  const handoverReviewMatrix = computed(() => {
    return handoverReviewRows.value.map((row) => ({
      building: row.building,
      status: row.status,
      text: row.text,
      tone: row.tone,
      url: row.url,
    }));
  });
  const handoverReviewBoardRows = computed(() => handoverReviewRows.value);
  const dashboardSystemStatusItems = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment.role_mode || "");
    const items = [
      {
        label: "角色模式",
        value: roleMode === "internal" ? "内网端" : roleMode === "external" ? "外网端" : "待选择角色",
        tone: roleMode === "internal" ? "warning" : roleMode === "external" ? "info" : "neutral",
      },
      {
        label: "当前任务",
        value: health.active_job_id || "空闲",
        tone: health.active_job_id ? "info" : "neutral",
      },
      {
        label: "更新状态",
        value: updaterResultText.value || "-",
        tone: health.updater?.update_available || health.updater?.force_apply_available ? "warning" : "neutral",
      },
      {
        label: "共享桥接",
        value:
          health.shared_bridge.db_status === "ok"
            ? `正常 / 内待 ${health.shared_bridge.pending_internal || 0} / 外待 ${health.shared_bridge.pending_external || 0}`
            : formatSharedBridgeDbStatus(health.shared_bridge.db_status),
        tone: health.shared_bridge.db_status === "ok" ? "info" : "neutral",
      },
      {
        label: "桥接代理",
        value: formatSharedBridgeAgentStatus(health.shared_bridge.agent_status, health.shared_bridge.node_heartbeat_ok),
        tone:
          health.shared_bridge.agent_status === "running"
            ? (health.shared_bridge.node_heartbeat_ok ? "success" : "warning")
            : health.shared_bridge.agent_status === "disabled"
              ? "neutral"
              : "warning",
      },
    ];
    const lastBridgeError = formatSharedBridgeRuntimeError(health.shared_bridge.last_error);
    if (lastBridgeError) {
      items.push({
        label: "桥接异常",
        value: lastBridgeError,
        tone: "danger",
      });
    }
    return items;
  });
  const dashboardScheduleStatusItems = computed(() => [
    {
      label: "月报调度",
      value: health.scheduler.status || "-",
      tone: health.scheduler.running ? "success" : "neutral",
    },
    {
      label: "交接班调度",
      value: health.handover_scheduler.status || "-",
      tone: health.handover_scheduler.running ? "success" : "neutral",
    },
    {
      label: "月报下次",
      value: health.scheduler.next_run_time || "-",
      tone: "neutral",
    },
    {
      label: "交接班上午/下午",
      value: `${(health.handover_scheduler.morning && health.handover_scheduler.morning.next_run_time) || "-"} / ${(
        health.handover_scheduler.afternoon && health.handover_scheduler.afternoon.next_run_time
      ) || "-"}`,
      tone: "neutral",
    },
    {
      label: "湿球采集调度",
      value: health.wet_bulb_collection.scheduler.status || "-",
      tone: health.wet_bulb_collection.scheduler.running ? "success" : "neutral",
    },
    {
      label: "月度事件统计调度",
      value: health.monthly_event_report.scheduler.status || "-",
      tone: health.monthly_event_report.scheduler.running ? "success" : "neutral",
    },
    {
      label: "湿球采集下次",
      value: health.wet_bulb_collection.scheduler.next_run_time || "-",
      tone: "neutral",
    },
    {
      label: "月度事件统计下次",
      value: health.monthly_event_report.scheduler.next_run_time || "-",
      tone: "neutral",
    },
    {
      label: "最近月报触发",
      value: `${health.scheduler.last_trigger_at || "-"} / ${schedulerTriggerText.value || "-"}`,
      tone: String(health.scheduler.last_trigger_result || "").trim().toLowerCase().includes("fail") ? "danger" : "neutral",
    },
    {
      label: "最近湿球采集触发",
      value: `${health.wet_bulb_collection.scheduler.last_trigger_at || "-"} / ${wetBulbSchedulerTriggerText.value || "-"}`,
      tone: String(health.wet_bulb_collection.scheduler.last_trigger_result || "").trim().toLowerCase().includes("fail")
        ? "danger"
        : "neutral",
    },
    {
      label: "最近月度事件统计触发",
      value: `${health.monthly_event_report.scheduler.last_trigger_at || "-"} / ${monthlyEventReportSchedulerTriggerText.value || "-"}`,
      tone: String(health.monthly_event_report.scheduler.last_trigger_result || "").trim().toLowerCase().includes("fail")
        ? "danger"
        : "neutral",
    },
  ]);
  const schedulerOverviewItems = computed(() => {
    const handoverMorningTriggerText = mapSchedulerTriggerText(health.handover_scheduler?.morning?.last_trigger_result);
    const handoverAfternoonTriggerText = mapSchedulerTriggerText(health.handover_scheduler?.afternoon?.last_trigger_result);
    return [
      {
        key: "auto_flow",
        title: "每日用电明细自动流程",
        moduleId: "auto_flow",
        focusKey: "",
        ...normalizeSchedulerStatusVm({
          running: health.scheduler.running,
          configured: Boolean(String(config.value?.scheduler?.run_time || "").trim()),
          status: health.scheduler.status,
          lastTriggerResult: health.scheduler.last_trigger_result,
          lastDecision: health.scheduler.last_decision,
        }),
        summaryText: schedulerDecisionText.value || schedulerTriggerText.value || "标准月报主流程调度",
        parts: [
          {
            label: "每日调度",
            runTimeText: normalizeSchedulerText(config.value?.scheduler?.run_time, "未设置"),
            nextRunText: normalizeSchedulerDateText(health.scheduler.next_run_time),
            lastTriggerText: normalizeSchedulerDateText(health.scheduler.last_trigger_at, "暂无记录"),
            resultText: schedulerTriggerText.value || "暂无记录",
          },
        ],
      },
      {
        key: "handover_log",
        title: "交接班日志",
        moduleId: "handover_log",
        focusKey: "",
        ...normalizeSchedulerStatusVm({
          running: health.handover_scheduler.running,
          configured: Boolean(
            String(config.value?.handover_log?.scheduler?.morning_time || "").trim()
              && String(config.value?.handover_log?.scheduler?.afternoon_time || "").trim(),
          ),
          status: health.handover_scheduler.status,
          lastTriggerResult: `${health.handover_scheduler?.morning?.last_trigger_result || ""} ${health.handover_scheduler?.afternoon?.last_trigger_result || ""}`,
          lastDecision: `${health.handover_scheduler?.morning?.last_decision || ""} ${health.handover_scheduler?.afternoon?.last_decision || ""}`,
        }),
        summaryText:
          handoverMorningDecisionText.value
          || handoverAfternoonDecisionText.value
          || handoverMorningTriggerText
          || handoverAfternoonTriggerText
          || "上午补跑夜班，下午执行白班",
        parts: [
          {
            label: "上午调度",
            runTimeText: normalizeSchedulerText(config.value?.handover_log?.scheduler?.morning_time, "未设置"),
            nextRunText: normalizeSchedulerDateText(health.handover_scheduler?.morning?.next_run_time),
            lastTriggerText: normalizeSchedulerDateText(health.handover_scheduler?.morning?.last_trigger_at, "暂无记录"),
            resultText: handoverMorningTriggerText || "暂无记录",
          },
          {
            label: "下午调度",
            runTimeText: normalizeSchedulerText(config.value?.handover_log?.scheduler?.afternoon_time, "未设置"),
            nextRunText: normalizeSchedulerDateText(health.handover_scheduler?.afternoon?.next_run_time),
            lastTriggerText: normalizeSchedulerDateText(health.handover_scheduler?.afternoon?.last_trigger_at, "暂无记录"),
            resultText: handoverAfternoonTriggerText || "暂无记录",
          },
        ],
      },
      {
        key: "day_metric_upload",
        title: "12项独立上传",
        moduleId: "day_metric_upload",
        focusKey: "",
        ...normalizeSchedulerStatusVm({
          running: health.day_metric_upload?.scheduler?.running,
          configured: Boolean(String(config.value?.day_metric_upload?.scheduler?.run_time || "").trim()),
          status: health.day_metric_upload?.scheduler?.status,
          lastTriggerResult: health.day_metric_upload?.scheduler?.last_trigger_result,
          lastDecision: health.day_metric_upload?.scheduler?.last_decision,
        }),
        summaryText: dayMetricUploadSchedulerDecisionText.value || dayMetricUploadSchedulerTriggerText.value || "固定处理当天、全部启用楼栋",
        parts: [
          {
            label: "每日调度",
            runTimeText: normalizeSchedulerText(config.value?.day_metric_upload?.scheduler?.run_time, "未设置"),
            nextRunText: normalizeSchedulerDateText(health.day_metric_upload?.scheduler?.next_run_time),
            lastTriggerText: normalizeSchedulerDateText(health.day_metric_upload?.scheduler?.last_trigger_at, "暂无记录"),
            resultText: dayMetricUploadSchedulerTriggerText.value || "暂无记录",
          },
        ],
      },
      {
        key: "wet_bulb_collection",
        title: "湿球温度定时采集",
        moduleId: "wet_bulb_collection",
        focusKey: "",
        ...normalizeSchedulerStatusVm({
          running: health.wet_bulb_collection?.scheduler?.running,
          configured: Number.parseInt(String(config.value?.wet_bulb_collection?.scheduler?.interval_minutes || 0), 10) > 0,
          status: health.wet_bulb_collection?.scheduler?.status,
          lastTriggerResult: health.wet_bulb_collection?.scheduler?.last_trigger_result,
          lastDecision: health.wet_bulb_collection?.scheduler?.last_decision,
        }),
        summaryText: wetBulbSchedulerDecisionText.value || wetBulbSchedulerTriggerText.value || "按固定分钟间隔循环执行",
        parts: [
          {
            label: "循环调度",
            runTimeText:
              Number.parseInt(String(config.value?.wet_bulb_collection?.scheduler?.interval_minutes || 0), 10) > 0
                ? `每 ${config.value?.wet_bulb_collection?.scheduler?.interval_minutes} 分钟`
                : "未设置",
            nextRunText: normalizeSchedulerDateText(health.wet_bulb_collection?.scheduler?.next_run_time),
            lastTriggerText: normalizeSchedulerDateText(health.wet_bulb_collection?.scheduler?.last_trigger_at, "暂无记录"),
            resultText: wetBulbSchedulerTriggerText.value || "暂无记录",
          },
        ],
      },
      {
        key: "monthly_event_report",
        title: "体系月度统计表-事件",
        moduleId: "monthly_event_report",
        focusKey: "monthly_event",
        ...normalizeSchedulerStatusVm({
          running: health.monthly_event_report?.scheduler?.running,
          configured: Boolean(
            Number.parseInt(String(config.value?.handover_log?.monthly_event_report?.scheduler?.day_of_month || 0), 10) > 0
              && String(config.value?.handover_log?.monthly_event_report?.scheduler?.run_time || "").trim(),
          ),
          status: health.monthly_event_report?.scheduler?.status,
          lastTriggerResult: health.monthly_event_report?.scheduler?.last_trigger_result,
          lastDecision: health.monthly_event_report?.scheduler?.last_decision,
        }),
        summaryText: monthlyEventReportSchedulerDecisionText.value || monthlyEventReportSchedulerTriggerText.value || "固定读取上一个自然月事件数据",
        parts: [
          {
            label: "事件月报",
            runTimeText: buildMonthlySchedulerRunText(
              config.value?.handover_log?.monthly_event_report?.scheduler?.day_of_month,
              config.value?.handover_log?.monthly_event_report?.scheduler?.run_time,
            ),
            nextRunText: normalizeSchedulerDateText(health.monthly_event_report?.scheduler?.next_run_time),
            lastTriggerText: normalizeSchedulerDateText(health.monthly_event_report?.scheduler?.last_trigger_at, "暂无记录"),
            resultText: monthlyEventReportSchedulerTriggerText.value || "暂无记录",
          },
        ],
      },
      {
        key: "monthly_change_report",
        title: "体系月度统计表-变更",
        moduleId: "monthly_event_report",
        focusKey: "monthly_change",
        ...normalizeSchedulerStatusVm({
          running: health.monthly_change_report?.scheduler?.running,
          configured: Boolean(
            Number.parseInt(String(config.value?.handover_log?.monthly_change_report?.scheduler?.day_of_month || 0), 10) > 0
              && String(config.value?.handover_log?.monthly_change_report?.scheduler?.run_time || "").trim(),
          ),
          status: health.monthly_change_report?.scheduler?.status,
          lastTriggerResult: health.monthly_change_report?.scheduler?.last_trigger_result,
          lastDecision: health.monthly_change_report?.scheduler?.last_decision,
        }),
        summaryText: monthlyChangeReportSchedulerDecisionText.value || monthlyChangeReportSchedulerTriggerText.value || "固定读取上一个自然月变更数据",
        parts: [
          {
            label: "变更月报",
            runTimeText: buildMonthlySchedulerRunText(
              config.value?.handover_log?.monthly_change_report?.scheduler?.day_of_month,
              config.value?.handover_log?.monthly_change_report?.scheduler?.run_time,
            ),
            nextRunText: normalizeSchedulerDateText(health.monthly_change_report?.scheduler?.next_run_time),
            lastTriggerText: normalizeSchedulerDateText(health.monthly_change_report?.scheduler?.last_trigger_at, "暂无记录"),
            resultText: monthlyChangeReportSchedulerTriggerText.value || "暂无记录",
          },
        ],
      },
      {
        key: "alarm_event_upload",
        title: "告警信息上传",
        moduleId: "alarm_event_upload",
        focusKey: "",
        ...normalizeSchedulerStatusVm({
          running: health.alarm_event_upload?.scheduler?.running,
          configured: Boolean(String(config.value?.alarm_export?.scheduler?.run_time || "").trim()),
          status: health.alarm_event_upload?.scheduler?.status,
          lastTriggerResult: health.alarm_event_upload?.scheduler?.last_trigger_result,
          lastDecision: health.alarm_event_upload?.scheduler?.last_decision,
        }),
        summaryText: alarmEventUploadSchedulerDecisionText.value || alarmEventUploadSchedulerTriggerText.value || "固定执行全部楼栋 60 天上传",
        parts: [
          {
            label: "每日调度",
            runTimeText: normalizeSchedulerText(config.value?.alarm_export?.scheduler?.run_time, "未设置"),
            nextRunText: normalizeSchedulerDateText(health.alarm_event_upload?.scheduler?.next_run_time),
            lastTriggerText: normalizeSchedulerDateText(health.alarm_event_upload?.scheduler?.last_trigger_at, "暂无记录"),
            resultText: alarmEventUploadSchedulerTriggerText.value || "暂无记录",
          },
        ],
      },
    ];
  });
  const schedulerOverviewSummary = computed(() => {
    const items = schedulerOverviewItems.value || [];
    const runningCount = items.filter((item) => item.statusText === "已启动").length;
    const stoppedCount = Math.max(0, items.length - runningCount);
    const attentionItems = items.filter((item) => ["warning", "danger"].includes(String(item?.tone || "").trim()));
    const upcomingCandidates = items
      .flatMap((item) =>
        (Array.isArray(item?.parts) ? item.parts : []).map((part) => ({
          title: item.title,
          label: part.label,
          nextRunText: part.nextRunText,
        })),
      )
      .filter((part) => toComparableSchedulerDateText(part.nextRunText));
    upcomingCandidates.sort((left, right) => compareSchedulerDateText(left.nextRunText, right.nextRunText));
    const nextItem = upcomingCandidates[0] || null;
    const attentionItem = attentionItems[0] || null;
    return {
      runningCount,
      stoppedCount,
      attentionCount: attentionItems.length,
      statusText: attentionItem ? "有待关注项" : runningCount > 0 ? "状态正常" : "全部未启动",
      tone: attentionItem ? attentionItem.tone : runningCount > 0 ? "success" : "neutral",
      nextSchedulerLabel: nextItem ? nextItem.title : "暂无安排",
      nextSchedulerText: nextItem
        ? `${nextItem.title}${nextItem.label ? ` · ${nextItem.label}` : ""} / ${nextItem.nextRunText}`
        : "当前没有已安排的调度",
      attentionText: attentionItem ? `${attentionItem.title}：${attentionItem.summaryText || attentionItem.statusText}` : "当前没有待关注调度",
      summaryText: attentionItem ? "请先查看待关注调度，再进入对应模块处理。" : "这里集中查看全部调度状态，需要调整时进入对应模块操作。",
    };
  });
  const handoverReviewOverview = computed(() => {
    const review = health.handover?.review_status || {};
    const required = Number(review.required_count || 0);
    const confirmed = Number(review.confirmed_count || 0);
    const pending = Math.max(0, required - confirmed);
    const dutyDate = String(review.duty_date || "").trim();
    const dutyShift = String(review.duty_shift || "").trim().toLowerCase();
    const dutyShiftText = shiftTextFromCode(dutyShift);
    return {
      batchKey: String(review.batch_key || "").trim(),
      dutyDate,
      dutyShift,
      dutyText: dutyDate && dutyShift ? `${dutyDate} / ${dutyShiftText}` : "",
      hasAnySession: Boolean(review.has_any_session),
      required,
      confirmed,
      pending,
      allConfirmed: Boolean(review.all_confirmed),
      readyForFollowupUpload: Boolean(review.ready_for_followup_upload),
      tone: !review.has_any_session ? "neutral" : review.all_confirmed ? "success" : pending > 0 ? "warning" : "neutral",
      summaryText: !review.has_any_session ? "当前批次未生成" : review.all_confirmed ? "5楼已全部确认" : `还有 ${pending} 个楼待确认`,
    };
  });
  const handoverFollowupProgress = computed(() => {
    const raw = health.handover?.review_status?.followup_progress || {};
    return {
      status: String(raw.status || "").trim().toLowerCase() || "idle",
      canResumeFollowup: Boolean(raw.can_resume_followup),
      pendingCount: Number.parseInt(String(raw.pending_count || 0), 10) || 0,
      failedCount: Number.parseInt(String(raw.failed_count || 0), 10) || 0,
      attachmentPendingCount: Number.parseInt(String(raw.attachment_pending_count || 0), 10) || 0,
      cloudPendingCount: Number.parseInt(String(raw.cloud_pending_count || 0), 10) || 0,
      dailyReportStatus: String(raw.daily_report_status || "").trim().toLowerCase() || "idle",
    };
  });
  const handoverDailyReportAuthVm = computed(() =>
    mapDailyReportAuthVm(handoverDailyReportContext.value?.screenshot_auth || {}),
  );
  const handoverDailyReportExportVm = computed(() =>
    mapDailyReportExportVm(
      handoverDailyReportContext.value?.daily_report_record_export || {},
      handoverDailyReportContext.value?.screenshot_auth || {},
    ),
  );
  const handoverDailyReportSpreadsheetUrl = computed(() =>
    String(
      handoverDailyReportContext.value?.daily_report_record_export?.spreadsheet_url ||
        health.handover?.review_status?.cloud_sheet_sync?.spreadsheet_url ||
        "",
    ).trim(),
  );
  const handoverDailyReportCaptureAssets = computed(() => {
    const raw = handoverDailyReportContext.value?.capture_assets || {};
    const dutyDate = String(handoverDailyReportContext.value?.duty_date || "").trim();
    const dutyShift = String(handoverDailyReportContext.value?.duty_shift || "").trim().toLowerCase();
    const exportState = handoverDailyReportContext.value?.daily_report_record_export || {};
    return {
      summarySheetImage: normalizeDailyReportAssetCard(raw.summary_sheet_image, "今日航图截图", {
        dutyDate,
        dutyShift,
        lastWrittenSource: exportState.summary_screenshot_source_used,
      }),
      externalPageImage: normalizeDailyReportAssetCard(raw.external_page_image, "排班截图", {
        dutyDate,
        dutyShift,
        lastWrittenSource: exportState.external_screenshot_source_used,
      }),
    };
  });
  const handoverDailyReportSummaryTestVm = computed(() => {
    const currentBatchKey = String(handoverDailyReportContext.value?.batch_key || "").trim();
    const testState = handoverDailyReportLastScreenshotTest.value || {};
    const raw =
      String(testState.batch_key || "").trim() === currentBatchKey ? testState.summary_sheet_image || {} : {};
    return mapDailyReportScreenshotTestVm(raw, {
      fallbackExists: Boolean(handoverDailyReportCaptureAssets.value.summarySheetImage.exists),
      fallbackPath: String(handoverDailyReportCaptureAssets.value.summarySheetImage.stored_path || ""),
      fallbackCapturedAt: String(handoverDailyReportCaptureAssets.value.summarySheetImage.captured_at || ""),
      skippedText: "本次测试已跳过",
      browserLabel: getDailyReportBrowserLabel(handoverDailyReportContext.value?.screenshot_auth || {}),
    });
  });
  const handoverDailyReportExternalTestVm = computed(() => {
    const currentBatchKey = String(handoverDailyReportContext.value?.batch_key || "").trim();
    const testState = handoverDailyReportLastScreenshotTest.value || {};
    const raw =
      String(testState.batch_key || "").trim() === currentBatchKey ? testState.external_page_image || {} : {};
    return mapDailyReportScreenshotTestVm(raw, {
      fallbackExists: Boolean(handoverDailyReportCaptureAssets.value.externalPageImage.exists),
      fallbackPath: String(handoverDailyReportCaptureAssets.value.externalPageImage.stored_path || ""),
      fallbackCapturedAt: String(handoverDailyReportCaptureAssets.value.externalPageImage.captured_at || ""),
      skippedText: "本次测试已跳过",
      browserLabel: getDailyReportBrowserLabel(handoverDailyReportContext.value?.screenshot_auth || {}),
    });
  });
  const canRewriteHandoverDailyReportRecord = computed(() =>
    Boolean(handoverDailyReportSpreadsheetUrl.value) &&
    Boolean(handoverDailyReportCaptureAssets.value.summarySheetImage.exists) &&
    Boolean(handoverDailyReportCaptureAssets.value.externalPageImage.exists),
  );
  const handoverConfiguredBuildings = computed(() => {
    const rows = Array.isArray(config.value?.input?.buildings) ? config.value.input.buildings : [];
    const output = [];
    for (const item of rows) {
      const building = String(item || "").trim();
      if (building && !output.includes(building)) {
        output.push(building);
      }
    }
    return output;
  });
  const handoverSelectedBuildings = computed(() =>
    handoverConfiguredBuildings.value.filter((building) => Boolean(handoverFilesByBuilding[building])),
  );
  const handoverSelectedFileCount = computed(() => handoverSelectedBuildings.value.length);
  const hasSelectedHandoverFiles = computed(() => handoverSelectedFileCount.value > 0);
  const handoverFileStatesByBuilding = computed(() => {
    const states = {};
    for (const building of handoverConfiguredBuildings.value) {
      const file = handoverFilesByBuilding[building];
      const name = String(file?.name || "").trim();
      if (!name) {
        states[building] = {
          state: "empty",
          label: "未选择",
          filename: "",
          helper: "未选择文件时，该楼将跳过。",
        };
        continue;
      }
      states[building] = {
        state: "selected",
        label: "已选择",
        filename: basenameFromPath(name),
        helper: "该楼本次会参与“从已有数据表生成”。",
      };
    }
    return states;
  });
  const updaterResultText = computed(() => {
    const resultKey = String(health.updater?.last_result || "").trim().toLowerCase();
    const disabledReason = String(health.updater?.disabled_reason || "").trim().toLowerCase();
    if (resultKey === "disabled" && disabledReason === "source_python_run") {
      return "本地源码运行不更新";
    }
    return mapUpdaterResultText(resultKey);
  });
  const dashboardActiveModuleTitle = computed(() => {
    const hit = dashboardModules.value.find((item) => item.id === dashboardActiveModule.value);
    return hit?.title || "业务模块";
  });
  const moduleMeta = computed(() => {
    const next = {};
    for (const item of dashboardModules.value || []) {
      const id = String(item?.id || "").trim();
      if (!id) continue;
      next[id] = item;
    }
    return next;
  });
  const dashboardActiveModuleHero = computed(() => {
    const active = moduleMeta.value?.[dashboardActiveModule.value] || {};
    const dutyText = `${handoverDutyDate.value || "-"} / ${shiftTextFromCode(handoverDutyShift.value)}`;
    const map = {
      scheduler_overview: {
        eyebrow: "统一扫读",
        title: "调度总览",
        description: "集中查看全部调度是否已启动、何时执行，以及哪些调度需要进入对应模块处理。",
        metrics: [
          { label: "已启动调度", value: `${schedulerOverviewSummary.value.runningCount} 项` },
          { label: "未启动调度", value: `${schedulerOverviewSummary.value.stoppedCount} 项` },
          { label: "待关注项", value: `${schedulerOverviewSummary.value.attentionCount} 项` },
        ],
      },
      auto_flow: {
        eyebrow: "推荐主路径",
        title: "自动流程主控面板",
        description: "适合日常标准流程，先切内网下载，再切外网计算并上传。",
        metrics: [
          { label: "当前网络", value: health.network.current_ssid || "-" },
          { label: "调度状态", value: health.scheduler.status || "-" },
          { label: "待续传任务", value: String(pendingResumeCount.value) },
        ],
      },
      multi_date: {
        eyebrow: "批量补跑",
        title: "多日用电明细自动流程",
        description: "适合补跑连续日期，保持统一下载与上传流程。",
        metrics: [
          { label: "已选日期", value: `${selectedDateCount.value} 天` },
          { label: "待续传任务", value: String(pendingResumeCount.value) },
          { label: "当前网络", value: health.network.current_ssid || "-" },
        ],
      },
      manual_upload: {
        eyebrow: "仅外网上传",
        title: "手动补传",
        description: "不执行内网下载，直接使用手动选择的文件进行补传。",
        metrics: [
          { label: "楼栋", value: manualBuilding.value || "-" },
          { label: "上传日期", value: manualUploadDate.value || "-" },
          { label: "角色", value: "固定按当前角色执行" },
        ],
      },
      sheet_import: {
        eyebrow: "一次性导表",
        title: "5Sheet 导入",
        description: "清空目标表后重新导入 5 个工作表，用于手动修复或覆盖。",
        metrics: [
          { label: "角色", value: "固定按当前角色执行" },
          { label: "当前网络", value: health.network.current_ssid || "-" },
          { label: "状态", value: health.active_job_id ? "处理中" : "待命" },
        ],
      },
      handover_log: {
        eyebrow: "5楼审核联动",
        title: "交接班日志工作台",
        description: "围绕文件生成、楼栋审核与确认后续动作组织界面，突出主路径与文件状态。",
        metrics: [
          { label: "目标班次", value: dutyText },
          { label: "已选文件", value: `${handoverSelectedFileCount.value} 个楼` },
          { label: "审核概况", value: handoverReviewOverview.value.summaryText },
        ],
      },
      day_metric_upload: {
        eyebrow: "独立重写",
        title: "12项独立上传",
        description: "按日期下载或导入本地文件，单独提取并重写 12 项；不进入交接班审核链路。",
        metrics: [
          { label: "已选日期", value: `${dayMetricSelectedDateCount.value} 天` },
          { label: "楼栋范围", value: dayMetricUploadScope.value === "all_enabled" ? "全部启用楼栋" : (dayMetricUploadBuilding.value || "-") },
          { label: "执行模式", value: "按角色固定执行" },
        ],
      },
      wet_bulb_collection: {
        eyebrow: "独立采集",
        title: "湿球温度定时采集",
        description: "复用交接班规则引擎提取湿球温度和冷源模式，并按楼栋写入多维表。",
        metrics: [
          { label: "调度状态", value: health.wet_bulb_collection?.scheduler?.status || "-" },
          { label: "下次执行", value: health.wet_bulb_collection?.scheduler?.next_run_time || "-" },
        ],
      },
      monthly_event_report: {
        eyebrow: "月度本地生成",
        title: "体系月度统计表",
        description: "读取上一个自然月的事件与变更数据，按楼栋生成两类月度统计表并输出到本地目录。",
        metrics: [
          {
            label: "事件调度",
            value: health.monthly_event_report?.scheduler?.status || "-",
          },
          {
            label: "变更调度",
            value: health.monthly_change_report?.scheduler?.status || "-",
          },
          {
            label: "最近生成",
            value: `${(health.monthly_event_report?.last_run?.generated_files || 0) + (health.monthly_change_report?.last_run?.generated_files || 0)} 份`,
          },
        ],
      },
      alarm_event_upload: (() => {
        const families = Array.isArray(sharedSourceCacheReadinessOverview.value?.families)
          ? sharedSourceCacheReadinessOverview.value.families
          : [];
        const alarmFamily = families.find((item) => String(item?.key || "").trim() === "alarm_event_family") || {};
        return {
          eyebrow: "专项上传",
          title: "告警信息上传",
          description: "按楼读取当天最新一份告警文件，缺失则回退昨天最新，并将 60 天内记录写入目标多维表。",
          metrics: [
            { label: "最近上传", value: alarmFamily.uploadLastRunAt || "-" },
            { label: "上传记录", value: `${alarmFamily.uploadRecordCount || 0} 条` },
            { label: "参与文件", value: `${alarmFamily.uploadFileCount || 0} 份` },
          ],
        };
      })(),
    };
    return map[dashboardActiveModule.value] || {
      eyebrow: active.group_title || "业务模块",
      title: active.title || "业务模块",
      description: active.desc || active.group_title || "当前模块",
      metrics: [],
    };
  });
  const handoverRuleScopeOptions = computed(() => {
    const opts = [{ value: "default", label: "全局默认" }];
    const buildings = Array.isArray(config.value?.input?.buildings) ? config.value.input.buildings : [];
    for (const item of buildings) {
      const building = String(item || "").trim();
      if (!building) continue;
      opts.push({ value: building, label: `${building}覆盖` });
    }
    return opts;
  });

  function syncCustomWindowLocalInputs() {
    const dl = config.value?.download || {};
    customAbsoluteStartLocal.value = apiDatetimeToLocal(dl.start_time || "");
    customAbsoluteEndLocal.value = apiDatetimeToLocal(dl.end_time || "");
  }

  return {
    health,
    config,
    currentView,
    activeConfigTab,
    dashboardMenuGroups,
    dashboardModules,
    dashboardActiveModule,
    dashboardModuleMenuOpen,
    applyDashboardRoleMode,
    selectedDate,
    rangeStartDate,
    rangeEndDate,
    selectedDates,
    logs,
    logFilter,
    internalOpsLogs,
    currentJob,
    jobsList,
    selectedJobId,
    bridgeTasks,
    selectedBridgeTaskId,
    bridgeTaskDetail,
    resourceSnapshot,
    busy,
    message,
    bootstrapReady,
    fullHealthLoaded,
    configLoaded,
    healthLoadError,
    configLoadError,
    engineerDirectoryLoaded,
    pendingResumeRuns,
    schedulerQuickSaving,
    handoverSchedulerQuickSaving,
    wetBulbSchedulerQuickSaving,
    dayMetricUploadSchedulerQuickSaving,
    alarmEventUploadSchedulerQuickSaving,
    monthlyEventReportSchedulerQuickSaving,
    monthlyChangeReportSchedulerQuickSaving,
    configAutoSaveSuspendDepth,
    autoResumeState,
    buildingsText,
    sheetRuleRows,
    manualBuilding,
    manualFile,
    manualUploadDate,
    sheetFile,
    dayMetricUploadScope,
    dayMetricUploadBuilding,
    dayMetricSelectedDate,
    dayMetricRangeStartDate,
    dayMetricRangeEndDate,
    dayMetricSelectedDates,
    dayMetricLocalBuilding,
    dayMetricLocalDate,
    dayMetricLocalFile,
    handoverFile,
    handoverFilesByBuilding,
    handoverDutyDate,
    handoverDutyShift,
    handoverDownloadScope,
    handoverEngineerDirectory,
    handoverEngineerLoading,
    handoverDailyReportContext,
    handoverDailyReportLastScreenshotTest,
    handoverDailyReportPreviewModal,
    handoverDailyReportUploadModal,
    handoverRuleScope,
    handoverDutyAutoFollow,
    handoverDutyLastAutoAt,
    customAbsoluteStartLocal,
    customAbsoluteEndLocal,
    systemLogOffset,
    timers,
    streamController,
    filteredLogs,
    canRun,
    isStatusView,
    isDashboardView,
    isConfigView,
    initialLoadingPhase,
    initialLoadingStatusText,
    selectedDateCount,
    dayMetricSelectedDateCount,
    pendingResumeCount,
    dayMetricCurrentPayload,
    dayMetricCurrentResultRows,
    dayMetricRetryableRows,
    dayMetricRetryableFailedCount,
    dayMetricRetryAllMode,
    handoverGenerationBusy,
    runningJobs,
    waitingResourceJobs,
    recentFinishedJobs,
    bridgeTasksEnabled,
    isInternalRole,
    activeBridgeTasks,
    displayedBridgeTasks,
    totalBridgeHistoryCount,
    hiddenBridgeHistoryCount,
    bridgeTaskHistoryDisplayLimit: BRIDGE_HISTORY_DISPLAY_LIMIT,
    recentFinishedBridgeTasks,
    currentBridgeTask,
    handoverDutyAutoLabel,
    schedulerDecisionText,
    schedulerTriggerText,
    wetBulbSchedulerDecisionText,
    wetBulbSchedulerTriggerText,
    dayMetricUploadSchedulerDecisionText,
    dayMetricUploadSchedulerTriggerText,
    alarmEventUploadSchedulerDecisionText,
    alarmEventUploadSchedulerTriggerText,
    monthlyEventReportSchedulerDecisionText,
    monthlyEventReportSchedulerTriggerText,
    monthlyChangeReportSchedulerDecisionText,
    monthlyChangeReportSchedulerTriggerText,
    handoverMorningDecisionText,
    handoverAfternoonDecisionText,
      handoverReviewStatusItems,
    handoverReviewLinks,
    handoverReviewMatrix,
    handoverReviewBoardRows,
    dashboardSystemStatusItems,
    schedulerOverviewItems,
    schedulerOverviewSummary,
    internalDownloadPoolOverview,
    internalSourceCacheOverview,
    internalRealtimeSourceFamilies,
    externalInternalAlertOverview,
    currentHourRefreshOverview,
    internalRuntimeOverview,
    internalSourceCacheHistoryOverview,
    sharedSourceCacheReadinessOverview,
    updaterMirrorOverview,
    dashboardScheduleStatusItems,
    handoverReviewOverview,
    handoverFollowupProgress,
    handoverDailyReportAuthVm,
    handoverDailyReportExportVm,
    handoverDailyReportSpreadsheetUrl,
    handoverDailyReportCaptureAssets,
    handoverDailyReportSummaryTestVm,
    handoverDailyReportExternalTestVm,
    canRewriteHandoverDailyReportRecord,
    handoverConfiguredBuildings,
    handoverSelectedBuildings,
    handoverSelectedFileCount,
    hasSelectedHandoverFiles,
    handoverFileStatesByBuilding,
    updaterResultText,
    dashboardActiveModuleTitle,
    moduleMeta,
    dashboardActiveModuleHero,
    handoverRuleScopeOptions,
    syncCustomWindowLocalInputs,
    actionGuard,
  };
}



