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
    : (modules.some((item) => item.id === defaultId) ? defaultId : (modules[0]?.id || "runtime_logs"));
  return { menuGroups, modules, activeModule };
}

const DASHBOARD_MODULE_STORAGE_KEY = "dashboard_active_module";

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

function mapDailyReportAuthVm(raw) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const error = String(raw?.error || "").trim();
  const browserLabel = getDailyReportBrowserLabel(raw);
  if (status === "ready") {
    return { text: "已登录", tone: "success", error: "" };
  }
  if (status === "missing_login") {
    if (error === "browser_not_started") {
      return {
        text: "待登录",
        tone: "warning",
        error: `${browserLabel} 登录页尚未打开，请点击“初始化飞书截图登录态”。`,
      };
    }
    if (error === "browser_started_without_pages" || error === "feishu_page_not_open") {
      return {
        text: "待登录",
        tone: "warning",
        error: `${browserLabel} 登录页已被关闭，请点击“初始化飞书截图登录态”重新打开。`,
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
  if (text === "auto_switch") return "单机切网流程";
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
          day_metric_pending_count: 0,
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
          scope_text: "当前小时",
        },
        handover_log_family: {
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
      },
    },
    network: { current_ssid: "-" },
    updater: {
      enabled: true,
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
  const dayMetricUploadEnabled = computed(() => Boolean(config.value?.day_metric_upload?.enabled));
  const dayMetricLocalImportEnabled = computed(() => Boolean(config.value?.day_metric_upload?.behavior?.local_import_enabled));
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
  const displayedBridgeTasks = computed(() => {
    const historyTasks = bridgeTasks.value
      .filter((item) => isBridgeTerminalStatus(item?.status))
      .slice(0, BRIDGE_HISTORY_DISPLAY_LIMIT);
    return [...activeBridgeTasks.value, ...historyTasks];
  });
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
    const lastUsedAt = String(slot?.last_used_at || "").trim();
    const lastLoginAt = String(slot?.last_login_at || "").trim();
    const lastResult = String(slot?.last_result || "").trim().toLowerCase();
    const lastError = String(slot?.last_error || "").trim();
    const loginError = String(slot?.login_error || "").trim();
    const loginState = String(slot?.login_state || "").trim().toLowerCase();
    let tone = "neutral";
    let stateText = "未建页";
    let loginTone = "warning";
    let loginText = "待登录";
    if (pageReady) {
      tone = "success";
      stateText = "待命";
    }
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
    if (loginState === "ready") {
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
    return {
      building,
      pageReady,
      inUse,
      lastUsedAt,
      lastLoginAt,
      lastResult,
      lastError,
      loginState,
      loginTone,
      loginText,
      loginError,
      tone,
      stateText,
      detailText: loginError || lastError || lastLoginAt || lastUsedAt || (pageReady ? "页签已就绪，等待下载任务" : "页签尚未初始化"),
    };
  }
  function normalizeSourceCacheBuildingStatus(raw, fallbackBucket) {
    const building = String(raw?.building || "").trim() || "-";
    const bucketKey = String(raw?.bucket_key || "").trim() || String(fallbackBucket || "").trim() || "-";
    const downloadedAt = String(raw?.downloaded_at || "").trim();
    const lastError = formatSharedBridgeRuntimeError(raw?.last_error);
    const relativePath = String(raw?.relative_path || "").trim();
    const resolvedFilePath = String(raw?.resolved_file_path || "").trim();
    const rawStatus = String(raw?.status || "").trim().toLowerCase();
    const statusKey = ["ready", "failed", "downloading"].includes(rawStatus) ? rawStatus : "waiting";
    const ready = statusKey === "ready" && Boolean(raw?.ready);
    let tone = "warning";
    let stateText = "等待中";
    if (statusKey === "ready" && ready) {
      tone = "success";
      stateText = "已就绪";
    } else if (statusKey === "downloading") {
      tone = "info";
      stateText = "下载中";
    } else if (statusKey === "failed") {
      tone = "danger";
      stateText = "失败";
    }
    return {
      building,
      bucketKey,
      statusKey,
      ready,
      downloadedAt,
      lastError,
      relativePath,
      resolvedFilePath,
      tone,
      stateText,
      detailText: lastError || downloadedAt || (resolvedFilePath ? resolvedFilePath : "等待共享文件就绪"),
    };
  }
  function normalizeSourceCacheFamilyOverview({ key, title, payload, fallbackBucket }) {
    const familyPayload = payload && typeof payload === "object" ? payload : {};
    const readyCount = Number.parseInt(String(familyPayload.ready_count || 0), 10) || 0;
    const failedBuildings = Array.isArray(familyPayload.failed_buildings)
      ? familyPayload.failed_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const buildings = Array.isArray(familyPayload.buildings)
      ? familyPayload.buildings.map((item) => normalizeSourceCacheBuildingStatus(item, fallbackBucket))
      : [];
    const currentBucket = String(familyPayload.current_bucket || "").trim() || String(fallbackBucket || "").trim();
    const lastSuccessAt = String(familyPayload.last_success_at || "").trim();
    const hasFailures = buildings.length
      ? buildings.some((item) => item.statusKey === "failed")
      : failedBuildings.length > 0;
    const allReady = buildings.length
      ? buildings.every((item) => item.statusKey === "ready")
      : readyCount > 0 && !hasFailures;
    const tone = hasFailures ? "danger" : allReady ? "success" : "warning";
    const statusText = hasFailures
      ? "本小时存在失败楼栋"
      : allReady
        ? "本小时全部就绪"
        : "本小时仍有楼栋等待中";
    return {
      key,
      title,
      readyCount,
      failedBuildings,
      lastSuccessAt,
      currentBucket,
      buildings,
      hasFailures,
      allReady,
      tone,
      statusText,
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
  const internalDownloadPoolOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    const rawPool = health.shared_bridge?.internal_download_pool || {};
    const enabled = Boolean(rawPool.enabled);
    const browserReady = Boolean(rawPool.browser_ready);
    const lastError = formatSharedBridgeRuntimeError(rawPool.last_error);
    const activeBuildings = Array.isArray(rawPool.active_buildings)
      ? rawPool.active_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const slots = Array.isArray(rawPool.page_slots)
      ? rawPool.page_slots.map((slot) => normalizeInternalDownloadPoolSlot(slot))
      : [];
    const readyLoginCount = slots.filter((slot) => slot.loginState === "ready").length;
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
          ? "5 个固定楼栋浏览器已打开并登录成功，下载前会先刷新，只有登录失效时才重新登录。"
          : `5 个固定楼栋浏览器已打开，已登录 ${readyLoginCount}/${slots.length || 5}。`;
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
    const monthlyFamily = rawCache.monthly_report_family && typeof rawCache.monthly_report_family === "object"
      ? rawCache.monthly_report_family
      : rawCache.monthly_family && typeof rawCache.monthly_family === "object"
        ? rawCache.monthly_family
        : {};
    const families = [
      normalizeSourceCacheFamilyOverview({
        key: "handover_log_family",
        title: "交接班日志源文件",
        payload: handoverFamily,
        fallbackBucket: currentHourBucket,
      }),
      normalizeSourceCacheFamilyOverview({
        key: "monthly_report_family",
        title: "全景平台月报源文件",
        payload: monthlyFamily,
        fallbackBucket: currentHourBucket,
      }),
    ];
    if (roleMode !== "internal") {
      return {
        tone: "neutral",
        statusText: "仅内网端启用",
        summaryText: "小时预下载缓存仓只在内网端运行，外网端默认只消费共享目录中的最新有效文件。",
        currentHourBucket: currentHourBucket || "-",
        lastRunAt: lastRunAt || "",
        lastSuccessAt: lastSuccessAt || "",
        errorText: "",
        cacheRoot,
        items: [],
        families: [],
      };
    }
    let tone = "warning";
    let statusText = "准备中";
    let summaryText = "内网端会按自然小时为每个楼预下载两类源文件：交接班日志源文件和全景平台月报源文件。";
    if (!enabled) {
      tone = "warning";
      statusText = "未启用";
      summaryText = "当前未启用小时预下载缓存仓。";
    } else if (families.some((family) => family.hasFailures) || lastError) {
      tone = "danger";
      statusText = "最近一轮存在失败";
      summaryText = "最近一轮小时预下载存在失败楼栋，请检查共享目录权限和内网页面登录状态。";
    } else if (families.every((family) => family.allReady && family.buildings.length > 0)) {
      tone = "success";
      statusText = "本小时缓存已全部就绪";
      summaryText = "两个源文件族当前小时的楼栋缓存都已就绪，外网会直接消费共享目录中的最新文件。";
    } else if (running) {
      tone = "warning";
      statusText = "运行中";
      summaryText = "当前小时缓存仓正在维护 latest 桶，外网默认只消费本小时最新的两类源文件。";
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
    if (roleMode !== "internal") {
      return {
        tone: "neutral",
        statusText: "当前角色未启用",
        summaryText: "",
        lastRunAt: "",
        lastSuccessAt: "",
        lastError: "",
        failedBuildings: [],
      };
    }
    if (running) {
      return {
        tone: "warning",
        statusText: "当前小时下载中",
        summaryText: "正在立即补下当前小时的交接班日志源文件和全景平台月报源文件。",
        lastRunAt,
        lastSuccessAt,
        lastError,
        failedBuildings,
      };
    }
    if (failedBuildings.length || lastError) {
      return {
        tone: "danger",
        statusText: "最近一轮存在失败",
        summaryText: "当前小时下载最近一轮存在失败项，请检查对应楼栋的登录态、共享目录权限和下载页面可用性。",
        lastRunAt,
        lastSuccessAt,
        lastError,
        failedBuildings,
      };
    }
    if (lastSuccessAt) {
      return {
        tone: "success",
        statusText: "最近一轮已完成",
        summaryText: "当前小时的双源文件已完成一轮立即补下。",
        lastRunAt,
        lastSuccessAt,
        lastError: "",
        failedBuildings: [],
      };
    }
    return {
      tone: "neutral",
      statusText: "尚未手动执行",
      summaryText: "可手动触发“立即下载当前小时全部文件”，补下当前小时 10 个文件。",
      lastRunAt,
      lastSuccessAt,
      lastError,
      failedBuildings,
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
        families: [],
        recentLogs: [],
      };
    }
    const sourceCache = internalSourceCacheOverview.value;
    const currentHourRefresh = currentHourRefreshOverview.value;
    const families = Array.isArray(sourceCache.families)
      ? sourceCache.families.map((family) => ({
        key: family.key,
        title: family.title,
        tone: family.tone,
        statusText: family.statusText,
        currentBucket: family.currentBucket || sourceCache.currentHourBucket || "-",
        lastSuccessAt: family.lastSuccessAt || "-",
        failedSummary: Array.isArray(family.failedBuildings) && family.failedBuildings.length
          ? family.failedBuildings.join(" / ")
          : "",
        readyCountText: `${Number(family.readyCount || 0)} / ${Array.isArray(family.buildings) ? family.buildings.length : 0} 个楼已就绪`,
      }))
      : [];
    const recentLogs = internalOpsLogs.value
      .filter((line) => line.includes("[共享缓存]"))
      .slice(0, 8);
    let tone = sourceCache.tone || "neutral";
    let statusText = "按小时维护中";
    let summaryText = "内网端会按自然小时持续维护双源文件缓存，并保留最近一次当前小时手动补下结果。";
    if (currentHourRefresh.tone === "danger" || sourceCache.tone === "danger") {
      tone = "danger";
      statusText = "最近存在失败";
      summaryText = "最近一轮小时下载或当前小时手动补下存在失败，请检查对应楼栋登录态、共享目录权限和下载页面可用性。";
    } else if (sourceCache.tone === "success") {
      tone = "success";
      statusText = "本小时缓存已就绪";
      summaryText = "当前小时的交接班日志源文件和全景平台月报源文件都已就绪，可供外网直接消费。";
    } else if (currentHourRefresh.tone === "warning") {
      tone = "warning";
      statusText = "当前小时下载中";
      summaryText = currentHourRefresh.summaryText || summaryText;
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
      ],
      families,
      recentLogs,
      lastError: currentHourRefresh.lastError || sourceCache.errorText || "",
    };
  });
  const sharedSourceCacheReadinessOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    const rawCache = health.shared_bridge?.internal_source_cache || {};
    const lastError = formatSharedBridgeRuntimeError(rawCache.last_error);
    const families = [
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
    if (roleMode !== "external") {
      return {
        tone: "neutral",
        statusText: "当前角色未使用共享缓存",
        summaryText: "",
        referenceBucketKey: "-",
        errorText: "",
        families: [],
        canProceedLatest: false,
        autoRetrySignature: "",
        familyCanProceed: {},
        familyRetrySignatures: {},
      };
    }
    const hasStale = families.some((family) => family.staleBuildings.length > 0);
    const hasTooOld = families.some((family) => family.isBestBucketTooOld);
    const hasMissing = families.some((family) => family.missingBuildings.length > 0);
    const hasFallback = families.some((family) => family.fallbackBuildings.length > 0);
    const allReady = families.every((family) => family.canProceed && family.buildings.length > 0);
    const referenceBucketKey = families
      .map((family) => family.bestBucketKey)
      .filter(Boolean)
      .sort()
      .slice(-1)[0] || "-";
    const autoRetrySignature = families.map((family) => [
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
      families.map((family) => [family.key, Boolean(family.canProceed)]),
    );
    const familyRetrySignatures = Object.fromEntries(
      families.map((family) => [
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
    return {
      tone: hasTooOld || hasStale ? "danger" : allReady ? "success" : "warning",
      statusText: hasTooOld ? "等待最新共享文件更新" : hasStale ? "等待共享文件就绪" : allReady ? "最新共享文件已就绪" : "等待共享文件就绪",
      summaryText: hasTooOld
        ? "当前最新共享文件整体已超过 3 小时，等待内网更新后会自动重试默认入口。"
        : hasStale
        ? "部分楼栋共享文件版本过旧，等待更新后会自动重试默认入口。"
        : hasMissing
          ? "部分楼栋共享文件缺失，等待补齐后会自动重试默认入口。"
          : hasFallback
            ? "当前允许部分楼栋回退到不超过 3 桶的上一版共享文件。"
            : "外网默认入口会读取共享目录中最新可用的双源文件。",
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
    const sourceKind = String(updater.source_kind || "").trim().toLowerCase();
    const sourceLabel = formatUpdaterSourceLabel(updater);
    const mirrorReady = Boolean(updater.mirror_ready);
    const mirrorVersion = String(updater.mirror_version || "").trim();
    const localVersion = String(updater.local_version || "").trim() || "-";
    const localRevision = Number.parseInt(String(updater.local_release_revision || 0), 10) || 0;
    const lastPublishAt = String(updater.last_publish_at || "").trim();
    const manifestPath = String(updater.mirror_manifest_path || "").trim();
    const errorText = String(updater.last_publish_error || "").trim();
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
  const handoverMorningDecisionText = computed(() =>
    mapSchedulerDecisionText(health.handover_scheduler?.morning?.last_decision),
  );
  const handoverAfternoonDecisionText = computed(() =>
    mapSchedulerDecisionText(health.handover_scheduler?.afternoon?.last_decision),
  );
  const handoverReviewStatusItems = computed(() => {
    const rows = Array.isArray(health.handover?.review_status?.buildings)
      ? health.handover.review_status.buildings
      : [];
    return rows.map((row) => {
      const building = String(row?.building || "").trim();
      if (!building) return null;
      let label = "未确认";
      if (!row?.has_session) {
        label = "未生成";
      } else if (row?.confirmed) {
        label = "已确认";
      }
      return `${building} ${label}`;
    }).filter(Boolean);
  });
  const handoverReviewLinks = computed(() => {
    const rows = Array.isArray(health.handover?.review_links) ? health.handover.review_links : [];
    return rows
      .map((row) => {
        const building = String(row?.building || "").trim();
        const url = String(row?.url || "").trim();
        if (!building || !url) return null;
        return {
          building,
          code: String(row?.code || "").trim().toLowerCase(),
          url,
        };
      })
      .filter(Boolean);
  });
  const handoverReviewMatrix = computed(() => {
    const rows = Array.isArray(health.handover?.review_status?.buildings)
      ? health.handover.review_status.buildings
      : [];
    const links = Array.isArray(handoverReviewLinks.value) ? handoverReviewLinks.value : [];
    const orderedBuildings = [];
    rows.forEach((row) => {
      const building = String(row?.building || "").trim();
      if (building && !orderedBuildings.includes(building)) orderedBuildings.push(building);
    });
    links.forEach((row) => {
      const building = String(row?.building || "").trim();
      if (building && !orderedBuildings.includes(building)) orderedBuildings.push(building);
    });
    return orderedBuildings.map((building) => {
      const statusRow = rows.find((row) => String(row?.building || "").trim() === building) || null;
      const link = links.find((row) => row.building === building) || null;
      if (!statusRow) {
        return {
          building,
          status: "reachable",
          text: "可访问",
          tone: "info",
          url: link?.url || "",
        };
      }
      if (!statusRow?.has_session) {
        return {
          building,
          status: "missing",
          text: "未生成",
          tone: "neutral",
          url: link?.url || "",
        };
      }
      if (statusRow?.confirmed) {
        return {
          building,
          status: "confirmed",
          text: "已确认",
          tone: "success",
          url: link?.url || "",
        };
      }
      return {
        building,
        status: "pending",
        text: "待确认",
        tone: "warning",
        url: link?.url || "",
      };
    });
  });
  const handoverReviewBoardRows = computed(() => {
    return handoverReviewMatrix.value.map((item) => {
      const link = (handoverReviewLinks.value || []).find((row) => row.building === item.building);
      const reviewRow =
        (Array.isArray(health.handover?.review_status?.buildings) ? health.handover.review_status.buildings : []).find(
          (row) => String(row?.building || "").trim() === item.building,
        ) || {};
      const cloudSheetSyncVm = mapCloudSheetSyncVm(reviewRow?.cloud_sheet_sync || {});
      return {
        ...item,
        url: link?.url || "",
        hasUrl: Boolean(String(link?.url || "").trim()),
        cloudSheetSyncText: cloudSheetSyncVm.text,
        cloudSheetSyncTone: cloudSheetSyncVm.tone,
        cloudSheetUrl: cloudSheetSyncVm.url,
        hasCloudSheetUrl: Boolean(cloudSheetSyncVm.url),
        cloudSheetError: cloudSheetSyncVm.error,
      };
    });
  });
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
      label: "湿球采集下次",
      value: health.wet_bulb_collection.scheduler.next_run_time || "-",
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
  ]);
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
      dayMetricPendingCount: Number.parseInt(String(raw.day_metric_pending_count || 0), 10) || 0,
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
  const updaterResultText = computed(() => mapUpdaterResultText(health.updater?.last_result));
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
        title: "多日期自动流程",
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
      runtime_logs: {
        eyebrow: "运行追踪",
        title: "运行日志与任务状态",
        description: "观察任务执行、错误和断点续传情况，作为问题定位的主视图。",
        metrics: [
          { label: "任务状态", value: currentJob.value?.status || "-" },
          { label: "任务编号", value: currentJob.value?.job_id || "-" },
          { label: "日志条数", value: String(logs.value.length) },
        ],
      },
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
    dayMetricUploadEnabled,
    dayMetricLocalImportEnabled,
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
    handoverMorningDecisionText,
    handoverAfternoonDecisionText,
      handoverReviewStatusItems,
    handoverReviewLinks,
    handoverReviewMatrix,
    handoverReviewBoardRows,
    dashboardSystemStatusItems,
    internalDownloadPoolOverview,
    internalSourceCacheOverview,
    currentHourRefreshOverview,
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


