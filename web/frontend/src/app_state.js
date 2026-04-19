import { apiDatetimeToLocal, ensureConfigShape, todayText } from "./config_helpers.js";
import { createActionGuard } from "./action_guard.js";
import { getDashboardMenuGroupsForRole } from "./dashboard_menu_config.js";
import {
  buildSourceCachePlaceholderBuilding,
  mapBackendActionListById,
  mapBackendActionState,
  mapBackendActionsState,
  normalizeBackendTaskItem,
} from "./backend_action_display_helpers.js";
import {
  mapPresentedInternalAlertOverview,
  mapPresentedSourceCacheFamilyOverview,
  normalizeInternalDownloadPoolSlot,
} from "./source_cache_display_helpers.js";
import {
  createEmptyBridgeTasksDisplay,
  createEmptyJobPanelDisplay,
  createEmptyOverviewCard,
  createNeutralMonthlyReportDeliveryRow,
  mapBackendOverviewCard,
  mapBackendSchedulerOverviewItem,
  mapBackendSchedulerOverviewSummary,
  mapBackendUpdaterMirrorOverview,
  mapCurrentHourRefreshOverview,
  normalizeBridgeTasksDisplayPayload,
  normalizeHandoverReviewOverview,
  normalizeJobPanelDisplayPayload,
  normalizeMonthlyReportDeliveryLastRun,
  normalizeMonthlyReportDeliveryOverview,
  normalizeMonthlyReportDeliveryRow,
  normalizeMonthlyReportLastRunDisplay,
  resolveBackendOverviewCard,
} from "./dashboard_overview_display_helpers.js";
import {
  emptyDailyReportAssetVariant,
  formatInternalDownloadPoolError,
  formatSharedBridgeRuntimeError,
  getDailyReportBrowserLabel,
  mapBackendDailyReportAssetCard,
  mapDailyReportScreenshotTestVm,
  normalizeDailyReportAssetCard,
} from "./daily_report_display_helpers.js";

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

function isAbortLikeText(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) return false;
  return text.includes("aborterror") || text.includes("signal is aborted") || text === "abort";
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
const INTERNAL_SOURCE_CACHE_FAMILY_KEYS = Object.freeze([
  "handover_log_family",
  "handover_capacity_report_family",
  "monthly_report_family",
  "alarm_event_family",
]);
const STATE_DIAGNOSTIC_SEEN = new Set();

function warnStateDiagnostic(kind, detail) {
  const signature = `${String(kind || "").trim()}::${String(detail || "").trim()}`;
  if (!signature || STATE_DIAGNOSTIC_SEEN.has(signature)) return;
  if (STATE_DIAGNOSTIC_SEEN.size >= 200) {
    STATE_DIAGNOSTIC_SEEN.clear();
  }
  STATE_DIAGNOSTIC_SEEN.add(signature);
  if (typeof console !== "undefined" && typeof console.warn === "function") {
    console.warn(`[状态诊断] ${detail}`);
  }
}

export function createEmptyInternalBuildingRuntimeStatusMap() {
  return Object.fromEntries(
    INTERNAL_BUILDINGS.map((building) => [
      building,
      {
        updated_at: "",
        building,
        building_code: building.replace("楼", "").toLowerCase(),
        page_slot: { building },
        source_families: {
          handover_log_family: buildSourceCachePlaceholderBuilding(building, ""),
          handover_capacity_report_family: buildSourceCachePlaceholderBuilding(building, ""),
          monthly_report_family: buildSourceCachePlaceholderBuilding(building, ""),
          alarm_event_family: buildSourceCachePlaceholderBuilding(building, ""),
        },
        pool: {
          browser_ready: false,
          last_error: "",
        },
      },
    ]),
  );
}

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

function readSchedulerDisplayText(scheduler, field, fallback = "-") {
  const display = scheduler && typeof scheduler.display === "object" ? scheduler.display : {};
  const text = String(display?.[field] || "").trim();
  return text || fallback;
}

function hasSuccessfulCurrentHourRefreshSnapshot(raw) {
  const payload = raw && typeof raw === "object" ? raw : {};
  const failedBuildings = Array.isArray(payload.failed_buildings) ? payload.failed_buildings : [];
  const blockedBuildings = Array.isArray(payload.blocked_buildings) ? payload.blocked_buildings : [];
  const runningBuildings = Array.isArray(payload.running_buildings) ? payload.running_buildings : [];
  const completedBuildings = Array.isArray(payload.completed_buildings) ? payload.completed_buildings : [];
  return (
    !Boolean(payload.running)
    && failedBuildings.length === 0
    && blockedBuildings.length === 0
    && runningBuildings.length === 0
    && completedBuildings.length > 0
  );
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
    startup_role_user_exited: false,
    startup_handoff: {
      active: false,
      mode: "",
      target_role_mode: "",
      requested_at: "",
      reason: "",
      nonce: "",
    },
    startup_shared_bridge: {
      enabled: false,
      root_dir: "",
      internal_root_dir: "",
      external_root_dir: "",
      poll_interval_sec: 2,
      heartbeat_interval_sec: 5,
      claim_lease_sec: 30,
      stale_task_timeout_sec: 1800,
      artifact_retention_days: 7,
      sqlite_busy_timeout_ms: 15000,
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
      remembered_enabled: false,
      effective_auto_start_in_gui: false,
      memory_source: "",
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
      remembered_enabled: false,
      effective_auto_start_in_gui: false,
      memory_source: "",
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
      review_recipient_status_by_building: [],
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
        remembered_enabled: false,
        effective_auto_start_in_gui: false,
        memory_source: "",
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
        remembered_enabled: false,
        effective_auto_start_in_gui: false,
        memory_source: "",
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
        remembered_enabled: false,
        effective_auto_start_in_gui: false,
        memory_source: "",
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
        remembered_enabled: false,
        effective_auto_start_in_gui: false,
        memory_source: "",
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
        remembered_enabled: false,
        effective_auto_start_in_gui: false,
        memory_source: "",
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
    shared_root_diagnostic: {
      role_mode: "",
      role_label: "",
      status: "",
      status_text: "",
      tone: "neutral",
      summary_text: "",
      source_kind: "",
      items: [],
      paths: [],
      notes: [],
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
      background_task_count: 0,
      background_running_count: 0,
      background_tasks: [],
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
      internal_peer: {
        available: false,
        online: false,
        heartbeat_at: "",
        node_id: "",
        node_label: "",
        local_version: "",
        local_release_revision: 0,
        last_check_at: "",
        last_result: "",
        last_error: "",
        update_available: false,
        restart_required: false,
        command: {
          exists: false,
          command_id: "",
          action: "",
          status: "",
          requested_at: "",
          consumed_at: "",
          finished_at: "",
          message: "",
          active: false,
        },
      },
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
  const bridgeTasksDisplay = ref(createEmptyBridgeTasksDisplay());
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
  const internalRuntimeSummary = ref(null);
  const internalBuildingRuntimeStatusMap = ref(createEmptyInternalBuildingRuntimeStatusMap());
  const runtimeWarmupReady = ref(false);
  const engineerDirectoryLoaded = ref(false);
  const pendingResumeRuns = ref([]);
  const schedulerQuickSaving = ref(false);
  const handoverSchedulerQuickSaving = ref(false);
  const wetBulbSchedulerQuickSaving = ref(false);
  const dayMetricUploadSchedulerQuickSaving = ref(false);
  const alarmEventUploadSchedulerQuickSaving = ref(false);
  const monthlyEventReportSchedulerQuickSaving = ref(false);
  const monthlyChangeReportSchedulerQuickSaving = ref(false);
  const schedulerToggleState = reactive({
    scheduler: { mode: "idle", rememberedOverride: null },
    handover: { mode: "idle", rememberedOverride: null },
    wet_bulb: { mode: "idle", rememberedOverride: null },
    day_metric_upload: { mode: "idle", rememberedOverride: null },
    alarm_event_upload: { mode: "idle", rememberedOverride: null },
    monthly_event_report: { mode: "idle", rememberedOverride: null },
    monthly_change_report: { mode: "idle", rememberedOverride: null },
  });
  const configSaveSuspendDepth = ref(0);
  const configSaveStatus = reactive({
    mode: "idle",
    last_saved_at: "",
    last_error: "",
    draft_dirty: false,
    saved_signature: "",
  });
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
  const dayMetricUploadScope = ref("all_enabled");
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
  const handoverDownloadScope = ref("all_enabled");
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
    display: {
      auth: null,
      export: null,
      actions: {},
      capture_assets: null,
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
  const handoverConfigBuilding = ref("A楼");
  const handoverConfigCommonRevision = ref(0);
  const handoverConfigCommonUpdatedAt = ref("");
  const handoverConfigBuildingRevision = ref(0);
  const handoverConfigBuildingUpdatedAt = ref("");
  const handoverRuleScope = ref("default");
  const handoverDutyAutoFollow = ref(true);
  const handoverDutyLastAutoAt = ref(0);
  const customAbsoluteStartLocal = ref("");
  const customAbsoluteEndLocal = ref("");

  const systemLogOffset = ref(0);
  const timers = {
    pollTimer: null,
    healthTimer: null,
    externalDashboardSummaryTimer: null,
    healthWarmupTimer: null,
    configRetryTimer: null,
    internalRuntimeTimer: null,
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
    return updaterMirrorOverview.value?.businessActions?.allowed === true;
  });
  const isStatusView = computed(() => currentView.value === "status");
  const isDashboardView = computed(() => currentView.value === "dashboard");
  const isConfigView = computed(() => currentView.value === "config");
  const initialLoadingPhase = computed(() => {
    if (!bootstrapReady.value) return "bootstrapping";
    return !fullHealthLoaded.value ? "background_loading" : "ready";
  });
  const initialLoadingStatusText = computed(() => {
    const loadingErrors = [];
    const healthErrorText = String(healthLoadError.value || "").trim();
    const configErrorText = String(configLoadError.value || "").trim();
    const isRoleSelectionConflictText = (text) => String(text || "").includes("请先在角色选择页进入系统");
    if (!fullHealthLoaded.value && healthErrorText && !isAbortLikeText(healthErrorText) && !isRoleSelectionConflictText(healthErrorText)) {
      loadingErrors.push(`运行状态加载失败：${healthErrorText}`);
    }
    const isConfigViewActive = String(currentView.value || "").trim().toLowerCase() === "config";
    if (
      isConfigViewActive
      && !configLoaded.value
      && configErrorText
      && !isAbortLikeText(configErrorText)
      && !isRoleSelectionConflictText(configErrorText)
    ) {
      loadingErrors.push(`配置加载失败：${configErrorText}`);
    }
    if (!bootstrapReady.value) return "页面正在启动...";
    if (!fullHealthLoaded.value) {
      return loadingErrors.length ? `页面已打开，但${loadingErrors[0]}` : "页面已打开，正在加载运行状态...";
    }
    if (isConfigViewActive && !configLoaded.value) {
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
  const jobPanelDisplay = computed(() => normalizeJobPanelDisplayPayload(health.job_panel_summary?.display));
  const handoverGenerationBusy = computed(() =>
    Boolean(jobPanelDisplay.value?.overview?.handover_generation_busy),
  );
  const handoverGenerationStatusText = computed(() =>
    String(jobPanelDisplay.value?.overview?.handover_generation_status_text || "").trim(),
  );
  const runningJobs = computed(() =>
    Array.isArray(jobPanelDisplay.value.running_jobs)
      ? jobPanelDisplay.value.running_jobs.map((item) => normalizeBackendTaskItem(item, "job"))
      : [],
  );
  const waitingResourceJobs = computed(() =>
    Array.isArray(jobPanelDisplay.value.waiting_resource_items)
      ? jobPanelDisplay.value.waiting_resource_items.map((item) =>
        normalizeBackendTaskItem(item, String(item?.item_kind || item?.__waiting_kind || "job").trim().toLowerCase() || "job"))
      : [],
  );
  const recentFinishedJobs = computed(() =>
    Array.isArray(jobPanelDisplay.value.recent_finished_jobs)
      ? jobPanelDisplay.value.recent_finished_jobs.map((item) => normalizeBackendTaskItem(item, "job"))
      : [],
  );
  const bridgeTasksEnabled = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    return Boolean(health.shared_bridge?.enabled) && (roleMode === "internal" || roleMode === "external");
  });
  const isInternalRole = computed(() => resolveDeploymentRoleMode(health.deployment?.role_mode || "") === "internal");
  const BRIDGE_HISTORY_DISPLAY_LIMIT = 30;
  const activeBridgeTasks = computed(() =>
    Array.isArray(bridgeTasksDisplay.value?.active_tasks)
      ? bridgeTasksDisplay.value.active_tasks.map((item) => normalizeBackendTaskItem(item, "bridge"))
      : [],
  );
  const totalBridgeHistoryCount = computed(() => {
    const rawCount = Number.parseInt(String(bridgeTasksDisplay.value?.finished_count ?? ""), 10);
    return Number.isFinite(rawCount) ? Math.max(0, rawCount) : 0;
  });
  const displayedBridgeTasks = computed(() => activeBridgeTasks.value);
  const hiddenBridgeHistoryCount = computed(() =>
    Math.max(0, totalBridgeHistoryCount.value - BRIDGE_HISTORY_DISPLAY_LIMIT),
  );
  const recentFinishedBridgeTasks = computed(() =>
    Array.isArray(bridgeTasksDisplay.value?.recent_finished_tasks)
      ? bridgeTasksDisplay.value.recent_finished_tasks.map((item) => normalizeBackendTaskItem(item, "bridge"))
      : [],
  );
  const internalRuntimeBridgeSnapshot = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    if (roleMode !== "internal" || !internalRuntimeSummary.value || typeof internalRuntimeSummary.value !== "object") {
      return health.shared_bridge || {};
    }
    const summary = internalRuntimeSummary.value;
    const buildingMap = internalBuildingRuntimeStatusMap.value && typeof internalBuildingRuntimeStatusMap.value === "object"
      ? internalBuildingRuntimeStatusMap.value
      : {};
    const sourceCacheSummary = summary.source_cache && typeof summary.source_cache === "object" ? summary.source_cache : {};
    const poolSummary = summary.pool && typeof summary.pool === "object" ? summary.pool : {};
    const summarySlotRows = Array.isArray(poolSummary?.page_slots) ? poolSummary.page_slots : [];
    const summarySlotMap = new Map(
      summarySlotRows
        .filter((item) => item && typeof item === "object")
        .map((item) => [String(item.building || "").trim(), item]),
    );
    const buildFamilyRows = (familyKey, fallbackBucket = "") => {
      const summaryFamily = sourceCacheSummary?.[familyKey] && typeof sourceCacheSummary[familyKey] === "object"
        ? sourceCacheSummary[familyKey]
        : {};
      const summaryRows = Array.isArray(summaryFamily?.buildings) ? summaryFamily.buildings : [];
      const summaryRowMap = new Map(
        summaryRows
          .filter((item) => item && typeof item === "object")
          .map((item) => [String(item.building || "").trim(), item]),
      );
      return INTERNAL_BUILDINGS.map((building) => {
        const summaryRow = summaryRowMap.get(building);
        const buildingStatus = buildingMap?.[building] && typeof buildingMap[building] === "object"
          ? buildingMap[building]
          : {};
        const buildingDisplay = buildingStatus?.display && typeof buildingStatus.display === "object"
          ? buildingStatus.display
          : {};
        const displayFamilies = buildingDisplay?.source_families && typeof buildingDisplay.source_families === "object"
          ? buildingDisplay.source_families
          : {};
        const buildingFamilyRow = displayFamilies?.[familyKey] && typeof displayFamilies[familyKey] === "object"
          ? displayFamilies[familyKey]
          : null;
        if (buildingFamilyRow) {
          return {
            ...(summaryRow && typeof summaryRow === "object"
              ? summaryRow
              : buildSourceCachePlaceholderBuilding(building, fallbackBucket)),
            ...buildingFamilyRow,
          };
        }
        return summaryRow && typeof summaryRow === "object"
          ? summaryRow
          : buildSourceCachePlaceholderBuilding(building, fallbackBucket);
      });
    };
    const currentHourBucket = String(sourceCacheSummary.current_hour_bucket || "").trim();
    const alarmBucket = String(sourceCacheSummary.alarm_event_family?.current_bucket || "").trim() || currentHourBucket;
    return {
      ...health.shared_bridge,
      internal_download_pool: {
        enabled: Boolean(poolSummary.enabled),
        browser_ready: Boolean(poolSummary.browser_ready),
        overview: poolSummary.overview && typeof poolSummary.overview === "object" ? poolSummary.overview : {},
        page_slots: INTERNAL_BUILDINGS.map((building) => {
          const summarySlot = summarySlotMap.get(building);
          const buildingStatus = buildingMap?.[building] && typeof buildingMap[building] === "object"
            ? buildingMap[building]
            : {};
          const buildingDisplay = buildingStatus?.display && typeof buildingStatus.display === "object"
            ? buildingStatus.display
            : {};
          const rawSlot = buildingDisplay?.page_slot && typeof buildingDisplay.page_slot === "object"
            ? buildingDisplay.page_slot
            : null;
          if (rawSlot && typeof rawSlot === "object") {
            return {
              ...(summarySlot && typeof summarySlot === "object" ? summarySlot : { building }),
              ...rawSlot,
            };
          }
          return summarySlot && typeof summarySlot === "object" ? summarySlot : { building };
        }),
        active_buildings: Array.isArray(poolSummary.active_buildings) ? poolSummary.active_buildings : [],
        last_error: String(poolSummary.last_error || "").trim(),
      },
      internal_source_cache: {
        enabled: Boolean(sourceCacheSummary.enabled),
        scheduler_running: Boolean(sourceCacheSummary.scheduler_running),
        overview: sourceCacheSummary.overview && typeof sourceCacheSummary.overview === "object" ? sourceCacheSummary.overview : {},
        current_hour_bucket: currentHourBucket,
        last_run_at: String(sourceCacheSummary.last_run_at || "").trim(),
        last_success_at: String(sourceCacheSummary.last_success_at || "").trim(),
        last_error: String(sourceCacheSummary.last_error || "").trim(),
        cache_root: String(sourceCacheSummary.cache_root || "").trim(),
        current_hour_refresh: sourceCacheSummary.current_hour_refresh && typeof sourceCacheSummary.current_hour_refresh === "object"
          ? sourceCacheSummary.current_hour_refresh
          : {},
        current_hour_refresh_overview:
          sourceCacheSummary.current_hour_refresh_overview && typeof sourceCacheSummary.current_hour_refresh_overview === "object"
            ? sourceCacheSummary.current_hour_refresh_overview
            : {},
        handover_log_family: {
          ...(sourceCacheSummary.handover_log_family && typeof sourceCacheSummary.handover_log_family === "object"
            ? sourceCacheSummary.handover_log_family
            : {}),
          buildings: buildFamilyRows("handover_log_family", currentHourBucket),
        },
        handover_capacity_report_family: {
          ...(sourceCacheSummary.handover_capacity_report_family && typeof sourceCacheSummary.handover_capacity_report_family === "object"
            ? sourceCacheSummary.handover_capacity_report_family
            : {}),
          buildings: buildFamilyRows("handover_capacity_report_family", currentHourBucket),
        },
        monthly_report_family: {
          ...(sourceCacheSummary.monthly_report_family && typeof sourceCacheSummary.monthly_report_family === "object"
            ? sourceCacheSummary.monthly_report_family
            : {}),
          buildings: buildFamilyRows("monthly_report_family", currentHourBucket),
        },
        alarm_event_family: {
          ...(sourceCacheSummary.alarm_event_family && typeof sourceCacheSummary.alarm_event_family === "object"
            ? sourceCacheSummary.alarm_event_family
            : {}),
          buildings: buildFamilyRows("alarm_event_family", alarmBucket),
        },
      },
    };
  });

  function buildLegacyInternalDownloadPoolOverview(rawPool) {
    const payload = rawPool && typeof rawPool === "object" ? rawPool : {};
    const slotRows = Array.isArray(payload.page_slots) ? payload.page_slots : [];
    const slotMap = new Map(
      slotRows
        .filter((item) => item && typeof item === "object")
        .map((item) => [String(item.building || "").trim(), item]),
    );
    const slots = INTERNAL_BUILDINGS.map((building) =>
      normalizeInternalDownloadPoolSlot(
        slotMap.get(building) || { building },
        { formatInternalDownloadPoolError },
      )
    );
    const abnormalCount = slots.filter((item) =>
      ["warning", "danger"].includes(String(item?.tone || "").trim().toLowerCase())
      || ["已暂停等待恢复", "最近失败"].includes(String(item?.stateText || "").trim()),
    ).length;
    return {
      tone: abnormalCount > 0 ? "warning" : "neutral",
      statusText: abnormalCount > 0 ? "存在异常楼栋页签" : "等待后端页池状态",
      summaryText:
        abnormalCount > 0
          ? `当前有 ${abnormalCount} 个楼栋页签存在异常或等待恢复。`
          : "页池展示状态由后端汇总后返回，当前等待首轮状态快照。",
      errorText: formatInternalDownloadPoolError(payload.last_error),
      items: [],
      slots,
    };
  }

  function buildLegacyExternalSharedSourceCacheOverview(rawCache) {
    const payload = rawCache && typeof rawCache === "object" ? rawCache : {};
    const currentHourBucket = String(payload.current_hour_bucket || payload.currentHourBucket || "").trim();
    const familyKeys = [
      "handover_log_family",
      "monthly_report_family",
      "alarm_event_family",
      "handover_capacity_report_family",
    ].filter((familyKey) => payload?.[familyKey] && typeof payload[familyKey] === "object");
    const families = familyKeys.map((familyKey) => {
      const rawFamily = payload[familyKey] && typeof payload[familyKey] === "object" ? payload[familyKey] : {};
      const rawSelection = rawFamily.latest_selection && typeof rawFamily.latest_selection === "object"
        ? rawFamily.latest_selection
        : {};
      const fallbackBucket = String(
        rawSelection.best_bucket_key
        || rawFamily.current_bucket
        || rawFamily.currentBucket
        || currentHourBucket
        || "",
      ).trim();
      return mapPresentedSourceCacheFamilyOverview(
        {
          key: familyKey,
          ...rawFamily,
          ...rawSelection,
          buildings: Array.isArray(rawFamily.buildings)
            ? rawFamily.buildings
            : (Array.isArray(rawSelection.buildings) ? rawSelection.buildings : []),
        },
        {
          fallbackBucket,
          internalBuildings: [],
          formatSharedBridgeRuntimeError,
          formatInternalDownloadPoolError,
        },
      );
    });
    const actionableFamilies = families.filter((item) => String(item?.key || "").trim().toLowerCase() !== "alarm_event_family");
    const hasStale = actionableFamilies.some((item) => Array.isArray(item?.staleBuildings) && item.staleBuildings.length > 0);
    const hasMissing = actionableFamilies.some((item) => Array.isArray(item?.missingBuildings) && item.missingBuildings.length > 0);
    const hasFallback = actionableFamilies.some((item) => Array.isArray(item?.fallbackBuildings) && item.fallbackBuildings.length > 0);
    const canProceedLatest =
      actionableFamilies.length > 0
        ? actionableFamilies.every((item) => item?.canProceed !== false)
        : false;
    const referenceBucketKey = String(
      families.find((item) => String(item?.bestBucketKey || "").trim())?.bestBucketKey
      || families.find((item) => String(item?.currentBucket || "").trim())?.currentBucket
      || currentHourBucket
      || "-",
    ).trim() || "-";
    let tone = "neutral";
    let statusText = "等待后端共享文件状态";
    let summaryText = "共享文件状态由后端聚合后返回。";
    if (hasStale) {
      tone = "warning";
      statusText = "等待共享文件就绪";
      summaryText = "部分楼栋共享文件版本过旧，等待更新后会自动重试默认入口。";
    } else if (hasMissing && !canProceedLatest) {
      tone = "warning";
      statusText = "等待共享文件就绪";
      summaryText = "仍有楼栋等待共享文件就绪。";
    } else if (canProceedLatest) {
      tone = hasFallback ? "warning" : "success";
      statusText = "共享文件已就绪";
      summaryText = hasFallback ? "部分楼栋已回退到上一版共享文件，但默认入口可继续执行。" : "共享文件已就绪，默认入口可继续执行。";
    }
    return {
      reasonCode: canProceedLatest ? "ready" : "pending_files",
      tone,
      statusText,
      summaryText,
      detailText: "",
      displayNoteText: "",
      referenceBucketKey,
      errorText: formatSharedBridgeRuntimeError(payload.last_error),
      items: [],
      families,
      canProceed: canProceedLatest,
      canProceedLatest,
      actions: {},
    };
  }

  function buildLegacyExternalInternalAlertOverview(rawAlert) {
    const payload = rawAlert && typeof rawAlert === "object" ? rawAlert : {};
    const rawBuildings = Array.isArray(payload.buildings) ? payload.buildings : [];
    const buildingMap = new Map(
      rawBuildings
        .filter((item) => item && typeof item === "object")
        .map((item) => [String(item.building || "").trim(), item]),
    );
    const activeCount = Number.parseInt(String(payload.active_count ?? payload.activeCount ?? 0), 10) || 0;
    const buildings = INTERNAL_BUILDINGS.map((building) => {
      const raw = buildingMap.get(building) || { building, status: "normal" };
      const status = String(raw.status || "").trim().toLowerCase();
      const isProblem = status === "problem" || (Number.parseInt(String(raw.active_count ?? raw.activeCount ?? 0), 10) || 0) > 0;
      return {
        building,
        tone: isProblem ? "warning" : "success",
        statusText: isProblem ? "异常" : "正常",
        summaryText: isProblem
          ? String(raw.summary || "").trim() || "存在未恢复告警"
          : (String(raw.summary || "").trim() === "正常" ? "已恢复正常" : (String(raw.summary || "").trim() || "已恢复正常")),
        detailText: String(raw.detail || "").trim(),
        timeText: String(raw.last_problem_at || raw.lastProblemAt || raw.last_recovered_at || raw.lastRecoveredAt || "").trim(),
        activeCount: Number.parseInt(String(raw.active_count ?? raw.activeCount ?? 0), 10) || 0,
      };
    });
    return {
      tone: activeCount > 0 ? "warning" : "success",
      statusText: activeCount > 0 ? "存在异常楼栋" : "全部正常",
      summaryText: activeCount > 0
        ? `当前有 ${activeCount} 个楼栋存在未恢复的内网告警。`
        : "当前 5 个楼栋都处于正常状态。",
      items: [],
      buildings,
    };
  }

  function buildLegacyHandoverReviewOverview() {
    const reviewStatus = health.handover?.review_status && typeof health.handover.review_status === "object"
      ? health.handover.review_status
      : {};
    const rawRows = Array.isArray(reviewStatus.buildings) ? reviewStatus.buildings : [];
    const rawLinks = Array.isArray(health.handover?.review_links) ? health.handover.review_links : [];
    const rowMap = new Map(
      rawRows
        .filter((item) => item && typeof item === "object")
        .map((item) => [String(item.building || "").trim(), item]),
    );
    const linkMap = new Map(
      rawLinks
        .filter((item) => item && typeof item === "object")
        .map((item) => [String(item.building || "").trim(), item]),
    );
    const buildings = [...INTERNAL_BUILDINGS];
    for (const item of [...rowMap.keys(), ...linkMap.keys()]) {
      const building = String(item || "").trim();
      if (building && !buildings.includes(building)) buildings.push(building);
    }
    const reviewBoardRows = buildings.map((building) => {
      const rawRow = rowMap.get(building) || {};
      const rawLink = linkMap.get(building) || {};
      const hasSession = Boolean(rawRow.has_session ?? rawRow.hasSession);
      const confirmed = Boolean(rawRow.confirmed);
      const url = String(rawLink.url || "").trim();
      const status = confirmed ? "confirmed" : (hasSession ? "pending" : (url ? "available" : "missing"));
      const text = confirmed ? "已确认" : (hasSession ? "待确认" : (url ? "可访问" : "未生成"));
      const cloudSheetSync = rawRow.cloud_sheet_sync && typeof rawRow.cloud_sheet_sync === "object"
        ? rawRow.cloud_sheet_sync
        : {};
      const cloudStatus = String(cloudSheetSync.status || "").trim().toLowerCase();
      return {
        building,
        status,
        text,
        tone: confirmed ? "success" : (hasSession ? "warning" : (url ? "info" : "neutral")),
        code: String(rawLink.code || "").trim().toLowerCase(),
        url,
        cloud_sheet_sync: {
          text: cloudStatus === "success" ? "云表已同步" : (cloudStatus === "pending_upload" ? "云表待最终上传" : "云表未执行"),
          tone: cloudStatus === "success" ? "success" : (cloudStatus === "pending_upload" ? "warning" : "neutral"),
          url: String(cloudSheetSync.spreadsheet_url || "").trim(),
          error: String(cloudSheetSync.error || "").trim(),
        },
      };
    });
    return normalizeHandoverReviewOverview({
      batch_key: String(reviewStatus.batch_key || "").trim(),
      duty_date: String(reviewStatus.duty_date || "").trim(),
      duty_shift: String(reviewStatus.duty_shift || "").trim().toLowerCase(),
      has_any_session: Boolean(reviewStatus.has_any_session ?? reviewStatus.hasAnySession),
      confirmed: Number(reviewStatus.confirmed_count ?? reviewStatus.confirmedCount ?? 0),
      required: Number(reviewStatus.required_count ?? reviewStatus.requiredCount ?? INTERNAL_BUILDINGS.length),
      pending: Math.max(
        0,
        Number(reviewStatus.required_count ?? reviewStatus.requiredCount ?? INTERNAL_BUILDINGS.length)
          - Number(reviewStatus.confirmed_count ?? reviewStatus.confirmedCount ?? 0),
      ),
      all_confirmed: Boolean(reviewStatus.all_confirmed ?? reviewStatus.allConfirmed),
      ready_for_followup_upload: Boolean(reviewStatus.ready_for_followup_upload ?? reviewStatus.readyForFollowupUpload),
      followup_progress: reviewStatus.followup_progress || reviewStatus.followupProgress || {},
      review_board_rows: reviewBoardRows,
      summary_text: "交接班审核状态由后端返回。",
    });
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
    const rawPool = internalRuntimeBridgeSnapshot.value?.internal_download_pool || {};
    const backendOverview = rawPool.overview && typeof rawPool.overview === "object" ? rawPool.overview : null;
    if (backendOverview) {
      return {
        tone: String(backendOverview.tone || "").trim() || "neutral",
        statusText: String(backendOverview.status_text || backendOverview.statusText || "").trim() || "未初始化",
        summaryText: String(backendOverview.summary_text || backendOverview.summaryText || "").trim(),
        errorText: String(backendOverview.error_text || backendOverview.errorText || "").trim(),
        items: Array.isArray(backendOverview.items) ? backendOverview.items : [],
        slots: Array.isArray(backendOverview.slots)
          ? backendOverview.slots.map((slot) => normalizeInternalDownloadPoolSlot(slot, {
            formatInternalDownloadPoolError,
          }))
          : [],
      };
    }
    return buildLegacyInternalDownloadPoolOverview(rawPool);
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
    const rawCache = internalRuntimeBridgeSnapshot.value?.internal_source_cache || {};
    const backendOverview = rawCache.overview && typeof rawCache.overview === "object" ? rawCache.overview : null;
    if (backendOverview) {
      const backendFamilies = Array.isArray(backendOverview.families) ? backendOverview.families : [];
      return {
        tone: String(backendOverview.tone || "").trim() || "neutral",
        statusText: String(backendOverview.status_text || backendOverview.statusText || "").trim() || "准备中",
        summaryText: String(backendOverview.summary_text || backendOverview.summaryText || "").trim(),
        currentHourBucket: String(backendOverview.current_hour_bucket || backendOverview.currentHourBucket || "").trim() || "-",
        lastRunAt: String(backendOverview.last_run_at || backendOverview.lastRunAt || "").trim(),
        lastSuccessAt: String(backendOverview.last_success_at || backendOverview.lastSuccessAt || "").trim(),
        errorText: String(backendOverview.error_text || backendOverview.errorText || "").trim(),
        cacheRoot: String(backendOverview.cache_root || backendOverview.cacheRoot || "").trim(),
        items: Array.isArray(backendOverview.items) ? backendOverview.items : [],
        families: backendFamilies.map((family) =>
          mapPresentedSourceCacheFamilyOverview(
            family,
            {
              fallbackBucket: String(family.current_bucket || family.currentBucket || "").trim(),
              internalBuildings: INTERNAL_BUILDINGS,
              formatSharedBridgeRuntimeError,
              formatInternalDownloadPoolError,
            },
          )
        ),
      };
    }
    const currentHourBucket = String(rawCache.current_hour_bucket || "").trim();
    const lastRunAt = String(rawCache.last_run_at || "").trim();
    const lastSuccessAt = String(rawCache.last_success_at || "").trim();
    const rawFamilies = INTERNAL_SOURCE_CACHE_FAMILY_KEYS
      .map((familyKey) => {
        const family = rawCache?.[familyKey];
        if (!family || typeof family !== "object") return null;
        return mapPresentedSourceCacheFamilyOverview(
          { key: familyKey, ...family },
          {
            fallbackBucket: String(family.current_bucket || family.currentBucket || currentHourBucket || "").trim(),
            internalBuildings: INTERNAL_BUILDINGS,
            formatSharedBridgeRuntimeError,
            formatInternalDownloadPoolError,
          },
        );
      })
      .filter(Boolean);
    const allKnownFamiliesReady = rawFamilies.length > 0
      && rawFamilies.every((family) => {
        const buildingRows = Array.isArray(family?.buildings) ? family.buildings : [];
        return buildingRows.length > 0 && buildingRows.every((row) => String(row?.statusKey || "").trim().toLowerCase() === "ready");
      });
    const hasSuccessfulRefresh = hasSuccessfulCurrentHourRefreshSnapshot(rawCache.current_hour_refresh);
    const shouldSuppressLastError = allKnownFamiliesReady || hasSuccessfulRefresh;
    const lastError = shouldSuppressLastError ? "" : formatSharedBridgeRuntimeError(rawCache.last_error);
    const cacheRoot = String(rawCache.cache_root || "").trim();
    if (allKnownFamiliesReady) {
      return {
        tone: "success",
        statusText: "本轮共享文件已全部就绪",
        summaryText: "共享缓存状态以后端快照为准，当前各文件族已完成本轮同步。",
        currentHourBucket: currentHourBucket || "-",
        lastRunAt,
        lastSuccessAt,
        errorText: "",
        cacheRoot,
        items: [],
        families: rawFamilies,
      };
    }
    return {
      tone: lastError ? "warning" : "neutral",
      statusText: "等待后端源文件状态",
      summaryText: "源文件总览由后端聚合后返回，当前等待首轮状态快照。",
      currentHourBucket: currentHourBucket || "-",
      lastRunAt,
      lastSuccessAt,
      errorText: lastError,
      cacheRoot,
      items: [],
      families: rawFamilies,
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
      ...family,
      key: String(family?.key || "").trim(),
      title: String(family?.title || "").trim(),
      tone: family.tone,
      statusText: family.statusText,
      currentBucket: family.currentBucket || internalSourceCacheOverview.value?.currentHourBucket || "-",
      lastSuccessAt: family.lastSuccessAt || "",
      buildings: Array.isArray(family.buildings) ? family.buildings : [],
      actions: family?.actions && typeof family.actions === "object" ? family.actions : {},
    }));
  });
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
    const displayOverview = health.dashboard_display?.internal_alert_overview
      && typeof health.dashboard_display.internal_alert_overview === "object"
      ? health.dashboard_display.internal_alert_overview
      : null;
    if (displayOverview) {
      return mapPresentedInternalAlertOverview(displayOverview, {
        internalBuildings: INTERNAL_BUILDINGS,
      });
    }
    return buildLegacyExternalInternalAlertOverview(health.shared_bridge?.internal_alert_status || {});
  });
  const currentHourRefreshOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    const rawCache = internalRuntimeBridgeSnapshot.value?.internal_source_cache || {};
    const backendOverview = rawCache.current_hour_refresh_overview && typeof rawCache.current_hour_refresh_overview === "object"
      ? rawCache.current_hour_refresh_overview
      : null;
    if (roleMode === "internal" && backendOverview) {
      return mapCurrentHourRefreshOverview(backendOverview);
    }
    if (roleMode !== "internal") {
      return {
        reasonCode: "role_mismatch",
        tone: "neutral",
        statusText: "当前角色未启用",
        summaryText: "",
        detailText: "",
        lastRunAt: "",
        lastSuccessAt: "",
        lastError: "",
        failedBuildings: [],
        blockedBuildings: [],
        runningBuildings: [],
        completedBuildings: [],
        items: [],
        actions: {},
      };
    }
    const rawRefresh = rawCache.current_hour_refresh && typeof rawCache.current_hour_refresh === "object"
      ? rawCache.current_hour_refresh
      : {};
    const failedBuildings = Array.isArray(rawRefresh.failed_buildings) ? rawRefresh.failed_buildings : [];
    const blockedBuildings = Array.isArray(rawRefresh.blocked_buildings) ? rawRefresh.blocked_buildings : [];
    const runningBuildings = Array.isArray(rawRefresh.running_buildings) ? rawRefresh.running_buildings : [];
    const completedBuildings = Array.isArray(rawRefresh.completed_buildings) ? rawRefresh.completed_buildings : [];
    const hasSuccessfulRefresh = hasSuccessfulCurrentHourRefreshSnapshot(rawRefresh);
    if (hasSuccessfulRefresh) {
      return {
        reasonCode: "success",
        tone: "success",
        statusText: "最近一轮已完成",
        summaryText: "当前小时共享文件补拉已完成，本轮没有失败或阻塞楼栋。",
        detailText: "",
        lastRunAt: String(rawRefresh.last_run_at || "").trim(),
        lastSuccessAt: String(rawRefresh.last_success_at || "").trim(),
        lastError: "",
        failedBuildings: [],
        blockedBuildings: [],
        runningBuildings: [],
        completedBuildings,
        items: [],
        actions: {},
      };
    }
    return {
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "等待后端补拉状态",
      summaryText: "当前小时即时补拉状态由后端汇总后返回，当前等待首轮状态快照。",
      detailText: "",
      lastRunAt: String(rawRefresh.last_run_at || "").trim(),
      lastSuccessAt: String(rawRefresh.last_success_at || "").trim(),
      lastError: "",
      failedBuildings,
      blockedBuildings,
      runningBuildings,
      completedBuildings,
      items: [],
      actions: {},
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
          items: [],
          actions: {},
        },
        families: [],
      };
    }
    const backendOverview = internalRuntimeSummary.value?.display?.runtime_overview
      && typeof internalRuntimeSummary.value.display.runtime_overview === "object"
      ? internalRuntimeSummary.value.display.runtime_overview
      : null;
    if (backendOverview) {
      const backendCurrentHour = backendOverview.current_hour_refresh && typeof backendOverview.current_hour_refresh === "object"
        ? backendOverview.current_hour_refresh
        : (backendOverview.currentHourRefresh && typeof backendOverview.currentHourRefresh === "object" ? backendOverview.currentHourRefresh : null);
      return {
        tone: String(backendOverview.tone || "").trim() || "neutral",
        statusText: String(backendOverview.status_text || backendOverview.statusText || "").trim() || "等待内网运行态",
        summaryText: String(backendOverview.summary_text || backendOverview.summaryText || "").trim(),
        items: Array.isArray(backendOverview.items) ? backendOverview.items : [],
        cacheRoot: String(backendOverview.cache_root || backendOverview.cacheRoot || "").trim(),
        errorText: String(backendOverview.error_text || backendOverview.errorText || "").trim(),
        poolStatusText: String(backendOverview.pool_status_text || backendOverview.poolStatusText || "").trim(),
        poolSummaryText: String(backendOverview.pool_summary_text || backendOverview.poolSummaryText || "").trim(),
        poolItems: Array.isArray(backendOverview.pool_items) ? backendOverview.pool_items : (Array.isArray(backendOverview.poolItems) ? backendOverview.poolItems : []),
        poolErrorText: String(backendOverview.pool_error_text || backendOverview.poolErrorText || "").trim(),
        slots: Array.isArray(backendOverview.slots)
          ? backendOverview.slots.map((slot) => normalizeInternalDownloadPoolSlot(slot, {
            formatInternalDownloadPoolError,
          }))
          : [],
        currentHourRefresh: backendCurrentHour ? mapCurrentHourRefreshOverview(backendCurrentHour) : {
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
          items: [],
          actions: {},
        },
        families: Array.isArray(backendOverview.families)
          ? backendOverview.families.map((family) =>
            mapPresentedSourceCacheFamilyOverview(
              family,
              {
                fallbackBucket: String(
                  family?.current_bucket
                  || family?.currentBucket
                  || family?.best_bucket_key
                  || family?.bestBucketKey
                  || "",
                ).trim(),
                internalBuildings: INTERNAL_BUILDINGS,
                formatSharedBridgeRuntimeError,
                formatInternalDownloadPoolError,
              },
            )
          )
          : [],
      };
    }
    return {
      tone: "neutral",
      statusText: "等待后端运行概览",
      summaryText: "内网运行概览由后端聚合后返回，当前等待首轮状态快照。",
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
        items: [],
        actions: {},
      },
      families: [],
    };
  });
  const internalSourceCacheHistoryOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    if (roleMode !== "internal") {
      return {
        reasonCode: "role_mismatch",
        tone: "neutral",
        statusText: "",
        summaryText: "",
        items: [],
        detailText: "",
        lastError: "",
        actions: {},
      };
    }
    const backendOverview = internalRuntimeSummary.value?.display?.history_overview
      && typeof internalRuntimeSummary.value.display.history_overview === "object"
      ? internalRuntimeSummary.value.display.history_overview
      : null;
    if (backendOverview) {
      return {
        reasonCode: String(backendOverview.reason_code || backendOverview.reasonCode || "").trim().toLowerCase() || "unknown",
        tone: String(backendOverview.tone || "").trim() || "neutral",
        statusText: String(backendOverview.status_text || backendOverview.statusText || "").trim() || "等待后端历史摘要",
        summaryText: String(backendOverview.summary_text || backendOverview.summaryText || "").trim() || "历史摘要由后端状态快照补齐。",
        items: Array.isArray(backendOverview.items) ? backendOverview.items : [],
        detailText: String(backendOverview.detail_text || backendOverview.detailText || "").trim(),
        lastError: String(backendOverview.last_error || backendOverview.lastError || "").trim(),
        actions: mapBackendActionsState(backendOverview.actions),
      };
    }
    return {
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "等待后端历史摘要",
      summaryText: "历史摘要由后端状态快照补齐。",
      items: [],
      detailText: "",
      lastError: "",
      actions: {},
    };
  });
  const sharedSourceCacheReadinessOverview = computed(() => {
    const roleMode = resolveDeploymentRoleMode(health.deployment?.role_mode || "");
    const backendOverview = health.dashboard_display?.shared_source_cache_overview
      && typeof health.dashboard_display.shared_source_cache_overview === "object"
      ? health.dashboard_display.shared_source_cache_overview
      : null;
    if (roleMode === "external" && backendOverview) {
      return {
        reasonCode: String(backendOverview.reason_code || backendOverview.reasonCode || "").trim().toLowerCase() || "unknown",
        tone: String(backendOverview.tone || "").trim() || "warning",
        statusText: String(backendOverview.status_text || backendOverview.statusText || "").trim() || "等待后端共享文件状态",
        summaryText: String(backendOverview.summary_text || backendOverview.summaryText || "").trim(),
        detailText: String(backendOverview.detail_text || backendOverview.detailText || "").trim(),
        displayNoteText: String(backendOverview.display_note_text || backendOverview.displayNoteText || "").trim(),
        referenceBucketKey: String(backendOverview.reference_bucket_key || backendOverview.referenceBucketKey || "").trim() || "-",
        errorText: String(backendOverview.error_text || backendOverview.errorText || "").trim(),
        items: Array.isArray(backendOverview.items)
          ? backendOverview.items
            .filter((item) => item && typeof item === "object")
            .map((item) => ({
              label: String(item.label || "").trim(),
              value: String(item.value ?? "").trim(),
              tone: String(item.tone || "").trim() || "neutral",
            }))
          : [],
        families: Array.isArray(backendOverview.families)
          ? backendOverview.families.map((family) =>
            mapPresentedSourceCacheFamilyOverview(
              family,
              {
                fallbackBucket: String(
                  family.current_bucket || family.currentBucket || family.best_bucket_key || family.bestBucketKey || "",
                ).trim(),
                internalBuildings: INTERNAL_BUILDINGS,
                formatSharedBridgeRuntimeError,
                formatInternalDownloadPoolError,
              },
            )
          )
          : [],
        canProceed: Boolean(backendOverview.can_proceed_latest ?? backendOverview.canProceedLatest),
        canProceedLatest: Boolean(backendOverview.can_proceed_latest ?? backendOverview.canProceedLatest),
        actions: mapBackendActionsState(backendOverview.actions),
      };
    }
    if (roleMode === "external") {
      return buildLegacyExternalSharedSourceCacheOverview(health.shared_bridge?.internal_source_cache || {});
    }
    if (roleMode !== "external") {
      return {
        reasonCode: "role_mismatch",
        tone: "neutral",
        statusText: "当前角色未使用共享缓存",
        summaryText: "",
        detailText: "",
        displayNoteText: "",
        referenceBucketKey: "-",
        errorText: "",
        items: [],
        families: [],
        canProceed: false,
        canProceedLatest: false,
        actions: {},
      };
    }
    return {
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "等待后端共享文件状态",
      summaryText: "共享文件状态由后端聚合后返回。",
      detailText: "",
      displayNoteText: "",
      referenceBucketKey: "-",
      errorText: "",
      items: [],
      families: [],
      canProceed: false,
      canProceedLatest: false,
      actions: {},
    };
  });
  const updaterMirrorOverview = computed(() => {
    const backendOverview = mapBackendUpdaterMirrorOverview(
      health.updater?.display_overview || health.dashboard_display?.updater_mirror_overview,
    );
    if (backendOverview) return backendOverview;
    return {
      tone: "neutral",
      kicker: "更新镜像",
      title: "共享目录批准版本",
      statusText: "等待后端更新状态",
      summaryText: "更新镜像状态由后端聚合后返回。",
      manifestPath: "",
      errorText: "",
      items: [],
      actions: {},
      businessActions: {
        allowed: false,
        reasonCode: "pending_backend",
        disabledReason: "等待后端更新状态。",
        statusText: "等待后端更新状态",
      },
      internalPeer: {
        available: false,
        online: false,
        updateAvailable: false,
        restartRequired: false,
        statusText: "等待后端状态",
        command: {
          active: false,
          action: "",
          status: "",
          message: "",
        },
      },
    };
  });
  const sharedRootDiagnosticOverview = computed(() => {
    const diagnostic = (
      health.dashboard_display?.shared_root_diagnostic_overview
      && typeof health.dashboard_display.shared_root_diagnostic_overview === "object"
        ? health.dashboard_display.shared_root_diagnostic_overview
        : {}
    ) || {};
    const base = resolveBackendOverviewCard(diagnostic, null, {
      kicker: "共享目录诊断",
      title: "共享目录一致性",
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "未诊断",
      summaryText: "当前还没有共享目录一致性诊断结果。",
      detailText: "",
      items: [],
      actions: [],
    });
    const rawItems = Array.isArray(diagnostic.items) ? diagnostic.items : [];
    const rawPaths = Array.isArray(diagnostic.paths) ? diagnostic.paths : [];
    const notes = Array.isArray(diagnostic.notes)
      ? diagnostic.notes.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    const paths = rawPaths
      .map((item) => {
        const label = String(item?.label || "").trim();
        const path = String(item?.path || "").trim();
        const canonicalPath = String(item?.canonical_path || "").trim();
        if (!label) return null;
        return {
          label,
          path: path || "未配置",
          canonicalPath,
          showCanonicalPath: Boolean(canonicalPath) && canonicalPath !== path,
        };
      })
      .filter(Boolean);
    return {
      ...base,
      items: rawItems.map((item) => ({
        label: String(item?.label || "").trim() || "-",
        value: String(item?.value || "").trim() || "-",
        tone: String(item?.tone || "").trim() || "neutral",
      })),
      paths,
      notes,
      actions: mapBackendActionsState(diagnostic.actions),
    };
  });
  const currentTaskOverview = computed(() => {
    const defaults = {
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "等待后端状态",
      summaryText: "任务状态由后端聚合后返回。",
      nextActionText: "",
      focusTitle: "等待后端任务状态",
      focusMeta: "",
      runningCount: 0,
      waitingCount: 0,
      bridgeActiveCount: 0,
      items: [],
      actions: [],
    };
    const backendDisplay = resolveDeploymentRoleMode(health.deployment?.role_mode || "") === "internal"
      ? internalRuntimeSummary.value?.display?.current_task_overview
      : health.dashboard_display?.current_task_overview;
    return resolveBackendOverviewCard(backendDisplay, null, defaults);
  });
  const taskPanelOverview = computed(() => {
    const defaults = {
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "等待后端状态",
      summaryText: "任务面板状态由后端聚合后返回。",
      nextActionText: "",
      focusTitle: "等待后端任务状态",
      focusMeta: "",
      runningCount: 0,
      waitingCount: 0,
      bridgeActiveCount: 0,
      items: [],
      actions: [],
    };
    return resolveBackendOverviewCard(health.dashboard_display?.task_panel_overview, null, defaults);
  });
  const bridgeTaskPanelOverview = computed(() => {
    const defaults = {
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "等待后端状态",
      summaryText: "共享桥接任务状态由后端聚合后返回。",
      nextActionText: "",
      focusTitle: "等待后端桥接任务状态",
      focusMeta: "",
      activeCount: 0,
      waitingCount: 0,
      finishedCount: 0,
      items: [],
      actions: [],
    };
    return resolveBackendOverviewCard(health.dashboard_display?.bridge_task_panel_overview, null, defaults);
  });
  const homeOverview = computed(() => {
    const backendDisplay = resolveDeploymentRoleMode(health.deployment?.role_mode || "") === "internal"
      ? internalRuntimeSummary.value?.display?.home_overview
      : health.dashboard_display?.home_overview;
    const defaults = {
      tone: "neutral",
      statusText: resolveDeploymentRoleMode(health.deployment?.role_mode || "") === "internal" ? "等待内网运行态" : "等待外网运行态",
      summaryText: "",
      nextActionText: "",
      items: [],
      actions: [],
    };
    return resolveBackendOverviewCard(backendDisplay, null, defaults);
  });
  const homeQuickActionsById = computed(() => mapBackendActionListById(homeOverview.value?.actions));
  const statusDiagnosisOverview = computed(() => {
    const backendDisplay = resolveDeploymentRoleMode(health.deployment?.role_mode || "") === "internal"
      ? internalRuntimeSummary.value?.display?.status_diagnosis_overview
      : health.dashboard_display?.status_diagnosis_overview;
    const defaults = {
      tone: "neutral",
      statusText: resolveDeploymentRoleMode(health.deployment?.role_mode || "") === "internal" ? "等待内网运行态" : "等待外网运行态",
      reasonText: "",
      actionText: "",
      items: [],
      actions: [],
    };
    return resolveBackendOverviewCard(backendDisplay, null, defaults);
  });
  const statusQuickActionsById = computed(() => mapBackendActionListById(statusDiagnosisOverview.value?.actions));
  const configGuidanceOverview = computed(() => {
    const defaults = {
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "等待后端配置状态",
      summaryText: "配置就绪概览由后端聚合后返回。",
      detailText: "",
      restartImpactText: "大多数配置保存后可直接生效；只有角色监听模式变化时才需要自动重启。",
      sections: [],
      quickTabs: [],
    };
    const backendOverview = health.dashboard_display?.config_guidance_overview;
    const base = resolveBackendOverviewCard(backendOverview, null, defaults);
    return {
      ...base,
      restartImpactText: String(
        backendOverview?.restart_impact_text || backendOverview?.restartImpactText || defaults.restartImpactText,
      ).trim() || defaults.restartImpactText,
      sections: Array.isArray(backendOverview?.sections) ? backendOverview.sections : [],
      quickTabs: Array.isArray(backendOverview?.quick_tabs)
        ? backendOverview.quick_tabs
        : (Array.isArray(backendOverview?.quickTabs) ? backendOverview.quickTabs : []),
    };
  });
  const externalAlarmUploadOverview = computed(() =>
    resolveBackendOverviewCard(health.dashboard_display?.alarm_upload_overview, null, {
      tone: "neutral",
      statusText: "等待后端状态",
      summaryText: "告警上传状态由后端聚合后返回。",
      detailText: "",
    }),
  );
  const monthlyEventReportLastRunOverview = computed(() =>
    normalizeMonthlyReportLastRunDisplay(
      health.monthly_event_report?.last_run,
      "事件月报",
    ),
  );
  const monthlyChangeReportLastRunOverview = computed(() =>
    normalizeMonthlyReportLastRunDisplay(
      health.monthly_change_report?.last_run,
      "变更月报",
    ),
  );
  const monthlyEventReportDeliveryOverview = computed(() =>
    normalizeMonthlyReportDeliveryOverview(
      health.monthly_event_report?.delivery?.display?.overview,
      "事件月报",
    ),
  );
  const monthlyChangeReportDeliveryOverview = computed(() =>
    normalizeMonthlyReportDeliveryOverview(
      health.monthly_change_report?.delivery?.display?.overview,
      "变更月报",
    ),
  );
  const monthlyEventReportRecipientRows = computed(() => {
    const backendRows = Array.isArray(health.monthly_event_report?.delivery?.display?.rows)
      ? health.monthly_event_report.delivery.display.rows
      : [];
    const rowMap = new Map(
      backendRows
        .filter((item) => item && typeof item === "object" && String(item.building || "").trim())
        .map((item) => {
          const row = normalizeMonthlyReportDeliveryRow(item);
          return [row.building, row];
        }),
    );
    return INTERNAL_BUILDINGS.map((building) => rowMap.get(building) || createNeutralMonthlyReportDeliveryRow(building));
  });
  const monthlyChangeReportRecipientRows = computed(() => {
    const backendRows = Array.isArray(health.monthly_change_report?.delivery?.display?.rows)
      ? health.monthly_change_report.delivery.display.rows
      : [];
    const rowMap = new Map(
      backendRows
        .filter((item) => item && typeof item === "object" && String(item.building || "").trim())
        .map((item) => {
          const row = normalizeMonthlyReportDeliveryRow(item);
          return [row.building, row];
        }),
    );
    return INTERNAL_BUILDINGS.map((building) => rowMap.get(building) || createNeutralMonthlyReportDeliveryRow(building));
  });
  const monthlyEventReportDeliveryLastRun = computed(() =>
    normalizeMonthlyReportDeliveryLastRun(
      health.monthly_event_report?.delivery?.display?.last_run,
      "事件月报",
    ),
  );
  const monthlyChangeReportDeliveryLastRun = computed(() =>
    normalizeMonthlyReportDeliveryLastRun(
      health.monthly_change_report?.delivery?.display?.last_run,
      "变更月报",
    ),
  );
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
  const schedulerDecisionText = computed(() => readSchedulerDisplayText(health.scheduler, "decision_text", "暂无记录"));
  const schedulerTriggerText = computed(() => readSchedulerDisplayText(health.scheduler, "trigger_text", "暂无记录"));
  const wetBulbSchedulerDecisionText = computed(() =>
    readSchedulerDisplayText(health.wet_bulb_collection?.scheduler, "decision_text", "暂无记录"),
  );
  const wetBulbSchedulerTriggerText = computed(() =>
    readSchedulerDisplayText(health.wet_bulb_collection?.scheduler, "trigger_text", "暂无记录"),
  );
  const monthlyEventReportSchedulerDecisionText = computed(() =>
    readSchedulerDisplayText(health.monthly_event_report?.scheduler, "decision_text", "暂无记录"),
  );
  const monthlyEventReportSchedulerTriggerText = computed(() =>
    readSchedulerDisplayText(health.monthly_event_report?.scheduler, "trigger_text", "暂无记录"),
  );
  const monthlyChangeReportSchedulerDecisionText = computed(() =>
    readSchedulerDisplayText(health.monthly_change_report?.scheduler, "decision_text", "暂无记录"),
  );
  const monthlyChangeReportSchedulerTriggerText = computed(() =>
    readSchedulerDisplayText(health.monthly_change_report?.scheduler, "trigger_text", "暂无记录"),
  );
  const dayMetricUploadSchedulerDecisionText = computed(() =>
    readSchedulerDisplayText(health.day_metric_upload?.scheduler, "decision_text", "暂无记录"),
  );
  const dayMetricUploadSchedulerTriggerText = computed(() =>
    readSchedulerDisplayText(health.day_metric_upload?.scheduler, "trigger_text", "暂无记录"),
  );
  const alarmEventUploadSchedulerDecisionText = computed(() =>
    readSchedulerDisplayText(health.alarm_event_upload?.scheduler, "decision_text", "暂无记录"),
  );
  const alarmEventUploadSchedulerTriggerText = computed(() =>
    readSchedulerDisplayText(health.alarm_event_upload?.scheduler, "trigger_text", "暂无记录"),
  );
  const handoverMorningDecisionText = computed(() =>
    readSchedulerDisplayText(health.handover_scheduler?.morning, "decision_text", "暂无记录"),
  );
  const handoverAfternoonDecisionText = computed(() =>
    readSchedulerDisplayText(health.handover_scheduler?.afternoon, "decision_text", "暂无记录"),
  );
  const handoverReviewRows = computed(() =>
    {
      const backendRows = Array.isArray(handoverReviewOverview.value?.reviewBoardRows)
        ? handoverReviewOverview.value.reviewBoardRows
        : [];
      if (!backendRows.length) return [];
      return backendRows.map((row) => {
        return {
          ...row,
          link: {
            building: String(row.building || "").trim(),
            code: String(row.code || "").trim().toLowerCase(),
            url: String(row.url || "").trim(),
          },
          url: String(row.url || "").trim(),
          hasUrl: Boolean(String(row.url || "").trim()),
          cloudSheetSyncText: String(row.cloudSheetSync?.text || "").trim(),
          cloudSheetSyncTone: String(row.cloudSheetSync?.tone || "").trim() || "neutral",
          cloudSheetUrl: String(row.cloudSheetSync?.url || "").trim(),
          hasCloudSheetUrl: Boolean(String(row.cloudSheetSync?.url || "").trim()),
          cloudSheetError: String(row.cloudSheetSync?.error || "").trim(),
          reviewLinkDeliveryText: String(row.reviewLinkDelivery?.text || "").trim(),
          reviewLinkDeliveryTone: String(row.reviewLinkDelivery?.tone || "").trim() || "neutral",
          reviewLinkDeliveryError: String(row.reviewLinkDelivery?.error || "").trim(),
          reviewLinkDeliveryLastSentAt: String(row.reviewLinkDelivery?.lastSentAt || "").trim(),
          reviewLinkDeliveryLastAttemptAt: String(row.reviewLinkDelivery?.lastAttemptAt || "").trim(),
        };
      });
    },
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
  const dashboardSystemOverview = computed(() => {
    const backendDisplay = health.dashboard_display?.system_overview;
    return resolveBackendOverviewCard(backendDisplay, null, {
      kicker: "系统与网络",
      title: "当前运行环境",
      tone: "neutral",
      statusText: "等待后端状态",
      summaryText: "系统概览由后端聚合后返回。",
      detailText: "",
      items: [],
    });
  });
  const dashboardSystemStatusItems = computed(() => {
    const backendItems = Array.isArray(dashboardSystemOverview.value?.items)
      ? dashboardSystemOverview.value.items
      : [];
    return backendItems.map((item) => ({
      label: String(item?.label || "").trim() || "-",
      value: String(item?.value || "").trim() || "-",
      tone: String(item?.tone || "").trim() || "neutral",
    }));
  });
  const dashboardScheduleOverview = computed(() => {
    const backendDisplay = health.dashboard_display?.scheduler_overview;
    return resolveBackendOverviewCard(backendDisplay, null, {
      kicker: "调度状态",
      title: "月报与交接班调度",
      tone: "neutral",
      statusText: "等待后端调度状态",
      summaryText: "调度状态由后端聚合后返回。",
      detailText: "",
      items: [],
    });
  });
  const dashboardScheduleStatusItems = computed(() => {
    const summaryItems = Array.isArray(dashboardScheduleOverview.value?.items)
      ? dashboardScheduleOverview.value.items
      : [];
    return summaryItems.map((item) => ({
      label: String(item?.label || "").trim() || "-",
      value: String(item?.value || "").trim() || "-",
      tone: String(item?.tone || "").trim() || "neutral",
    }));
  });
  const schedulerOverviewItems = computed(() => {
    const backendItems = Array.isArray(health.dashboard_display?.scheduler_overview_items)
      ? health.dashboard_display.scheduler_overview_items
      : [];
    if (backendItems.length) {
      return backendItems.map(mapBackendSchedulerOverviewItem);
    }
    return [];
  });
  const schedulerOverviewSummary = computed(() => {
    const backendSummary = health.dashboard_display?.scheduler_overview_summary;
    if (backendSummary && typeof backendSummary === "object") {
      return mapBackendSchedulerOverviewSummary(backendSummary);
    }
    return {
      runningCount: 0,
      stoppedCount: 0,
      attentionCount: 0,
      statusText: "等待后端调度状态",
      tone: "neutral",
      nextSchedulerLabel: "等待后端状态",
      nextSchedulerText: "调度总览由后端聚合后返回。",
      attentionText: "等待后端状态",
      summaryText: "调度状态由后端聚合后返回。",
    };
  });
  const handoverReviewOverview = computed(() => {
    const backendOverview = health.dashboard_display?.handover_review_overview;
    if (backendOverview && typeof backendOverview === "object") {
      return normalizeHandoverReviewOverview(backendOverview);
    }
    return buildLegacyHandoverReviewOverview();
  });
  const handoverFollowupProgress = computed(() => {
    const presented = handoverReviewOverview.value?.followupProgress;
    if (presented && typeof presented === "object") {
      return presented;
    }
    return {
      status: "idle",
      canResumeFollowup: false,
      pendingCount: 0,
      failedCount: 0,
      attachmentPendingCount: 0,
      cloudPendingCount: 0,
      dailyReportStatus: "idle",
      tone: "neutral",
      statusText: "等待后端交接班状态",
      summaryText: "已清空",
    };
  });
  const handoverDailyReportAuthVm = computed(() =>
    handoverDailyReportContext.value?.display?.auth && typeof handoverDailyReportContext.value.display.auth === "object"
      ? {
        text: String(handoverDailyReportContext.value.display.auth.text || "").trim(),
        tone: String(handoverDailyReportContext.value.display.auth.tone || "").trim() || "neutral",
        error: String(handoverDailyReportContext.value.display.auth.error || "").trim(),
        profileText: String(handoverDailyReportContext.value.display.auth.profile_text || handoverDailyReportContext.value.display.auth.profileText || "").trim(),
        profileLabel: String(handoverDailyReportContext.value.display.auth.profile_label || handoverDailyReportContext.value.display.auth.profileLabel || "").trim(),
      }
      : {
        text: "等待后端状态",
        tone: "neutral",
        error: "",
        profileText: "",
        profileLabel: "当前目标浏览器",
      },
  );
  const handoverDailyReportExportVm = computed(() =>
    handoverDailyReportContext.value?.display?.export && typeof handoverDailyReportContext.value.display.export === "object"
      ? {
        text: String(handoverDailyReportContext.value.display.export.text || "").trim(),
        tone: String(handoverDailyReportContext.value.display.export.tone || "").trim() || "neutral",
        error: String(handoverDailyReportContext.value.display.export.error || "").trim(),
      }
      : {
        text: "等待后端状态",
        tone: "neutral",
        error: "",
      },
  );
  const handoverDailyReportActions = computed(() => {
    const backendActions = handoverDailyReportContext.value?.display?.actions;
    if (backendActions && typeof backendActions === "object") {
      return mapBackendActionsState(backendActions);
    }
    return {
      open_auth: mapBackendActionState({
        allowed: false,
        label: "等待后端动作",
        disabled_reason: "",
        reason_code: "daily_report_state_not_ready",
      }),
      screenshot_test: mapBackendActionState({
        allowed: false,
        label: "等待后端动作",
        disabled_reason: "",
        reason_code: "daily_report_state_not_ready",
      }),
      rewrite_record: mapBackendActionState({
        allowed: false,
        label: "等待后端动作",
        disabled_reason: "",
        reason_code: "daily_report_state_not_ready",
      }),
    };
  });
  const handoverDailyReportSpreadsheetUrl = computed(() =>
    String(
      handoverDailyReportContext.value?.daily_report_record_export?.spreadsheet_url ||
        health.handover?.review_status?.cloud_sheet_sync?.spreadsheet_url ||
        "",
    ).trim(),
  );
  const handoverDailyReportCaptureAssets = computed(() => {
    const dutyDate = String(handoverDailyReportContext.value?.duty_date || "").trim();
    const dutyShift = String(handoverDailyReportContext.value?.duty_shift || "").trim().toLowerCase();
    const rawAssets = handoverDailyReportContext.value?.capture_assets;
    const summaryLastWrittenSource = String(
      handoverDailyReportContext.value?.daily_report_record_export?.summary_screenshot_source_used || "",
    ).trim();
    const externalLastWrittenSource = String(
      handoverDailyReportContext.value?.daily_report_record_export?.external_screenshot_source_used || "",
    ).trim();
    const backendDisplay = handoverDailyReportContext.value?.display?.capture_assets;
    if (backendDisplay && typeof backendDisplay === "object") {
      return {
        summarySheetImage: backendDisplay.summary_sheet_image && typeof backendDisplay.summary_sheet_image === "object"
          ? mapBackendDailyReportAssetCard(backendDisplay.summary_sheet_image, "今日航图截图")
          : normalizeDailyReportAssetCard({}, "今日航图截图"),
        externalPageImage: backendDisplay.external_page_image && typeof backendDisplay.external_page_image === "object"
          ? mapBackendDailyReportAssetCard(backendDisplay.external_page_image, "排班截图")
          : normalizeDailyReportAssetCard({}, "排班截图"),
      };
    }
    if (rawAssets && typeof rawAssets === "object") {
      return {
        summarySheetImage: normalizeDailyReportAssetCard(
          rawAssets.summary_sheet_image || {},
          "今日航图截图",
          {
            dutyDate,
            dutyShift,
            lastWrittenSource: summaryLastWrittenSource,
          },
        ),
        externalPageImage: normalizeDailyReportAssetCard(
          rawAssets.external_page_image || {},
          "排班截图",
          {
            dutyDate,
            dutyShift,
            lastWrittenSource: externalLastWrittenSource,
          },
        ),
      };
    }
    return {
      summarySheetImage: normalizeDailyReportAssetCard({}, "今日航图截图"),
      externalPageImage: normalizeDailyReportAssetCard({}, "排班截图"),
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
    handoverDailyReportActions.value?.rewrite_record?.allowed !== false,
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
    const backendText = String(updaterMirrorOverview.value?.badgeText || "").trim();
    if (backendText) return backendText;
    return "等待后端更新状态";
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
  const backendDashboardModuleHeroMap = computed(() => {
    const payload = health.dashboard_display?.module_hero_overviews;
    if (!payload || typeof payload !== "object") return {};
    const next = {};
    for (const [rawKey, rawValue] of Object.entries(payload)) {
      const key = String(rawKey || "").trim();
      const hero = rawValue && typeof rawValue === "object" ? rawValue : null;
      if (!key || !hero) continue;
      next[key] = {
        eyebrow: String(hero.eyebrow || "").trim(),
        title: String(hero.title || "").trim(),
        description: String(hero.description || "").trim(),
        metrics: Array.isArray(hero.metrics)
          ? hero.metrics.map((metric) => ({
            label: String(metric?.label || "").trim(),
            value: String(metric?.value || "").trim(),
          })).filter((metric) => metric.label || metric.value)
          : [],
      };
    }
    return next;
  });
  const dashboardActiveModuleHero = computed(() => {
    const active = moduleMeta.value?.[dashboardActiveModule.value] || {};
    const backendHero = backendDashboardModuleHeroMap.value?.[dashboardActiveModule.value];
    if (backendHero && typeof backendHero === "object") {
      return {
        eyebrow: backendHero.eyebrow || active.group_title || "业务模块",
        title: backendHero.title || active.title || "业务模块",
        description: backendHero.description || active.desc || active.group_title || "当前模块",
        metrics: Array.isArray(backendHero.metrics) ? backendHero.metrics : [],
      };
    }
    return {
      eyebrow: active.group_title || "业务模块",
      title: active.title || "业务模块",
      description: active.desc || active.group_title || "当前模块",
      metrics: [],
    };
  });
  const handoverConfigBuildingOptions = computed(() => {
    const buildings = Array.isArray(config.value?.input?.buildings) ? config.value.input.buildings : [];
    const normalized = buildings
      .map((item) => String(item || "").trim())
      .filter(Boolean);
    const fallback = ["A楼", "B楼", "C楼", "D楼", "E楼"];
    const list = normalized.length ? normalized : fallback;
    return list.map((building) => ({ value: building, label: building }));
  });
  const handoverRuleScopeOptions = computed(() => {
    const currentBuilding = String(handoverConfigBuilding.value || "").trim() || "A楼";
    return [
      { value: "default", label: "全局默认" },
      { value: currentBuilding, label: `${currentBuilding}覆盖` },
    ];
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
    bridgeTasksDisplay,
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
    internalRuntimeSummary,
    internalBuildingRuntimeStatusMap,
    runtimeWarmupReady,
    engineerDirectoryLoaded,
    pendingResumeRuns,
    schedulerQuickSaving,
    handoverSchedulerQuickSaving,
    wetBulbSchedulerQuickSaving,
    dayMetricUploadSchedulerQuickSaving,
    alarmEventUploadSchedulerQuickSaving,
    monthlyEventReportSchedulerQuickSaving,
    monthlyChangeReportSchedulerQuickSaving,
    schedulerToggleState,
    configSaveSuspendDepth,
    configSaveStatus,
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
    handoverConfigBuilding,
    handoverConfigCommonRevision,
    handoverConfigCommonUpdatedAt,
    handoverConfigBuildingRevision,
    handoverConfigBuildingUpdatedAt,
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
    handoverGenerationStatusText,
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
    dashboardSystemOverview,
    dashboardSystemStatusItems,
    schedulerOverviewItems,
    schedulerOverviewSummary,
    internalRuntimeBridgeSnapshot,
    internalDownloadPoolOverview,
    internalSourceCacheOverview,
    internalRealtimeSourceFamilies,
    externalInternalAlertOverview,
    currentHourRefreshOverview,
    internalRuntimeOverview,
    internalSourceCacheHistoryOverview,
    sharedSourceCacheReadinessOverview,
    sharedRootDiagnosticOverview,
    updaterMirrorOverview,
    currentTaskOverview,
    taskPanelOverview,
    bridgeTaskPanelOverview,
    homeOverview,
    homeQuickActionsById,
    statusDiagnosisOverview,
    statusQuickActionsById,
    configGuidanceOverview,
    externalAlarmUploadOverview,
    monthlyEventReportLastRunOverview,
    monthlyChangeReportLastRunOverview,
    monthlyEventReportDeliveryOverview,
    monthlyChangeReportDeliveryOverview,
    monthlyEventReportRecipientRows,
    monthlyChangeReportRecipientRows,
    monthlyEventReportDeliveryLastRun,
    monthlyChangeReportDeliveryLastRun,
    dashboardScheduleOverview,
    dashboardScheduleStatusItems,
    handoverReviewOverview,
    handoverFollowupProgress,
    handoverDailyReportAuthVm,
    handoverDailyReportExportVm,
    handoverDailyReportSpreadsheetUrl,
    handoverDailyReportActions,
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
    backendDashboardModuleHeroMap,
    dashboardActiveModuleHero,
    handoverConfigBuildingOptions,
    handoverRuleScopeOptions,
    syncCustomWindowLocalInputs,
    actionGuard,
  };
}



