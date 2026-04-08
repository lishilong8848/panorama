import { createLogStreamController } from "./log_stream.js";
import { createDashboardActions } from "./dashboard_actions.js";
import { createDateHandoverActions } from "./date_handover_actions.js";
import { createRuntimeActions } from "./runtime_actions.js";
import { createUiLocalActions } from "./ui_local_actions.js";
import { createAppState } from "./app_state.js";
import { registerAppLifecycle } from "./app_lifecycle.js";
import { APP_TEMPLATE } from "./app_template.js";
import { isHandoverReviewPath, mountHandoverReviewApp } from "./handover_review_app.js";
import { clone, expandDateRange, todayText } from "./config_helpers.js";

const { createApp, onMounted, onBeforeUnmount, computed, watch, ref } = Vue;
const HANDOVER_DUTY_CONTEXT_STORAGE_KEY = "handover_duty_context";
const APP_BOOT_OVERLAY_ID = "app-boot-overlay";
const STARTUP_ROLE_RESTART_PENDING_KEY = "startup_role_restart_pending_v1";
const STARTUP_ROLE_RESTART_RESUME_KEY = "startup_role_restart_resume_v1";
const STARTUP_ROLE_RESTART_PENDING_TTL_MS = 5 * 60 * 1000;

function normalizeDeploymentRoleMode(value) {
  const text = String(value || "").trim().toLowerCase();
  if (["internal", "external"].includes(text)) return text;
  return "";
}

function formatDeploymentRoleLabel(value) {
  const role = normalizeDeploymentRoleMode(value);
  if (role === "internal") return "内网端";
  if (role === "external") return "外网端";
  return "待选择角色";
}

function formatDateTimeFromEpoch(value) {
  const timestamp = Number.parseInt(String(value || 0), 10);
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "";
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "";
  const pad = (num) => String(num).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

function normalizeReceiveIdsText(value) {
  const sourceItems = Array.isArray(value) ? value : [value];
  const values = [];
  for (const item of sourceItems) {
    const text = String(item || "").trim();
    if (!text) continue;
    const parts = text.split(/[\s,，;；\r\n]+/).map((segment) => String(segment || "").trim()).filter(Boolean);
    values.push(...parts);
  }
  return values.filter((item, index, list) => list.indexOf(item) === index);
}

const STARTUP_BRIDGE_DEFAULTS = Object.freeze({
  root_dir: "",
  poll_interval_sec: 2,
  heartbeat_interval_sec: 5,
  claim_lease_sec: 30,
  stale_task_timeout_sec: 1800,
  artifact_retention_days: 7,
  sqlite_busy_timeout_ms: 5000,
});

function resolveSharedBridgeRoleRoot(config, roleMode) {
  const role = normalizeDeploymentRoleMode(roleMode);
  const sharedBridge = config && typeof config.shared_bridge === "object" ? config.shared_bridge : {};
  const legacyRoot = String(sharedBridge.root_dir || "").trim();
  if (role === "internal") {
    return String(sharedBridge.internal_root_dir || legacyRoot || "").trim();
  }
  if (role === "external") {
    return String(sharedBridge.external_root_dir || legacyRoot || "").trim();
  }
  return legacyRoot;
}

function normalizePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function buildStartupBridgeDraft(config, roleMode = "") {
  const deployment = config && typeof config.deployment === "object" ? config.deployment : {};
  const sharedBridge = config && typeof config.shared_bridge === "object" ? config.shared_bridge : {};
  const effectiveRole = normalizeDeploymentRoleMode(roleMode || deployment.role_mode || "");
  return {
    root_dir: resolveSharedBridgeRoleRoot(config, effectiveRole),
    poll_interval_sec: normalizePositiveInteger(
      sharedBridge.poll_interval_sec,
      STARTUP_BRIDGE_DEFAULTS.poll_interval_sec,
    ),
    heartbeat_interval_sec: normalizePositiveInteger(
      sharedBridge.heartbeat_interval_sec,
      STARTUP_BRIDGE_DEFAULTS.heartbeat_interval_sec,
    ),
    claim_lease_sec: normalizePositiveInteger(
      sharedBridge.claim_lease_sec,
      STARTUP_BRIDGE_DEFAULTS.claim_lease_sec,
    ),
    stale_task_timeout_sec: normalizePositiveInteger(
      sharedBridge.stale_task_timeout_sec,
      STARTUP_BRIDGE_DEFAULTS.stale_task_timeout_sec,
    ),
    artifact_retention_days: normalizePositiveInteger(
      sharedBridge.artifact_retention_days,
      STARTUP_BRIDGE_DEFAULTS.artifact_retention_days,
    ),
    sqlite_busy_timeout_ms: normalizePositiveInteger(
      sharedBridge.sqlite_busy_timeout_ms,
      STARTUP_BRIDGE_DEFAULTS.sqlite_busy_timeout_ms,
    ),
  };
}

function validateStartupBridgeDraft(roleMode, draft) {
  const role = normalizeDeploymentRoleMode(roleMode);
  if (!["internal", "external"].includes(role)) return "";
  if (!String(draft?.root_dir || "").trim()) {
    return "请先填写共享目录后再切换。";
  }
  const numericRules = [
    ["poll_interval_sec", "轮询间隔", 1],
    ["heartbeat_interval_sec", "心跳间隔", 1],
    ["claim_lease_sec", "阶段租约", 5],
    ["stale_task_timeout_sec", "任务超时", 60],
    ["artifact_retention_days", "产物保留天数", 1],
    ["sqlite_busy_timeout_ms", "SQLite 忙等待", 1000],
  ];
  for (const [field, label, minValue] of numericRules) {
    const value = Number.parseInt(String(draft?.[field] ?? "").trim(), 10);
    if (!Number.isFinite(value) || value < minValue) {
      return `${label}必须大于等于 ${minValue}。`;
    }
  }
  return "";
}

function isStartupBridgeDraftChanged(config, draft, roleMode = "") {
  const current = buildStartupBridgeDraft(config, roleMode);
  return (
    current.root_dir !== String(draft?.root_dir || "").trim()
    || current.poll_interval_sec !== normalizePositiveInteger(draft?.poll_interval_sec, current.poll_interval_sec)
    || current.heartbeat_interval_sec !== normalizePositiveInteger(draft?.heartbeat_interval_sec, current.heartbeat_interval_sec)
    || current.claim_lease_sec !== normalizePositiveInteger(draft?.claim_lease_sec, current.claim_lease_sec)
    || current.stale_task_timeout_sec !== normalizePositiveInteger(draft?.stale_task_timeout_sec, current.stale_task_timeout_sec)
    || current.artifact_retention_days !== normalizePositiveInteger(draft?.artifact_retention_days, current.artifact_retention_days)
    || current.sqlite_busy_timeout_ms !== normalizePositiveInteger(draft?.sqlite_busy_timeout_ms, current.sqlite_busy_timeout_ms)
  );
}

function buildRoleNodeIdPreview(currentNodeId, currentRole, targetRole) {
  const runtimeNodeId = String(currentNodeId || "").trim();
  const normalizedCurrentRole = normalizeDeploymentRoleMode(currentRole);
  const normalizedTargetRole = normalizeDeploymentRoleMode(targetRole);
  if (!runtimeNodeId) {
    return "切换后自动生成并长期固定";
  }
  if (normalizedCurrentRole === normalizedTargetRole) {
    return runtimeNodeId;
  }
  const autoPrefix = `${normalizedCurrentRole}-`;
  if (runtimeNodeId.startsWith(autoPrefix) && runtimeNodeId.length > autoPrefix.length) {
    return `${normalizedTargetRole}-${runtimeNodeId.slice(autoPrefix.length)}`;
  }
  return "切换后自动生成并长期固定";
}

function dismissBootOverlay() {
  if (typeof window === "undefined" || typeof document === "undefined") return;
  document.body?.classList.remove("app-boot-pending");
  const overlay = document.getElementById(APP_BOOT_OVERLAY_ID);
  if (!overlay) return;
  overlay.classList.add("is-hidden");
  window.setTimeout(() => {
    if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
  }, 260);
}

function finishAppBoot() {
  if (typeof window === "undefined") return;
  const run = () => {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(dismissBootOverlay);
    });
  };
  if (document.readyState === "complete") {
    run();
    return;
  }
  window.addEventListener("load", run, { once: true });
}

function persistHandoverDutyContext(dutyDate, dutyShift) {
  if (typeof window === "undefined" || !window.localStorage) return;
  const nextDutyDate = String(dutyDate || "").trim();
  const nextDutyShift = String(dutyShift || "").trim().toLowerCase();
  if (!nextDutyDate || !["day", "night"].includes(nextDutyShift)) return;
  try {
    window.localStorage.setItem(
      HANDOVER_DUTY_CONTEXT_STORAGE_KEY,
      JSON.stringify({
        duty_date: nextDutyDate,
        duty_shift: nextDutyShift,
        updated_at: Date.now(),
      }),
    );
  } catch (_) {
    // ignore localStorage errors
  }
}

function readStartupRoleRestartPending() {
  if (typeof window === "undefined" || !window.sessionStorage) return null;
  try {
    const raw = window.sessionStorage.getItem(STARTUP_ROLE_RESTART_PENDING_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const role = normalizeDeploymentRoleMode(parsed?.role_mode);
    const requestedAt = Number.parseInt(String(parsed?.requested_at || 0), 10);
    if (!role || !Number.isFinite(requestedAt) || requestedAt <= 0) {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_PENDING_KEY);
      return null;
    }
    if (Date.now() - requestedAt > STARTUP_ROLE_RESTART_PENDING_TTL_MS) {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_PENDING_KEY);
      return null;
    }
    return {
      role_mode: role,
      requested_at: requestedAt,
      source_startup_token: String(parsed?.source_startup_token || "").trim(),
    };
  } catch (_) {
    try {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_PENDING_KEY);
    } catch (_) {
      // ignore sessionStorage errors
    }
    return null;
  }
}

function writeStartupRoleRestartPending(roleMode, sourceStartupToken = "") {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  const role = normalizeDeploymentRoleMode(roleMode);
  if (!role) return;
  try {
    window.sessionStorage.setItem(
      STARTUP_ROLE_RESTART_PENDING_KEY,
      JSON.stringify({
        role_mode: role,
        requested_at: Date.now(),
        source_startup_token: String(sourceStartupToken || "").trim(),
      }),
    );
  } catch (_) {
    // ignore sessionStorage errors
  }
}

function readStartupRoleRestartResume() {
  if (typeof window === "undefined" || !window.sessionStorage) return null;
  try {
    const raw = window.sessionStorage.getItem(STARTUP_ROLE_RESTART_RESUME_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const role = normalizeDeploymentRoleMode(parsed?.role_mode);
    const requestedAt = Number.parseInt(String(parsed?.requested_at || 0), 10);
    const sourceStartupToken = String(parsed?.source_startup_token || "").trim();
    if (!role || !Number.isFinite(requestedAt) || requestedAt <= 0 || !sourceStartupToken) {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_RESUME_KEY);
      return null;
    }
    if (Date.now() - requestedAt > STARTUP_ROLE_RESTART_PENDING_TTL_MS) {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_RESUME_KEY);
      return null;
    }
    return {
      role_mode: role,
      requested_at: requestedAt,
      source_startup_token: sourceStartupToken,
    };
  } catch (_) {
    try {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_RESUME_KEY);
    } catch (_) {
      // ignore sessionStorage errors
    }
    return null;
  }
}

function clearStartupRoleRestartPending() {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  try {
    window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_PENDING_KEY);
  } catch (_) {
    // ignore sessionStorage errors
  }
}

function clearStartupRoleRestartResume() {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  try {
    window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_RESUME_KEY);
  } catch (_) {
    // ignore sessionStorage errors
  }
}

if (isHandoverReviewPath(window.location.pathname)) {
  mountHandoverReviewApp(Vue);
  finishAppBoot();
} else {
createApp({
  setup() {
    const state = createAppState(Vue);
    const {
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
      busy,
      message,
      bootstrapReady,
      fullHealthLoaded,
      configLoaded,
      healthLoadError,
      configLoadError,
      engineerDirectoryLoaded,
      initialLoadingPhase,
      initialLoadingStatusText,
      pendingResumeRuns,
      schedulerQuickSaving,
      handoverSchedulerQuickSaving,
      wetBulbSchedulerQuickSaving,
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
      handoverDailyReportCaptureAssets,
      handoverDailyReportLastScreenshotTest,
      handoverDailyReportPreviewModal,
      handoverDailyReportUploadModal,
      handoverRuleScope,
      handoverDutyAutoFollow,
      handoverDutyLastAutoAt,
      customAbsoluteStartLocal,
      customAbsoluteEndLocal,
      systemLogOffset,
      streamController,
      timers,
      filteredLogs,
      canRun,
      isStatusView,
      isDashboardView,
      isConfigView,
      selectedDateCount,
      dayMetricSelectedDateCount,
      pendingResumeCount,
      dayMetricUploadEnabled,
      dayMetricLocalImportEnabled,
      dayMetricCurrentPayload,
      dayMetricCurrentResultRows,
      dayMetricRetryableFailedCount,
      dayMetricRetryAllMode,
      handoverGenerationBusy,
      runningJobs,
      waitingResourceJobs: baseWaitingResourceJobs,
      recentFinishedJobs,
      bridgeTasksEnabled,
      activeBridgeTasks,
      displayedBridgeTasks,
      totalBridgeHistoryCount,
      hiddenBridgeHistoryCount,
      bridgeTaskHistoryDisplayLimit,
      recentFinishedBridgeTasks,
      currentBridgeTask,
      resourceSnapshot,
      handoverDutyAutoLabel,
      schedulerDecisionText,
      schedulerTriggerText,
      wetBulbSchedulerDecisionText,
      wetBulbSchedulerTriggerText,
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
      dashboardScheduleStatusItems,
      isInternalRole,
      internalDownloadPoolOverview,
      internalSourceCacheOverview,
      internalRealtimeSourceFamilies,
      externalInternalAlertOverview,
      currentHourRefreshOverview,
      internalRuntimeOverview,
      internalSourceCacheHistoryOverview,
      sharedSourceCacheReadinessOverview,
      updaterMirrorOverview,
      handoverReviewOverview,
      handoverFollowupProgress,
      handoverDailyReportAuthVm,
      handoverDailyReportExportVm,
      handoverDailyReportSpreadsheetUrl,
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
    } = state;
    const { runSingleFlight, isActionLocked } = actionGuard;
    const updaterUiOverlayVisible = ref(false);
    const updaterUiOverlayTitle = ref("");
    const updaterUiOverlaySubtitle = ref("");
    const updaterUiOverlayStage = ref("");
    const updaterUiOverlayKicker = ref("");
    const updaterAwaitingRestartRecovery = ref(false);
    const startupRoleSelectorVisible = ref(false);
    const startupRoleDecisionReady = ref(false);
    const startupRoleSelectorBusy = ref(false);
    const startupRoleSelectorSelection = ref("internal");
    const startupRoleSelectorMessage = ref("");
    const startupRoleSelectorHandled = ref(false);
    const startupRoleLoadingVisible = ref(false);
    const startupRoleLoadingTitle = ref("");
    const startupRoleLoadingSubtitle = ref("");
    const startupRoleLoadingStage = ref("");
    const startupRoleAutoActivationKey = ref("");
    const startupRoleSuppressedHandoffNonce = ref("");
    const startupRoleFlowState = ref("selecting");
    const startupRoleBridgeDraft = ref(buildStartupBridgeDraft({}));
    const startupRoleAdvancedVisible = ref(false);
    const startupRoleOptions = Object.freeze([
      {
        value: "internal",
        label: "内网端",
        description: "只负责内网下载、查询、采集，并把产物写入共享目录。",
      },
      {
        value: "external",
        label: "外网端",
        description: "统一发起协同任务，优先读取共享文件；缺失时再等待内网补采。",
      },
    ]);
    clearStartupRoleRestartPending();
    clearStartupRoleRestartResume();

    const actionKeyAutoOnce = "job:auto_once";
    const actionKeyMultiDate = "job:multi_date";
    const actionKeyManualUpload = "job:manual_upload";
    const actionKeySheetImport = "job:sheet_import";
    const actionKeyHandoverFromFile = "job:handover_from_file";
    const actionKeyHandoverFromDownload = "job:handover_from_download";
    const actionKeyDayMetricFromDownload = "job:day_metric_from_download";
    const actionKeyDayMetricFromFile = "job:day_metric_from_file";
    const actionKeyDayMetricRetryUnit = "job:day_metric_retry_unit";
    const actionKeyDayMetricRetryFailed = "job:day_metric_retry_failed";
    const actionKeySchedulerStart = "scheduler:start";
    const actionKeySchedulerStop = "scheduler:stop";
    const actionKeySchedulerSave = "scheduler:save";
    const actionKeyHandoverSchedulerStart = "handover_scheduler:start";
    const actionKeyHandoverSchedulerStop = "handover_scheduler:stop";
    const actionKeyHandoverSchedulerSave = "handover_scheduler:save";
    const actionKeyWetBulbCollectionRun = "job:wet_bulb_collection";
    const actionKeyWetBulbSchedulerStart = "wet_bulb_scheduler:start";
    const actionKeyWetBulbSchedulerStop = "wet_bulb_scheduler:stop";
    const actionKeyWetBulbSchedulerSave = "wet_bulb_scheduler:save";
    const actionKeyMonthlyEventReportRunAll = "job:monthly_event_report:all";
    const actionKeyMonthlyEventReportRunBuildingPrefix = "job:monthly_event_report:building:";
    const actionKeyMonthlyChangeReportRunAll = "job:monthly_change_report:all";
    const actionKeyMonthlyChangeReportRunBuildingPrefix = "job:monthly_change_report:building:";
    const actionKeyMonthlyEventReportSchedulerStart = "monthly_event_report_scheduler:start";
    const actionKeyMonthlyEventReportSchedulerStop = "monthly_event_report_scheduler:stop";
    const actionKeyMonthlyEventReportSchedulerSave = "monthly_event_report_scheduler:save";
    const actionKeyMonthlyChangeReportSchedulerStart = "monthly_change_report_scheduler:start";
    const actionKeyMonthlyChangeReportSchedulerStop = "monthly_change_report_scheduler:stop";
    const actionKeyMonthlyChangeReportSchedulerSave = "monthly_change_report_scheduler:save";
    const actionKeyMonthlyReportSendAllPrefix = "job:monthly_report_send:all:";
    const actionKeyMonthlyReportSendBuildingPrefix = "job:monthly_report_send:building:";
    const actionKeyMonthlyReportSendTestPrefix = "job:monthly_report_send:test:";
    const actionKeyConfigSave = "config:save";
    const actionKeyUpdaterCheck = "updater:check";
    const actionKeyUpdaterApply = "updater:apply";
    const actionKeyUpdaterRestart = "updater:restart";
    const actionKeySourceCacheRefreshCurrentHour = "bridge:source_cache_refresh_current_hour";
    const actionKeySourceCacheRefreshBuildingLatestPrefix = "bridge:source_cache_refresh_building_latest:";
    const actionKeySourceCacheRefreshAlarmManual = "bridge:source_cache_refresh_alarm_manual";
    const actionKeySourceCacheDeleteAlarmManual = "bridge:source_cache_delete_alarm_manual";
    const actionKeySourceCacheUploadAlarmFull = "bridge:source_cache_upload_alarm_full";
    const actionKeySourceCacheUploadAlarmBuildingPrefix = "bridge:source_cache_upload_alarm_building:";
    const actionKeyHandoverConfirmAll = "handover_review:confirm_all";
    const actionKeyHandoverCloudRetryAll = "handover_review:cloud_retry_all";
    const actionKeyHandoverFollowupContinue = "job:handover_followup_continue";
    const actionKeyHandoverDailyReportAuthOpen = "handover_daily_report:auth_open";
    const actionKeyHandoverDailyReportScreenshotTest = "handover_daily_report:screenshot_test";
    const actionKeyHandoverReviewAccessReprobe = "handover_review:access_reprobe";
    const isUpdaterActionLocked = computed(
      () =>
        isActionLocked(actionKeyUpdaterCheck) ||
        isActionLocked(actionKeyUpdaterApply) ||
        isActionLocked(actionKeyUpdaterRestart),
    );
    const isSourceCacheRefreshCurrentHourLocked = computed(() => isActionLocked(actionKeySourceCacheRefreshCurrentHour));
    const isSourceCacheRefreshAlarmManualLocked = computed(() => isActionLocked(actionKeySourceCacheRefreshAlarmManual));
    const isSourceCacheDeleteAlarmManualLocked = computed(() => isActionLocked(actionKeySourceCacheDeleteAlarmManual));
    const externalAlarmUploadBuilding = ref("全部楼栋");
    const monthlyReportTestReceiveIdDraftEvent = ref("");
    const monthlyReportTestReceiveIdDraftChange = ref("");
    const isAlarmSourceCacheUploadRunning = computed(() => Boolean(externalAlarmReadinessFamily.value?.uploadRunning));
    const isSourceCacheUploadAlarmFullLocked = computed(() =>
      isAlarmSourceCacheUploadRunning.value || isActionLocked(actionKeySourceCacheUploadAlarmFull),
    );
    const isSourceCacheUploadAlarmBuildingLocked = computed(() =>
      isAlarmSourceCacheUploadRunning.value ||
      isActionLocked(`${actionKeySourceCacheUploadAlarmBuildingPrefix}${String(externalAlarmUploadBuilding.value || "").trim()}`),
    );
    const isSourceCacheUploadAlarmSelectedLocked = computed(() => {
      const buildingText = String(externalAlarmUploadBuilding.value || "").trim();
      if (buildingText === "全部楼栋") {
        return isSourceCacheUploadAlarmFullLocked.value;
      }
      return isAlarmSourceCacheUploadRunning.value
        || isActionLocked(`${actionKeySourceCacheUploadAlarmBuildingPrefix}${buildingText}`);
    });
    const currentHourRefreshButtonText = computed(() =>
      isSourceCacheRefreshCurrentHourLocked.value ? "下载中..." : "立即下载当前小时全部文件",
    );
    const manualAlarmRefreshButtonText = computed(() =>
      isSourceCacheRefreshAlarmManualLocked.value ? "拉取中..." : "一键拉取告警文件",
    );
    const manualAlarmDeleteButtonText = computed(() =>
      isSourceCacheDeleteAlarmManualLocked.value ? "删除中..." : "删除手动告警文件",
    );
    function getInternalSourceCacheRefreshActionKey(sourceFamily, building) {
      const sourceFamilyText = String(sourceFamily || "").trim();
      const buildingText = String(building || "").trim();
      if (typeof getSourceCacheRefreshBuildingActionKey === "function") {
        return getSourceCacheRefreshBuildingActionKey(sourceFamilyText, buildingText);
      }
      return `${actionKeySourceCacheRefreshBuildingLatestPrefix}${sourceFamilyText}:${buildingText}`;
    }
    function getInternalSourceCacheRefreshDisabledReason(family, building) {
      const familyKey = String(family?.key || "").trim();
      const buildingName = String(building?.building || "").trim();
      if (!familyKey || !buildingName) return "缺少楼栋或文件类型";
      if (String(building?.statusKey || "").trim().toLowerCase() === "downloading") return "";
      if (
        familyKey === "alarm_event_family"
        && Number.parseInt(String(new Date().getHours()), 10) < 8
      ) {
        return "当前不在告警定时窗口，请使用一键拉取告警文件";
      }
      return "";
    }
    function isInternalSourceCacheRefreshLocked(family, building) {
      const actionKey = getInternalSourceCacheRefreshActionKey(family?.key, building?.building);
      if (String(building?.statusKey || "").trim().toLowerCase() === "downloading") return true;
      if (getInternalSourceCacheRefreshDisabledReason(family, building)) return true;
      return isActionLocked(actionKey);
    }
    function getInternalSourceCacheRefreshButtonText(family, building) {
      const actionKey = getInternalSourceCacheRefreshActionKey(family?.key, building?.building);
      if (String(building?.statusKey || "").trim().toLowerCase() === "downloading" || isActionLocked(actionKey)) {
        return "拉取中...";
      }
      return "重新拉取";
    }
    const externalAlarmUploadActionButtonText = computed(() => {
      if (isAlarmSourceCacheUploadRunning.value) return "上传进行中...";
      const buildingText = String(externalAlarmUploadBuilding.value || "").trim();
      if (buildingText === "全部楼栋") {
        return isActionLocked(actionKeySourceCacheUploadAlarmFull) ? "上传中..." : "使用共享文件上传60天";
      }
      return isActionLocked(`${actionKeySourceCacheUploadAlarmBuildingPrefix}${buildingText}`)
        ? "上传中..."
        : "使用共享文件上传60天";
    });
    async function uploadSelectedAlarmSourceCache() {
      const buildingText = String(externalAlarmUploadBuilding.value || "").trim();
      if (!buildingText || buildingText === "全部楼栋") {
        return uploadAlarmSourceCacheFull();
      }
      return uploadAlarmSourceCacheBuilding(buildingText);
    }
    const externalAlarmReadinessFamily = computed(() => {
      const families = Array.isArray(sharedSourceCacheReadinessOverview.value?.families)
        ? sharedSourceCacheReadinessOverview.value.families
        : [];
      return (
        families.find((item) => String(item?.key || "").trim() === "alarm_event_family") || {
          key: "alarm_event_family",
          title: "告警信息源文件",
          tone: "neutral",
          statusText: "暂无状态",
          summaryText: "当前还没有告警文件状态。",
          buildings: [],
          uploadLastRunAt: "",
          uploadLastSuccessAt: "",
          uploadLastError: "",
          uploadRecordCount: 0,
          uploadFileCount: 0,
          uploadRunning: false,
          uploadStartedAt: "",
          uploadCurrentMode: "",
          uploadCurrentScope: "",
          uploadRunningText: "",
        }
      );
    });
    const externalAlarmUploadStatus = computed(() => {
      const family = externalAlarmReadinessFamily.value || {};
      const uploadRunning = Boolean(family.uploadRunning);
      const uploadLastError = String(family.uploadLastError || "").trim();
      const uploadLastRunAt = String(family.uploadLastRunAt || "").trim();
      const uploadLastSuccessAt = String(family.uploadLastSuccessAt || "").trim();
      const uploadRecordCount = Number.parseInt(String(family.uploadRecordCount || 0), 10) || 0;
      const uploadFileCount = Number.parseInt(String(family.uploadFileCount || 0), 10) || 0;
      if (uploadRunning) {
        return {
          tone: "info",
          statusText: "上传进行中",
          summaryText: String(family.uploadRunningText || "").trim() || "外网正在上传告警信息文件。",
        };
      }
      if (uploadLastError) {
        return {
          tone: "danger",
          statusText: "最近上传失败",
          summaryText: `最近上传：${uploadLastRunAt || "-"}。${uploadLastError}`,
        };
      }
      if (uploadLastSuccessAt) {
        return {
          tone: "success",
          statusText: "最近上传成功",
          summaryText: `最近上传：${uploadLastRunAt || uploadLastSuccessAt}（记录 ${uploadRecordCount} 条，文件 ${uploadFileCount} 份，源文件保留）。`,
        };
      }
      return {
        tone: family.tone || "warning",
        statusText: "尚未上传",
        summaryText: "尚未执行告警信息上传。",
      };
    });
    const monthlyEventReportLastRun = computed(() => health.monthly_event_report?.last_run || {});
    const monthlyEventReportOutputDir = computed(() =>
      String(config.value?.handover_log?.monthly_event_report?.template?.output_dir || "").trim()
      || String(monthlyEventReportLastRun.value?.output_dir || "").trim()
      || "-"
    );
    const monthlyChangeReportLastRun = computed(() => health.monthly_change_report?.last_run || {});
    const monthlyChangeReportOutputDir = computed(() =>
      String(config.value?.handover_log?.monthly_change_report?.template?.output_dir || "").trim()
      || String(monthlyChangeReportLastRun.value?.output_dir || "").trim()
      || "-"
    );
    const monthlyEventReportDelivery = computed(() => health.monthly_event_report?.delivery || {});
    const monthlyChangeReportDelivery = computed(() => health.monthly_change_report?.delivery || {});
    const monthlyEventReportDeliveryLastRun = computed(() => monthlyEventReportDelivery.value?.last_run || {});
    const monthlyChangeReportDeliveryLastRun = computed(() => monthlyChangeReportDelivery.value?.last_run || {});
    const monthlyEventReportDeliveryError = computed(() => String(monthlyEventReportDelivery.value?.error || "").trim());
    const monthlyChangeReportDeliveryError = computed(() => String(monthlyChangeReportDelivery.value?.error || "").trim());
    function mapMonthlyReportRecipientStatusRows(deliveryState) {
      const rows = Array.isArray(deliveryState?.recipient_status_by_building)
        ? deliveryState.recipient_status_by_building
        : [];
      const rowMap = new Map(
        rows
          .filter((item) => item && typeof item === "object" && String(item.building || "").trim())
          .map((item) => [String(item.building || "").trim(), item]),
      );
      return ["A楼", "B楼", "C楼", "D楼", "E楼"].map((building) => {
        const item = rowMap.get(building) || {};
        const sendReady = Boolean(item.send_ready);
        return {
          building,
          supervisor: String(item.supervisor || "").trim(),
          position: String(item.position || "").trim(),
          recipientId: String(item.recipient_id || "").trim(),
          receiveIdType: String(item.receive_id_type || "").trim() || "user_id",
          sendReady,
          reason: String(item.reason || "").trim(),
          fileName: String(item.file_name || "").trim(),
          filePath: String(item.file_path || "").trim(),
          fileExists: Boolean(item.file_exists),
          tone: sendReady ? "success" : "warning",
          statusText: sendReady ? "可发送" : "不可发送",
          detailText: sendReady ? "已匹配设施运维主管并找到可发送文件。" : (String(item.reason || "").trim() || "当前楼栋不可发送。"),
        };
      });
    }
    const monthlyEventReportRecipientStatusByBuilding = computed(() =>
      mapMonthlyReportRecipientStatusRows(monthlyEventReportDelivery.value),
    );
    const monthlyChangeReportRecipientStatusByBuilding = computed(() =>
      mapMonthlyReportRecipientStatusRows(monthlyChangeReportDelivery.value),
    );
    const monthlyEventReportSendReadyCount = computed(() =>
      monthlyEventReportRecipientStatusByBuilding.value.filter((item) => item.sendReady).length,
    );
    const monthlyChangeReportSendReadyCount = computed(() =>
      monthlyChangeReportRecipientStatusByBuilding.value.filter((item) => item.sendReady).length,
    );
    function resolveMonthlyReportTargetMonth(reportType) {
      const normalizedReportType = String(reportType || "event").trim().toLowerCase() === "change" ? "change" : "event";
      const sourceLastRun = normalizedReportType === "change" ? monthlyChangeReportLastRun.value : monthlyEventReportLastRun.value;
      return String(sourceLastRun?.target_month || "").trim() || "latest";
    }
    const monthlyEventReportSendAllActionKey = computed(() => {
      return `${actionKeyMonthlyReportSendAllPrefix}event:${resolveMonthlyReportTargetMonth("event")}`;
    });
    const monthlyChangeReportSendAllActionKey = computed(() => {
      return `${actionKeyMonthlyReportSendAllPrefix}change:${resolveMonthlyReportTargetMonth("change")}`;
    });
    const monthlyEventReportSendTestActionKey = computed(() => {
      return `${actionKeyMonthlyReportSendTestPrefix}event:${resolveMonthlyReportTargetMonth("event")}`;
    });
    const monthlyChangeReportSendTestActionKey = computed(() => {
      return `${actionKeyMonthlyReportSendTestPrefix}change:${resolveMonthlyReportTargetMonth("change")}`;
    });
    function getMonthlyReportTestDeliveryConfig() {
      const handover = config.value?.handover_log;
      if (!handover || typeof handover !== "object") return null;
      const monthly = handover.monthly_event_report;
      if (!monthly || typeof monthly !== "object") return null;
      const delivery = monthly.test_delivery;
      if (!delivery || typeof delivery !== "object") return null;
      return delivery;
    }
    function ensureMonthlyReportTestDeliveryConfig() {
      if (!config.value || typeof config.value !== "object") return null;
      config.value.handover_log = config.value.handover_log && typeof config.value.handover_log === "object"
        ? config.value.handover_log
        : {};
      const handover = config.value.handover_log;
      handover.monthly_event_report = handover.monthly_event_report && typeof handover.monthly_event_report === "object"
        ? handover.monthly_event_report
        : {};
      const monthly = handover.monthly_event_report;
      monthly.test_delivery = monthly.test_delivery && typeof monthly.test_delivery === "object"
        ? monthly.test_delivery
        : {};
      const delivery = monthly.test_delivery;
      delivery.receive_id_type = String(delivery.receive_id_type || "open_id").trim() || "open_id";
      delivery.receive_ids = normalizeReceiveIdsText(delivery.receive_ids || []);
      return delivery;
    }
    const monthlyReportTestReceiveIdType = computed({
      get() {
        return String(getMonthlyReportTestDeliveryConfig()?.receive_id_type || "open_id").trim() || "open_id";
      },
      set(value) {
        const delivery = ensureMonthlyReportTestDeliveryConfig();
        if (!delivery) return;
        delivery.receive_id_type = String(value || "open_id").trim() || "open_id";
      },
    });
    const monthlyReportTestReceiveIds = computed(() =>
      normalizeReceiveIdsText(getMonthlyReportTestDeliveryConfig()?.receive_ids || []),
    );
    const monthlyReportTestReceiveCount = computed(() => monthlyReportTestReceiveIds.value.length);
    function addMonthlyReportTestReceiveId(reportType = "event") {
      const draftRef = String(reportType || "event").trim().toLowerCase() === "change"
        ? monthlyReportTestReceiveIdDraftChange
        : monthlyReportTestReceiveIdDraftEvent;
      const candidate = String(draftRef.value || "").trim();
      if (!candidate) {
        message.value = "请先输入一个测试接收人 ID。";
        return;
      }
      const delivery = ensureMonthlyReportTestDeliveryConfig();
      if (!delivery) return;
      const nextIds = normalizeReceiveIdsText([...(delivery.receive_ids || []), candidate]);
      if (nextIds.length === delivery.receive_ids.length) {
        message.value = "该测试接收人 ID 已存在。";
        draftRef.value = "";
        return;
      }
      delivery.receive_ids = nextIds;
      draftRef.value = "";
      message.value = "测试接收人 ID 已加入当前配置，请点击“保存测试配置”。";
    }
    function removeMonthlyReportTestReceiveId(targetId) {
      const delivery = ensureMonthlyReportTestDeliveryConfig();
      if (!delivery) return;
      const target = String(targetId || "").trim();
      delivery.receive_ids = (delivery.receive_ids || []).filter((item) => String(item || "").trim() !== target);
      message.value = "测试接收人 ID 已从当前配置移除，请点击“保存测试配置”。";
    }
    function getMonthlyReportSendBuildingActionKey(reportType, building) {
      const normalizedReportType = String(reportType || "event").trim().toLowerCase() === "change" ? "change" : "event";
      const monthText = resolveMonthlyReportTargetMonth(normalizedReportType);
      return `${actionKeyMonthlyReportSendBuildingPrefix}${normalizedReportType}:${String(building || "").trim()}:${monthText}`;
    }
    function buildMonthlyReportDeliveryStatus({ deliveryLastRun, deliveryError, reportLastRun, sendReadyCount, reportLabel }) {
      const lastRun = deliveryLastRun || {};
      const status = String(lastRun.status || "").trim().toLowerCase();
      const isTestMode = Boolean(lastRun.test_mode);
      if (deliveryError) {
        return {
          tone: "danger",
          statusText: "发送前置检查失败",
          summaryText: deliveryError,
        };
      }
      if (status === "success") {
        const successReceiverCount = Array.isArray(lastRun.test_successful_receivers)
          ? lastRun.test_successful_receivers.length
          : 0;
        return {
          tone: "success",
          statusText: isTestMode ? "最近测试发送成功" : "最近发送成功",
          summaryText: isTestMode
            ? `最近测试发送：${lastRun.finished_at || lastRun.started_at || "-"}，成功发送 ${successReceiverCount} 人，文件 ${String(lastRun.test_file_building || "-")} / ${String(lastRun.test_file_name || "-")}`
            : `最近发送：${lastRun.finished_at || lastRun.started_at || "-"}，成功 ${Array.isArray(lastRun.successful_buildings) ? lastRun.successful_buildings.length : 0} 楼。`,
        };
      }
      if (status === "partial_failed") {
        const successReceiverCount = Array.isArray(lastRun.test_successful_receivers)
          ? lastRun.test_successful_receivers.length
          : 0;
        const failedReceiverCount = Array.isArray(lastRun.test_failed_receivers)
          ? lastRun.test_failed_receivers.length
          : 0;
        return {
          tone: "warning",
          statusText: isTestMode ? "最近测试发送部分失败" : "最近发送部分失败",
          summaryText: isTestMode
            ? `最近测试发送：${lastRun.finished_at || lastRun.started_at || "-"}，成功 ${successReceiverCount} 人，失败 ${failedReceiverCount} 人。`
            : `最近发送：${lastRun.finished_at || lastRun.started_at || "-"}，请查看失败楼栋并修正收件人或文件。`,
        };
      }
      if (status === "failed") {
        return {
          tone: "danger",
          statusText: isTestMode ? "最近测试发送失败" : "最近发送失败",
          summaryText: String(lastRun.error || "").trim() || (isTestMode ? "最近一次测试发送失败，请查看运行日志。" : "最近一次发送失败，请查看运行日志。"),
        };
      }
      if (!String(reportLastRun?.target_month || "").trim()) {
        return {
          tone: "neutral",
          statusText: "待生成",
          summaryText: `请先生成${reportLabel}月度统计表，再执行文件发送。`,
        };
      }
      return {
        tone: sendReadyCount > 0 ? "info" : "warning",
        statusText: sendReadyCount > 0 ? "待发送" : "缺少收件人",
        summaryText:
          sendReadyCount > 0
            ? `当前有 ${sendReadyCount}/5 个楼栋满足发送条件。`
            : "当前没有楼栋满足发送条件，请先检查工程师目录和最近生成文件。",
      };
    }
    const monthlyEventReportDeliveryStatus = computed(() =>
      buildMonthlyReportDeliveryStatus({
        deliveryLastRun: monthlyEventReportDeliveryLastRun.value,
        deliveryError: monthlyEventReportDeliveryError.value,
        reportLastRun: monthlyEventReportLastRun.value,
        sendReadyCount: monthlyEventReportSendReadyCount.value,
        reportLabel: "事件",
      }),
    );
    const monthlyChangeReportDeliveryStatus = computed(() =>
      buildMonthlyReportDeliveryStatus({
        deliveryLastRun: monthlyChangeReportDeliveryLastRun.value,
        deliveryError: monthlyChangeReportDeliveryError.value,
        reportLastRun: monthlyChangeReportLastRun.value,
        sendReadyCount: monthlyChangeReportSendReadyCount.value,
        reportLabel: "变更",
      }),
    );
    const handoverEngineerDirectoryTarget = computed(() => {
      const shiftRosterCfg = config.value?.handover_log?.shift_roster || {};
      const source = shiftRosterCfg?.engineer_directory?.source && typeof shiftRosterCfg.engineer_directory.source === "object"
        ? shiftRosterCfg.engineer_directory.source
        : {};
      const fallbackSource = shiftRosterCfg?.source && typeof shiftRosterCfg.source === "object"
        ? shiftRosterCfg.source
        : {};
      const preview = health.handover?.engineer_directory?.target_preview || {};
      const appToken = String(source.app_token || "").trim() || String(fallbackSource.app_token || "").trim();
      const tableId = String(source.table_id || "").trim();
      const displayUrl = String(preview?.display_url || preview?.bitable_url || "").trim();
      const targetKind = String(preview?.target_kind || "").trim();
      const hasConfiguredTokenPair = Boolean(appToken && tableId);
      return {
        appToken: String(preview?.configured_app_token || "").trim() || appToken,
        operationAppToken: String(preview?.operation_app_token || "").trim(),
        tableId: String(preview?.table_id || "").trim() || tableId,
        displayUrl,
        bitableUrl: displayUrl,
        targetKind,
        configured: hasConfiguredTokenPair,
        statusText: displayUrl ? "已解析" : hasConfiguredTokenPair ? "待解析" : "未配置",
        hintText:
          String(preview?.message || "").trim()
          || (
            targetKind === "wiki_token_pair" || targetKind === "wiki_url"
              ? "当前自动识别为 Wiki 多维表链接。"
              : targetKind === "base_token_pair" || targetKind === "base_url"
                ? "当前自动识别为 Base 多维表链接。"
                : hasConfiguredTokenPair
                  ? "保存配置后会自动解析工程师目录多维表链接。"
                  : "请先填写工程师目录多维 App Token 和 Table ID。"
          ),
      };
    });
    const alarmEventUploadTarget = computed(() => {
      const alarmExportCfg = config.value?.alarm_export || {};
      const legacyTarget = alarmExportCfg?.feishu && typeof alarmExportCfg.feishu === "object"
        ? alarmExportCfg.feishu
        : {};
      const sharedUploadCfg = alarmExportCfg?.shared_source_upload && typeof alarmExportCfg.shared_source_upload === "object"
        ? alarmExportCfg.shared_source_upload
        : {};
      const overrideTarget = sharedUploadCfg?.target && typeof sharedUploadCfg.target === "object"
        ? sharedUploadCfg.target
        : {};
      const mergedTarget = { ...legacyTarget, ...overrideTarget };
      const preview = health.alarm_event_upload?.target_preview || {};
      const appToken = String(mergedTarget.app_token || "").trim();
      const tableId = String(mergedTarget.table_id || "").trim();
      const baseUrl = String(mergedTarget.base_url || "").trim();
      const wikiUrl = String(mergedTarget.wiki_url || "").trim();
      const displayUrl = String(preview?.display_url || preview?.bitable_url || "").trim()
        || wikiUrl
        || baseUrl
        || (appToken && tableId ? `https://vnet.feishu.cn/base/${appToken}?table=${tableId}` : "");
      const targetKind = String(preview?.target_kind || "").trim()
        || (wikiUrl
          ? "wiki_url"
          : baseUrl
            ? "base_url"
            : appToken && tableId
              ? "token_pair"
              : "");
      const replaceExistingOnFull = sharedUploadCfg.replace_existing_on_full !== false;
      return {
        appToken: String(preview?.configured_app_token || "").trim() || appToken,
        operationAppToken: String(preview?.operation_app_token || "").trim(),
        tableId: String(preview?.table_id || "").trim() || tableId,
        baseUrl,
        wikiUrl,
        displayUrl,
        bitableUrl: displayUrl,
        targetKind,
        configured: Boolean(displayUrl || (appToken && tableId)),
        replaceExistingOnFull,
        statusText: displayUrl || (appToken && tableId) ? "已配置" : "未配置",
        hintText:
          String(preview?.message || "").trim()
          || (
            targetKind === "wiki_token_pair" || targetKind === "wiki_url"
              ? "当前自动识别为 Wiki 多维表链接。"
              : targetKind === "base_token_pair" || targetKind === "base_url"
                ? "当前自动识别为 Base 多维表链接。"
                : appToken && tableId
                  ? "当前按 App Token 和 Table ID 生成目标多维表链接。"
                  : "请先在配置中心的功能配置里补齐告警信息上传目标多维表。"
          ),
      };
    });
    const dayMetricUploadTarget = computed(() => {
      const exportCfg = config.value?.handover_log?.day_metric_export || {};
      const source = exportCfg?.source && typeof exportCfg.source === "object"
        ? exportCfg.source
        : {};
      const preview = health.day_metric_upload?.target_preview || {};
      const appToken = String(source.app_token || "").trim();
      const tableId = String(source.table_id || "").trim();
      const baseUrl = String(source.base_url || "").trim();
      const wikiUrl = String(source.wiki_url || "").trim();
      const displayUrl = String(preview?.display_url || preview?.bitable_url || "").trim()
        || wikiUrl
        || baseUrl
        || (appToken && tableId ? `https://vnet.feishu.cn/base/${appToken}?table=${tableId}` : "");
      const targetKind = String(preview?.target_kind || "").trim()
        || (wikiUrl
          ? "wiki_url"
          : baseUrl
            ? "base_url"
            : appToken && tableId
              ? "token_pair"
              : "");
      return {
        appToken: String(preview?.configured_app_token || "").trim() || appToken,
        operationAppToken: String(preview?.operation_app_token || "").trim(),
        tableId: String(preview?.table_id || "").trim() || tableId,
        baseUrl,
        wikiUrl,
        displayUrl,
        bitableUrl: displayUrl,
        targetKind,
        configured: Boolean(displayUrl || (appToken && tableId)),
        statusText: displayUrl || (appToken && tableId) ? "已配置" : "未配置",
        hintText:
          String(preview?.message || "").trim()
          || (
            targetKind === "wiki_token_pair" || targetKind === "wiki_url"
              ? "当前自动识别为 Wiki 多维表链接。"
              : targetKind === "base_token_pair" || targetKind === "base_url"
                ? "当前自动识别为 Base 多维表链接。"
                : appToken && tableId
                  ? "当前按 App Token 和 Table ID 生成目标多维表链接。"
                  : "请先在配置中心补齐 12 项独立上传目标多维表配置。"
          ),
      };
    });
    const effectiveRoleMode = computed(() =>
      normalizeDeploymentRoleMode(
        config.value?.deployment?.role_mode || health.deployment?.role_mode || "",
      ),
    );
    const deploymentRoleMode = computed(() => effectiveRoleMode.value);
    const isInternalDeploymentRole = computed(() => deploymentRoleMode.value === "internal");
    const isExternalDeploymentRole = computed(() => deploymentRoleMode.value === "external");
    const configRoleMode = computed(() =>
      normalizeDeploymentRoleMode(config.value?.deployment?.role_mode || deploymentRoleMode.value),
    );
    const showCommonPathsConfigTab = computed(() => configRoleMode.value !== "internal");
    const showCommonSchedulerConfigTab = computed(() => configRoleMode.value !== "internal");
    const showNotifyConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeishuAuthConfigTab = computed(() => configRoleMode.value !== "internal");
    const showCommonAlarmDbConfigTab = computed(() => configRoleMode.value === "internal");
    const showConsoleConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeatureMonthlyConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeatureHandoverConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeatureWetBulbCollectionConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeatureAlarmExportConfigTab = computed(() => configRoleMode.value !== "internal");
    const showSheetImportConfigTab = computed(() => configRoleMode.value !== "internal");
    const showManualFeatureConfigTab = computed(() => configRoleMode.value !== "internal");
    const showRuntimeNetworkPanel = computed(() => false);
    const showDashboardPageNav = computed(() => deploymentRoleMode.value !== "internal");
    const currentTaskOverview = computed(() => {
      const current = currentJob.value || runningJobs.value[0] || waitingResourceJobs.value[0] || null;
      const runningCount = Number(runningJobs.value.length || 0);
      const waitingCount = Number(waitingResourceJobs.value.length || 0);
      const bridgeCount = Number(activeBridgeTasks.value.length || 0);
      const recentFailure = (recentFinishedJobs.value || []).find((job) =>
        ["failed", "partial_failed", "blocked_precondition", "interrupted"].includes(
          String(job?.status || "").trim().toLowerCase(),
        ),
      );
      let tone = "neutral";
      let statusText = "当前空闲";
      let summaryText = "暂无长耗时任务，可直接从主动作开始。";
      let nextActionText = "需要细节时再展开“任务与资源”，避免先陷进状态细节。";
      if (runningCount > 0) {
        tone = "info";
        statusText = "有任务正在执行";
        summaryText = `当前有 ${runningCount} 个运行中任务${waitingCount > 0 ? `，另有 ${waitingCount} 个等待资源` : ""}。`;
        nextActionText = "优先盯住当前任务；长操作的结果、进度和错误都以任务区为准。";
      } else if (waitingCount > 0) {
        tone = "warning";
        statusText = "任务正在等待资源";
        summaryText = `当前有 ${waitingCount} 个任务等待资源，可先检查网络、共享桥接或浏览器池状态。`;
        nextActionText = "先处理资源阻塞，再决定是否重试任务。";
      } else if (bridgeCount > 0) {
        tone = "warning";
        statusText = "共享桥接仍在推进";
        summaryText = `当前有 ${bridgeCount} 个共享协同任务仍未结束。`;
        nextActionText = "优先查看共享协同任务，再执行新的跨机动作。";
      } else if (recentFailure) {
        tone = "danger";
        statusText = "最近有失败任务";
        summaryText = `最近失败任务：${recentFailure.name || recentFailure.feature || recentFailure.job_id || "-"}`;
        nextActionText = "先看失败摘要和任务详情，再决定是否重试。";
      }
      return {
        tone,
        statusText,
        summaryText,
        nextActionText,
        focusTitle: current ? (current.name || current.feature || current.job_id || "-") : "当前没有选中任务",
        focusMeta: current
          ? `${formatJobKind(current)} / ${formatJobStatus(current.status || "running")}`
          : "可以直接开始新的流程动作",
        items: [
          { label: "运行中任务", value: `${runningCount} 个`, tone: runningCount > 0 ? "info" : "neutral" },
          { label: "等待资源", value: `${waitingCount} 个`, tone: waitingCount > 0 ? "warning" : "neutral" },
          { label: "共享协同", value: `${bridgeCount} 个`, tone: bridgeCount > 0 ? "warning" : "neutral" },
          {
            label: "最近失败",
            value: recentFailure ? (recentFailure.name || recentFailure.feature || recentFailure.job_id || "-") : "无",
            tone: recentFailure ? "danger" : "success",
          },
        ],
      };
    });
    const homeOverview = computed(() => {
      if (deploymentRoleMode.value === "internal") {
        const runtime = internalRuntimeOverview.value || {};
        return {
          tone: runtime.tone || "neutral",
          statusText: runtime.statusText || "等待内网运行态",
          summaryText: runtime.summaryText || "内网端首页应优先关注浏览器池、共享文件和当前小时刷新。",
          nextActionText: runtime.errorText
            ? "先处理最近异常，再决定是否重新下载当前小时文件。"
            : "先看浏览器池和共享文件是否健康，再执行手动动作。",
          items: [
            { label: "共享文件", value: runtime.statusText || "-", tone: runtime.tone || "neutral" },
            { label: "浏览器池", value: runtime.poolStatusText || "-", tone: runtime.poolStatusText ? "info" : "neutral" },
            {
              label: "当前轮次",
              value: runtime.currentHourRefresh?.statusText || "-",
              tone: runtime.currentHourRefresh?.tone || "neutral",
            },
            { label: "当前任务", value: currentTaskOverview.value.statusText, tone: currentTaskOverview.value.tone },
          ],
          actions: [
            { id: "refresh_current_hour", label: currentHourRefreshButtonText.value, desc: "立即刷新当前小时四组共享文件" },
            { id: "refresh_manual_alarm", label: manualAlarmRefreshButtonText.value, desc: "单独拉取近 60 天告警 JSON" },
            { id: "open_config", label: "打开本地配置", desc: "检查共享目录、浏览器池和桥接参数" },
          ],
        };
      }
      const cache = sharedSourceCacheReadinessOverview.value || {};
      const review = handoverReviewOverview.value || {};
      const alarmUpload = externalAlarmUploadStatus.value || {};
      let tone = "success";
      let statusText = "可以继续外网主流程";
      let summaryText = "共享文件已就绪，当前可以直接进入自动流程、交接班或告警上传。";
      let nextActionText = "优先从“每日用电明细自动流程”开始；需要专项处理时再进入交接班日志或告警信息上传。";
      if (!cache.canProceedLatest) {
        tone = cache.tone || "warning";
        statusText = cache.statusText || "等待共享文件就绪";
        summaryText = cache.summaryText || "共享文件还没准备好，先不要急着做外网上传。";
        nextActionText = "先去状态总览确认缺哪一组文件、哪几个楼还在等待。";
      } else if (review.hasAnySession && !review.allConfirmed) {
        tone = review.tone || "warning";
        statusText = "当前批次还有待确认楼栋";
        summaryText = review.summaryText || "交接班批次还没完成确认。";
        nextActionText = "先处理交接班确认，再继续后续云表或派生上传动作。";
      } else if (alarmUpload.tone === "danger") {
        tone = "warning";
        statusText = "最近专项上传有异常";
        summaryText = alarmUpload.summaryText || "最近专项上传失败，但共享源文件仍保留。";
        nextActionText = "进入告警信息上传模块看任务摘要和运行日志，不要只盯卡片提示。";
      } else if (currentTaskOverview.value.tone === "info" || currentTaskOverview.value.tone === "warning") {
        tone = currentTaskOverview.value.tone;
        statusText = currentTaskOverview.value.statusText;
        summaryText = currentTaskOverview.value.summaryText;
        nextActionText = currentTaskOverview.value.nextActionText;
      }
      return {
        tone,
        statusText,
        summaryText,
        nextActionText,
        items: [
          { label: "共享文件", value: cache.statusText || "-", tone: cache.tone || "neutral" },
          { label: "交接班确认", value: review.summaryText || "当前无待确认批次", tone: review.tone || "neutral" },
          { label: "告警上传", value: alarmUpload.statusText || "-", tone: alarmUpload.tone || "neutral" },
          { label: "当前任务", value: currentTaskOverview.value.statusText, tone: currentTaskOverview.value.tone },
        ],
        actions: [
          { id: "open_auto_flow", label: "每日用电明细自动流程", desc: "从共享文件主链开始执行外网默认流程" },
          { id: "open_handover_log", label: "交接班处理", desc: "处理审核、回补和交接班后续上传" },
          { id: "open_alarm_upload", label: "告警上传", desc: "检查今天最新告警文件并执行 60 天上传" },
        ],
      };
    });
    const statusDiagnosisOverview = computed(() => {
      if (deploymentRoleMode.value === "internal") {
        const runtime = internalRuntimeOverview.value || {};
        const currentRefresh = runtime.currentHourRefresh || {};
        const failureText = String(currentRefresh.lastError || runtime.poolErrorText || runtime.errorText || "").trim();
        const currentProblemSlots = Array.isArray(runtime.slots)
          ? runtime.slots.filter((slot) =>
            Boolean(slot?.suspended) || ["failed"].includes(String(slot?.loginState || "").trim().toLowerCase()))
          : [];
        const currentProblemFamilies = Array.isArray(runtime.families)
          ? runtime.families.filter((family) => Boolean(family?.hasFailures) || Boolean(family?.hasBlocked))
          : [];
        const hasCurrentFailure = Boolean(
          (Array.isArray(currentRefresh.failedBuildings) && currentRefresh.failedBuildings.length)
          || (Array.isArray(currentRefresh.blockedBuildings) && currentRefresh.blockedBuildings.length)
          || currentProblemSlots.length
          || currentProblemFamilies.length,
        );
        let tone = runtime.tone || "neutral";
        let statusText = runtime.statusText || "等待内网运行态";
        let reasonText = runtime.summaryText || "当前没有足够的内网运行态摘要。";
        let actionText = "先确认浏览器池和共享目录是否正常，再决定是否触发下载。";
        if (hasCurrentFailure) {
          tone = "danger";
          statusText = "当前有需要人工处理的问题";
          if (currentRefresh.failedBuildings?.length || currentRefresh.blockedBuildings?.length) {
            reasonText = currentRefresh.summaryText || failureText || runtime.summaryText || "";
          } else if (currentProblemSlots.length) {
            const slot = currentProblemSlots[0] || {};
            reasonText = `${slot.building || "-"} ${slot.detailText || slot.stateText || "当前楼栋状态异常"}`;
          } else if (currentProblemFamilies.length) {
            const family = currentProblemFamilies[0] || {};
            reasonText = family.statusText || failureText || runtime.summaryText || "";
          } else {
            reasonText = failureText || runtime.summaryText || "";
          }
          actionText = "优先看失败楼栋或登录失败楼，再决定是否重新下载当前小时或手动拉取告警。";
        } else if (currentRefresh.tone === "warning" || currentRefresh.tone === "info") {
          tone = currentRefresh.tone || "warning";
          statusText = currentRefresh.statusText || "当前共享文件仍在推进";
          reasonText = currentRefresh.summaryText || runtime.summaryText || "";
          actionText = "先等待本轮执行结束；需要抢修时再用手动拉取。";
        }
        return {
          tone,
          statusText,
          reasonText,
          actionText,
          items: [
            { label: "共享文件", value: runtime.statusText || "-", tone: runtime.tone || "neutral" },
            { label: "浏览器池", value: runtime.poolStatusText || "-", tone: runtime.poolStatusText ? "info" : "neutral" },
            { label: "当前轮次", value: currentRefresh.statusText || "-", tone: currentRefresh.tone || "neutral" },
            { label: "当前任务", value: currentTaskOverview.value.statusText, tone: currentTaskOverview.value.tone },
          ],
          actions: [
            { id: "refresh_current_hour", label: currentHourRefreshButtonText.value },
            { id: "refresh_manual_alarm", label: manualAlarmRefreshButtonText.value },
            { id: "open_config", label: "打开本地配置" },
          ],
        };
      }
      const cache = sharedSourceCacheReadinessOverview.value || {};
      const review = handoverReviewOverview.value || {};
      const alarmUpload = externalAlarmUploadStatus.value || {};
      let tone = "success";
      let statusText = "外网链路可继续执行";
      let reasonText = "共享文件已就绪，没有发现需要先处理的阻塞。";
      let actionText = "优先使用自动流程；需要专项处理时再进入交接班或告警上传模块。";
      if (!cache.canProceedLatest) {
        tone = cache.tone || "warning";
        statusText = cache.statusText || "等待共享文件就绪";
        reasonText = cache.summaryText || "共享文件还不完整。";
        actionText = "先看最新共享文件就绪情况，确认缺失楼栋和等待原因。";
      } else if (review.hasAnySession && !review.allConfirmed) {
        tone = review.tone || "warning";
        statusText = "当前批次还有待确认楼栋";
        reasonText = review.summaryText || "交接班确认未结束。";
        actionText = "先完成交接班确认，再执行后续上传或派生动作。";
      } else if (alarmUpload.tone === "danger") {
        tone = "warning";
        statusText = "最近告警上传异常";
        reasonText = alarmUpload.summaryText || "最近一次告警上传失败。";
        actionText = "进入告警上传模块查看任务和运行日志，文件状态本身仍以 ready 为准。";
      }
      return {
        tone,
        statusText,
        reasonText,
        actionText,
        items: [
          { label: "共享文件", value: cache.statusText || "-", tone: cache.tone || "neutral" },
          { label: "交接班确认", value: review.summaryText || "当前无待确认批次", tone: review.tone || "neutral" },
          { label: "告警上传", value: alarmUpload.statusText || "-", tone: alarmUpload.tone || "neutral" },
          { label: "当前任务", value: currentTaskOverview.value.statusText, tone: currentTaskOverview.value.tone },
        ],
        actions: [
          { id: "open_auto_flow", label: "进入自动流程" },
          { id: "open_handover_log", label: "进入交接班" },
          { id: "open_alarm_upload", label: "进入告警上传" },
        ],
      };
    });
    const configGuidanceOverview = computed(() => {
      const role = configRoleMode.value || deploymentRoleMode.value;
      const sharedRoot = resolveSharedBridgeRoleRoot(config.value, role);
      const feishuAppId = String(config.value?.feishu?.app_id || "").trim();
      const feishuAppSecret = String(config.value?.feishu?.app_secret || "").trim();
      const handoverTemplatePath = String(config.value?.handover_log?.template?.source_path || "").trim();
      const handoverCloudRoot = String(config.value?.handover_log?.cloud_sheet_sync?.root_wiki_url || "").trim();
      const sections = [
        {
          id: "common_deployment",
          label: "角色与监听",
          ready: Boolean(role),
          value: role ? formatDeploymentRoleLabel(role) : "未选择",
          tone: role ? "success" : "warning",
          hint: role
            ? `当前配置角色：${formatDeploymentRoleLabel(role)}`
            : "需要先选择有效角色，否则无法确定本机监听模式。",
        },
        {
          id: "common_deployment",
          label: "共享目录",
          ready: Boolean(sharedRoot),
          value: sharedRoot || "未配置",
          tone: sharedRoot ? "success" : "warning",
          hint: sharedRoot
            ? "共享桥接、源文件和批准版本都会依赖该目录。"
            : "未配置共享目录时，内外网主链无法通过共享缓存协同。",
        },
        {
          id: "common_feishu_auth",
          label: "飞书鉴权",
          ready: Boolean(feishuAppId && feishuAppSecret),
          value: feishuAppId && feishuAppSecret ? "已配置" : "未配置",
          tone: feishuAppId && feishuAppSecret ? "success" : "warning",
          hint: feishuAppId && feishuAppSecret
            ? "飞书应用鉴权已具备。"
            : "缺少 app_id 或 app_secret 时，涉及多维表的模块无法稳定运行。",
        },
      ];
      if (role !== "internal") {
        sections.push(
          {
            id: "feature_handover",
            label: "交接班模板",
            ready: Boolean(handoverTemplatePath),
            value: handoverTemplatePath ? "已配置" : "未配置",
            tone: handoverTemplatePath ? "success" : "warning",
            hint: handoverTemplatePath || "交接班日志没有模板路径时无法生成文件。",
          },
          {
            id: "feature_handover",
            label: "交接班云表",
            ready: Boolean(handoverCloudRoot),
            value: handoverCloudRoot ? "已配置" : "未配置",
            tone: handoverCloudRoot ? "success" : "warning",
            hint: handoverCloudRoot || "未配置根 Wiki 地址时，交接班后续云表链路无法完整执行。",
          },
          {
            id: "feature_handover",
            label: "12项目标",
            ready: Boolean(dayMetricUploadTarget.value?.configured),
            value: dayMetricUploadTarget.value?.statusText || "未配置",
            tone: dayMetricUploadTarget.value?.configured ? "success" : "warning",
            hint: dayMetricUploadTarget.value?.hintText || "",
          },
          {
            id: "feature_alarm_export",
            label: "告警目标",
            ready: Boolean(alarmEventUploadTarget.value?.configured),
            value: alarmEventUploadTarget.value?.statusText || "未配置",
            tone: alarmEventUploadTarget.value?.configured ? "success" : "warning",
            hint: alarmEventUploadTarget.value?.hintText || "",
          },
        );
      }
      const readyCount = sections.filter((item) => item.ready).length;
      const missingLabels = sections.filter((item) => !item.ready).map((item) => item.label);
      const restartRequired = Boolean(
        configRoleMode.value && deploymentRoleMode.value && configRoleMode.value !== deploymentRoleMode.value,
      );
      let tone = "warning";
      let statusText = "仍有关键配置待补齐";
      let summaryText = `当前已完成 ${readyCount}/${sections.length} 项关键配置。`;
      if (readyCount === sections.length) {
        tone = "success";
        statusText = "关键配置已齐套";
        summaryText = "当前高频主链所需配置已经齐套，后续再按模块补高级参数即可。";
      } else if (readyCount === 0) {
        tone = "danger";
        statusText = "当前还没有完成关键配置";
        summaryText = "建议先从角色、共享目录和飞书鉴权开始，不要直接填全部细项。";
      } else if (missingLabels.length) {
        summaryText = `当前已完成 ${readyCount}/${sections.length} 项关键配置，仍缺：${missingLabels.join(" / ")}。`;
      }
      const quickTabs = [
        { id: "common_deployment", label: "角色与共享目录" },
        ...(showFeishuAuthConfigTab.value ? [{ id: "common_feishu_auth", label: "飞书鉴权" }] : []),
        ...(showFeatureHandoverConfigTab.value ? [{ id: "feature_handover", label: "交接班" }] : []),
        ...(showFeatureAlarmExportConfigTab.value ? [{ id: "feature_alarm_export", label: "告警上传" }] : []),
      ];
      return {
        tone,
        statusText,
        summaryText,
        restartImpactText: restartRequired
          ? `当前配置角色与正在运行角色不同，保存后会自动重启并切换到${formatDeploymentRoleLabel(configRoleMode.value)}。`
          : "大多数配置保存后可直接生效；只有角色监听模式变化时才需要自动重启。",
        sections,
        quickTabs,
      };
    });
    const appShellTitle = computed(() => {
      if (deploymentRoleMode.value === "internal") return "内网端本地管理页";
      if (deploymentRoleMode.value === "external") return "外网业务控制台";
      return "全景月报平台";
    });
    const statusNavLabel = computed(() => (deploymentRoleMode.value === "internal" ? "内网下载中心" : "状态总览"));
    const dashboardNavLabel = computed(() => (deploymentRoleMode.value === "internal" ? "运行日志" : "业务控制台"));
    const configNavLabel = computed(() => (deploymentRoleMode.value === "internal" ? "本地配置" : "配置中心"));
    const configShellTitle = computed(() => (
      deploymentRoleMode.value === "internal"
        ? "本地管理配置"
        : "配置中心（公共 + 功能分组）"
    ));
    const configShellDescription = computed(() => {
      if (deploymentRoleMode.value === "internal") {
        return "这里只保留内网端下载中心需要的部署、桥接、共享文件和更新镜像相关配置。";
      }
      if (deploymentRoleMode.value === "external") {
        return "左侧切换配置分组，右侧仅显示当前分组内容；外网端不展示内网下载细节配置。";
      }
      return "请选择内网端或外网端后进入对应页面。";
    });
    const configReturnButtonText = computed(() => (
      deploymentRoleMode.value === "internal" ? "返回内网状态页" : "返回业务控制台"
    ));
    const statusHeroTitle = computed(() => {
      if (deploymentRoleMode.value === "internal") return "共享桥接、下载页池与镜像更新";
      if (deploymentRoleMode.value === "external") return "外网业务状态、共享任务与更新发布";
      return "运行状态";
    });
    const statusHeroDescription = computed(() => {
      if (deploymentRoleMode.value === "internal") {
        return "这一页只显示内网端本地管理能力：共享桥接、5个常驻下载页签、更新镜像和诊断日志。";
      }
      if (deploymentRoleMode.value === "external") {
        return "这一页负责查看外网业务运行状态，并保留共享任务、审核与后续上传入口。";
      }
      return "这一页负责查看当前运行状态，并保留交接班批次级确认与云表重试入口。";
    });
    const bridgeExecutionHint = computed(() => {
      if (deploymentRoleMode.value === "internal") {
        return "当前为内网端，请在外网端发起；内网端只负责共享桥接前置阶段。";
      }
      return "当前为外网端，默认优先读取共享文件；缺失时再等待内网端补采。";
    });
    const externalExecutionHint = computed(() => {
      if (deploymentRoleMode.value === "internal") {
        return "当前为内网端，本模块不在本机执行上传。";
      }
      return "当前为外网端，按当前网络直接执行。";
    });
    const resumeExecutionHint = computed(() => {
      if (deploymentRoleMode.value === "internal") {
        return "当前为内网端，断点续传请在外网端执行。";
      }
      return "外网端会从共享文件继续上传，不重新触发共享文件准备。";
    });
    const startupRoleCurrentMode = computed(() => effectiveRoleMode.value);
    const startupRoleCurrentToken = computed(() => String(health.startup_time || "").trim());
    const startupRoleCurrentNodeId = computed(() => String(health.deployment?.node_id || "").trim());
    const startupRoleCurrentLabel = computed(() => formatDeploymentRoleLabel(startupRoleCurrentMode.value));
    const startupRoleSelectedLabel = computed(() => formatDeploymentRoleLabel(startupRoleSelectorSelection.value));
    const startupRoleNodeIdDisplayText = computed(() =>
      buildRoleNodeIdPreview(
        startupRoleCurrentNodeId.value,
        startupRoleCurrentMode.value,
        startupRoleSelectorSelection.value,
      ),
    );
    const startupRoleNodeIdDisplayHint = computed(() =>
      startupRoleNodeIdDisplayText.value === "切换后自动生成并长期固定"
        ? "当前角色变更后会按本机自动生成并长期固定。"
        : normalizeDeploymentRoleMode(startupRoleSelectorSelection.value) === startupRoleCurrentMode.value
          ? "当前生效节点 ID"
          : "按当前机器推导出的目标角色节点 ID"
    );
    const startupRoleRequiresBridgeConfig = computed(() =>
      ["internal", "external"].includes(normalizeDeploymentRoleMode(startupRoleSelectorSelection.value)),
    );
    const startupRoleBridgeValidationMessage = computed(() =>
      validateStartupBridgeDraft(startupRoleSelectorSelection.value, startupRoleBridgeDraft.value),
    );
    const startupRoleCurrentHasBridgeConfig = computed(() =>
      Boolean(resolveSharedBridgeRoleRoot(config.value || {}, startupRoleSelectorSelection.value)),
    );
    const startupRoleBridgeNoticeText = computed(() => {
      if (!startupRoleRequiresBridgeConfig.value) return "";
      if (startupRoleBridgeValidationMessage.value) {
        return startupRoleBridgeValidationMessage.value;
      }
      if (startupRoleCurrentHasBridgeConfig.value) {
        return "已检测到现有共享桥接配置，请确认后继续。";
      }
      if (String(startupRoleBridgeDraft.value?.root_dir || "").trim()) {
        return "共享目录已填写，确认后将自动启用共享桥接并加载对应角色页面。";
      }
      return "请先填写共享目录。节点名称会自动使用角色中文名，节点 ID 也会自动生成并长期固定。";
    });
    const startupRoleHasDraftChanges = computed(() =>
      isStartupBridgeDraftChanged(config.value || {}, startupRoleBridgeDraft.value, startupRoleSelectorSelection.value),
    );
    const startupRoleHasRelevantDraftChanges = computed(() =>
      startupRoleRequiresBridgeConfig.value && startupRoleHasDraftChanges.value,
    );
    const startupRoleWillSaveChanges = computed(() => {
      const targetRole = normalizeDeploymentRoleMode(startupRoleSelectorSelection.value);
      return targetRole !== startupRoleCurrentMode.value || startupRoleHasRelevantDraftChanges.value;
    });
    const startupRoleActionButtonText = computed(() =>
      startupRoleSelectorBusy.value
        ? "处理中..."
        : startupRoleWillSaveChanges.value
          ? "保存并加载"
          : "按此角色进入",
    );
    const startupRoleConfirmDisabled = computed(() =>
      Boolean(startupRoleSelectorBusy.value || startupRoleBridgeValidationMessage.value),
    );
    const startupRoleGateReady = computed(() =>
      Boolean(bootstrapReady.value && configLoaded.value),
    );
    const startupRoleGateVisible = computed(() =>
      Boolean(startupRoleDecisionReady.value && startupRoleSelectorVisible.value),
    );
    const shouldRenderAppShell = computed(() =>
      Boolean(config.value) && startupRoleDecisionReady.value && !startupRoleSelectorVisible.value,
    );
    const deploymentNodeIdDisplayText = computed(() =>
      buildRoleNodeIdPreview(
        String(health.deployment?.node_id || "").trim(),
        deploymentRoleMode.value,
        configRoleMode.value,
      ),
    );
    const deploymentNodeIdDisplayHint = computed(() =>
      deploymentNodeIdDisplayText.value === "切换后自动生成并长期固定"
        ? "保存后会按当前机器自动生成并长期固定。"
        : configRoleMode.value === deploymentRoleMode.value
          ? "当前生效节点 ID"
          : "保存后将使用该节点 ID"
    );
    const isHandoverConfirmAllLocked = computed(() => isActionLocked(actionKeyHandoverConfirmAll));
    const isHandoverCloudRetryAllLocked = computed(() => isActionLocked(actionKeyHandoverCloudRetryAll));
    const handoverConfirmAllButtonText = computed(() => {
      if (isHandoverConfirmAllLocked.value) return "确认并上传中...";
      if (!health.handover?.review_status?.has_any_session) return "当前批次未生成";
      if (health.handover?.review_status?.all_confirmed) return "已全部确认";
      return "一键全确认";
    });
    const handoverCloudRetryFailureCount = computed(() => {
      const rows = Array.isArray(health.handover?.review_status?.buildings)
        ? health.handover.review_status.buildings
        : [];
      return rows.filter((row) => {
        const status = String(row?.cloud_sheet_sync?.status || "").trim().toLowerCase();
        return status === "failed" || status === "prepare_failed";
      }).length;
    });
    const canShowHandoverCloudRetryAll = computed(() =>
      Boolean(String(health.handover?.review_status?.batch_key || "").trim()) &&
      Boolean(health.handover?.review_status?.has_any_session),
    );
    const handoverCloudRetryAllButtonText = computed(() => {
      if (isHandoverCloudRetryAllLocked.value) return "重试中...";
      if (!health.handover?.review_status?.all_confirmed) return "待全部确认后可重试";
      if (handoverCloudRetryFailureCount.value <= 0) return "云表已全部同步";
      return "一键全部重试云表上传";
    });
    const isHandoverCloudRetryAllDisabled = computed(() => {
      if (isHandoverCloudRetryAllLocked.value) return true;
      if (!String(health.handover?.review_status?.batch_key || "").trim()) return true;
      if (!health.handover?.review_status?.all_confirmed) return true;
      return handoverCloudRetryFailureCount.value <= 0;
    });
    const isHandoverFollowupContinueLocked = computed(() => isActionLocked(actionKeyHandoverFollowupContinue));
    const canShowHandoverFollowupContinue = computed(
      () =>
        Boolean(health.handover?.review_status?.all_confirmed) &&
        Boolean(handoverFollowupProgress.value?.canResumeFollowup),
    );
    const handoverFollowupContinueButtonText = computed(() => {
      if (isHandoverFollowupContinueLocked.value) return "继续上传中...";
      const failedCount = Number(handoverFollowupProgress.value?.failedCount || 0);
      const pendingCount = Number(handoverFollowupProgress.value?.pendingCount || 0);
      if (failedCount > 0) return `继续后续上传（失败 ${failedCount}）`;
      if (pendingCount > 0) return `继续后续上传（待处理 ${pendingCount}）`;
      return "继续后续上传";
    });
    const updaterMainButtonText = computed(() => {
      if (isActionLocked(actionKeyUpdaterRestart)) return "重启中...";
      if (isActionLocked(actionKeyUpdaterApply)) return "更新中...";
      if (isActionLocked(actionKeyUpdaterCheck)) return "检查中...";
      if (health.updater.restart_required) return "立即重启生效";
      if (health.updater.queued_apply?.queued) return "任务结束后自动更新";
      if (health.updater.update_available || health.updater.force_apply_available) return "开始更新";
      return "检查并更新";
    });

    async function runUpdaterMainAction() {
      if (isUpdaterActionLocked.value) return;
      if (health.updater.restart_required) {
        await restartUpdaterApp();
        return;
      }
      if (health.updater.queued_apply?.queued) {
        await applyUpdaterPatch();
        return;
      }
      if (health.updater.update_available) {
        await applyUpdaterPatch();
        return;
      }
      if (health.updater.force_apply_available) {
        await applyUpdaterPatch();
        return;
      }
      await checkUpdaterNow({ autoApplyIfAvailable: true });
    }

    const uiLocalActions = createUiLocalActions({
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
    });

    const {
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
    } = uiLocalActions;

    const dateHandoverActions = createDateHandoverActions({
      config,
      message,
      selectedDate,
      rangeStartDate,
      rangeEndDate,
      selectedDates,
      handoverDutyDate,
      handoverDutyShift,
      handoverDownloadScope,
      handoverDutyAutoFollow,
      handoverDutyLastAutoAt,
    });

    const {
      syncHandoverDutyFromNow,
      onHandoverDutyDateManualChange,
      onHandoverDutyShiftManualChange,
      restoreAutoHandoverDuty,
      addDate,
      addDateRange,
      quickRangeToday,
      removeDate,
      clearDates,
    } = dateHandoverActions;

    function appendDayMetricDate(dateText) {
      const text = String(dateText || "").trim();
      if (!text) return false;
      if (dayMetricSelectedDates.value.includes(text)) return false;
      dayMetricSelectedDates.value = [...dayMetricSelectedDates.value, text].sort();
      return true;
    }

    function addDayMetricDate() {
      appendDayMetricDate(dayMetricSelectedDate.value);
    }

    function addDayMetricDateRange() {
      const startText = String(dayMetricRangeStartDate.value || "").trim();
      const endText = String(dayMetricRangeEndDate.value || "").trim();
      if (!startText || !endText) {
        message.value = "请选择有效的起止日期";
        return;
      }
      if (startText > endText) {
        message.value = "开始日期不能晚于结束日期";
        return;
      }
      const today = todayText();
      if (endText > today) {
        message.value = "结束日期不能超过今天";
        return;
      }
      const rangeDates = expandDateRange(startText, endText);
      if (!rangeDates.length) {
        message.value = "日期区间无效";
        return;
      }
      const next = new Set(dayMetricSelectedDates.value);
      rangeDates.forEach((item) => next.add(item));
      dayMetricSelectedDates.value = Array.from(next).sort();
    }

    function removeDayMetricDate(dateText) {
      const text = String(dateText || "").trim();
      dayMetricSelectedDates.value = dayMetricSelectedDates.value.filter((item) => item !== text);
    }

    function clearDayMetricDates() {
      dayMetricSelectedDates.value = [];
    }

    const runtimeActions = createRuntimeActions({
      health,
      config,
      logs,
      message,
      busy,
      currentJob,
      jobsList,
      selectedJobId,
      bridgeTasks,
      selectedBridgeTaskId,
      bridgeTaskDetail,
      resourceSnapshot,
      pendingResumeRuns,
      autoResumeState,
      buildingsText,
      sheetRuleRows,
      manualBuilding,
      dayMetricUploadBuilding,
      dayMetricLocalBuilding,
      customAbsoluteStartLocal,
      customAbsoluteEndLocal,
      syncCustomWindowLocalInputs,
      systemLogOffset,
      handoverEngineerDirectory,
      handoverEngineerLoading,
      handoverDailyReportContext,
      handoverDailyReportCaptureAssets,
      handoverDailyReportLastScreenshotTest,
      handoverDailyReportPreviewModal,
      handoverDailyReportUploadModal,
      handoverDutyDate,
      handoverDutyShift,
      canRun,
      streamController,
      runSingleFlight,
      bootstrapReady,
      fullHealthLoaded,
      configLoaded,
      healthLoadError,
      configLoadError,
      engineerDirectoryLoaded,
      updaterUiOverlayVisible,
      updaterUiOverlayTitle,
      updaterUiOverlaySubtitle,
      updaterUiOverlayStage,
      updaterUiOverlayKicker,
      updaterAwaitingRestartRecovery,
      shouldIncludeHandoverHealthContext: () => shouldIncludeHandoverHealthContext.value,
      shouldLoadEngineerDirectory: () => shouldLoadEngineerDirectory.value,
    });

    const {
      appendLog,
      fetchBootstrapHealth,
      fetchHealth,
      fetchJobs,
      fetchBridgeTasks,
      fetchBridgeTaskDetail,
      cancelBridgeTask,
      retryBridgeTask,
      refreshCurrentHourSourceCache,
      refreshBuildingLatestSourceCache,
      refreshManualAlarmSourceCache,
      deleteManualAlarmSourceCacheFiles,
      uploadAlarmSourceCacheFull,
      uploadAlarmSourceCacheBuilding,
      openAlarmEventUploadTarget,
      fetchRuntimeResources,
      fetchConfig,
      fetchHandoverEngineerDirectory,
      ensureHandoverEngineerDirectoryLoaded,
      scheduleEngineerDirectoryPrefetch,
      saveConfig,
      autoSaveConfig,
      activateStartupRuntime,
      restartApplication,
      checkUpdaterNow,
      applyUpdaterPatch,
      restartUpdaterApp,
      confirmAllHandoverReview,
      retryAllFailedHandoverCloudSync,
      fetchHandoverDailyReportContext,
      openHandoverDailyReportScreenshotAuth,
      runHandoverDailyReportScreenshotTest,
      openHandoverDailyReportPreview,
      closeHandoverDailyReportPreview,
      openHandoverDailyReportUploadDialog,
      closeHandoverDailyReportUploadDialog,
      uploadHandoverDailyReportAsset,
      recaptureHandoverDailyReportAsset,
      restoreHandoverDailyReportAutoAsset,
      rewriteHandoverDailyReportRecord,
      reprobeHandoverReviewAccess,
      getBridgeTaskCancelActionKey,
      getBridgeTaskRetryActionKey,
      getSourceCacheRefreshBuildingActionKey,
      getHandoverDailyReportRecaptureActionKey,
      getHandoverDailyReportUploadActionKey,
      getHandoverDailyReportRestoreActionKey,
      fetchPendingResumeRuns,
      runResumeUpload,
      deleteResumeRun,
      getResumeRunId,
      getResumeRunActionKey,
      getResumeDeleteActionKey,
      formatResumeDateSummary,
      formatResumeDateFull,
      tryAutoResume,
      ACTION_KEY_HANDOVER_DAILY_REPORT_RECORD_REWRITE,
      ACTION_KEY_HANDOVER_REVIEW_ACCESS_REPROBE,
    } = runtimeActions;

    async function runHomeQuickAction(actionId) {
      const action = String(actionId || "").trim().toLowerCase();
      if (!action) return;
      if (action === "open_auto_flow") {
        openDashboardPage();
        setDashboardActiveModule("auto_flow");
        return;
      }
      if (action === "open_handover_log") {
        openDashboardPage();
        setDashboardActiveModule("handover_log");
        return;
      }
      if (action === "open_alarm_upload") {
        openDashboardPage();
        setDashboardActiveModule("alarm_event_upload");
        return;
      }
      if (action === "open_runtime_logs") {
        openDashboardPage();
        setDashboardActiveModule("runtime_logs");
        return;
      }
      if (action === "refresh_current_hour") {
        await refreshCurrentHourSourceCache();
        return;
      }
      if (action === "refresh_manual_alarm") {
        await refreshManualAlarmSourceCache();
        return;
      }
      if (action === "open_config") {
        openConfigPage();
      }
    }

    function closeStartupRoleSelector({ handled = false } = {}) {
      startupRoleSelectorVisible.value = false;
      startupRoleSelectorBusy.value = false;
      startupRoleSelectorMessage.value = "";
      startupRoleDecisionReady.value = true;
      if (handled) {
        startupRoleSelectorHandled.value = true;
        startupRoleFlowState.value = "activated";
      }
    }

    function showStartupRoleSelector(messageText = "") {
      startupRoleLoadingVisible.value = false;
      startupRoleLoadingTitle.value = "";
      startupRoleLoadingSubtitle.value = "";
      startupRoleLoadingStage.value = "";
      startupRoleSelectorMessage.value = String(messageText || "").trim();
      startupRoleDecisionReady.value = true;
      startupRoleSelectorVisible.value = true;
      startupRoleSelectorHandled.value = false;
      startupRoleSelectorBusy.value = false;
      startupRoleFlowState.value = "selecting";
    }

    function showStartupRoleLoading({ title = "", subtitle = "", stage = "" } = {}) {
      startupRoleSelectorVisible.value = false;
      startupRoleLoadingVisible.value = true;
      startupRoleLoadingTitle.value = String(title || "").trim();
      startupRoleLoadingSubtitle.value = String(subtitle || "").trim();
      startupRoleLoadingStage.value = String(stage || "").trim();
      const normalizedStage = String(stage || "").trim().toLowerCase();
      if (normalizedStage === "restarting") {
        startupRoleFlowState.value = "restarting";
      } else if (normalizedStage === "reloading" || normalizedStage === "recovering") {
        startupRoleFlowState.value = "recovering";
      } else {
        startupRoleFlowState.value = "activating";
      }
    }

    function hideStartupRoleLoading() {
      startupRoleLoadingVisible.value = false;
      startupRoleLoadingTitle.value = "";
      startupRoleLoadingSubtitle.value = "";
      startupRoleLoadingStage.value = "";
    }

    function clearStartupRoleRestartPendingState() {
      clearStartupRoleRestartPending();
    }

    function clearStartupRoleRestartResumeState() {
      clearStartupRoleRestartResume();
    }

    function clearLegacyStartupRoleRestartState() {
      clearStartupRoleRestartPendingState();
      clearStartupRoleRestartResumeState();
    }

    function currentStartupHandoff() {
      const raw = health.startup_handoff;
      if (!raw || typeof raw !== "object") {
        return {
          active: false,
          mode: "",
          target_role_mode: "",
          requested_at: "",
          reason: "",
          nonce: "",
        };
      }
      return {
        active: Boolean(raw.active),
        mode: String(raw.mode || "").trim(),
        target_role_mode: normalizeDeploymentRoleMode(raw.target_role_mode),
        requested_at: String(raw.requested_at || "").trim(),
        reason: String(raw.reason || "").trim(),
        nonce: String(raw.nonce || "").trim(),
      };
    }

    function suppressCurrentStartupHandoff() {
      const nonce = String(health.startup_handoff?.nonce || "").trim();
      if (nonce) {
        startupRoleSuppressedHandoffNonce.value = nonce;
      }
      if (health.startup_handoff && typeof health.startup_handoff === "object") {
        Object.assign(health.startup_handoff, {
          active: false,
          mode: "",
          target_role_mode: "",
          requested_at: "",
          reason: "",
          nonce: "",
        });
      }
    }

    function syncStartupRoleBridgeDraft() {
      const role = normalizeDeploymentRoleMode(startupRoleSelectorSelection.value || startupRoleCurrentMode.value || "internal") || "internal";
      startupRoleBridgeDraft.value = buildStartupBridgeDraft(config.value || {}, role);
      startupRoleAdvancedVisible.value = false;
    }

    function selectStartupRole(value) {
      const normalized = normalizeDeploymentRoleMode(value);
      startupRoleSelectorSelection.value = normalized || "internal";
      startupRoleSelectorMessage.value = "";
      syncStartupRoleBridgeDraft();
    }

    async function activateStartupRuntimeAfterSelection(source, options = {}) {
      const targetRole = normalizeDeploymentRoleMode(
        options?.targetRoleMode || startupRoleSelectorSelection.value || config.value?.deployment?.role_mode || startupRoleCurrentMode.value,
      );
      showStartupRoleLoading({
        title: `正在加载${formatDeploymentRoleLabel(targetRole || "internal")}`,
        subtitle: "正在连接后台运行时，请稍候。",
        stage: "activating",
      });
      const activationResult = await activateStartupRuntime({
        source,
        startupHandoffNonce: String(options?.startupHandoffNonce || "").trim(),
      });
      if (activationResult?.ok === false) {
        hideStartupRoleLoading();
        startupRoleFlowState.value = "selecting";
        message.value = String(activationResult?.error || "").trim() || "后台运行时激活失败。";
        return false;
      }
      // 先把门控字段本地置位，避免 watch 在远端快照刷新前回弹角色选择页。
      health.runtime_activated = true;
      health.startup_role_confirmed = true;
      health.role_selection_required = false;
      if (health.startup_handoff && typeof health.startup_handoff === "object") {
        Object.assign(health.startup_handoff, {
          active: false,
          mode: "",
          target_role_mode: "",
          requested_at: "",
          reason: "",
          nonce: "",
        });
      }
      startupRoleSuppressedHandoffNonce.value = "";
      await fetchBootstrapHealth({ silentMessage: true });
      await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
      hideStartupRoleLoading();
      startupRoleFlowState.value = "activated";
      return true;
    }

    async function confirmStartupRoleSelection() {
      if (startupRoleSelectorBusy.value) return;
      const targetRole = normalizeDeploymentRoleMode(startupRoleSelectorSelection.value);
      const currentRole = startupRoleCurrentMode.value;
      startupRoleSelectorBusy.value = true;
      startupRoleSelectorMessage.value = "";
      showStartupRoleLoading({
        title: `正在准备${formatDeploymentRoleLabel(targetRole || "internal")}`,
        subtitle: "正在校验启动参数，请稍候。",
        stage: "validating",
      });

      const draftValidationMessage = validateStartupBridgeDraft(targetRole, startupRoleBridgeDraft.value);
      if (draftValidationMessage) {
        hideStartupRoleLoading();
        startupRoleSelectorVisible.value = true;
        startupRoleSelectorBusy.value = false;
        startupRoleSelectorMessage.value = draftValidationMessage;
        return;
      }

      if (targetRole === currentRole && !startupRoleHasRelevantDraftChanges.value) {
        showStartupRoleLoading({
          title: `正在启动${formatDeploymentRoleLabel(targetRole)}`,
          subtitle: "角色配置无需变更，正在进入对应页面。",
          stage: "activating",
        });
        const activated = await activateStartupRuntimeAfterSelection("startup_role_confirm", {
          targetRoleMode: targetRole,
        });
        if (!activated) {
          startupRoleSelectorBusy.value = false;
          showStartupRoleSelector("后台运行时激活失败。");
          return;
        }
        closeStartupRoleSelector({ handled: true });
        message.value = `本次启动已按${formatDeploymentRoleLabel(targetRole)}运行。`;
        return;
      }

      const previousConfig = clone(config.value || {});
      showStartupRoleLoading({
        title: "正在保存角色配置",
        subtitle: `正在应用${formatDeploymentRoleLabel(targetRole)}配置，请稍候。`,
        stage: "saving",
      });
      try {
        if (!config.value?.deployment || typeof config.value.deployment !== "object") {
          config.value.deployment = {};
        }
        if (!config.value?.shared_bridge || typeof config.value.shared_bridge !== "object") {
          config.value.shared_bridge = {};
        }
        config.value.deployment.role_mode = targetRole;
        if (targetRole === "internal" || targetRole === "external") {
          config.value.deployment.node_label = formatDeploymentRoleLabel(targetRole);
          const nextRootDir = String(startupRoleBridgeDraft.value.root_dir || "").trim();
          const roleRootKey = targetRole === "internal" ? "internal_root_dir" : "external_root_dir";
          Object.assign(config.value.shared_bridge, {
            enabled: true,
            [roleRootKey]: nextRootDir,
            root_dir: nextRootDir,
            poll_interval_sec: normalizePositiveInteger(
              startupRoleBridgeDraft.value.poll_interval_sec,
              STARTUP_BRIDGE_DEFAULTS.poll_interval_sec,
            ),
            heartbeat_interval_sec: normalizePositiveInteger(
              startupRoleBridgeDraft.value.heartbeat_interval_sec,
              STARTUP_BRIDGE_DEFAULTS.heartbeat_interval_sec,
            ),
            claim_lease_sec: normalizePositiveInteger(
              startupRoleBridgeDraft.value.claim_lease_sec,
              STARTUP_BRIDGE_DEFAULTS.claim_lease_sec,
            ),
            stale_task_timeout_sec: normalizePositiveInteger(
              startupRoleBridgeDraft.value.stale_task_timeout_sec,
              STARTUP_BRIDGE_DEFAULTS.stale_task_timeout_sec,
            ),
            artifact_retention_days: normalizePositiveInteger(
              startupRoleBridgeDraft.value.artifact_retention_days,
              STARTUP_BRIDGE_DEFAULTS.artifact_retention_days,
            ),
            sqlite_busy_timeout_ms: normalizePositiveInteger(
              startupRoleBridgeDraft.value.sqlite_busy_timeout_ms,
              STARTUP_BRIDGE_DEFAULTS.sqlite_busy_timeout_ms,
            ),
          });
        }
        const isRoleSwitch = targetRole !== currentRole;
        const saveResult = await saveConfig({
          skipPostSaveHealthRefresh: isRoleSwitch || Boolean(health.runtime_activated),
        });
        if (!saveResult?.saved) {
          config.value = previousConfig;
          showStartupRoleSelector(
            saveResult?.reason === "invalid"
              ? String(saveResult?.error || "").trim() || "当前配置校验失败，请检查启动角色参数或其余配置。"
              : String(saveResult?.error || "").trim() || "保存角色配置失败。",
          );
          return;
        }
        const shouldRestartForStartupConfirm =
          Boolean(saveResult?.restartRequired) && (isRoleSwitch || Boolean(health.runtime_activated));
        if (shouldRestartForStartupConfirm) {
          hideStartupRoleLoading();
          const restartResult = await restartApplication({
            source: "startup_role_picker",
            targetRoleMode: targetRole,
            reason: isRoleSwitch ? "role_mode_switch" : "startup_bridge_config_confirm",
            kicker: isRoleSwitch ? "角色切换中" : "桥接配置生效中",
            title: isRoleSwitch
              ? `正在切换到${formatDeploymentRoleLabel(targetRole)}`
              : `正在应用${formatDeploymentRoleLabel(targetRole)}桥接配置`,
            subtitle: isRoleSwitch
              ? "角色配置已保存，程序正在当前窗口内重启并切换监听地址。"
              : "桥接配置已保存，程序正在当前窗口内重启并应用新的运行参数。",
            reloadSubtitle: isRoleSwitch
              ? "服务已恢复，正在刷新当前页面并继续启动新的运行角色。"
              : "服务已恢复，正在刷新当前页面并接入新的桥接配置。",
            message: isRoleSwitch
              ? `已提交切换到${formatDeploymentRoleLabel(targetRole)}，正在当前窗口内重启并等待服务恢复。`
              : `已提交${formatDeploymentRoleLabel(targetRole)}桥接配置更新，正在等待服务恢复。`,
          });
          if (restartResult?.ok === false) {
            clearLegacyStartupRoleRestartState();
            showStartupRoleSelector(
              String(restartResult?.error || "").trim() || "角色配置已保存，但触发程序重启失败。",
            );
          }
          return;
        }
        showStartupRoleLoading({
          title: `正在加载${formatDeploymentRoleLabel(targetRole)}`,
          subtitle: "配置已保存，正在连接后台运行时。",
          stage: "activating",
        });
        const activated = await activateStartupRuntimeAfterSelection("startup_role_confirm_after_save", {
          targetRoleMode: targetRole,
        });
        if (!activated) {
          showStartupRoleSelector("后台运行时激活失败。");
          return;
        }
        closeStartupRoleSelector({ handled: true });
        clearLegacyStartupRoleRestartState();
        syncStartupRoleBridgeDraft();
        message.value =
          targetRole === currentRole
            ? `已确认${formatDeploymentRoleLabel(targetRole)}启动配置。`
            : `已切换到${formatDeploymentRoleLabel(targetRole)}。`;
      } catch (err) {
        config.value = previousConfig;
        clearLegacyStartupRoleRestartState();
        showStartupRoleSelector(`角色切换失败: ${err}`);
      }
    }

    function formatJobWaitReason(job) {
      const raw = String(job?.wait_reason || "").trim();
      if (!raw) return String(job?.status || "waiting_resource").trim() || "waiting_resource";
      const parts = raw.split(",").map((item) => String(item || "").trim()).filter(Boolean);
      const mapped = parts.map((item) => {
        if (item === "waiting:browser_controlled") return "等待受控浏览器";
        if (item === "waiting:handover_batch") return "等待交接班批次锁";
        if (item === "waiting:network_pipeline") return "等待网络流水线";
        if (item === "waiting:network_internal") return "等待内网窗口";
        if (item === "waiting:network_external") return "等待外网窗口";
        if (item === "waiting:network_internal_unreachable") return "等待内网可达";
        if (item === "waiting:network_external_unreachable") return "等待外网可达";
        if (item === "waiting:output_path") return "等待输出文件锁";
        if (item === "waiting:source_identity") return "等待共享源文件";
        if (item === "waiting:app_update") return "等待更新独占";
        return item;
      });
      return mapped.join(" / ");
    }

    function normalizeLegacyNetworkSide(side) {
      const normalized = String(side || "").trim().toLowerCase();
      return normalized;
    }

    function normalizeLegacyNetworkMode(mode) {
      const normalized = String(mode || "").trim().toLowerCase();
      return normalized;
    }

    function formatNetworkWindowSide(side) {
      const normalized = normalizeLegacyNetworkSide(side);
      if (normalized === "internal") return "内网";
      if (normalized === "external") return "外网";
      if (normalized === "pipeline") return "流水线";
      return "空闲";
    }

    function formatDetectedNetworkSide(side) {
      const normalized = normalizeLegacyNetworkSide(side);
      if (normalized === "internal") return "当前在内网";
      if (normalized === "external") return "当前在外网";
      if (normalized === "other") return "当前不在目标网络";
      if (normalized === "none") return "当前未连接 WiFi";
      return "当前网络未知";
    }

    function formatSsidSide(side) {
      const normalized = String(side || "").trim().toLowerCase();
      if (normalized === "internal") return "内网";
      if (normalized === "external") return "外网";
      if (normalized === "other") return "其他";
      if (normalized === "none") return "未连接";
      return "-";
    }

    function formatNetworkMode(mode) {
      const normalized = normalizeLegacyNetworkMode(mode);
      if (normalized === "internal_only") return "仅内网可达";
      if (normalized === "external_only") return "仅外网可达";
      if (normalized === "none_reachable") return "当前均不可达";
      return String(mode || "").trim() || "-";
    }

    function formatBooleanReachability(value) {
      return value ? "是" : "否";
    }

    function formatWetBulbTargetKind(kind) {
      const normalized = String(kind || "").trim().toLowerCase();
      if (normalized === "base_token_pair") return "Base";
      if (normalized === "wiki_token_pair") return "Wiki";
      if (normalized === "probe_error") return "探测失败";
      if (normalized === "invalid") return "配置无效";
      return "-";
    }

    const wetBulbConfiguredTarget = computed(() => {
      const preview = health.wet_bulb_collection?.target_preview || {};
      const configTarget = config.value?.wet_bulb_collection?.target || {};
      return {
        configuredAppToken:
          String(preview?.configured_app_token || "").trim() || String(configTarget?.app_token || "").trim(),
        operationAppToken: String(preview?.operation_app_token || "").trim(),
        tableId: String(preview?.table_id || "").trim() || String(configTarget?.table_id || "").trim(),
        targetKind: String(preview?.target_kind || "").trim(),
        url: String(preview?.display_url || preview?.bitable_url || "").trim(),
        message: String(preview?.message || "").trim(),
        resolvedAt: String(preview?.resolved_at || "").trim(),
      };
    });

    const wetBulbLatestRunTarget = computed(() => {
      const result = currentJob.value?.result;
      const target = result?.target;
      return {
        configuredAppToken: String(target?.configured_app_token || "").trim(),
        operationAppToken: String(target?.operation_app_token || target?.app_token || "").trim(),
        tableId: String(target?.table_id || "").trim(),
        targetKind: String(target?.target_kind || "").trim(),
        url: String(target?.display_url || target?.bitable_url || "").trim(),
        message: String(target?.message || "").trim(),
        resolvedAt: String(target?.resolved_at || "").trim(),
      };
    });

    function formatJobStageStatus(stage) {
      const status = String(stage?.status || "").trim().toLowerCase();
      if (status === "pending") return "待执行";
      if (status === "waiting_resource") return "等待资源";
      if (status === "ready") return "可执行";
      if (status === "running") return "执行中";
      if (status === "cancelling") return "取消中";
      if (status === "success") return "成功";
      if (status === "failed") return "失败";
      if (status === "partial_failed") return "部分失败";
      if (status === "skipped") return "已跳过";
      if (status === "blocked") return "已阻塞";
      if (status === "cancelled") return "已取消";
      if (status === "interrupted") return "已中断";
      return String(stage?.status || "").trim() || "-";
    }

    function formatJobStageTone(stage) {
      const status = String(stage?.status || "").trim().toLowerCase();
      if (status === "success") return "success";
      if (status === "failed") return "danger";
      if (status === "cancelled") return "neutral";
      if (status === "interrupted" || status === "partial_failed") return "danger";
      if (status === "running") return "info";
      if (status === "cancelling") return "warning";
      if (status === "waiting_resource" || status === "pending" || status === "ready") return "warning";
      return "neutral";
    }

    function formatJobStatus(status) {
      const normalized = String(status || "").trim().toLowerCase();
      if (normalized === "queued") return "排队中";
      if (normalized === "waiting_resource") return "等待资源";
      if (normalized === "running") return "执行中";
      if (normalized === "success") return "成功";
      if (normalized === "failed") return "失败";
      if (normalized === "cancelled") return "已取消";
      if (normalized === "interrupted") return "已中断";
      if (normalized === "partial_failed") return "部分失败";
      if (normalized === "blocked_precondition") return "前置条件阻塞";
      return String(status || "").trim() || "-";
    }

    function formatJobKind(job) {
      const normalized = String(job?.kind || "").trim().toLowerCase();
      if (normalized === "bridge") return "共享桥接代理";
      return "本地任务";
    }

    function formatJobTone(status) {
      const normalized = String(status || "").trim().toLowerCase();
      if (normalized === "success") return "success";
      if (normalized === "running") return "info";
      if (normalized === "queued" || normalized === "waiting_resource") return "warning";
      if (normalized === "cancelled") return "neutral";
      if (normalized === "interrupted" || normalized === "failed" || normalized === "partial_failed") return "danger";
      return "neutral";
    }

    function formatJobPriority(priority) {
      const normalized = String(priority || "").trim().toLowerCase();
      if (normalized === "manual") return "手动";
      if (normalized === "resume") return "恢复";
      if (normalized === "scheduler") return "调度";
      return String(priority || "").trim() || "-";
    }

    function formatJobSubmittedBy(source) {
      const normalized = String(source || "").trim().toLowerCase();
      if (normalized === "manual") return "手动";
      if (normalized === "resume") return "恢复";
      if (normalized === "scheduler") return "调度";
      return String(source || "").trim() || "-";
    }

    function formatBridgeFeature(feature) {
      const normalized = String(feature || "").trim().toLowerCase();
      if (normalized === "handover_from_download") return "交接班使用共享文件生成";
      if (normalized === "day_metric_from_download") return "12项使用共享文件上传";
      if (normalized === "wet_bulb_collection") return "湿球温度采集";
      if (normalized === "monthly_report_pipeline") return "月报主流程";
      if (normalized === "internal_browser_alert") return "内网环境告警";
      return String(feature || "").trim() || "-";
    }

    function formatBridgeTaskStatus(status) {
      const normalized = String(status || "").trim().toLowerCase();
      if (normalized === "pending") return "待执行";
      if (normalized === "claimed") return "已认领";
      if (normalized === "running") return "执行中";
      if (normalized === "blocked") return "已阻塞";
      if (normalized === "expired") return "已过期";
      if (normalized === "waiting_next_side") return "等待下一侧";
      if (normalized === "queued_for_internal") return "等待共享文件";
      if (normalized === "internal_claimed") return "共享文件已认领";
      if (normalized === "internal_running") return "共享文件准备中";
      if (normalized === "ready_for_external") return "等待外网继续";
      if (normalized === "external_claimed") return "外网已认领";
      if (normalized === "external_running") return "外网处理中";
      if (normalized === "success") return "成功";
      if (normalized === "partial_failed") return "部分失败";
      if (normalized === "failed") return "失败";
      if (normalized === "cancelled") return "已取消";
      if (normalized === "stale") return "超时失效";
      return String(status || "").trim() || "-";
    }

    function formatBridgeTaskTone(status) {
      const normalized = String(status || "").trim().toLowerCase();
      if (normalized === "success") return "success";
      if (normalized === "failed" || normalized === "partial_failed" || normalized === "stale" || normalized === "expired") return "danger";
      if (normalized === "blocked") return "warning";
      if (normalized === "cancelled") return "neutral";
      if (normalized === "queued_for_internal" || normalized === "ready_for_external" || normalized === "pending" || normalized === "waiting_next_side") {
        return "warning";
      }
      if (
        normalized === "internal_claimed" ||
        normalized === "internal_running" ||
        normalized === "external_claimed" ||
        normalized === "external_running" ||
        normalized === "claimed" ||
        normalized === "running"
      ) {
        return "info";
      }
      return "neutral";
    }

    function formatBridgeRole(role) {
      const normalized = String(role || "").trim().toLowerCase();
      if (normalized === "internal") return "内网端";
      if (normalized === "external") return "外网端";
      return String(role || "").trim() || "-";
    }

    function formatBridgeStageName(stage, feature = "", mode = "") {
      const explicit = String(stage?.stage_name || "").trim();
      if (explicit) return explicit;
      const featureText = String(feature || "").trim().toLowerCase();
      const modeText = String(mode || "").trim().toLowerCase();
      const stageId = String(stage?.stage_id || stage?.handler || "").trim().toLowerCase();
      if (stageId === "internal_download") {
        if (featureText === "handover_from_download") return "准备交接班共享文件";
        if (featureText === "day_metric_from_download") return "准备12项共享文件";
        if (featureText === "wet_bulb_collection") return "准备湿球共享文件";
        if (featureText === "monthly_report_pipeline") {
          if (modeText === "multi_date") return "准备月报历史共享文件";
          return "准备月报共享文件";
        }
        return "准备共享文件";
      }
      if (stageId === "internal_query") return "内网查询告警数据";
      if (stageId === "external_generate_review_output") return "使用共享文件生成交接班";
      if (stageId === "external_upload") {
        if (featureText === "day_metric_from_download") return "使用共享文件上传12项";
        return "外网继续上传";
      }
      if (stageId === "external_extract_and_upload") return "使用共享文件上传湿球温度";
      if (stageId === "external_resume") {
        if (featureText === "monthly_report_pipeline" && modeText === "resume_upload") return "外网断点续传月报";
        if (featureText === "monthly_report_pipeline") return "使用共享文件上传月报";
        return "外网继续处理";
      }
      if (stageId === "external_notify") {
        if (featureText === "internal_browser_alert") return "外网发送告警";
        return "外网通知";
      }
      return String(stage?.stage_id || stage?.handler || "").trim() || "-";
    }

    function formatBridgeArtifactKind(artifact) {
      const explicit = String(artifact?.artifact_kind_label || "").trim();
      if (explicit) return explicit;
      const normalized = String(artifact?.artifact_kind || "").trim().toLowerCase();
      if (normalized === "source_file") return "源文件";
      if (normalized === "prepared_rows") return "预处理结果";
      if (normalized === "output_file") return "输出文件";
      if (normalized === "daily_report_asset") return "日报截图资产";
      if (normalized === "resume_state") return "续传状态";
      if (normalized === "manifest") return "清单";
      return String(artifact?.artifact_kind || "").trim() || "-";
    }

    function formatBridgeArtifactStatus(status) {
      const normalized = String(status || "").trim().toLowerCase();
      if (normalized === "preparing") return "生成中";
      if (normalized === "ready") return "可用";
      if (normalized === "failed") return "失败";
      if (normalized === "removed") return "已移除";
      return String(status || "").trim() || "-";
    }

    function formatBridgeEventLevel(level) {
      const normalized = String(level || "").trim().toLowerCase();
      if (normalized === "info") return "信息";
      if (normalized === "warning" || normalized === "warn") return "警告";
      if (normalized === "error") return "错误";
      return String(level || "").trim() || "-";
    }

    function formatBridgeErrorText(value) {
      const normalized = String(value || "").trim();
      if (!normalized) return "";
      const lowered = normalized.toLowerCase();
      if (lowered === "internal_download_failed") return "共享文件准备失败";
      if (lowered === "internal_query_failed") return "内网查询失败";
      if (lowered === "external_upload_failed") return "外网上传失败";
      if (lowered === "external_continue_failed") return "外网继续处理失败";
      if (lowered === "missing_source_file") return "缺少共享文件";
      if (lowered === "await_external") return "等待外网继续处理";
      if (lowered === "shared_bridge_disabled") return "共享桥接未启用";
      if (lowered === "shared_bridge_service_unavailable") return "共享桥接服务不可用";
      if (lowered === "disabled_or_switching" || lowered === "disabled_or_unselected") return "当前未启用共享桥接";
      if (lowered === "misconfigured") return "共享桥接目录未配置";
      if (lowered === "database is locked") return "共享桥接数据库正忙，请稍后重试";
      if (lowered === "unable to open database file") return "无法打开共享桥接数据库文件";
      if (lowered === "cannot operate on a closed database" || lowered === "cannot operate on a closed database.") {
        return "共享桥接数据库连接已关闭";
      }
      if (lowered.includes("permissionerror") || lowered.includes("winerror 5")) {
        return "共享桥接目录无写入权限";
      }
      if (lowered.includes("no such table")) {
        return "共享桥接数据库结构未初始化";
      }
      return normalized;
    }

    function formatBridgeTaskError(task) {
      return (
        formatBridgeErrorText(task?.display_error) ||
        formatBridgeErrorText(task?.current_stage_error) ||
        formatBridgeErrorText(task?.error) ||
        "-"
      );
    }

    function canCancelBridgeTask(task) {
      const taskId = String(task?.task_id || "").trim();
      if (!taskId) return false;
      const status = String(task?.status || "").trim().toLowerCase();
      return !["success", "failed", "partial_failed", "cancelled", "stale"].includes(status);
    }

    function isBridgeTerminalStatusLocal(status) {
      const normalized = String(status || "").trim().toLowerCase();
      return ["success", "failed", "partial_failed", "cancelled", "stale"].includes(normalized);
    }

    function isBridgeWaitingResourceTask(task) {
      if (!task || typeof task !== "object") return false;
      if (isBridgeTerminalStatusLocal(task?.status)) return false;
      const combined = `${formatBridgeTaskError(task)} ${formatBridgeStageSummary(task)}`.trim();
      return (
        combined.includes("等待最新共享文件更新")
        || combined.includes("等待缺失楼栋共享文件补齐")
        || combined.includes("等待过旧楼栋共享文件更新")
        || combined.includes("等待共享文件")
      );
    }

    const waitingResourceJobs = computed(() => {
      const localJobs = Array.isArray(baseWaitingResourceJobs.value)
        ? baseWaitingResourceJobs.value.map((item) => ({
            __waiting_kind: "job",
            __waiting_id: `job:${String(item?.job_id || "").trim()}`,
            ...item,
          }))
        : [];
      const bridgeWaits = Array.isArray(bridgeTasks.value)
        ? bridgeTasks.value
            .filter((item) => isBridgeWaitingResourceTask(item))
            .map((item) => ({
              __waiting_kind: "bridge",
              __waiting_id: `bridge:${String(item?.task_id || "").trim()}`,
              ...item,
            }))
        : [];
      return [...bridgeWaits, ...localJobs];
    });

    function isWaitingResourceItemSelected(item) {
      const kind = String(item?.__waiting_kind || "job").trim().toLowerCase();
      if (kind === "bridge") {
        return String(selectedBridgeTaskId.value || "").trim() === String(item?.task_id || "").trim();
      }
      return String(selectedJobId.value || "").trim() === String(item?.job_id || "").trim();
    }

    async function focusWaitingResourceItem(item) {
      const kind = String(item?.__waiting_kind || "job").trim().toLowerCase();
      if (kind === "bridge") {
        await focusBridgeTask(item);
        return;
      }
      await focusJob(item);
    }

    function formatWaitingResourceItemTitle(item) {
      const kind = String(item?.__waiting_kind || "job").trim().toLowerCase();
      if (kind === "bridge") {
        return item?.feature_label || formatBridgeFeature(item?.feature) || item?.task_id || "-";
      }
      return item?.name || item?.feature || item?.job_id || "-";
    }

    function formatWaitingResourceItemMeta(item) {
      const kind = String(item?.__waiting_kind || "job").trim().toLowerCase();
      if (kind === "bridge") {
        const reason = formatBridgeTaskError(item);
        const summary = reason !== "-" ? reason : formatBridgeStageSummary(item);
        return `共享桥接 | #${String(item?.task_id || "").trim() || "-"} | ${summary || "-"}`;
      }
      return `${formatJobKind(item)} | #${String(item?.job_id || "").trim() || "-"} | ${formatJobWaitReason(item)}`;
    }

    function formatBridgeStageSummary(task) {
      const currentStageName = String(task?.current_stage_name || "").trim();
      const currentStageRole = String(task?.current_stage_role || "").trim();
      const currentStageStatus = String(task?.current_stage_status || "").trim();
      if (currentStageName) {
        return `${formatBridgeRole(currentStageRole)} / ${currentStageName} / ${formatBridgeTaskStatus(currentStageStatus || task?.status)}`;
      }
      const stages = Array.isArray(task?.stages) ? task.stages : [];
      if (!stages.length) return "阶段信息待同步";
      const current =
        stages.find((item) => {
          const status = String(item?.status || "").trim().toLowerCase();
          return status === "running" || status === "claimed" || status === "pending" || status === "waiting_next_side";
        }) || stages.find((item) => String(item?.error || "").trim()) || stages[stages.length - 1];
      return `${formatBridgeRole(current?.role_target)} / ${formatBridgeStageName(current, task?.feature, task?.mode)} / ${formatBridgeTaskStatus(current?.status)}`;
    }

    function formatBridgeArtifactSummary(task) {
      const artifacts = Array.isArray(task?.artifacts) ? task.artifacts : [];
      if (!artifacts.length) return "暂无产物";
      const readyCount = artifacts.filter((item) => String(item?.status || "").trim().toLowerCase() === "ready").length;
      return `产物 ${readyCount}/${artifacts.length}`;
    }

    function formatBridgeEventText(event) {
      const explicit = String(event?.event_text || "").trim();
      if (explicit) return explicit;
      const payload = event?.payload && typeof event.payload === "object" ? event.payload : {};
      return (
        formatBridgeErrorText(payload?.message) ||
        formatBridgeErrorText(payload?.error) ||
        formatBridgeErrorText(event?.event_type) ||
        "-"
      );
    }

    async function focusJob(jobLike) {
      const job =
        jobLike && typeof jobLike === "object"
          ? jobLike
          : jobsList.value.find((item) => String(item?.job_id || "").trim() === String(jobLike || "").trim());
      const jobId = String(job?.job_id || "").trim();
      if (!jobId) return;
      selectedJobId.value = jobId;
      currentJob.value = { ...(currentJob.value || {}), ...job };
      streamController.attachJobStream(jobId);
      await fetchJob(jobId);
    }

    async function focusBridgeTask(taskLike) {
      const task =
        taskLike && typeof taskLike === "object"
          ? taskLike
          : bridgeTasks.value.find((item) => String(item?.task_id || "").trim() === String(taskLike || "").trim());
      const taskId = String(task?.task_id || "").trim();
      if (!taskId) return;
      selectedBridgeTaskId.value = taskId;
      if (bridgeTaskDetail.value && String(bridgeTaskDetail.value?.task_id || "").trim() === taskId) {
        bridgeTaskDetail.value = { ...bridgeTaskDetail.value, ...task };
      } else {
        bridgeTaskDetail.value = task;
      }
      await fetchBridgeTaskDetail(taskId, { silentMessage: true });
    }

    async function onHandoverDailyReportAssetFileChange(target, event) {
      const file = event?.target?.files?.[0];
      if (!file) return;
      try {
        await uploadHandoverDailyReportAsset(target, file, String(file.name || "").trim());
      } finally {
        if (event?.target) event.target.value = "";
      }
    }

    async function onHandoverDailyReportUploadPaste(event) {
      const items = Array.from(event?.clipboardData?.items || []);
      const imageItem = items.find((item) => String(item?.type || "").toLowerCase().startsWith("image/"));
      if (!imageItem) {
        event?.preventDefault?.();
        message.value = "剪贴板中没有图片";
        return;
      }
      const blob = imageItem.getAsFile();
      if (!blob) {
        event?.preventDefault?.();
        message.value = "剪贴板图片读取失败";
        return;
      }
      event?.preventDefault?.();
      const target = String(handoverDailyReportUploadModal.value?.target || "").trim().toLowerCase();
      await uploadHandoverDailyReportAsset(target, blob, "clipboard.png");
    }

    let configAutoSaveTimer = null;
    const scheduleAutoSaveConfig = () => {
      if (!config.value) return;
      if ((configAutoSaveSuspendDepth?.value || 0) > 0) return;
      if (configAutoSaveTimer) {
        window.clearTimeout(configAutoSaveTimer);
      }
      configAutoSaveTimer = window.setTimeout(() => {
        configAutoSaveTimer = null;
        autoSaveConfig();
      }, 1200);
    };

    const shouldPauseRuntimeRequests = computed(() => {
      return Boolean(
        !startupRoleSelectorHandled.value
        || !startupRoleGateReady.value
        || updaterUiOverlayVisible.value
        || updaterAwaitingRestartRecovery.value
        || startupRoleSelectorVisible.value
        || startupRoleLoadingVisible.value,
      );
    });

    watch(
      () => ({
        bootstrapReady: bootstrapReady.value,
        configLoaded: configLoaded.value,
        currentRole: startupRoleCurrentMode.value,
        currentStartupToken: startupRoleCurrentToken.value,
        flowState: startupRoleFlowState.value,
        overlayVisible: updaterUiOverlayVisible.value,
        selectorVisible: startupRoleSelectorVisible.value,
        selectorBusy: startupRoleSelectorBusy.value,
        loadingVisible: startupRoleLoadingVisible.value,
        startupRoleConfirmed: Boolean(health.startup_role_confirmed),
        runtimeActivated: Boolean(health.runtime_activated),
        roleSelectionRequired: Boolean(health.role_selection_required),
        startupHandoffActive: Boolean(health.startup_handoff?.active),
        startupHandoffRole: normalizeDeploymentRoleMode(health.startup_handoff?.target_role_mode),
        startupHandoffNonce: String(health.startup_handoff?.nonce || "").trim(),
      }),
      (state) => {
        if (!state.bootstrapReady || !state.configLoaded) return;
        if (state.overlayVisible || state.loadingVisible) return;
        const savedRole = normalizeDeploymentRoleMode(state.currentRole);
        const startupHandoff = currentStartupHandoff();
        const canResumeAfterRestart =
          Boolean(startupHandoff.active)
          && Boolean(startupHandoff.target_role_mode)
          && Boolean(startupHandoff.nonce)
          && startupHandoff.nonce !== startupRoleSuppressedHandoffNonce.value;
        if (canResumeAfterRestart && !state.runtimeActivated) {
          const resumeRole = startupHandoff.target_role_mode || savedRole || startupRoleSelectorSelection.value || "internal";
          const activationKey = `${state.currentStartupToken || ""}|${resumeRole}|${startupHandoff.nonce}|restart_resume`;
          if (startupRoleSelectorBusy.value || startupRoleLoadingVisible.value) return;
          if (startupRoleAutoActivationKey.value === activationKey) return;
          selectStartupRole(resumeRole);
          syncStartupRoleBridgeDraft();
          startupRoleDecisionReady.value = true;
          startupRoleSelectorHandled.value = true;
          startupRoleSelectorVisible.value = false;
          startupRoleAutoActivationKey.value = activationKey;
          startupRoleSelectorBusy.value = true;
          showStartupRoleLoading({
            title: `正在继续启动${formatDeploymentRoleLabel(resumeRole || "internal")}`,
            subtitle: "服务已恢复，正在继续连接后台运行时，请稍候。",
            stage: "restarting",
          });
          void (async () => {
            const activated = await activateStartupRuntimeAfterSelection("startup_role_resume_after_restart", {
              targetRoleMode: resumeRole,
              startupHandoffNonce: startupHandoff.nonce,
            });
            startupRoleSelectorBusy.value = false;
            if (!activated) {
              suppressCurrentStartupHandoff();
              clearLegacyStartupRoleRestartState();
              message.value = "服务已恢复，但后台运行时启动失败，请重新确认启动角色。";
              showStartupRoleSelector("后台运行时激活失败，请重新确认启动角色。");
              return;
            }
            clearLegacyStartupRoleRestartState();
            closeStartupRoleSelector({ handled: true });
          })();
          return;
        }
        if (state.runtimeActivated) {
          const activatedRole =
            savedRole || normalizeDeploymentRoleMode(startupRoleSelectorSelection.value) || "internal";
          const activationKey = `${state.currentStartupToken || ""}|${activatedRole}`;
          startupRoleDecisionReady.value = true;
          startupRoleSelectorHandled.value = true;
          startupRoleSelectorVisible.value = false;
          startupRoleAutoActivationKey.value = activationKey;
          hideStartupRoleLoading();
          startupRoleSelectorBusy.value = false;
          clearLegacyStartupRoleRestartState();
          startupRoleSuppressedHandoffNonce.value = "";
          return;
        }
        if (state.selectorVisible || state.selectorBusy) return;
        clearLegacyStartupRoleRestartState();
        startupRoleAutoActivationKey.value = "";
        selectStartupRole(savedRole || startupRoleSelectorSelection.value || "internal");
        syncStartupRoleBridgeDraft();
        hideStartupRoleLoading();
        showStartupRoleSelector(savedRole ? "" : "请先选择有效角色。");
      },
      { immediate: true, deep: false },
    );

    watch(
      () => startupRoleSelectorVisible.value,
      (visible) => {
        if (visible) {
          streamController?.pauseAll?.();
          return;
        }
        if (!updaterUiOverlayVisible.value && !updaterAwaitingRestartRecovery.value) {
          streamController?.resumeAll?.();
        }
      },
      { immediate: false },
    );

    const shouldFetchHealth = computed(() => {
      if (shouldPauseRuntimeRequests.value) return false;
      const view = String(currentView.value || "").trim().toLowerCase();
      return view === "dashboard" || view === "status";
    });

    const shouldPollJobPanel = computed(() => {
      if (shouldPauseRuntimeRequests.value) return false;
      const view = String(currentView.value || "").trim().toLowerCase();
      return view === "dashboard";
    });
    const shouldFetchPendingResumeRuns = computed(() => {
      if (shouldPauseRuntimeRequests.value) return false;
      if (!fullHealthLoaded.value) return false;
      if (deploymentRoleMode.value === "internal") return false;
      const view = String(currentView.value || "").trim().toLowerCase();
      return view === "dashboard";
    });
    const healthPollIntervalMs = computed(() => {
      if (shouldPauseRuntimeRequests.value) return 5000;
      const view = String(currentView.value || "").trim().toLowerCase();
      if (deploymentRoleMode.value === "internal" && view === "status") {
        return 2000;
      }
      return 5000;
    });
    const shouldPollBridgeTasks = computed(() => {
      if (shouldPauseRuntimeRequests.value) return false;
      if (!bridgeTasksEnabled.value) return false;
      const view = String(currentView.value || "").trim().toLowerCase();
      return view === "dashboard" || view === "status";
    });

    const shouldIncludeHandoverHealthContext = computed(() => {
      if (shouldPauseRuntimeRequests.value) return false;
      if (deploymentRoleMode.value === "internal") return false;
      const view = String(currentView.value || "").trim().toLowerCase();
      const moduleId = String(dashboardActiveModule.value || "").trim();
      if (view === "status") return true;
      if (view === "dashboard") {
        return moduleId === "handover_log";
      }
      return false;
    });

    watch(
      () => ({
        config: config.value,
        buildingsText: buildingsText.value,
        sheetRuleRows: sheetRuleRows.value,
        customAbsoluteStartLocal: customAbsoluteStartLocal.value,
        customAbsoluteEndLocal: customAbsoluteEndLocal.value,
      }),
      () => {
        if (!configLoaded.value) return;
        if ((configAutoSaveSuspendDepth?.value || 0) > 0) return;
        scheduleAutoSaveConfig();
      },
      { deep: true },
    );

    watch(
      () => [handoverDutyDate.value, handoverDutyShift.value],
      () => {
        persistHandoverDutyContext(handoverDutyDate.value, handoverDutyShift.value);
        if (!bootstrapReady.value) return;
        if (shouldFetchHealth.value) {
          fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
        }
        if (shouldPollHandoverDailyReportContext.value) {
          fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
        }
      },
      { immediate: false },
    );

    const shouldPollHandoverDailyReportContext = computed(() => {
      if (shouldPauseRuntimeRequests.value) return false;
      if (deploymentRoleMode.value === "internal") return false;
      const view = String(currentView.value || "").trim().toLowerCase();
      const moduleId = String(dashboardActiveModule.value || "").trim();
      const configTab = String(activeConfigTab.value || "").trim();
      if (view === "dashboard") {
        return moduleId === "handover_log";
      }
      if (view === "config") {
        return configTab === "feature_handover";
      }
      return false;
    });

    const shouldLoadEngineerDirectory = computed(() => {
      if (shouldPauseRuntimeRequests.value) return false;
      if (deploymentRoleMode.value === "internal") return false;
      const moduleId = String(dashboardActiveModule.value || "").trim();
      const configTab = String(activeConfigTab.value || "").trim();
      return moduleId === "handover_log" || configTab === "feature_handover";
    });

    watch(
      () => [dashboardActiveModule.value, activeConfigTab.value],
      () => {
        if (!shouldLoadEngineerDirectory.value) return;
        void ensureHandoverEngineerDirectoryLoaded({ silentMessage: true });
      },
      { immediate: true },
    );

    watch(
      () => deploymentRoleMode.value,
      (roleMode) => {
        applyDashboardRoleMode(roleMode);
        const hiddenCommonTabs = roleMode === "internal"
          ? new Set(["common_paths", "common_console", "common_scheduler", "common_notify", "common_feishu_auth"])
          : new Set(["common_alarm_db"]);
        const hiddenFeatureTabs = new Set(["feature_alarm"]);
        if (roleMode === "internal") {
          hiddenFeatureTabs.add("feature_monthly");
          hiddenFeatureTabs.add("feature_handover");
          hiddenFeatureTabs.add("feature_wet_bulb_collection");
          hiddenFeatureTabs.add("feature_alarm_export");
          hiddenFeatureTabs.add("feature_sheet");
          hiddenFeatureTabs.add("feature_manual");
        }
        const currentTab = String(activeConfigTab.value || "").trim();
        if (hiddenCommonTabs.has(currentTab)) {
          activeConfigTab.value = "common_deployment";
        } else if (hiddenFeatureTabs.has(currentTab)) {
          activeConfigTab.value = "common_deployment";
        }
        if (roleMode === "internal" && currentView.value === "dashboard") {
          currentView.value = "status";
        }
      },
      { immediate: true },
    );

    watch(
      () => shouldPollHandoverDailyReportContext.value,
      (enabled) => {
        if (!enabled || !bootstrapReady.value) return;
        void fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
      },
      { immediate: true },
    );

    watch(
      () => shouldPollBridgeTasks.value,
      (enabled) => {
        if (!enabled || !bootstrapReady.value) return;
        void fetchBridgeTasks({ silentMessage: true });
      },
      { immediate: true },
    );

    watch(
      () => [currentView.value, dashboardActiveModule.value],
      ([view]) => {
        if (!bootstrapReady.value) return;
        if (String(view || "").trim().toLowerCase() === "dashboard") {
          void fetchJobs({ silentMessage: true });
          void fetchRuntimeResources({ silentMessage: true });
          if (shouldPollBridgeTasks.value) {
            void fetchBridgeTasks({ silentMessage: true });
          }
        }
        if (shouldFetchHealth.value) {
          void fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
        }
      },
      { immediate: true },
    );

    const dashboardActions = createDashboardActions({
      canRun,
      busy,
      message,
      currentJob,
      selectedJobId,
      selectedBridgeTaskId,
      bridgeTaskDetail,
      config,
      schedulerQuickSaving,
      handoverSchedulerQuickSaving,
      wetBulbSchedulerQuickSaving,
      monthlyEventReportSchedulerQuickSaving,
      monthlyChangeReportSchedulerQuickSaving,
      monthlyReportTestReceiveIds,
      monthlyReportTestReceiveIdType,
      selectedDates,
      manualBuilding,
      manualFile,
      manualUploadDate,
      sheetFile,
      handoverFilesByBuilding,
      handoverConfiguredBuildings,
      handoverDutyDate,
      handoverDutyShift,
      handoverDownloadScope,
      handoverDutyAutoFollow,
      dayMetricUploadScope,
      dayMetricUploadBuilding,
      dayMetricSelectedDates,
      dayMetricLocalBuilding,
      dayMetricLocalDate,
      dayMetricLocalFile,
      streamController,
      fetchHealth,
      fetchJobs,
      fetchBridgeTasks,
      fetchBridgeTaskDetail,
      syncHandoverDutyFromNow,
      runSingleFlight,
    });

    const {
      runAutoOnce,
      runWetBulbCollection,
      runMonthlyEventReport,
      runMonthlyChangeReport,
      sendMonthlyReport,
      sendMonthlyReportTest,
      runMultiDate,
      runManualUpload,
      runSheetImport,
      fetchJob,
      startScheduler,
      saveSchedulerQuickConfig,
      startHandoverScheduler,
      stopHandoverScheduler,
      saveHandoverSchedulerQuickConfig,
      startWetBulbCollectionScheduler,
      stopWetBulbCollectionScheduler,
      saveWetBulbCollectionSchedulerQuickConfig,
      startMonthlyEventReportScheduler,
      stopMonthlyEventReportScheduler,
      saveMonthlyEventReportSchedulerQuickConfig,
      startMonthlyChangeReportScheduler,
      stopMonthlyChangeReportScheduler,
      saveMonthlyChangeReportSchedulerQuickConfig,
      runHandoverFromFile,
      runHandoverFromDownload,
      runDayMetricFromDownload,
      runDayMetricFromFile,
      retryDayMetricUnit,
      retryFailedDayMetricUnits,
      continueHandoverFollowupUpload,
      cancelCurrentJob,
      retryCurrentJob,
      getJobCancelActionKey,
      getJobRetryActionKey,
      stopScheduler,
    } = dashboardActions;

    const realStreamController = createLogStreamController({
      appendLog,
      setMessage: (text) => {
        message.value = String(text || "");
      },
      getSystemOffset: () => systemLogOffset.value,
      setSystemOffset: (offset) => {
        const next = Number.parseInt(String(offset), 10);
        if (Number.isInteger(next) && next >= 0) {
          systemLogOffset.value = next;
        }
      },
      onJobDone: async (jobId) => {
        await fetchJob(jobId);
        await fetchJobs({ silentMessage: true });
        await fetchRuntimeResources({ silentMessage: true });
        await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
        if (shouldFetchPendingResumeRuns.value) {
          await fetchPendingResumeRuns({ silentMessage: true });
        }
      },
      onJobReconnect: async (jobId) => {
        await fetchJob(jobId);
        await fetchJobs({ silentMessage: true });
        await fetchRuntimeResources({ silentMessage: true });
        await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
      },
    });
    Object.assign(streamController, realStreamController);

    registerAppLifecycle(
      { onMounted, onBeforeUnmount },
      {
        fetchBootstrapHealth,
        fetchHealth,
        fetchJobs,
        fetchBridgeTasks,
        fetchRuntimeResources,
        fetchHandoverDailyReportContext,
        fetchConfig,
        syncHandoverDutyFromNow,
        fetchPendingResumeRuns,
        shouldFetchPendingResumeRuns: () => shouldFetchPendingResumeRuns.value,
        shouldPollHandoverDailyReportContext: () => shouldPollHandoverDailyReportContext.value,
        shouldPollBridgeTasks: () => shouldPollBridgeTasks.value,
        shouldFetchHealth: () => shouldFetchHealth.value,
        shouldPollJobPanel: () => shouldPollJobPanel.value,
        shouldLoadEngineerDirectory: () => shouldLoadEngineerDirectory.value,
        shouldPauseRuntimeRequests: () => shouldPauseRuntimeRequests.value,
        scheduleEngineerDirectoryPrefetch,
        tryAutoResume,
        fetchJob,
        currentJob,
        streamController,
        timers,
        bootstrapReady,
        getHealthPollIntervalMs: () => healthPollIntervalMs.value,
      },
    );

    onBeforeUnmount(() => {
      if (configAutoSaveTimer) {
        window.clearTimeout(configAutoSaveTimer);
        configAutoSaveTimer = null;
      }
    });

    return {
      health,
      config,
      currentView,
      activeConfigTab,
      dashboardMenuGroups,
      dashboardModules,
      dashboardActiveModule,
      dashboardModuleMenuOpen,
      dashboardActiveModuleTitle,
      moduleMeta,
      isStatusView,
      isDashboardView,
      isConfigView,
      selectedDate,
      rangeStartDate,
      rangeEndDate,
      selectedDates,
      selectedDateCount,
      dayMetricUploadScope,
      dayMetricUploadBuilding,
      dayMetricSelectedDate,
      dayMetricRangeStartDate,
      dayMetricRangeEndDate,
      dayMetricSelectedDates,
      dayMetricSelectedDateCount,
      dayMetricLocalBuilding,
      dayMetricLocalDate,
      dayMetricLocalFile,
      pendingResumeRuns,
      pendingResumeCount,
      jobsList,
      selectedJobId,
      bridgeTasks,
      selectedBridgeTaskId,
      bridgeTaskDetail,
      runningJobs,
      waitingResourceJobs,
      recentFinishedJobs,
      bridgeTasksEnabled,
      activeBridgeTasks,
      displayedBridgeTasks,
      totalBridgeHistoryCount,
      hiddenBridgeHistoryCount,
      bridgeTaskHistoryDisplayLimit,
      recentFinishedBridgeTasks,
      currentBridgeTask,
      resourceSnapshot,
      schedulerQuickSaving,
      handoverSchedulerQuickSaving,
      wetBulbSchedulerQuickSaving,
      monthlyEventReportSchedulerQuickSaving,
      monthlyChangeReportSchedulerQuickSaving,
      schedulerDecisionText,
      schedulerTriggerText,
      wetBulbSchedulerDecisionText,
      wetBulbSchedulerTriggerText,
      monthlyEventReportSchedulerDecisionText,
      monthlyEventReportSchedulerTriggerText,
      monthlyChangeReportSchedulerDecisionText,
      monthlyChangeReportSchedulerTriggerText,
      logs,
      filteredLogs,
      logFilter,
      currentJob,
      busy,
      message,
      bootstrapReady,
      fullHealthLoaded,
      configLoaded,
      engineerDirectoryLoaded,
      initialLoadingPhase,
      initialLoadingStatusText,
      buildingsText,
      sheetRuleRows,
      manualBuilding,
      manualUploadDate,
      handoverDutyDate,
      handoverDutyShift,
      handoverRuleScope,
      handoverDutyAutoFollow,
      handoverDutyLastAutoAt,
      handoverDutyAutoLabel,
      handoverConfiguredBuildings,
      handoverSelectedBuildings,
      handoverSelectedFileCount,
      hasSelectedHandoverFiles,
      handoverFileStatesByBuilding,
      handoverEngineerDirectory,
      handoverEngineerLoading,
      handoverDailyReportContext,
      handoverDailyReportLastScreenshotTest,
      handoverDailyReportPreviewModal,
      handoverDailyReportUploadModal,
      handoverRuleScopeOptions,
      handoverDownloadScope,
      handoverMorningDecisionText,
      handoverAfternoonDecisionText,
      handoverReviewStatusItems,
      handoverReviewLinks,
      handoverReviewMatrix,
      handoverReviewBoardRows,
      dashboardSystemStatusItems,
      dashboardScheduleStatusItems,
      isInternalRole,
      internalDownloadPoolOverview,
      internalSourceCacheOverview,
      internalRealtimeSourceFamilies,
      externalInternalAlertOverview,
      currentHourRefreshOverview,
      internalRuntimeOverview,
      internalSourceCacheHistoryOverview,
      sharedSourceCacheReadinessOverview,
      updaterMirrorOverview,
      dayMetricUploadEnabled,
      dayMetricLocalImportEnabled,
      dayMetricCurrentPayload,
      dayMetricCurrentResultRows,
      dayMetricRetryableFailedCount,
      dayMetricRetryAllMode,
      handoverReviewOverview,
      handoverFollowupProgress,
      handoverDailyReportAuthVm,
      handoverDailyReportExportVm,
      handoverDailyReportSpreadsheetUrl,
      handoverDailyReportCaptureAssets,
      handoverDailyReportSummaryTestVm,
      handoverDailyReportExternalTestVm,
      canRewriteHandoverDailyReportRecord,
      updaterResultText,
      currentTaskOverview,
      homeOverview,
      statusDiagnosisOverview,
      configGuidanceOverview,
      dashboardActiveModuleHero,
      updaterMainButtonText,
      isUpdaterActionLocked,
      deploymentRoleMode,
      deploymentNodeIdDisplayText,
      deploymentNodeIdDisplayHint,
      showCommonPathsConfigTab,
      showCommonSchedulerConfigTab,
      showNotifyConfigTab,
      showFeishuAuthConfigTab,
      showCommonAlarmDbConfigTab,
      showConsoleConfigTab,
      showFeatureMonthlyConfigTab,
      showFeatureHandoverConfigTab,
      showFeatureWetBulbCollectionConfigTab,
      showFeatureAlarmExportConfigTab,
      showSheetImportConfigTab,
      showManualFeatureConfigTab,
      showRuntimeNetworkPanel,
      showDashboardPageNav,
      appShellTitle,
      statusNavLabel,
      dashboardNavLabel,
      configNavLabel,
      configShellTitle,
      configShellDescription,
      configReturnButtonText,
      statusHeroTitle,
      statusHeroDescription,
      bridgeExecutionHint,
      externalExecutionHint,
      resumeExecutionHint,
      isInternalDeploymentRole,
      isExternalDeploymentRole,
      startupRoleGateReady,
      startupRoleGateVisible,
      shouldRenderAppShell,
      startupRoleSelectorVisible,
      startupRoleSelectorBusy,
      startupRoleSelectorSelection,
      startupRoleSelectorMessage,
      startupRoleLoadingVisible,
      startupRoleLoadingTitle,
      startupRoleLoadingSubtitle,
      startupRoleLoadingStage,
      startupRoleOptions,
      startupRoleCurrentLabel,
      startupRoleSelectedLabel,
      startupRoleNodeIdDisplayText,
      startupRoleNodeIdDisplayHint,
      startupRoleActionButtonText,
      startupRoleRequiresBridgeConfig,
      startupRoleBridgeDraft,
      startupRoleAdvancedVisible,
      startupRoleBridgeValidationMessage,
      startupRoleBridgeNoticeText,
      startupRoleConfirmDisabled,
      updaterUiOverlayVisible,
      updaterUiOverlayTitle,
      updaterUiOverlaySubtitle,
      updaterUiOverlayStage,
      updaterUiOverlayKicker,
      customAbsoluteStartLocal,
      customAbsoluteEndLocal,
      canRun,
      handoverGenerationBusy,
      isActionLocked,
      actionKeyAutoOnce,
      actionKeyMultiDate,
      actionKeyManualUpload,
      actionKeySheetImport,
      actionKeyHandoverFromFile,
      actionKeyHandoverFromDownload,
      actionKeyDayMetricFromDownload,
      actionKeyDayMetricFromFile,
      actionKeyDayMetricRetryUnit,
      actionKeyDayMetricRetryFailed,
      actionKeySchedulerStart,
      actionKeySchedulerStop,
      actionKeySchedulerSave,
      actionKeyHandoverSchedulerStart,
      actionKeyHandoverSchedulerStop,
      actionKeyHandoverSchedulerSave,
      actionKeyWetBulbCollectionRun,
      actionKeyWetBulbSchedulerStart,
      actionKeyWetBulbSchedulerStop,
      actionKeyWetBulbSchedulerSave,
      actionKeyConfigSave,
      actionKeyUpdaterCheck,
      actionKeyUpdaterApply,
      actionKeySourceCacheRefreshCurrentHour,
      actionKeySourceCacheRefreshAlarmManual,
      actionKeySourceCacheDeleteAlarmManual,
      actionKeyHandoverConfirmAll,
      actionKeyHandoverCloudRetryAll,
      actionKeyHandoverDailyReportAuthOpen,
      actionKeyHandoverDailyReportScreenshotTest,
      actionKeyHandoverReviewAccessReprobe: ACTION_KEY_HANDOVER_REVIEW_ACCESS_REPROBE,
      actionKeyHandoverDailyReportRecordRewrite: ACTION_KEY_HANDOVER_DAILY_REPORT_RECORD_REWRITE,
      isHandoverConfirmAllLocked,
      isHandoverCloudRetryAllLocked,
      handoverConfirmAllButtonText,
      canShowHandoverCloudRetryAll,
      handoverCloudRetryAllButtonText,
      isHandoverCloudRetryAllDisabled,
      isHandoverFollowupContinueLocked,
      canShowHandoverFollowupContinue,
      handoverFollowupContinueButtonText,
      openStatusPage,
      openDashboardPage,
      openConfigPage,
      runHomeQuickAction,
      switchConfigTab,
      setDashboardActiveModule,
      openDashboardMenuDrawer,
      closeDashboardMenuDrawer,
      addDate,
      addDateRange,
      quickRangeToday,
      removeDate,
      clearDates,
      selectStartupRole,
      addDayMetricDate,
      addDayMetricDateRange,
      removeDayMetricDate,
      clearDayMetricDates,
      runAutoOnce,
      runWetBulbCollection,
      runMultiDate,
      runResumeUpload,
      deleteResumeRun,
      getResumeRunId,
      getResumeRunActionKey,
      getResumeDeleteActionKey,
      formatResumeDateSummary,
      formatResumeDateFull,
      runManualUpload,
      runSheetImport,
      runHandoverFromFile,
      runHandoverFromDownload,
      runDayMetricFromDownload,
      runDayMetricFromFile,
      retryDayMetricUnit,
      retryFailedDayMetricUnits,
      cancelCurrentJob,
      retryCurrentJob,
      getJobCancelActionKey,
      getJobRetryActionKey,
      onHandoverDutyDateManualChange,
      onHandoverDutyShiftManualChange,
      restoreAutoHandoverDuty,
      saveConfig,
      confirmStartupRoleSelection,
      checkUpdaterNow,
      applyUpdaterPatch,
      restartUpdaterApp,
      confirmAllHandoverReview,
      retryAllFailedHandoverCloudSync,
      continueHandoverFollowupUpload,
      formatJobWaitReason,
      formatNetworkWindowSide,
      formatDetectedNetworkSide,
      formatSsidSide,
      formatNetworkMode,
      formatBooleanReachability,
      formatWetBulbTargetKind,
      wetBulbConfiguredTarget,
      wetBulbLatestRunTarget,
      formatJobStageStatus,
      formatJobStageTone,
      formatJobStatus,
      formatJobKind,
      formatJobTone,
      formatJobPriority,
      formatJobSubmittedBy,
      formatBridgeFeature,
      formatBridgeTaskStatus,
      formatBridgeTaskTone,
      formatBridgeRole,
      formatBridgeStageName,
      formatBridgeStageSummary,
      formatBridgeArtifactKind,
      formatBridgeArtifactStatus,
      formatBridgeArtifactSummary,
      formatBridgeTaskError,
      canCancelBridgeTask,
      isWaitingResourceItemSelected,
      focusWaitingResourceItem,
      formatWaitingResourceItemTitle,
      formatWaitingResourceItemMeta,
      formatBridgeEventLevel,
      formatBridgeEventText,
      focusJob,
      focusBridgeTask,
      cancelBridgeTask,
      retryBridgeTask,
      getBridgeTaskCancelActionKey,
      getBridgeTaskRetryActionKey,
      openHandoverDailyReportScreenshotAuth,
      runHandoverDailyReportScreenshotTest,
      openHandoverDailyReportPreview,
      closeHandoverDailyReportPreview,
      openHandoverDailyReportUploadDialog,
      closeHandoverDailyReportUploadDialog,
      onHandoverDailyReportAssetFileChange,
      onHandoverDailyReportUploadPaste,
      recaptureHandoverDailyReportAsset,
      restoreHandoverDailyReportAutoAsset,
      rewriteHandoverDailyReportRecord,
      reprobeHandoverReviewAccess,
      getHandoverDailyReportRecaptureActionKey,
      getHandoverDailyReportUploadActionKey,
      getHandoverDailyReportRestoreActionKey,
      runUpdaterMainAction,
      refreshCurrentHourSourceCache,
      refreshBuildingLatestSourceCache,
      refreshManualAlarmSourceCache,
      deleteManualAlarmSourceCacheFiles,
      uploadAlarmSourceCacheFull,
      uploadAlarmSourceCacheBuilding,
      openAlarmEventUploadTarget,
      runMonthlyEventReport,
      runMonthlyChangeReport,
      sendMonthlyReport,
      sendMonthlyReportTest,
      addSheetRuleRow,
      removeSheetRuleRow,
      startScheduler,
      saveSchedulerQuickConfig,
      startHandoverScheduler,
      stopHandoverScheduler,
      saveHandoverSchedulerQuickConfig,
      startWetBulbCollectionScheduler,
      stopWetBulbCollectionScheduler,
      saveWetBulbCollectionSchedulerQuickConfig,
      startMonthlyEventReportScheduler,
      stopMonthlyEventReportScheduler,
      saveMonthlyEventReportSchedulerQuickConfig,
      startMonthlyChangeReportScheduler,
      stopMonthlyChangeReportScheduler,
      saveMonthlyChangeReportSchedulerQuickConfig,
      stopScheduler,
      onManualFileChange,
      onSheetFileChange,
      onDayMetricLocalFileChange,
      onHandoverBuildingFileChange,
      addSiteRow,
      removeSiteRow,
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
      fetchHandoverEngineerDirectory,
      getInternalSourceCacheRefreshActionKey,
      getInternalSourceCacheRefreshDisabledReason,
      isInternalSourceCacheRefreshLocked,
      getInternalSourceCacheRefreshButtonText,
      isSourceCacheRefreshCurrentHourLocked,
      currentHourRefreshButtonText,
      isSourceCacheRefreshAlarmManualLocked,
      manualAlarmRefreshButtonText,
      isSourceCacheDeleteAlarmManualLocked,
      manualAlarmDeleteButtonText,
      externalAlarmUploadBuilding,
      isSourceCacheUploadAlarmSelectedLocked,
      externalAlarmUploadActionButtonText,
      uploadSelectedAlarmSourceCache,
      externalAlarmReadinessFamily,
      externalAlarmUploadStatus,
      monthlyEventReportLastRun,
      monthlyChangeReportLastRun,
      monthlyEventReportOutputDir,
      monthlyChangeReportOutputDir,
      monthlyEventReportDeliveryLastRun,
      monthlyChangeReportDeliveryLastRun,
      monthlyEventReportRecipientStatusByBuilding,
      monthlyChangeReportRecipientStatusByBuilding,
      monthlyEventReportSendReadyCount,
      monthlyChangeReportSendReadyCount,
      monthlyEventReportDeliveryStatus,
      monthlyChangeReportDeliveryStatus,
      monthlyEventReportSendAllActionKey,
      monthlyChangeReportSendAllActionKey,
      monthlyEventReportSendTestActionKey,
      monthlyChangeReportSendTestActionKey,
      monthlyReportTestReceiveIdDraftEvent,
      monthlyReportTestReceiveIdDraftChange,
      monthlyReportTestReceiveIdType,
      monthlyReportTestReceiveIds,
      monthlyReportTestReceiveCount,
      addMonthlyReportTestReceiveId,
      removeMonthlyReportTestReceiveId,
      getMonthlyReportSendBuildingActionKey,
      handoverEngineerDirectoryTarget,
      alarmEventUploadTarget,
      dayMetricUploadTarget,
      actionKeyMonthlyEventReportRunAll,
      actionKeyMonthlyEventReportRunBuildingPrefix,
      actionKeyMonthlyChangeReportRunAll,
      actionKeyMonthlyChangeReportRunBuildingPrefix,
      actionKeyMonthlyEventReportSchedulerStart,
      actionKeyMonthlyEventReportSchedulerStop,
      actionKeyMonthlyEventReportSchedulerSave,
      actionKeyMonthlyChangeReportSchedulerStart,
      actionKeyMonthlyChangeReportSchedulerStop,
      actionKeyMonthlyChangeReportSchedulerSave,
      actionKeyMonthlyReportSendAllPrefix,
      actionKeyMonthlyReportSendBuildingPrefix,
      actionKeyMonthlyReportSendTestPrefix,
    };
  },
  template: APP_TEMPLATE,
}).mount("#app");
finishAppBoot();
}

