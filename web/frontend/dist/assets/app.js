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
    const actionKeyConfigSave = "config:save";
    const actionKeyUpdaterCheck = "updater:check";
    const actionKeyUpdaterApply = "updater:apply";
    const actionKeyUpdaterRestart = "updater:restart";
    const actionKeySourceCacheRefreshCurrentHour = "bridge:source_cache_refresh_current_hour";
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
    const currentHourRefreshButtonText = computed(() =>
      isSourceCacheRefreshCurrentHourLocked.value ? "下载中..." : "立即下载当前小时全部文件",
    );
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
    const showNetworkConfigTab = computed(() => false);
    const showCommonPathsConfigTab = computed(() => configRoleMode.value !== "internal");
    const showCommonSchedulerConfigTab = computed(() => configRoleMode.value !== "internal");
    const showNotifyConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeishuAuthConfigTab = computed(() => configRoleMode.value !== "internal");
    const showCommonAlarmDbConfigTab = computed(() => configRoleMode.value !== "internal");
    const showConsoleConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeatureMonthlyConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeatureHandoverConfigTab = computed(() => configRoleMode.value !== "internal");
    const showFeatureWetBulbCollectionConfigTab = computed(() => configRoleMode.value !== "internal");
    const showSheetImportConfigTab = computed(() => configRoleMode.value !== "internal");
    const showManualFeatureConfigTab = computed(() => configRoleMode.value !== "internal");
    const showRuntimeNetworkPanel = computed(() => false);
    const showDashboardPageNav = computed(() => deploymentRoleMode.value !== "internal");
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

    function closeStartupRoleSelector({ handled = false } = {}) {
      startupRoleSelectorVisible.value = false;
      startupRoleSelectorBusy.value = false;
      startupRoleSelectorMessage.value = "";
      startupRoleDecisionReady.value = true;
      if (handled) {
        startupRoleSelectorHandled.value = true;
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
    }

    function showStartupRoleLoading({ title = "", subtitle = "", stage = "" } = {}) {
      startupRoleSelectorVisible.value = false;
      startupRoleLoadingVisible.value = true;
      startupRoleLoadingTitle.value = String(title || "").trim();
      startupRoleLoadingSubtitle.value = String(subtitle || "").trim();
      startupRoleLoadingStage.value = String(stage || "").trim();
    }

    function hideStartupRoleLoading() {
      startupRoleLoadingVisible.value = false;
      startupRoleLoadingTitle.value = "";
      startupRoleLoadingSubtitle.value = "";
      startupRoleLoadingStage.value = "";
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

    async function activateStartupRuntimeAfterSelection(source) {
      const targetRole = normalizeDeploymentRoleMode(
        config.value?.deployment?.role_mode || startupRoleSelectorSelection.value || startupRoleCurrentMode.value,
      );
      showStartupRoleLoading({
        title: `正在加载${formatDeploymentRoleLabel(targetRole || "internal")}`,
        subtitle: "正在连接后台运行时，请稍候。",
        stage: "activating",
      });
      const activationResult = await activateStartupRuntime({ source });
      if (activationResult?.ok === false) {
        hideStartupRoleLoading();
        message.value = String(activationResult?.error || "").trim() || "后台运行时激活失败。";
        return false;
      }
      await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
      hideStartupRoleLoading();
      return true;
    }

    async function confirmStartupRoleSelection() {
      if (startupRoleSelectorBusy.value) return;
      const targetRole = normalizeDeploymentRoleMode(startupRoleSelectorSelection.value);
      const currentRole = startupRoleCurrentMode.value;
      startupRoleSelectorMessage.value = "";

      const draftValidationMessage = validateStartupBridgeDraft(targetRole, startupRoleBridgeDraft.value);
      if (draftValidationMessage) {
        startupRoleSelectorMessage.value = draftValidationMessage;
        return;
      }

      if (targetRole === currentRole && !startupRoleHasRelevantDraftChanges.value) {
        startupRoleSelectorBusy.value = true;
        showStartupRoleLoading({
          title: `正在启动${formatDeploymentRoleLabel(targetRole)}`,
          subtitle: "角色配置无需变更，正在进入对应页面。",
          stage: "activating",
        });
        const activated = await activateStartupRuntimeAfterSelection("startup_role_confirm");
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
      startupRoleSelectorBusy.value = true;
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
        const saveResult = await saveConfig();
        if (!saveResult?.saved) {
          config.value = previousConfig;
          showStartupRoleSelector(
            saveResult?.reason === "invalid"
              ? "当前配置校验失败，请检查启动角色参数或其余配置。"
              : String(saveResult?.error || "").trim() || "保存角色配置失败。",
          );
          return;
        }
        const shouldRestartForStartupConfirm =
          Boolean(saveResult?.restartRequired) && Boolean(health.runtime_activated);
        if (shouldRestartForStartupConfirm) {
          const isRoleSwitch = targetRole !== currentRole;
          hideStartupRoleLoading();
          const restartResult = await restartApplication({
            source: "startup_role_picker",
            reason: isRoleSwitch ? "role_mode_switch" : "startup_bridge_config_confirm",
            kicker: isRoleSwitch ? "角色切换中" : "桥接配置生效中",
            title: isRoleSwitch
              ? `正在切换到${formatDeploymentRoleLabel(targetRole)}`
              : `正在应用${formatDeploymentRoleLabel(targetRole)}桥接配置`,
            subtitle: isRoleSwitch
              ? "角色配置已保存，程序正在重启并切换运行角色。"
              : "桥接配置已保存，程序正在重启并应用新的运行参数。",
            reloadSubtitle: isRoleSwitch
              ? "服务已恢复，正在刷新当前页面并接入新的运行角色。"
              : "服务已恢复，正在刷新当前页面并接入新的桥接配置。",
            message: isRoleSwitch
              ? `已提交切换到${formatDeploymentRoleLabel(targetRole)}，正在等待服务恢复。`
              : `已提交${formatDeploymentRoleLabel(targetRole)}桥接配置更新，正在等待服务恢复。`,
          });
          if (restartResult?.ok === false) {
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
        const activated = await activateStartupRuntimeAfterSelection("startup_role_confirm_after_save");
        if (!activated) {
          showStartupRoleSelector("后台运行时激活失败。");
          return;
        }
        closeStartupRoleSelector({ handled: true });
        syncStartupRoleBridgeDraft();
        message.value =
          targetRole === currentRole
            ? `已确认${formatDeploymentRoleLabel(targetRole)}启动配置。`
            : `已切换到${formatDeploymentRoleLabel(targetRole)}。`;
      } catch (err) {
        config.value = previousConfig;
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
        overlayVisible: updaterUiOverlayVisible.value,
        startupRoleConfirmed: Boolean(health.startup_role_confirmed),
        runtimeActivated: Boolean(health.runtime_activated),
        roleSelectionRequired: Boolean(health.role_selection_required),
      }),
      (state) => {
        if (!state.bootstrapReady || !state.configLoaded) return;
        if (state.overlayVisible) return;
        const savedRole = normalizeDeploymentRoleMode(state.currentRole);
        const needsRoleSelection = Boolean(state.roleSelectionRequired) || !state.startupRoleConfirmed;
        if (needsRoleSelection) {
          startupRoleAutoActivationKey.value = "";
          selectStartupRole(savedRole || startupRoleSelectorSelection.value || "internal");
          syncStartupRoleBridgeDraft();
          hideStartupRoleLoading();
          showStartupRoleSelector("");
          return;
        }
        if (!savedRole) {
          startupRoleAutoActivationKey.value = "";
          hideStartupRoleLoading();
          showStartupRoleSelector("请先选择有效角色。");
          return;
        }
        startupRoleDecisionReady.value = true;
        startupRoleSelectorHandled.value = true;
        startupRoleSelectorVisible.value = false;
        const activationKey = `${state.currentStartupToken || ""}|${savedRole}`;
        if (state.runtimeActivated) {
          startupRoleAutoActivationKey.value = activationKey;
          hideStartupRoleLoading();
          startupRoleSelectorBusy.value = false;
          return;
        }
        if (startupRoleSelectorBusy.value || startupRoleLoadingVisible.value) return;
        if (startupRoleAutoActivationKey.value === activationKey) return;
        startupRoleAutoActivationKey.value = activationKey;
        startupRoleSelectorBusy.value = true;
        showStartupRoleLoading({
          title: `正在加载${formatDeploymentRoleLabel(savedRole)}`,
          subtitle: "正在应用已保存角色并连接后台运行时。",
          stage: "activating",
        });
        void (async () => {
          const activated = await activateStartupRuntimeAfterSelection("startup_role_resume");
          startupRoleSelectorBusy.value = false;
          if (!activated) {
            message.value = "已检测到已保存角色，但后台运行时启动失败。";
            hideStartupRoleLoading();
            return;
          }
          closeStartupRoleSelector({ handled: true });
        })();
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
      () => showNetworkConfigTab.value,
      (enabled) => {
        if (enabled) return;
        if (String(activeConfigTab.value || "").trim() === "common_network") {
          activeConfigTab.value = "common_deployment";
        }
      },
      { immediate: true },
    );

    watch(
      () => deploymentRoleMode.value,
      (roleMode) => {
        applyDashboardRoleMode(roleMode);
        const hiddenCommonTabs = roleMode === "internal"
          ? new Set(["common_paths", "common_console", "common_network", "common_scheduler", "common_notify", "common_feishu_auth", "common_alarm_db"])
          : new Set(["common_network"]);
        const hiddenFeatureTabs = new Set(["feature_alarm"]);
        if (roleMode === "internal") {
          hiddenFeatureTabs.add("feature_monthly");
          hiddenFeatureTabs.add("feature_handover");
          hiddenFeatureTabs.add("feature_wet_bulb_collection");
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
      schedulerDecisionText,
      schedulerTriggerText,
      wetBulbSchedulerDecisionText,
      wetBulbSchedulerTriggerText,
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
      dashboardActiveModuleHero,
      updaterMainButtonText,
      isUpdaterActionLocked,
      deploymentRoleMode,
      deploymentNodeIdDisplayText,
      deploymentNodeIdDisplayHint,
      showNetworkConfigTab,
      showCommonPathsConfigTab,
      showCommonSchedulerConfigTab,
      showNotifyConfigTab,
      showFeishuAuthConfigTab,
      showCommonAlarmDbConfigTab,
      showConsoleConfigTab,
      showFeatureMonthlyConfigTab,
      showFeatureHandoverConfigTab,
      showFeatureWetBulbCollectionConfigTab,
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
      isSourceCacheRefreshCurrentHourLocked,
      currentHourRefreshButtonText,
    };
  },
  template: APP_TEMPLATE,
}).mount("#app");
finishAppBoot();
}
