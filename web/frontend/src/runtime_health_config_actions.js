import {
  clone,
  convertLegacyConfigToV3,
  convertV3ConfigToLegacy,
  ensureConfigShape,
  isTransientNetworkError,
  normalizeSheetRules,
  todayText,
} from "./config_helpers.js";
import {
  cancelBridgeTaskApi,
  confirmAllHandoverReviewBatchApi,
  retryHandoverReviewBatchCloudSyncApi,
  applyUpdaterApi,
  activateStartupRuntimeApi,
  exitCurrentRuntimeApi,
  checkUpdaterApi,
  getUpdaterStatusApi,
  restartUpdaterApi,
  restartAppApi,
  triggerInternalPeerUpdaterApplyApi,
  triggerInternalPeerUpdaterCheckApi,
  getBridgeTaskApi,
  getBridgeTasksApi,
  getInternalRuntimeBuildingStatusApi,
  getInternalRuntimeStatusApi,
  buildHandoverDailyReportCaptureAssetUrl,
  getBootstrapHealthApi,
  getConfigApi,
  getExternalDashboardSummaryApi,
  getHandoverBuildingConfigSegmentApi,
  getHandoverCommonConfigSegmentApi,
  getHandoverDailyReportContextApi,
  getHandoverEngineerDirectoryApi,
  getHealthApi,
  getJobApi,
  getJobsApi,
  getRuntimeResourcesApi,
  reprobeHandoverReviewAccessApi,
  openHandoverDailyReportScreenshotAuthApi,
  recaptureHandoverDailyReportAssetApi,
  refreshCurrentHourSourceCacheApi,
  refreshBuildingLatestSourceCacheApi,
  restoreHandoverDailyReportManualAssetApi,
  retryBridgeTaskApi,
  rewriteHandoverDailyReportRecordApi,
  runHandoverDailyReportScreenshotTestApi,
  uploadHandoverDailyReportAssetApi,
  putConfigApi,
  repairDayMetricUploadConfigApi,
  putHandoverBuildingConfigSegmentApi,
  putHandoverCommonConfigSegmentApi,
  refreshManualAlarmSourceCacheApi,
  deleteManualAlarmSourceCacheFilesApi,
  uploadAlarmSourceCacheFullApi,
  uploadAlarmSourceCacheBuildingApi,
  openAlarmEventUploadTargetApi,
  startJsonJobApi,
} from "./api_client.js";
import { prepareConfigPayloadForSave } from "./config_save_validation.js";
import { buildUpdaterApplyMessage, mapUpdaterResultText } from "./updater_text.js";

const ACTION_KEY_SAVE_CONFIG = "config:save";
const ACTION_KEY_APP_RESTART = "app:restart";
const ACTION_KEY_UPDATER_CHECK = "updater:check";
const ACTION_KEY_UPDATER_APPLY = "updater:apply";
const ACTION_KEY_UPDATER_RESTART = "updater:restart";
const ACTION_KEY_UPDATER_INTERNAL_PEER_CHECK = "updater:internal_peer_check";
const ACTION_KEY_UPDATER_INTERNAL_PEER_APPLY = "updater:internal_peer_apply";
const ACTION_KEY_HANDOVER_CONFIRM_ALL = "handover_review:confirm_all";
const ACTION_KEY_HANDOVER_CLOUD_RETRY_ALL = "handover_review:cloud_retry_all";
const ACTION_KEY_HANDOVER_DAILY_REPORT_AUTH_OPEN = "handover_daily_report:auth_open";
const ACTION_KEY_HANDOVER_DAILY_REPORT_SCREENSHOT_TEST = "handover_daily_report:screenshot_test";
const ACTION_KEY_HANDOVER_DAILY_REPORT_RECAPTURE_PREFIX = "handover_daily_report:recapture:";
const ACTION_KEY_HANDOVER_DAILY_REPORT_UPLOAD_PREFIX = "handover_daily_report:upload:";
const ACTION_KEY_HANDOVER_DAILY_REPORT_RESTORE_PREFIX = "handover_daily_report:restore:";
const ACTION_KEY_HANDOVER_DAILY_REPORT_RECORD_REWRITE = "handover_daily_report:record_rewrite";
const ACTION_KEY_HANDOVER_REVIEW_ACCESS_REPROBE = "handover_review:access_reprobe";
const ACTION_KEY_HANDOVER_REVIEW_BASE_URL_SAVE = "handover_review:base_url_save";
const ACTION_KEY_BRIDGE_CANCEL_PREFIX = "bridge:cancel:";
const ACTION_KEY_BRIDGE_RETRY_PREFIX = "bridge:retry:";
const ACTION_KEY_SOURCE_CACHE_REFRESH_CURRENT_HOUR = "bridge:source_cache_refresh_current_hour";
const ACTION_KEY_SOURCE_CACHE_REFRESH_BUILDING_LATEST_PREFIX = "bridge:source_cache_refresh_building_latest:";
const ACTION_KEY_SOURCE_CACHE_REFRESH_ALARM_MANUAL = "bridge:source_cache_refresh_alarm_manual";
const ACTION_KEY_SOURCE_CACHE_DELETE_ALARM_MANUAL = "bridge:source_cache_delete_alarm_manual";
const ACTION_KEY_SOURCE_CACHE_UPLOAD_ALARM_FULL = "bridge:source_cache_upload_alarm_full";
const ACTION_KEY_SOURCE_CACHE_UPLOAD_ALARM_BUILDING = "bridge:source_cache_upload_alarm_building";
const ACTION_KEY_HANDOVER_CONFIG_COMMON_SAVE = "handover_config:common_save";
const ACTION_KEY_HANDOVER_CONFIG_BUILDING_SAVE = "handover_config:building_save";
const ACTION_KEY_DAY_METRIC_CONFIG_REPAIR = "day_metric_upload:config_repair";
const ACTION_KEY_HANDOVER_REVIEW_LINK_SEND_PREFIX = "handover_review:link_send:";
const HANDOVER_REVIEW_STATUS_BROADCAST_KEY = "handover_review_status_broadcast_v1";
const SOURCE_CACHE_FAMILY_LABELS = {
  handover_log_family: "交接班日志源文件",
  handover_capacity_report_family: "交接班容量报表源文件",
  monthly_report_family: "全景平台月报源文件",
  alarm_event_family: "告警信息源文件",
};
const ENGINEER_DIRECTORY_CACHE_KEY = "handover_engineer_directory_daily_cache_v1";

export function createRuntimeHealthConfigActions(ctx) {
  const {
    health,
    config,
    logs,
    message,
    currentJob,
    jobsList,
    selectedJobId,
    bridgeTasks,
    bridgeTasksDisplay,
    selectedBridgeTaskId,
    bridgeTaskDetail,
    resourceSnapshot,
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
    handoverConfigBuilding,
    handoverRuleScope,
    handoverConfigCommonRevision,
    handoverConfigCommonUpdatedAt,
    handoverConfigBuildingRevision,
    handoverConfigBuildingUpdatedAt,
    handoverDutyDate,
    handoverDutyShift,
    currentView,
    configSaveSuspendDepth,
    configSaveStatus,
    timers,
    streamController,
    runSingleFlight,
    bootstrapReady,
    fullHealthLoaded,
    configLoaded,
    healthLoadError,
    configLoadError,
    internalRuntimeSummary,
    internalBuildingRuntimeStatusMap,
    runtimeWarmupReady,
    engineerDirectoryLoaded,
    updaterUiOverlayVisible,
    updaterUiOverlayTitle,
    updaterUiOverlaySubtitle,
    updaterUiOverlayStage,
    updaterUiOverlayKicker,
    updaterAwaitingRestartRecovery,
    markRestartRecoveryIntent,
    clearRestartRecoveryIntent,
    readUpdaterRecoveryIntent,
    writeUpdaterRecoveryIntent,
    clearUpdaterRecoveryIntent,
    nextTick,
    scheduleExternalDashboardRefresh,
    shouldIncludeHandoverHealthContext,
    shouldFetchHandoverDailyReportContext,
    shouldLoadEngineerDirectory,
  } = ctx;
  let lastSavedConfigSignature = "";
  let serverConfigSnapshot = null;
  let engineerDirectoryPrefetchTimer = null;
  let configRequestInFlight = null;
  let externalDashboardSummaryRequestInFlight = null;
  const healthRequestInFlight = {
    lite: null,
    full: null,
  };
  let dailyReportContextRequestInFlight = null;
  let bridgeTasksRequestInFlight = null;
  let bridgeTaskDetailRequestInFlight = null;
  let lastBridgeTasksFetchAt = 0;
  let handoverCommonSegmentRequestSeq = 0;
  let handoverBuildingSegmentRequestSeq = 0;
  let bootstrapRetryTimer = null;
  let updaterReconnectTimer = null;
  let updaterQueueMonitorTimer = null;
  let updaterHealthHydratedOnce = false;
  let handoverReviewStatusBroadcastBound = false;
  let internalRuntimeSummaryRequestInFlight = null;
  let internalRuntimeSummaryRefetchQueued = false;
  let runtimeRoleConflictRecoveryInFlight = null;
  let runtimeRoleConflictReloadAt = 0;
  const internalRuntimeBuildingRequestsInFlight = new Map();
  const internalRuntimeBuildingRefetchQueued = new Map();
  let internalRuntimeRefreshDebounceTimer = null;
  let internalRuntimeRefreshAllPending = false;
  const internalRuntimeRefreshBuildingsPending = new Set();
  const BRIDGE_TASKS_FETCH_COOLDOWN_MS = 1200;
  const INTERNAL_RUNTIME_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"];

  function shouldLoadConfigNow() {
    return String(currentView?.value || "").trim().toLowerCase() === "config";
  }

  function resolveCurrentRoleMode() {
    return String(health?.deployment?.role_mode || config?.value?.deployment?.role_mode || "").trim().toLowerCase();
  }

  function canUseInternalRuntimeStatus() {
    return resolveCurrentRoleMode() === "internal" && Boolean(runtimeWarmupReady?.value);
  }

  function normalizeInternalRuntimeBuildingName(building) {
    const raw = String(building || "").trim().toUpperCase();
    if (INTERNAL_RUNTIME_BUILDINGS.includes(raw)) return raw;
    const compact = raw.replace("楼", "");
    if (["A", "B", "C", "D", "E"].includes(compact)) {
      return `${compact}楼`;
    }
    return "A楼";
  }

  function normalizeInternalRuntimeBuildingCode(building) {
    return normalizeInternalRuntimeBuildingName(building).replace("楼", "").toLowerCase();
  }

  function resetInternalBuildingRuntimeStatusMap() {
    if (!internalBuildingRuntimeStatusMap?.value || typeof internalBuildingRuntimeStatusMap.value !== "object") {
      return;
    }
    internalBuildingRuntimeStatusMap.value = Object.fromEntries(
      INTERNAL_RUNTIME_BUILDINGS.map((building) => [
        building,
        {
          updated_at: "",
          building,
          building_code: normalizeInternalRuntimeBuildingCode(building),
          page_slot: { building },
          source_families: {},
          pool: { browser_ready: false, last_error: "" },
        },
      ]),
    );
  }

  function scheduleInternalRuntimeStatusRefresh(options = {}) {
    if (!canUseInternalRuntimeStatus()) return;
    const buildingText = String(options?.building || "").trim();
    if (buildingText) {
      internalRuntimeRefreshBuildingsPending.add(normalizeInternalRuntimeBuildingName(buildingText));
    } else {
      internalRuntimeRefreshAllPending = true;
    }
    if (internalRuntimeRefreshDebounceTimer) {
      window.clearTimeout(internalRuntimeRefreshDebounceTimer);
    }
    const delayMs = Math.max(0, Number.parseInt(String(options?.delayMs || 250), 10) || 250);
    internalRuntimeRefreshDebounceTimer = window.setTimeout(() => {
      const refreshAll = internalRuntimeRefreshAllPending || internalRuntimeRefreshBuildingsPending.size <= 0;
      const buildingTargets = refreshAll ? [] : Array.from(internalRuntimeRefreshBuildingsPending);
      internalRuntimeRefreshDebounceTimer = null;
      internalRuntimeRefreshAllPending = false;
      internalRuntimeRefreshBuildingsPending.clear();
      if (!canUseInternalRuntimeStatus()) return;
      void fetchInternalRuntimeSummary({ silentMessage: true, force: true });
      if (refreshAll) {
        void fetchAllInternalBuildingRuntimeStatuses({ silentMessage: true, force: true });
        return;
      }
      buildingTargets.forEach((building) => {
        void fetchInternalRuntimeBuildingRuntimeStatus(building, { silentMessage: true, force: true });
      });
    }, delayMs);
  }

  function triggerInternalRuntimeStatusRefreshFromLogLine(line) {
    const text = String(line || "").trim();
    if (!text || !canUseInternalRuntimeStatus()) return;
    const normalized = text.toLowerCase();
    if (
      !normalized.includes("[共享桥接]")
      && !normalized.includes("[共享缓存]")
      && !normalized.includes("浏览器池")
      && !normalized.includes("楼栋浏览器")
      && !normalized.includes("共享文件")
      && !normalized.includes("源文件")
      && !normalized.includes("内网下载")
      && !normalized.includes("补采")
      && !normalized.includes("页池")
    ) {
      return;
    }
    const matchedBuilding = INTERNAL_RUNTIME_BUILDINGS.find((building) => text.includes(building)) || "";
    scheduleInternalRuntimeStatusRefresh({
      building: matchedBuilding,
      delayMs: matchedBuilding ? 180 : 300,
    });
  }

  function isUpdaterTrafficPaused() {
    return Boolean(updaterUiOverlayVisible?.value || updaterAwaitingRestartRecovery?.value);
  }

  function isLocallyExitedToRoleSelection() {
    return Boolean(
      health?.startup_role_user_exited
      && health?.role_selection_required
      && !health?.runtime_activated,
    );
  }

  function isRuntimeApiReady() {
    return Boolean(
      bootstrapReady?.value
      && health?.runtime_activated
      && health?.startup_role_confirmed,
    );
  }

  function refreshRoleScopedRuntimeStatus(reason = "runtime_action", options = {}) {
    const buildingText = String(options?.building || "").trim();
    if (resolveCurrentRoleMode() === "internal") {
      scheduleInternalRuntimeStatusRefresh({
        building: buildingText,
        delayMs: Math.max(0, Number.parseInt(String(options?.delayMs || 120), 10) || 120),
      });
      return;
    }
    if (typeof scheduleExternalDashboardRefresh === "function") {
      scheduleExternalDashboardRefresh(reason, {
        includePendingResume: Boolean(options?.includePendingResume),
      });
      return;
    }
    if (typeof fetchExternalDashboardSummary === "function") {
      void fetchExternalDashboardSummary({ silentMessage: true });
    }
  }

  function isRoleSelectionConflictError(err) {
    if (Number.parseInt(String(err?.httpStatus || 0), 10) !== 409) return false;
    const text = String(
      err?.responseText
      || err?.message
      || err
      || "",
    ).trim();
    return text.includes("请先在角色选择页进入系统");
  }

  async function tryRecoverFromRoleSelectionConflict() {
    if (typeof window === "undefined") return false;
    if (isLocallyExitedToRoleSelection()) return false;
    const now = Date.now();
    if (runtimeRoleConflictRecoveryInFlight) {
      return runtimeRoleConflictRecoveryInFlight;
    }
    if (runtimeRoleConflictReloadAt && now - runtimeRoleConflictReloadAt < 3000) {
      return true;
    }
    runtimeRoleConflictRecoveryInFlight = (async () => {
      await fetchBootstrapHealth({ silentMessage: true });
      const roleMode = resolveCurrentRoleMode();
      const roleReady = roleMode === "internal" || roleMode === "external";
      const restorable = Boolean(
        health?.runtime_activated
        || health?.startup_role_confirmed
        || !health?.role_selection_required,
      );
      if (
        !bootstrapReady?.value
        || !roleReady
        || !restorable
        || Boolean(health?.startup_role_user_exited)
      ) {
        return false;
      }
      runtimeRoleConflictReloadAt = Date.now();
      if (healthLoadError) {
        healthLoadError.value = "";
      }
      if (configLoadError) {
        configLoadError.value = "";
      }
      window.setTimeout(() => {
        window.location.reload();
      }, 120);
      return true;
    })().finally(() => {
      runtimeRoleConflictRecoveryInFlight = null;
    });
    return runtimeRoleConflictRecoveryInFlight;
  }

  function clearBootstrapRetryTimer() {
    if (bootstrapRetryTimer) {
      window.clearTimeout(bootstrapRetryTimer);
      bootstrapRetryTimer = null;
    }
  }

  function clearConfigRetryTimer() {
    if (timers?.configRetryTimer) {
      window.clearTimeout(timers.configRetryTimer);
      timers.configRetryTimer = null;
    }
  }

  function scheduleConfigRetry() {
    if (configLoaded?.value) return;
    if (!shouldLoadConfigNow()) return;
    clearConfigRetryTimer();
    timers.configRetryTimer = window.setTimeout(() => {
      timers.configRetryTimer = null;
      if (!shouldLoadConfigNow()) return;
      void fetchConfig({ silentMessage: true });
    }, 1000);
  }

  function scheduleBootstrapHealthRetry() {
    if (bootstrapReady?.value) return;
    clearBootstrapRetryTimer();
    bootstrapRetryTimer = window.setTimeout(() => {
      bootstrapRetryTimer = null;
      void fetchBootstrapHealth({ silentMessage: true });
    }, 1000);
  }

  function clearUpdaterReconnectTimer() {
    if (updaterReconnectTimer) {
      window.clearTimeout(updaterReconnectTimer);
      updaterReconnectTimer = null;
    }
  }

  function clearUpdaterQueueMonitorTimer() {
    if (updaterQueueMonitorTimer) {
      window.clearTimeout(updaterQueueMonitorTimer);
      updaterQueueMonitorTimer = null;
    }
  }

  function normalizeUpdaterLastResult(runtime) {
    return String(runtime?.last_result || "").trim().toLowerCase();
  }

  function normalizeUpdaterDependencyStatus(runtime) {
    return String(runtime?.dependency_sync_status || "").trim().toLowerCase();
  }

  function isUpdaterApplyingRuntime(runtime) {
    const lastResult = normalizeUpdaterLastResult(runtime);
    const dependencyStatus = normalizeUpdaterDependencyStatus(runtime);
    return (
      lastResult === "downloading_patch"
      || lastResult === "applying_patch"
      || lastResult === "dependency_checking"
      || lastResult === "dependency_syncing"
      || lastResult === "dependency_rollback"
      || dependencyStatus === "running"
    );
  }

  function isSharedMirrorUpdater(runtime) {
    return String(runtime?.source_kind || "").trim().toLowerCase() === "shared_mirror";
  }

  function buildAutomaticUpdaterOverlayPayload(runtime, options = {}) {
    const queued = Boolean(options?.queued);
    if (queued) {
      return {
        title: "等待任务结束后自动更新",
        subtitle: "后台任务尚未完成。控制台已暂停轮询和日志流，任务结束后会自动开始更新。",
        stage: "queued",
      };
    }
    if (isSharedMirrorUpdater(runtime)) {
      return {
        title: "已检测到外网新版本",
        subtitle: "内网端正在自动更新，请保持当前页面打开，完成后会自动恢复。",
        stage: "applying",
        kicker: "自动跟随更新",
      };
    }
    return {
      title: "正在更新程序",
      subtitle: "检测到新版本，正在自动应用补丁，请保持当前页面打开。",
      stage: "applying",
    };
  }

  function normalizeUpdaterRecoveryStage(value) {
    const stage = String(value || "").trim().toLowerCase();
    if (["queued", "applying", "restarting", "reloading"].includes(stage)) {
      return stage;
    }
    return "applying";
  }

  function resolveUpdaterRecoveryRoleMode(explicitRoleMode = "") {
    return String(explicitRoleMode || resolveCurrentRoleMode() || "").trim().toLowerCase();
  }

  function buildUpdaterRecoveryIntent(stage, source = "", options = {}) {
    return {
      role_mode: resolveUpdaterRecoveryRoleMode(options?.targetRoleMode),
      path: typeof window !== "undefined" ? window.location.pathname : "/",
      requested_at: Date.now(),
      stage: normalizeUpdaterRecoveryStage(stage),
      source: String(source || options?.source || "").trim().toLowerCase(),
      startup_token: String(options?.startupToken || health?.startup_time || "").trim(),
    };
  }

  function persistUpdaterRecoveryIntent(stage, source = "", options = {}) {
    if (typeof writeUpdaterRecoveryIntent !== "function") return;
    writeUpdaterRecoveryIntent(buildUpdaterRecoveryIntent(stage, source, options));
  }

  function clearPersistedUpdaterRecoveryIntent() {
    if (typeof clearUpdaterRecoveryIntent === "function") {
      clearUpdaterRecoveryIntent();
    }
  }

  async function flushUpdaterOverlayPaint() {
    if (typeof nextTick === "function") {
      await nextTick();
    }
    if (typeof window === "undefined" || typeof window.requestAnimationFrame !== "function") {
      return;
    }
    await new Promise((resolve) => {
      window.requestAnimationFrame(() => {
        window.requestAnimationFrame(() => resolve());
      });
    });
  }

  function startUpdaterBootstrapRecoveryWatch(options = {}) {
    clearUpdaterReconnectTimer();
    const previousStartupToken = String(options?.startupToken || health?.startup_time || "").trim();
    const source = String(options?.source || "updater_apply").trim().toLowerCase();
    const kicker = String(options?.kicker || "").trim();
    const reloadTitle = String(options?.reloadTitle || "服务已恢复");
    const reloadSubtitle = String(options?.reloadSubtitle || "正在刷新当前页面并接入新版本。");
    const targetRoleMode = String(options?.targetRoleMode || "").trim().toLowerCase();
    const poll = async () => {
      const ok = await fetchBootstrapHealth({ silentMessage: true });
      if (ok) {
        const nextStartupToken = String(health?.startup_time || "").trim();
        if (previousStartupToken && nextStartupToken && nextStartupToken === previousStartupToken) {
          updaterReconnectTimer = window.setTimeout(poll, 2500);
          return;
        }
        if (typeof markRestartRecoveryIntent === "function") {
          markRestartRecoveryIntent(targetRoleMode);
        }
        persistUpdaterRecoveryIntent("reloading", source, {
          targetRoleMode,
          source,
        });
        if (updaterAwaitingRestartRecovery) updaterAwaitingRestartRecovery.value = true;
        setUpdaterOverlay(true, {
          title: reloadTitle,
          subtitle: reloadSubtitle,
          stage: "reloading",
          kicker,
        });
        window.setTimeout(() => {
          window.location.reload();
        }, 350);
        return;
      }
      updaterReconnectTimer = window.setTimeout(poll, 2500);
    };
    updaterReconnectTimer = window.setTimeout(poll, 2500);
  }

  function startUpdaterRuntimeMonitor(options = {}) {
    clearUpdaterQueueMonitorTimer();
    pauseRuntimeTraffic();
    setUpdaterOverlay(true, buildAutomaticUpdaterOverlayPayload(health.updater || {}, options));
    const startedAt = Date.now();
    const initialGraceMs = Math.max(0, Number.parseInt(String(options?.initialGraceMs || 0), 10) || 0);
    let observedActive = false;

    const poll = async () => {
      try {
        const data = await getUpdaterStatusApi();
        const runtime = data?.runtime && typeof data.runtime === "object" ? data.runtime : {};
        Object.assign(health.updater, runtime);
        const lastResult = normalizeUpdaterLastResult(runtime);
        const queued = Boolean(runtime?.queued_apply?.queued);
        const applying = isUpdaterApplyingRuntime(runtime);
        const requestPending = typeof options?.isRequestPending === "function"
          ? Boolean(options.isRequestPending())
          : false;

        if (queued || runtime?.running || applying) {
          observedActive = true;
        }

        if (lastResult === "failed") {
          hideUpdaterOverlay();
          message.value = `应用更新失败: ${String(runtime?.last_error || "请查看系统日志").trim() || "请查看系统日志"}`;
          return;
        }
        if (lastResult === "updated_restart_scheduled" || runtime?.restart_required) {
          beginUpdaterRestartRecovery();
          return;
        }
        if (applying) {
          setUpdaterOverlay(true, buildAutomaticUpdaterOverlayPayload(runtime, options));
        }
        if (!queued && !runtime?.running && !applying) {
          if (requestPending || (!observedActive && Date.now() - startedAt < initialGraceMs)) {
            updaterQueueMonitorTimer = window.setTimeout(poll, 1500);
            return;
          }
          hideUpdaterOverlay();
          return;
        }
        updaterQueueMonitorTimer = window.setTimeout(poll, 3000);
      } catch (_err) {
        beginUpdaterRestartRecovery();
      }
    };

    updaterQueueMonitorTimer = window.setTimeout(poll, 3000);
  }

  function handleUpdaterRuntimeSideEffects(previousRuntime, nextRuntime) {
    const previous = previousRuntime && typeof previousRuntime === "object" ? previousRuntime : {};
    const next = nextRuntime && typeof nextRuntime === "object" ? nextRuntime : {};
    const nextEnabled = next?.enabled !== false;
    const nextDisabledReason = String(next?.disabled_reason || "").trim().toLowerCase();
    if (!nextEnabled && nextDisabledReason === "source_python_run") {
      return;
    }

    const nextSourceKind = String(next?.source_kind || "").trim().toLowerCase();
    const nextLastPublishAt = String(next?.last_publish_at || "").trim();
    const nextMirrorVersion = String(next?.mirror_version || "").trim();
    const nextLastPublishError = String(next?.last_publish_error || "").trim();
    const prevPublishMarker = `${String(previous?.last_publish_at || "").trim()}|${String(previous?.mirror_version || "").trim()}|${String(previous?.last_publish_error || "").trim()}`;
    const nextPublishMarker = `${nextLastPublishAt}|${nextMirrorVersion}|${nextLastPublishError}`;

    if (
      updaterHealthHydratedOnce
      && nextSourceKind === "remote"
      && nextPublishMarker !== prevPublishMarker
      && nextLastPublishAt
      && !nextLastPublishError
    ) {
      message.value = `已将批准版本发布到共享目录（${nextMirrorVersion || "最新版本"}），内网端会自动跟随更新。`;
    }

    const nextLastResult = normalizeUpdaterLastResult(next);
    if (nextSourceKind === "shared_mirror" && isUpdaterApplyingRuntime(next)) {
      if (!updaterUiOverlayVisible?.value && !updaterAwaitingRestartRecovery?.value) {
        startUpdaterRuntimeMonitor({ queued: false });
        return;
      }
    }
    if (nextSourceKind === "shared_mirror" && (nextLastResult === "updated_restart_scheduled" || next?.restart_required)) {
      if (!updaterAwaitingRestartRecovery?.value) {
        beginUpdaterRestartRecovery({
          title: "内网更新完成，正在自动重启",
          subtitle: "已完成补丁应用，服务恢复后会自动刷新当前页面。",
          kicker: "自动跟随更新",
        });
      }
    }
  }

  function pauseRuntimeTraffic() {
    streamController?.pauseAll?.();
  }

  function resumeRuntimeTraffic() {
    streamController?.resumeAll?.();
  }

  function setUpdaterOverlay(visible, { title = "", subtitle = "", stage = "", kicker = "" } = {}) {
    if (updaterUiOverlayVisible) updaterUiOverlayVisible.value = Boolean(visible);
    if (updaterUiOverlayTitle) updaterUiOverlayTitle.value = String(title || "");
    if (updaterUiOverlaySubtitle) updaterUiOverlaySubtitle.value = String(subtitle || "");
    if (updaterUiOverlayStage) updaterUiOverlayStage.value = String(stage || "");
    if (updaterUiOverlayKicker) updaterUiOverlayKicker.value = String(kicker || "");
  }

  function hideUpdaterOverlay(options = {}) {
    clearUpdaterReconnectTimer();
    clearUpdaterQueueMonitorTimer();
    if (updaterAwaitingRestartRecovery) updaterAwaitingRestartRecovery.value = false;
    if (options?.clearRecoveryIntent !== false) {
      clearPersistedUpdaterRecoveryIntent();
    }
    setUpdaterOverlay(false, { title: "", subtitle: "", stage: "", kicker: "" });
    resumeRuntimeTraffic();
  }

  function beginUpdaterRestartRecovery(options = {}) {
    clearUpdaterReconnectTimer();
    clearUpdaterQueueMonitorTimer();
    pauseRuntimeTraffic();
    const recoveryIntent = typeof readUpdaterRecoveryIntent === "function" ? readUpdaterRecoveryIntent() : null;
    const startupToken = String(
      options?.startupToken
      || recoveryIntent?.startup_token
      || health?.startup_time
      || "",
    ).trim();
    if (typeof markRestartRecoveryIntent === "function") {
      markRestartRecoveryIntent(String(options?.targetRoleMode || "").trim().toLowerCase());
    }
    persistUpdaterRecoveryIntent("restarting", options?.source || "updater_apply", {
      ...options,
      startupToken,
    });
    if (updaterAwaitingRestartRecovery) updaterAwaitingRestartRecovery.value = true;
    setUpdaterOverlay(true, {
      title: String(options?.title || "更新完成，正在重启服务"),
      subtitle: String(options?.subtitle || "请保持当前页面打开。服务恢复后会自动刷新当前页面。"),
      stage: String(options?.stage || "restarting"),
      kicker: String(options?.kicker || ""),
    });
    startUpdaterBootstrapRecoveryWatch({
      ...options,
      source: options?.source || "updater_apply",
      startupToken,
      targetRoleMode: String(options?.targetRoleMode || "").trim().toLowerCase(),
      kicker: String(options?.kicker || ""),
      reloadTitle: String(options?.reloadTitle || "服务已恢复"),
      reloadSubtitle: String(options?.reloadSubtitle || "正在刷新当前页面并接入新版本。"),
    });
  }

  function handoffUpdaterToRestartRecovery(
    messageText = "更新请求已提交，正在等待服务恢复。",
    options = {},
  ) {
    if (health?.updater && typeof health.updater === "object") {
      health.updater.last_result = "updated_restart_scheduled";
    }
    message.value = String(messageText || "更新请求已提交，正在等待服务恢复。");
    beginUpdaterRestartRecovery(options);
    return {
      ok: true,
      accepted: true,
      recovering: true,
      reason: "restart_recovery",
    };
  }

  function startQueuedUpdaterMonitor() {
    persistUpdaterRecoveryIntent("queued", "updater_apply");
    startUpdaterRuntimeMonitor({ queued: true });
  }

  async function resumeUpdaterRecoveryIfNeeded() {
    const intent = typeof readUpdaterRecoveryIntent === "function" ? readUpdaterRecoveryIntent() : null;
    if (!intent) return false;
    const stage = normalizeUpdaterRecoveryStage(intent.stage);
    const source = String(intent.source || "").trim().toLowerCase();
    if (stage === "restarting" || stage === "reloading") {
      pauseRuntimeTraffic();
      if (updaterAwaitingRestartRecovery) updaterAwaitingRestartRecovery.value = true;
      setUpdaterOverlay(true, {
        title: "更新完成，正在恢复服务",
        subtitle: "检测到程序仍在恢复，服务可用后会自动继续进入当前页面。",
        stage: "restarting",
        kicker: source === "updater_restart" ? "程序重启恢复中" : "程序更新恢复中",
      });
      startUpdaterBootstrapRecoveryWatch({
        source,
        startupToken: String(intent.startup_token || "").trim(),
        targetRoleMode: String(intent.role_mode || "").trim().toLowerCase(),
        kicker: source === "updater_restart" ? "程序重启恢复中" : "程序更新恢复中",
        reloadTitle: "服务已恢复",
        reloadSubtitle: "正在刷新当前页面并恢复当前系统。",
      });
      return true;
    }
    startUpdaterBootstrapRecoveryWatch({
      source,
      startupToken: String(intent.startup_token || "").trim(),
      targetRoleMode: String(intent.role_mode || "").trim().toLowerCase(),
    });
    startUpdaterRuntimeMonitor({ queued: stage === "queued" });
    return true;
  }

  function mergeConfigWithServerSnapshot(baseConfig, changedConfig) {
    if (!baseConfig || typeof baseConfig !== "object") {
      return clone(changedConfig || {});
    }
    if (!changedConfig || typeof changedConfig !== "object") {
      return clone(baseConfig);
    }
    if (Array.isArray(baseConfig) || Array.isArray(changedConfig)) {
      return clone(changedConfig);
    }
    const output = clone(baseConfig);
    Object.keys(changedConfig).forEach((key) => {
      const nextValue = changedConfig[key];
      const baseValue = baseConfig[key];
      if (
        baseValue
        && typeof baseValue === "object"
        && !Array.isArray(baseValue)
        && nextValue
        && typeof nextValue === "object"
        && !Array.isArray(nextValue)
      ) {
        output[key] = mergeConfigWithServerSnapshot(baseValue, nextValue);
        return;
      }
      output[key] = clone(nextValue);
    });
    return output;
  }

  function buildEngineerDirectoryCacheSignature() {
    try {
      return JSON.stringify(config.value?.handover_log?.shift_roster?.engineer_directory || {});
    } catch (_) {
      return "";
    }
  }

  function readEngineerDirectoryCache(signature) {
    if (typeof window === "undefined" || !window.localStorage) return null;
    try {
      const raw = window.localStorage.getItem(ENGINEER_DIRECTORY_CACHE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return null;
      if (String(parsed.date || "").trim() !== todayText()) return null;
      if (String(parsed.signature || "").trim() !== String(signature || "").trim()) return null;
      return Array.isArray(parsed.rows) ? parsed.rows : null;
    } catch (_) {
      return null;
    }
  }

  function writeEngineerDirectoryCache(signature, rows) {
    if (typeof window === "undefined" || !window.localStorage) return;
    try {
      window.localStorage.setItem(
        ENGINEER_DIRECTORY_CACHE_KEY,
        JSON.stringify({
          date: todayText(),
          signature: String(signature || "").trim(),
          rows: Array.isArray(rows) ? rows : [],
        }),
      );
    } catch (_) {
      // ignore storage errors
    }
  }

  function clearEngineerDirectoryCache() {
    if (typeof window === "undefined" || !window.localStorage) return;
    try {
      window.localStorage.removeItem(ENGINEER_DIRECTORY_CACHE_KEY);
    } catch (_) {
      // ignore storage errors
    }
  }

  function normalizeLogEntry(payload) {
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
      const line = String(payload.line || "").trim();
      if (!line) return null;
      return {
        id: Number.parseInt(String(payload.id || 0), 10) || 0,
        line,
        level: String(payload.level || "info").trim().toLowerCase() || "info",
        source: String(payload.source || "system").trim().toLowerCase() || "system",
      };
    }
    const line = String(payload || "").trim();
    if (!line) return null;
    return { id: 0, line, level: "info", source: "system" };
  }

  function emptyDailyReportScreenshotTestState(batchKey = "") {
    return {
      batch_key: String(batchKey || "").trim(),
      status: "",
      tested_at: "",
      summary_sheet_image: { status: "", error: "", path: "" },
      external_page_image: { status: "", error: "", path: "" },
    };
  }

  function isAbortError(err) {
    const text = String(err || "").trim().toLowerCase();
    return err?.name === "AbortError" || text.includes("abort");
  }

  function isIncompleteJobStatus(status) {
    const normalized = String(status || "").trim().toLowerCase();
    return normalized === "queued" || normalized === "running" || normalized === "waiting_resource";
  }

  async function focusAcceptedJob(data, submitMessage = "") {
    const job = data?.job && typeof data.job === "object" ? { ...data.job } : data && typeof data === "object" ? { ...data } : null;
    const jobId = String(job?.job_id || "").trim();
    if (!jobId) {
      if (submitMessage) message.value = submitMessage;
      return "";
    }
    if (selectedJobId) {
      selectedJobId.value = jobId;
    }
    if (currentJob) {
      currentJob.value = { ...(currentJob.value || {}), ...job };
    }
    if (streamController?.attachJobStream) {
      streamController.attachJobStream(jobId);
    }
    if (data?.job_panel_summary && typeof data.job_panel_summary === "object") {
      applyJobPanelSummary(data.job_panel_summary);
    } else {
      void fetchJobs({ silentMessage: true });
    }
    refreshRoleScopedRuntimeStatus("accepted_job", { includePendingResume: true });
    if (submitMessage) {
      message.value = submitMessage;
    }
    return jobId;
  }

  async function waitForAcceptedJobCompletion(jobId, options = {}) {
    const targetJobId = String(jobId || "").trim();
    if (!targetJobId) return null;
    const timeoutMs = Math.max(1000, Number.parseInt(String(options?.timeoutMs || 120000), 10) || 120000);
    const intervalMs = Math.max(500, Number.parseInt(String(options?.intervalMs || 1500), 10) || 1500);
    const startedAt = Date.now();
    while (Date.now() - startedAt <= timeoutMs) {
      try {
        const job = await getJobApi(targetJobId);
        if (!isIncompleteJobStatus(job?.status)) {
          return job;
        }
      } catch (_err) {
        // Ignore transient read failures; the next poll can recover.
      }
      await new Promise((resolve) => window.setTimeout(resolve, intervalMs));
    }
    return null;
  }

  function shouldPublishAcceptedJobResult(jobId) {
    const selected = String(selectedJobId?.value || "").trim();
    const current = String(currentJob?.value?.job_id || "").trim();
    const target = String(jobId || "").trim();
    return !!target && (selected === target || current === target);
  }

  function summarizeConfirmAllResult(data) {
    const batchStatus = data?.batch_status || {};
    const confirmedCount = Number.parseInt(String(batchStatus?.confirmed_count || 0), 10) || 0;
    const requiredCount = Number.parseInt(String(batchStatus?.required_count || 0), 10) || 0;
    const followupStatus = String(data?.followup_result?.status || "").trim();
    const cloudStatus = String(data?.followup_result?.cloud_sheet_sync?.status || "").trim().toLowerCase();
    const cloudSkippedReasons = Array.isArray(data?.followup_result?.cloud_sheet_sync?.skipped_buildings)
      ? data.followup_result.cloud_sheet_sync.skipped_buildings
          .map((item) => String(item?.reason || "").trim())
          .filter(Boolean)
      : [];
    if (followupStatus === "ok") {
      if (cloudStatus === "ok") {
        return "已一键全确认，并自动完成后续上传和云文档同步";
      }
      if (cloudStatus === "skipped") {
        const reasonText = cloudSkippedReasons.length
          ? `，云文档已跳过（${cloudSkippedReasons.join(" / ")}）`
          : "，但云文档已跳过";
        return `已一键全确认，并自动完成后续上传${reasonText}`;
      }
      if (cloudStatus === "failed" || cloudStatus === "partial_failed") {
        return "已一键全确认，后续上传成功，但云文档同步失败，请查看系统日志";
      }
      return "已一键全确认，并自动完成后续上传";
    }
    if (followupStatus === "partial_failed") {
      if (cloudStatus === "failed" || cloudStatus === "partial_failed") {
        return "已一键全确认，但后续上传或云文档同步存在部分失败，请查看系统日志";
      }
      return "已一键全确认，但后续上传存在部分失败，请查看系统日志";
    }
    if (followupStatus === "failed") {
      if (cloudStatus === "failed" || cloudStatus === "partial_failed") {
        return "已一键全确认，但后续上传和云文档同步失败，请查看系统日志";
      }
      return "已一键全确认，但后续上传失败，请查看系统日志";
    }
    if (cloudStatus === "blocked") {
      const blockedReason = String(data?.followup_result?.cloud_sheet_sync?.blocked_reason || "").trim();
      return blockedReason
        ? `已一键全确认（${confirmedCount}/${requiredCount}），云文档未执行：${blockedReason}`
        : `已一键全确认（${confirmedCount}/${requiredCount}）`;
    }
    return `已一键全确认（${confirmedCount}/${requiredCount}）`;
  }

  function summarizeBatchCloudRetryResult(data) {
    const cloudStatus = String(data?.cloud_sheet_sync?.status || data?.status || "").trim().toLowerCase();
    if (cloudStatus === "ok") return "失败楼栋已完成云表重试上传";
    if (cloudStatus === "partial_failed") return "云表批量重试已执行，但仍有部分楼栋失败";
    if (cloudStatus === "failed") return "云表批量重试失败，请查看系统日志";
    if (cloudStatus === "blocked") {
      return (
        String(data?.cloud_sheet_sync?.blocked_reason || "").trim()
        || "当前批次尚未全部确认，暂不能重试云表上传"
      );
    }
    return "当前没有需要重试的失败楼栋";
  }

  function resolveDailyReportRewriteMessage(data) {
    const error = String(data?.error || "").trim();
    if (error) return error;
    return data?.ok ? "日报多维记录已重写" : "日报多维记录重写失败";
  }

  function resolveDailyReportCaptureFailureMessage(label, result) {
    const errorMessage = String(result?.error_message || "").trim();
    if (errorMessage) return `${label}重新截图失败：${errorMessage}`;
    const error = String(result?.error || "").trim();
    if (error) return `${label}重新截图失败：${error}`;
    return `${label}重新截图失败，请查看系统错误日志`;
  }

  function getDailyReportTargetLabel(target) {
    const targetText = String(target || "").trim().toLowerCase();
    return targetText === "summary_sheet" ? "今日航图截图" : "排班截图";
  }

  function buildPreparedSavePayload() {
    const prepared = prepareConfigPayloadForSave({
      config: config.value,
      buildingsText: buildingsText.value,
      customAbsoluteStartLocal: customAbsoluteStartLocal.value,
      customAbsoluteEndLocal: customAbsoluteEndLocal.value,
      sheetRuleRows: sheetRuleRows.value,
    });
    if (!prepared.ok) {
      return { ok: false, error: prepared.error || "配置校验失败" };
    }
    const v3Payload = convertLegacyConfigToV3(prepared.payload);
    const signature = JSON.stringify(v3Payload || {});
    return { ok: true, v3Payload, signature };
  }

  function syncConfigSaveSignature(signature = "", { pending = true } = {}) {
    if (!configSaveStatus || typeof configSaveStatus !== "object") return;
    const normalizedSignature = String(signature || "");
    configSaveStatus.saved_signature = normalizedSignature;
    if (pending) configSaveStatus.draft_dirty = false;
  }

  function setLastSavedSignatureFromPreparedPayload() {
    const payloadState = buildPreparedSavePayload();
    if (!payloadState.ok) {
      lastSavedConfigSignature = "";
      syncConfigSaveSignature("", { pending: true });
      return;
    }
    lastSavedConfigSignature = payloadState.signature || "";
    syncConfigSaveSignature(lastSavedConfigSignature, { pending: true });
  }

  function withConfigSaveSuspended(applyFn) {
    if (!configSaveSuspendDepth) {
      applyFn();
      return;
    }
    configSaveSuspendDepth.value += 1;
    try {
      applyFn();
    } finally {
      window.setTimeout(() => {
        configSaveSuspendDepth.value = Math.max(0, configSaveSuspendDepth.value - 1);
      }, 0);
    }
  }

  function appendLog(payload) {
    const logLine = String(
      payload?.line
      || payload?.message
      || payload?.payload?.message
      || payload?.payload?.error
      || "",
    ).trim();
    if (logLine) {
      triggerInternalRuntimeStatusRefreshFromLogLine(logLine);
    }
    const entry = normalizeLogEntry(payload);
    if (!entry || !["warning", "error"].includes(entry.level)) return;
    if (entry.id > 0 && logs.value.some((item) => Number.parseInt(String(item?.id || 0), 10) === entry.id)) {
      return;
    }
    logs.value.push(entry);
    const maxSize = Number.parseInt(String(config.value?.web?.log_buffer_size || 5000), 10);
    const max = Number.isFinite(maxSize) && maxSize > 100 ? maxSize : 5000;
    if (logs.value.length > max) {
      logs.value.splice(0, logs.value.length - max);
    }
  }

  function applyHandoverReviewAccessSnapshot(snapshot) {
    if (!snapshot || typeof snapshot !== "object") return;
    health.handover.review_base_url = String(snapshot.review_base_url || "");
    health.handover.review_base_url_effective = String(snapshot.review_base_url_effective || "");
    health.handover.review_base_url_effective_source = String(snapshot.review_base_url_effective_source || "");
    health.handover.review_base_url_candidates = Array.isArray(snapshot.review_base_url_candidates)
      ? snapshot.review_base_url_candidates
      : [];
    health.handover.review_base_url_status = String(snapshot.review_base_url_status || "");
    health.handover.review_base_url_error = String(snapshot.review_base_url_error || "");
    health.handover.review_base_url_validated_candidates = Array.isArray(snapshot.review_base_url_validated_candidates)
      ? snapshot.review_base_url_validated_candidates
      : [];
    health.handover.review_base_url_candidate_results = Array.isArray(snapshot.review_base_url_candidate_results)
      ? snapshot.review_base_url_candidate_results
      : [];
    health.handover.review_base_url_manual_available = Boolean(snapshot.review_base_url_manual_available);
    health.handover.configured = Boolean(snapshot.configured);
    health.handover.review_base_url_configured_at = String(snapshot.review_base_url_configured_at || "");
    health.handover.review_base_url_last_probe_at = String(snapshot.review_base_url_last_probe_at || "");
    health.handover.review_links = Array.isArray(snapshot.review_links)
      ? snapshot.review_links
      : [];
  }

  function mergeLiteSharedBridgeFamily(currentFamily, nextFamily) {
    const current = currentFamily && typeof currentFamily === "object" ? currentFamily : {};
    const next = nextFamily && typeof nextFamily === "object" ? nextFamily : {};
    if (!Object.keys(next).length) {
      return current;
    }
    const merged = { ...current, ...next };
    const currentBuildings = Array.isArray(current.buildings) ? current.buildings : [];
    const nextBuildings = Array.isArray(next.buildings) ? next.buildings : [];
    if (!nextBuildings.length && currentBuildings.length) {
      merged.buildings = currentBuildings;
    }
    const objectKeys = ["display_overview", "latest_selection", "upload_status", "external_upload"];
    objectKeys.forEach((key) => {
      const nextValue = next[key];
      const currentValue = current[key];
      if (
        (!nextValue || typeof nextValue !== "object" || !Object.keys(nextValue).length)
        && currentValue
        && typeof currentValue === "object"
        && Object.keys(currentValue).length
      ) {
        merged[key] = currentValue;
      }
    });
    return merged;
  }

  function mergeLiteInternalSourceCache(currentPayload, nextPayload) {
    const current = currentPayload && typeof currentPayload === "object" ? currentPayload : {};
    const next = nextPayload && typeof nextPayload === "object" ? nextPayload : {};
    if (!Object.keys(next).length) {
      return current;
    }
    const merged = { ...current, ...next };
    if (
      (!next.display_overview || typeof next.display_overview !== "object" || !Object.keys(next.display_overview).length)
      && current.display_overview
      && typeof current.display_overview === "object"
      && Object.keys(current.display_overview).length
    ) {
      merged.display_overview = current.display_overview;
    }
    if (
      (!next.current_hour_refresh_overview
        || typeof next.current_hour_refresh_overview !== "object"
        || !Object.keys(next.current_hour_refresh_overview).length)
      && current.current_hour_refresh_overview
      && typeof current.current_hour_refresh_overview === "object"
      && Object.keys(current.current_hour_refresh_overview).length
    ) {
      merged.current_hour_refresh_overview = current.current_hour_refresh_overview;
    }
    ["handover_log_family", "handover_capacity_report_family", "monthly_report_family", "alarm_event_family"].forEach((key) => {
      merged[key] = mergeLiteSharedBridgeFamily(current[key], next[key]);
    });
    return merged;
  }

  function mergePresentedPayload(currentPayload, nextPayload, objectKeys = []) {
    const current = currentPayload && typeof currentPayload === "object" ? currentPayload : {};
    const next = nextPayload && typeof nextPayload === "object" ? nextPayload : {};
    if (!Object.keys(next).length) {
      return current;
    }
    const merged = { ...current, ...next };
    objectKeys.forEach((key) => {
      const nextValue = next[key];
      const currentValue = current[key];
      if (
        (!nextValue || typeof nextValue !== "object" || !Object.keys(nextValue).length)
        && currentValue
        && typeof currentValue === "object"
        && Object.keys(currentValue).length
      ) {
        merged[key] = currentValue;
      }
    });
    return merged;
  }

  function applyHealthSnapshot(data) {
    if (!data || typeof data !== "object") return;
    const healthMode = String(data?.health_mode || "").trim().toLowerCase();
    const isLiteHealth = healthMode === "lite";
    const shouldPreserveExitedSelectorState = Boolean(
      isLocallyExitedToRoleSelection()
      && !Boolean(data?.startup_role_user_exited)
      && !Boolean(data?.role_selection_required)
      && (data?.runtime_activated === true || data?.startup_role_confirmed === true),
    );
    if (shouldPreserveExitedSelectorState) {
      return;
    }
    health.version = String(data.version || "");
    health.startup_time = String(data.startup_time || health.startup_time || "");
    health.startup_role_confirmed = Boolean(
      typeof data.startup_role_confirmed === "boolean"
        ? data.startup_role_confirmed
        : health.startup_role_confirmed,
    );
    health.role_selection_required = Boolean(
      typeof data.role_selection_required === "boolean"
        ? data.role_selection_required
        : health.role_selection_required,
    );
    health.startup_role_user_exited = Boolean(
      typeof data.startup_role_user_exited === "boolean"
        ? data.startup_role_user_exited
        : health.startup_role_user_exited,
    );
    if (data.startup_handoff && typeof data.startup_handoff === "object") {
      Object.assign(health.startup_handoff, {
        active: Boolean(data.startup_handoff.active),
        mode: String(data.startup_handoff.mode || "").trim(),
        target_role_mode: String(data.startup_handoff.target_role_mode || "").trim().toLowerCase(),
        requested_at: String(data.startup_handoff.requested_at || "").trim(),
        reason: String(data.startup_handoff.reason || "").trim(),
        nonce: String(data.startup_handoff.nonce || "").trim(),
      });
    } else if (health.startup_handoff && typeof health.startup_handoff === "object") {
      Object.assign(health.startup_handoff, {
        active: false,
        mode: "",
        target_role_mode: "",
        requested_at: "",
        reason: "",
        nonce: "",
      });
    }
    if (data.startup_shared_bridge && typeof data.startup_shared_bridge === "object") {
      Object.assign(health.startup_shared_bridge, {
        enabled: Boolean(data.startup_shared_bridge.enabled),
        root_dir: String(data.startup_shared_bridge.root_dir || "").trim(),
        internal_root_dir: String(data.startup_shared_bridge.internal_root_dir || "").trim(),
        external_root_dir: String(data.startup_shared_bridge.external_root_dir || "").trim(),
        poll_interval_sec: data.startup_shared_bridge.poll_interval_sec,
        heartbeat_interval_sec: data.startup_shared_bridge.heartbeat_interval_sec,
        claim_lease_sec: data.startup_shared_bridge.claim_lease_sec,
        stale_task_timeout_sec: data.startup_shared_bridge.stale_task_timeout_sec,
        artifact_retention_days: data.startup_shared_bridge.artifact_retention_days,
        sqlite_busy_timeout_ms: data.startup_shared_bridge.sqlite_busy_timeout_ms,
      });
    }
    health.runtime_activated = Boolean(
      typeof data.runtime_activated === "boolean"
        ? data.runtime_activated
        : health.runtime_activated,
    );
    health.activation_phase = String(data.activation_phase || health.activation_phase || "");
    health.activation_error = String(data.activation_error || health.activation_error || "");
    health.active_job_id = String(data.active_job_id || "");
    health.active_job_ids = Array.isArray(data.active_job_ids) ? data.active_job_ids : [];
    health.job_counts = data.job_counts && typeof data.job_counts === "object" ? { ...data.job_counts } : {};
    if (data.scheduler && typeof data.scheduler === "object") {
      Object.assign(health.scheduler, data.scheduler);
    }
    if (data.handover_scheduler && typeof data.handover_scheduler === "object") {
      Object.assign(health.handover_scheduler, data.handover_scheduler);
    }
    if (data.handover && typeof data.handover === "object") {
      if (data.handover.engineer_directory && typeof data.handover.engineer_directory === "object") {
        if (
          data.handover.engineer_directory.target_preview
          && typeof data.handover.engineer_directory.target_preview === "object"
        ) {
          Object.assign(health.handover.engineer_directory.target_preview, data.handover.engineer_directory.target_preview);
        } else {
          Object.assign(health.handover.engineer_directory.target_preview, {
            configured_app_token: "",
            operation_app_token: "",
            table_id: "",
            target_kind: "",
            display_url: "",
            bitable_url: "",
            wiki_node_token: "",
            message: "",
            resolved_at: "",
          });
        }
      } else if (health?.handover?.engineer_directory?.target_preview) {
        Object.assign(health.handover.engineer_directory.target_preview, {
          configured_app_token: "",
          operation_app_token: "",
          table_id: "",
          target_kind: "",
          display_url: "",
          bitable_url: "",
          wiki_node_token: "",
          message: "",
          resolved_at: "",
        });
      }
      if (data.handover.review_status && typeof data.handover.review_status === "object") {
        health.handover.review_status = {
          ...health.handover.review_status,
          ...data.handover.review_status,
        };
      }
      health.handover.review_recipient_status_by_building = Array.isArray(data.handover.review_recipient_status_by_building)
        ? data.handover.review_recipient_status_by_building
        : [];
      applyHandoverReviewAccessSnapshot(data.handover);
    } else if (health?.handover) {
      health.handover.review_recipient_status_by_building = [];
    }
    if (data.wet_bulb_collection && typeof data.wet_bulb_collection === "object") {
      health.wet_bulb_collection.enabled = Boolean(data.wet_bulb_collection.enabled);
      if (data.wet_bulb_collection.scheduler && typeof data.wet_bulb_collection.scheduler === "object") {
        Object.assign(health.wet_bulb_collection.scheduler, data.wet_bulb_collection.scheduler);
      }
      if (data.wet_bulb_collection.target_preview && typeof data.wet_bulb_collection.target_preview === "object") {
        Object.assign(health.wet_bulb_collection.target_preview, data.wet_bulb_collection.target_preview);
      } else {
        Object.assign(health.wet_bulb_collection.target_preview, {
          configured_app_token: "",
          operation_app_token: "",
          table_id: "",
          target_kind: "",
          display_url: "",
          bitable_url: "",
          wiki_node_token: "",
          message: "",
          resolved_at: "",
        });
      }
    }
    if (data.monthly_event_report && typeof data.monthly_event_report === "object") {
      health.monthly_event_report.enabled = Boolean(data.monthly_event_report.enabled);
      if (data.monthly_event_report.scheduler && typeof data.monthly_event_report.scheduler === "object") {
        Object.assign(health.monthly_event_report.scheduler, data.monthly_event_report.scheduler);
      }
      if (data.monthly_event_report.last_run && typeof data.monthly_event_report.last_run === "object") {
        Object.assign(health.monthly_event_report.last_run, data.monthly_event_report.last_run);
      } else {
        Object.assign(health.monthly_event_report.last_run, {
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
        });
      }
      if (data.monthly_event_report.delivery && typeof data.monthly_event_report.delivery === "object") {
        health.monthly_event_report.delivery.error = String(data.monthly_event_report.delivery.error || "");
        if (data.monthly_event_report.delivery.last_run && typeof data.monthly_event_report.delivery.last_run === "object") {
          Object.assign(health.monthly_event_report.delivery.last_run, data.monthly_event_report.delivery.last_run);
        } else {
          Object.assign(health.monthly_event_report.delivery.last_run, {
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
          });
        }
        health.monthly_event_report.delivery.recipient_status_by_building = Array.isArray(data.monthly_event_report.delivery.recipient_status_by_building)
          ? data.monthly_event_report.delivery.recipient_status_by_building
          : [];
      } else {
        health.monthly_event_report.delivery.error = "";
        Object.assign(health.monthly_event_report.delivery.last_run, {
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
        });
        health.monthly_event_report.delivery.recipient_status_by_building = [];
      }
    }
    if (data.monthly_change_report && typeof data.monthly_change_report === "object") {
      health.monthly_change_report.enabled = Boolean(data.monthly_change_report.enabled);
      if (data.monthly_change_report.scheduler && typeof data.monthly_change_report.scheduler === "object") {
        Object.assign(health.monthly_change_report.scheduler, data.monthly_change_report.scheduler);
      }
      if (data.monthly_change_report.last_run && typeof data.monthly_change_report.last_run === "object") {
        Object.assign(health.monthly_change_report.last_run, data.monthly_change_report.last_run);
      } else {
        Object.assign(health.monthly_change_report.last_run, {
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
        });
      }
      if (data.monthly_change_report.delivery && typeof data.monthly_change_report.delivery === "object") {
        health.monthly_change_report.delivery.error = String(data.monthly_change_report.delivery.error || "");
        if (data.monthly_change_report.delivery.last_run && typeof data.monthly_change_report.delivery.last_run === "object") {
          Object.assign(health.monthly_change_report.delivery.last_run, data.monthly_change_report.delivery.last_run);
        } else {
          Object.assign(health.monthly_change_report.delivery.last_run, {
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
          });
        }
        health.monthly_change_report.delivery.recipient_status_by_building = Array.isArray(data.monthly_change_report.delivery.recipient_status_by_building)
          ? data.monthly_change_report.delivery.recipient_status_by_building
          : [];
      } else {
        health.monthly_change_report.delivery.error = "";
        Object.assign(health.monthly_change_report.delivery.last_run, {
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
        });
        health.monthly_change_report.delivery.recipient_status_by_building = [];
      }
    }
    if (data.day_metric_upload && typeof data.day_metric_upload === "object") {
      if (data.day_metric_upload.scheduler && typeof data.day_metric_upload.scheduler === "object") {
        Object.assign(health.day_metric_upload.scheduler, {
          enabled: Boolean(data.day_metric_upload.scheduler.enabled),
          running: Boolean(data.day_metric_upload.scheduler.running),
          status: String(data.day_metric_upload.scheduler.status || ""),
          next_run_time: String(data.day_metric_upload.scheduler.next_run_time || ""),
          last_check_at: String(data.day_metric_upload.scheduler.last_check_at || ""),
          last_decision: String(data.day_metric_upload.scheduler.last_decision || ""),
          last_trigger_at: String(data.day_metric_upload.scheduler.last_trigger_at || ""),
          last_trigger_result: String(data.day_metric_upload.scheduler.last_trigger_result || ""),
          state_path: String(data.day_metric_upload.scheduler.state_path || ""),
          state_exists: Boolean(data.day_metric_upload.scheduler.state_exists),
          executor_bound: Boolean(data.day_metric_upload.scheduler.executor_bound),
          callback_name: String(data.day_metric_upload.scheduler.callback_name || ""),
          remembered_enabled: Boolean(data.day_metric_upload.scheduler.remembered_enabled),
          effective_auto_start_in_gui: Boolean(data.day_metric_upload.scheduler.effective_auto_start_in_gui),
          memory_source: String(data.day_metric_upload.scheduler.memory_source || ""),
        });
      } else {
        Object.assign(health.day_metric_upload.scheduler, {
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
          remembered_enabled: false,
          effective_auto_start_in_gui: false,
          memory_source: "",
        });
      }
      if (data.day_metric_upload.target_preview && typeof data.day_metric_upload.target_preview === "object") {
        Object.assign(health.day_metric_upload.target_preview, data.day_metric_upload.target_preview);
      } else {
        Object.assign(health.day_metric_upload.target_preview, {
          configured_app_token: "",
          operation_app_token: "",
          table_id: "",
          target_kind: "",
          display_url: "",
          bitable_url: "",
          wiki_node_token: "",
          message: "",
          resolved_at: "",
        });
      }
    }
    if (data.alarm_event_upload && typeof data.alarm_event_upload === "object") {
      health.alarm_event_upload.enabled = Boolean(data.alarm_event_upload.enabled);
      if (data.alarm_event_upload.scheduler && typeof data.alarm_event_upload.scheduler === "object") {
        Object.assign(health.alarm_event_upload.scheduler, {
          enabled: Boolean(data.alarm_event_upload.scheduler.enabled),
          running: Boolean(data.alarm_event_upload.scheduler.running),
          status: String(data.alarm_event_upload.scheduler.status || ""),
          next_run_time: String(data.alarm_event_upload.scheduler.next_run_time || ""),
          last_check_at: String(data.alarm_event_upload.scheduler.last_check_at || ""),
          last_decision: String(data.alarm_event_upload.scheduler.last_decision || ""),
          last_trigger_at: String(data.alarm_event_upload.scheduler.last_trigger_at || ""),
          last_trigger_result: String(data.alarm_event_upload.scheduler.last_trigger_result || ""),
          state_path: String(data.alarm_event_upload.scheduler.state_path || ""),
          state_exists: Boolean(data.alarm_event_upload.scheduler.state_exists),
          executor_bound: Boolean(data.alarm_event_upload.scheduler.executor_bound),
          callback_name: String(data.alarm_event_upload.scheduler.callback_name || ""),
          remembered_enabled: Boolean(data.alarm_event_upload.scheduler.remembered_enabled),
          effective_auto_start_in_gui: Boolean(data.alarm_event_upload.scheduler.effective_auto_start_in_gui),
          memory_source: String(data.alarm_event_upload.scheduler.memory_source || ""),
        });
      } else {
        Object.assign(health.alarm_event_upload.scheduler, {
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
          remembered_enabled: false,
          effective_auto_start_in_gui: false,
          memory_source: "",
        });
      }
      if (data.alarm_event_upload.target_preview && typeof data.alarm_event_upload.target_preview === "object") {
        Object.assign(health.alarm_event_upload.target_preview, data.alarm_event_upload.target_preview);
      } else {
        Object.assign(health.alarm_event_upload.target_preview, {
          configured_app_token: "",
          operation_app_token: "",
          table_id: "",
          target_kind: "",
          display_url: "",
          bitable_url: "",
          wiki_node_token: "",
          message: "",
          resolved_at: "",
        });
      }
    }
    if (data.deployment && typeof data.deployment === "object") {
      Object.assign(health.deployment, data.deployment);
    }
    if (data.shared_bridge && typeof data.shared_bridge === "object") {
      const nextSharedBridge = { ...data.shared_bridge };
      if (
        nextSharedBridge.internal_source_cache
        && typeof nextSharedBridge.internal_source_cache === "object"
        && health.shared_bridge?.internal_source_cache
        && typeof health.shared_bridge.internal_source_cache === "object"
      ) {
        nextSharedBridge.internal_source_cache = mergeLiteInternalSourceCache(
          health.shared_bridge.internal_source_cache,
          nextSharedBridge.internal_source_cache,
        );
      }
      if (
        nextSharedBridge.internal_download_pool
        && typeof nextSharedBridge.internal_download_pool === "object"
        && health.shared_bridge?.internal_download_pool
        && typeof health.shared_bridge.internal_download_pool === "object"
      ) {
        nextSharedBridge.internal_download_pool = mergePresentedPayload(
          health.shared_bridge.internal_download_pool,
          nextSharedBridge.internal_download_pool,
          ["overview"],
        );
      }
      if (
        nextSharedBridge.internal_alert_status
        && typeof nextSharedBridge.internal_alert_status === "object"
        && health.shared_bridge?.internal_alert_status
        && typeof health.shared_bridge.internal_alert_status === "object"
      ) {
        nextSharedBridge.internal_alert_status = mergePresentedPayload(
          health.shared_bridge.internal_alert_status,
          nextSharedBridge.internal_alert_status,
          ["display_overview"],
        );
      }
      if (isLiteHealth) {
        if (
          (!nextSharedBridge.internal_source_cache || typeof nextSharedBridge.internal_source_cache !== "object")
          && health.shared_bridge?.internal_source_cache
          && typeof health.shared_bridge.internal_source_cache === "object"
        ) {
          nextSharedBridge.internal_source_cache = health.shared_bridge.internal_source_cache;
        }
        if (
          (!nextSharedBridge.internal_download_pool || typeof nextSharedBridge.internal_download_pool !== "object")
          && health.shared_bridge?.internal_download_pool
          && typeof health.shared_bridge.internal_download_pool === "object"
        ) {
          nextSharedBridge.internal_download_pool = health.shared_bridge.internal_download_pool;
        }
        if (
          (!nextSharedBridge.internal_alert_status || typeof nextSharedBridge.internal_alert_status !== "object")
          && health.shared_bridge?.internal_alert_status
          && typeof health.shared_bridge.internal_alert_status === "object"
        ) {
          nextSharedBridge.internal_alert_status = health.shared_bridge.internal_alert_status;
        }
      }
      Object.assign(health.shared_bridge, nextSharedBridge);
    }
    if (data.network && typeof data.network === "object") {
      Object.assign(health.network, data.network);
    }
    if (data.updater && typeof data.updater === "object") {
      Object.assign(health.updater, data.updater);
    }
    if (Array.isArray(data.alert_log_entries) && logs.value.length === 0) {
      data.alert_log_entries.forEach((entry) => appendLog(entry));
    }
    if (Number.isInteger(data.system_log_next_offset)) {
      systemLogOffset.value = Number.parseInt(String(data.system_log_next_offset), 10) || 0;
    } else if (Array.isArray(data.system_log_entries) && data.system_log_entries.length) {
      const lastEntry = data.system_log_entries[data.system_log_entries.length - 1];
      systemLogOffset.value = Number.parseInt(String(lastEntry?.id || 0), 10) || 0;
    }
  }

  function hydrateConfigView(normalized) {
    config.value = normalized;
    const buildings = Array.isArray(normalized?.input?.buildings) ? normalized.input.buildings : [];
    buildingsText.value = buildings.join(", ");
    if (!manualBuilding.value || !buildings.includes(manualBuilding.value)) {
      manualBuilding.value = buildings.length ? buildings[0] : "";
    }
    if (!dayMetricUploadBuilding.value || !buildings.includes(dayMetricUploadBuilding.value)) {
      dayMetricUploadBuilding.value = buildings.length ? buildings[0] : "";
    }
    if (!dayMetricLocalBuilding.value || !buildings.includes(dayMetricLocalBuilding.value)) {
      dayMetricLocalBuilding.value = buildings.length ? buildings[0] : "";
    }
    const rows = normalizeSheetRules(normalized?.feishu_sheet_import?.sheet_rules);
    sheetRuleRows.value = rows.length ? rows : [{ sheet_name: "", table_id: "", header_row: 1 }];
    syncCustomWindowLocalInputs();
  }

  async function fetchBootstrapHealth(options = {}) {
    const silentMessage = Boolean(options?.silentMessage);
    try {
      const data = await getBootstrapHealthApi();
      applyHealthSnapshot(data);
      clearBootstrapRetryTimer();
      if (bootstrapReady) {
        bootstrapReady.value = true;
      }
      if (healthLoadError) {
        healthLoadError.value = "";
      }
      return true;
    } catch (err) {
      if (isAbortError(err)) {
        scheduleBootstrapHealthRetry();
        return false;
      }
      if (healthLoadError) {
        healthLoadError.value = String(err || "").trim();
      }
      if (isTransientNetworkError(err)) {
        scheduleBootstrapHealthRetry();
        return false;
      }
      if (!bootstrapReady?.value) {
        scheduleBootstrapHealthRetry();
      }
      if (!silentMessage) {
        message.value = `启动状态读取失败: ${err}`;
      }
      return false;
    }
  }

  async function fetchHealth(options = {}) {
    if (isUpdaterTrafficPaused() || !isRuntimeApiReady()) return false;
    const silentTransientNetworkError = Boolean(options?.silentTransientNetworkError);
    const silentMessage = Boolean(options?.silentMessage);
    const includeHandoverContext =
      typeof options?.includeHandoverContext === "boolean"
        ? options.includeHandoverContext
        : typeof shouldIncludeHandoverHealthContext === "function"
          ? Boolean(shouldIncludeHandoverHealthContext())
          : true;
    const lightweight = Boolean(options?.lightweight);
    const requestKey = lightweight ? "lite" : "full";
    if (healthRequestInFlight[requestKey]) {
      return healthRequestInFlight[requestKey];
    }
    healthRequestInFlight[requestKey] = (async () => {
      try {
        const previousUpdaterSnapshot = clone(health?.updater || {});
        const params = includeHandoverContext
          ? {
              handover_duty_date: String(handoverDutyDate?.value || "").trim(),
              handover_duty_shift: String(handoverDutyShift?.value || "").trim().toLowerCase(),
            }
          : {};
        if (lightweight) {
          params.health_mode = "lite";
        }
        const data = await getHealthApi(params);
        if (lightweight && fullHealthLoaded?.value) {
          return true;
        }
        applyHealthSnapshot(data);
        handleUpdaterRuntimeSideEffects(previousUpdaterSnapshot, health?.updater || {});
        updaterHealthHydratedOnce = true;
        if (!lightweight && fullHealthLoaded) {
          fullHealthLoaded.value = true;
        }
        if (healthLoadError) {
          healthLoadError.value = "";
        }
        return true;
      } catch (err) {
        if (isAbortError(err)) return false;
        if (isRoleSelectionConflictError(err)) {
          await tryRecoverFromRoleSelectionConflict();
          return false;
        }
        if (healthLoadError) {
          healthLoadError.value = String(err || "").trim();
        }
        if (silentTransientNetworkError && isTransientNetworkError(err)) return false;
        if (!silentMessage) {
          message.value = `健康检查失败: ${err}`;
        }
        return false;
      } finally {
        healthRequestInFlight[requestKey] = null;
      }
    })();
    return healthRequestInFlight[requestKey];
  }

  async function fetchJobs(options = {}) {
    if (isUpdaterTrafficPaused() || !isRuntimeApiReady()) return false;
    const silentMessage = Boolean(options?.silentMessage);
    try {
      const data = await getJobsApi({ limit: 60 });
      applyJobPanelSummary(data || {});
      let detailJobId = "";
      const currentSelectedJobId = String(selectedJobId?.value || currentJob?.value?.job_id || "").trim();
      if (currentSelectedJobId) {
        const matched = jobsList.value.find((item) => String(item?.job_id || "").trim() === currentSelectedJobId);
        if (matched && currentJob) {
          currentJob.value = { ...(currentJob.value || {}), ...matched };
        }
        detailJobId = currentSelectedJobId;
      } else if (jobsList.value.length && currentJob) {
        const fallback =
          jobsList.value.find((item) => String(item?.status || "").trim().toLowerCase() === "running") ||
          jobsList.value.find((item) => String(item?.status || "").trim().toLowerCase() === "waiting_resource") ||
          jobsList.value[0];
        if (fallback) {
          currentJob.value = { ...(currentJob.value || {}), ...fallback };
          if (selectedJobId) {
            selectedJobId.value = String(fallback?.job_id || "").trim();
          }
          detailJobId = String(fallback?.job_id || "").trim();
        }
      }
      if (detailJobId && currentJob) {
        void (async () => {
          try {
            const detail = await getJobApi(detailJobId);
            const latestSelectedJobId = String(selectedJobId?.value || currentJob?.value?.job_id || "").trim();
            if (detail && typeof detail === "object" && (!latestSelectedJobId || latestSelectedJobId === detailJobId)) {
              currentJob.value = { ...(currentJob.value || {}), ...detail };
            }
          } catch (_) {
            // keep list summary when detail refresh fails
          }
        })();
      }
      return true;
    } catch (err) {
      if (isRoleSelectionConflictError(err)) {
        await tryRecoverFromRoleSelectionConflict();
        return false;
      }
      if (!silentMessage) {
        message.value = `读取任务列表失败: ${err}`;
      }
      return false;
    }
  }

  async function fetchRuntimeResources(options = {}) {
    if (isUpdaterTrafficPaused() || !isRuntimeApiReady()) return false;
    const silentMessage = Boolean(options?.silentMessage);
    try {
      const data = await getRuntimeResourcesApi();
      resourceSnapshot.value =
        data && typeof data === "object"
          ? data
          : { network: {}, controlled_browser: { holder_job_id: "", queue_length: 0 }, batch_locks: [], resources: [] };
      return true;
    } catch (err) {
      if (isRoleSelectionConflictError(err)) {
        await tryRecoverFromRoleSelectionConflict();
        return false;
      }
      if (!silentMessage) {
        message.value = `读取资源状态失败: ${err}`;
      }
      return false;
    }
  }

  function applyExternalSchedulerSummary(summary) {
    if (!summary || typeof summary !== "object") return;
    if (summary.scheduler && typeof summary.scheduler === "object") {
      Object.assign(health.scheduler, summary.scheduler);
    }
    if (summary.handover_scheduler && typeof summary.handover_scheduler === "object") {
      Object.assign(health.handover_scheduler, summary.handover_scheduler);
    }
    const wet = summary.wet_bulb_collection_scheduler;
    if (wet && typeof wet === "object" && health?.wet_bulb_collection?.scheduler) {
      Object.assign(health.wet_bulb_collection.scheduler, wet);
    }
    const dayMetric = summary.day_metric_upload_scheduler;
    if (dayMetric && typeof dayMetric === "object" && health?.day_metric_upload?.scheduler) {
      Object.assign(health.day_metric_upload.scheduler, dayMetric);
    }
    const alarm = summary.alarm_event_upload_scheduler;
    if (alarm && typeof alarm === "object" && health?.alarm_event_upload?.scheduler) {
      Object.assign(health.alarm_event_upload.scheduler, alarm);
    }
    const monthlyEvent = summary.monthly_event_report_scheduler;
    if (monthlyEvent && typeof monthlyEvent === "object" && health?.monthly_event_report?.scheduler) {
      Object.assign(health.monthly_event_report.scheduler, monthlyEvent);
    }
    const monthlyChange = summary.monthly_change_report_scheduler;
    if (monthlyChange && typeof monthlyChange === "object" && health?.monthly_change_report?.scheduler) {
      Object.assign(health.monthly_change_report.scheduler, monthlyChange);
    }
  }

  function applyJobPanelSummary(summary) {
    const jobPanel = summary && typeof summary === "object" ? summary : {};
    const nextJobs = Array.isArray(jobPanel.jobs) ? jobPanel.jobs : [];
    jobsList.value = nextJobs;
    health.job_panel_summary = {
      jobs: nextJobs,
      count: Number(jobPanel.count || nextJobs.length || 0),
      active_job_ids: Array.isArray(jobPanel.active_job_ids) ? [...jobPanel.active_job_ids] : [],
      job_counts: jobPanel.job_counts && typeof jobPanel.job_counts === "object" ? { ...jobPanel.job_counts } : {},
      display: jobPanel.display && typeof jobPanel.display === "object" ? { ...jobPanel.display } : {},
    };
    const selectedId = String(selectedJobId?.value || currentJob?.value?.job_id || "").trim();
    if (selectedId) {
      const matched = nextJobs.find((item) => String(item?.job_id || "").trim() === selectedId);
      if (matched && currentJob) {
        currentJob.value = { ...(currentJob.value || {}), ...matched };
      } else if (selectedJobId) {
        selectedJobId.value = "";
      }
    }
    if ((!selectedId || !String(selectedJobId?.value || "").trim()) && nextJobs.length && currentJob) {
      const fallback =
        nextJobs.find((item) => String(item?.status || "").trim().toLowerCase() === "running")
        || nextJobs.find((item) => String(item?.status || "").trim().toLowerCase() === "waiting_resource")
        || nextJobs[0];
      if (fallback) {
        currentJob.value = { ...(currentJob.value || {}), ...fallback };
        if (selectedJobId) {
          selectedJobId.value = String(fallback?.job_id || "").trim();
        }
      }
    }
    if (Array.isArray(jobPanel.active_job_ids)) {
      health.active_job_ids = [...jobPanel.active_job_ids];
    }
    if (jobPanel.job_counts && typeof jobPanel.job_counts === "object") {
      health.job_counts = { ...jobPanel.job_counts };
    }
  }

  function buildPendingActionState(action, patch = {}) {
    const base = action && typeof action === "object" ? action : {};
    const normalizedPatch = patch && typeof patch === "object" ? patch : {};
    const disabledReason = String(
      normalizedPatch.disabled_reason
      || normalizedPatch.disabledReason
      || base.disabled_reason
      || base.disabledReason
      || "",
    ).trim();
    return {
      ...base,
      ...normalizedPatch,
      pending: normalizedPatch.pending !== undefined ? Boolean(normalizedPatch.pending) : Boolean(base.pending),
      disabled_reason: disabledReason,
      disabledReason,
    };
  }

  function patchJobPanelActionState(jobId, actionName, patch = {}) {
    const jobIdText = String(jobId || "").trim();
    const actionKey = String(actionName || "").trim();
    if (!jobIdText || !actionKey) return false;
    let changed = false;
    const updateRow = (item) => {
      if (!item || typeof item !== "object") return item;
      if (String(item?.job_id || "").trim() !== jobIdText) return item;
      changed = true;
      const actions = item.actions && typeof item.actions === "object" ? item.actions : {};
      return {
        ...item,
        actions: {
          ...actions,
          [actionKey]: buildPendingActionState(actions[actionKey], patch),
        },
      };
    };
    if (Array.isArray(jobsList.value)) {
      jobsList.value = jobsList.value.map(updateRow);
    }
    if (currentJob?.value && String(currentJob.value?.job_id || "").trim() === jobIdText) {
      currentJob.value = updateRow(currentJob.value);
    }
    const summary = health.job_panel_summary && typeof health.job_panel_summary === "object"
      ? health.job_panel_summary
      : null;
    if (summary) {
      const display = summary.display && typeof summary.display === "object" ? summary.display : {};
      health.job_panel_summary = {
        ...summary,
        jobs: Array.isArray(summary.jobs) ? summary.jobs.map(updateRow) : summary.jobs,
        display: {
          ...display,
          running_jobs: Array.isArray(display.running_jobs) ? display.running_jobs.map(updateRow) : display.running_jobs,
          waiting_resource_items: Array.isArray(display.waiting_resource_items)
            ? display.waiting_resource_items.map(updateRow)
            : display.waiting_resource_items,
          recent_finished_jobs: Array.isArray(display.recent_finished_jobs)
            ? display.recent_finished_jobs.map(updateRow)
            : display.recent_finished_jobs,
        },
      };
    }
    return changed;
  }

  function applyBridgeTasksSummary(summary, options = {}) {
    const bridgeSummary = summary && typeof summary === "object" ? summary : {};
    if (bridgeTasksDisplay) {
      const previousDisplay = bridgeTasksDisplay.value && typeof bridgeTasksDisplay.value === "object"
        ? bridgeTasksDisplay.value
        : {};
      const display = bridgeSummary.display && typeof bridgeSummary.display === "object"
        ? bridgeSummary.display
        : {};
      bridgeTasksDisplay.value = {
        active_tasks: Array.isArray(display.active_tasks)
          ? display.active_tasks
          : (Array.isArray(previousDisplay.active_tasks) ? previousDisplay.active_tasks : []),
        waiting_resource_items: Array.isArray(display.waiting_resource_items)
          ? display.waiting_resource_items
          : (Array.isArray(previousDisplay.waiting_resource_items) ? previousDisplay.waiting_resource_items : []),
        recent_finished_tasks: Array.isArray(display.recent_finished_tasks)
          ? display.recent_finished_tasks
          : (Array.isArray(previousDisplay.recent_finished_tasks) ? previousDisplay.recent_finished_tasks : []),
        active_count: Number.parseInt(String(display.active_count ?? previousDisplay.active_count ?? 0), 10) || 0,
        waiting_count: Number.parseInt(String(display.waiting_count ?? previousDisplay.waiting_count ?? 0), 10) || 0,
        finished_count: Number.parseInt(String(display.finished_count ?? previousDisplay.finished_count ?? 0), 10) || 0,
        overview: display.overview && typeof display.overview === "object"
          ? display.overview
          : (previousDisplay.overview && typeof previousDisplay.overview === "object"
            ? previousDisplay.overview
            : {
            reason_code: "idle",
            tone: "neutral",
            status_text: "当前空闲",
            summary_text: "暂无共享桥接任务。",
            detail_text: "当前没有共享桥接任务。",
            focus_title: "当前没有选中共享桥接任务",
            focus_meta: "暂无共享桥接任务",
            active_count: 0,
            waiting_count: 0,
            finished_count: 0,
            items: [],
            actions: [],
          }),
      };
    }
    const tasks = (Array.isArray(bridgeSummary.tasks) ? bridgeSummary.tasks : []).filter((item) => {
      const requestPayload = item?.request && typeof item.request === "object" ? item.request : {};
      return !String(requestPayload.resume_job_id || "").trim();
    });
    bridgeTasks.value = tasks;
    const preferredTaskId = String(options?.preferredTaskId || "").trim();
    const selectedTaskId = String(preferredTaskId || selectedBridgeTaskId?.value || "").trim();
    let nextTaskId = selectedTaskId;
    if (selectedTaskId && !tasks.some((item) => String(item?.task_id || "").trim() === selectedTaskId)) {
      nextTaskId = "";
    }
    if (!nextTaskId && tasks.length) {
      const preferred =
        tasks.find((item) => {
          const status = String(item?.status || "").trim().toLowerCase();
          return !["success", "failed", "partial_failed", "cancelled", "stale"].includes(status);
        }) || tasks[0];
      nextTaskId = String(preferred?.task_id || "").trim();
    }
    if (selectedBridgeTaskId) {
      selectedBridgeTaskId.value = nextTaskId;
    }
    if (bridgeTaskDetail) {
      const matchedDetail = nextTaskId
        ? tasks.find((item) => String(item?.task_id || "").trim() === nextTaskId) || null
        : null;
      bridgeTaskDetail.value = matchedDetail;
    }
    return nextTaskId;
  }

  function applyBridgeTaskMutationResult(data, taskId) {
    const taskIdText = String(taskId || "").trim();
    let applied = false;
    const summary = data?.bridge_tasks_summary && typeof data.bridge_tasks_summary === "object"
      ? data.bridge_tasks_summary
      : null;
    if (summary) {
      applyBridgeTasksSummary(summary, { preferredTaskId: taskIdText });
      applied = true;
    }
    const task = data?.task && typeof data.task === "object" ? data.task : null;
    if (task) {
      const currentTaskId = String(task?.task_id || "").trim();
      if (currentTaskId) {
        if (Array.isArray(bridgeTasks.value) && bridgeTasks.value.length) {
          let matched = false;
          bridgeTasks.value = bridgeTasks.value.map((item) => {
            if (String(item?.task_id || "").trim() !== currentTaskId) return item;
            matched = true;
            return { ...item, ...task };
          });
          if (!matched) {
            bridgeTasks.value = [task, ...bridgeTasks.value];
          }
        } else {
          bridgeTasks.value = [task];
        }
        const selectedTaskId = String(selectedBridgeTaskId?.value || "").trim();
        if (!selectedTaskId || selectedTaskId === currentTaskId || currentTaskId === taskIdText) {
          if (selectedBridgeTaskId) {
            selectedBridgeTaskId.value = currentTaskId;
          }
          if (bridgeTaskDetail) {
            bridgeTaskDetail.value = task;
          }
        }
      }
      applied = true;
    }
    return applied;
  }

  function patchBridgeTaskActionState(taskId, actionName, patch = {}) {
    const taskIdText = String(taskId || "").trim();
    const actionKey = String(actionName || "").trim();
    if (!taskIdText || !actionKey) return false;
    let changed = false;
    const updateRow = (item) => {
      if (!item || typeof item !== "object") return item;
      if (String(item?.task_id || "").trim() !== taskIdText) return item;
      changed = true;
      const actions = item.actions && typeof item.actions === "object" ? item.actions : {};
      return {
        ...item,
        actions: {
          ...actions,
          [actionKey]: buildPendingActionState(actions[actionKey], patch),
        },
      };
    };
    if (Array.isArray(bridgeTasks.value)) {
      bridgeTasks.value = bridgeTasks.value.map(updateRow);
    }
    if (bridgeTaskDetail?.value && String(bridgeTaskDetail.value?.task_id || "").trim() === taskIdText) {
      bridgeTaskDetail.value = updateRow(bridgeTaskDetail.value);
    }
    if (bridgeTasksDisplay?.value && typeof bridgeTasksDisplay.value === "object") {
      bridgeTasksDisplay.value = {
        ...bridgeTasksDisplay.value,
        active_tasks: Array.isArray(bridgeTasksDisplay.value.active_tasks)
          ? bridgeTasksDisplay.value.active_tasks.map(updateRow)
          : bridgeTasksDisplay.value.active_tasks,
        waiting_resource_items: Array.isArray(bridgeTasksDisplay.value.waiting_resource_items)
          ? bridgeTasksDisplay.value.waiting_resource_items.map(updateRow)
          : bridgeTasksDisplay.value.waiting_resource_items,
        recent_finished_tasks: Array.isArray(bridgeTasksDisplay.value.recent_finished_tasks)
          ? bridgeTasksDisplay.value.recent_finished_tasks.map(updateRow)
          : bridgeTasksDisplay.value.recent_finished_tasks,
      };
    }
    return changed;
  }

  function applyExternalDashboardSummary(data) {
    if (!data || typeof data !== "object") return;
    if (isLocallyExitedToRoleSelection()) return;
    const healthLite = data.health_lite && typeof data.health_lite === "object" ? data.health_lite : null;
    if (healthLite) {
      applyHealthSnapshot(healthLite);
    }
    const nextDisplay = data.display && typeof data.display === "object" ? data.display : null;
    if (nextDisplay) {
      const previousDisplay = health.dashboard_display && typeof health.dashboard_display === "object"
        ? health.dashboard_display
        : {};
      health.dashboard_display = {
        ...previousDisplay,
        ...nextDisplay,
      };
    } else if (!health.dashboard_display || typeof health.dashboard_display !== "object") {
      health.dashboard_display = {};
    }
    applyExternalSchedulerSummary(
      data.scheduler_status_summary && typeof data.scheduler_status_summary === "object"
        ? data.scheduler_status_summary
        : null,
    );
    const updater = data.updater_summary && typeof data.updater_summary === "object" ? data.updater_summary : null;
    if (updater && health?.updater && typeof health.updater === "object") {
      Object.assign(health.updater, updater);
    }
    const jobPanel = data.job_panel_summary && typeof data.job_panel_summary === "object" ? data.job_panel_summary : null;
    if (jobPanel) {
      applyJobPanelSummary(jobPanel);
    }
    const bridgeSummary = data.bridge_tasks_summary && typeof data.bridge_tasks_summary === "object"
      ? data.bridge_tasks_summary
      : null;
    if (bridgeSummary) {
      applyBridgeTasksSummary(bridgeSummary);
    }
    const runtimeResources = data.runtime_resources_summary && typeof data.runtime_resources_summary === "object"
      ? data.runtime_resources_summary
      : null;
    if (runtimeResources) {
      resourceSnapshot.value = runtimeResources;
    }
  }

  async function fetchExternalDashboardSummary(options = {}) {
    if (isUpdaterTrafficPaused() || isLocallyExitedToRoleSelection() || !isRuntimeApiReady()) return false;
    if (resolveCurrentRoleMode() === "internal") return false;
    const silentMessage = Boolean(options?.silentMessage);
    if (externalDashboardSummaryRequestInFlight) {
      return externalDashboardSummaryRequestInFlight;
    }
    externalDashboardSummaryRequestInFlight = (async () => {
      try {
        const data = await getExternalDashboardSummaryApi();
        applyExternalDashboardSummary(data || {});
        if (fullHealthLoaded) {
          fullHealthLoaded.value = true;
        }
        if (healthLoadError) {
          healthLoadError.value = "";
        }
        return true;
      } catch (err) {
        if (isAbortError(err)) return false;
        if (isRoleSelectionConflictError(err)) {
          await tryRecoverFromRoleSelectionConflict();
          return false;
        }
        if (!silentMessage) {
          message.value = `读取外网首页状态失败: ${err}`;
        }
        return false;
      } finally {
        externalDashboardSummaryRequestInFlight = null;
      }
    })();
    return externalDashboardSummaryRequestInFlight;
  }

  async function fetchInternalRuntimeSummary(options = {}) {
    if (isUpdaterTrafficPaused() || !isRuntimeApiReady() || !canUseInternalRuntimeStatus()) return false;
    const silentMessage = Boolean(options?.silentMessage);
    const force = Boolean(options?.force);
    if (internalRuntimeSummaryRequestInFlight) {
      if (force) {
        internalRuntimeSummaryRefetchQueued = true;
      }
      return internalRuntimeSummaryRequestInFlight;
    }
    internalRuntimeSummaryRequestInFlight = (async () => {
      try {
        const data = await getInternalRuntimeStatusApi();
        const summary = data?.summary && typeof data.summary === "object" ? data.summary : null;
        if (summary) {
          internalRuntimeSummary.value = summary;
        } else if (!internalRuntimeSummary.value || typeof internalRuntimeSummary.value !== "object") {
          internalRuntimeSummary.value = {};
        }
        return true;
      } catch (err) {
        if (isRoleSelectionConflictError(err)) {
          await tryRecoverFromRoleSelectionConflict();
          return false;
        }
        if (!silentMessage) {
          message.value = `读取内网运行状态失败: ${err}`;
        }
        return false;
      } finally {
        internalRuntimeSummaryRequestInFlight = null;
        if (internalRuntimeSummaryRefetchQueued) {
          internalRuntimeSummaryRefetchQueued = false;
          if (!isUpdaterTrafficPaused() && isRuntimeApiReady() && canUseInternalRuntimeStatus()) {
            void fetchInternalRuntimeSummary({ silentMessage: true });
          }
        }
      }
    })();
    return internalRuntimeSummaryRequestInFlight;
  }

  async function fetchInternalRuntimeBuildingRuntimeStatus(building, options = {}) {
    if (isUpdaterTrafficPaused() || !isRuntimeApiReady() || !canUseInternalRuntimeStatus()) return false;
    const buildingText = normalizeInternalRuntimeBuildingName(building);
    const buildingCode = normalizeInternalRuntimeBuildingCode(buildingText);
    const silentMessage = Boolean(options?.silentMessage);
    const force = Boolean(options?.force);
    const currentInFlight = internalRuntimeBuildingRequestsInFlight.get(buildingCode);
    if (currentInFlight) {
      if (force) {
        internalRuntimeBuildingRefetchQueued.set(buildingCode, true);
      }
      return currentInFlight;
    }
    const request = (async () => {
      try {
        const data = await getInternalRuntimeBuildingStatusApi(buildingCode);
        const status = data?.status && typeof data.status === "object" ? data.status : null;
        if (!status || !internalBuildingRuntimeStatusMap?.value || typeof internalBuildingRuntimeStatusMap.value !== "object") {
          return false;
        }
        internalBuildingRuntimeStatusMap.value = {
          ...internalBuildingRuntimeStatusMap.value,
          [buildingText]: {
            ...(internalBuildingRuntimeStatusMap.value?.[buildingText] || {}),
            ...status,
            building: status.building || buildingText,
            building_code: status.building_code || buildingCode,
          },
        };
        return true;
      } catch (err) {
        if (isRoleSelectionConflictError(err)) {
          await tryRecoverFromRoleSelectionConflict();
          return false;
        }
        if (!silentMessage) {
          message.value = `读取 ${buildingText} 内网状态失败: ${err}`;
        }
        return false;
      } finally {
        internalRuntimeBuildingRequestsInFlight.delete(buildingCode);
        if (internalRuntimeBuildingRefetchQueued.get(buildingCode)) {
          internalRuntimeBuildingRefetchQueued.delete(buildingCode);
          if (!isUpdaterTrafficPaused() && isRuntimeApiReady() && canUseInternalRuntimeStatus()) {
            void fetchInternalRuntimeBuildingRuntimeStatus(buildingText, { silentMessage: true });
          }
        }
      }
    })();
    internalRuntimeBuildingRequestsInFlight.set(buildingCode, request);
    return request;
  }

  async function fetchAllInternalBuildingRuntimeStatuses(options = {}) {
    if (isUpdaterTrafficPaused() || !canUseInternalRuntimeStatus()) return false;
    const results = await Promise.all(
      INTERNAL_RUNTIME_BUILDINGS.map((building) =>
        fetchInternalRuntimeBuildingRuntimeStatus(building, options),
      ),
    );
    return results.every(Boolean);
  }

  function bindHandoverReviewStatusBroadcast() {
    if (handoverReviewStatusBroadcastBound) return;
    if (typeof window === "undefined" || !window.addEventListener) return;
    handoverReviewStatusBroadcastBound = true;
    window.addEventListener("storage", (event) => {
      if (String(event?.key || "").trim() !== HANDOVER_REVIEW_STATUS_BROADCAST_KEY) return;
      if (!String(event?.newValue || "").trim()) return;
      refreshRoleScopedRuntimeStatus("handover_review_status_broadcast", { includePendingResume: true });
    });
  }

  function patchAlarmUploadRunningState(data, fallbackMode, fallbackScope) {
    const family =
      health.shared_bridge?.internal_source_cache?.alarm_event_family &&
      typeof health.shared_bridge.internal_source_cache.alarm_event_family === "object"
        ? health.shared_bridge.internal_source_cache.alarm_event_family
        : null;
    if (!family) return;
    const uploadState =
      family.external_upload && typeof family.external_upload === "object"
        ? family.external_upload
        : {};
    family.external_upload = {
      ...uploadState,
      running: Boolean(data?.running),
      started_at: String(data?.started_at || uploadState.started_at || "").trim(),
      current_mode: String(data?.mode || fallbackMode || uploadState.current_mode || "").trim(),
      current_scope: String(data?.scope || fallbackScope || uploadState.current_scope || "").trim(),
      last_error: "",
    };
  }

  async function fetchBridgeTaskDetail(taskId, options = {}) {
    if (isUpdaterTrafficPaused()) return false;
    const taskIdText = String(taskId || "").trim();
    if (!taskIdText) {
      if (bridgeTaskDetail) bridgeTaskDetail.value = null;
      return true;
    }
    const silentMessage = Boolean(options?.silentMessage);
    bridgeTaskDetailRequestInFlight = (async () => {
      try {
        const data = await getBridgeTaskApi(taskIdText);
        bridgeTaskDetail.value = data?.task && typeof data.task === "object" ? data.task : null;
        return true;
      } catch (err) {
        if (bridgeTaskDetail && String(bridgeTaskDetail.value?.task_id || "").trim() === taskIdText) {
          bridgeTaskDetail.value = null;
        }
        if (!silentMessage) {
          message.value = `读取共享任务详情失败: ${err}`;
        }
        return false;
      } finally {
        bridgeTaskDetailRequestInFlight = null;
      }
    })();
    return bridgeTaskDetailRequestInFlight;
  }

  async function fetchBridgeTasks(options = {}) {
    if (isUpdaterTrafficPaused() || !isRuntimeApiReady()) return false;
    if (bridgeTasksRequestInFlight) return bridgeTasksRequestInFlight;
    const force = Boolean(options?.force);
    const now = Date.now();
    if (!force && lastBridgeTasksFetchAt > 0 && now - lastBridgeTasksFetchAt < BRIDGE_TASKS_FETCH_COOLDOWN_MS) {
      return true;
    }
    lastBridgeTasksFetchAt = now;
    const silentMessage = Boolean(options?.silentMessage);
    bridgeTasksRequestInFlight = (async () => {
      try {
        const data = await getBridgeTasksApi({ limit: 60 });
        const nextTaskId = applyBridgeTasksSummary(data || {});
        if (nextTaskId) {
          await fetchBridgeTaskDetail(nextTaskId, { silentMessage: true });
        } else if (bridgeTaskDetail) {
          bridgeTaskDetail.value = null;
        }
        return true;
      } catch (err) {
        if (isRoleSelectionConflictError(err)) {
          await tryRecoverFromRoleSelectionConflict();
          return false;
        }
        if (!silentMessage) {
          message.value = `读取共享任务失败: ${err}`;
        }
        return false;
      } finally {
        bridgeTasksRequestInFlight = null;
      }
    })();
    return bridgeTasksRequestInFlight;
  }

  function getBridgeTaskCancelActionKey(taskId) {
    return `${ACTION_KEY_BRIDGE_CANCEL_PREFIX}${String(taskId || "").trim()}`;
  }

  function getBridgeTaskRetryActionKey(taskId) {
    return `${ACTION_KEY_BRIDGE_RETRY_PREFIX}${String(taskId || "").trim()}`;
  }

  async function cancelBridgeTask(taskId) {
    const taskIdText = String(taskId || selectedBridgeTaskId?.value || bridgeTaskDetail?.value?.task_id || "").trim();
    if (!taskIdText) {
      message.value = "当前没有可取消的共享任务";
      return false;
    }
    const runner = async () => {
      try {
        patchBridgeTaskActionState(taskIdText, "cancel", {
          allowed: false,
          pending: true,
          label: "取消中...",
          disabled_reason: "取消请求已提交",
        });
        const data = await cancelBridgeTaskApi(taskIdText);
        const applied = applyBridgeTaskMutationResult(data, taskIdText);
        if (!applied) {
          await fetchBridgeTasks({ silentMessage: true, force: true });
          await fetchBridgeTaskDetail(taskIdText, { silentMessage: true });
        }
        message.value = "共享任务取消请求已提交";
        return true;
      } catch (err) {
        await fetchBridgeTasks({ silentMessage: true, force: true });
        message.value = `共享任务取消失败: ${err}`;
        return false;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(getBridgeTaskCancelActionKey(taskIdText), runner, { cooldownMs: 0 });
    }
    return runner();
  }

  async function retryBridgeTask(taskId) {
    const taskIdText = String(taskId || selectedBridgeTaskId?.value || bridgeTaskDetail?.value?.task_id || "").trim();
    if (!taskIdText) {
      message.value = "当前没有可重试的共享任务";
      return false;
    }
    const runner = async () => {
      try {
        patchBridgeTaskActionState(taskIdText, "retry", {
          allowed: false,
          pending: true,
          label: "重试中...",
          disabled_reason: "重试请求已提交",
        });
        const data = await retryBridgeTaskApi(taskIdText);
        const applied = applyBridgeTaskMutationResult(data, taskIdText);
        if (!applied) {
          await fetchBridgeTasks({ silentMessage: true, force: true });
          await fetchBridgeTaskDetail(taskIdText, { silentMessage: true });
        }
        message.value = "共享任务已重新排队";
        return true;
      } catch (err) {
        await fetchBridgeTasks({ silentMessage: true, force: true });
        message.value = `共享任务重试失败: ${err}`;
        return false;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(getBridgeTaskRetryActionKey(taskIdText), runner, { cooldownMs: 0 });
    }
    return runner();
  }

  async function refreshCurrentHourSourceCache() {
    const runner = async () => {
      try {
        const data = await refreshCurrentHourSourceCacheApi();
        refreshRoleScopedRuntimeStatus("source_cache_refresh_current_hour");
        message.value = String(data?.message || "").trim() || "已开始下载当前小时全部文件";
        return data;
      } catch (err) {
        message.value = `触发当前小时下载失败: ${err}`;
        return { ok: false, error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_SOURCE_CACHE_REFRESH_CURRENT_HOUR, runner, { cooldownMs: 0 });
    }
    return runner();
  }

  function getSourceCacheRefreshBuildingActionKey(sourceFamily, building) {
    return `${ACTION_KEY_SOURCE_CACHE_REFRESH_BUILDING_LATEST_PREFIX}${String(sourceFamily || "").trim()}:${String(building || "").trim()}`;
  }

  async function refreshBuildingLatestSourceCache(sourceFamily, building) {
    const sourceFamilyText = String(sourceFamily || "").trim();
    const buildingText = String(building || "").trim();
    if (!sourceFamilyText || !buildingText) {
      message.value = "缺少楼栋或文件类型，无法执行单楼拉取。";
      return { ok: false, error: "missing_source_family_or_building" };
    }
    const runner = async () => {
      try {
        const data = await refreshBuildingLatestSourceCacheApi(sourceFamilyText, buildingText);
        refreshRoleScopedRuntimeStatus("source_cache_refresh_building_latest", { building: buildingText });
        const familyLabel = SOURCE_CACHE_FAMILY_LABELS[sourceFamilyText] || sourceFamilyText;
        message.value = String(data?.message || "").trim() || `已开始重新拉取 ${buildingText} ${familyLabel}`;
        return data;
      } catch (err) {
        const familyLabel = SOURCE_CACHE_FAMILY_LABELS[sourceFamilyText] || sourceFamilyText;
        message.value = `${buildingText} ${familyLabel}拉取失败: ${err}`;
        return { ok: false, error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(getSourceCacheRefreshBuildingActionKey(sourceFamilyText, buildingText), runner, { cooldownMs: 0 });
    }
    return runner();
  }

  async function refreshManualAlarmSourceCache() {
    const runner = async () => {
      try {
        const data = await refreshManualAlarmSourceCacheApi();
        refreshRoleScopedRuntimeStatus("source_cache_refresh_alarm_manual");
        message.value = String(data?.message || "").trim() || "已开始手动拉取告警信息文件";
        return data;
      } catch (err) {
        message.value = `手动拉取告警信息文件失败: ${err}`;
        return { ok: false, error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_SOURCE_CACHE_REFRESH_ALARM_MANUAL, runner, { cooldownMs: 0 });
    }
    return runner();
  }

  async function deleteManualAlarmSourceCacheFiles() {
    const runner = async () => {
      try {
        const data = await deleteManualAlarmSourceCacheFilesApi();
        refreshRoleScopedRuntimeStatus("source_cache_delete_alarm_manual");
        message.value = String(data?.message || "").trim() || "已删除手动拉取的告警信息文件";
        return data;
      } catch (err) {
        message.value = `删除手动告警信息文件失败: ${err}`;
        return { ok: false, error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_SOURCE_CACHE_DELETE_ALARM_MANUAL, runner, { cooldownMs: 0 });
    }
    return runner();
  }

  async function uploadAlarmSourceCacheFull() {
    const runner = async () => {
      try {
        const data = await uploadAlarmSourceCacheFullApi();
        patchAlarmUploadRunningState(data, "full", "all");
        await focusAcceptedJob(
          data,
          String(data?.message || "").trim() || "已提交 使用共享文件上传60天-全部楼栋",
        );
        return data;
      } catch (err) {
        message.value = `使用共享文件上传60天失败（全部楼栋）: ${err}`;
        return { ok: false, error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_SOURCE_CACHE_UPLOAD_ALARM_FULL, runner, { cooldownMs: 0 });
    }
    return runner();
  }

  async function uploadAlarmSourceCacheBuilding(building) {
    const buildingText = String(building || "").trim();
    if (!buildingText) {
      message.value = "请选择要上传的楼栋";
      return { ok: false, error: "missing_building" };
    }
    const runner = async () => {
      try {
        const data = await uploadAlarmSourceCacheBuildingApi(buildingText);
        patchAlarmUploadRunningState(data, "single_building", buildingText);
        await focusAcceptedJob(
          data,
          String(data?.message || "").trim() || `已提交 使用共享文件上传60天-${buildingText}`,
        );
        return data;
      } catch (err) {
        message.value = `使用共享文件上传60天失败（${buildingText}）: ${err}`;
        return { ok: false, error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(`${ACTION_KEY_SOURCE_CACHE_UPLOAD_ALARM_BUILDING}:${buildingText}`, runner, { cooldownMs: 0 });
    }
    return runner();
  }

  async function openAlarmEventUploadTarget() {
    const runner = async () => {
      try {
        const data = await openAlarmEventUploadTargetApi();
        const preview =
          data?.target_preview && typeof data.target_preview === "object"
            ? data.target_preview
            : {};
        if (health?.alarm_event_upload?.target_preview && typeof health.alarm_event_upload.target_preview === "object") {
          Object.assign(health.alarm_event_upload.target_preview, preview);
        }
        const displayUrl = String(preview?.display_url || preview?.bitable_url || "").trim();
        if (!displayUrl) {
          message.value =
            String(preview?.message || "").trim() || "当前未解析到可用的告警多维表链接，请查看系统日志。";
          return { ok: false, error: "missing_display_url", target_preview: preview };
        }
        window.open(displayUrl, "_blank", "noopener,noreferrer");
        const targetKind = String(preview?.target_kind || "").trim();
        message.value = `已打开告警多维表（${targetKind || "unknown"}）`;
        return data;
      } catch (err) {
        message.value = `打开告警多维表失败: ${err}`;
        return { ok: false, error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight("alarm_event_upload:open_target", runner, { cooldownMs: 0 });
    }
    return runner();
  }

  function applyHandoverDailyReportContext(data) {
    const nextBatchKey = String(data?.batch_key || "").trim();
    if (!handoverDailyReportContext) return;
    handoverDailyReportContext.value = {
      ok: true,
      batch_key: nextBatchKey,
      duty_date: String(data?.duty_date || "").trim(),
      duty_shift: String(data?.duty_shift || "").trim().toLowerCase(),
      daily_report_record_export:
        data?.daily_report_record_export && typeof data.daily_report_record_export === "object"
          ? { ...data.daily_report_record_export }
          : {
              status: "idle",
              updated_at: "",
              record_id: "",
              record_url: "",
              spreadsheet_url: "",
              error: "",
              summary_screenshot_path: "",
              external_screenshot_path: "",
            },
      screenshot_auth:
        data?.screenshot_auth && typeof data.screenshot_auth === "object"
          ? { ...data.screenshot_auth }
          : {
              status: "missing_login",
              profile_dir: "",
              last_checked_at: "",
              error: "",
              browser_kind: "",
              browser_label: "",
              browser_executable: "",
            },
      capture_assets:
        data?.capture_assets && typeof data.capture_assets === "object"
          ? { ...data.capture_assets }
          : {
              summary_sheet_image: {
                exists: false,
                source: "none",
                stored_path: "",
                captured_at: "",
                preview_url: "",
                thumbnail_url: "",
                full_image_url: "",
                auto: {
                  exists: false,
                  stored_path: "",
                  captured_at: "",
                  preview_url: "",
                  thumbnail_url: "",
                  full_image_url: "",
                },
                manual: {
                  exists: false,
                  stored_path: "",
                  captured_at: "",
                  preview_url: "",
                  thumbnail_url: "",
                  full_image_url: "",
                },
              },
              external_page_image: {
                exists: false,
                source: "none",
                stored_path: "",
                captured_at: "",
                preview_url: "",
                thumbnail_url: "",
                full_image_url: "",
                auto: {
                  exists: false,
                  stored_path: "",
                  captured_at: "",
                  preview_url: "",
                  thumbnail_url: "",
                  full_image_url: "",
                },
                manual: {
                  exists: false,
                  stored_path: "",
                  captured_at: "",
                  preview_url: "",
                  thumbnail_url: "",
                  full_image_url: "",
                },
              },
            },
      display:
        data?.display && typeof data.display === "object"
          ? { ...data.display }
          : {
              auth: null,
              export: null,
              actions: {},
              capture_assets: null,
            },
    };
    if (handoverDailyReportLastScreenshotTest) {
      const currentBatchKey = String(handoverDailyReportLastScreenshotTest.value?.batch_key || "").trim();
      if (currentBatchKey && currentBatchKey !== nextBatchKey) {
        handoverDailyReportLastScreenshotTest.value = emptyDailyReportScreenshotTestState(nextBatchKey);
      }
      if (!nextBatchKey && currentBatchKey) {
        handoverDailyReportLastScreenshotTest.value = emptyDailyReportScreenshotTestState("");
      }
    }
  }

  async function fetchHandoverDailyReportContext(options = {}) {
    if (isUpdaterTrafficPaused()) return false;
    if (dailyReportContextRequestInFlight) return dailyReportContextRequestInFlight;
    const silentTransientNetworkError = Boolean(options?.silentTransientNetworkError);
    const silentMessage = Boolean(options?.silentMessage);
    const dutyDate = String(handoverDutyDate?.value || "").trim();
    const dutyShift = String(handoverDutyShift?.value || "").trim().toLowerCase();
    if (!dutyDate || !["day", "night"].includes(dutyShift)) {
      if (handoverDailyReportContext) {
        applyHandoverDailyReportContext({
          duty_date: dutyDate,
          duty_shift: dutyShift,
        });
      }
      return true;
    }
    if (!Boolean(bootstrapReady?.value)) {
      if (handoverDailyReportContext) {
        applyHandoverDailyReportContext({
          duty_date: dutyDate,
          duty_shift: dutyShift,
        });
      }
      return true;
    }
    if (resolveCurrentRoleMode() !== "external" || !Boolean(health?.runtime_activated) || !Boolean(health?.startup_role_confirmed)) {
      if (handoverDailyReportContext) {
        applyHandoverDailyReportContext({
          duty_date: dutyDate,
          duty_shift: dutyShift,
        });
      }
      return true;
    }
    if (typeof shouldFetchHandoverDailyReportContext === "function" && !Boolean(shouldFetchHandoverDailyReportContext())) {
      if (handoverDailyReportContext) {
        applyHandoverDailyReportContext({
          duty_date: dutyDate,
          duty_shift: dutyShift,
        });
      }
      return true;
    }
    dailyReportContextRequestInFlight = (async () => {
      try {
        const data = await getHandoverDailyReportContextApi({
          duty_date: dutyDate,
          duty_shift: dutyShift,
        });
        applyHandoverDailyReportContext(data || {});
        return true;
      } catch (err) {
        if (isAbortError(err)) return false;
        if (silentTransientNetworkError && isTransientNetworkError(err)) return false;
        const httpStatus = Number.parseInt(String(err?.httpStatus || 0), 10) || 0;
        if (silentMessage && (httpStatus === 404 || httpStatus === 409)) {
          if (handoverDailyReportContext) {
            applyHandoverDailyReportContext({
              duty_date: dutyDate,
              duty_shift: dutyShift,
            });
          }
          return false;
        }
        if (handoverDailyReportContext) {
          applyHandoverDailyReportContext({
            duty_date: dutyDate,
            duty_shift: dutyShift,
            daily_report_record_export: {
              status: "failed",
              error: String(err || ""),
            },
            screenshot_auth: {
              status: "expired",
              error: String(err || ""),
              browser_kind: "",
              browser_label: "",
              browser_executable: "",
            },
          });
        }
        if (!silentMessage) {
          message.value = `读取日报多维状态失败: ${err}`;
        }
        return false;
      } finally {
        dailyReportContextRequestInFlight = null;
      }
    })();
    return dailyReportContextRequestInFlight;
  }

  function openHandoverDailyReportPreview(target) {
    if (!handoverDailyReportPreviewModal || !handoverDailyReportCaptureAssets) return;
    const targetText = String(target || "").trim().toLowerCase();
    const assets = handoverDailyReportCaptureAssets.value || {};
    const asset =
      targetText === "summary_sheet"
        ? assets.summarySheetImage || {}
        : targetText === "external_page"
          ? assets.externalPageImage || {}
          : {};
    const fullImageUrl = String(asset?.full_image_url || asset?.preview_url || "").trim();
    if (!fullImageUrl) {
      message.value = "当前还没有可预览的截图";
      return;
    }
    handoverDailyReportPreviewModal.value = {
      open: true,
      title: `${getDailyReportTargetLabel(targetText)}棰勮`,
      imageUrl: fullImageUrl,
      downloadName: String(asset?.downloadName || "").trim(),
    };
  }

  function closeHandoverDailyReportPreview() {
    if (!handoverDailyReportPreviewModal) return;
    handoverDailyReportPreviewModal.value = { open: false, title: "", imageUrl: "", downloadName: "" };
  }

  function openHandoverDailyReportUploadDialog(target) {
    if (!handoverDailyReportUploadModal) return;
    const targetText = String(target || "").trim().toLowerCase();
    handoverDailyReportUploadModal.value = {
      open: true,
      target: targetText,
      title: `上传/粘贴替换${getDailyReportTargetLabel(targetText)}`,
      hint: "点击弹层后按 Ctrl+V，可直接粘贴剪贴板图片。",
    };
  }

  function closeHandoverDailyReportUploadDialog() {
    if (!handoverDailyReportUploadModal) return;
    handoverDailyReportUploadModal.value = { open: false, target: "", title: "", hint: "" };
  }

  function buildDailyReportAssetActionPayload(target) {
    return {
      duty_date: String(handoverDutyDate?.value || "").trim(),
      duty_shift: String(handoverDutyShift?.value || "").trim().toLowerCase(),
      target: String(target || "").trim().toLowerCase(),
    };
  }

  function getHandoverDailyReportRecaptureActionKey(target) {
    return `${ACTION_KEY_HANDOVER_DAILY_REPORT_RECAPTURE_PREFIX}${String(target || "").trim().toLowerCase()}`;
  }

  function getHandoverDailyReportUploadActionKey(target) {
    return `${ACTION_KEY_HANDOVER_DAILY_REPORT_UPLOAD_PREFIX}${String(target || "").trim().toLowerCase()}`;
  }

  function getHandoverDailyReportRestoreActionKey(target) {
    return `${ACTION_KEY_HANDOVER_DAILY_REPORT_RESTORE_PREFIX}${String(target || "").trim().toLowerCase()}`;
  }

  async function fetchHandoverEngineerDirectory(options = {}) {
    if (isUpdaterTrafficPaused()) return false;
    if (!handoverEngineerDirectory || !handoverEngineerLoading) return;
    const silentMessage = Boolean(options?.silentMessage);
    const forceRefresh = Boolean(options?.forceRefresh);
    if (engineerDirectoryPrefetchTimer) {
      window.clearTimeout(engineerDirectoryPrefetchTimer);
      engineerDirectoryPrefetchTimer = null;
    }
    const cacheSignature = buildEngineerDirectoryCacheSignature();
    if (!forceRefresh) {
      const cachedRows = readEngineerDirectoryCache(cacheSignature);
      if (cachedRows) {
        handoverEngineerDirectory.value = cachedRows;
        if (engineerDirectoryLoaded) {
          engineerDirectoryLoaded.value = true;
        }
        handoverEngineerLoading.value = false;
        return true;
      }
    }
    try {
      handoverEngineerLoading.value = true;
      const data = await getHandoverEngineerDirectoryApi();
      const rows = Array.isArray(data?.rows) ? data.rows : [];
      const preview =
        data?.target_preview && typeof data.target_preview === "object"
          ? data.target_preview
          : null;
      handoverEngineerDirectory.value = rows;
      if (preview && health?.handover?.engineer_directory?.target_preview) {
        Object.assign(health.handover.engineer_directory.target_preview, preview);
      }
      writeEngineerDirectoryCache(cacheSignature, rows);
      if (engineerDirectoryLoaded) {
        engineerDirectoryLoaded.value = true;
      }
      return true;
    } catch (err) {
      handoverEngineerDirectory.value = [];
      if (engineerDirectoryLoaded) {
        engineerDirectoryLoaded.value = false;
      }
      if (!silentMessage) {
        message.value = `读取工程师目录失败: ${err}`;
      }
      return false;
    } finally {
      handoverEngineerLoading.value = false;
    }
  }

  function ensureHandoverEngineerDirectoryLoaded(options = {}) {
    if (!handoverEngineerDirectory || !handoverEngineerLoading) return Promise.resolve(false);
    if (handoverEngineerLoading.value) return Promise.resolve(true);
    if (engineerDirectoryLoaded?.value) return Promise.resolve(true);
    return fetchHandoverEngineerDirectory(options);
  }

  function scheduleEngineerDirectoryPrefetch(delayMs = 3000) {
    if (!handoverEngineerDirectory || !handoverEngineerLoading) return;
    if (typeof shouldLoadEngineerDirectory === "function" && !shouldLoadEngineerDirectory()) return;
    if (handoverEngineerLoading.value || engineerDirectoryLoaded?.value) return;
    if (engineerDirectoryPrefetchTimer) {
      window.clearTimeout(engineerDirectoryPrefetchTimer);
    }
    engineerDirectoryPrefetchTimer = window.setTimeout(() => {
      engineerDirectoryPrefetchTimer = null;
      void ensureHandoverEngineerDirectoryLoaded({ silentMessage: true });
    }, Math.max(0, Number.parseInt(String(delayMs || 0), 10) || 0));
  }

  function normalizeHandoverBuildingName(building) {
    const raw = String(building || "").trim();
    if (["A楼", "B楼", "C楼", "D楼", "E楼"].includes(raw)) return raw;
    const upper = raw.toUpperCase();
    if (["A", "B", "C", "D", "E"].includes(upper)) return `${upper}楼`;
    return "A楼";
  }

  function normalizeHandoverBuildingCode(building) {
    return normalizeHandoverBuildingName(building).replace("楼", "");
  }

  function normalizeHandoverReviewBaseUrlInput(rawValue) {
    const trimmed = String(rawValue || "").trim();
    if (!trimmed) {
      return { ok: true, value: "" };
    }
    const prefixed = /^https?:\/\//i.test(trimmed) ? trimmed : `http://${trimmed}`;
    try {
      const parsed = new URL(prefixed);
      if (!/^https?:$/i.test(parsed.protocol) || !String(parsed.host || "").trim()) {
        return { ok: false, error: "审核页外部访问基地址必须是合法的 http/https 地址" };
      }
      return { ok: true, value: `${parsed.protocol}//${parsed.host}` };
    } catch (_err) {
      return { ok: false, error: "审核页外部访问基地址格式错误，应类似 http://192.168.220.160:18765" };
    }
  }

  function ensureHandoverSegmentConfigShape() {
    if (!config.value || typeof config.value !== "object") return null;
    config.value.handover_log = config.value.handover_log && typeof config.value.handover_log === "object"
      ? config.value.handover_log
      : {};
    const handover = config.value.handover_log;
    handover.capacity_report = handover.capacity_report && typeof handover.capacity_report === "object"
      ? handover.capacity_report
      : {};
    handover.capacity_report.weather = handover.capacity_report.weather && typeof handover.capacity_report.weather === "object"
      ? handover.capacity_report.weather
      : {};
    handover.cell_rules = handover.cell_rules && typeof handover.cell_rules === "object"
      ? handover.cell_rules
      : {};
    handover.cell_rules.default_rows = Array.isArray(handover.cell_rules.default_rows)
      ? handover.cell_rules.default_rows
      : [];
    handover.cell_rules.building_rows = handover.cell_rules.building_rows && typeof handover.cell_rules.building_rows === "object"
      ? handover.cell_rules.building_rows
      : {};
    handover.cloud_sheet_sync = handover.cloud_sheet_sync && typeof handover.cloud_sheet_sync === "object"
      ? handover.cloud_sheet_sync
      : {};
    handover.cloud_sheet_sync.sheet_names = handover.cloud_sheet_sync.sheet_names && typeof handover.cloud_sheet_sync.sheet_names === "object"
      ? handover.cloud_sheet_sync.sheet_names
      : {};
    handover.review_ui = handover.review_ui && typeof handover.review_ui === "object"
      ? handover.review_ui
      : {};
    handover.review_ui.cabinet_power_defaults_by_building = handover.review_ui.cabinet_power_defaults_by_building
      && typeof handover.review_ui.cabinet_power_defaults_by_building === "object"
      ? handover.review_ui.cabinet_power_defaults_by_building
      : {};
    handover.review_ui.footer_inventory_defaults_by_building = handover.review_ui.footer_inventory_defaults_by_building
      && typeof handover.review_ui.footer_inventory_defaults_by_building === "object"
      ? handover.review_ui.footer_inventory_defaults_by_building
      : {};
    handover.review_ui.review_link_recipients_by_building = handover.review_ui.review_link_recipients_by_building
      && typeof handover.review_ui.review_link_recipients_by_building === "object"
      ? handover.review_ui.review_link_recipients_by_building
      : {};
    if (!String(handover.capacity_report.weather.provider || "").trim()) {
      handover.capacity_report.weather.provider = "seniverse";
    }
    if (!String(handover.capacity_report.weather.location || "").trim()) {
      handover.capacity_report.weather.location = "崇川区";
    }
    handover.capacity_report.weather.fallback_locations = Array.isArray(handover.capacity_report.weather.fallback_locations)
      ? handover.capacity_report.weather.fallback_locations
        .map((item) => String(item || "").trim())
        .filter(Boolean)
      : ["南通"];
    if (!handover.capacity_report.weather.fallback_locations.length) {
      handover.capacity_report.weather.fallback_locations = ["南通"];
    }
    if (!String(handover.capacity_report.weather.language || "").trim()) {
      handover.capacity_report.weather.language = "zh-Hans";
    }
    if (!String(handover.capacity_report.weather.unit || "").trim()) {
      handover.capacity_report.weather.unit = "c";
    }
    if (!String(handover.capacity_report.weather.auth_mode || "").trim()) {
      handover.capacity_report.weather.auth_mode = "signed";
    }
    if (!Number.isInteger(Number.parseInt(String(handover.capacity_report.weather.timeout_sec ?? ""), 10))
      || Number.parseInt(String(handover.capacity_report.weather.timeout_sec ?? ""), 10) <= 0) {
      handover.capacity_report.weather.timeout_sec = 8;
    }
    if (typeof handover.capacity_report.weather.seniverse_public_key !== "string") {
      handover.capacity_report.weather.seniverse_public_key = "";
    }
    if (typeof handover.capacity_report.weather.seniverse_private_key !== "string") {
      handover.capacity_report.weather.seniverse_private_key = "";
    }
    Object.keys(handover.review_ui.review_link_recipients_by_building).forEach((building) => {
      const rows = Array.isArray(handover.review_ui.review_link_recipients_by_building[building])
        ? handover.review_ui.review_link_recipients_by_building[building]
        : [];
      handover.review_ui.review_link_recipients_by_building[building] = rows
        .filter((row) => row && typeof row === "object")
        .map((row) => ({
          ...row,
          enabled: row.enabled === false ? false : true,
        }));
    });
    return handover;
  }

  function applyHandoverCommonSegmentData(segmentData) {
    const handover = ensureHandoverSegmentConfigShape();
    if (!handover) return;
    const preservedBuildingRows = clone(handover.cell_rules?.building_rows || {});
    const preservedSheetNames = clone(handover.cloud_sheet_sync?.sheet_names || {});
    const preservedCabinetDefaults = clone(handover.review_ui?.cabinet_power_defaults_by_building || {});
    const preservedFooterDefaults = clone(handover.review_ui?.footer_inventory_defaults_by_building || {});
    const preservedReviewLinkRecipients = clone(handover.review_ui?.review_link_recipients_by_building || {});
    const preservedCapacityReport = clone(handover.capacity_report || {});
    const next = segmentData && typeof segmentData === "object" ? clone(segmentData) : {};
    next.capacity_report = next.capacity_report && typeof next.capacity_report === "object"
      ? { ...preservedCapacityReport, ...next.capacity_report }
      : preservedCapacityReport;
    next.capacity_report.weather = next.capacity_report.weather && typeof next.capacity_report.weather === "object"
      ? { ...(preservedCapacityReport.weather || {}), ...next.capacity_report.weather }
      : clone(preservedCapacityReport.weather || {});
    next.cell_rules = next.cell_rules && typeof next.cell_rules === "object" ? next.cell_rules : {};
    next.cloud_sheet_sync = next.cloud_sheet_sync && typeof next.cloud_sheet_sync === "object" ? next.cloud_sheet_sync : {};
    next.review_ui = next.review_ui && typeof next.review_ui === "object" ? next.review_ui : {};
    next.cell_rules.building_rows = preservedBuildingRows;
    next.cloud_sheet_sync.sheet_names = preservedSheetNames;
    next.review_ui.cabinet_power_defaults_by_building = preservedCabinetDefaults;
    next.review_ui.footer_inventory_defaults_by_building = preservedFooterDefaults;
    next.review_ui.review_link_recipients_by_building = preservedReviewLinkRecipients;
    config.value.handover_log = next;
  }

  function applyHandoverBuildingSegmentData(building, segmentData) {
    const handover = ensureHandoverSegmentConfigShape();
    if (!handover) return;
    const buildingText = normalizeHandoverBuildingName(building);
    const payload = segmentData && typeof segmentData === "object" ? segmentData : {};
    const buildingRows = payload?.cell_rules?.building_rows && typeof payload.cell_rules.building_rows === "object"
      ? payload.cell_rules.building_rows
      : {};
    const sheetNames = payload?.cloud_sheet_sync?.sheet_names && typeof payload.cloud_sheet_sync.sheet_names === "object"
      ? payload.cloud_sheet_sync.sheet_names
      : {};
    const cabinetDefaults = payload?.review_ui?.cabinet_power_defaults_by_building
      && typeof payload.review_ui.cabinet_power_defaults_by_building === "object"
      ? payload.review_ui.cabinet_power_defaults_by_building
      : {};
    const footerDefaults = payload?.review_ui?.footer_inventory_defaults_by_building
      && typeof payload.review_ui.footer_inventory_defaults_by_building === "object"
      ? payload.review_ui.footer_inventory_defaults_by_building
      : {};
    const reviewLinkRecipients = payload?.review_ui?.review_link_recipients_by_building
      && typeof payload.review_ui.review_link_recipients_by_building === "object"
      ? payload.review_ui.review_link_recipients_by_building
      : {};
    handover.cell_rules.building_rows[buildingText] = clone(buildingRows[buildingText] || []);
    handover.cloud_sheet_sync.sheet_names[buildingText] = String(sheetNames[buildingText] || "").trim();
    if (Object.prototype.hasOwnProperty.call(cabinetDefaults, buildingText)) {
      handover.review_ui.cabinet_power_defaults_by_building[buildingText] = clone(cabinetDefaults[buildingText] || {});
    } else {
      delete handover.review_ui.cabinet_power_defaults_by_building[buildingText];
    }
    if (Object.prototype.hasOwnProperty.call(footerDefaults, buildingText)) {
      handover.review_ui.footer_inventory_defaults_by_building[buildingText] = clone(footerDefaults[buildingText] || {});
    } else {
      delete handover.review_ui.footer_inventory_defaults_by_building[buildingText];
    }
    handover.review_ui.review_link_recipients_by_building[buildingText] = Array.isArray(reviewLinkRecipients[buildingText])
      ? clone(reviewLinkRecipients[buildingText]).map((row) => ({
        ...row,
        enabled: row?.enabled === false ? false : true,
      }))
      : [];
  }

  function syncServerConfigSnapshotHandoverSegment() {
    if (!config.value || typeof config.value !== "object") return;
    const legacyHandover = config.value.handover_log && typeof config.value.handover_log === "object"
      ? clone(config.value.handover_log)
      : {};
    const partialV3 = convertLegacyConfigToV3({ handover_log: legacyHandover });
    const nextHandover = partialV3?.features?.handover_log && typeof partialV3.features.handover_log === "object"
      ? partialV3.features.handover_log
      : {};
    serverConfigSnapshot = mergeConfigWithServerSnapshot(serverConfigSnapshot, {
      features: {
        handover_log: nextHandover,
      },
    });
  }

  function buildHandoverCommonSegmentPayload() {
    const handover = ensureHandoverSegmentConfigShape();
    if (!handover) return {};
    const payload = clone(handover);
    payload.capacity_report = payload.capacity_report && typeof payload.capacity_report === "object"
      ? payload.capacity_report
      : {};
    payload.capacity_report.weather = payload.capacity_report.weather && typeof payload.capacity_report.weather === "object"
      ? payload.capacity_report.weather
      : {};
    payload.cell_rules = payload.cell_rules && typeof payload.cell_rules === "object" ? payload.cell_rules : {};
    payload.cloud_sheet_sync = payload.cloud_sheet_sync && typeof payload.cloud_sheet_sync === "object" ? payload.cloud_sheet_sync : {};
    payload.review_ui = payload.review_ui && typeof payload.review_ui === "object" ? payload.review_ui : {};
    payload.cell_rules.building_rows = payload.cell_rules.building_rows && typeof payload.cell_rules.building_rows === "object"
      ? payload.cell_rules.building_rows
      : {};
    payload.cloud_sheet_sync.sheet_names = payload.cloud_sheet_sync.sheet_names && typeof payload.cloud_sheet_sync.sheet_names === "object"
      ? payload.cloud_sheet_sync.sheet_names
      : {};
    payload.review_ui.cabinet_power_defaults_by_building = payload.review_ui.cabinet_power_defaults_by_building
      && typeof payload.review_ui.cabinet_power_defaults_by_building === "object"
      ? payload.review_ui.cabinet_power_defaults_by_building
      : {};
    payload.review_ui.footer_inventory_defaults_by_building = payload.review_ui.footer_inventory_defaults_by_building
      && typeof payload.review_ui.footer_inventory_defaults_by_building === "object"
      ? payload.review_ui.footer_inventory_defaults_by_building
      : {};
    payload.review_ui.review_link_recipients_by_building = payload.review_ui.review_link_recipients_by_building
      && typeof payload.review_ui.review_link_recipients_by_building === "object"
      ? payload.review_ui.review_link_recipients_by_building
      : {};
    for (const building of ["A楼", "B楼", "C楼", "D楼", "E楼"]) {
      delete payload.cell_rules.building_rows[building];
      delete payload.cloud_sheet_sync.sheet_names[building];
      delete payload.review_ui.cabinet_power_defaults_by_building[building];
      delete payload.review_ui.footer_inventory_defaults_by_building[building];
      delete payload.review_ui.review_link_recipients_by_building[building];
    }
    return payload;
  }

  function buildHandoverBuildingSegmentPayload(building) {
    const handover = ensureHandoverSegmentConfigShape();
    const buildingText = normalizeHandoverBuildingName(building);
    if (!handover) {
      return {
        cell_rules: { building_rows: { [buildingText]: [] } },
        cloud_sheet_sync: { sheet_names: { [buildingText]: "" } },
        review_ui: {
          cabinet_power_defaults_by_building: { [buildingText]: [] },
          footer_inventory_defaults_by_building: { [buildingText]: {} },
          review_link_recipients_by_building: { [buildingText]: [] },
        },
      };
    }
    return {
      cell_rules: {
        building_rows: {
          [buildingText]: clone(handover.cell_rules?.building_rows?.[buildingText] || []),
        },
      },
      cloud_sheet_sync: {
        sheet_names: {
          [buildingText]: String(handover.cloud_sheet_sync?.sheet_names?.[buildingText] || "").trim(),
        },
      },
      review_ui: {
        cabinet_power_defaults_by_building: Object.prototype.hasOwnProperty.call(
          handover.review_ui?.cabinet_power_defaults_by_building || {},
          buildingText,
        )
          ? {
            [buildingText]: clone(handover.review_ui?.cabinet_power_defaults_by_building?.[buildingText] || {}),
          }
          : {},
        footer_inventory_defaults_by_building: Object.prototype.hasOwnProperty.call(
          handover.review_ui?.footer_inventory_defaults_by_building || {},
          buildingText,
        )
          ? {
            [buildingText]: clone(handover.review_ui?.footer_inventory_defaults_by_building?.[buildingText] || {}),
          }
          : {},
        review_link_recipients_by_building: {
          [buildingText]: clone(handover.review_ui?.review_link_recipients_by_building?.[buildingText] || []),
        },
      },
    };
  }

  function collectHandoverReviewRecipientDraftIssues(building = handoverConfigBuilding?.value) {
    const handover = ensureHandoverSegmentConfigShape();
    const buildingText = normalizeHandoverBuildingName(building);
    if (!handover) return [];
    const rawItems = Array.isArray(handover.review_ui?.review_link_recipients_by_building?.[buildingText])
      ? handover.review_ui.review_link_recipients_by_building[buildingText]
      : [];
    const issues = [];
    const seenOpenIds = new Set();
    rawItems.forEach((rawItem, index) => {
      if (!rawItem || typeof rawItem !== "object") return;
      const note = String(rawItem.note || "").trim();
      const openId = String(rawItem.open_id || "").trim();
      if (!note && !openId) return;
      if (!openId) {
        issues.push(`第 ${index + 1} 行缺少 Open ID`);
        return;
      }
      if (seenOpenIds.has(openId)) {
        issues.push(`第 ${index + 1} 行 Open ID 重复`);
        return;
      }
      seenOpenIds.add(openId);
    });
    return issues;
  }

  async function sendHandoverReviewLink(building, options = {}) {
    const buildingText = normalizeHandoverBuildingName(building);
    const batchKey = String(options?.batchKey || health.handover?.review_status?.batch_key || "").trim();
    const actionKey = `${ACTION_KEY_HANDOVER_REVIEW_LINK_SEND_PREFIX}${batchKey || "manual-test"}:${buildingText}`;
    const runner = async () => {
      try {
        const data = await startJsonJobApi("/api/jobs/handover/review-link/send", {
          batch_key: batchKey,
          building: buildingText,
        });
        message.value = `${buildingText}审核链接测试发送任务已提交`;
        refreshRoleScopedRuntimeStatus("handover_review_link_send", { includePendingResume: true });
        return data;
      } catch (err) {
        message.value = `发送${buildingText}审核链接测试消息失败: ${err}`;
        throw err;
      }
    };
    return runSingleFlight(actionKey, runner, { cooldownMs: 300 });
  }

  async function fetchHandoverCommonConfigSegment(options = {}) {
    const requestSeq = ++handoverCommonSegmentRequestSeq;
    try {
      const data = await getHandoverCommonConfigSegmentApi();
      if (requestSeq !== handoverCommonSegmentRequestSeq) {
        return null;
      }
      if (handoverConfigCommonRevision) {
        handoverConfigCommonRevision.value = Number.parseInt(String(data?.revision || 0), 10) || 0;
      }
      if (handoverConfigCommonUpdatedAt) {
        handoverConfigCommonUpdatedAt.value = String(data?.updated_at || "").trim();
      }
      applyHandoverCommonSegmentData(data?.data || {});
      return data;
    } catch (err) {
      if (!options?.silentMessage) {
        message.value = `读取交接班公共配置失败: ${err}`;
      }
      return null;
    }
  }

  async function fetchHandoverBuildingConfigSegment(building = handoverConfigBuilding?.value, options = {}) {
    const buildingText = normalizeHandoverBuildingName(building);
    const buildingCode = normalizeHandoverBuildingCode(buildingText);
    const requestSeq = ++handoverBuildingSegmentRequestSeq;
    try {
      const data = await getHandoverBuildingConfigSegmentApi(buildingCode);
      if (requestSeq !== handoverBuildingSegmentRequestSeq) {
        return null;
      }
      withConfigSaveSuspended(() => {
        if (handoverConfigBuilding) {
          handoverConfigBuilding.value = buildingText;
        }
        if (handoverConfigBuildingRevision) {
          handoverConfigBuildingRevision.value = Number.parseInt(String(data?.revision || 0), 10) || 0;
        }
        if (handoverConfigBuildingUpdatedAt) {
          handoverConfigBuildingUpdatedAt.value = String(data?.updated_at || "").trim();
        }
        if (handoverRuleScope && String(handoverRuleScope.value || "").trim() !== "default") {
          handoverRuleScope.value = buildingText;
        }
        applyHandoverBuildingSegmentData(buildingText, data?.data || {});
      });
      return data;
    } catch (err) {
      if (!options?.silentMessage) {
        message.value = `读取${buildingText}交接班配置失败: ${err}`;
      }
      return null;
    }
  }

  async function fetchConfig(options = {}) {
    if (configRequestInFlight) return configRequestInFlight;
    const silentMessage = Boolean(options?.silentMessage);
    const applyToDraft = options?.applyToDraft !== false;
    const loadHandoverSegments = Boolean(options?.loadHandoverSegments);
    configRequestInFlight = (async () => {
      try {
        const data = await getConfigApi();
        serverConfigSnapshot = clone(data || {});
        if (applyToDraft) {
          const normalized = ensureConfigShape(convertV3ConfigToLegacy(data || {}));
          withConfigSaveSuspended(() => {
            hydrateConfigView(normalized);
            setLastSavedSignatureFromPreparedPayload();
          });
        }
        clearConfigRetryTimer();
        if (configLoaded) {
          configLoaded.value = true;
        }
        if (configLoadError) {
          configLoadError.value = "";
        }
        if (applyToDraft && configSaveStatus && typeof configSaveStatus === "object") {
          configSaveStatus.mode = "idle";
          configSaveStatus.last_error = "";
        }
        if (applyToDraft && loadHandoverSegments) {
          void fetchHandoverCommonConfigSegment({ silentMessage: true });
          void fetchHandoverBuildingConfigSegment(handoverConfigBuilding?.value, { silentMessage: true });
        }
        return true;
      } catch (err) {
        if (configLoadError) {
          configLoadError.value = String(err || "").trim();
        }
        const statusCode = Number.parseInt(String(err?.httpStatus || 0), 10) || 0;
        if (!(statusCode === 409 && !health.runtime_activated)) {
          scheduleConfigRetry();
        }
        if (!silentMessage) {
          message.value = `读取配置失败: ${err}`;
        }
        return false;
      } finally {
        configRequestInFlight = null;
      }
    })();
    return configRequestInFlight;
  }

  async function saveHandoverCommonConfig(options = {}) {
    const runner = async () => {
      try {
        const data = await putHandoverCommonConfigSegmentApi({
          base_revision: Number.parseInt(String(handoverConfigCommonRevision?.value || 0), 10) || 0,
          data: buildHandoverCommonSegmentPayload(),
        });
        withConfigSaveSuspended(() => {
          if (handoverConfigCommonRevision) {
            handoverConfigCommonRevision.value = Number.parseInt(String(data?.revision || 0), 10) || 0;
          }
          if (handoverConfigCommonUpdatedAt) {
            handoverConfigCommonUpdatedAt.value = String(data?.updated_at || "").trim();
          }
          applyHandoverCommonSegmentData(data?.data || {});
        });
        syncServerConfigSnapshotHandoverSegment();
        if (!options?.silentSuccess) {
          message.value = "交接班公共配置已保存";
        }
        return { saved: true, reason: "saved", data };
      } catch (err) {
        if (Number.parseInt(String(err?.httpStatus || 0), 10) === 409) {
          await fetchHandoverCommonConfigSegment({ silentMessage: true });
          if (!options?.silentConflictMessage) {
            message.value = "交接班公共配置已被其他人修改，请刷新后重试";
          }
          return { saved: false, reason: "conflict", error: String(err || "") };
        }
        if (!options?.silentErrorMessage) {
          message.value = `保存交接班公共配置失败: ${err}`;
        }
        return { saved: false, reason: "error", error: String(err || "") };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_CONFIG_COMMON_SAVE, runner, { cooldownMs: 300 });
    }
    return runner();
  }

  async function saveHandoverReviewBaseUrlQuickConfig(options = {}) {
    const handover = ensureHandoverSegmentConfigShape();
    if (!handover) {
      return { saved: false, reason: "missing_config" };
    }
    const normalized = normalizeHandoverReviewBaseUrlInput(handover.review_ui?.public_base_url || "");
    if (!normalized.ok) {
      if (!options?.silentMessage) {
        message.value = normalized.error;
      }
      return { saved: false, reason: "invalid", error: normalized.error };
    }
    const normalizedBaseUrl = normalized.value;
    handover.review_ui.public_base_url = normalizedBaseUrl;
    const currentSavedBaseUrl = String(health.handover?.review_base_url || "").trim();
    if (normalizedBaseUrl === currentSavedBaseUrl) {
      if (!options?.silentNoChange) {
        message.value = normalizedBaseUrl
          ? `审核访问地址已保持为：${normalizedBaseUrl}`
          : "审核访问地址已清空";
      }
      return { saved: true, reason: "unchanged", value: normalizedBaseUrl };
    }
    const runner = async () => {
      try {
        const data = await putHandoverCommonConfigSegmentApi({
          base_revision: Number.parseInt(String(handoverConfigCommonRevision?.value || 0), 10) || 0,
          data: {
            review_ui: {
              public_base_url: normalizedBaseUrl,
            },
          },
        });
        if (handoverConfigCommonRevision) {
          handoverConfigCommonRevision.value = Number.parseInt(String(data?.revision || 0), 10) || 0;
        }
        if (handoverConfigCommonUpdatedAt) {
          handoverConfigCommonUpdatedAt.value = String(data?.updated_at || "").trim();
        }
        const savedBaseUrl = String(data?.data?.review_ui?.public_base_url || normalizedBaseUrl).trim();
        handover.review_ui.public_base_url = savedBaseUrl;
        health.handover.review_base_url = savedBaseUrl;
        syncServerConfigSnapshotHandoverSegment();
        if (!options?.silentSuccess) {
          message.value = savedBaseUrl
            ? `审核访问地址已保存：${savedBaseUrl}`
            : "审核访问地址已清空";
        }
        return { saved: true, reason: "saved", value: savedBaseUrl, data };
      } catch (err) {
        if (Number.parseInt(String(err?.httpStatus || 0), 10) === 409) {
          await fetchHandoverCommonConfigSegment({ silentMessage: true });
          if (!options?.silentMessage) {
            message.value = "审核访问地址已被其他人修改，请刷新后重试";
          }
          return { saved: false, reason: "conflict", error: String(err || "") };
        }
        if (!options?.silentMessage) {
          message.value = `保存审核访问地址失败: ${err}`;
        }
        return { saved: false, reason: "error", error: String(err || "") };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_REVIEW_BASE_URL_SAVE, runner, { cooldownMs: 300 });
    }
    return runner();
  }

  async function saveHandoverBuildingConfig(building = handoverConfigBuilding?.value, options = {}) {
    const buildingText = normalizeHandoverBuildingName(building);
    const buildingCode = normalizeHandoverBuildingCode(buildingText);
    const runner = async () => {
      const recipientDraftIssues = collectHandoverReviewRecipientDraftIssues(buildingText);
      if (recipientDraftIssues.length) {
        const errorText = `${buildingText}审核链接接收人未填写完整：${recipientDraftIssues.join("；")}`;
        if (!options?.silentErrorMessage) {
          message.value = errorText;
        }
        return { saved: false, reason: "invalid_recipient_draft", error: errorText };
      }
      try {
        const data = await putHandoverBuildingConfigSegmentApi(buildingCode, {
          base_revision: Number.parseInt(String(handoverConfigBuildingRevision?.value || 0), 10) || 0,
          data: buildHandoverBuildingSegmentPayload(buildingText),
        });
        withConfigSaveSuspended(() => {
          if (handoverConfigBuilding) {
            handoverConfigBuilding.value = buildingText;
          }
          if (handoverConfigBuildingRevision) {
            handoverConfigBuildingRevision.value = Number.parseInt(String(data?.revision || 0), 10) || 0;
          }
          if (handoverConfigBuildingUpdatedAt) {
            handoverConfigBuildingUpdatedAt.value = String(data?.updated_at || "").trim();
          }
          applyHandoverBuildingSegmentData(buildingText, data?.data || {});
        });
        syncServerConfigSnapshotHandoverSegment();
        if (!options?.silentSuccess) {
          message.value = `${buildingText}交接班配置已保存`;
        }
        return { saved: true, reason: "saved", data };
      } catch (err) {
        if (Number.parseInt(String(err?.httpStatus || 0), 10) === 409) {
          await fetchHandoverBuildingConfigSegment(buildingText, { silentMessage: true });
          if (!options?.silentConflictMessage) {
            message.value = "当前楼配置已被其他人修改，请刷新后重试";
          }
          return { saved: false, reason: "conflict", error: String(err || "") };
        }
        if (!options?.silentErrorMessage) {
          message.value = `保存${buildingText}交接班配置失败: ${err}`;
        }
        return { saved: false, reason: "error", error: String(err || "") };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_CONFIG_BUILDING_SAVE, runner, { cooldownMs: 300 });
    }
    return runner();
  }

  async function saveConfigInternal({
    auto = false,
    skipPostSaveHealthRefresh = false,
    responseMode = "",
    skipHydrateOnSuccess = false,
    silentErrorMessage = false,
  } = {}) {
    const payloadState = buildPreparedSavePayload();
    if (!payloadState.ok) {
      if (!auto && !silentErrorMessage) {
        message.value = payloadState.error || "配置校验失败";
      }
      return {
        saved: false,
        reason: "invalid",
        restartRequired: false,
        error: payloadState.error || "配置校验失败",
      };
    }
    const { v3Payload, signature } = payloadState;
    const requestPayload = mergeConfigWithServerSnapshot(serverConfigSnapshot, v3Payload);
    if (auto && signature && signature === lastSavedConfigSignature) {
      syncConfigSaveSignature(signature, { pending: true });
      return { saved: false, reason: "unchanged", restartRequired: false, signature };
    }

    try {
      const normalizedResponseMode = String(responseMode || "").trim().toLowerCase();
      const finalRequestPayload = normalizedResponseMode
        ? {
            ...requestPayload,
            _meta: {
              ...(requestPayload?._meta && typeof requestPayload._meta === "object" ? requestPayload._meta : {}),
              response_mode: normalizedResponseMode,
            },
          }
        : requestPayload;
      const data = await putConfigApi(finalRequestPayload);
      const returnedConfig = data?.config && typeof data.config === "object" ? data.config : requestPayload;
      serverConfigSnapshot = clone(returnedConfig);
      lastSavedConfigSignature = signature || "";
      syncConfigSaveSignature(lastSavedConfigSignature, { pending: true });
      if (!skipHydrateOnSuccess) {
        const normalized = ensureConfigShape(convertV3ConfigToLegacy(returnedConfig));
        withConfigSaveSuspended(() => {
          hydrateConfigView(normalized);
          setLastSavedSignatureFromPreparedPayload();
        });
      }
      if (configLoaded) {
        configLoaded.value = true;
      }
      if (engineerDirectoryLoaded) {
        engineerDirectoryLoaded.value = false;
      }
      clearEngineerDirectoryCache();
      applyHandoverReviewAccessSnapshot(data?.handover_review_access);
      if (!skipPostSaveHealthRefresh) {
        refreshRoleScopedRuntimeStatus("config_saved");
        scheduleEngineerDirectoryPrefetch(0);
      }
      if (!auto) {
        const warnings = Array.isArray(data?.warnings) ? data.warnings.filter(Boolean) : [];
        const restartNote = Boolean(data?.restart_required) ? "；角色/共享桥接配置需重启后完全生效" : "";
        message.value = warnings.length
          ? `配置已保存（${warnings.join("；")}）${restartNote}`
          : `配置已保存${restartNote}`;
      }
      return {
        saved: true,
        reason: "saved",
        restartRequired: Boolean(data?.restart_required),
        signature: lastSavedConfigSignature,
        warnings: Array.isArray(data?.warnings) ? data.warnings.filter(Boolean) : [],
        data,
      };
    } catch (err) {
      if (Number.parseInt(String(err?.httpStatus || 0), 10) === 409) {
        await fetchConfig({
          silentMessage: true,
          applyToDraft: false,
          loadHandoverSegments: false,
        });
        if (!auto && !silentErrorMessage) {
          message.value = `保存配置失败: ${String(err || "").trim() || "配置已被其他操作更新，请稍后重试"}`;
        }
        return { saved: false, reason: "conflict", error: String(err), restartRequired: false, signature };
      }
      if (!auto && !silentErrorMessage) {
        message.value = `保存配置失败: ${err}`;
      }
      return { saved: false, reason: "error", error: String(err), restartRequired: false, signature };
    }
  }

  async function savePartialConfig(requestPayload, options = {}) {
    if (!requestPayload || typeof requestPayload !== "object" || Array.isArray(requestPayload)) {
      if (!options?.silentErrorMessage) {
        message.value = "配置未加载，无法保存";
      }
      return { saved: false, reason: "missing_config", restartRequired: false, error: "配置未加载，无法保存" };
    }

    const mergedRequestPayload = mergeConfigWithServerSnapshot(serverConfigSnapshot, requestPayload);
    try {
      const data = await putConfigApi(mergedRequestPayload);
      serverConfigSnapshot = clone(data?.config || mergedRequestPayload);
      const normalized = ensureConfigShape(convertV3ConfigToLegacy(data?.config || mergedRequestPayload));
      withConfigSaveSuspended(() => {
        hydrateConfigView(normalized);
        setLastSavedSignatureFromPreparedPayload();
      });
      if (configLoaded) {
        configLoaded.value = true;
      }
      if (engineerDirectoryLoaded) {
        engineerDirectoryLoaded.value = false;
      }
      clearEngineerDirectoryCache();
      applyHandoverReviewAccessSnapshot(data?.handover_review_access);
      if (!options?.skipPostSaveHealthRefresh) {
        refreshRoleScopedRuntimeStatus("config_saved");
        scheduleEngineerDirectoryPrefetch(0);
      }
      if (!options?.silentSuccessMessage) {
        const warnings = Array.isArray(data?.warnings) ? data.warnings.filter(Boolean) : [];
        const restartNote = Boolean(data?.restart_required) ? "；角色/共享桥接配置需重启后完全生效" : "";
        message.value = warnings.length
          ? `配置已保存（${warnings.join("；")}）${restartNote}`
          : `配置已保存${restartNote}`;
      }
      return {
        saved: true,
        reason: "saved",
        restartRequired: Boolean(data?.restart_required),
        data,
      };
    } catch (err) {
      if (Number.parseInt(String(err?.httpStatus || 0), 10) === 409) {
        await fetchConfig({
          silentMessage: true,
          applyToDraft: false,
          loadHandoverSegments: false,
        });
        if (!options?.silentErrorMessage) {
          message.value = `保存配置失败: ${String(err || "").trim() || "配置已被其他操作更新，请稍后重试"}`;
        }
        return { saved: false, reason: "conflict", error: String(err), restartRequired: false };
      }
      if (!options?.silentErrorMessage) {
        message.value = `保存配置失败: ${err}`;
      }
      return { saved: false, reason: "error", error: String(err), restartRequired: false };
    }
  }

  async function saveConfig(options = {}) {
    const runner = async () => saveConfigInternal({
      auto: false,
      skipPostSaveHealthRefresh: Boolean(options?.skipPostSaveHealthRefresh),
      responseMode: String(options?.responseMode || ""),
      skipHydrateOnSuccess: Boolean(options?.skipHydrateOnSuccess),
      silentErrorMessage: Boolean(options?.silentErrorMessage),
    });
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_SAVE_CONFIG, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function repairDayMetricUploadConfig() {
    const runner = async () => {
      try {
        const data = await repairDayMetricUploadConfigApi();
        serverConfigSnapshot = clone(data?.config || serverConfigSnapshot || config.value || {});
        const normalized = ensureConfigShape(convertV3ConfigToLegacy(data?.config || serverConfigSnapshot || {}));
        withConfigSaveSuspended(() => {
          hydrateConfigView(normalized);
          setLastSavedSignatureFromPreparedPayload();
        });
        if (configLoaded) {
          configLoaded.value = true;
        }
        if (engineerDirectoryLoaded) {
          engineerDirectoryLoaded.value = false;
        }
        clearEngineerDirectoryCache();
        applyHandoverReviewAccessSnapshot(data?.handover_review_access);
        refreshRoleScopedRuntimeStatus("day_metric_config_repaired");
        scheduleEngineerDirectoryPrefetch(0);
        const notes = Array.isArray(data?.notes) ? data.notes.filter(Boolean) : [];
        if (data?.repaired) {
          message.value = notes.length ? `12项配置已修复（${notes.join("；")}）` : "12项配置已修复";
        } else {
          message.value = "12项配置检查完成，无需修复";
        }
        return data;
      } catch (err) {
        message.value = `修复12项配置失败: ${err}`;
        throw err;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_DAY_METRIC_CONFIG_REPAIR, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function checkUpdaterNow(options = {}) {
    const autoApplyIfAvailable = Boolean(options?.autoApplyIfAvailable);
    const runner = async () => {
      if (autoApplyIfAvailable) {
        pauseRuntimeTraffic();
        setUpdaterOverlay(true, {
          title: "正在检查更新",
          subtitle: "正在确认是否存在可用更新，请保持当前页面打开。",
          stage: "applying",
          kicker: "更新准备中",
        });
        await flushUpdaterOverlayPaint();
      }
      try {
        const data = await checkUpdaterApi();
        const result = data?.result || {};
        const runtimeAfterCheck =
          data?.runtime && typeof data.runtime === "object" ? data.runtime : {};
        Object.assign(health.updater, runtimeAfterCheck, {
          last_result: String(
            result?.last_result
              || runtimeAfterCheck?.last_result
              || health.updater?.last_result
              || "",
          ),
        });
        const updateAvailableAfterCheck = Boolean(
          result?.update_available
          || runtimeAfterCheck?.update_available
          || result?.force_apply_available
          || runtimeAfterCheck?.force_apply_available,
        );
        const restartRequiredAfterCheck = Boolean(
          result?.restart_required ?? runtimeAfterCheck?.restart_required ?? health.updater?.restart_required,
        );
        if (
          autoApplyIfAvailable
          && !restartRequiredAfterCheck
          && updateAvailableAfterCheck
        ) {
          await applyUpdaterPatch();
          return data;
        }
        await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
        const resultKey = String(result?.last_result || "").trim();
        if (resultKey === "failed") {
          if (autoApplyIfAvailable) {
            hideUpdaterOverlay();
          }
          message.value = `更新检查失败: ${mapUpdaterResultText(resultKey)}`;
        } else if (resultKey === "mirror_pending_publish" || resultKey === "ahead_of_mirror") {
          if (autoApplyIfAvailable) {
            hideUpdaterOverlay();
          }
          message.value = buildUpdaterApplyMessage(result);
        } else {
          if (autoApplyIfAvailable) {
            hideUpdaterOverlay();
          }
          // 成功时不占用全局提示区，避免与顶部版本状态重复显示
          message.value = "";
        }
      } catch (err) {
        if (autoApplyIfAvailable) {
          hideUpdaterOverlay();
        }
        message.value = `手动检查更新失败: ${err}`;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_UPDATER_CHECK, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function applyUpdaterPatch() {
    const isGitPullMode = String(health.updater?.update_mode || "").trim().toLowerCase() === "git_pull";
    const requestedMode = String(
      health.updater?.queued_apply?.queued && String(health.updater?.queued_apply?.mode || "").trim()
        ? health.updater.queued_apply.mode
        : health.updater?.update_available
          ? "normal"
          : health.updater?.force_apply_available
            ? "force_remote"
            : "normal",
    ).trim();
    const runner = async () => {
      let requestPending = true;
      pauseRuntimeTraffic();
      setUpdaterOverlay(true, {
        title: isGitPullMode ? "正在拉取代码" : "正在更新程序",
        subtitle: isGitPullMode
          ? "正在从 Git 仓库拉取最新代码，请保持当前页面打开。"
          : "请保持当前页面打开，更新完成后会自动恢复。",
        stage: "applying",
        kicker: isGitPullMode ? "Git 拉取中" : "",
      });
      await flushUpdaterOverlayPaint();
      if (!isGitPullMode) {
        persistUpdaterRecoveryIntent("applying", "updater_apply");
        startUpdaterBootstrapRecoveryWatch({
          source: "updater_apply",
          startupToken: String(health?.startup_time || "").trim(),
        });
        startUpdaterRuntimeMonitor({
          queued: false,
          initialGraceMs: 15000,
          isRequestPending: () => requestPending,
        });
      }
      try {
        const data = await applyUpdaterApi({
          mode: requestedMode,
          queue_if_busy: true,
        });
        requestPending = false;
        const result = data?.result || {};
        Object.assign(health.updater, data?.runtime || {}, {
          last_result: String(result?.last_result || health.updater.last_result || ""),
        });
        message.value = buildUpdaterApplyMessage(result);
        const finalResult = String(result?.last_result || "").trim();
        if (finalResult === "queued_busy") {
          if (!isGitPullMode) {
            startQueuedUpdaterMonitor();
          }
          return data;
        }
        if (finalResult === "updated_restart_scheduled") {
          if (!isGitPullMode) {
            beginUpdaterRestartRecovery({ source: "updater_apply" });
          }
          return data;
        }
        await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
        hideUpdaterOverlay();
        return data;
      } catch (err) {
        requestPending = false;
        if (!isGitPullMode && isTransientNetworkError(err)) {
          return handoffUpdaterToRestartRecovery("更新已开始，服务正在重启，正在等待恢复。", {
            source: "updater_apply",
          });
        }
        hideUpdaterOverlay();
        message.value = `${isGitPullMode ? "拉取代码失败" : "应用更新失败"}: ${err}`;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_UPDATER_APPLY, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function restartUpdaterApp() {
    const runner = async () => {
      let requestPending = true;
      pauseRuntimeTraffic();
      setUpdaterOverlay(true, {
        title: "正在重启程序",
        subtitle: "请保持当前页面打开，服务恢复后会自动刷新。",
        stage: "restarting",
      });
      persistUpdaterRecoveryIntent("restarting", "updater_restart");
      await flushUpdaterOverlayPaint();
      startUpdaterBootstrapRecoveryWatch({
        source: "updater_restart",
        startupToken: String(health?.startup_time || "").trim(),
        kicker: "程序重启恢复中",
        reloadSubtitle: "正在刷新当前页面并恢复当前系统。",
      });
      startUpdaterRuntimeMonitor({
        queued: false,
        initialGraceMs: 8000,
        isRequestPending: () => requestPending,
      });
      try {
        const data = await restartUpdaterApi();
        requestPending = false;
        const result = data?.result || {};
        Object.assign(health.updater, data?.runtime || {}, {
          last_result: String(result?.last_result || health.updater.last_result || ""),
        });
        message.value = buildUpdaterApplyMessage(result?.last_result ? result : "updated_restart_scheduled");
        beginUpdaterRestartRecovery({ source: "updater_restart" });
        return data;
      } catch (err) {
        requestPending = false;
        if (isTransientNetworkError(err)) {
          return handoffUpdaterToRestartRecovery("重启已触发，正在等待服务恢复。", {
            source: "updater_restart",
          });
        }
        hideUpdaterOverlay();
        message.value = `触发重启失败: ${err}`;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_UPDATER_RESTART, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function triggerInternalPeerUpdaterCheck() {
    const runner = async () => {
      try {
        const data = await triggerInternalPeerUpdaterCheckApi();
        if (data?.runtime && typeof data.runtime === "object") {
          Object.assign(health.updater, data.runtime);
        }
        const result = data?.result || {};
        message.value = String(result?.message || "").trim() || "已下发内网端检查更新命令。";
        try {
          await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
        } catch (_err) {
          // ignore transient fetch failures; command submission already succeeded
        }
        return data;
      } catch (err) {
        message.value = `下发内网端检查更新失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_UPDATER_INTERNAL_PEER_CHECK, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function triggerInternalPeerUpdaterApply() {
    const runner = async () => {
      try {
        const data = await triggerInternalPeerUpdaterApplyApi();
        if (data?.runtime && typeof data.runtime === "object") {
          Object.assign(health.updater, data.runtime);
        }
        const result = data?.result || {};
        message.value = String(result?.message || "").trim() || "已下发内网端开始更新命令。";
        try {
          await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
        } catch (_err) {
          // ignore transient fetch failures; command submission already succeeded
        }
        return data;
      } catch (err) {
        message.value = `下发内网端开始更新失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_UPDATER_INTERNAL_PEER_APPLY, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function restartApplication(options = {}) {
    const runner = async () => {
      const kicker = String(options?.kicker || "角色切换中");
      const title = String(options?.title || "正在切换角色并重启程序");
      const subtitle = String(options?.subtitle || "请保持当前页面打开，服务恢复后会自动刷新。");
      pauseRuntimeTraffic();
      setUpdaterOverlay(true, {
        title,
        subtitle,
        stage: "restarting",
        kicker,
      });
      persistUpdaterRecoveryIntent("restarting", String(options?.source || "manual_restart"), options);
      await flushUpdaterOverlayPaint();
      startUpdaterBootstrapRecoveryWatch({
        source: String(options?.source || "manual_restart"),
        startupToken: String(health?.startup_time || "").trim(),
        kicker,
        targetRoleMode: String(options?.targetRoleMode || "").trim().toLowerCase(),
        reloadTitle: String(options?.reloadTitle || "服务已恢复"),
        reloadSubtitle: String(options?.reloadSubtitle || "正在刷新当前页面并接入新的运行角色。"),
      });
      try {
        const data = await restartAppApi({
          source: String(options?.source || "manual").trim() || "manual",
          reason: String(options?.reason || "").trim(),
          target_role_mode: String(options?.targetRoleMode || "").trim().toLowerCase(),
        });
        message.value = String(options?.message || "角色切换已提交，正在等待服务恢复。");
        beginUpdaterRestartRecovery({
          kicker,
          title,
          subtitle,
          targetRoleMode: String(options?.targetRoleMode || "").trim().toLowerCase(),
          reloadTitle: String(options?.reloadTitle || "服务已恢复"),
          reloadSubtitle: String(options?.reloadSubtitle || "正在刷新当前页面并接入新的运行角色。"),
        });
        return data;
      } catch (err) {
        if (isTransientNetworkError(err)) {
          return handoffUpdaterToRestartRecovery(
            String(options?.message || "角色切换已提交，正在等待服务恢复。"),
            {
              kicker,
              title,
              subtitle,
              targetRoleMode: String(options?.targetRoleMode || "").trim().toLowerCase(),
              reloadTitle: String(options?.reloadTitle || "服务已恢复"),
              reloadSubtitle: String(options?.reloadSubtitle || "正在刷新当前页面并接入新的运行角色。"),
            },
          );
        }
        hideUpdaterOverlay();
        message.value = `触发程序重启失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_APP_RESTART, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function activateStartupRuntime(options = {}) {
    try {
      const payload = {
        source: String(options?.source || "").trim() || "启动角色确认",
        startup_handoff_nonce: String(options?.startupHandoffNonce || "").trim(),
      };
      const roleMode = String(options?.roleMode || options?.role_mode || "").trim();
      if (roleMode) {
        payload.role_mode = roleMode;
      }
      const sharedBridge = options?.sharedBridge || options?.shared_bridge;
      if (sharedBridge && typeof sharedBridge === "object" && !Array.isArray(sharedBridge)) {
        payload.shared_bridge = sharedBridge;
      }
      const data = await activateStartupRuntimeApi(payload);
      return {
        ok: data?.ok !== false,
        activated: Boolean(data?.activated),
        alreadyActive: Boolean(data?.already_active),
        roleMode: String(data?.role_mode || "").trim(),
        savedRole: data?.saved_role && typeof data.saved_role === "object" ? data.saved_role : null,
        phase: String(data?.phase || "").trim(),
        error: String(data?.error || "").trim(),
      };
    } catch (err) {
      return {
        ok: false,
        activated: false,
        alreadyActive: false,
        roleMode: "",
        savedRole: null,
        phase: "failed",
        error: String(err || "").trim() || "激活后台运行时失败",
      };
    }
  }

  async function exitCurrentRuntime(options = {}) {
    try {
      const data = await exitCurrentRuntimeApi({
        source: String(options?.source || "").trim() || "退出当前系统",
      });
      return {
        ok: data?.ok !== false,
        deactivated: Boolean(data?.deactivated),
        roleMode: String(data?.role_mode || "").trim(),
        error: String(data?.error || "").trim(),
      };
    } catch (err) {
      return {
        ok: false,
        deactivated: false,
        roleMode: "",
        error: String(err || "").trim() || "退出当前系统失败",
      };
    }
  }

  async function confirmAllHandoverReview() {

    const runner = async () => {
      const batchKey = String(health.handover?.review_status?.batch_key || "").trim();
      if (!batchKey) {
        message.value = "当前没有可确认的交接班批次";
        return { ok: false, reason: "no_batch" };
      }
      try {
        const data = await confirmAllHandoverReviewBatchApi(batchKey, {});
        const jobId = await focusAcceptedJob(data, "一键全确认任务已提交，请在任务与资源面板查看进度。");
        if (jobId) {
          void (async () => {
            const job = await waitForAcceptedJobCompletion(jobId, { timeoutMs: 15 * 60 * 1000 });
            if (!job) return;
            refreshRoleScopedRuntimeStatus("handover_confirm_all_completed", { includePendingResume: true });
            if (job.status === "success" && shouldPublishAcceptedJobResult(jobId)) {
              message.value = summarizeConfirmAllResult(job?.result || {});
            }
            if (job.status === "failed" && shouldPublishAcceptedJobResult(jobId)) {
              message.value = `一键全确认失败: ${String(job?.error || "请查看系统错误日志").trim() || "请查看系统错误日志"}`;
            }
          })();
        }
        return data;
      } catch (err) {
        message.value = `一键全确认失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_CONFIRM_ALL, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function retryAllFailedHandoverCloudSync() {
    const runner = async () => {
      const batchKey = String(health.handover?.review_status?.batch_key || "").trim();
      if (!batchKey) {
        message.value = "当前没有可重试的交接班批次";
        return { ok: false, reason: "no_batch" };
      }
      try {
        const data = await retryHandoverReviewBatchCloudSyncApi(batchKey);
        const jobId = await focusAcceptedJob(data, "云表批量重试任务已提交，请在任务与资源面板查看进度。");
        if (jobId) {
          void (async () => {
            const job = await waitForAcceptedJobCompletion(jobId, { timeoutMs: 10 * 60 * 1000 });
            if (!job) return;
            refreshRoleScopedRuntimeStatus("handover_cloud_retry_completed", { includePendingResume: true });
            if (job.status === "success" && shouldPublishAcceptedJobResult(jobId)) {
              message.value = summarizeBatchCloudRetryResult(job?.result || {});
            }
            if (job.status === "failed" && shouldPublishAcceptedJobResult(jobId)) {
              message.value = `云表批量重试失败: ${String(job?.error || "请查看系统错误日志").trim() || "请查看系统错误日志"}`;
            }
          })();
        }
        return data;
      } catch (err) {
        message.value = `一键全部重试云表上传失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_CLOUD_RETRY_ALL, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function waitForDailyReportScreenshotAuthReady(options = {}) {
    const timeoutMs = Math.max(1000, Number.parseInt(String(options?.timeoutMs || 15000), 10) || 15000);
    const intervalMs = Math.max(300, Number.parseInt(String(options?.intervalMs || 1500), 10) || 1500);
    const startedAt = Date.now();
    while (Date.now() - startedAt <= timeoutMs) {
      await fetchHandoverDailyReportContext({
        silentTransientNetworkError: true,
        silentMessage: true,
      });
      const status = String(handoverDailyReportContext?.value?.screenshot_auth?.status || "")
        .trim()
        .toLowerCase();
      if (status === "ready" || status === "ready_without_target_page") return true;
      await new Promise((resolve) => window.setTimeout(resolve, intervalMs));
    }
    return false;
  }

  async function openHandoverDailyReportScreenshotAuth() {
    const runner = async () => {
      const dutyDate = String(handoverDutyDate?.value || "").trim();
      const dutyShift = String(handoverDutyShift?.value || "").trim().toLowerCase();
      if (!dutyDate || !["day", "night"].includes(dutyShift)) {
        message.value = "请先选择有效的交接班日期和班次。";
        return { ok: false, reason: "invalid_duty_context" };
      }
      try {
        const data = await openHandoverDailyReportScreenshotAuthApi({
          duty_date: dutyDate,
          duty_shift: dutyShift,
        });
        const jobId = await focusAcceptedJob(data, "飞书截图登录态初始化任务已提交，请在任务与资源面板查看进度。");
        if (jobId) {
          void (async () => {
            const job = await waitForAcceptedJobCompletion(jobId, { timeoutMs: 60 * 1000 });
            await fetchHandoverDailyReportContext({
              silentTransientNetworkError: true,
              silentMessage: true,
            });
            const ready = await waitForDailyReportScreenshotAuthReady({ timeoutMs: 15000, intervalMs: 1500 });
            if (!shouldPublishAcceptedJobResult(jobId)) return;
            if (ready) {
              message.value = "飞书截图登录态已就绪";
            } else if (job?.status === "failed") {
              message.value = `飞书截图登录态初始化失败: ${String(job?.error || "请查看系统错误日志").trim() || "请查看系统错误日志"}`;
            } else {
              message.value = String(job?.result?.message || "").trim() || "已打开飞书截图登录浏览器，请完成登录。";
            }
          })();
        }
        return data;
      } catch (err) {
        message.value = `打开飞书截图登录浏览器失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_DAILY_REPORT_AUTH_OPEN, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function runHandoverDailyReportScreenshotTest() {
    const runner = async () => {
      const dutyDate = String(handoverDutyDate?.value || "").trim();
      const dutyShift = String(handoverDutyShift?.value || "").trim().toLowerCase();
      if (!dutyDate || !["day", "night"].includes(dutyShift)) {
        message.value = "请先选择有效的交接班日期和班次。";
        return { ok: false, reason: "invalid_duty_context" };
      }
      try {
        const data = await runHandoverDailyReportScreenshotTestApi({
          duty_date: dutyDate,
          duty_shift: dutyShift,
        });
        if (handoverDailyReportLastScreenshotTest) {
          handoverDailyReportLastScreenshotTest.value = {
            batch_key: `${dutyDate}|${dutyShift}`,
            status: "queued",
            tested_at: new Date().toISOString(),
            summary_sheet_image: { status: "", error: "", path: "" },
            external_page_image: { status: "", error: "", path: "" },
          };
        }
        const jobId = await focusAcceptedJob(data, "截图测试任务已提交，请在任务与资源面板查看进度。");
        if (jobId) {
          void (async () => {
            const job = await waitForAcceptedJobCompletion(jobId, { timeoutMs: 2 * 60 * 1000 });
            if (!job) return;
            const result = job?.result && typeof job.result === "object" ? job.result : {};
            if (handoverDailyReportLastScreenshotTest) {
              handoverDailyReportLastScreenshotTest.value = {
                batch_key: String(result?.batch_key || `${dutyDate}|${dutyShift}`).trim(),
                status: String(result?.status || (job.status === "failed" ? "failed" : "")).trim().toLowerCase(),
                tested_at: new Date().toISOString(),
                summary_sheet_image:
                  result?.summary_sheet_image && typeof result.summary_sheet_image === "object"
                    ? { ...result.summary_sheet_image }
                    : { status: job.status === "failed" ? "failed" : "", error: String(job?.error || ""), path: "" },
                external_page_image:
                  result?.external_page_image && typeof result.external_page_image === "object"
                    ? { ...result.external_page_image }
                    : { status: job.status === "failed" ? "failed" : "", error: String(job?.error || ""), path: "" },
              };
            }
            await fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
            if (!shouldPublishAcceptedJobResult(jobId)) return;
            const status = String(result?.status || "").trim().toLowerCase();
            if (job.status === "failed") {
              message.value = "截图测试失败，请查看系统错误日志后重试。";
            } else if (status === "ok") {
              message.value = "截图测试完成";
            } else if (status === "partial_failed") {
              message.value = "截图测试部分成功，请查看截图状态和系统日志";
            } else {
              message.value = "截图测试失败，请查看系统日志";
            }
          })();
        }
        return data;
      } catch (err) {
        if (handoverDailyReportLastScreenshotTest) {
          handoverDailyReportLastScreenshotTest.value = {
            batch_key: `${dutyDate}|${dutyShift}`,
            status: "failed",
            tested_at: new Date().toISOString(),
            summary_sheet_image: {
              status: "failed",
              error: String(err || ""),
              path: "",
            },
            external_page_image: {
              status: "failed",
              error: String(err || ""),
              path: "",
            },
          };
        }
        message.value = isAbortError(err)
          ? "截图测试超时，请查看系统错误日志后重试。"
          : "截图测试失败，请查看系统错误日志后重试。";
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_DAILY_REPORT_SCREENSHOT_TEST, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function recaptureHandoverDailyReportAsset(target) {
    const targetText = String(target || "").trim().toLowerCase();
    const runner = async () => {
      const payload = buildDailyReportAssetActionPayload(targetText);
      if (!payload.duty_date || !["day", "night"].includes(payload.duty_shift)) {
        message.value = "请先选择有效的交接班日期和班次。";
        return { ok: false, reason: "invalid_duty_context" };
      }
      try {
        const data = await recaptureHandoverDailyReportAssetApi(payload);
        const resultStatus = String(data?.result?.status || "").trim().toLowerCase();
        const label = getDailyReportTargetLabel(targetText);
        const jobId = await focusAcceptedJob(data, `${label}重新截图任务已提交，请在任务与资源面板查看进度。`);
        if (jobId) {
          void (async () => {
            const job = await waitForAcceptedJobCompletion(jobId, { timeoutMs: 2 * 60 * 1000 });
            if (!job) return;
            await fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
            if (!shouldPublishAcceptedJobResult(jobId)) return;
            const jobResult = job?.result && typeof job.result === "object" ? job.result : {};
            const finalStatus = String(jobResult?.result?.status || resultStatus || "").trim().toLowerCase();
            if (job.status === "failed") {
              message.value = `${label}重新截图失败，请查看系统错误日志后重试。`;
            } else if (finalStatus === "ok") {
              message.value = `${label}已重新截图`;
            } else {
              message.value = resolveDailyReportCaptureFailureMessage(label, jobResult?.result || {});
            }
          })();
        }
        return data;
      } catch (err) {
        const label = getDailyReportTargetLabel(targetText);
        message.value = isAbortError(err)
          ? `${label}重新截图超时，请查看系统错误日志后重试。`
          : `${label}重新截图失败，请查看系统错误日志后重试。`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(`${ACTION_KEY_HANDOVER_DAILY_REPORT_RECAPTURE_PREFIX}${targetText}`, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function uploadHandoverDailyReportAsset(target, fileLike, fileName = "") {
    const targetText = String(target || "").trim().toLowerCase();
    const runner = async () => {
      const payload = buildDailyReportAssetActionPayload(targetText);
      if (!payload.duty_date || !["day", "night"].includes(payload.duty_shift)) {
        message.value = "请先选择有效的交接班日期和班次。";
        return { ok: false, reason: "invalid_duty_context" };
      }
      if (!(fileLike instanceof Blob)) {
        message.value = "未检测到可上传的图片";
        return { ok: false, reason: "missing_file" };
      }
      const form = new FormData();
      form.append("duty_date", payload.duty_date);
      form.append("duty_shift", payload.duty_shift);
      form.append("target", payload.target);
      form.append("file", fileLike, fileName || "clipboard.png");
      try {
        const data = await uploadHandoverDailyReportAssetApi(form);
        await fetchHandoverDailyReportContext();
        closeHandoverDailyReportUploadDialog();
        message.value = `${getDailyReportTargetLabel(targetText)}已替换为手工图`;
        return data;
      } catch (err) {
        message.value = `上传截图失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(`${ACTION_KEY_HANDOVER_DAILY_REPORT_UPLOAD_PREFIX}${targetText}`, runner, { cooldownMs: 300 });
    }
    return runner();
  }

  async function restoreHandoverDailyReportAutoAsset(target) {
    const targetText = String(target || "").trim().toLowerCase();
    const runner = async () => {
      const payload = buildDailyReportAssetActionPayload(targetText);
      if (!payload.duty_date || !["day", "night"].includes(payload.duty_shift)) {
        message.value = "请先选择有效的交接班日期和班次。";
        return { ok: false, reason: "invalid_duty_context" };
      }
      try {
        const data = await restoreHandoverDailyReportManualAssetApi(payload);
        await fetchHandoverDailyReportContext();
        message.value = `${getDailyReportTargetLabel(targetText)}已恢复自动图`;
        return data;
      } catch (err) {
        message.value = `恢复自动图失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(`${ACTION_KEY_HANDOVER_DAILY_REPORT_RESTORE_PREFIX}${targetText}`, runner, { cooldownMs: 300 });
    }
    return runner();
  }

  async function rewriteHandoverDailyReportRecord() {
    const runner = async () => {
      const dutyDate = String(handoverDutyDate?.value || "").trim();
      const dutyShift = String(handoverDutyShift?.value || "").trim().toLowerCase();
      if (!dutyDate || !["day", "night"].includes(dutyShift)) {
        message.value = "请先选择有效的交接班日期和班次。";
        return { ok: false, reason: "invalid_duty_context" };
      }
      try {
        const data = await rewriteHandoverDailyReportRecordApi({
          duty_date: dutyDate,
          duty_shift: dutyShift,
        });
        const jobId = await focusAcceptedJob(data, "日报多维重写任务已提交，请在任务与资源面板查看进度。");
        if (jobId) {
          void (async () => {
            const job = await waitForAcceptedJobCompletion(jobId, { timeoutMs: 2 * 60 * 1000 });
            if (!job) return;
            await fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
            if (!shouldPublishAcceptedJobResult(jobId)) return;
            if (job.status === "failed") {
              message.value = "重写日报多维记录失败，请查看系统错误日志。";
              return;
            }
            message.value = resolveDailyReportRewriteMessage(job?.result || {});
          })();
        }
        return data;
      } catch (err) {
        message.value = "重写日报多维记录失败，请查看系统错误日志。";
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_DAILY_REPORT_RECORD_REWRITE, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function reprobeHandoverReviewAccess() {
    const runner = async () => {
      try {
        const saveResult = await saveHandoverReviewBaseUrlQuickConfig({
          silentSuccess: true,
          silentNoChange: true,
        });
        if (!saveResult?.saved) {
          return saveResult;
        }
        const data = await reprobeHandoverReviewAccessApi();
        const snapshot = data?.handover_review_access || {};
        applyHandoverReviewAccessSnapshot(snapshot);
        refreshRoleScopedRuntimeStatus("handover_review_access_reprobe");
        const effectiveBaseUrl = String(snapshot?.review_base_url_effective || "").trim();
        if (effectiveBaseUrl) {
          message.value = `审核访问地址已刷新，当前生效地址：${effectiveBaseUrl}`;
        } else {
          message.value = "审核访问地址已刷新，请先在配置中心填写手工地址。";
        }
        return data;
      } catch (err) {
        message.value = `刷新审核访问地址失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_REVIEW_ACCESS_REPROBE, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  bindHandoverReviewStatusBroadcast();

  return {
    appendLog,
    applyJobPanelSummary,
    patchJobPanelActionState,
    fetchBootstrapHealth,
    fetchExternalDashboardSummary,
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
    fetchInternalRuntimeSummary,
    fetchInternalRuntimeBuildingRuntimeStatus,
    fetchAllInternalBuildingRuntimeStatuses,
    scheduleInternalRuntimeStatusRefresh,
    fetchHandoverDailyReportContext,
    fetchConfig,
    fetchHandoverCommonConfigSegment,
    fetchHandoverBuildingConfigSegment,
    fetchHandoverEngineerDirectory,
    ensureHandoverEngineerDirectoryLoaded,
    scheduleEngineerDirectoryPrefetch,
    saveConfig,
    savePartialConfig,
    getPreparedConfigPayloadState: buildPreparedSavePayload,
    repairDayMetricUploadConfig,
    saveHandoverCommonConfig,
    saveHandoverReviewBaseUrlQuickConfig,
    saveHandoverBuildingConfig,
    activateStartupRuntime,
    exitCurrentRuntime,
    restartApplication,
    checkUpdaterNow,
    applyUpdaterPatch,
    restartUpdaterApp,
    resumeUpdaterRecoveryIfNeeded,
    triggerInternalPeerUpdaterCheck,
    triggerInternalPeerUpdaterApply,
    confirmAllHandoverReview,
    retryAllFailedHandoverCloudSync,
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
    sendHandoverReviewLink,
    buildHandoverDailyReportCaptureAssetUrl,
    getBridgeTaskCancelActionKey,
    getBridgeTaskRetryActionKey,
    getSourceCacheRefreshBuildingActionKey,
    getHandoverDailyReportRecaptureActionKey,
    getHandoverDailyReportUploadActionKey,
    getHandoverDailyReportRestoreActionKey,
    ACTION_KEY_HANDOVER_CONFIRM_ALL,
    ACTION_KEY_HANDOVER_CLOUD_RETRY_ALL,
    ACTION_KEY_HANDOVER_DAILY_REPORT_AUTH_OPEN,
    ACTION_KEY_SOURCE_CACHE_REFRESH_ALARM_MANUAL,
    ACTION_KEY_SOURCE_CACHE_DELETE_ALARM_MANUAL,
    ACTION_KEY_SOURCE_CACHE_UPLOAD_ALARM_FULL,
    ACTION_KEY_SOURCE_CACHE_UPLOAD_ALARM_BUILDING,
    ACTION_KEY_HANDOVER_CONFIG_COMMON_SAVE,
    ACTION_KEY_HANDOVER_CONFIG_BUILDING_SAVE,
    ACTION_KEY_DAY_METRIC_CONFIG_REPAIR,
    ACTION_KEY_HANDOVER_REVIEW_BASE_URL_SAVE,
    ACTION_KEY_HANDOVER_REVIEW_LINK_SEND_PREFIX,
    ACTION_KEY_UPDATER_INTERNAL_PEER_CHECK,
    ACTION_KEY_UPDATER_INTERNAL_PEER_APPLY,
    ACTION_KEY_HANDOVER_DAILY_REPORT_SCREENSHOT_TEST,
    ACTION_KEY_HANDOVER_DAILY_REPORT_RECORD_REWRITE,
    ACTION_KEY_HANDOVER_REVIEW_ACCESS_REPROBE,
  };
}






