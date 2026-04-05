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
  checkUpdaterApi,
  getUpdaterStatusApi,
  restartUpdaterApi,
  restartAppApi,
  getBridgeTaskApi,
  getBridgeTasksApi,
  buildHandoverDailyReportCaptureAssetUrl,
  getBootstrapHealthApi,
  getConfigApi,
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
  restoreHandoverDailyReportManualAssetApi,
  retryBridgeTaskApi,
  rewriteHandoverDailyReportRecordApi,
  runHandoverDailyReportScreenshotTestApi,
  uploadHandoverDailyReportAssetApi,
  putConfigApi,
  refreshManualAlarmSourceCacheApi,
  deleteManualAlarmSourceCacheFilesApi,
  uploadAlarmSourceCacheFullApi,
  uploadAlarmSourceCacheBuildingApi,
  openAlarmEventUploadTargetApi,
} from "./api_client.js";
import { prepareConfigPayloadForSave } from "./config_save_validation.js";
import { buildUpdaterApplyMessage, mapUpdaterResultText } from "./updater_text.js";

const ACTION_KEY_SAVE_CONFIG = "config:save";
const ACTION_KEY_APP_RESTART = "app:restart";
const ACTION_KEY_UPDATER_CHECK = "updater:check";
const ACTION_KEY_UPDATER_APPLY = "updater:apply";
const ACTION_KEY_UPDATER_RESTART = "updater:restart";
const ACTION_KEY_HANDOVER_CONFIRM_ALL = "handover_review:confirm_all";
const ACTION_KEY_HANDOVER_CLOUD_RETRY_ALL = "handover_review:cloud_retry_all";
const ACTION_KEY_HANDOVER_DAILY_REPORT_AUTH_OPEN = "handover_daily_report:auth_open";
const ACTION_KEY_HANDOVER_DAILY_REPORT_SCREENSHOT_TEST = "handover_daily_report:screenshot_test";
const ACTION_KEY_HANDOVER_DAILY_REPORT_RECAPTURE_PREFIX = "handover_daily_report:recapture:";
const ACTION_KEY_HANDOVER_DAILY_REPORT_UPLOAD_PREFIX = "handover_daily_report:upload:";
const ACTION_KEY_HANDOVER_DAILY_REPORT_RESTORE_PREFIX = "handover_daily_report:restore:";
const ACTION_KEY_HANDOVER_DAILY_REPORT_RECORD_REWRITE = "handover_daily_report:record_rewrite";
const ACTION_KEY_HANDOVER_REVIEW_ACCESS_REPROBE = "handover_review:access_reprobe";
const ACTION_KEY_BRIDGE_CANCEL_PREFIX = "bridge:cancel:";
const ACTION_KEY_BRIDGE_RETRY_PREFIX = "bridge:retry:";
const ACTION_KEY_SOURCE_CACHE_REFRESH_CURRENT_HOUR = "bridge:source_cache_refresh_current_hour";
const ACTION_KEY_SOURCE_CACHE_REFRESH_ALARM_MANUAL = "bridge:source_cache_refresh_alarm_manual";
const ACTION_KEY_SOURCE_CACHE_DELETE_ALARM_MANUAL = "bridge:source_cache_delete_alarm_manual";
const ACTION_KEY_SOURCE_CACHE_UPLOAD_ALARM_FULL = "bridge:source_cache_upload_alarm_full";
const ACTION_KEY_SOURCE_CACHE_UPLOAD_ALARM_BUILDING = "bridge:source_cache_upload_alarm_building";
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
    handoverDutyDate,
    handoverDutyShift,
    configAutoSaveSuspendDepth,
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
    shouldIncludeHandoverHealthContext,
    shouldLoadEngineerDirectory,
  } = ctx;
  let lastSavedConfigSignature = "";
  let serverConfigSnapshot = null;
  let engineerDirectoryPrefetchTimer = null;
  let healthRequestInFlight = null;
  let dailyReportContextRequestInFlight = null;
  let bridgeTasksRequestInFlight = null;
  let bridgeTaskDetailRequestInFlight = null;
  let updaterReconnectTimer = null;
  let updaterQueueMonitorTimer = null;

  function isUpdaterTrafficPaused() {
    return Boolean(updaterUiOverlayVisible?.value || updaterAwaitingRestartRecovery?.value);
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

  function hideUpdaterOverlay() {
    clearUpdaterReconnectTimer();
    clearUpdaterQueueMonitorTimer();
    if (updaterAwaitingRestartRecovery) updaterAwaitingRestartRecovery.value = false;
    setUpdaterOverlay(false, { title: "", subtitle: "", stage: "", kicker: "" });
    resumeRuntimeTraffic();
  }

  function beginUpdaterRestartRecovery(options = {}) {
    clearUpdaterReconnectTimer();
    clearUpdaterQueueMonitorTimer();
    pauseRuntimeTraffic();
    if (updaterAwaitingRestartRecovery) updaterAwaitingRestartRecovery.value = true;
    setUpdaterOverlay(true, {
      title: String(options?.title || "更新完成，正在重启服务"),
      subtitle: String(options?.subtitle || "请保持当前页面打开。服务恢复后会自动刷新当前页面。"),
      stage: String(options?.stage || "restarting"),
      kicker: String(options?.kicker || ""),
    });

    const poll = async () => {
      const ok = await fetchBootstrapHealth({ silentMessage: true });
      if (ok) {
        if (updaterAwaitingRestartRecovery) updaterAwaitingRestartRecovery.value = false;
        setUpdaterOverlay(true, {
          title: String(options?.reloadTitle || "服务已恢复"),
          subtitle: String(options?.reloadSubtitle || "正在刷新当前页面并接入新版本。"),
          stage: "reloading",
          kicker: String(options?.reloadKicker || options?.kicker || ""),
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
    clearUpdaterQueueMonitorTimer();
    pauseRuntimeTraffic();
    setUpdaterOverlay(true, {
      title: "等待任务结束后自动更新",
      subtitle: "后台任务尚未完成。控制台已暂停轮询和日志流，任务结束后会自动开始更新。",
      stage: "queued",
    });

    const poll = async () => {
      try {
        const data = await getUpdaterStatusApi();
        const runtime = data?.runtime && typeof data.runtime === "object" ? data.runtime : {};
        Object.assign(health.updater, runtime);
        const lastResult = String(runtime?.last_result || "").trim().toLowerCase();
        const queued = Boolean(runtime?.queued_apply?.queued);

        if (lastResult === "failed") {
          hideUpdaterOverlay();
          message.value = `应用更新失败: ${String(runtime?.last_error || "请查看系统日志").trim() || "请查看系统日志"}`;
          return;
        }
        if (lastResult === "updated_restart_scheduled" || runtime?.restart_required) {
          beginUpdaterRestartRecovery();
          return;
        }
        if (lastResult === "downloading_patch" || lastResult === "applying_patch" || lastResult.startsWith("dependency_")) {
          setUpdaterOverlay(true, {
            title: "正在更新程序",
            subtitle: "后台任务已结束，正在应用补丁，请保持当前页面打开。",
            stage: "applying",
          });
        }
        if (!queued && !runtime?.running && (lastResult === "updated" || lastResult === "restart_pending")) {
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
    await fetchJobs({ silentMessage: true });
    await fetchRuntimeResources({ silentMessage: true });
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
        return "已一键全确认，并自动完成 12 项上传和云文档上传";
      }
      if (cloudStatus === "skipped") {
        const reasonText = cloudSkippedReasons.length
          ? `，云文档已跳过（${cloudSkippedReasons.join(" / ")}）`
          : "，但云文档已跳过";
        return `已一键全确认，并自动完成 12 项上传${reasonText}`;
      }
      if (cloudStatus === "failed" || cloudStatus === "partial_failed") {
        return "已一键全确认，12 项上传成功，但云文档上传失败，请查看系统日志";
      }
      return "已一键全确认，并自动完成后续上传";
    }
    if (followupStatus === "partial_failed") {
      if (cloudStatus === "failed" || cloudStatus === "partial_failed") {
        return "已一键全确认，但 12 项或云文档上传存在部分失败，请查看系统日志";
      }
      return "已一键全确认，但 12 项上传存在部分失败，请查看系统日志";
    }
    if (followupStatus === "failed") {
      if (cloudStatus === "failed" || cloudStatus === "partial_failed") {
        return "已一键全确认，但 12 项和云文档上传失败，请查看系统日志";
      }
      return "已一键全确认，但 12 项上传失败，请查看系统日志";
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

  function setLastSavedSignatureFromPreparedPayload() {
    const payloadState = buildPreparedSavePayload();
    if (!payloadState.ok) {
      lastSavedConfigSignature = "";
      return;
    }
    lastSavedConfigSignature = payloadState.signature || "";
  }

  function withAutoSaveSuspended(applyFn) {
    if (!configAutoSaveSuspendDepth) {
      applyFn();
      return;
    }
    configAutoSaveSuspendDepth.value += 1;
    try {
      applyFn();
    } finally {
      window.setTimeout(() => {
        configAutoSaveSuspendDepth.value = Math.max(0, configAutoSaveSuspendDepth.value - 1);
      }, 0);
    }
  }

  function appendLog(payload) {
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

  function applyHealthSnapshot(data) {
    if (!data || typeof data !== "object") return;
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
      if (data.handover.review_status && typeof data.handover.review_status === "object") {
        health.handover.review_status = {
          ...health.handover.review_status,
          ...data.handover.review_status,
        };
      }
      applyHandoverReviewAccessSnapshot(data.handover);
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
    if (data.day_metric_upload && typeof data.day_metric_upload === "object") {
      health.day_metric_upload.enabled = Boolean(data.day_metric_upload.enabled);
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
      Object.assign(health.shared_bridge, data.shared_bridge);
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
      if (healthLoadError) {
        healthLoadError.value = "";
      }
      return true;
    } catch (err) {
      if (healthLoadError) {
        healthLoadError.value = String(err || "").trim();
      }
      if (isTransientNetworkError(err)) return false;
      if (!silentMessage) {
        message.value = `启动状态读取失败: ${err}`;
      }
      return false;
    }
  }

  async function fetchHealth(options = {}) {
    if (isUpdaterTrafficPaused()) return false;
    if (healthRequestInFlight) return healthRequestInFlight;
    const silentTransientNetworkError = Boolean(options?.silentTransientNetworkError);
    const silentMessage = Boolean(options?.silentMessage);
    const includeHandoverContext =
      typeof options?.includeHandoverContext === "boolean"
        ? options.includeHandoverContext
        : typeof shouldIncludeHandoverHealthContext === "function"
          ? Boolean(shouldIncludeHandoverHealthContext())
          : true;
    healthRequestInFlight = (async () => {
      try {
        const params = includeHandoverContext
          ? {
              handover_duty_date: String(handoverDutyDate?.value || "").trim(),
              handover_duty_shift: String(handoverDutyShift?.value || "").trim().toLowerCase(),
            }
          : {};
        const data = await getHealthApi(params);
        applyHealthSnapshot(data);
        if (fullHealthLoaded) {
          fullHealthLoaded.value = true;
        }
        if (healthLoadError) {
          healthLoadError.value = "";
        }
        return true;
      } catch (err) {
        if (isAbortError(err)) return false;
        if (healthLoadError) {
          healthLoadError.value = String(err || "").trim();
        }
        if (silentTransientNetworkError && isTransientNetworkError(err)) return false;
        if (!silentMessage) {
          message.value = `健康检查失败: ${err}`;
        }
        return false;
      } finally {
        healthRequestInFlight = null;
      }
    })();
    return healthRequestInFlight;
  }

  async function fetchJobs(options = {}) {
    if (isUpdaterTrafficPaused()) return false;
    const silentMessage = Boolean(options?.silentMessage);
    try {
      const data = await getJobsApi({ limit: 60 });
      jobsList.value = Array.isArray(data?.jobs) ? data.jobs : [];
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
        try {
          const detail = await getJobApi(detailJobId);
          if (detail && typeof detail === "object") {
            currentJob.value = { ...(currentJob.value || {}), ...detail };
          }
        } catch (_) {
          // keep list summary when detail refresh fails
        }
      }
      return true;
    } catch (err) {
      if (!silentMessage) {
        message.value = `读取任务列表失败: ${err}`;
      }
      return false;
    }
  }

  async function fetchRuntimeResources(options = {}) {
    if (isUpdaterTrafficPaused()) return false;
    const silentMessage = Boolean(options?.silentMessage);
    try {
      const data = await getRuntimeResourcesApi();
      resourceSnapshot.value =
        data && typeof data === "object"
          ? data
          : { network: {}, controlled_browser: { holder_job_id: "", queue_length: 0 }, batch_locks: [], resources: [] };
      return true;
    } catch (err) {
      if (!silentMessage) {
        message.value = `读取资源状态失败: ${err}`;
      }
      return false;
    }
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
    if (isUpdaterTrafficPaused()) return false;
    if (bridgeTasksRequestInFlight) return bridgeTasksRequestInFlight;
    const silentMessage = Boolean(options?.silentMessage);
    bridgeTasksRequestInFlight = (async () => {
      try {
        const data = await getBridgeTasksApi({ limit: 60 });
        const tasks = Array.isArray(data?.tasks) ? data.tasks : [];
        bridgeTasks.value = tasks;
        const selectedTaskId = String(selectedBridgeTaskId?.value || "").trim();
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
        if (nextTaskId) {
          await fetchBridgeTaskDetail(nextTaskId, { silentMessage: true });
        } else if (bridgeTaskDetail) {
          bridgeTaskDetail.value = null;
        }
        return true;
      } catch (err) {
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
        await cancelBridgeTaskApi(taskIdText);
        await fetchBridgeTasks({ silentMessage: true });
        await fetchBridgeTaskDetail(taskIdText, { silentMessage: true });
        message.value = "共享任务取消请求已提交";
        return true;
      } catch (err) {
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
        await retryBridgeTaskApi(taskIdText);
        await fetchBridgeTasks({ silentMessage: true });
        await fetchBridgeTaskDetail(taskIdText, { silentMessage: true });
        message.value = "共享任务已重新排队";
        return true;
      } catch (err) {
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
        await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
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

  async function refreshManualAlarmSourceCache() {
    const runner = async () => {
      try {
        const data = await refreshManualAlarmSourceCacheApi();
        await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
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
        await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
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
          String(data?.message || "").trim() || "已提交告警信息文件全量上传任务",
        );
        return data;
      } catch (err) {
        message.value = `告警信息文件全量上传失败: ${err}`;
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
          String(data?.message || "").trim() || `已提交 ${buildingText} 告警信息文件刷新上传任务`,
        );
        return data;
      } catch (err) {
        message.value = `${buildingText} 告警信息文件刷新上传失败: ${err}`;
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
      handoverEngineerDirectory.value = rows;
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

  async function fetchConfig(options = {}) {
    if (isUpdaterTrafficPaused()) return false;
    const silentMessage = Boolean(options?.silentMessage);
    try {
      const data = await getConfigApi();
      serverConfigSnapshot = clone(data || {});
      const normalized = ensureConfigShape(convertV3ConfigToLegacy(data || {}));
      withAutoSaveSuspended(() => {
        hydrateConfigView(normalized);
        setLastSavedSignatureFromPreparedPayload();
      });
      if (configLoaded) {
        configLoaded.value = true;
      }
      if (configLoadError) {
        configLoadError.value = "";
      }
      return true;
    } catch (err) {
      if (configLoadError) {
        configLoadError.value = String(err || "").trim();
      }
      if (!silentMessage) {
        message.value = `读取配置失败: ${err}`;
      }
      return false;
    }
  }

  async function saveConfigInternal({ auto = false, skipPostSaveHealthRefresh = false } = {}) {
    const payloadState = buildPreparedSavePayload();
    if (!payloadState.ok) {
      if (!auto) {
        message.value = payloadState.error || "配置校验失败";
      }
      return { saved: false, reason: "invalid", restartRequired: false };
    }
    const { v3Payload, signature } = payloadState;
    const requestPayload = mergeConfigWithServerSnapshot(serverConfigSnapshot, v3Payload);
    if (auto && signature && signature === lastSavedConfigSignature) {
      return { saved: false, reason: "unchanged", restartRequired: false };
    }

    try {
      const data = await putConfigApi(requestPayload);
      serverConfigSnapshot = clone(data?.config || requestPayload);
      const normalized = ensureConfigShape(convertV3ConfigToLegacy(data?.config || requestPayload));
      withAutoSaveSuspended(() => {
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
      if (!skipPostSaveHealthRefresh) {
        void fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
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
        data,
      };
    } catch (err) {
      if (!auto) {
        message.value = `保存配置失败: ${err}`;
      }
      return { saved: false, reason: "error", error: String(err), restartRequired: false };
    }
  }

  async function saveConfig(options = {}) {
    const runner = async () => saveConfigInternal({
      auto: false,
      skipPostSaveHealthRefresh: Boolean(options?.skipPostSaveHealthRefresh),
    });
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_SAVE_CONFIG, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function autoSaveConfig(options = {}) {
    const runner = async () => saveConfigInternal({
      auto: true,
      skipPostSaveHealthRefresh: Boolean(options?.skipPostSaveHealthRefresh),
    });
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_SAVE_CONFIG, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function checkUpdaterNow(options = {}) {
    const autoApplyIfAvailable = Boolean(options?.autoApplyIfAvailable);
    const runner = async () => {
      try {
        const data = await checkUpdaterApi();
        const result = data?.result || {};
        await fetchHealth();
        if (
          autoApplyIfAvailable
          && !health.updater?.restart_required
          && (health.updater?.update_available || health.updater?.force_apply_available)
        ) {
          await applyUpdaterPatch();
          return data;
        }
        const resultKey = String(result?.last_result || "").trim();
        if (resultKey === "failed") {
          message.value = `更新检查失败: ${mapUpdaterResultText(resultKey)}`;
        } else if (resultKey === "mirror_pending_publish" || resultKey === "ahead_of_mirror") {
          message.value = buildUpdaterApplyMessage(result);
        } else {
          // 成功时不占用全局提示区，避免与顶部版本状态重复显示
          message.value = "";
        }
      } catch (err) {
        message.value = `手动检查更新失败: ${err}`;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_UPDATER_CHECK, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function applyUpdaterPatch() {
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
      pauseRuntimeTraffic();
      setUpdaterOverlay(true, {
        title: "正在更新程序",
        subtitle: "请保持当前页面打开，更新完成后会自动恢复。",
        stage: "applying",
      });
      try {
        const data = await applyUpdaterApi({
          mode: requestedMode,
          queue_if_busy: true,
        });
        const result = data?.result || {};
        Object.assign(health.updater, data?.runtime || {}, {
          last_result: String(result?.last_result || health.updater.last_result || ""),
        });
        message.value = buildUpdaterApplyMessage(result);
        const finalResult = String(result?.last_result || "").trim();
        if (finalResult === "queued_busy") {
          startQueuedUpdaterMonitor();
          return data;
        }
        if (finalResult === "updated_restart_scheduled") {
          beginUpdaterRestartRecovery();
          return data;
        }
        await fetchHealth();
        hideUpdaterOverlay();
        return data;
      } catch (err) {
        if (isTransientNetworkError(err)) {
          return handoffUpdaterToRestartRecovery("更新已开始，服务正在重启，正在等待恢复。");
        }
        hideUpdaterOverlay();
        message.value = `应用更新失败: ${err}`;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_UPDATER_APPLY, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function restartUpdaterApp() {
    const runner = async () => {
      pauseRuntimeTraffic();
      setUpdaterOverlay(true, {
        title: "正在重启程序",
        subtitle: "请保持当前页面打开，服务恢复后会自动刷新。",
        stage: "restarting",
      });
      try {
        const data = await restartUpdaterApi();
        const result = data?.result || {};
        Object.assign(health.updater, data?.runtime || {}, {
          last_result: String(result?.last_result || health.updater.last_result || ""),
        });
        message.value = buildUpdaterApplyMessage(result?.last_result ? result : "updated_restart_scheduled");
        beginUpdaterRestartRecovery();
        return data;
      } catch (err) {
        if (isTransientNetworkError(err)) {
          return handoffUpdaterToRestartRecovery("重启已触发，正在等待服务恢复。");
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
      const data = await activateStartupRuntimeApi({
        source: String(options?.source || "").trim() || "启动角色确认",
        startup_handoff_nonce: String(options?.startupHandoffNonce || "").trim(),
      });
      return {
        ok: data?.ok !== false,
        activated: Boolean(data?.activated),
        alreadyActive: Boolean(data?.already_active),
        roleMode: String(data?.role_mode || "").trim(),
        error: String(data?.error || "").trim(),
      };
    } catch (err) {
      return {
        ok: false,
        activated: false,
        alreadyActive: false,
        roleMode: "",
        error: String(err || "").trim() || "激活后台运行时失败",
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
            await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
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
            await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
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
      if (status === "ready") return true;
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
        const data = await reprobeHandoverReviewAccessApi();
        const snapshot = data?.handover_review_access || {};
        applyHandoverReviewAccessSnapshot(snapshot);
        void fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
        const effectiveBaseUrl = String(snapshot?.review_base_url_effective || "").trim();
        const source = String(snapshot?.review_base_url_effective_source || "").trim().toLowerCase();
        if (effectiveBaseUrl) {
          message.value =
            source === "manual"
              ? `审核访问地址已刷新，当前仍使用手工地址：${effectiveBaseUrl}`
              : `审核访问地址重新探测完成，当前生效地址：${effectiveBaseUrl}`;
        } else {
          message.value = "审核访问地址重新探测完成，但当前未找到可用地址。";
        }
        return data;
      } catch (err) {
        message.value = `重新探测审核访问地址失败: ${err}`;
        return { ok: false, reason: "error", error: String(err) };
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(ACTION_KEY_HANDOVER_REVIEW_ACCESS_REPROBE, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  return {
    appendLog,
    fetchBootstrapHealth,
    fetchHealth,
    fetchJobs,
    fetchBridgeTasks,
    fetchBridgeTaskDetail,
    cancelBridgeTask,
    retryBridgeTask,
    refreshCurrentHourSourceCache,
    refreshManualAlarmSourceCache,
    deleteManualAlarmSourceCacheFiles,
    uploadAlarmSourceCacheFull,
    uploadAlarmSourceCacheBuilding,
    openAlarmEventUploadTarget,
    fetchRuntimeResources,
    fetchHandoverDailyReportContext,
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
    buildHandoverDailyReportCaptureAssetUrl,
    getBridgeTaskCancelActionKey,
    getBridgeTaskRetryActionKey,
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
    ACTION_KEY_HANDOVER_DAILY_REPORT_SCREENSHOT_TEST,
    ACTION_KEY_HANDOVER_DAILY_REPORT_RECORD_REWRITE,
    ACTION_KEY_HANDOVER_REVIEW_ACCESS_REPROBE,
  };
}






