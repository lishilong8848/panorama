import { apiJson } from "./config_helpers.js";

function appendQuery(url, params = {}) {
  const query = new URLSearchParams();
  Object.entries(params || {}).forEach(([key, value]) => {
    const text = String(value ?? "").trim();
    if (text) query.set(key, text);
  });
  const suffix = query.toString();
  return suffix ? `${url}?${suffix}` : url;
}

function buildTimeoutSignal(timeoutMs) {
  const normalized = Number.parseInt(String(timeoutMs || 0), 10);
  if (!Number.isFinite(normalized) || normalized <= 0 || typeof AbortController !== "function") {
    return { signal: undefined, dispose() {} };
  }
  const controller = new AbortController();
  const timerId = window.setTimeout(() => controller.abort(), normalized);
  return {
    signal: controller.signal,
    dispose() {
      window.clearTimeout(timerId);
    },
  };
}

async function apiJsonWithTimeout(url, options = {}, timeoutMs = 0) {
  const timeout = buildTimeoutSignal(timeoutMs);
  try {
    const requestOptions = timeout.signal ? { ...options, signal: timeout.signal } : options;
    return await apiJson(url, requestOptions);
  } finally {
    timeout.dispose();
  }
}

export async function getHealthApi(params = {}) {
  return apiJsonWithTimeout(appendQuery("/api/health", { ...params, _t: Date.now() }), {}, 25000);
}

export async function getBootstrapHealthApi() {
  return apiJsonWithTimeout(`/api/health/bootstrap?_t=${Date.now()}`, {}, 8000);
}

export async function getConfigApi() {
  return apiJsonWithTimeout(`/api/config?_t=${Date.now()}`, {}, 8000);
}

export async function putConfigApi(v3Config) {
  return apiJson("/api/config", {
    method: "PUT",
    body: JSON.stringify(v3Config),
  });
}

export async function repairDayMetricUploadConfigApi() {
  return apiJson("/api/config-repair/day-metric-upload", {
    method: "POST",
    body: "{}",
  });
}

export async function getHandoverCommonConfigSegmentApi() {
  return apiJsonWithTimeout("/api/config-segments/handover/common", {}, 8000);
}

export async function putHandoverCommonConfigSegmentApi(payload) {
  return apiJson("/api/config-segments/handover/common", {
    method: "PUT",
    body: JSON.stringify(payload || {}),
  });
}

export async function getHandoverBuildingConfigSegmentApi(code) {
  return apiJsonWithTimeout(`/api/config-segments/handover/buildings/${encodeURIComponent(String(code || "").trim())}`, {}, 8000);
}

export async function putHandoverBuildingConfigSegmentApi(code, payload) {
  return apiJson(`/api/config-segments/handover/buildings/${encodeURIComponent(String(code || "").trim())}`, {
    method: "PUT",
    body: JSON.stringify(payload || {}),
  });
}

export async function getPendingResumeRunsApi() {
  return apiJson("/api/jobs/resume/pending");
}

export async function deleteResumeRunApi(runId) {
  return apiJson("/api/jobs/resume/delete", {
    method: "POST",
    body: JSON.stringify({ run_id: runId }),
  });
}

export async function startJsonJobApi(url, body = {}) {
  return apiJson(url, {
    method: "POST",
    body: JSON.stringify(body || {}),
  });
}

export async function getJobApi(jobId) {
  return apiJson(`/api/jobs/${jobId}`);
}

export async function cancelJobApi(jobId) {
  return apiJson(`/api/jobs/${jobId}/cancel`, { method: "POST", body: "{}" });
}

export async function retryJobApi(jobId) {
  return apiJson(`/api/jobs/${jobId}/retry`, { method: "POST", body: "{}" });
}

export async function getJobsApi(params = {}) {
  return apiJson(appendQuery("/api/jobs", params));
}

export async function getRuntimeResourcesApi() {
  return apiJson("/api/runtime/resources");
}

export async function getExternalDashboardSummaryApi() {
  return apiJsonWithTimeout("/api/runtime/external-dashboard-summary", {}, 20000);
}

export async function getExternalRuntimeBootstrapApi() {
  return apiJsonWithTimeout("/api/runtime/external/bootstrap", {}, 5000);
}

export async function getExternalRuntimeSourceCacheApi() {
  return apiJsonWithTimeout("/api/runtime/external/source-cache", {}, 5000);
}

export async function getExternalRuntimeJobsApi() {
  return apiJsonWithTimeout("/api/runtime/external/jobs", {}, 5000);
}

export async function getExternalRuntimeBridgeTasksApi() {
  return apiJsonWithTimeout("/api/runtime/external/bridge-tasks", {}, 5000);
}

export async function getExternalRuntimeSchedulersApi() {
  return apiJsonWithTimeout("/api/runtime/external/schedulers", {}, 5000);
}

export async function getExternalRuntimeUpdaterApi() {
  return apiJsonWithTimeout("/api/runtime/external/updater", {}, 5000);
}

export async function getExternalRuntimeReviewOverviewApi() {
  return apiJsonWithTimeout("/api/runtime/external/review-overview", {}, 5000);
}

export async function getExternalRuntimeConfigGuidanceApi() {
  return apiJsonWithTimeout("/api/runtime/external/config-guidance", {}, 5000);
}

export async function getExternalRuntimeSystemApi() {
  return apiJsonWithTimeout("/api/runtime/external/system", {}, 5000);
}

export async function getBridgeTasksApi(params = {}) {
  return apiJson(appendQuery("/api/bridge/tasks", params));
}

export async function getInternalRuntimeStatusApi() {
  return apiJsonWithTimeout("/api/bridge/internal-runtime-status", {}, 8000);
}

export async function getInternalRuntimeBuildingStatusApi(buildingCode) {
  const codeText = String(buildingCode || "").trim();
  return apiJsonWithTimeout(
    `/api/bridge/internal-runtime-status/buildings/${encodeURIComponent(codeText)}`,
    {},
    8000,
  );
}

export async function getBridgeTaskApi(taskId) {
  return apiJson(`/api/bridge/tasks/${encodeURIComponent(String(taskId || "").trim())}`);
}

export async function cancelBridgeTaskApi(taskId) {
  return apiJson(`/api/bridge/tasks/${encodeURIComponent(String(taskId || "").trim())}/cancel`, {
    method: "POST",
    body: "{}",
  });
}

export async function retryBridgeTaskApi(taskId) {
  return apiJson(`/api/bridge/tasks/${encodeURIComponent(String(taskId || "").trim())}/retry`, {
    method: "POST",
    body: "{}",
  });
}

export async function refreshTodaySourceCacheApi() {
  return apiJson("/api/bridge/source-cache/refresh-today", {
    method: "POST",
    body: "{}",
  });
}

export async function refreshCurrentHourSourceCacheApi() {
  return apiJson("/api/bridge/source-cache/refresh-current-hour", {
    method: "POST",
    body: "{}",
  });
}

export async function refreshManualAlarmSourceCacheApi() {
  return apiJson("/api/bridge/source-cache/refresh-alarm-manual", {
    method: "POST",
    body: "{}",
  });
}

export async function refreshBuildingLatestSourceCacheApi(sourceFamily, building) {
  const sourceFamilyText = String(sourceFamily || "").trim();
  const buildingText = String(building || "").trim();
  return apiJson(
    appendQuery("/api/bridge/source-cache/refresh-building-latest", {
      source_family: sourceFamilyText,
      building: buildingText,
    }),
    {
      method: "POST",
      body: "{}",
    },
  );
}

export async function deleteManualAlarmSourceCacheFilesApi() {
  return apiJson("/api/bridge/source-cache/delete-manual-alarm-files", {
    method: "POST",
    body: "{}",
  });
}

export async function uploadAlarmSourceCacheFullApi() {
  return apiJson("/api/bridge/source-cache/alarm/upload-full", {
    method: "POST",
    body: "{}",
  });
}

export async function uploadAlarmSourceCacheBuildingApi(building) {
  const buildingText = String(building || "").trim();
  return apiJson(appendQuery("/api/bridge/source-cache/alarm/upload-building", { building: buildingText }), {
    method: "POST",
    body: "{}",
  });
}

export async function runSharedBridgeSelfCheckApi() {
  return apiJson("/api/bridge/shared-root/self-check", {
    method: "POST",
    body: "{}",
  });
}

export async function openAlarmEventUploadTargetApi() {
  return apiJson("/api/runtime/alarm-event-upload-target/open", {
    method: "POST",
    body: "{}",
  });
}

export async function startSchedulerApi() {
  return apiJson("/api/scheduler/start", { method: "POST", body: "{}" });
}

export async function stopSchedulerApi() {
  return apiJson("/api/scheduler/stop", { method: "POST", body: "{}" });
}

export async function saveSchedulerConfigApi(payload) {
  return apiJson("/api/scheduler/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function startHandoverSchedulerApi() {
  return apiJson("/api/scheduler/handover/start", { method: "POST", body: "{}" });
}

export async function stopHandoverSchedulerApi() {
  return apiJson("/api/scheduler/handover/stop", { method: "POST", body: "{}" });
}

export async function getHandoverSchedulerStatusApi() {
  return apiJson("/api/scheduler/handover/status");
}

export async function saveHandoverSchedulerConfigApi(payload) {
  return apiJson("/api/scheduler/handover/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function startWetBulbCollectionJobApi() {
  return apiJson("/api/jobs/wet-bulb-collection/run", { method: "POST", body: "{}" });
}

export async function startWetBulbCollectionSchedulerApi() {
  return apiJson("/api/scheduler/wet-bulb-collection/start", { method: "POST", body: "{}" });
}

export async function stopWetBulbCollectionSchedulerApi() {
  return apiJson("/api/scheduler/wet-bulb-collection/stop", { method: "POST", body: "{}" });
}

export async function getWetBulbCollectionSchedulerStatusApi() {
  return apiJson("/api/scheduler/wet-bulb-collection/status");
}

export async function saveWetBulbCollectionSchedulerConfigApi(payload) {
  return apiJson("/api/scheduler/wet-bulb-collection/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function startDayMetricUploadSchedulerApi() {
  return apiJson("/api/scheduler/day-metric-upload/start", { method: "POST", body: "{}" });
}

export async function stopDayMetricUploadSchedulerApi() {
  return apiJson("/api/scheduler/day-metric-upload/stop", { method: "POST", body: "{}" });
}

export async function getDayMetricUploadSchedulerStatusApi() {
  return apiJson("/api/scheduler/day-metric-upload/status");
}

export async function saveDayMetricUploadSchedulerConfigApi(payload) {
  return apiJson("/api/scheduler/day-metric-upload/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function startBranchPowerUploadSchedulerApi() {
  return apiJson("/api/scheduler/branch-power-upload/start", { method: "POST", body: "{}" });
}

export async function stopBranchPowerUploadSchedulerApi() {
  return apiJson("/api/scheduler/branch-power-upload/stop", { method: "POST", body: "{}" });
}

export async function getBranchPowerUploadSchedulerStatusApi() {
  return apiJson("/api/scheduler/branch-power-upload/status");
}

export async function saveBranchPowerUploadSchedulerConfigApi(payload) {
  return apiJson("/api/scheduler/branch-power-upload/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function startAlarmEventUploadSchedulerApi() {
  return apiJson("/api/scheduler/alarm-event-upload/start", { method: "POST", body: "{}" });
}

export async function stopAlarmEventUploadSchedulerApi() {
  return apiJson("/api/scheduler/alarm-event-upload/stop", { method: "POST", body: "{}" });
}

export async function getAlarmEventUploadSchedulerStatusApi() {
  return apiJson("/api/scheduler/alarm-event-upload/status");
}

export async function saveAlarmEventUploadSchedulerConfigApi(payload) {
  return apiJson("/api/scheduler/alarm-event-upload/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function startMonthlyEventReportJobApi(payload = {}) {
  return apiJson("/api/jobs/monthly-event-report/run", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function startMonthlyChangeReportJobApi(payload = {}) {
  return apiJson("/api/jobs/monthly-change-report/run", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function startMonthlyReportSendJobApi(payload = {}) {
  return apiJson("/api/jobs/monthly-report/send", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function startMonthlyReportSendTestJobApi(payload = {}) {
  return apiJson("/api/jobs/monthly-report/send", {
    method: "POST",
    body: JSON.stringify({ ...(payload || {}), test_mode: true }),
  });
}

export async function startMonthlyEventReportSchedulerApi() {
  return apiJson("/api/scheduler/monthly-event-report/start", { method: "POST", body: "{}" });
}

export async function stopMonthlyEventReportSchedulerApi() {
  return apiJson("/api/scheduler/monthly-event-report/stop", { method: "POST", body: "{}" });
}

export async function getMonthlyEventReportSchedulerStatusApi() {
  return apiJson("/api/scheduler/monthly-event-report/status");
}

export async function saveMonthlyEventReportSchedulerConfigApi(payload) {
  return apiJson("/api/scheduler/monthly-event-report/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function startMonthlyChangeReportSchedulerApi() {
  return apiJson("/api/scheduler/monthly-change-report/start", { method: "POST", body: "{}" });
}

export async function stopMonthlyChangeReportSchedulerApi() {
  return apiJson("/api/scheduler/monthly-change-report/stop", { method: "POST", body: "{}" });
}

export async function getMonthlyChangeReportSchedulerStatusApi() {
  return apiJson("/api/scheduler/monthly-change-report/status");
}

export async function saveMonthlyChangeReportSchedulerConfigApi(payload) {
  return apiJson("/api/scheduler/monthly-change-report/config", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function getHandoverEngineerDirectoryApi() {
  return apiJson("/api/handover/engineer-directory");
}

export async function checkUpdaterApi() {
  return apiJson("/api/updater/check", { method: "POST", body: "{}" });
}

export async function getUpdaterStatusApi() {
  return apiJson("/api/updater/status");
}

export async function applyUpdaterApi(payload = {}) {
  return apiJson("/api/updater/apply", { method: "POST", body: JSON.stringify(payload || {}) });
}

export async function restartUpdaterApi() {
  return apiJson("/api/updater/restart", { method: "POST", body: "{}" });
}

export async function publishUpdaterApprovedApi() {
  throw new Error("外网端远程更新内网端功能已移除");
}

export async function triggerInternalPeerUpdaterCheckApi() {
  throw new Error("外网端远程更新内网端功能已移除");
}

export async function triggerInternalPeerUpdaterApplyApi() {
  throw new Error("外网端远程更新内网端功能已移除");
}

export async function triggerInternalPeerUpdaterRestartApi() {
  throw new Error("外网端远程更新内网端功能已移除");
}

export async function restartAppApi(payload = {}) {
  return apiJson("/api/app/restart", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function activateStartupRuntimeApi(payload = {}) {
  return apiJson("/api/runtime/activate-startup", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function exitCurrentRuntimeApi(payload = {}) {
  return apiJson("/api/runtime/exit-current", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function getHandoverReviewApi(buildingCode, params = {}, options = {}) {
  return apiJson(appendQuery(`/api/handover/review/${buildingCode}`, params), options);
}

export async function getHandoverReviewBootstrapApi(buildingCode, params = {}, options = {}) {
  return apiJson(appendQuery(`/api/handover/review/${buildingCode}/bootstrap`, params), options);
}

export async function getHandoverReviewStatusApi(buildingCode, params = {}, options = {}) {
  return apiJson(appendQuery(`/api/handover/review/${buildingCode}/status`, params), options);
}

export async function getHandoverReviewHistoryApi(buildingCode, params = {}, options = {}) {
  return apiJson(appendQuery(`/api/handover/review/${buildingCode}/history`, params), options);
}

export async function claimHandoverReviewLockApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/lock/claim`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function heartbeatHandoverReviewLockApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/lock/heartbeat`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function releaseHandoverReviewLockApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/lock/release`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function claimHandoverReview110kvLockApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/shared-blocks/110kv/lock/claim`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function heartbeatHandoverReview110kvLockApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/shared-blocks/110kv/lock/heartbeat`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function releaseHandoverReview110kvLockApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/shared-blocks/110kv/lock/release`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function markHandoverReview110kvDirtyApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/shared-blocks/110kv/dirty`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function saveHandoverReview110kvApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/shared-blocks/110kv`, {
    method: "PUT",
    body: JSON.stringify(payload || {}),
  });
}

export function buildHandoverReviewDownloadUrl(buildingCode, sessionId, params = {}) {
  return appendQuery(`/api/handover/review/${encodeURIComponent(buildingCode)}/download`, {
    session_id: sessionId,
    ...params,
  });
}

export function buildHandoverReviewCapacityDownloadUrl(buildingCode, sessionId, params = {}) {
  return appendQuery(`/api/handover/review/${encodeURIComponent(buildingCode)}/capacity-download`, {
    session_id: sessionId,
    ...params,
  });
}

export async function sendHandoverReviewCapacityImageApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/capacity-image/send`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function regenerateHandoverReviewApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/regenerate`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function getHandoverReviewBatchStatusApi(batchKey) {
  return apiJson(`/api/handover/review/batch/${encodeURIComponent(batchKey)}/status`);
}

export async function confirmAllHandoverReviewBatchApi(batchKey, payload = {}) {
  return apiJson(`/api/handover/review/batch/${encodeURIComponent(batchKey)}/confirm-all`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function getHandoverDailyReportContextApi(params = {}) {
  return apiJsonWithTimeout(appendQuery("/api/handover/daily-report/context", params), {}, 8000);
}

export async function reprobeHandoverReviewAccessApi() {
  return apiJsonWithTimeout("/api/handover/review-access/reprobe", {
    method: "POST",
    body: "{}",
  }, 30000);
}

export async function openHandoverDailyReportScreenshotAuthApi(payload = {}) {
  return apiJson("/api/handover/daily-report/screenshot-auth/open", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function runHandoverDailyReportScreenshotTestApi(payload = {}) {
  return apiJsonWithTimeout("/api/handover/daily-report/screenshot-test", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  }, 90000);
}

export function buildHandoverDailyReportCaptureAssetUrl(params = {}) {
  return appendQuery("/api/handover/daily-report/capture-assets/file", params);
}

export async function recaptureHandoverDailyReportAssetApi(payload = {}) {
  return apiJsonWithTimeout("/api/handover/daily-report/capture-assets/recapture", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  }, 90000);
}

export async function uploadHandoverDailyReportAssetApi(form) {
  return postFormJob("/api/handover/daily-report/capture-assets/upload", form);
}

export async function restoreHandoverDailyReportManualAssetApi(params = {}) {
  const resp = await fetch(appendQuery("/api/handover/daily-report/capture-assets/manual", params), {
    method: "DELETE",
  });
  if (!resp.ok) throw new Error(await resp.text());
  return resp.json();
}

export async function rewriteHandoverDailyReportRecordApi(payload = {}) {
  return apiJson("/api/handover/daily-report/record/rewrite", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function saveHandoverReviewApi(buildingCode, payload) {
  return apiJson(`/api/handover/review/${buildingCode}`, {
    method: "PUT",
    body: JSON.stringify(payload || {}),
  });
}

export async function confirmHandoverReviewApi(buildingCode, payload) {
  return apiJson(`/api/handover/review/${buildingCode}/confirm`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function unconfirmHandoverReviewApi(buildingCode, payload) {
  return apiJson(`/api/handover/review/${buildingCode}/unconfirm`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function retryHandoverReviewCloudSyncApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/cloud-sync/retry`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function updateHandoverReviewCloudSyncApi(buildingCode, payload = {}) {
  return apiJson(`/api/handover/review/${buildingCode}/cloud-sync/update`, {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export async function retryHandoverReviewBatchCloudSyncApi(batchKey) {
  return apiJson(`/api/handover/review/batch/${encodeURIComponent(batchKey)}/cloud-sync/retry`, {
    method: "POST",
    body: "{}",
  });
}

export async function submitHandoverFollowupContinueJob(payload = {}) {
  return startJsonJobApi("/api/jobs/handover/followup/continue", payload || {});
}

async function postFormJob(url, form) {
  const resp = await fetch(url, { method: "POST", body: form });
  if (!resp.ok) throw new Error(await resp.text());
  return resp.json();
}

export async function postManualUploadJob(form) {
  return postFormJob("/api/jobs/manual-upload", form);
}

export async function postSheetImportJob(form) {
  return postFormJob("/api/jobs/sheet-import", form);
}

export async function postHandoverFromFileJob(form) {
  return postFormJob("/api/jobs/handover/from-file", form);
}

export async function postHandoverFromFilesJob(form) {
  return postFormJob("/api/jobs/handover/from-files", form);
}

export async function submitDayMetricFromDownloadJob(payload = {}) {
  return startJsonJobApi("/api/jobs/day-metric/from-download", payload || {});
}

export async function submitBranchPowerFromDownloadJob(payload = {}) {
  return startJsonJobApi("/api/jobs/branch-power/from-download", payload || {});
}

export async function submitDayMetricFromFileJob(form) {
  return postFormJob("/api/jobs/day-metric/from-file", form);
}

export async function submitDayMetricRetryUnitJob(payload = {}) {
  return startJsonJobApi("/api/jobs/day-metric/retry-unit", payload || {});
}

export async function submitDayMetricRetryFailedJob(payload = {}) {
  return startJsonJobApi("/api/jobs/day-metric/retry-failed", payload || {});
}

