// Cloud document wording is shown as "班次附件/多维记录"; legacy backends still send cloud_sheet keys.
export const ATTACHMENT_DELIVERY_STATUSES = ["queued", "preparing", "ready", "uploaded", "failed", "stale"];
export const ATTACHMENT_DELIVERY_TERMINAL_FAILED = ["failed", "stale"];
export const ATTACHMENT_DELIVERY_BUSY_STATUSES = ["queued", "preparing"];

const ATTACHMENT_DELIVERY_STATUS_TEXT = {
  queued: "排队中",
  preparing: "准备中",
  ready: "成品已就绪",
  uploaded: "附件已上传",
  failed: "生成失败",
  stale: "已过期",
};

const ATTACHMENT_DELIVERY_IDLE_TEXT = "尚未生成成品";

function attachmentDeliveryText(value) {
  return String(value ?? "").trim();
}

function normalizeAttachmentDeliveryStatus(value) {
  const normalized = attachmentDeliveryText(value).toLowerCase();
  return ATTACHMENT_DELIVERY_STATUSES.includes(normalized) ? normalized : "";
}

export function normalizeAttachmentDelivery(raw) {
  if (!raw || typeof raw !== "object") {
    return emptyAttachmentDelivery();
  }
  const status = normalizeAttachmentDeliveryStatus(raw.status);
  // A manifest may lose its status; readiness still means the generation finished.
  const artifactsReady = Boolean(raw.artifacts_ready ?? raw.artifactsReady) && (!status || status === "ready" || status === "uploaded");
  return {
    status: status || (artifactsReady ? "ready" : ""),
    artifacts_ready: artifactsReady,
    generation_id: attachmentDeliveryText(raw.generation_id ?? raw.generationId),
    error: attachmentDeliveryText(raw.error),
    upload_error: attachmentDeliveryText(raw.upload_error),
    upload_disabled: Boolean(raw.upload_disabled),
    record_url: attachmentDeliveryText(raw.record_url ?? raw.recordUrl),
  };
}

export function emptyAttachmentDelivery() {
  return { status: "", artifacts_ready: false, generation_id: "", error: "", record_url: "" };
}

export function resolveAttachmentDeliveryGate(raw, { expectedGenerationId = "" } = {}) {
  const delivery = normalizeAttachmentDelivery(raw);
  const expected = attachmentDeliveryText(expectedGenerationId);
  const hasGeneration = Boolean(delivery.generation_id);
  const isBusy = ATTACHMENT_DELIVERY_BUSY_STATUSES.includes(delivery.status);
  const isFailed = ATTACHMENT_DELIVERY_TERMINAL_FAILED.includes(delivery.status);
  // Any older generation is stale even if the backend has not relabelled it yet.
  const isStale = Boolean(expected && hasGeneration && delivery.generation_id !== expected);
  const isReady = delivery.artifacts_ready && hasGeneration && (!expected || !isStale);
  let reason = "";
  let reasonCode = "";
  if (isStale) {
    reason = "成品版本已变化，请重新生成成品";
    reasonCode = "delivery_generation_changed";
  } else if (delivery.status === "failed") {
    reason = delivery.error ? `成品生成失败：${delivery.error}` : "成品生成失败，请重试";
    reasonCode = "delivery_failed";
  } else if (delivery.status === "stale") {
    reason = "成品已过期，请重新生成成品";
    reasonCode = "delivery_stale";
  } else if (delivery.status === "queued" || delivery.status === "preparing") {
    reason = "成品生成中，请等待本楼任务完成";
    reasonCode = "delivery_busy";
  } else if (!hasGeneration) {
    reason = "尚未生成成品，请先点击“生成成品并提交”";
    reasonCode = "delivery_missing";
  } else if (!delivery.artifacts_ready) {
    reason = "成品尚未就绪，请等待生成完成";
    reasonCode = "delivery_not_ready";
  }
  return {
    delivery,
    expected_generation_id: expected,
    is_busy: isBusy,
    is_failed: isFailed,
    is_stale: isStale,
    is_ready: isReady,
    can_download: isReady,
    can_send: isReady,
    can_regenerate: !isBusy,
    can_prepare: !isBusy,
    show_retry: isFailed || isStale,
    can_refresh: true,
    reason,
    reason_code: reasonCode,
  };
}

export function resolveAttachmentDeliveryUi(raw, { expectedGenerationId = "" } = {}) {
  const gate = resolveAttachmentDeliveryGate(raw, { expectedGenerationId });
  const { delivery } = gate;
  if (gate.is_stale) {
    return { text: ATTACHMENT_DELIVERY_STATUS_TEXT.stale, tone: "danger", gate };
  }
  if (delivery.status === "failed") {
    return {
      text: delivery.error ? `${ATTACHMENT_DELIVERY_STATUS_TEXT.failed}：${delivery.error}` : ATTACHMENT_DELIVERY_STATUS_TEXT.failed,
      tone: "danger",
      gate,
    };
  }
  if (gate.is_ready) {
    return { text: ATTACHMENT_DELIVERY_STATUS_TEXT[delivery.status] || ATTACHMENT_DELIVERY_STATUS_TEXT.ready, tone: "success", gate };
  }
  if (gate.is_busy) {
    return { text: ATTACHMENT_DELIVERY_STATUS_TEXT[delivery.status], tone: "info", gate };
  }
  return { text: ATTACHMENT_DELIVERY_IDLE_TEXT, tone: "neutral", gate };
}

export async function waitForBackgroundJob(jobId, getJobApi, { timeoutMs = 120000, intervalMs = 1500 } = {}) {
  const targetJobId = String(jobId || "").trim();
  if (!targetJobId) return null;
  const startedAt = Date.now();
  while (Date.now() - startedAt <= timeoutMs) {
    try {
      const job = await getJobApi(targetJobId);
      const status = String(job?.status || "").trim().toLowerCase();
      if (status && !["queued", "running", "waiting_resource"].includes(status)) return job;
    } catch (_error) {
      // Ignore transient polling failures and keep waiting.
    }
    await new Promise((resolve) => window.setTimeout(resolve, intervalMs));
  }
  return null;
}

export function createHandoverReviewActionHelpers(options = {}) {
  const {
    session,
    reviewContext,
    building,
    buildingCode,
    dirty,
    saving,
    confirming,
    cloudSyncBusy,
    syncingRemoteRevision,
    needsRefresh,
    staleRevisionConflict,
    downloading,
    capacityDownloading,
    capacityImageSending,
    regenerating,
    attachmentDelivery,
    attachmentGenerationId,
    preparingDelivery,
    retryingCloudSync,
    updatingHistoryCloudSync,
    activeRouteSelection,
    selectedSessionId,
    latestSessionId,
    isHistoryMode,
    statusText,
    errorText,
    reviewClientId,
    returnToLatestActionBase,
    confirmActionBase,
    confirmActionVm,
    retryCloudSyncActionBase,
    retryCloudSyncActionVm,
    updateHistoryCloudSyncActionBase,
    updateHistoryCloudSyncActionVm,
    downloadActionBase,
    downloadActionVm,
    capacityDownloadActionBase,
    capacityDownloadActionVm,
    capacityImageSendActionBase,
    capacityImageSendActionVm,
    regenerateActionBase,
    regenerateActionVm,
    refreshActionBase,
    refreshActionVm,
    clearSaveTimers,
    saveDocument,
    ensureEditingLock,
    releaseCurrentLock,
    loadReviewData,
    shouldPreferBootstrapLoad,
    // Optional poll rhythm override; production keeps the 2.5s tick / 15min budget.
    attachmentDeliveryPollOptions,
    beginRemoteSaveRefresh,
    isRevisionConflictError,
    applyPayloadMeta,
    broadcastHandoverReviewStatusChange,
    resolveOperationFeedbackText,
    syncReviewSelectionToUrl,
    confirmHandoverReviewApi,
    retryHandoverReviewCloudSyncApi,
    updateHandoverReviewCloudSyncApi,
    prepareHandoverReviewCapacityImageApi,
    sendHandoverReviewCapacityImageApi,
    regenerateHandoverReviewApi,
    regenerateHandoverReviewFromFilesApi,
    buildHandoverReviewDownloadUrl,
    buildHandoverReviewCapacityDownloadUrl,
    prepareHandoverReviewDeliveryApi,
    buildHandoverReviewDeliveryDownloadUrl,
    triggerBrowserDownload,
  } = options;

  function readDeliveryGenerationId() {
    return String(attachmentGenerationId?.value || "").trim();
  }

  function setDeliveryGenerationId(value) {
    if (attachmentGenerationId) {
      attachmentGenerationId.value = String(value || "").trim();
    }
  }

  // This building's own generation unlocks downloads; the five-building upload gate is irrelevant here.
  function resolveDeliveryGate() {
    return resolveAttachmentDeliveryGate(attachmentDelivery?.value, {
      expectedGenerationId: readDeliveryGenerationId(),
    });
  }

  async function refreshAfterRevisionConflict(message = "已同步最新审核内容，请重新下载。") {
    beginRemoteSaveRefresh();
    try {
      const loaded = await loadReviewData({
        background: false,
        mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full",
      });
      if (loaded === false) {
        throw new Error(errorText.value || "加载最新审核内容失败");
      }
      statusText.value = message;
    } catch (error) {
      syncingRemoteRevision.value = false;
      needsRefresh.value = true;
      staleRevisionConflict.value = true;
      errorText.value = `刷新最新内容失败：${String(error?.message || error || "加载失败")}`;
      statusText.value = "";
    }
  }

  // Stop on this floor's delivery result, not on the batch or H-floor follow-up job.
  async function waitForDeliveryResult(generationId, options = {}) {
    const {
      timeoutMs = attachmentDeliveryPollOptions?.timeoutMs ?? 15 * 60 * 1000,
      intervalMs = attachmentDeliveryPollOptions?.intervalMs ?? 2500,
    } = options || {};
    const targetGenerationId = String(generationId || "").trim();
    if (!targetGenerationId || typeof loadReviewData !== "function") return resolveDeliveryGate();
    const startedAt = Date.now();
    while (Date.now() - startedAt <= timeoutMs) {
      try {
        await loadReviewData({ background: true, deliveryPoll: true });
      } catch (_error) {
        // Transient polling failures keep the known state and retry on the next tick.
      }
      const gate = resolveDeliveryGate();
      if (gate.delivery.generation_id === targetGenerationId && (
        gate.delivery.status === "uploaded" || gate.is_failed || gate.is_stale
        || (gate.is_ready && (gate.delivery.upload_error || gate.delivery.upload_disabled))
      )) {
        return gate;
      }
      await new Promise((resolve) => {
        const timer = typeof window !== "undefined" ? window.setTimeout : setTimeout;
        timer(resolve, intervalMs);
      });
    }
    return resolveDeliveryGate();
  }

  async function switchToSession(sessionId, { toLatest = false, dutyDate = "", dutyShift = "" } = {}) {
    const nextSessionId = String(sessionId || "").trim();
    const explicitDuty = Boolean(dutyDate && ["day", "night"].includes(dutyShift));
    if (!toLatest && !explicitDuty && (!nextSessionId || nextSessionId === selectedSessionId.value)) return;
    if (dirty.value) {
      const saved = await saveDocument({ reason: "switch" });
      if (!saved) return;
    }
    await releaseCurrentLock();
    clearSaveTimers();
    needsRefresh.value = false;
    staleRevisionConflict.value = false;
    errorText.value = "";
    activeRouteSelection.value = {
      sessionId: toLatest ? "" : nextSessionId,
      dutyDate: explicitDuty ? dutyDate : "",
      dutyShift: explicitDuty ? dutyShift : "",
    };
    syncReviewSelectionToUrl(explicitDuty ? { dutyDate, dutyShift } : { sessionId: toLatest ? "" : nextSessionId, isLatest: toLatest });
    statusText.value = toLatest ? "正在切换到最新交接班日志..." : "正在切换历史交接班日志...";
    await loadReviewData({
      background: false,
      mode: shouldPreferBootstrapLoad({ forceLatest: toLatest }) ? "bootstrap" : "full",
    });
  }

  async function onHistorySelectionChange(nextSessionId) {
    const targetSessionId = String(nextSessionId || "").trim();
    if (!targetSessionId || targetSessionId === selectedSessionId.value) return;
    if (latestSessionId.value && targetSessionId === latestSessionId.value) {
      await switchToSession(latestSessionId.value, { toLatest: true });
      return;
    }
    await switchToSession(targetSessionId, { toLatest: false });
  }

  async function returnToLatestSession() {
    if (!returnToLatestActionBase.value.allowed) return;
    await switchToSession(latestSessionId.value, { toLatest: true });
  }

  function deliveryConflictText(gate) {
    return gate.reason || "成品尚未就绪，请先点击“生成成品并提交”";
  }

  // "生成成品并提交": save content, hold the edit lock, then queue this building's artifact job.
  async function toggleConfirm() {
    if (confirmActionVm.value.disabled) {
      statusText.value = confirmActionVm.value.disabledReason || "";
      return;
    }
    if (!confirmActionBase.value.allowed) {
      statusText.value = confirmActionVm.value.disabledReason || "";
      return;
    }
    if (
      !session.value
      || saving.value
      || regenerating.value
      || confirming.value
      || preparingDelivery?.value
      || cloudSyncBusy.value
      || syncingRemoteRevision.value
      || needsRefresh.value
      || staleRevisionConflict.value
    ) return;
    if (!resolveDeliveryGate().can_prepare) {
      statusText.value = deliveryConflictText(resolveDeliveryGate());
      return;
    }
    // The lock must be held before saving; otherwise another terminal could save in between.
    statusText.value = "正在获取审核页编辑锁...";
    errorText.value = "";
    let locked = true;
    if (typeof ensureEditingLock === "function") {
      locked = await ensureEditingLock();
    }
    if (!locked) {
      errorText.value = "当前审核页编辑锁获取失败，请确认没有其他终端编辑后重试";
      statusText.value = "生成成品失败，请处理后重试。";
      return;
    }
    if (dirty.value) {
      const saved = await saveDocument({ reason: "prepare_delivery", allowSubstationFailure: true });
      if (!saved) return;
    }
    confirming.value = true;
    if (preparingDelivery) preparingDelivery.value = true;
    const requestBaseRevision = session.value?.document_revision || session.value?.revision;
    try {
      statusText.value = "正在保存审核内容并生成班次附件成品...";
      const request = {
        session_id: session.value.session_id,
        base_revision: requestBaseRevision,
        client_id: reviewClientId,
      };
      const response = await prepareHandoverReviewDeliveryApi(buildingCode, request);
      const generationId = String(response?.generation_id || response?.attachment_delivery?.generation_id || "").trim();
      setDeliveryGenerationId(generationId);
      const deliverySnippet = response?.attachment_delivery && typeof response.attachment_delivery === "object"
        ? response.attachment_delivery
        : (generationId ? { status: "queued", artifacts_ready: false, generation_id: generationId } : {});
      applyPayloadMeta({ ...(response || {}), attachment_delivery: deliverySnippet });
      broadcastHandoverReviewStatusChange({ ...(response || {}), attachment_delivery: deliverySnippet });
      staleRevisionConflict.value = false;
      needsRefresh.value = false;
      const jobId = String(response?.job?.job_id || response?.job_id || "").trim();
      if (!jobId) {
        throw new Error("生成成品任务提交失败");
      }
      statusText.value = "已提交本楼任务，正在生成并上传附件...";
      const gate = await waitForDeliveryResult(generationId);
      if (gate.delivery.generation_id !== generationId) {
        // A newer generation replaced this one; readiness must be checked against the newest id.
        setDeliveryGenerationId(gate.delivery.generation_id || generationId);
      }
      const latest = resolveDeliveryGate();
      if (latest.delivery.status === "uploaded" && latest.is_ready) {
        statusText.value = "本楼班次附件已上传";
        errorText.value = "";
      } else if (latest.delivery.upload_error) {
        statusText.value = "本楼成品已就绪，附件上传未完成";
        errorText.value = latest.delivery.upload_error;
      } else if (latest.is_ready && latest.delivery.upload_disabled) {
        statusText.value = resolveOperationFeedbackText(response, "本楼班次附件成品已就绪，可下载或发送");
        errorText.value = "";
      } else if (latest.is_failed || latest.is_stale) {
        errorText.value = latest.reason || "班次附件成品生成失败，请重试";
        statusText.value = "班次附件成品生成失败，请重试。";
      } else {
        statusText.value = "本楼任务仍在后台处理，可稍后刷新查看结果";
        errorText.value = "";
      }
    } catch (error) {
      if (isRevisionConflictError(error)) {
        beginRemoteSaveRefresh();
        await loadReviewData({
          background: false,
          mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full",
        });
        statusText.value = "已同步最新审核内容";
      } else {
        errorText.value = String(error?.message || error || "生成成品失败");
        statusText.value = "生成成品失败，请处理后重试。";
      }
    } finally {
      confirming.value = false;
      if (preparingDelivery) preparingDelivery.value = false;
    }
  }

  async function retryCloudSheetSync(getJobApi) {
    if (!retryCloudSyncActionBase.value.allowed) {
      statusText.value = retryCloudSyncActionVm.value.disabledReason || "";
      return;
    }
    if (!buildingCode || !session.value || retryingCloudSync.value) return;
    retryingCloudSync.value = true;
    errorText.value = "";
    statusText.value = "正在重试班次附件多维记录...";
    try {
      const response = await retryHandoverReviewCloudSyncApi(buildingCode, {
        session_id: session.value.session_id,
      });
      const jobId = String(response?.job?.job_id || response?.job_id || "").trim();
      if (!jobId) {
        throw new Error("班次附件多维记录重试任务提交失败");
      }
      statusText.value = "已提交班次附件多维记录同步任务，正在处理中...";
      void (async () => {
        const job = await waitForBackgroundJob(jobId, getJobApi, { timeoutMs: 10 * 60 * 1000 });
        if (!job) return;
        await loadReviewData({ background: true });
        if (job.status === "success") {
          const result = job?.result && typeof job.result === "object" ? job.result : {};
          applyPayloadMeta(result || {});
          broadcastHandoverReviewStatusChange(result || {});
          const retryStatus = String(result.status || "").trim().toLowerCase();
          if (retryStatus === "ok" || retryStatus === "success") {
            statusText.value = resolveOperationFeedbackText(result, "班次附件多维记录上传成功");
            errorText.value = "";
          } else if (retryStatus === "blocked") {
            errorText.value = resolveOperationFeedbackText(result, "当前楼栋成品尚未就绪，不能重试班次附件多维记录上传。");
          } else {
            const failedRows = Array.isArray(result?.cloud_sheet_sync?.failed_buildings)
              ? result.cloud_sheet_sync.failed_buildings
              : [];
            const failedItem = failedRows.find((item) => String(item?.building || "").trim() === String(building.value || "").trim());
            errorText.value = String(failedItem?.error || "班次附件多维记录上传失败");
            statusText.value = resolveOperationFeedbackText(result, "班次附件多维记录上传失败");
          }
        } else {
          errorText.value = String(job?.error || "班次附件多维记录重试失败");
          statusText.value = "班次附件多维记录上传失败";
        }
      })();
    } catch (error) {
      errorText.value = String(error?.message || error || "班次附件多维记录重试失败");
      statusText.value = "班次附件多维记录上传失败";
    } finally {
      retryingCloudSync.value = false;
    }
  }

  async function updateHistoryCloudSync() {
    if (!updateHistoryCloudSyncActionBase.value.allowed) {
      statusText.value = updateHistoryCloudSyncActionVm.value.disabledReason || "";
      return;
    }
    if (!buildingCode || !session.value) return;
    // History updates must never republish a generation that is no longer current.
    if (!resolveDeliveryGate().can_regenerate) {
      statusText.value = deliveryConflictText(resolveDeliveryGate());
      return;
    }
    if (dirty.value) {
      const saved = await saveDocument({ reason: "cloud_update", allowSubstationFailure: true });
      if (!saved) return;
    }
    updatingHistoryCloudSync.value = true;
    errorText.value = "";
    statusText.value = "正在更新历史班次附件多维记录...";
    try {
      const response = await updateHandoverReviewCloudSyncApi(buildingCode, {
        session_id: session.value.session_id,
        client_id: reviewClientId,
      });
      applyPayloadMeta(response || {});
      broadcastHandoverReviewStatusChange(response || {});
      const updateStatus = String(response.status || "").trim().toLowerCase();
      if (updateStatus === "ok" || updateStatus === "success") {
        statusText.value = resolveOperationFeedbackText(response, "历史班次附件多维记录已更新");
      } else {
        errorText.value = String(response?.cloud_sheet_sync?.failed_buildings?.[0]?.error || response?.status || "历史班次附件多维记录更新失败");
        statusText.value = resolveOperationFeedbackText(response, "历史班次附件多维记录更新失败");
      }
    } catch (error) {
      errorText.value = String(error?.message || error || "历史班次附件多维记录更新失败");
      statusText.value = "历史班次附件多维记录更新失败";
    } finally {
      updatingHistoryCloudSync.value = false;
    }
  }

  async function downloadCurrentReviewFile() {
    if (!downloadActionBase.value.allowed) {
      statusText.value = downloadActionVm.value.disabledReason || "";
      return;
    }
    // 下载与成品提交解耦：不再要求成品就绪；但成品正在生成（排队/准备）期间暂缓下载，与后端硬限制一致。
    const gate = resolveDeliveryGate();
    if (gate.is_busy) {
      statusText.value = deliveryConflictText(gate);
      return;
    }
    if (saving.value || regenerating.value || confirming.value || preparingDelivery?.value || cloudSyncBusy.value || syncingRemoteRevision.value || capacityImageSending.value) {
      statusText.value = "请先等待当前保存或同步完成后再下载。";
      return;
    }
    const sessionId = String(session.value?.session_id || "").trim();
    if (!buildingCode || !sessionId) {
      statusText.value = downloadActionVm.value.disabledReason || "";
      return;
    }
    if (dirty.value) {
      const saved = await saveDocument({ reason: "download", allowSubstationFailure: true });
      if (!saved) return;
    }
    downloading.value = true;
    errorText.value = "";
    statusText.value = "正在下载本楼班次附件成品...";
    try {
      const url = buildHandoverReviewDownloadUrl(buildingCode, sessionId, {
        client_id: reviewClientId,
        ts: Date.now(),
      });
      const result = await triggerBrowserDownload(url, session.value?.output_file || "班次附件.xlsx");
      statusText.value = result?.warning
        ? `成品已下载，但部分填充失败：${result.warning}`
        : "本楼班次附件成品已下载";
    } catch (error) {
      if (isRevisionConflictError(error)) {
        await refreshAfterRevisionConflict();
        return;
      }
      errorText.value = String(error?.message || error || "下载失败");
    } finally {
      downloading.value = false;
    }
  }

  async function downloadCurrentCapacityReviewFile() {
    if (!capacityDownloadActionBase.value.allowed) {
      statusText.value = capacityDownloadActionVm.value.disabledReason || "";
      return;
    }
    // 下载与成品提交解耦：不再要求成品就绪；但成品正在生成（排队/准备）期间暂缓下载，与后端硬限制一致。
    const gate = resolveDeliveryGate();
    if (gate.is_busy) {
      statusText.value = deliveryConflictText(gate);
      return;
    }
    if (saving.value || regenerating.value || confirming.value || preparingDelivery?.value || cloudSyncBusy.value || syncingRemoteRevision.value || capacityDownloading.value || capacityImageSending.value) {
      statusText.value = "请先等待当前保存或同步完成后再下载。";
      return;
    }
    if (dirty.value) {
      const saved = await saveDocument({ reason: "capacity_download", allowSubstationFailure: true });
      if (!saved) return;
    }
    if (!session.value || !session.value.session_id) {
      statusText.value = capacityDownloadActionVm.value.disabledReason || "";
      return;
    }
    const sessionId = String(session.value?.session_id || "").trim();
    const capacityOutputFile = String(session.value?.capacity_output_file || "").trim();
    if (!buildingCode || !sessionId) {
      statusText.value = capacityDownloadActionVm.value.disabledReason || "";
      return;
    }
    capacityDownloading.value = true;
    errorText.value = "";
    statusText.value = "正在下载本楼容量表成品...";
    try {
      const url = buildHandoverReviewCapacityDownloadUrl(buildingCode, sessionId, {
        client_id: reviewClientId,
        ts: Date.now(),
      });
      const result = await triggerBrowserDownload(url, capacityOutputFile || "交接班容量报表.xlsx");
      statusText.value = result?.warning
        ? `成品已下载，但部分填充失败：${result.warning}`
        : "本楼容量表成品已下载";
    } catch (error) {
      if (isRevisionConflictError(error)) {
        await refreshAfterRevisionConflict();
        return;
      }
      errorText.value = String(error?.message || error || "下载失败");
    } finally {
      capacityDownloading.value = false;
    }
  }

  async function sendCurrentCapacityImage(getJobApi) {
    if (!capacityImageSendActionBase.value.allowed) {
      statusText.value = capacityImageSendActionVm.value.disabledReason || "";
      return;
    }
    // Sending is always a manual click; generation never sends on its own.
    const gate = resolveDeliveryGate();
    if (!gate.can_send) {
      statusText.value = deliveryConflictText(gate);
      return;
    }
    if (
      saving.value
      || confirming.value
      || preparingDelivery?.value
      || cloudSyncBusy.value
      || downloading.value
      || capacityDownloading.value
      || regenerating.value
      || syncingRemoteRevision.value
      || capacityImageSending.value
    ) {
      statusText.value = "请先等待当前保存或同步完成后再发送。";
      return;
    }
    if (dirty.value) {
      const saved = await saveDocument({ reason: "capacity_image_send", allowSubstationFailure: true });
      if (!saved) return;
    }
    const sessionId = String(session.value?.session_id || "").trim();
    if (!buildingCode || !sessionId) {
      statusText.value = capacityImageSendActionVm.value.disabledReason || "";
      return;
    }
    capacityImageSending.value = true;
    errorText.value = "";
    statusText.value = "正在发送审核文本和容量表图片...";
    try {
      const prepared = await prepareHandoverReviewCapacityImageApi(buildingCode, {
        session_id: sessionId,
        client_id: reviewClientId,
        generation_id: gate.delivery.generation_id,
        trigger_source: "capacity_image_send_button",
      });
      const sendToken = String(prepared?.send_token || "").trim();
      if (!sendToken) {
        throw new Error("发送确认令牌获取失败，请刷新审核页后重试");
      }
      const response = await sendHandoverReviewCapacityImageApi(buildingCode, {
        session_id: sessionId,
        client_id: reviewClientId,
        generation_id: gate.delivery.generation_id,
        trigger_source: "capacity_image_send_button",
        send_token: sendToken,
      });
      try {
        await loadReviewData({ background: true });
      } catch (_error) {
        // Sending result is authoritative; refresh failures should not hide it.
      }
      const delivery = response?.capacity_image_delivery && typeof response.capacity_image_delivery === "object"
        ? response.capacity_image_delivery
        : {};
      const failedRecipients = Array.isArray(response?.failed_recipients)
        ? response.failed_recipients
        : (Array.isArray(delivery.failed_recipients) ? delivery.failed_recipients : []);
      if (response?.ok === true && String(response?.status || delivery.status || "").trim().toLowerCase() === "success") {
        statusText.value = "审核文本和容量表图片发送成功";
        errorText.value = "";
      } else {
        const detail = failedRecipients
          .map((item) => `${item.note || item.open_id || "-"}(${item.step || "-"}): ${item.error || "发送失败"}`)
          .join("；");
        let baseError = String(response?.error || delivery.error || "").trim();
        if (baseError.includes("部分收件人") || baseError.includes("存在收件人") || baseError === "发送失败，详见收件人明细") {
          baseError = detail ? "" : "审核文本和容量表图片发送失败";
        }
        errorText.value = [baseError, detail].filter(Boolean).join("；") || "审核文本和容量表图片发送失败";
        statusText.value = "审核文本和容量表图片发送失败";
      }
      capacityImageSending.value = false;
    } catch (error) {
      errorText.value = String(error?.message || error || "审核文本和容量表图片发送失败");
      statusText.value = "审核文本和容量表图片发送失败";
      capacityImageSending.value = false;
    }
  }

  async function regenerateCurrentHandover(getJobApi, sourceFiles = null) {
    if (sourceFiles && (!sourceFiles.handover || !sourceFiles.capacity)) {
      errorText.value = "请同时选择交接班日志源文件和交接班容量报表源文件";
      return;
    }
    if (!regenerateActionBase.value.allowed) {
      statusText.value = regenerateActionVm.value.disabledReason || "";
      return;
    }
    // A queued/preparing artifact job must finish or fail before regenerating again.
    const deliveryGate = resolveDeliveryGate();
    if (!deliveryGate.can_regenerate) {
      statusText.value = deliveryConflictText(deliveryGate);
      return;
    }
    if (
      saving.value
      || confirming.value
      || preparingDelivery?.value
      || cloudSyncBusy.value
      || downloading.value
      || capacityDownloading.value
      || capacityImageSending.value
      || syncingRemoteRevision.value
      || regenerating.value
    ) {
      statusText.value = "请先等待当前保存或同步完成后再重新生成。";
      return;
    }
    const sessionId = String(session.value?.session_id || "").trim();
    const dutyDate = String(session.value?.duty_date || reviewContext?.value?.duty_date || activeRouteSelection.value?.dutyDate || "").trim();
    const dutyShift = String(session.value?.duty_shift || reviewContext?.value?.duty_shift || activeRouteSelection.value?.dutyShift || "").trim().toLowerCase();
    const canGenerateWithoutSession = Boolean(!sessionId && dutyDate && (dutyShift === "day" || dutyShift === "night"));
    if (!buildingCode || (!sessionId && !canGenerateWithoutSession)) {
      statusText.value = regenerateActionVm.value.disabledReason || "";
      return;
    }
    clearSaveTimers();
    regenerating.value = true;
    errorText.value = "";
    statusText.value = sessionId
      ? (dirty.value
        ? "正在重新生成交接班及容量表，当前未保存修改将被覆盖..."
        : "正在重新生成交接班及容量表...")
      : "正在生成当前班次交接班及容量表...";
    try {
      const request = {
        client_id: reviewClientId,
      };
      if (sessionId) {
        request.session_id = sessionId;
      } else {
        request.duty_date = dutyDate;
        request.duty_shift = dutyShift;
      }
      let response;
      if (sourceFiles) {
        statusText.value = "正在上传两个源文件并提交生成任务...";
        const form = new FormData();
        Object.entries(request).forEach(([key, value]) => form.append(key, value));
        form.append("handover_source_file", sourceFiles.handover);
        form.append("capacity_source_file", sourceFiles.capacity);
        response = await regenerateHandoverReviewFromFilesApi(buildingCode, form);
      } else {
        response = await regenerateHandoverReviewApi(buildingCode, request);
      }
      const jobId = String(response?.job?.job_id || response?.job_id || "").trim();
      if (!jobId) {
        throw new Error(sessionId ? "重新生成任务提交失败" : "生成任务提交失败");
      }
      if (sourceFiles) statusText.value = "两份源文件已上传，等待本楼生成交接班日志和容量表...";
      const job = await waitForBackgroundJob(jobId, getJobApi, { timeoutMs: 10 * 60 * 1000, intervalMs: 1500 });
      if (!job) {
        throw new Error(sessionId ? "重新生成任务等待超时，请稍后刷新查看结果" : "生成任务等待超时，请稍后刷新查看结果");
      }
      await loadReviewData({
        background: false,
        mode: shouldPreferBootstrapLoad({ forceLatest: true }) ? "bootstrap" : "full",
      });
      if (job.status === "success") {
        dirty.value = false;
        statusText.value = sourceFiles ? "已使用上传的两个源文件生成，本楼本班次自动生成已跳过" : (sessionId ? "交接班日志和容量表已重新生成" : "当前班次交接班日志和容量表已生成");
        errorText.value = "";
      } else {
        errorText.value = String(job?.error || job?.summary || (sessionId ? "重新生成失败" : "生成失败"));
        statusText.value = sessionId ? "重新生成失败，请处理后重试。" : "生成失败，请处理后重试。";
      }
    } catch (error) {
      try {
        await loadReviewData({ background: false, mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full" });
      } catch (_error) {
        // Keep the original regenerate error visible.
      }
      errorText.value = String(error?.message || error || (sessionId ? "重新生成失败" : "生成失败"));
      statusText.value = sessionId ? "重新生成失败，请处理后重试。" : "生成失败，请处理后重试。";
    } finally {
      regenerating.value = false;
    }
  }

  async function refreshData() {
    if (!refreshActionBase.value.allowed) {
      statusText.value = refreshActionVm.value.disabledReason || "";
      return;
    }
    clearSaveTimers();
    if (dirty.value) {
      const saved = await saveDocument({ reason: "refresh", allowSubstationFailure: true });
      if (!saved) return;
    }
    needsRefresh.value = false;
    await loadReviewData({
      background: false,
      mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full",
    });
  }

  async function saveCurrentReview() {
    if (!dirty.value) {
      statusText.value = isHistoryMode.value ? "历史交接班日志已保存" : "已保存";
      return;
    }
    await saveDocument({ reason: "manual" });
  }

  return {
    switchToSession,
    onHistorySelectionChange,
    returnToLatestSession,
    toggleConfirm,
    // Same entry point the "生成成品并提交" button uses.
    prepareAttachmentDelivery: toggleConfirm,
    retryCloudSheetSync,
    updateHistoryCloudSync,
    downloadCurrentReviewFile,
    downloadCurrentCapacityReviewFile,
    sendCurrentCapacityImage,
    regenerateCurrentHandover,
    refreshData,
    saveCurrentReview,
    resolveDeliveryGate,
  };
}
