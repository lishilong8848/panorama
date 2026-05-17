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
    beginRemoteSaveRefresh,
    isRevisionConflictError,
    applyPayloadMeta,
    broadcastHandoverReviewStatusChange,
    resolveOperationFeedbackText,
    syncReviewSelectionToUrl,
    confirmHandoverReviewApi,
    retryHandoverReviewCloudSyncApi,
    updateHandoverReviewCloudSyncApi,
    sendHandoverReviewCapacityImageApi,
    regenerateHandoverReviewApi,
    buildHandoverReviewDownloadUrl,
    buildHandoverReviewCapacityDownloadUrl,
    triggerBrowserDownload,
  } = options;

  function isIncompleteJobStatus(status) {
    const normalized = String(status || "").trim().toLowerCase();
    return normalized === "queued" || normalized === "running" || normalized === "waiting_resource";
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

  async function waitForBackgroundJob(jobId, getJobApi, { timeoutMs = 120000, intervalMs = 1500 } = {}) {
    const targetJobId = String(jobId || "").trim();
    if (!targetJobId) return null;
    const startedAt = Date.now();
    while (Date.now() - startedAt <= timeoutMs) {
      try {
        const job = await getJobApi(targetJobId);
        if (!isIncompleteJobStatus(job?.status)) {
          return job;
        }
      } catch (_error) {
        // Ignore transient polling failures and keep waiting.
      }
      await new Promise((resolve) => window.setTimeout(resolve, intervalMs));
    }
    return null;
  }

  async function switchToSession(sessionId, { toLatest = false } = {}) {
    const nextSessionId = String(sessionId || "").trim();
    if (!toLatest && (!nextSessionId || nextSessionId === selectedSessionId.value)) return;
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
      dutyDate: "",
      dutyShift: "",
    };
    syncReviewSelectionToUrl({ sessionId: toLatest ? "" : nextSessionId, isLatest: toLatest });
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
      || cloudSyncBusy.value
      || syncingRemoteRevision.value
      || needsRefresh.value
      || staleRevisionConflict.value
    ) return;
    if (dirty.value) {
      const saved = await saveDocument({ reason: "confirm" });
      if (!saved) return;
      if (dirty.value) {
        statusText.value = "审核内容仍有未保存修改，请保存完成后再确认。";
        return;
      }
    }
    confirming.value = true;
    errorText.value = "";
    statusText.value = "正在获取审核页编辑锁...";
    const wasConfirmedBefore = Boolean(session.value?.confirmed);
    try {
      if (typeof ensureEditingLock === "function") {
        const locked = await ensureEditingLock();
        if (!locked) {
          errorText.value = "当前审核页编辑锁获取失败，请确认没有其他终端编辑后重试";
          statusText.value = "确认失败，请处理后重试。";
          return;
        }
      }
      statusText.value = wasConfirmedBefore
        ? "正在同步交接班文件并重传本楼云文档..."
        : "正在同步交接班文件并确认上传本楼云文档...";
      const request = {
        session_id: session.value.session_id,
        base_revision: session.value.revision,
        client_id: reviewClientId,
      };
      const response = await confirmHandoverReviewApi(buildingCode, request);
      applyPayloadMeta(response || {});
      broadcastHandoverReviewStatusChange(response || {});
      staleRevisionConflict.value = false;
      needsRefresh.value = false;
      statusText.value = resolveOperationFeedbackText(
        response,
        wasConfirmedBefore ? "本楼云文档已重新提交上传" : "已确认本楼并提交云文档上传",
      );
    } catch (error) {
      if (isRevisionConflictError(error)) {
        beginRemoteSaveRefresh();
        await loadReviewData({
          background: false,
          mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full",
        });
        statusText.value = "已同步最新审核内容";
      } else {
        errorText.value = String(error?.message || error || "确认失败");
        statusText.value = "确认失败，请处理后重试。";
      }
    } finally {
      confirming.value = false;
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
    statusText.value = "正在重试云表同步...";
    try {
      const response = await retryHandoverReviewCloudSyncApi(buildingCode, {
        session_id: session.value.session_id,
      });
      const jobId = String(response?.job?.job_id || response?.job_id || "").trim();
      if (!jobId) {
        throw new Error("云表重试任务提交失败");
      }
      statusText.value = "已提交云表同步任务，正在处理中...";
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
            statusText.value = resolveOperationFeedbackText(result, "云表上传成功");
            errorText.value = "";
          } else if (retryStatus === "blocked") {
            errorText.value = resolveOperationFeedbackText(result, "当前楼栋尚未确认，不能重试云表上传。");
          } else {
            const failedRows = Array.isArray(result?.cloud_sheet_sync?.failed_buildings)
              ? result.cloud_sheet_sync.failed_buildings
              : [];
            const failedItem = failedRows.find((item) => String(item?.building || "").trim() === String(building.value || "").trim());
            errorText.value = String(failedItem?.error || "云表上传失败");
            statusText.value = resolveOperationFeedbackText(result, "云表上传失败");
          }
        } else {
          errorText.value = String(job?.error || "云表重试失败");
          statusText.value = "云表上传失败";
        }
      })();
    } catch (error) {
      errorText.value = String(error?.message || error || "云表重试失败");
      statusText.value = "云表上传失败";
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
    if (dirty.value) {
      const saved = await saveDocument({ reason: "cloud_update" });
      if (!saved) return;
    }
    updatingHistoryCloudSync.value = true;
    errorText.value = "";
    statusText.value = "正在更新历史云文档...";
    try {
      const response = await updateHandoverReviewCloudSyncApi(buildingCode, {
        session_id: session.value.session_id,
        client_id: reviewClientId,
      });
      applyPayloadMeta(response || {});
      broadcastHandoverReviewStatusChange(response || {});
      const updateStatus = String(response.status || "").trim().toLowerCase();
      if (updateStatus === "ok" || updateStatus === "success") {
        statusText.value = resolveOperationFeedbackText(response, "历史云文档已更新");
      } else {
        errorText.value = String(response?.cloud_sheet_sync?.failed_buildings?.[0]?.error || response?.status || "历史云文档更新失败");
        statusText.value = resolveOperationFeedbackText(response, "历史云文档更新失败");
      }
    } catch (error) {
      errorText.value = String(error?.message || error || "历史云文档更新失败");
      statusText.value = "历史云文档更新失败";
    } finally {
      updatingHistoryCloudSync.value = false;
    }
  }

  async function downloadCurrentReviewFile() {
    if (!downloadActionBase.value.allowed) {
      statusText.value = downloadActionVm.value.disabledReason || "";
      return;
    }
    if (saving.value || regenerating.value || confirming.value || cloudSyncBusy.value || syncingRemoteRevision.value || capacityImageSending.value) {
      statusText.value = "请先等待当前保存或同步完成后再下载。";
      return;
    }
    const sessionId = String(session.value?.session_id || "").trim();
    if (!buildingCode || !sessionId) {
      statusText.value = downloadActionVm.value.disabledReason || "";
      return;
    }
    if (dirty.value) {
      const saved = await saveDocument({ reason: "download" });
      if (!saved) return;
    }
    downloading.value = true;
    errorText.value = "";
    statusText.value = "正在同步交接班文件...";
    try {
      const url = buildHandoverReviewDownloadUrl(buildingCode, sessionId, {
        client_id: reviewClientId,
        ts: Date.now(),
      });
      const result = await triggerBrowserDownload(url, session.value?.output_file || "交接班日志.xlsx");
      statusText.value = result?.warning
        ? `文件已下载，但部分填充失败：${result.warning}`
        : "交接班文件已下载";
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
    if (saving.value || regenerating.value || confirming.value || cloudSyncBusy.value || syncingRemoteRevision.value || capacityDownloading.value || capacityImageSending.value) {
      statusText.value = "请先等待当前保存或同步完成后再下载。";
      return;
    }
    if (dirty.value) {
      const saved = await saveDocument({ reason: "capacity_download" });
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
    statusText.value = "正在同步并下载容量报表...";
    try {
      const url = buildHandoverReviewCapacityDownloadUrl(buildingCode, sessionId, {
        client_id: reviewClientId,
        ts: Date.now(),
      });
      const result = await triggerBrowserDownload(url, capacityOutputFile || "交接班容量报表.xlsx");
      statusText.value = result?.warning
        ? `文件已下载，但部分填充失败：${result.warning}`
        : "容量报表已下载";
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
    if (
      saving.value
      || confirming.value
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
      const saved = await saveDocument({ reason: "capacity_image_send" });
      if (!saved) return;
    }
    const sessionId = String(session.value?.session_id || "").trim();
    if (!buildingCode || !sessionId) {
      statusText.value = capacityImageSendActionVm.value.disabledReason || "";
      return;
    }
    capacityImageSending.value = true;
    errorText.value = "";
    statusText.value = "正在生成并发送审核文本和容量表图片...";
    try {
      const response = await sendHandoverReviewCapacityImageApi(buildingCode, {
        session_id: sessionId,
        client_id: reviewClientId,
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

  async function regenerateCurrentHandover(getJobApi) {
    if (!regenerateActionBase.value.allowed) {
      statusText.value = regenerateActionVm.value.disabledReason || "";
      return;
    }
    if (
      saving.value
      || confirming.value
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
      const response = await regenerateHandoverReviewApi(buildingCode, request);
      const jobId = String(response?.job?.job_id || response?.job_id || "").trim();
      if (!jobId) {
        throw new Error(sessionId ? "重新生成任务提交失败" : "生成任务提交失败");
      }
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
        statusText.value = sessionId ? "交接班日志和容量表已重新生成" : "当前班次交接班日志和容量表已生成";
        errorText.value = "";
      } else {
        errorText.value = String(job?.error || job?.summary || (sessionId ? "重新生成失败" : "生成失败"));
        statusText.value = sessionId ? "重新生成失败，请处理后重试。" : "生成失败，请处理后重试。";
      }
    } catch (error) {
      errorText.value = String(error?.message || error || (sessionId ? "重新生成失败" : "生成失败"));
      statusText.value = sessionId ? "重新生成失败，请处理后重试。" : "生成失败，请处理后重试。";
      try {
        await loadReviewData({ background: false, mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full" });
      } catch (_error) {
        // Keep the original regenerate error visible.
      }
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
      const saved = await saveDocument({ reason: "manual" });
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
    retryCloudSheetSync,
    updateHistoryCloudSync,
    downloadCurrentReviewFile,
    downloadCurrentCapacityReviewFile,
    sendCurrentCapacityImage,
    regenerateCurrentHandover,
    refreshData,
    saveCurrentReview,
  };
}
