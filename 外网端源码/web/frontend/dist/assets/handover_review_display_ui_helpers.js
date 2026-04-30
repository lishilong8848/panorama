export function createHandoverReviewDisplayUiHelpers(options = {}) {
  const {
    computed,
    session,
    reviewDisplayState,
    historyState,
    historyLoading,
    activeRouteSelection,
    loading,
    saving,
    downloading,
    capacityDownloading,
    capacityImageSending,
    regenerating,
    confirming,
    retryingCloudSync,
    updatingHistoryCloudSync,
    dirty,
    syncingRemoteRevision,
    needsRefresh,
    staleRevisionConflict,
    cloudSyncBusy,
    errorText,
    statusText,
    badgeVm,
    badgeVmFromDisplayItem,
    normalizeDisplayBadge,
    buildReviewActionVmBase,
    emptyReviewCloudSheetVm,
    resolveReviewActionDisabledReasonStrict,
    shiftTextFromCode,
  } = options;

  const selectedSessionId = computed(() => String(historyState.value?.selected_session_id || session.value?.session_id || "").trim());
  const latestSessionId = computed(() => String(historyState.value?.latest_session_id || "").trim());
  const isHistoryMode = computed(() => {
    const backendMode = String(reviewDisplayState.value?.mode?.code || "").trim().toLowerCase();
    if (backendMode === "history") return true;
    if (backendMode === "latest") return false;
    return Boolean(session.value) && !Boolean(historyState.value?.selected_is_latest);
  });
  const historySessions = computed(() => Array.isArray(historyState.value?.sessions) ? historyState.value.sessions : []);
  const selectedSessionInHistoryList = computed(() => Boolean(historyState.value?.selected_in_history_list));
  const selectedSessionIdInListOrEmpty = computed(() => (selectedSessionInHistoryList.value ? selectedSessionId.value : ""));
  const historySelectorHint = computed(() => {
    const backendHint = String(reviewDisplayState.value?.history_hint || "").trim();
    if (backendHint) {
      return backendHint;
    }
    if (historyLoading.value) {
      return "正在加载历史交接班日志...";
    }
    return "";
  });

  const sessionSummary = computed(() => {
    if (!session.value) return "暂无会话";
    const dutyDate = session.value.duty_date || "-";
    return `${dutyDate} / ${shiftTextFromCode(session.value.duty_shift || "")}`;
  });

  const currentDutyDateText = computed(() => String(session.value?.duty_date || "").trim() || "-");
  const currentDutyShiftText = computed(() => shiftTextFromCode(session.value?.duty_shift || ""));
  const currentModeText = computed(() => String(reviewDisplayState.value?.mode?.text || "").trim() || "-");
  const refreshActionBase = computed(() => reviewDisplayState.value.actions.refresh);
  const saveActionBase = computed(() => reviewDisplayState.value.actions.save);
  const downloadActionBase = computed(() => reviewDisplayState.value.actions.download);
  const capacityDownloadActionBase = computed(() => reviewDisplayState.value.actions.capacity_download);
  const capacityImageSendActionBase = computed(() => reviewDisplayState.value.actions.capacity_image_send);
  const regenerateActionBase = computed(() => reviewDisplayState.value.actions.regenerate);
  const confirmActionBase = computed(() => reviewDisplayState.value.actions.confirm);
  const retryCloudSyncActionBase = computed(() => reviewDisplayState.value.actions.retry_cloud_sync);
  const updateHistoryCloudSyncActionBase = computed(() => reviewDisplayState.value.actions.update_history_cloud_sync);
  const returnToLatestActionBase = computed(() => reviewDisplayState.value.actions.return_to_latest);
  const showRefreshAction = computed(() => Boolean(refreshActionBase.value.visible));
  const showSaveAction = computed(() => Boolean(saveActionBase.value.visible));
  const showDownloadAction = computed(() => Boolean(downloadActionBase.value.visible));
  const showCapacityDownloadAction = computed(() => Boolean(capacityDownloadActionBase.value.visible));
  const showCapacityImageSendAction = computed(() => Boolean(capacityImageSendActionBase.value.visible));
  const showRegenerateAction = computed(() => Boolean(regenerateActionBase.value.visible));
  const showConfirmAction = computed(() => Boolean(confirmActionBase.value.visible));
  const showReturnToLatestAction = computed(() => Boolean(returnToLatestActionBase.value.visible));

  const reviewSaveBadge = computed(() => {
    if (errorText.value) return badgeVm("保存异常", "danger", "soft", "error");
    if (syncingRemoteRevision.value) return badgeVm(statusText.value || "同步中", "warning", "soft", "clock");
    if (saving.value) return badgeVm(statusText.value || "正在保存...", "info", "soft", "clock");
    if (dirty.value) return badgeVm("待保存", "warning", "soft", "warn");
    const backendSaveBadge = badgeVmFromDisplayItem(reviewDisplayState.value?.save_state, {
      text: statusText.value || "已保存",
      tone: "success",
      emphasis: "soft",
      icon: "check",
    });
    return backendSaveBadge || badgeVm(statusText.value || "已保存", "success", "soft", "check");
  });
  const reviewCloudSheetVm = computed(() => reviewDisplayState.value?.cloud_sheet || emptyReviewCloudSheetVm());
  const reviewCloudSheetUrl = computed(() => String(reviewCloudSheetVm.value.url || "").trim());
  const reviewHeaderBadges = computed(() => {
    const backendBadges = Array.isArray(reviewDisplayState.value?.header_badges)
      ? reviewDisplayState.value.header_badges.filter((badge) => badge?.text)
      : [];
    const localSaveBadge = {
      code: "save",
      ...reviewSaveBadge.value,
    };
    if (backendBadges.length) {
      return backendBadges.map((badge) => (
        String(badge.code || "").trim().toLowerCase() === "save"
          ? { ...badge, ...localSaveBadge }
          : badge
      ));
    }
    const modeBadge = reviewDisplayState.value?.mode;
    const confirmBadge = reviewDisplayState.value?.confirm_badge;
    const badges = [localSaveBadge];
    if (String(modeBadge?.text || "").trim()) {
      badges.unshift({
        code: "mode",
        ...badgeVm(
          String(modeBadge?.text || "").trim(),
          String(modeBadge?.tone || "neutral").trim() || "neutral",
          String(modeBadge?.emphasis || "outline").trim() || "outline",
          String(modeBadge?.icon || "clock").trim() || "clock",
        ),
      });
    }
    const normalizedConfirmBadge = normalizeDisplayBadge(confirmBadge, {
      code: "pending",
      text: "",
      tone: "warning",
      emphasis: "soft",
      icon: "warn",
    });
    if (normalizedConfirmBadge.text) {
      badges.push({
        code: normalizedConfirmBadge.code || "confirm",
        ...badgeVm(
          normalizedConfirmBadge.text,
          normalizedConfirmBadge.tone,
          normalizedConfirmBadge.emphasis,
          normalizedConfirmBadge.icon,
        ),
      });
    }
    return badges;
  });
  const refreshActionVm = computed(() => {
    return buildReviewActionVmBase({
      baseAction: refreshActionBase.value,
      fallbackLabel: "刷新",
      inFlight: loading.value,
      inFlightText: "刷新中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || confirming.value
        || syncingRemoteRevision.value
        || cloudSyncBusy.value
        || refreshActionBase.value.pending
        || !refreshActionBase.value.allowed,
    });
  });
  const downloadActionVm = computed(() => {
    return buildReviewActionVmBase({
      baseAction: downloadActionBase.value,
      fallbackLabel: "下载交接班日志",
      inFlight: downloading.value,
      inFlightText: "下载中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || syncingRemoteRevision.value
        || downloading.value
        || capacityImageSending.value
        || cloudSyncBusy.value
        || downloadActionBase.value.pending
        || !downloadActionBase.value.allowed,
    });
  });
  const capacityDownloadActionVm = computed(() => {
    return buildReviewActionVmBase({
      baseAction: capacityDownloadActionBase.value,
      fallbackLabel: "下载交接班容量报表",
      inFlight: capacityDownloading.value,
      inFlightText: "下载中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || confirming.value
        || cloudSyncBusy.value
        || syncingRemoteRevision.value
        || capacityDownloading.value
        || capacityImageSending.value
        || capacityDownloadActionBase.value.pending
        || !capacityDownloadActionBase.value.allowed,
    });
  });
  const capacityImageSendActionVm = computed(() => {
    return buildReviewActionVmBase({
      baseAction: capacityImageSendActionBase.value,
      fallbackLabel: "发送容量表图片",
      inFlight: capacityImageSending.value,
      inFlightText: "发送中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || confirming.value
        || cloudSyncBusy.value
        || downloading.value
        || capacityDownloading.value
        || syncingRemoteRevision.value
        || capacityImageSending.value
        || capacityImageSendActionBase.value.pending
        || !capacityImageSendActionBase.value.allowed,
    });
  });
  const regenerateActionVm = computed(() => {
    return buildReviewActionVmBase({
      baseAction: regenerateActionBase.value,
      fallbackLabel: "重新生成交接班及容量表",
      inFlight: regenerating.value,
      inFlightText: "重新生成中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || confirming.value
        || cloudSyncBusy.value
        || downloading.value
        || capacityDownloading.value
        || capacityImageSending.value
        || syncingRemoteRevision.value
        || regenerating.value
        || regenerateActionBase.value.pending
        || !regenerateActionBase.value.allowed,
    });
  });
  const capacityDownloadDisabled = computed(() => Boolean(capacityDownloadActionVm.value.disabled));

  const reviewStatusBanners = computed(() => {
    const rows = [];
    const pushBanner = (text, tone = "neutral") => {
      const normalizedText = String(text || "").trim();
      const normalizedTone = String(tone || "").trim() || "neutral";
      if (!normalizedText) return;
      if (rows.some((item) => item.text === normalizedText && item.tone === normalizedTone)) return;
      rows.push({ text: normalizedText, tone: normalizedTone });
    };
    const backendBanners = Array.isArray(reviewDisplayState.value?.status_banners)
      ? reviewDisplayState.value.status_banners
      : [];
    backendBanners.forEach((banner) => {
      pushBanner(banner?.text, banner?.tone || "neutral");
    });
    const hasLocalOperationBanner =
      loading.value
      || saving.value
      || downloading.value
      || capacityDownloading.value
      || capacityImageSending.value
      || regenerating.value
      || confirming.value
      || retryingCloudSync.value
      || updatingHistoryCloudSync.value
      || cloudSyncBusy.value
      || syncingRemoteRevision.value;
    if (statusText.value && hasLocalOperationBanner && !errorText.value) {
      pushBanner(statusText.value, "info");
    }
    if (errorText.value) {
      pushBanner(errorText.value, "danger");
    }
    return rows;
  });

  const confirmActionVm = computed(() => {
    const base = confirmActionBase.value;
    const baseDisabledReason = resolveReviewActionDisabledReasonStrict(base);
    let localDisabledReason = "";
    if (!base.allowed) {
      localDisabledReason = baseDisabledReason;
    } else if (loading.value) {
      localDisabledReason = "正在加载审核内容，请稍候";
    } else if (saving.value) {
      localDisabledReason = "正在保存审核内容，请稍候";
    } else if (regenerating.value) {
      localDisabledReason = "正在重新生成交接班及容量表，请稍候";
    } else if (confirming.value) {
      localDisabledReason = "确认处理中，请稍候";
    } else if (downloading.value) {
      localDisabledReason = "交接班文件正在下载，请稍候";
    } else if (capacityDownloading.value) {
      localDisabledReason = "容量报表正在下载，请稍候";
    } else if (capacityImageSending.value) {
      localDisabledReason = "容量表图片正在发送，请稍候";
    } else if (retryingCloudSync.value || updatingHistoryCloudSync.value || cloudSyncBusy.value) {
      localDisabledReason = "云表同步处理中，请稍候";
    } else if (syncingRemoteRevision.value) {
      localDisabledReason = "正在同步最新审核内容，请稍候";
    } else if (needsRefresh?.value) {
      localDisabledReason = "审核内容已变化，请刷新后重试";
    } else if (staleRevisionConflict?.value) {
      localDisabledReason = "审核内容版本已变化，请等待同步后重试";
    } else if (base.pending) {
      localDisabledReason = baseDisabledReason || "请求处理中，请稍候";
    }
    const actionVm = buildReviewActionVmBase({
      baseAction: base,
      fallbackLabel: "确认当前楼栋",
      inFlight: confirming.value,
      inFlightText: "处理中...",
      disabled: Boolean(localDisabledReason),
    });
    return {
      text: actionVm.text,
      variant: base.variant || "warning",
      disabled: actionVm.disabled,
      disabledReason: localDisabledReason || actionVm.disabledReason,
    };
  });

  const saveActionVm = computed(() => {
    if (saving.value || saveActionBase.value.pending) {
      return {
        text: "保存中...",
        disabled: true,
        disabledReason: resolveReviewActionDisabledReasonStrict(saveActionBase.value),
      };
    }
    if (!dirty.value) {
      return { text: "已保存", disabled: true, disabledReason: "" };
    }
    return buildReviewActionVmBase({
      baseAction: saveActionBase.value,
      fallbackLabel: "保存",
      inFlight: false,
      inFlightText: "保存中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || confirming.value
        || cloudSyncBusy.value
        || downloading.value
        || capacityDownloading.value
        || capacityImageSending.value
        || syncingRemoteRevision.value
        || saveActionBase.value.pending
        || !saveActionBase.value.allowed,
    });
  });
  const retryCloudSyncActionVm = computed(() => {
    return buildReviewActionVmBase({
      baseAction: retryCloudSyncActionBase.value,
      fallbackLabel: "重试云表上传",
      inFlight: retryingCloudSync.value,
      inFlightText: "重试上传中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || confirming.value
        || cloudSyncBusy.value
        || retryingCloudSync.value
        || retryCloudSyncActionBase.value.pending
        || !retryCloudSyncActionBase.value.allowed,
    });
  });
  const showRetryCloudSyncAction = computed(() => Boolean(retryCloudSyncActionBase.value.visible));
  const updateHistoryCloudSyncActionVm = computed(() => {
    return buildReviewActionVmBase({
      baseAction: updateHistoryCloudSyncActionBase.value,
      fallbackLabel: "更新云文档",
      inFlight: updatingHistoryCloudSync.value,
      inFlightText: "更新中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || confirming.value
        || cloudSyncBusy.value
        || updatingHistoryCloudSync.value
        || updateHistoryCloudSyncActionBase.value.pending
        || !updateHistoryCloudSyncActionBase.value.allowed,
    });
  });
  const showUpdateHistoryCloudSyncAction = computed(() => Boolean(updateHistoryCloudSyncActionBase.value.visible));
  const returnToLatestActionVm = computed(() => {
    return buildReviewActionVmBase({
      baseAction: returnToLatestActionBase.value,
      fallbackLabel: "返回最新",
      inFlight: loading.value && Boolean(activeRouteSelection.value.sessionId),
      inFlightText: "切换中...",
      disabled:
        loading.value
        || saving.value
        || regenerating.value
        || confirming.value
        || cloudSyncBusy.value
        || returnToLatestActionBase.value.pending
        || !returnToLatestActionBase.value.allowed,
    });
  });

  return {
    selectedSessionId,
    latestSessionId,
    isHistoryMode,
    historySessions,
    selectedSessionInHistoryList,
    selectedSessionIdInListOrEmpty,
    historySelectorHint,
    sessionSummary,
    currentDutyDateText,
    currentDutyShiftText,
    currentModeText,
    refreshActionBase,
    saveActionBase,
    downloadActionBase,
    capacityDownloadActionBase,
    capacityImageSendActionBase,
    regenerateActionBase,
    confirmActionBase,
    retryCloudSyncActionBase,
    updateHistoryCloudSyncActionBase,
    returnToLatestActionBase,
    showRefreshAction,
    showSaveAction,
    showDownloadAction,
    showCapacityDownloadAction,
    showCapacityImageSendAction,
    showRegenerateAction,
    showConfirmAction,
    showReturnToLatestAction,
    reviewSaveBadge,
    reviewCloudSheetVm,
    reviewCloudSheetUrl,
    reviewHeaderBadges,
    refreshActionVm,
    downloadActionVm,
    capacityDownloadActionVm,
    capacityImageSendActionVm,
    regenerateActionVm,
    capacityDownloadDisabled,
    reviewStatusBanners,
    confirmActionVm,
    saveActionVm,
    retryCloudSyncActionVm,
    showRetryCloudSyncAction,
    updateHistoryCloudSyncActionVm,
    showUpdateHistoryCloudSyncAction,
    returnToLatestActionVm,
  };
}
