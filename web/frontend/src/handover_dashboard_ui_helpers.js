export function createHandoverDashboardUiHelpers(options = {}) {
  const {
    health,
    handoverReviewOverview,
    handoverDailyReportActions,
    homeQuickActionsById,
    statusQuickActionsById,
    isSourceCacheRefreshCurrentHourLocked,
    isSourceCacheRefreshAlarmManualLocked,
    isActionLocked,
    reviewLinkSendActionKeyPrefix = "",
    openDashboardPage,
    setDashboardActiveModule,
    refreshCurrentHourSourceCache,
    refreshManualAlarmSourceCache,
    openConfigPage,
  } = options || {};

  function getHandoverReviewLinkSendActionKey(building, batchKey = "") {
    const buildingText = String(building || "").trim();
    const targetBatchKey = String(
      batchKey || health?.handover?.review_status?.batch_key || "manual-test",
    ).trim();
    return `${String(reviewLinkSendActionKeyPrefix || "").trim()}${targetBatchKey}:${buildingText}`;
  }

  function getHandoverReviewLinkSendAction(rowLike) {
    const row = rowLike && typeof rowLike === "object" ? rowLike : {};
    const action = row?.actions?.reviewLinkSend && typeof row.actions.reviewLinkSend === "object"
      ? row.actions.reviewLinkSend
      : {};
    return {
      allowed: action.allowed !== false,
      pending: Boolean(action.pending),
      label: String(action.label || "").trim() || "手动发送审核链接",
      disabledReason: String(action.disabledReason || action.disabled_reason || "").trim(),
    };
  }

  function isHandoverReviewLinkSendDisabled(rowLike) {
    const action = getHandoverReviewLinkSendAction(rowLike);
    if (action.pending || action.allowed === false) return true;
    const building = rowLike && typeof rowLike === "object" ? rowLike.building : rowLike;
    return Boolean(
      isActionLocked?.(
        getHandoverReviewLinkSendActionKey(
          building,
          handoverReviewOverview?.value?.batchKey || "",
        ),
      ),
    );
  }

  function getHandoverReviewLinkSendButtonText(rowLike) {
    const action = getHandoverReviewLinkSendAction(rowLike);
    const building = rowLike && typeof rowLike === "object" ? rowLike.building : rowLike;
    if (
      isActionLocked?.(
        getHandoverReviewLinkSendActionKey(
          building,
          handoverReviewOverview?.value?.batchKey || "",
        ),
      )
    ) {
      return "发送中...";
    }
    return action.label || "手动发送审核链接";
  }

  function getHandoverReviewLinkSendDisabledReason(rowLike) {
    const action = getHandoverReviewLinkSendAction(rowLike);
    if (action.pending) return action.disabledReason || "请求处理中，请稍候";
    return action.disabledReason || "";
  }

  function getHandoverDailyReportAction(actionName, fallbackLabel = "等待后端动作") {
    const action =
      handoverDailyReportActions?.value &&
      typeof handoverDailyReportActions.value === "object" &&
      handoverDailyReportActions.value[actionName] &&
      typeof handoverDailyReportActions.value[actionName] === "object"
        ? handoverDailyReportActions.value[actionName]
        : null;
    return action || {
      id: "",
      allowed: false,
      pending: false,
      label: fallbackLabel,
      disabledReason: "",
      reasonCode: "daily_report_state_not_ready",
    };
  }

  function isHandoverDailyReportActionDisabled(actionName) {
    const action = getHandoverDailyReportAction(actionName);
    return action.pending || action.allowed === false;
  }

  function getHandoverDailyReportActionButtonText(actionName, fallbackLabel = "等待后端动作") {
    const action = getHandoverDailyReportAction(actionName, fallbackLabel);
    return String(action.label || "").trim() || fallbackLabel;
  }

  function getHandoverDailyReportActionDisabledReason(actionName) {
    const action = getHandoverDailyReportAction(actionName);
    if (action.pending) return action.disabledReason || "请求处理中，请稍候";
    return action.disabledReason || "";
  }

  function getHandoverDailyReportAssetAction(asset, actionName, fallbackLabel = "等待后端动作") {
    const action =
      asset &&
      typeof asset === "object" &&
      asset.actions &&
      typeof asset.actions === "object" &&
      asset.actions[actionName] &&
      typeof asset.actions[actionName] === "object"
        ? asset.actions[actionName]
        : null;
    return action || {
      id: "",
      allowed: false,
      pending: false,
      label: fallbackLabel,
      disabledReason: "",
      reasonCode: "daily_report_asset_state_not_ready",
    };
  }

  function isHandoverDailyReportAssetActionDisabled(asset, actionName) {
    const action = getHandoverDailyReportAssetAction(asset, actionName);
    return action.pending || action.allowed === false;
  }

  function getHandoverDailyReportAssetActionButtonText(asset, actionName, fallbackLabel = "等待后端动作") {
    const action = getHandoverDailyReportAssetAction(asset, actionName, fallbackLabel);
    return String(action.label || "").trim() || fallbackLabel;
  }

  function getHandoverDailyReportAssetActionDisabledReason(asset, actionName) {
    const action = getHandoverDailyReportAssetAction(asset, actionName);
    if (action.pending) return action.disabledReason || "请求处理中，请稍候";
    return action.disabledReason || "";
  }

  function getHomeQuickActionState(actionLike) {
    const requestedId = actionLike && typeof actionLike === "object"
      ? String(actionLike?.id || "").trim().toLowerCase()
      : String(actionLike || "").trim().toLowerCase();
    const backendAction =
      (requestedId && homeQuickActionsById?.value?.[requestedId])
      || (requestedId && statusQuickActionsById?.value?.[requestedId])
      || null;
    const fallbackAction = actionLike && typeof actionLike === "object" ? actionLike : {};
    const rawAction = backendAction || fallbackAction || { id: requestedId };
    const action = String((backendAction?.id || rawAction?.id || requestedId) || "").trim().toLowerCase();
    const base = {
      id: action,
      label: String(backendAction?.label || rawAction?.label || "").trim(),
      allowed: backendAction ? (backendAction.allowed !== false) : (rawAction?.allowed !== false),
      pending: backendAction ? Boolean(backendAction.pending) : Boolean(rawAction?.pending),
      disabledReason: String(
        backendAction?.disabledReason
        || backendAction?.disabled_reason
        || rawAction?.disabledReason
        || rawAction?.disabled_reason
        || "",
      ).trim(),
    };
    let localPending = false;
    if (action === "refresh_current_hour") {
      localPending = Boolean(isSourceCacheRefreshCurrentHourLocked?.value);
    } else if (action === "refresh_manual_alarm") {
      localPending = Boolean(isSourceCacheRefreshAlarmManualLocked?.value);
    }
    return {
      ...base,
      pending: base.pending || localPending,
    };
  }

  function getStatusQuickAction(actionId) {
    const targetId = String(actionId || "").trim().toLowerCase();
    if (!targetId) return { id: "", label: "", allowed: false, pending: false, disabledReason: "" };
    const matched = statusQuickActionsById?.value?.[targetId] || homeQuickActionsById?.value?.[targetId];
    if (matched && typeof matched === "object") return matched;
    return {
      id: targetId,
      label: "等待后端动作",
      allowed: false,
      pending: false,
      disabledReason: "",
    };
  }

  async function runHomeQuickAction(actionLike) {
    const actionState = getHomeQuickActionState(actionLike);
    const action = String(actionState.id || "").trim().toLowerCase();
    if (!action || actionState.allowed === false || actionState.pending) return;
    if (action === "open_auto_flow") {
      openDashboardPage?.();
      setDashboardActiveModule?.("auto_flow");
      return;
    }
    if (action === "open_handover_log") {
      openDashboardPage?.();
      setDashboardActiveModule?.("handover_log");
      return;
    }
    if (action === "open_alarm_upload") {
      openDashboardPage?.();
      setDashboardActiveModule?.("alarm_event_upload");
      return;
    }
    if (action === "refresh_current_hour") {
      await refreshCurrentHourSourceCache?.();
      return;
    }
    if (action === "refresh_manual_alarm") {
      await refreshManualAlarmSourceCache?.();
      return;
    }
    if (action === "open_config") {
      openConfigPage?.();
    }
  }

  function isHomeQuickActionLocked(actionLike) {
    const actionState = getHomeQuickActionState(actionLike);
    if (!actionState.id) return false;
    return actionState.pending || actionState.allowed === false;
  }

  function getHomeQuickActionButtonText(actionLike) {
    const actionState = getHomeQuickActionState(actionLike);
    return actionState.label || "等待后端动作";
  }

  function getHomeQuickActionDisabledReason(actionLike) {
    const actionState = getHomeQuickActionState(actionLike);
    if (actionState.pending) return actionState.disabledReason || "请求处理中，请稍候";
    return actionState.disabledReason || "";
  }

  return {
    getHandoverReviewLinkSendActionKey,
    getHandoverReviewLinkSendAction,
    isHandoverReviewLinkSendDisabled,
    getHandoverReviewLinkSendButtonText,
    getHandoverReviewLinkSendDisabledReason,
    getHandoverDailyReportActionButtonText,
    getHandoverDailyReportActionDisabledReason,
    isHandoverDailyReportActionDisabled,
    getHandoverDailyReportAssetActionButtonText,
    getHandoverDailyReportAssetActionDisabledReason,
    isHandoverDailyReportAssetActionDisabled,
    getStatusQuickAction,
    runHomeQuickAction,
    isHomeQuickActionLocked,
    getHomeQuickActionButtonText,
    getHomeQuickActionDisabledReason,
  };
}
