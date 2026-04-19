export function createHandoverReviewOverviewUiHelpers(options = {}) {
  const {
    computed,
    handoverReviewOverview,
    isActionLocked,
    actionKeyHandoverConfirmAll = "",
    actionKeyHandoverCloudRetryAll = "",
    actionKeyHandoverFollowupContinue = "",
  } = options;

  const isHandoverConfirmAllLocked = computed(() => isActionLocked(actionKeyHandoverConfirmAll));
  const isHandoverCloudRetryAllLocked = computed(() => isActionLocked(actionKeyHandoverCloudRetryAll));
  const handoverConfirmAllActionBase = computed(() => handoverReviewOverview.value?.actions?.confirmAll || {
    allowed: false,
    pending: false,
    visible: false,
    label: "一键全确认",
    disabledReason: "",
  });
  const handoverCloudRetryAllActionBase = computed(() => handoverReviewOverview.value?.actions?.retryCloudSyncAll || {
    allowed: false,
    pending: false,
    visible: false,
    label: "一键全部重试云表上传",
    disabledReason: "",
  });
  const handoverFollowupContinueActionBase = computed(() => handoverReviewOverview.value?.actions?.continueFollowup || {
    allowed: false,
    pending: false,
    visible: false,
    label: "继续后续上传",
    disabledReason: "",
  });
  const handoverConfirmAllButtonText = computed(() => {
    if (isHandoverConfirmAllLocked.value) return "确认并上传中...";
    return handoverConfirmAllActionBase.value.label || "一键全确认";
  });
  const isHandoverConfirmAllDisabled = computed(() =>
    Boolean(isHandoverConfirmAllLocked.value || !handoverConfirmAllActionBase.value.allowed),
  );
  const canShowHandoverCloudRetryAll = computed(() =>
    Boolean(handoverCloudRetryAllActionBase.value.visible),
  );
  const handoverCloudRetryAllButtonText = computed(() => {
    if (isHandoverCloudRetryAllLocked.value) return "重试中...";
    return handoverCloudRetryAllActionBase.value.label || "一键全部重试云表上传";
  });
  const isHandoverCloudRetryAllDisabled = computed(() => {
    if (isHandoverCloudRetryAllLocked.value) return true;
    return !handoverCloudRetryAllActionBase.value.allowed;
  });
  const isHandoverFollowupContinueLocked = computed(() => isActionLocked(actionKeyHandoverFollowupContinue));
  const handoverFollowupBatchKey = computed(() => String(handoverReviewOverview.value?.batchKey || "").trim());
  const canShowHandoverFollowupContinue = computed(
    () => Boolean(handoverFollowupContinueActionBase.value.visible),
  );
  const handoverFollowupContinueButtonText = computed(() => {
    if (isHandoverFollowupContinueLocked.value) return "继续上传中...";
    return handoverFollowupContinueActionBase.value.label || "继续后续上传";
  });
  const isHandoverFollowupContinueDisabled = computed(() =>
    Boolean(isHandoverFollowupContinueLocked.value || !handoverFollowupContinueActionBase.value.allowed),
  );

  return {
    isHandoverConfirmAllLocked,
    isHandoverCloudRetryAllLocked,
    handoverConfirmAllActionBase,
    handoverCloudRetryAllActionBase,
    handoverFollowupContinueActionBase,
    handoverConfirmAllButtonText,
    isHandoverConfirmAllDisabled,
    canShowHandoverCloudRetryAll,
    handoverCloudRetryAllButtonText,
    isHandoverCloudRetryAllDisabled,
    isHandoverFollowupContinueLocked,
    handoverFollowupBatchKey,
    canShowHandoverFollowupContinue,
    handoverFollowupContinueButtonText,
    isHandoverFollowupContinueDisabled,
  };
}
