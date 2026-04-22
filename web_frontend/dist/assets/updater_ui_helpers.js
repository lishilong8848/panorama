export function createUpdaterUiHelpers(options = {}) {
  const {
    computed,
    updaterMirrorOverview,
    isActionLocked,
    actionKeyUpdaterCheck = "",
    actionKeyUpdaterApply = "",
    actionKeyUpdaterRestart = "",
    actionKeyUpdaterPublishApproved = "",
    actionKeyUpdaterInternalPeerCheck = "",
    actionKeyUpdaterInternalPeerApply = "",
    actionKeyUpdaterInternalPeerRestart = "",
  } = options;

  function getUpdaterDisabledText(action) {
    const reasonCode = String(action?.reasonCode || "").trim().toLowerCase();
    if (reasonCode === "source_python_run") return "当前为 Python 本地源码运行，已跳过更新。";
    if (reasonCode === "git_not_installed") return "当前电脑未安装 Git，无法执行代码拉取更新。";
    if (reasonCode === "git_repo_missing") return "当前代码目录不是 Git 工作区，无法执行代码拉取更新。";
    if (reasonCode === "git_remote_missing") return "当前未配置 Git 更新仓库地址。";
    if (reasonCode === "shared_root_missing") return "共享目录未配置，无法同步内网代码。";
    return String(action?.disabledReason || "").trim() || "当前运行模式已跳过更新。";
  }

  const updaterMainAction = computed(() => updaterMirrorOverview.value?.actions?.main || {
    id: "check_apply",
    allowed: false,
    pending: false,
    label: "检查并更新",
    disabledReason: "",
    reasonCode: "",
  });
  const updaterInternalPeerCheckAction = computed(() => updaterMirrorOverview.value?.actions?.internal_peer_check || {
    id: "internal_peer_check",
    allowed: false,
    pending: false,
    label: "刷新内网状态",
    disabledReason: "",
    reasonCode: "",
  });
  const updaterInternalPeerApplyAction = computed(() => updaterMirrorOverview.value?.actions?.internal_peer_apply || {
    id: "internal_peer_apply",
    allowed: false,
    pending: false,
    label: "内网端应用代码",
    disabledReason: "",
    reasonCode: "",
  });
  const updaterPublishApprovedAction = computed(() => updaterMirrorOverview.value?.actions?.publish_approved || {
    id: "publish_approved",
    allowed: false,
    pending: false,
    label: "手动同步当前代码",
    disabledReason: "",
    reasonCode: "",
  });
  const updaterInternalPeerRestartAction = computed(() => updaterMirrorOverview.value?.actions?.internal_peer_restart || {
    id: "internal_peer_restart",
    allowed: false,
    pending: false,
    label: "内网端重启生效",
    disabledReason: "",
    reasonCode: "",
  });
  const isUpdaterSourceRunDisabled = computed(
    () => String(updaterMainAction.value?.reasonCode || "").trim().toLowerCase() === "source_python_run",
  );
  const updaterBadgeToneClass = computed(() => {
    if (isUpdaterSourceRunDisabled.value) return "tone-info";
    return updaterMainAction.value?.id === "apply" ? "tone-warning" : "tone-neutral";
  });
  const updaterButtonClass = computed(() => {
    if (isUpdaterSourceRunDisabled.value) return "btn-ghost";
    return updaterMainAction.value?.id === "apply" ? "btn-warning" : "btn-secondary";
  });
  const isUpdaterActionLocked = computed(
    () =>
      !updaterMainAction.value.allowed
      || updaterMainAction.value.pending
      || isActionLocked(actionKeyUpdaterCheck)
      || isActionLocked(actionKeyUpdaterApply)
      || isActionLocked(actionKeyUpdaterRestart),
  );
  const updaterInternalPeerSnapshot = computed(() =>
    updaterMirrorOverview.value?.internalPeer && typeof updaterMirrorOverview.value.internalPeer === "object"
      ? updaterMirrorOverview.value.internalPeer
      : {},
  );
  const updaterInternalPeerCommandActive = computed(() =>
    Boolean(updaterInternalPeerSnapshot.value?.command?.active),
  );
  const updaterInternalPeerCommandAction = computed(() =>
    String(updaterInternalPeerSnapshot.value?.command?.action || "").trim().toLowerCase(),
  );
  const updaterInternalPeerOnline = computed(() =>
    Boolean(updaterInternalPeerSnapshot.value?.online),
  );
  const isUpdaterInternalPeerCheckLocked = computed(() =>
    !updaterInternalPeerCheckAction.value.allowed
    || updaterInternalPeerCheckAction.value.pending
    || isActionLocked(actionKeyUpdaterInternalPeerCheck),
  );
  const isUpdaterInternalPeerApplyLocked = computed(() =>
    !updaterInternalPeerApplyAction.value.allowed
    || updaterInternalPeerApplyAction.value.pending
    || isActionLocked(actionKeyUpdaterInternalPeerApply),
  );
  const isUpdaterPublishApprovedLocked = computed(() =>
    !updaterPublishApprovedAction.value.allowed
    || updaterPublishApprovedAction.value.pending
    || isActionLocked(actionKeyUpdaterPublishApproved),
  );
  const isUpdaterInternalPeerRestartLocked = computed(() =>
    !updaterInternalPeerRestartAction.value.allowed
    || updaterInternalPeerRestartAction.value.pending
    || isActionLocked(actionKeyUpdaterInternalPeerRestart),
  );
  const updaterPublishApprovedButtonText = computed(() => {
    if (isActionLocked(actionKeyUpdaterPublishApproved)) return "发布中...";
    return updaterPublishApprovedAction.value.label || "手动同步当前代码";
  });
  const updaterInternalPeerCheckButtonText = computed(() => {
    if (isActionLocked(actionKeyUpdaterInternalPeerCheck)) return "下发中...";
    return updaterInternalPeerCheckAction.value.label || "刷新内网状态";
  });
  const updaterInternalPeerApplyButtonText = computed(() => {
    if (isActionLocked(actionKeyUpdaterInternalPeerApply)) return "下发中...";
    return updaterInternalPeerApplyAction.value.label || "内网端应用代码";
  });
  const updaterInternalPeerRestartButtonText = computed(() => {
    if (isActionLocked(actionKeyUpdaterInternalPeerRestart)) return "下发中...";
    return updaterInternalPeerRestartAction.value.label || "内网端重启生效";
  });
  const updaterMainButtonText = computed(() => {
    if (isActionLocked(actionKeyUpdaterRestart)) return "重启中...";
    if (isActionLocked(actionKeyUpdaterApply)) return "更新中...";
    if (isActionLocked(actionKeyUpdaterCheck)) return "检查中...";
    return updaterMainAction.value.label || "检查并更新";
  });

  return {
    getUpdaterDisabledText,
    updaterMainAction,
    updaterPublishApprovedAction,
    updaterInternalPeerCheckAction,
    updaterInternalPeerApplyAction,
    updaterInternalPeerRestartAction,
    isUpdaterSourceRunDisabled,
    updaterBadgeToneClass,
    updaterButtonClass,
    isUpdaterActionLocked,
    isUpdaterPublishApprovedLocked,
    updaterInternalPeerSnapshot,
    updaterInternalPeerCommandActive,
    updaterInternalPeerCommandAction,
    updaterInternalPeerOnline,
    isUpdaterInternalPeerCheckLocked,
    isUpdaterInternalPeerApplyLocked,
    isUpdaterInternalPeerRestartLocked,
    updaterPublishApprovedButtonText,
    updaterInternalPeerCheckButtonText,
    updaterInternalPeerApplyButtonText,
    updaterInternalPeerRestartButtonText,
    updaterMainButtonText,
  };
}
