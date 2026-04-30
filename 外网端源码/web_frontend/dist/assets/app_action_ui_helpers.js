export function createAppActionUiHelpers(options = {}) {
  const {
    message,
    updaterMainAction,
    updaterPublishApprovedAction,
    updaterInternalPeerCheckAction,
    updaterInternalPeerApplyAction,
    updaterInternalPeerRestartAction,
    isUpdaterActionLocked,
    isUpdaterPublishApprovedLocked,
    isUpdaterInternalPeerCheckLocked,
    isUpdaterInternalPeerApplyLocked,
    isUpdaterInternalPeerRestartLocked,
    getUpdaterDisabledText,
    restartUpdaterApp,
    applyUpdaterPatch,
    checkUpdaterNow,
    publishUpdaterApproved,
    triggerInternalPeerUpdaterCheck,
    triggerInternalPeerUpdaterApply,
    triggerInternalPeerUpdaterRestart,
    setDashboardActiveModule,
    dashboardSchedulerOverviewFocusKey,
    nextTick,
    timerState,
  } = options;

  function clearDashboardSchedulerOverviewFocus() {
    dashboardSchedulerOverviewFocusKey.value = "";
    if (timerState?.timer && typeof window !== "undefined") {
      window.clearTimeout(timerState.timer);
    }
    if (timerState) timerState.timer = null;
  }

  async function openDashboardSchedulerOverviewTarget(moduleId, focusKey = "") {
    setDashboardActiveModule(moduleId);
    const nextFocusKey = String(focusKey || "").trim();
    if (!nextFocusKey || typeof window === "undefined" || typeof document === "undefined") {
      clearDashboardSchedulerOverviewFocus();
      return;
    }
    dashboardSchedulerOverviewFocusKey.value = nextFocusKey;
    await nextTick();
    window.requestAnimationFrame(() => {
      const target = document.querySelector(`[data-scheduler-overview-target="${nextFocusKey}"]`);
      if (target && typeof target.scrollIntoView === "function") {
        target.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    });
    if (timerState?.timer) {
      window.clearTimeout(timerState.timer);
    }
    if (timerState) {
      timerState.timer = window.setTimeout(() => {
        dashboardSchedulerOverviewFocusKey.value = "";
        timerState.timer = null;
      }, 3200);
    }
  }

  async function runUpdaterMainAction() {
    if (!updaterMainAction.value.allowed) {
      message.value = getUpdaterDisabledText(updaterMainAction.value);
      return;
    }
    if (isUpdaterActionLocked.value) return;
    if (updaterMainAction.value.id === "restart") {
      await restartUpdaterApp();
      return;
    }
    if (updaterMainAction.value.id === "apply") {
      await applyUpdaterPatch();
      return;
    }
    await checkUpdaterNow({ autoApplyIfAvailable: updaterMainAction.value.id !== "check" });
  }

  async function checkInternalPeerUpdaterNow() {
    if (!updaterInternalPeerCheckAction.value.allowed) {
      message.value = getUpdaterDisabledText(updaterInternalPeerCheckAction.value);
      return;
    }
    if (isUpdaterInternalPeerCheckLocked.value) return;
    await triggerInternalPeerUpdaterCheck();
  }

  async function publishUpdaterApprovedNow() {
    if (!updaterPublishApprovedAction.value.allowed) {
      message.value = getUpdaterDisabledText(updaterPublishApprovedAction.value);
      return;
    }
    if (isUpdaterPublishApprovedLocked.value) return;
    await publishUpdaterApproved();
  }

  async function applyInternalPeerUpdaterNow() {
    if (!updaterInternalPeerApplyAction.value.allowed) {
      message.value = getUpdaterDisabledText(updaterInternalPeerApplyAction.value);
      return;
    }
    if (isUpdaterInternalPeerApplyLocked.value) return;
    await triggerInternalPeerUpdaterApply();
  }

  async function restartInternalPeerUpdaterNow() {
    if (!updaterInternalPeerRestartAction.value.allowed) {
      message.value = getUpdaterDisabledText(updaterInternalPeerRestartAction.value);
      return;
    }
    if (isUpdaterInternalPeerRestartLocked.value) return;
    await triggerInternalPeerUpdaterRestart();
  }

  return {
    clearDashboardSchedulerOverviewFocus,
    openDashboardSchedulerOverviewTarget,
    runUpdaterMainAction,
    publishUpdaterApprovedNow,
    checkInternalPeerUpdaterNow,
    applyInternalPeerUpdaterNow,
    restartInternalPeerUpdaterNow,
  };
}
