export function createAppActionUiHelpers(options = {}) {
  const {
    message,
    updaterMainAction,
    isUpdaterActionLocked,
    getUpdaterDisabledText,
    restartUpdaterApp,
    applyUpdaterPatch,
    checkUpdaterNow,
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

  return {
    clearDashboardSchedulerOverviewFocus,
    openDashboardSchedulerOverviewTarget,
    runUpdaterMainAction,
  };
}
