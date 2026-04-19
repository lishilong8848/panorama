export function createRuntimeRequestPolicyUiHelpers(options = {}) {
  const {
    computed,
    startupRoleSelectorHandled,
    updaterUiOverlayVisible,
    updaterAwaitingRestartRecovery,
    startupRoleSelectorVisible,
    startupRoleLoadingVisible,
    bootstrapReady,
    health,
    currentView,
    deploymentRoleMode,
    fullHealthLoaded,
    bridgeTasksEnabled,
    dashboardActiveModule,
    activeConfigTab,
  } = options;

  const shouldPauseRuntimeRequests = computed(() => {
    return Boolean(
      !startupRoleSelectorHandled.value
      || updaterUiOverlayVisible.value
      || updaterAwaitingRestartRecovery.value
      || startupRoleSelectorVisible.value
      || startupRoleLoadingVisible.value
      || (Boolean(health.startup_role_user_exited) && !Boolean(health.runtime_activated))
    );
  });

  const runtimeRequestsReady = computed(() => (
    !shouldPauseRuntimeRequests.value
    && bootstrapReady.value
    && Boolean(health.runtime_activated)
    && Boolean(health.startup_role_confirmed)
    && !Boolean(health.startup_role_user_exited)
    && !Boolean(health.role_selection_required)
  ));

  const shouldFetchHealth = computed(() => {
    if (!runtimeRequestsReady.value) return false;
    const view = String(currentView.value || "").trim().toLowerCase();
    return view === "status";
  });

  const shouldPollExternalDashboardSummary = computed(() => {
    if (!runtimeRequestsReady.value) return false;
    if (deploymentRoleMode.value === "internal") return false;
    const view = String(currentView.value || "").trim().toLowerCase();
    return view === "dashboard";
  });

  const shouldPollJobPanel = computed(() => false);

  const shouldFetchPendingResumeRuns = computed(() => {
    if (!runtimeRequestsReady.value) return false;
    if (!fullHealthLoaded.value) return false;
    if (deploymentRoleMode.value === "internal") return false;
    const view = String(currentView.value || "").trim().toLowerCase();
    return view === "dashboard";
  });

  const healthPollIntervalMs = computed(() => {
    if (shouldPauseRuntimeRequests.value) return 60000;
    return 60000;
  });

  const shouldPollBridgeTasks = computed(() => {
    if (!runtimeRequestsReady.value) return false;
    if (!bridgeTasksEnabled.value) return false;
    const view = String(currentView.value || "").trim().toLowerCase();
    return view === "status";
  });

  const shouldPollInternalRuntimeStatus = computed(() => {
    if (!runtimeRequestsReady.value) return false;
    if (deploymentRoleMode.value !== "internal") return false;
    const view = String(currentView.value || "").trim().toLowerCase();
    return view === "dashboard" || view === "status";
  });

  const shouldIncludeHandoverHealthContext = computed(() => {
    if (!runtimeRequestsReady.value) return false;
    if (deploymentRoleMode.value === "internal") return false;
    const view = String(currentView.value || "").trim().toLowerCase();
    const moduleId = String(dashboardActiveModule.value || "").trim();
    if (view === "status") return true;
    if (view === "dashboard") {
      return moduleId === "handover_log";
    }
    return false;
  });

  const shouldPollHandoverDailyReportContext = computed(() => {
    if (!runtimeRequestsReady.value) return false;
    if (deploymentRoleMode.value === "internal") return false;
    const view = String(currentView.value || "").trim().toLowerCase();
    const moduleId = String(dashboardActiveModule.value || "").trim();
    const configTab = String(activeConfigTab.value || "").trim();
    if (view === "dashboard") {
      return moduleId === "handover_log";
    }
    if (view === "config") {
      return configTab === "feature_handover";
    }
    return false;
  });

  const shouldLoadEngineerDirectory = computed(() => {
    if (!runtimeRequestsReady.value) return false;
    if (deploymentRoleMode.value === "internal") return false;
    const view = String(currentView.value || "").trim().toLowerCase();
    const moduleId = String(dashboardActiveModule.value || "").trim();
    const configTab = String(activeConfigTab.value || "").trim();
    if (view === "dashboard") {
      return moduleId === "handover_log";
    }
    if (view === "config") {
      return configTab === "feature_handover";
    }
    return false;
  });

  return {
    shouldPauseRuntimeRequests,
    runtimeRequestsReady,
    shouldFetchHealth,
    shouldPollExternalDashboardSummary,
    shouldPollJobPanel,
    shouldFetchPendingResumeRuns,
    healthPollIntervalMs,
    shouldPollBridgeTasks,
    shouldPollInternalRuntimeStatus,
    shouldIncludeHandoverHealthContext,
    shouldPollHandoverDailyReportContext,
    shouldLoadEngineerDirectory,
  };
}
