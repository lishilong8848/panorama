export function registerAppRuntimeWatchers(options = {}) {
  const {
    watch,
    onBeforeUnmount,
    parseAppBrowserRoute,
    applyBrowserRoute,
    syncBrowserRoute,
    shouldPauseRuntimeRequests,
    streamController,
    runtimeRequestsReady,
    shouldPollExternalDashboardSummary,
    shouldFetchHealth,
    shouldPollJobPanel,
    shouldPollBridgeTasks,
    shouldPollHandoverDailyReportContext,
    shouldFetchPendingResumeRuns,
    shouldLoadEngineerDirectory,
    shouldPollInternalRuntimeStatus,
    runtimeWarmupReady,
    deploymentRoleMode,
    health,
    internalRuntimeSummary,
    internalBuildingRuntimeStatusMap,
    createEmptyInternalBuildingRuntimeStatusMap,
    bootstrapReady,
    configLoaded,
    currentView,
    activeConfigTab,
    startupRoleSelectorVisible,
    startupRoleGateReady,
    browserRouteReady,
    config,
    buildingsText,
    sheetRuleRows,
    customAbsoluteStartLocal,
    customAbsoluteEndLocal,
    configSaveSuspendDepth,
    markCurrentConfigDraftDirty,
    fetchConfig,
    fetchHandoverCommonConfigSegment,
    fetchHandoverBuildingConfigSegment,
    handoverConfigBuilding,
    handoverConfigCommonRevision,
    syncSavedHandoverCommonSignature,
    handoverConfigBuildingRevision,
    syncSavedHandoverBuildingSignature,
    handoverDutyDate,
    handoverDutyShift,
    persistHandoverDutyContext,
    fetchHealth,
    fetchHandoverDailyReportContext,
    fetchExternalDashboardSummary,
    fetchJobs,
    fetchBridgeTasks,
    fetchPendingResumeRuns,
    scheduleEngineerDirectoryPrefetch,
    fetchInternalRuntimeSummary,
    fetchAllInternalBuildingRuntimeStatuses,
    resetInternalRuntimeRequestState,
    resetExternalDashboardRequestState,
    ensureHandoverEngineerDirectoryLoaded,
    applyDashboardRoleMode,
    dashboardActiveModule,
  } = options || {};

  watch(
    () => shouldPauseRuntimeRequests.value,
    (paused) => {
      if (!streamController || typeof streamController.pauseAll !== "function" || typeof streamController.resumeAll !== "function") {
        return;
      }
      if (paused) {
        streamController.pauseAll();
        return;
      }
      streamController.resumeAll();
    },
  );

  watch(
    () => ({
      bootstrapReady: Boolean(bootstrapReady.value),
      runtimeActivated: Boolean(options.health?.runtime_activated),
      startupRoleConfirmed: Boolean(options.health?.startup_role_confirmed),
      selectorVisible: Boolean(startupRoleSelectorVisible.value),
      configLoaded: Boolean(configLoaded.value),
      view: String(currentView.value || "").trim().toLowerCase(),
    }),
    (state) => {
      if (!state.bootstrapReady) return;
      if (state.configLoaded) return;
      if (state.view !== "config") return;
      if (!state.runtimeActivated || !state.startupRoleConfirmed || state.selectorVisible) return;
      void fetchConfig({ silentMessage: true });
    },
    { immediate: true, deep: false },
  );

  watch(
    () => String(currentView.value || "").trim().toLowerCase(),
    (view) => {
      if (view !== "config") return;
      if (configLoaded.value) return;
      if (!runtimeRequestsReady.value) return;
      void fetchConfig({ silentMessage: true });
    },
    { immediate: true },
  );

  watch(
    () => ({
      config: config.value,
      buildingsText: buildingsText.value,
      sheetRuleRows: sheetRuleRows.value,
      customAbsoluteStartLocal: customAbsoluteStartLocal.value,
      customAbsoluteEndLocal: customAbsoluteEndLocal.value,
    }),
    () => {
      if (!configLoaded.value) return;
      if ((configSaveSuspendDepth?.value || 0) > 0) return;
      markCurrentConfigDraftDirty();
    },
    { deep: true },
  );

  watch(
    () => [String(currentView.value || "").trim(), String(activeConfigTab.value || "").trim()],
    ([view, tab]) => {
      const isHandoverTab = view === "config" && tab === "feature_handover";
      if (!isHandoverTab) return;
      if (!configLoaded.value) return;
      void fetchHandoverCommonConfigSegment({ silentMessage: true });
      void fetchHandoverBuildingConfigSegment(handoverConfigBuilding.value, { silentMessage: true });
    },
  );

  watch(
    () => handoverConfigCommonRevision.value,
    () => {
      if (!configLoaded.value) return;
      syncSavedHandoverCommonSignature();
    },
    { immediate: true },
  );

  watch(
    () => ({
      loaded: Boolean(configLoaded.value),
      view: String(currentView.value || "").trim(),
      tab: String(activeConfigTab.value || "").trim(),
    }),
    (state) => {
      if (!state.loaded) return;
      if (state.view !== "config" || state.tab !== "feature_handover") return;
      void fetchHandoverCommonConfigSegment({ silentMessage: true });
      void fetchHandoverBuildingConfigSegment(handoverConfigBuilding.value, { silentMessage: true });
    },
    { immediate: true, deep: false },
  );

  watch(
    () => [handoverConfigBuilding.value, handoverConfigBuildingRevision.value],
    ([building]) => {
      if (!configLoaded.value) return;
      if (!building) return;
      syncSavedHandoverBuildingSignature(building);
    },
    { immediate: true },
  );

  watch(
    () => [handoverDutyDate.value, handoverDutyShift.value],
    () => {
      persistHandoverDutyContext(handoverDutyDate.value, handoverDutyShift.value);
      if (!bootstrapReady.value) return;
      if (shouldFetchHealth.value) {
        fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
      }
      if (shouldPollHandoverDailyReportContext.value) {
        fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
      }
    },
    { immediate: false },
  );

  watch(
    () => ({
      runtimeReady: runtimeRequestsReady.value,
      role: String(health?.deployment?.role_mode || deploymentRoleMode.value || "").trim().toLowerCase(),
      shouldPollExternalDashboardSummary: shouldPollExternalDashboardSummary.value,
      shouldFetchHealth: shouldFetchHealth.value,
      shouldPollJobPanel: shouldPollJobPanel.value,
      shouldPollBridgeTasks: shouldPollBridgeTasks.value,
      shouldPollDailyReport: shouldPollHandoverDailyReportContext.value,
      shouldFetchPendingResumeRuns: shouldFetchPendingResumeRuns.value,
      shouldLoadEngineerDirectory: shouldLoadEngineerDirectory.value,
      shouldPollInternalRuntime: shouldPollInternalRuntimeStatus.value,
    }),
    (state, prevState) => {
      const previous = prevState && typeof prevState === "object" ? prevState : {};
      const wasRuntimeReady = Boolean(previous.runtimeReady);
      runtimeWarmupReady.value = Boolean(state.runtimeReady && state.role === "internal");
      if (state.role !== "internal") {
        if (typeof resetInternalRuntimeRequestState === "function") {
          resetInternalRuntimeRequestState();
        }
        internalRuntimeSummary.value = null;
        internalBuildingRuntimeStatusMap.value = createEmptyInternalBuildingRuntimeStatusMap();
      }
      if (state.role !== "external" && typeof resetExternalDashboardRequestState === "function") {
        resetExternalDashboardRequestState();
      }
      if (!state.runtimeReady) {
        return;
      }
      const justReady = !wasRuntimeReady && Boolean(state.runtimeReady);
      if (state.shouldPollExternalDashboardSummary && (justReady || !Boolean(previous.shouldPollExternalDashboardSummary))) {
        void fetchExternalDashboardSummary({ silentMessage: true });
      }
      if (state.shouldFetchHealth && (justReady || !Boolean(previous.shouldFetchHealth))) {
        void fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
      }
      if (state.shouldPollJobPanel && (justReady || !Boolean(previous.shouldPollJobPanel))) {
        void fetchJobs({ silentMessage: true });
      }
      if (state.shouldPollBridgeTasks && (justReady || !Boolean(previous.shouldPollBridgeTasks))) {
        void fetchBridgeTasks({ silentMessage: true });
      }
      if (state.shouldPollDailyReport && (justReady || !Boolean(previous.shouldPollDailyReport))) {
        void fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
      }
      if (state.shouldFetchPendingResumeRuns && (justReady || !Boolean(previous.shouldFetchPendingResumeRuns))) {
        void fetchPendingResumeRuns({ silentMessage: true });
      }
      if (state.shouldLoadEngineerDirectory && (justReady || !Boolean(previous.shouldLoadEngineerDirectory))) {
        scheduleEngineerDirectoryPrefetch(300);
      }
      if (state.shouldPollInternalRuntime && (justReady || !Boolean(previous.shouldPollInternalRuntime))) {
        void fetchInternalRuntimeSummary({ silentMessage: true });
        void fetchAllInternalBuildingRuntimeStatuses({ silentMessage: true });
      }
    },
    { immediate: true, deep: false },
  );

  watch(
    () => [dashboardActiveModule.value, activeConfigTab.value],
    () => {
      if (!shouldLoadEngineerDirectory.value) return;
      void ensureHandoverEngineerDirectoryLoaded({ silentMessage: true });
    },
    { immediate: true },
  );

  watch(
    () => deploymentRoleMode.value,
    (roleMode) => {
      applyDashboardRoleMode(roleMode);
      const hiddenCommonTabs = roleMode === "internal"
        ? new Set(["common_paths", "common_console", "common_scheduler", "common_notify", "common_feishu_auth", "common_alarm_db"])
        : new Set();
      const hiddenFeatureTabs = new Set(["feature_alarm"]);
      if (roleMode === "internal") {
        hiddenFeatureTabs.add("feature_monthly");
        hiddenFeatureTabs.add("feature_handover");
        hiddenFeatureTabs.add("feature_day_metric_upload");
        hiddenFeatureTabs.add("feature_wet_bulb_collection");
        hiddenFeatureTabs.add("feature_alarm_export");
        hiddenFeatureTabs.add("feature_sheet");
        hiddenFeatureTabs.add("feature_manual");
      }
      const currentTab = String(activeConfigTab.value || "").trim();
      if (hiddenCommonTabs.has(currentTab)) {
        activeConfigTab.value = "common_deployment";
      } else if (hiddenFeatureTabs.has(currentTab)) {
        activeConfigTab.value = "common_deployment";
      }
      if (roleMode === "internal" && currentView.value === "dashboard") {
        currentView.value = "status";
      }
    },
    { immediate: true },
  );

  watch(
    () => [
      deploymentRoleMode.value,
      String(currentView.value || "").trim(),
      Boolean(startupRoleSelectorVisible.value),
      Boolean(startupRoleGateReady.value),
    ],
    ([, , selectorVisible, gateReady]) => {
      if (!gateReady) return;
      syncBrowserRoute({
        replace: !browserRouteReady.value,
        selectorVisible,
      });
    },
    { immediate: true },
  );

  const handleBrowserPopstate = () => {
    const nextRoute = parseAppBrowserRoute(window.location.pathname);
    if (nextRoute.kind === "login") {
      if (!startupRoleSelectorVisible.value) {
        syncBrowserRoute({ replace: true, selectorVisible: false });
      }
      return;
    }
    applyBrowserRoute(nextRoute);
  };

  if (typeof window !== "undefined") {
    window.addEventListener("popstate", handleBrowserPopstate);
    onBeforeUnmount(() => {
      window.removeEventListener("popstate", handleBrowserPopstate);
    });
  }
}
