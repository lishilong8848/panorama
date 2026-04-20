export function registerAppLifecycle(vueHooks, ctx) {
  const { onMounted, onBeforeUnmount } = vueHooks;
  const {
    fetchBootstrapHealth,
    fetchExternalDashboardSummary,
    fetchHealth,
    fetchJobs,
    fetchBridgeTasks,
    fetchRuntimeResources,
    fetchInternalRuntimeSummary,
    fetchAllInternalBuildingRuntimeStatuses,
    fetchHandoverDailyReportContext,
    fetchConfig,
    syncHandoverDutyFromNow,
    fetchPendingResumeRuns,
    shouldFetchPendingResumeRuns,
    shouldPollHandoverDailyReportContext,
    shouldPollBridgeTasks,
    shouldPollInternalRuntimeStatus,
    shouldFetchHealth,
    shouldPollJobPanel,
    shouldLoadEngineerDirectory,
    shouldPauseRuntimeRequests,
    shouldPollExternalDashboardSummary,
    scheduleEngineerDirectoryPrefetch,
    tryAutoResume,
    fetchJob,
    currentJob,
    streamController,
    timers,
    bootstrapReady,
    runtimeWarmupReady,
    getHealthPollIntervalMs,
  } = ctx;

  onMounted(() => {
    const canPollHandoverDailyReportContext = () =>
      typeof shouldPollHandoverDailyReportContext === "function"
        ? Boolean(shouldPollHandoverDailyReportContext())
        : true;
    const canFetchHealth = () =>
      typeof shouldFetchHealth === "function"
        ? Boolean(shouldFetchHealth())
        : true;
    const canPollExternalDashboardSummary = () =>
      typeof shouldPollExternalDashboardSummary === "function"
        ? Boolean(shouldPollExternalDashboardSummary())
        : false;
    const canPollJobPanel = () =>
      typeof shouldPollJobPanel === "function"
        ? Boolean(shouldPollJobPanel())
        : true;
    const canPollBridgeTasks = () =>
      typeof shouldPollBridgeTasks === "function"
        ? Boolean(shouldPollBridgeTasks())
        : false;
    const canPollInternalRuntimeStatus = () =>
      typeof shouldPollInternalRuntimeStatus === "function"
        ? Boolean(shouldPollInternalRuntimeStatus())
        : false;
    const canFetchPendingResumeRuns = () =>
      typeof shouldFetchPendingResumeRuns === "function"
        ? Boolean(shouldFetchPendingResumeRuns())
        : true;
    const canLoadEngineerDirectory = () =>
      typeof shouldLoadEngineerDirectory === "function"
        ? Boolean(shouldLoadEngineerDirectory())
        : false;
    const isRuntimeTrafficPaused = () =>
      typeof shouldPauseRuntimeRequests === "function"
        ? Boolean(shouldPauseRuntimeRequests())
        : false;
    const isBootstrapReady = () => Boolean(bootstrapReady?.value);

    const resolveHealthPollIntervalMs = () =>
      typeof getHealthPollIntervalMs === "function"
        ? Math.max(1000, Number.parseInt(String(getHealthPollIntervalMs() || 5000), 10) || 5000)
        : 5000;
    const dashboardSummaryPollIntervalMs = 10000;

    const scheduleDashboardSummaryPoll = (delayMs = dashboardSummaryPollIntervalMs) => {
      if (timers.externalDashboardSummaryTimer) clearTimeout(timers.externalDashboardSummaryTimer);
      timers.externalDashboardSummaryTimer = window.setTimeout(async () => {
        if (!isBootstrapReady()) {
          scheduleDashboardSummaryPoll(dashboardSummaryPollIntervalMs);
          return;
        }
        if (isRuntimeTrafficPaused()) {
          scheduleDashboardSummaryPoll(dashboardSummaryPollIntervalMs);
          return;
        }
        if (canPollExternalDashboardSummary()) {
          await fetchExternalDashboardSummary({ silentMessage: true });
        }
        scheduleDashboardSummaryPoll(dashboardSummaryPollIntervalMs);
      }, delayMs);
    };

    const scheduleHealthPoll = (delayMs = 5000) => {
      if (timers.healthTimer) clearTimeout(timers.healthTimer);
      timers.healthTimer = window.setTimeout(async () => {
        if (!isBootstrapReady()) {
          scheduleHealthPoll(resolveHealthPollIntervalMs());
          return;
        }
        if (isRuntimeTrafficPaused()) {
          scheduleHealthPoll(resolveHealthPollIntervalMs());
          return;
        }
        if (canFetchHealth()) {
          const healthOk = await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
          if (healthOk !== false && canFetchPendingResumeRuns()) {
            await fetchPendingResumeRuns({ silentMessage: true });
            await tryAutoResume();
          }
        }
        scheduleHealthPoll(resolveHealthPollIntervalMs());
      }, delayMs);
    };
    const jobPanelPollIntervalMs = 60000;
    const scheduleJobPanelPoll = (delayMs = jobPanelPollIntervalMs) => {
      if (timers.jobsTimer) clearTimeout(timers.jobsTimer);
      timers.jobsTimer = window.setTimeout(async () => {
        if (!isBootstrapReady()) {
          scheduleJobPanelPoll(jobPanelPollIntervalMs);
          return;
        }
        if (isRuntimeTrafficPaused()) {
          scheduleJobPanelPoll(jobPanelPollIntervalMs);
          return;
        }
        if (canPollJobPanel()) {
          await fetchJobs({ silentMessage: true });
          await fetchRuntimeResources({ silentMessage: true });
        }
        scheduleJobPanelPoll(jobPanelPollIntervalMs);
      }, delayMs);
    };
    const bridgeTasksPollIntervalMs = 60000;
    const scheduleBridgeTasksPoll = (delayMs = bridgeTasksPollIntervalMs) => {
      if (timers.bridgeTasksTimer) clearTimeout(timers.bridgeTasksTimer);
      timers.bridgeTasksTimer = window.setTimeout(async () => {
        if (!isBootstrapReady()) {
          scheduleBridgeTasksPoll(bridgeTasksPollIntervalMs);
          return;
        }
        if (isRuntimeTrafficPaused()) {
          scheduleBridgeTasksPoll(bridgeTasksPollIntervalMs);
          return;
        }
        if (canPollBridgeTasks()) {
          await fetchBridgeTasks({ silentMessage: true });
        }
        scheduleBridgeTasksPoll(bridgeTasksPollIntervalMs);
      }, delayMs);
    };
    const internalRuntimePollIntervalMs = 3000;
    const scheduleInternalRuntimePoll = (delayMs = internalRuntimePollIntervalMs) => {
      if (timers.internalRuntimeTimer) clearTimeout(timers.internalRuntimeTimer);
      timers.internalRuntimeTimer = window.setTimeout(async () => {
        if (!isBootstrapReady()) {
          scheduleInternalRuntimePoll(internalRuntimePollIntervalMs);
          return;
        }
        if (isRuntimeTrafficPaused()) {
          scheduleInternalRuntimePoll(internalRuntimePollIntervalMs);
          return;
        }
        if (canPollInternalRuntimeStatus()) {
          await Promise.all([
            fetchInternalRuntimeSummary({ silentMessage: true }),
            fetchAllInternalBuildingRuntimeStatuses({ silentMessage: true }),
          ]);
        }
        scheduleInternalRuntimePoll(internalRuntimePollIntervalMs);
      }, delayMs);
    };
    const scheduleDailyReportContextPoll = (delayMs = 30000) => {
      if (timers.dailyReportContextTimer) clearTimeout(timers.dailyReportContextTimer);
      timers.dailyReportContextTimer = window.setTimeout(async () => {
        if (!isBootstrapReady()) {
          scheduleDailyReportContextPoll(30000);
          return;
        }
        if (isRuntimeTrafficPaused()) {
          scheduleDailyReportContextPoll(30000);
          return;
        }
        if (canPollHandoverDailyReportContext()) {
          await fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
        }
        scheduleDailyReportContextPoll(30000);
      }, delayMs);
    };

    syncHandoverDutyFromNow(true);
    if (!isRuntimeTrafficPaused()) {
      streamController.attachSystemStream();
    }
    void fetchBootstrapHealth({ silentMessage: true });
    if (!isRuntimeTrafficPaused() && canLoadEngineerDirectory() && typeof scheduleEngineerDirectoryPrefetch === "function") {
      scheduleEngineerDirectoryPrefetch(3000);
    }
    scheduleHealthPoll(resolveHealthPollIntervalMs());
    scheduleDashboardSummaryPoll(dashboardSummaryPollIntervalMs);
    scheduleJobPanelPoll(jobPanelPollIntervalMs);
    scheduleBridgeTasksPoll(bridgeTasksPollIntervalMs);
    scheduleInternalRuntimePoll(internalRuntimePollIntervalMs);
    scheduleDailyReportContextPoll(30000);
    timers.handoverDutyTimer = setInterval(() => {
      syncHandoverDutyFromNow(false);
    }, 30000);
    timers.pollTimer = setInterval(() => {
      if (!isBootstrapReady()) {
        return;
      }
      if (isRuntimeTrafficPaused()) {
        return;
      }
      if (
        currentJob.value &&
        (
          currentJob.value.status === "running" ||
          currentJob.value.status === "queued" ||
          currentJob.value.status === "waiting_resource"
        )
      ) {
        fetchJob(currentJob.value.job_id);
      }
    }, 2000);
  });

  onBeforeUnmount(() => {
    streamController.dispose();
    if (timers.pollTimer) clearInterval(timers.pollTimer);
    if (timers.healthTimer) clearTimeout(timers.healthTimer);
    if (timers.externalDashboardSummaryTimer) clearTimeout(timers.externalDashboardSummaryTimer);
    if (timers.healthWarmupTimer) clearTimeout(timers.healthWarmupTimer);
    if (timers.configRetryTimer) clearTimeout(timers.configRetryTimer);
    if (timers.internalRuntimeTimer) clearTimeout(timers.internalRuntimeTimer);
    if (timers.jobsTimer) clearTimeout(timers.jobsTimer);
    if (timers.bridgeTasksTimer) clearTimeout(timers.bridgeTasksTimer);
    if (timers.dailyReportContextTimer) clearTimeout(timers.dailyReportContextTimer);
    if (timers.handoverDutyTimer) clearInterval(timers.handoverDutyTimer);
  });
}

