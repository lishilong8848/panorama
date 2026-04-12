export function registerAppLifecycle(vueHooks, ctx) {
  const { onMounted, onBeforeUnmount } = vueHooks;
  const {
    fetchBootstrapHealth,
    fetchHealth,
    fetchJobs,
    fetchBridgeTasks,
    fetchRuntimeResources,
    fetchHandoverDailyReportContext,
    fetchConfig,
    syncHandoverDutyFromNow,
    fetchPendingResumeRuns,
    shouldFetchPendingResumeRuns,
    shouldPollHandoverDailyReportContext,
    shouldPollBridgeTasks,
    shouldFetchHealth,
    shouldPollJobPanel,
    shouldLoadEngineerDirectory,
    shouldPauseRuntimeRequests,
    scheduleEngineerDirectoryPrefetch,
    tryAutoResume,
    fetchJob,
    currentJob,
    streamController,
    timers,
    bootstrapReady,
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
    const canPollJobPanel = () =>
      typeof shouldPollJobPanel === "function"
        ? Boolean(shouldPollJobPanel())
        : true;
    const canPollBridgeTasks = () =>
      typeof shouldPollBridgeTasks === "function"
        ? Boolean(shouldPollBridgeTasks())
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

    const resolveHealthPollIntervalMs = () =>
      typeof getHealthPollIntervalMs === "function"
        ? Math.max(1000, Number.parseInt(String(getHealthPollIntervalMs() || 5000), 10) || 5000)
        : 5000;

    const scheduleHealthPoll = (delayMs = 5000) => {
      if (timers.healthTimer) clearTimeout(timers.healthTimer);
      timers.healthTimer = window.setTimeout(async () => {
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
    const scheduleJobPanelPoll = (delayMs = 5000) => {
      if (timers.jobsTimer) clearTimeout(timers.jobsTimer);
      timers.jobsTimer = window.setTimeout(async () => {
        if (isRuntimeTrafficPaused()) {
          scheduleJobPanelPoll(5000);
          return;
        }
        if (canPollJobPanel()) {
          await fetchJobs({ silentMessage: true });
          await fetchRuntimeResources({ silentMessage: true });
        }
        scheduleJobPanelPoll(5000);
      }, delayMs);
    };
    const bridgeTasksPollIntervalMs = 10000;
    const scheduleBridgeTasksPoll = (delayMs = bridgeTasksPollIntervalMs) => {
      if (timers.bridgeTasksTimer) clearTimeout(timers.bridgeTasksTimer);
      timers.bridgeTasksTimer = window.setTimeout(async () => {
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
    const scheduleDailyReportContextPoll = (delayMs = 30000) => {
      if (timers.dailyReportContextTimer) clearTimeout(timers.dailyReportContextTimer);
      timers.dailyReportContextTimer = window.setTimeout(async () => {
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
    void fetchConfig({ silentMessage: true });
    if (!isRuntimeTrafficPaused() && canFetchHealth()) {
      void fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
    }
    if (!isRuntimeTrafficPaused() && canPollJobPanel()) {
      void fetchJobs({ silentMessage: true });
      void fetchRuntimeResources({ silentMessage: true });
    }
    if (!isRuntimeTrafficPaused() && canPollBridgeTasks()) {
      void fetchBridgeTasks({ silentMessage: true });
    }
    if (!isRuntimeTrafficPaused() && canPollHandoverDailyReportContext()) {
      void fetchHandoverDailyReportContext({ silentTransientNetworkError: true, silentMessage: true });
    }
    if (!isRuntimeTrafficPaused() && canFetchPendingResumeRuns()) {
      void fetchPendingResumeRuns({ silentMessage: true });
    }
    if (!isRuntimeTrafficPaused() && canLoadEngineerDirectory() && typeof scheduleEngineerDirectoryPrefetch === "function") {
      scheduleEngineerDirectoryPrefetch(3000);
    }
    scheduleHealthPoll(resolveHealthPollIntervalMs());
    scheduleJobPanelPoll(5000);
    scheduleBridgeTasksPoll(bridgeTasksPollIntervalMs);
    scheduleDailyReportContextPoll(30000);
    timers.handoverDutyTimer = setInterval(() => {
      syncHandoverDutyFromNow(false);
    }, 30000);
    timers.pollTimer = setInterval(() => {
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
    if (timers.jobsTimer) clearTimeout(timers.jobsTimer);
    if (timers.bridgeTasksTimer) clearTimeout(timers.bridgeTasksTimer);
    if (timers.dailyReportContextTimer) clearTimeout(timers.dailyReportContextTimer);
    if (timers.handoverDutyTimer) clearInterval(timers.handoverDutyTimer);
  });
}

