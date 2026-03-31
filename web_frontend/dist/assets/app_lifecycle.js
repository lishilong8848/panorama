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

    const scheduleHealthPoll = (delayMs = 5000) => {
      if (timers.healthTimer) clearTimeout(timers.healthTimer);
      timers.healthTimer = window.setTimeout(async () => {
        if (isRuntimeTrafficPaused()) {
          scheduleHealthPoll(5000);
          return;
        }
        if (canFetchHealth()) {
          const healthOk = await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
          if (healthOk !== false && canFetchPendingResumeRuns()) {
            await fetchPendingResumeRuns({ silentMessage: true });
            await tryAutoResume();
          }
        }
        scheduleHealthPoll(5000);
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
    const scheduleBridgeTasksPoll = (delayMs = 5000) => {
      if (timers.bridgeTasksTimer) clearTimeout(timers.bridgeTasksTimer);
      timers.bridgeTasksTimer = window.setTimeout(async () => {
        if (isRuntimeTrafficPaused()) {
          scheduleBridgeTasksPoll(5000);
          return;
        }
        if (canPollBridgeTasks()) {
          await fetchBridgeTasks({ silentMessage: true });
        }
        scheduleBridgeTasksPoll(5000);
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
    if (bootstrapReady) {
      bootstrapReady.value = true;
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
    scheduleHealthPoll(5000);
    scheduleJobPanelPoll(5000);
    scheduleBridgeTasksPoll(5000);
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

