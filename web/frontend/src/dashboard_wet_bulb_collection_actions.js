import {
  saveWetBulbCollectionSchedulerConfigApi,
  startWetBulbCollectionJobApi,
  startWetBulbCollectionSchedulerApi,
  stopWetBulbCollectionSchedulerApi,
} from "./api_client.js";
import { cleanupWetBulbCollectionCompat } from "./config_compat_cleanup.js";

const ACTION_KEYS = {
  run: "job:wet_bulb_collection",
  schedulerStart: "wet_bulb_scheduler:start",
  schedulerStop: "wet_bulb_scheduler:stop",
  schedulerSave: "wet_bulb_scheduler:save",
};

function formatWetBulbSchedulerActionReason(reason) {
  const normalized = String(reason || "").trim().toLowerCase();
  if (!normalized || normalized === "ok") return "已完成";
  if (normalized === "started") return "已启动";
  if (normalized === "stopped") return "已停止";
  if (normalized === "already_running") return "已在运行";
  if (normalized === "disabled") return "未启用";
  if (normalized === "not_initialized") return "尚未初始化";
  return String(reason || "").trim() || "已完成";
}

function syncLocalWetBulbSchedulerAutoStart(targetScheduler, autoStart, options = {}) {
  if (!targetScheduler || typeof targetScheduler !== "object") return;
  targetScheduler.auto_start_in_gui = Boolean(autoStart);
  if (options.enableOnStart && autoStart) {
    targetScheduler.enabled = true;
  }
}

export function createDashboardWetBulbCollectionActions(ctx) {
  const {
    canRun,
    health,
    message,
    currentJob,
    selectedJobId,
    selectedBridgeTaskId,
    bridgeTaskDetail,
    config,
    wetBulbSchedulerQuickSaving,
    streamController,
    fetchHealth,
    fetchConfig,
    fetchJobs,
    fetchBridgeTasks,
    fetchBridgeTaskDetail,
    runSingleFlight,
    setSchedulerToggleState,
  } = ctx;
  let healthRefreshTimer = null;
  let configRefreshTimer = null;

  function scheduleHealthRefresh(delayMs = 800) {
    if (healthRefreshTimer) {
      window.clearTimeout(healthRefreshTimer);
    }
    healthRefreshTimer = window.setTimeout(() => {
      healthRefreshTimer = null;
      void fetchHealth({ silentMessage: true });
    }, Math.max(0, Number.parseInt(String(delayMs || 0), 10) || 0));
  }

  function scheduleConfigBaselineRefresh(delayMs = 300) {
    if (typeof fetchConfig !== "function") return;
    if (configRefreshTimer) {
      window.clearTimeout(configRefreshTimer);
    }
    configRefreshTimer = window.setTimeout(() => {
      configRefreshTimer = null;
      void fetchConfig({
        silentMessage: true,
        applyToDraft: false,
        loadHandoverSegments: false,
      });
    }, Math.max(0, Number.parseInt(String(delayMs || 0), 10) || 0));
  }

  function markSchedulerToggle(mode, runningOverride) {
    if (typeof setSchedulerToggleState !== "function") return;
    setSchedulerToggleState("wet_bulb", { mode, runningOverride });
  }

  function isInternalRole() {
    return String(config?.value?.deployment?.role_mode || "").trim().toLowerCase() === "internal";
  }

  async function guardedRun(actionKey, taskFn, options = {}) {
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(actionKey, taskFn, options);
    }
    return taskFn();
  }

  function applyWetBulbSchedulerSnapshotFromAction(data) {
    if (!data || typeof data !== "object" || !health?.wet_bulb_collection?.scheduler) return;
    Object.assign(health.wet_bulb_collection.scheduler, {
      running: Boolean(data.running),
      status: String(data.status || ""),
      next_run_time: String(data.next_run_time || ""),
      last_check_at: String(data.last_check_at || ""),
      last_decision: String(data.last_decision || ""),
      last_trigger_at: String(data.last_trigger_at || ""),
      last_trigger_result: String(data.last_trigger_result || ""),
      state_path: String(data.state_path || ""),
      state_exists: Boolean(data.state_exists),
      executor_bound: Boolean(data.executor_bound),
      callback_name: String(data.callback_name || "-"),
    });
  }

  function formatWetBulbCollectionError(err, actionLabel) {
    const text = String(err || "").trim();
    if (
      text.includes('{"detail":"Not Found"}')
      || text.includes("HTTP 404")
      || text.includes("Not Found")
    ) {
      return `${actionLabel}失败: 当前进程仍是更新前的旧版本，湿球温度接口尚未注册，请重启程序后再试。`;
    }
    return `${actionLabel}失败: ${text}`;
  }

  async function runWetBulbCollection() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供该业务入口，请在外网端发起。";
      return;
    }
    if (!canRun.value) return;
    return guardedRun(
      ACTION_KEYS.run,
      async () => {
        try {
          message.value = "湿球温度定时采集任务已提交";
          const response = await startWetBulbCollectionJobApi();
          const wrappedJob =
            response
            && typeof response === "object"
            && response.accepted
            && response.job
            && typeof response.job === "object"
              ? response.job
              : response;
          const isBridgeJob = String(wrappedJob?.kind || "").trim().toLowerCase() === "bridge";
          const isWaitingSharedBridge = String(wrappedJob?.wait_reason || "").trim().toLowerCase() === "waiting:shared_bridge";
          const bridgeTaskId = String(response?.bridge_task?.task_id || "").trim();
          const job = wrappedJob;
          if (isBridgeJob) {
            currentJob.value = null;
            if (selectedJobId) {
              selectedJobId.value = "";
            }
            if (selectedBridgeTaskId) {
              selectedBridgeTaskId.value = bridgeTaskId;
            }
            if (bridgeTaskDetail) {
              bridgeTaskDetail.value =
                response?.bridge_task && typeof response.bridge_task === "object" ? { ...response.bridge_task } : null;
            }
            if (typeof fetchBridgeTasks === "function") {
              await fetchBridgeTasks({ silentMessage: true });
            }
            if (bridgeTaskId && typeof fetchBridgeTaskDetail === "function") {
              await fetchBridgeTaskDetail(bridgeTaskId, { silentMessage: true });
            }
            message.value = "湿球温度定时采集已进入内外网同步处理";
            return;
          }

          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          if (job?.job_id) {
            streamController.attachJobStream(job.job_id);
          }
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
          if (isWaitingSharedBridge) {
            message.value = "湿球温度定时采集已进入等待内网补采同步，共享文件到位后会自动继续";
          }
        } catch (err) {
          message.value = formatWetBulbCollectionError(err, "湿球温度定时采集提交");
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function startWetBulbCollectionScheduler() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供该业务入口，请在外网端发起。";
      return;
    }
    return guardedRun(
      ACTION_KEYS.schedulerStart,
      async () => {
        markSchedulerToggle("starting", true);
        try {
          const data = await startWetBulbCollectionSchedulerApi();
          syncLocalWetBulbSchedulerAutoStart(config.value?.wet_bulb_collection?.scheduler, true, { enableOnStart: true });
          applyWetBulbSchedulerSnapshotFromAction(data);
          markSchedulerToggle("idle", true);
          scheduleHealthRefresh();
          scheduleConfigBaselineRefresh();
          message.value = `湿球温度定时采集调度启动结果: ${formatWetBulbSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("idle", null);
          message.value = formatWetBulbCollectionError(err, "启动湿球温度定时采集调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopWetBulbCollectionScheduler() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供该业务入口，请在外网端发起。";
      return;
    }
    return guardedRun(
      ACTION_KEYS.schedulerStop,
      async () => {
        markSchedulerToggle("stopping", false);
        try {
          const data = await stopWetBulbCollectionSchedulerApi();
          syncLocalWetBulbSchedulerAutoStart(config.value?.wet_bulb_collection?.scheduler, false);
          applyWetBulbSchedulerSnapshotFromAction(data);
          markSchedulerToggle("idle", false);
          scheduleHealthRefresh();
          scheduleConfigBaselineRefresh();
          message.value = `湿球温度定时采集调度停止结果: ${formatWetBulbSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("idle", null);
          message.value = formatWetBulbCollectionError(err, "停止湿球温度定时采集调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveWetBulbCollectionSchedulerQuickConfig() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供该业务入口，请在外网端发起。";
      return;
    }
    if (!config.value) return;
    const wet = config.value.wet_bulb_collection || {};
    const scheduler = wet.scheduler || {};
    const payload = {
      enabled: true,
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
      interval_minutes: Number.parseInt(String(scheduler.interval_minutes ?? 60), 10) || 60,
      check_interval_sec: Number.parseInt(String(scheduler.check_interval_sec ?? 30), 10) || 30,
      retry_failed_on_next_tick: Boolean(scheduler.retry_failed_on_next_tick),
      state_file: String(scheduler.state_file || "").trim(),
    };
    if (!payload.state_file) {
      message.value = "湿球温度定时采集状态文件名不能为空";
      return;
    }
    if (!Number.isInteger(payload.interval_minutes) || payload.interval_minutes <= 0) {
      message.value = "湿球温度定时采集执行间隔必须大于 0 分钟";
      return;
    }
    if (!Number.isInteger(payload.check_interval_sec) || payload.check_interval_sec <= 0) {
      message.value = "湿球温度定时采集检查间隔必须大于 0 秒";
      return;
    }
    return guardedRun(
      ACTION_KEYS.schedulerSave,
      async () => {
        try {
          wetBulbSchedulerQuickSaving.value = true;
          const data = await saveWetBulbCollectionSchedulerConfigApi(payload);
          if (config.value?.wet_bulb_collection?.scheduler && data?.scheduler_config) {
            const next = cleanupWetBulbCollectionCompat({ scheduler: data.scheduler_config }).scheduler;
            Object.assign(config.value.wet_bulb_collection.scheduler, next || {});
          }
          scheduleConfigBaselineRefresh();
          await fetchHealth();
          message.value = data?.message || "湿球温度定时采集调度配置已更新";
        } catch (err) {
          message.value = formatWetBulbCollectionError(err, "湿球温度定时采集调度自动更新");
        } finally {
          wetBulbSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 500 },
    );
  }

  return {
    runWetBulbCollection,
    startWetBulbCollectionScheduler,
    stopWetBulbCollectionScheduler,
    saveWetBulbCollectionSchedulerQuickConfig,
  };
}
