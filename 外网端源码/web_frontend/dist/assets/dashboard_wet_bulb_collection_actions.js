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
    fetchExternalDashboardSummary,
    scheduleExternalDashboardRefresh,
    fetchJobs,
    fetchBridgeTasks,
    fetchBridgeTaskDetail,
    runSingleFlight,
    setSchedulerToggleState,
  } = ctx;

  function triggerDashboardRefresh(reason = "wet_bulb_action") {
    if (typeof scheduleExternalDashboardRefresh === "function") {
      scheduleExternalDashboardRefresh(reason);
      return;
    }
    if (typeof fetchExternalDashboardSummary === "function") {
      void fetchExternalDashboardSummary({ silentMessage: true });
    }
  }

  function markSchedulerToggle(mode, rememberedOverride) {
    if (typeof setSchedulerToggleState !== "function") return;
    setSchedulerToggleState("wet_bulb", { mode, rememberedOverride });
  }
async function guardedRun(actionKey, taskFn, options = {}) {
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(actionKey, taskFn, {
        ...options,
        onCooldown: () => {
          message.value = "请求处理中，请稍候";
        },
      });
    }
    return taskFn();
  }

  function applyWetBulbSchedulerSnapshotFromAction(data) {
    if (!data || typeof data !== "object" || !health?.wet_bulb_collection?.scheduler) return;
    const targetScheduler = health.wet_bulb_collection.scheduler;
    Object.assign(targetScheduler, {
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
      remembered_enabled: Object.prototype.hasOwnProperty.call(data, "remembered_enabled")
        ? Boolean(data.remembered_enabled)
        : Boolean(health.wet_bulb_collection.scheduler.remembered_enabled),
      effective_auto_start_in_gui: Object.prototype.hasOwnProperty.call(data, "effective_auto_start_in_gui")
        ? Boolean(data.effective_auto_start_in_gui)
        : Boolean(health.wet_bulb_collection.scheduler.effective_auto_start_in_gui),
      memory_source: Object.prototype.hasOwnProperty.call(data, "memory_source")
        ? String(data.memory_source || "")
        : String(targetScheduler.memory_source || ""),
      display: data.display && typeof data.display === "object"
        ? { ...data.display }
        : (targetScheduler.display && typeof targetScheduler.display === "object" ? { ...targetScheduler.display } : {}),
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
          const hasBridgeTask = Boolean(response?.bridge_task && typeof response.bridge_task === "object");
          const isBridgeJob = hasBridgeTask || String(wrappedJob?.kind || "").trim().toLowerCase() === "bridge";
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
            message.value = "湿球温度定时采集已进入共享补采处理";
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
            message.value = "湿球温度定时采集已进入等待采集端补采，共享文件到位后会自动继续";
          }
        } catch (err) {
          message.value = formatWetBulbCollectionError(err, "湿球温度定时采集提交");
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function startWetBulbCollectionScheduler() {
return guardedRun(
      ACTION_KEYS.schedulerStart,
      async () => {
        markSchedulerToggle("starting", true);
        try {
          const data = await startWetBulbCollectionSchedulerApi();
          syncLocalWetBulbSchedulerAutoStart(config.value?.wet_bulb_collection?.scheduler, true, { enableOnStart: true });
          applyWetBulbSchedulerSnapshotFromAction(data);
          markSchedulerToggle("idle", true);
          triggerDashboardRefresh("wet_bulb_scheduler_start");
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
return guardedRun(
      ACTION_KEYS.schedulerStop,
      async () => {
        markSchedulerToggle("stopping", false);
        try {
          const data = await stopWetBulbCollectionSchedulerApi();
          syncLocalWetBulbSchedulerAutoStart(config.value?.wet_bulb_collection?.scheduler, false);
          applyWetBulbSchedulerSnapshotFromAction(data);
          markSchedulerToggle("idle", false);
          triggerDashboardRefresh("wet_bulb_scheduler_stop");
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
          applyWetBulbSchedulerSnapshotFromAction(data?.scheduler_status || data);
          triggerDashboardRefresh("wet_bulb_scheduler_save");
          message.value = data?.message || "湿球温度定时采集调度配置已更新";
        } catch (err) {
          message.value = formatWetBulbCollectionError(err, "湿球温度定时采集调度自动更新");
        } finally {
          wetBulbSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 0, queueLatest: true },
    );
  }

  return {
    runWetBulbCollection,
    startWetBulbCollectionScheduler,
    stopWetBulbCollectionScheduler,
    saveWetBulbCollectionSchedulerQuickConfig,
  };
}


