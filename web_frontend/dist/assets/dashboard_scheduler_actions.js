import {
  saveHandoverSchedulerConfigApi,
  saveSchedulerConfigApi,
  startHandoverSchedulerApi,
  startSchedulerApi,
  stopHandoverSchedulerApi,
  stopSchedulerApi,
} from "./api_client.js";
import { normalizeRunTimeText } from "./config_helpers.js";

const ACTION_KEYS = {
  schedulerStart: "scheduler:start",
  schedulerStop: "scheduler:stop",
  schedulerSave: "scheduler:save",
  handoverSchedulerStart: "handover_scheduler:start",
  handoverSchedulerStop: "handover_scheduler:stop",
  handoverSchedulerSave: "handover_scheduler:save",
};

function formatSchedulerActionReason(reason) {
  const normalized = String(reason || "").trim().toLowerCase();
  if (!normalized || normalized === "ok") return "已完成";
  if (normalized === "started") return "已启动";
  if (normalized === "stopped") return "已停止";
  if (normalized === "already_running") return "已在运行";
  if (normalized === "disabled") return "未启用";
  if (normalized === "partial_started") return "部分已启动";
  if (normalized === "not_initialized") return "尚未初始化";
  return String(reason || "").trim() || "已完成";
}

export function createDashboardSchedulerActions(ctx) {
  const {
    config,
    message,
    schedulerQuickSaving,
    handoverSchedulerQuickSaving,
    fetchHealth,
    runSingleFlight,
  } = ctx;

  async function guardedRun(actionKey, taskFn, options = {}) {
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(actionKey, taskFn, options);
    }
    return taskFn();
  }

  async function startScheduler() {
    return guardedRun(
      ACTION_KEYS.schedulerStart,
      async () => {
        try {
          const data = await startSchedulerApi();
          await fetchHealth();
          message.value = `调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = `启动调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveSchedulerQuickConfig() {
    if (!config.value) return;
    const scheduler = config.value.scheduler || {};
    const runTime = normalizeRunTimeText(scheduler.run_time);
    const payload = {
      enabled: Boolean(scheduler.enabled),
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
      run_time: runTime,
      catch_up_if_missed: Boolean(scheduler.catch_up_if_missed),
      retry_failed_in_same_period: Boolean(scheduler.retry_failed_in_same_period),
    };
    if (!payload.run_time) {
      message.value = "调度时间格式错误，必须是 HH:MM 或 HH:MM:SS";
      return;
    }
    return guardedRun(
      ACTION_KEYS.schedulerSave,
      async () => {
        try {
          schedulerQuickSaving.value = true;
          const data = await saveSchedulerConfigApi(payload);
          if (data?.scheduler_config && config.value?.scheduler) {
            Object.assign(config.value.scheduler, data.scheduler_config);
          }
          await fetchHealth();
          const executorBound = data?.executor_bound_after_reload !== false;
          if (data?.run_time_changed && executorBound) {
            message.value = data?.message || "调度配置已更新；检测到每日执行时间变化，已自动重置今日调度状态，执行器已绑定";
          } else if (data?.run_time_changed && !executorBound) {
            message.value = "调度时间已更新并重置今日状态，但执行器未绑定，自动调度暂不可用";
          } else if (!executorBound) {
            message.value = "调度配置已更新，但执行器未绑定，自动调度暂不可用";
          } else {
            message.value = data?.message || "调度配置已更新";
          }
        } catch (err) {
          message.value = `保存调度配置失败: ${err}`;
        } finally {
          schedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopScheduler() {
    return guardedRun(
      ACTION_KEYS.schedulerStop,
      async () => {
        try {
          const data = await stopSchedulerApi();
          await fetchHealth();
          message.value = `调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = `停止调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function startHandoverScheduler() {
    return guardedRun(
      ACTION_KEYS.handoverSchedulerStart,
      async () => {
        try {
          const data = await startHandoverSchedulerApi();
          await fetchHealth();
          message.value = `交接班调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = `启动交接班调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopHandoverScheduler() {
    return guardedRun(
      ACTION_KEYS.handoverSchedulerStop,
      async () => {
        try {
          const data = await stopHandoverSchedulerApi();
          await fetchHealth();
          message.value = `交接班调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = `停止交接班调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveHandoverSchedulerQuickConfig() {
    if (!config.value) return;
    const handoverScheduler = config.value?.handover_log?.scheduler || {};
    const morningTime = normalizeRunTimeText(handoverScheduler.morning_time);
    const afternoonTime = normalizeRunTimeText(handoverScheduler.afternoon_time);
    if (!morningTime || !afternoonTime) {
      message.value = "交接班调度时间格式错误，必须是 HH:MM 或 HH:MM:SS";
      return;
    }
    const payload = {
      enabled: Boolean(handoverScheduler.enabled),
      auto_start_in_gui: Boolean(handoverScheduler.auto_start_in_gui),
      morning_time: morningTime,
      afternoon_time: afternoonTime,
      check_interval_sec: Number.parseInt(String(handoverScheduler.check_interval_sec ?? 30), 10) || 30,
      catch_up_if_missed: Boolean(handoverScheduler.catch_up_if_missed),
      retry_failed_in_same_period: Boolean(handoverScheduler.retry_failed_in_same_period),
      morning_state_file: String(handoverScheduler.morning_state_file || "").trim(),
      afternoon_state_file: String(handoverScheduler.afternoon_state_file || "").trim(),
    };
    if (!payload.morning_state_file || !payload.afternoon_state_file) {
      message.value = "交接班调度状态文件不能为空";
      return;
    }
    if (!Number.isInteger(payload.check_interval_sec) || payload.check_interval_sec <= 0) {
      message.value = "交接班调度检查间隔必须大于0";
      return;
    }
    return guardedRun(
      ACTION_KEYS.handoverSchedulerSave,
      async () => {
        try {
          handoverSchedulerQuickSaving.value = true;
          const data = await saveHandoverSchedulerConfigApi(payload);
          if (data?.scheduler_config && config.value?.handover_log?.scheduler) {
            Object.assign(config.value.handover_log.scheduler, data.scheduler_config);
          }
          await fetchHealth();
          const changed = Boolean(data?.morning_time_changed || data?.afternoon_time_changed);
          message.value = changed
            ? "交接班调度配置已更新，已重置对应时段今日状态"
            : data?.message || "交接班调度配置已更新";
        } catch (err) {
          message.value = `保存交接班调度配置失败: ${err}`;
        } finally {
          handoverSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 500 },
    );
  }

  return {
    startScheduler,
    saveSchedulerQuickConfig,
    stopScheduler,
    startHandoverScheduler,
    stopHandoverScheduler,
    saveHandoverSchedulerQuickConfig,
  };
}
