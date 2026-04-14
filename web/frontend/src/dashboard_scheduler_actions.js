import {
  saveAlarmEventUploadSchedulerConfigApi,
  saveDayMetricUploadSchedulerConfigApi,
  saveHandoverSchedulerConfigApi,
  saveSchedulerConfigApi,
  startAlarmEventUploadSchedulerApi,
  startDayMetricUploadSchedulerApi,
  startHandoverSchedulerApi,
  startSchedulerApi,
  stopAlarmEventUploadSchedulerApi,
  stopDayMetricUploadSchedulerApi,
  stopHandoverSchedulerApi,
  stopSchedulerApi,
} from "./api_client.js";
import { normalizeRunTimeText } from "./config_helpers.js";

function toPositiveInt(value, fallback) {
  const n = Number.parseInt(String(value ?? ""), 10);
  return Number.isInteger(n) && n > 0 ? n : fallback;
}

const ACTION_KEYS = {
  schedulerStart: "scheduler:start",
  schedulerStop: "scheduler:stop",
  schedulerSave: "scheduler:save",
  handoverSchedulerStart: "handover_scheduler:start",
  handoverSchedulerStop: "handover_scheduler:stop",
  handoverSchedulerSave: "handover_scheduler:save",
  dayMetricUploadSchedulerStart: "day_metric_upload_scheduler:start",
  dayMetricUploadSchedulerStop: "day_metric_upload_scheduler:stop",
  dayMetricUploadSchedulerSave: "day_metric_upload_scheduler:save",
  alarmEventUploadSchedulerStart: "alarm_event_upload_scheduler:start",
  alarmEventUploadSchedulerStop: "alarm_event_upload_scheduler:stop",
  alarmEventUploadSchedulerSave: "alarm_event_upload_scheduler:save",
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
    health,
    config,
    message,
    schedulerQuickSaving,
    handoverSchedulerQuickSaving,
    dayMetricUploadSchedulerQuickSaving,
    alarmEventUploadSchedulerQuickSaving,
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
    const payload = {
      enabled: true,
      auto_start_in_gui: false,
      interval_minutes: toPositiveInt(scheduler.interval_minutes, 60),
      check_interval_sec: toPositiveInt(scheduler.check_interval_sec, 30),
      retry_failed_on_next_tick: scheduler.retry_failed_on_next_tick !== false,
      state_file: String(scheduler.state_file || "daily_scheduler_state.json").trim(),
    };
    if (!payload.state_file) {
      message.value = "调度状态文件不能为空";
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
          if (!executorBound) {
            message.value = "调度配置已更新，但执行器未绑定，自动调度暂不可用";
          } else {
            message.value = data?.message || "调度配置已更新";
          }
        } catch (err) {
          message.value = `调度自动更新失败: ${err}`;
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
      enabled: true,
      auto_start_in_gui: false,
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
          message.value = `交接班调度自动更新失败: ${err}`;
        } finally {
          handoverSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 500 },
    );
  }

  function applySchedulerSnapshot(targetScheduler, data) {
    if (!targetScheduler || typeof targetScheduler !== "object" || !data || typeof data !== "object") return;
    Object.assign(targetScheduler, {
      enabled: Boolean(data.enabled),
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
      callback_name: String(data.callback_name || ""),
    });
  }

  async function startDayMetricUploadScheduler() {
    return guardedRun(
      ACTION_KEYS.dayMetricUploadSchedulerStart,
      async () => {
        try {
          const data = await startDayMetricUploadSchedulerApi();
          applySchedulerSnapshot(health?.day_metric_upload?.scheduler, data);
          await fetchHealth();
          message.value = `12项独立上传调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = `启动12项独立上传调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopDayMetricUploadScheduler() {
    return guardedRun(
      ACTION_KEYS.dayMetricUploadSchedulerStop,
      async () => {
        try {
          const data = await stopDayMetricUploadSchedulerApi();
          applySchedulerSnapshot(health?.day_metric_upload?.scheduler, data);
          await fetchHealth();
          message.value = `12项独立上传调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = `停止12项独立上传调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveDayMetricUploadSchedulerQuickConfig() {
    if (!config.value) return;
    const scheduler = config.value?.day_metric_upload?.scheduler || {};
    const payload = {
      enabled: true,
      auto_start_in_gui: false,
      interval_minutes: toPositiveInt(scheduler.interval_minutes, 60),
      check_interval_sec: toPositiveInt(scheduler.check_interval_sec, 30),
      retry_failed_on_next_tick: scheduler.retry_failed_on_next_tick !== false,
      state_file: String(scheduler.state_file || "day_metric_upload_scheduler_state.json").trim(),
    };
    if (!payload.state_file) {
      message.value = "12项独立上传调度状态文件不能为空";
      return;
    }
    return guardedRun(
      ACTION_KEYS.dayMetricUploadSchedulerSave,
      async () => {
        try {
          dayMetricUploadSchedulerQuickSaving.value = true;
          const data = await saveDayMetricUploadSchedulerConfigApi(payload);
          if (data?.scheduler_config && config.value?.day_metric_upload?.scheduler) {
            Object.assign(config.value.day_metric_upload.scheduler, data.scheduler_config);
          }
          applySchedulerSnapshot(health?.day_metric_upload?.scheduler, data);
          await fetchHealth();
          message.value = data?.message || "12项独立上传调度配置已更新";
        } catch (err) {
          message.value = `12项独立上传调度自动更新失败: ${err}`;
        } finally {
          dayMetricUploadSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function startAlarmEventUploadScheduler() {
    return guardedRun(
      ACTION_KEYS.alarmEventUploadSchedulerStart,
      async () => {
        try {
          const data = await startAlarmEventUploadSchedulerApi();
          applySchedulerSnapshot(health?.alarm_event_upload?.scheduler, data);
          await fetchHealth();
          message.value = `告警信息上传调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = `启动告警信息上传调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopAlarmEventUploadScheduler() {
    return guardedRun(
      ACTION_KEYS.alarmEventUploadSchedulerStop,
      async () => {
        try {
          const data = await stopAlarmEventUploadSchedulerApi();
          applySchedulerSnapshot(health?.alarm_event_upload?.scheduler, data);
          await fetchHealth();
          message.value = `告警信息上传调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = `停止告警信息上传调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveAlarmEventUploadSchedulerQuickConfig() {
    if (!config.value) return;
    const scheduler = config.value?.alarm_export?.scheduler || {};
    const runTime = normalizeRunTimeText(scheduler.run_time);
    const payload = {
      enabled: true,
      auto_start_in_gui: false,
      run_time: runTime,
      state_file: String(scheduler.state_file || "").trim(),
    };
    if (!payload.run_time) {
      message.value = "告警信息上传调度时间格式错误，必须是 HH:MM 或 HH:MM:SS";
      return;
    }
    if (!payload.state_file) {
      message.value = "告警信息上传调度状态文件不能为空";
      return;
    }
    return guardedRun(
      ACTION_KEYS.alarmEventUploadSchedulerSave,
      async () => {
        try {
          alarmEventUploadSchedulerQuickSaving.value = true;
          const data = await saveAlarmEventUploadSchedulerConfigApi(payload);
          if (data?.scheduler_config && config.value?.alarm_export?.scheduler) {
            Object.assign(config.value.alarm_export.scheduler, data.scheduler_config);
          }
          applySchedulerSnapshot(health?.alarm_event_upload?.scheduler, data);
          await fetchHealth();
          message.value = data?.message || "告警信息上传调度配置已更新";
        } catch (err) {
          message.value = `告警信息上传调度自动更新失败: ${err}`;
        } finally {
          alarmEventUploadSchedulerQuickSaving.value = false;
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
    startDayMetricUploadScheduler,
    stopDayMetricUploadScheduler,
    saveDayMetricUploadSchedulerQuickConfig,
    startAlarmEventUploadScheduler,
    stopAlarmEventUploadScheduler,
    saveAlarmEventUploadSchedulerQuickConfig,
  };
}
