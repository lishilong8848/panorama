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

function syncLocalSchedulerAutoStart(targetScheduler, autoStart, options = {}) {
  if (!targetScheduler || typeof targetScheduler !== "object") return;
  targetScheduler.auto_start_in_gui = Boolean(autoStart);
  if (options.enableOnStart && autoStart) {
    targetScheduler.enabled = true;
  }
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
    fetchExternalDashboardSummary,
    scheduleExternalDashboardRefresh,
    runSingleFlight,
    setSchedulerToggleState,
  } = ctx;

  function triggerDashboardRefresh(reason = "scheduler_action") {
    if (typeof scheduleExternalDashboardRefresh === "function") {
      scheduleExternalDashboardRefresh(reason);
      return;
    }
    if (typeof fetchExternalDashboardSummary === "function") {
      void fetchExternalDashboardSummary({ silentMessage: true });
    }
  }

  function markSchedulerToggle(key, mode, rememberedOverride) {
    if (typeof setSchedulerToggleState !== "function") return;
    setSchedulerToggleState(key, { mode, rememberedOverride });
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

  async function startScheduler() {
    return guardedRun(
      ACTION_KEYS.schedulerStart,
      async () => {
        markSchedulerToggle("scheduler", "starting", true);
        try {
          const data = await startSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.scheduler, true, { enableOnStart: true });
          applySchedulerSnapshot(health?.scheduler, { ...data, enabled: true, running: true });
          markSchedulerToggle("scheduler", "idle", true);
          triggerDashboardRefresh("scheduler_start");
          message.value = `调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("scheduler", "idle", null);
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
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
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
          applySchedulerSnapshot(health?.scheduler, data?.scheduler_status || data);
          triggerDashboardRefresh("scheduler_save");
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
      { cooldownMs: 0, queueLatest: true },
    );
  }

  async function stopScheduler() {
    return guardedRun(
      ACTION_KEYS.schedulerStop,
      async () => {
        markSchedulerToggle("scheduler", "stopping", false);
        try {
          const data = await stopSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.scheduler, false);
          applySchedulerSnapshot(health?.scheduler, { ...data, running: false });
          markSchedulerToggle("scheduler", "idle", false);
          triggerDashboardRefresh("scheduler_stop");
          message.value = `调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("scheduler", "idle", null);
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
        markSchedulerToggle("handover", "starting", true);
        try {
          const data = await startHandoverSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.handover_log?.scheduler, true, { enableOnStart: true });
          applyHandoverSchedulerSnapshot(health?.handover_scheduler, { ...data, enabled: true, running: true });
          markSchedulerToggle("handover", "idle", true);
          triggerDashboardRefresh("handover_scheduler_start");
          message.value = `交接班调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("handover", "idle", null);
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
        markSchedulerToggle("handover", "stopping", false);
        try {
          const data = await stopHandoverSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.handover_log?.scheduler, false);
          applyHandoverSchedulerSnapshot(health?.handover_scheduler, { ...data, running: false });
          markSchedulerToggle("handover", "idle", false);
          triggerDashboardRefresh("handover_scheduler_stop");
          message.value = `交接班调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("handover", "idle", null);
          message.value = `停止交接班调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveHandoverSchedulerQuickConfig(overrides = {}) {
    if (!config.value) return;
    const handoverScheduler = config.value?.handover_log?.scheduler || {};
    const overrideValues = overrides && typeof overrides === "object" ? overrides : {};
    const overrideKeys = Object.keys(overrideValues);
    const hasExplicitOverrides = overrideKeys.length > 0;
    const shouldSaveMorning =
      !hasExplicitOverrides || Object.prototype.hasOwnProperty.call(overrideValues, "morning_time");
    const shouldSaveAfternoon =
      !hasExplicitOverrides || Object.prototype.hasOwnProperty.call(overrideValues, "afternoon_time");
    const payload = {
      enabled: true,
    };

    if (shouldSaveMorning) {
      const morningTime = normalizeRunTimeText(
        Object.prototype.hasOwnProperty.call(overrideValues, "morning_time")
          ? overrideValues.morning_time
          : handoverScheduler.morning_time,
      );
      if (!morningTime) {
        message.value = "交接班调度上午时间格式错误，必须是 HH:MM 或 HH:MM:SS";
        return;
      }
      handoverScheduler.morning_time = morningTime;
      payload.morning_time = morningTime;
    }

    if (shouldSaveAfternoon) {
      const afternoonTime = normalizeRunTimeText(
        Object.prototype.hasOwnProperty.call(overrideValues, "afternoon_time")
          ? overrideValues.afternoon_time
          : handoverScheduler.afternoon_time,
      );
      if (!afternoonTime) {
        message.value = "交接班调度下午时间格式错误，必须是 HH:MM 或 HH:MM:SS";
        return;
      }
      handoverScheduler.afternoon_time = afternoonTime;
      payload.afternoon_time = afternoonTime;
    }

    if (!hasExplicitOverrides) {
      payload.auto_start_in_gui = Boolean(handoverScheduler.auto_start_in_gui);
      payload.check_interval_sec = Number.parseInt(String(handoverScheduler.check_interval_sec ?? 30), 10) || 30;
      payload.catch_up_if_missed = Boolean(handoverScheduler.catch_up_if_missed);
      payload.retry_failed_in_same_period = Boolean(handoverScheduler.retry_failed_in_same_period);
      payload.morning_state_file = String(handoverScheduler.morning_state_file || "").trim();
      payload.afternoon_state_file = String(handoverScheduler.afternoon_state_file || "").trim();
      if (!payload.morning_state_file || !payload.afternoon_state_file) {
        message.value = "交接班调度状态文件不能为空";
        return;
      }
      if (!Number.isInteger(payload.check_interval_sec) || payload.check_interval_sec <= 0) {
        message.value = "交接班调度检查间隔必须大于0";
        return;
      }
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
          applyHandoverSchedulerSnapshot(health?.handover_scheduler, data?.scheduler_status || data);
          triggerDashboardRefresh("handover_scheduler_save");
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
      { cooldownMs: 0, queueLatest: true },
    );
  }

  function applySchedulerSnapshot(targetScheduler, data) {
    if (!targetScheduler || typeof targetScheduler !== "object" || !data || typeof data !== "object") return;
    Object.assign(targetScheduler, {
      enabled: typeof data.enabled === "boolean" ? data.enabled : Boolean(targetScheduler.enabled),
      running: typeof data.running === "boolean" ? data.running : Boolean(targetScheduler.running),
      status: Object.prototype.hasOwnProperty.call(data, "status")
        ? String(data.status || "")
        : String(targetScheduler.status || ""),
      next_run_time: Object.prototype.hasOwnProperty.call(data, "next_run_time")
        ? String(data.next_run_time || "")
        : String(targetScheduler.next_run_time || ""),
      last_check_at: Object.prototype.hasOwnProperty.call(data, "last_check_at")
        ? String(data.last_check_at || "")
        : String(targetScheduler.last_check_at || ""),
      last_decision: Object.prototype.hasOwnProperty.call(data, "last_decision")
        ? String(data.last_decision || "")
        : String(targetScheduler.last_decision || ""),
      last_trigger_at: Object.prototype.hasOwnProperty.call(data, "last_trigger_at")
        ? String(data.last_trigger_at || "")
        : String(targetScheduler.last_trigger_at || ""),
      last_trigger_result: Object.prototype.hasOwnProperty.call(data, "last_trigger_result")
        ? String(data.last_trigger_result || "")
        : String(targetScheduler.last_trigger_result || ""),
      state_path: Object.prototype.hasOwnProperty.call(data, "state_path")
        ? String(data.state_path || "")
        : String(targetScheduler.state_path || ""),
      state_exists: typeof data.state_exists === "boolean" ? data.state_exists : Boolean(targetScheduler.state_exists),
      executor_bound: typeof data.executor_bound === "boolean" ? data.executor_bound : Boolean(targetScheduler.executor_bound),
      callback_name: Object.prototype.hasOwnProperty.call(data, "callback_name")
        ? String(data.callback_name || "")
        : String(targetScheduler.callback_name || ""),
      remembered_enabled: Object.prototype.hasOwnProperty.call(data, "remembered_enabled")
        ? Boolean(data.remembered_enabled)
        : Boolean(targetScheduler.remembered_enabled),
      effective_auto_start_in_gui: Object.prototype.hasOwnProperty.call(data, "effective_auto_start_in_gui")
        ? Boolean(data.effective_auto_start_in_gui)
        : Boolean(targetScheduler.effective_auto_start_in_gui),
      memory_source: Object.prototype.hasOwnProperty.call(data, "memory_source")
        ? String(data.memory_source || "")
        : String(targetScheduler.memory_source || ""),
      display: data.display && typeof data.display === "object"
        ? { ...data.display }
        : (targetScheduler.display && typeof targetScheduler.display === "object" ? { ...targetScheduler.display } : {}),
    });
  }

  function applyHandoverSchedulerSnapshot(targetScheduler, data) {
    if (!targetScheduler || typeof targetScheduler !== "object" || !data || typeof data !== "object") return;
    Object.assign(targetScheduler, {
      enabled: typeof data.enabled === "boolean" ? data.enabled : Boolean(targetScheduler.enabled),
      running: typeof data.running === "boolean" ? data.running : Boolean(targetScheduler.running),
      status: Object.prototype.hasOwnProperty.call(data, "status")
        ? String(data.status || "")
        : String(targetScheduler.status || ""),
      executor_bound: typeof data.executor_bound === "boolean" ? data.executor_bound : Boolean(targetScheduler.executor_bound),
      callback_name: Object.prototype.hasOwnProperty.call(data, "callback_name")
        ? String(data.callback_name || "")
        : String(targetScheduler.callback_name || ""),
      remembered_enabled: Object.prototype.hasOwnProperty.call(data, "remembered_enabled")
        ? Boolean(data.remembered_enabled)
        : Boolean(targetScheduler.remembered_enabled),
      effective_auto_start_in_gui: Object.prototype.hasOwnProperty.call(data, "effective_auto_start_in_gui")
        ? Boolean(data.effective_auto_start_in_gui)
        : Boolean(targetScheduler.effective_auto_start_in_gui),
      memory_source: Object.prototype.hasOwnProperty.call(data, "memory_source")
        ? String(data.memory_source || "")
        : String(targetScheduler.memory_source || ""),
      display: data.display && typeof data.display === "object"
        ? { ...data.display }
        : (targetScheduler.display && typeof targetScheduler.display === "object" ? { ...targetScheduler.display } : {}),
      state_paths: data.state_paths && typeof data.state_paths === "object"
        ? { ...data.state_paths }
        : (targetScheduler.state_paths && typeof targetScheduler.state_paths === "object" ? { ...targetScheduler.state_paths } : {}),
    });
    if (data.morning && typeof data.morning === "object" && targetScheduler.morning && typeof targetScheduler.morning === "object") {
      Object.assign(targetScheduler.morning, data.morning);
    }
    if (data.afternoon && typeof data.afternoon === "object" && targetScheduler.afternoon && typeof targetScheduler.afternoon === "object") {
      Object.assign(targetScheduler.afternoon, data.afternoon);
    }
  }

  async function startDayMetricUploadScheduler() {
    return guardedRun(
      ACTION_KEYS.dayMetricUploadSchedulerStart,
      async () => {
        markSchedulerToggle("day_metric_upload", "starting", true);
        try {
          const data = await startDayMetricUploadSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.day_metric_upload?.scheduler, true, { enableOnStart: true });
          applySchedulerSnapshot(health?.day_metric_upload?.scheduler, { ...data, enabled: true, running: true });
          markSchedulerToggle("day_metric_upload", "idle", true);
          triggerDashboardRefresh("day_metric_scheduler_start");
          message.value = `12项独立上传调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("day_metric_upload", "idle", null);
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
        markSchedulerToggle("day_metric_upload", "stopping", false);
        try {
          const data = await stopDayMetricUploadSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.day_metric_upload?.scheduler, false);
          applySchedulerSnapshot(health?.day_metric_upload?.scheduler, { ...data, running: false });
          markSchedulerToggle("day_metric_upload", "idle", false);
          triggerDashboardRefresh("day_metric_scheduler_stop");
          message.value = `12项独立上传调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("day_metric_upload", "idle", null);
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
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
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
          applySchedulerSnapshot(health?.day_metric_upload?.scheduler, data?.scheduler_status || data);
          triggerDashboardRefresh("day_metric_scheduler_save");
          message.value = data?.message || "12项独立上传调度配置已更新";
        } catch (err) {
          message.value = `12项独立上传调度自动更新失败: ${err}`;
        } finally {
          dayMetricUploadSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 0, queueLatest: true },
    );
  }

  async function startAlarmEventUploadScheduler() {
    return guardedRun(
      ACTION_KEYS.alarmEventUploadSchedulerStart,
      async () => {
        markSchedulerToggle("alarm_event_upload", "starting", true);
        try {
          const data = await startAlarmEventUploadSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.alarm_export?.scheduler, true, { enableOnStart: true });
          applySchedulerSnapshot(health?.alarm_event_upload?.scheduler, { ...data, enabled: true, running: true });
          markSchedulerToggle("alarm_event_upload", "idle", true);
          triggerDashboardRefresh("alarm_scheduler_start");
          message.value = `告警信息上传调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("alarm_event_upload", "idle", null);
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
        markSchedulerToggle("alarm_event_upload", "stopping", false);
        try {
          const data = await stopAlarmEventUploadSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.alarm_export?.scheduler, false);
          applySchedulerSnapshot(health?.alarm_event_upload?.scheduler, { ...data, running: false });
          markSchedulerToggle("alarm_event_upload", "idle", false);
          triggerDashboardRefresh("alarm_scheduler_stop");
          message.value = `告警信息上传调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("alarm_event_upload", "idle", null);
          message.value = `停止告警信息上传调度失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveAlarmEventUploadSchedulerQuickConfig(overrides = {}) {
    if (!config.value) return;
    const scheduler = config.value?.alarm_export?.scheduler || {};
    const overrideValues = overrides && typeof overrides === "object" ? overrides : {};
    const runTime = normalizeRunTimeText(
      Object.prototype.hasOwnProperty.call(overrideValues, "run_time")
        ? overrideValues.run_time
        : scheduler.run_time,
    );
    const payload = {
      enabled: true,
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
      run_time: runTime,
      state_file: String(scheduler.state_file || "").trim(),
    };
    if (!payload.run_time) {
      message.value = "告警信息上传调度时间格式错误，必须是 HH:MM 或 HH:MM:SS";
      return;
    }
    scheduler.run_time = runTime;
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
          applySchedulerSnapshot(health?.alarm_event_upload?.scheduler, data?.scheduler_status || data);
          triggerDashboardRefresh("alarm_scheduler_save");
          message.value = data?.message || "告警信息上传调度配置已更新";
        } catch (err) {
          message.value = `告警信息上传调度自动更新失败: ${err}`;
        } finally {
          alarmEventUploadSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 0, queueLatest: true },
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
