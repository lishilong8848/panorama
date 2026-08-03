import {
  saveTop5PowerReportSchedulerConfigApi,
  startTop5PowerReportSchedulerApi,
  stopTop5PowerReportSchedulerApi,
} from "./api_client.js";
import { normalizeRunTimeText } from "./config_helpers.js";

const ACTION_KEYS = {
  start: "top5_power_report_scheduler:start",
  stop: "top5_power_report_scheduler:stop",
  save: "top5_power_report_scheduler:save",
};

function formatSchedulerActionReason(reason) {
  const normalized = String(reason || "").trim().toLowerCase();
  if (!normalized || normalized === "ok") return "已完成";
  if (normalized === "started") return "已启动";
  if (normalized === "stopped") return "已停止";
  if (normalized === "already_running") return "已在运行";
  if (normalized === "disabled") return "未启用";
  if (normalized === "not_initialized") return "尚未初始化";
  return String(reason || "").trim() || "已完成";
}

function applySchedulerSnapshot(targetScheduler, data) {
  if (!data || typeof data !== "object" || !targetScheduler || typeof targetScheduler !== "object") return;
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
      : Boolean(targetScheduler.remembered_enabled),
    effective_auto_start_in_gui: Object.prototype.hasOwnProperty.call(data, "effective_auto_start_in_gui")
      ? Boolean(data.effective_auto_start_in_gui)
      : Boolean(targetScheduler.effective_auto_start_in_gui),
    memory_source: Object.prototype.hasOwnProperty.call(data, "memory_source")
      ? String(data.memory_source || "")
      : String(targetScheduler.memory_source || ""),
  });
}

export function createDashboardTop5PowerReportActions(ctx) {
  const {
    health,
    message,
    config,
    top5PowerReportSchedulerQuickSaving,
    fetchExternalDashboardSummary,
    scheduleExternalDashboardRefresh,
    runSingleFlight,
    setSchedulerToggleState,
  } = ctx;

  function guardedRun(actionKey, taskFn, options = {}) {
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

  function triggerDashboardRefresh(reason) {
    if (typeof scheduleExternalDashboardRefresh === "function") {
      scheduleExternalDashboardRefresh(reason, { force: true, delayMs: 0 });
      return;
    }
    if (typeof fetchExternalDashboardSummary === "function") {
      void fetchExternalDashboardSummary({ silentMessage: true, force: true });
    }
  }

  function markSchedulerToggle(mode, rememberedOverride) {
    if (typeof setSchedulerToggleState === "function") {
      setSchedulerToggleState("top5_power_report", { mode, rememberedOverride });
    }
  }

  function syncLocalAutoStart(autoStart, options = {}) {
    const scheduler = config.value?.handover_log?.top5_power_report?.scheduler;
    if (!scheduler || typeof scheduler !== "object") return;
    scheduler.auto_start_in_gui = Boolean(autoStart);
    if (options.enableOnStart && autoStart) scheduler.enabled = true;
    scheduler.catch_up_if_missed = false;
  }

  async function startTop5PowerReportScheduler() {
    return guardedRun(
      ACTION_KEYS.start,
      async () => {
        markSchedulerToggle("starting", true);
        try {
          const data = await startTop5PowerReportSchedulerApi();
          syncLocalAutoStart(true, { enableOnStart: true });
          applySchedulerSnapshot(health?.top5_power_report?.scheduler, data);
          markSchedulerToggle("idle", true);
          triggerDashboardRefresh("top5_power_report_scheduler_start");
          message.value = `TOP5月度调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("idle", null);
          message.value = `启动TOP5月度调度失败: ${String(err || "未知错误")}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopTop5PowerReportScheduler() {
    return guardedRun(
      ACTION_KEYS.stop,
      async () => {
        markSchedulerToggle("stopping", false);
        try {
          const data = await stopTop5PowerReportSchedulerApi();
          syncLocalAutoStart(false);
          applySchedulerSnapshot(health?.top5_power_report?.scheduler, data);
          markSchedulerToggle("idle", false);
          triggerDashboardRefresh("top5_power_report_scheduler_stop");
          message.value = `TOP5月度调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("idle", null);
          message.value = `停止TOP5月度调度失败: ${String(err || "未知错误")}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveTop5PowerReportSchedulerQuickConfig(overrides = {}) {
    if (!config.value) return;
    const scheduler = config.value.handover_log?.top5_power_report?.scheduler || {};
    const previousScheduler = { ...scheduler };
    const overrideValues = overrides && typeof overrides === "object" ? overrides : {};
    const runTime = normalizeRunTimeText(
      Object.prototype.hasOwnProperty.call(overrideValues, "run_time")
        ? overrideValues.run_time
        : scheduler.run_time,
    );
    const payload = {
      enabled: true,
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
      day_of_month: Number.parseInt(String(
        Object.prototype.hasOwnProperty.call(overrideValues, "day_of_month")
          ? overrideValues.day_of_month
          : scheduler.day_of_month ?? 3,
      ), 10) || 3,
      run_time: runTime,
      check_interval_sec: Number.parseInt(String(
        Object.prototype.hasOwnProperty.call(overrideValues, "check_interval_sec")
          ? overrideValues.check_interval_sec
          : scheduler.check_interval_sec ?? 30,
      ), 10) || 30,
      catch_up_if_missed: false,
      state_file: String(scheduler.state_file || "top5_power_report_scheduler_state.json").trim(),
    };
    if (!payload.run_time) {
      message.value = "TOP5月度调度时间格式错误，必须是 HH:MM 或 HH:MM:SS。";
      return;
    }
    if (!Number.isInteger(payload.day_of_month) || payload.day_of_month < 1 || payload.day_of_month > 31) {
      message.value = "TOP5月度调度日期必须在 1 到 31 之间。";
      return;
    }
    if (!Number.isInteger(payload.check_interval_sec) || payload.check_interval_sec <= 0) {
      message.value = "TOP5月度调度检查间隔必须大于 0 秒。";
      return;
    }
    if (!payload.state_file) {
      message.value = "TOP5月度调度状态文件名不能为空。";
      return;
    }

    return guardedRun(
      ACTION_KEYS.save,
      async () => {
        try {
          top5PowerReportSchedulerQuickSaving.value = true;
          const data = await saveTop5PowerReportSchedulerConfigApi(payload);
          const targetScheduler = config.value?.handover_log?.top5_power_report?.scheduler;
          if (targetScheduler && data?.scheduler_config) {
            Object.assign(targetScheduler, data.scheduler_config, { catch_up_if_missed: false });
          }
          applySchedulerSnapshot(health?.top5_power_report?.scheduler, data?.scheduler_status || data);
          triggerDashboardRefresh("top5_power_report_scheduler_save");
          message.value = data?.message || "TOP5月度调度配置已更新";
        } catch (err) {
          const targetScheduler = config.value?.handover_log?.top5_power_report?.scheduler;
          if (targetScheduler) Object.assign(targetScheduler, previousScheduler);
          message.value = `TOP5月度调度配置更新失败: ${String(err || "未知错误")}`;
        } finally {
          top5PowerReportSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 0, queueLatest: true },
    );
  }

  return {
    startTop5PowerReportScheduler,
    stopTop5PowerReportScheduler,
    saveTop5PowerReportSchedulerQuickConfig,
  };
}
