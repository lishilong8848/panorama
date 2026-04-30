import {
  saveMonthlyChangeReportSchedulerConfigApi,
  saveMonthlyEventReportSchedulerConfigApi,
  startMonthlyChangeReportJobApi,
  startMonthlyChangeReportSchedulerApi,
  startMonthlyEventReportJobApi,
  startMonthlyEventReportSchedulerApi,
  startMonthlyReportSendJobApi,
  startMonthlyReportSendTestJobApi,
  stopMonthlyChangeReportSchedulerApi,
  stopMonthlyEventReportSchedulerApi,
} from "./api_client.js";
import { normalizeRunTimeText } from "./config_helpers.js";

const ACTION_KEYS = {
  runAll: "job:monthly_event_report:all",
  runBuildingPrefix: "job:monthly_event_report:building:",
  changeRunAll: "job:monthly_change_report:all",
  changeRunBuildingPrefix: "job:monthly_change_report:building:",
  sendAllPrefix: "job:monthly_report_send:all:",
  sendBuildingPrefix: "job:monthly_report_send:building:",
  sendTestPrefix: "job:monthly_report_send:test:",
  schedulerStart: "monthly_event_report_scheduler:start",
  schedulerStop: "monthly_event_report_scheduler:stop",
  schedulerSave: "monthly_event_report_scheduler:save",
  changeSchedulerStart: "monthly_change_report_scheduler:start",
  changeSchedulerStop: "monthly_change_report_scheduler:stop",
  changeSchedulerSave: "monthly_change_report_scheduler:save",
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

function syncLocalMonthlySchedulerAutoStart(targetScheduler, autoStart, options = {}) {
  if (!targetScheduler || typeof targetScheduler !== "object") return;
  targetScheduler.auto_start_in_gui = Boolean(autoStart);
  if (options.enableOnStart && autoStart) {
    targetScheduler.enabled = true;
  }
}

export function createDashboardMonthlyEventReportActions(ctx) {
  const {
    canRun,
    health,
    message,
    currentJob,
    selectedJobId,
    config,
    monthlyEventReportSchedulerQuickSaving,
    monthlyChangeReportSchedulerQuickSaving,
    monthlyReportTestReceiveIds,
    monthlyReportTestReceiveIdType,
    streamController,
    fetchExternalDashboardSummary,
    scheduleExternalDashboardRefresh,
    fetchJobs,
    runSingleFlight,
    setSchedulerToggleState,
  } = ctx;
  function triggerDashboardRefresh(reason = "monthly_report_action") {
    if (typeof scheduleExternalDashboardRefresh === "function") {
      scheduleExternalDashboardRefresh(reason, { force: true, delayMs: 0 });
      return;
    }
    if (typeof fetchExternalDashboardSummary === "function") {
      void fetchExternalDashboardSummary({ silentMessage: true, force: true });
    }
  }

  function markSchedulerToggle(key, mode, rememberedOverride) {
    if (typeof setSchedulerToggleState !== "function") return;
    setSchedulerToggleState(key, { mode, rememberedOverride });
  }

  function isInternalRole() {
    return String(config?.value?.deployment?.role_mode || "").trim().toLowerCase() === "internal";
  }

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

  function attachAcceptedJob(job, successMessage) {
    currentJob.value = job;
    if (selectedJobId) {
      selectedJobId.value = String(job?.job_id || "").trim();
    }
    if (job?.job_id) {
      streamController.attachJobStream(job.job_id);
    }
    message.value = successMessage;
  }

  function formatError(err, actionLabel) {
    const text = String(err || "").trim();
    return `${actionLabel}失败: ${text || "未知错误"}`;
  }

  function resolveTargetMonth(reportType) {
    const normalizedReportType = String(reportType || "event").trim().toLowerCase() || "event";
    if (normalizedReportType === "change") {
      return String(health?.monthly_change_report?.last_run?.target_month || "").trim() || "latest";
    }
    return String(health?.monthly_event_report?.last_run?.target_month || "").trim() || "latest";
  }

  async function runMonthlyEventReport(scope, building = "") {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供体系月度统计表入口，请在外网端发起。";
      return;
    }
    if (!canRun.value) return;
    const normalizedBuilding = String(building || "").trim();
    const actionKey =
      scope === "building"
        ? `${ACTION_KEYS.runBuildingPrefix}${normalizedBuilding}`
        : ACTION_KEYS.runAll;

    return guardedRun(
      actionKey,
      async () => {
        try {
          const payload =
            scope === "building"
              ? { scope: "building", building: normalizedBuilding }
              : { scope: "all" };
          const job = await startMonthlyEventReportJobApi(payload);
          attachAcceptedJob(
            job,
            scope === "building"
              ? `月度事件统计表处理已提交: ${normalizedBuilding}`
              : "月度事件统计表处理已提交: 全部楼栋",
          );
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = formatError(err, "提交月度事件统计表处理任务");
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function runMonthlyChangeReport(scope, building = "") {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供体系月度统计表入口，请在外网端发起。";
      return;
    }
    if (!canRun.value) return;
    const normalizedBuilding = String(building || "").trim();
    const actionKey =
      scope === "building"
        ? `${ACTION_KEYS.changeRunBuildingPrefix}${normalizedBuilding}`
        : ACTION_KEYS.changeRunAll;

    return guardedRun(
      actionKey,
      async () => {
        try {
          const payload =
            scope === "building"
              ? { scope: "building", building: normalizedBuilding }
              : { scope: "all" };
          const job = await startMonthlyChangeReportJobApi(payload);
          attachAcceptedJob(
            job,
            scope === "building"
              ? `月度变更统计表处理已提交: ${normalizedBuilding}`
              : "月度变更统计表处理已提交: 全部楼栋",
          );
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = formatError(err, "提交月度变更统计表处理任务");
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function sendMonthlyReport(reportType, scope, building = "") {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表发送入口，请在外网端发起。";
      return;
    }
    if (!canRun.value) return;
    const normalizedReportType = String(reportType || "event").trim().toLowerCase() || "event";
    const normalizedBuilding = String(building || "").trim();
    const targetMonth = resolveTargetMonth(normalizedReportType);
    const actionKey =
      scope === "building"
        ? `${ACTION_KEYS.sendBuildingPrefix}${normalizedReportType}:${normalizedBuilding}:${targetMonth}`
        : `${ACTION_KEYS.sendAllPrefix}${normalizedReportType}:${targetMonth}`;

    return guardedRun(
      actionKey,
      async () => {
        try {
          const payload =
            scope === "building"
              ? { report_type: normalizedReportType, scope: "building", building: normalizedBuilding }
              : { report_type: normalizedReportType, scope: "all" };
          const job = await startMonthlyReportSendJobApi(payload);
          attachAcceptedJob(
            job,
            scope === "building"
              ? `月度统计表发送已提交: ${normalizedBuilding}`
              : "月度统计表发送已提交: 全部楼栋",
          );
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = formatError(err, "提交月度统计表发送任务");
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function sendMonthlyReportTest(reportType = "event") {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表发送入口，请在外网端发起。";
      return;
    }
    if (!canRun.value) return;
    const normalizedReportType = String(reportType || "event").trim().toLowerCase() || "event";
    const targetMonth = resolveTargetMonth(normalizedReportType);
    const actionKey = `${ACTION_KEYS.sendTestPrefix}${normalizedReportType}:${targetMonth}`;
    const receiveIds = Array.isArray(monthlyReportTestReceiveIds?.value) ? monthlyReportTestReceiveIds.value : [];
    const receiveIdType = String(monthlyReportTestReceiveIdType?.value || "open_id").trim() || "open_id";
    if (!receiveIds.length) {
      message.value = "请先填写至少一个测试接收人 ID。";
      return;
    }

    return guardedRun(
      actionKey,
      async () => {
        try {
          const job = await startMonthlyReportSendTestJobApi({
            report_type: normalizedReportType,
            receive_ids: receiveIds,
            receive_id_type: receiveIdType,
          });
          attachAcceptedJob(job, `月度统计表测试发送已提交: ${receiveIds.length} 人`);
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = formatError(err, "提交月度统计表测试发送任务");
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function startMonthlyEventReportScheduler() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表调度入口，请在外网端发起。";
      return;
    }
    return guardedRun(
      ACTION_KEYS.schedulerStart,
      async () => {
        markSchedulerToggle("monthly_event_report", "starting", true);
        try {
          const data = await startMonthlyEventReportSchedulerApi();
          syncLocalMonthlySchedulerAutoStart(config.value?.handover_log?.monthly_event_report?.scheduler, true, { enableOnStart: true });
          applySchedulerSnapshot(health?.monthly_event_report?.scheduler, data);
          markSchedulerToggle("monthly_event_report", "idle", true);
          triggerDashboardRefresh("monthly_event_scheduler_start");
          message.value = `月度事件统计表调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("monthly_event_report", "idle", null);
          message.value = formatError(err, "启动月度事件统计表调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopMonthlyEventReportScheduler() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表调度入口，请在外网端发起。";
      return;
    }
    return guardedRun(
      ACTION_KEYS.schedulerStop,
      async () => {
        markSchedulerToggle("monthly_event_report", "stopping", false);
        try {
          const data = await stopMonthlyEventReportSchedulerApi();
          syncLocalMonthlySchedulerAutoStart(config.value?.handover_log?.monthly_event_report?.scheduler, false);
          applySchedulerSnapshot(health?.monthly_event_report?.scheduler, data);
          markSchedulerToggle("monthly_event_report", "idle", false);
          triggerDashboardRefresh("monthly_event_scheduler_stop");
          message.value = `月度事件统计表调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("monthly_event_report", "idle", null);
          message.value = formatError(err, "停止月度事件统计表调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveMonthlyEventReportSchedulerQuickConfig(overrides = {}) {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表调度入口，请在外网端发起。";
      return;
    }
    if (!config.value) return;
    const monthly = config.value.handover_log?.monthly_event_report || {};
    const scheduler = monthly.scheduler || {};
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
          : scheduler.day_of_month ?? 1,
      ), 10) || 1,
      run_time: runTime,
      check_interval_sec: Number.parseInt(String(
        Object.prototype.hasOwnProperty.call(overrideValues, "check_interval_sec")
          ? overrideValues.check_interval_sec
          : scheduler.check_interval_sec ?? 30,
      ), 10) || 30,
      state_file: String(scheduler.state_file || "").trim(),
    };
    if (!payload.run_time) {
      message.value = "月度事件统计表调度时间格式错误，必须是 HH:MM 或 HH:MM:SS。";
      return;
    }
    if (!payload.state_file) {
      message.value = "月度事件统计表调度状态文件名不能为空。";
      return;
    }
    if (!Number.isInteger(payload.day_of_month) || payload.day_of_month < 1 || payload.day_of_month > 31) {
      message.value = "月度事件统计表调度日期必须在 1 到 31 之间。";
      return;
    }
    if (!Number.isInteger(payload.check_interval_sec) || payload.check_interval_sec <= 0) {
      message.value = "月度事件统计表调度检查间隔必须大于 0 秒。";
      return;
    }
    return guardedRun(
      ACTION_KEYS.schedulerSave,
      async () => {
        try {
          monthlyEventReportSchedulerQuickSaving.value = true;
          const data = await saveMonthlyEventReportSchedulerConfigApi(payload);
          if (config.value?.handover_log?.monthly_event_report?.scheduler && data?.scheduler_config) {
            Object.assign(config.value.handover_log.monthly_event_report.scheduler, data.scheduler_config);
          }
          applySchedulerSnapshot(health?.monthly_event_report?.scheduler, data?.scheduler_status || data);
          triggerDashboardRefresh("monthly_event_scheduler_save");
          message.value = data?.message || "月度事件统计表调度配置已更新";
        } catch (err) {
          if (config.value?.handover_log?.monthly_event_report?.scheduler) {
            Object.assign(config.value.handover_log.monthly_event_report.scheduler, previousScheduler);
          }
          message.value = formatError(err, "月度事件统计表调度自动更新");
        } finally {
          monthlyEventReportSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 0, queueLatest: true },
    );
  }

  async function startMonthlyChangeReportScheduler() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表调度入口，请在外网端发起。";
      return;
    }
    return guardedRun(
      ACTION_KEYS.changeSchedulerStart,
      async () => {
        markSchedulerToggle("monthly_change_report", "starting", true);
        try {
          const data = await startMonthlyChangeReportSchedulerApi();
          syncLocalMonthlySchedulerAutoStart(config.value?.handover_log?.monthly_change_report?.scheduler, true, { enableOnStart: true });
          applySchedulerSnapshot(health?.monthly_change_report?.scheduler, data);
          markSchedulerToggle("monthly_change_report", "idle", true);
          triggerDashboardRefresh("monthly_change_scheduler_start");
          message.value = `月度变更统计表调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("monthly_change_report", "idle", null);
          message.value = formatError(err, "启动月度变更统计表调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopMonthlyChangeReportScheduler() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表调度入口，请在外网端发起。";
      return;
    }
    return guardedRun(
      ACTION_KEYS.changeSchedulerStop,
      async () => {
        markSchedulerToggle("monthly_change_report", "stopping", false);
        try {
          const data = await stopMonthlyChangeReportSchedulerApi();
          syncLocalMonthlySchedulerAutoStart(config.value?.handover_log?.monthly_change_report?.scheduler, false);
          applySchedulerSnapshot(health?.monthly_change_report?.scheduler, data);
          markSchedulerToggle("monthly_change_report", "idle", false);
          triggerDashboardRefresh("monthly_change_scheduler_stop");
          message.value = `月度变更统计表调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("monthly_change_report", "idle", null);
          message.value = formatError(err, "停止月度变更统计表调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveMonthlyChangeReportSchedulerQuickConfig(overrides = {}) {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表调度入口，请在外网端发起。";
      return;
    }
    if (!config.value) return;
    const monthly = config.value.handover_log?.monthly_change_report || {};
    const scheduler = monthly.scheduler || {};
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
          : scheduler.day_of_month ?? 1,
      ), 10) || 1,
      run_time: runTime,
      check_interval_sec: Number.parseInt(String(
        Object.prototype.hasOwnProperty.call(overrideValues, "check_interval_sec")
          ? overrideValues.check_interval_sec
          : scheduler.check_interval_sec ?? 30,
      ), 10) || 30,
      state_file: String(scheduler.state_file || "").trim(),
    };
    if (!payload.run_time) {
      message.value = "月度变更统计表调度时间格式错误，必须是 HH:MM 或 HH:MM:SS。";
      return;
    }
    if (!payload.state_file) {
      message.value = "月度变更统计表调度状态文件名不能为空。";
      return;
    }
    if (!Number.isInteger(payload.day_of_month) || payload.day_of_month < 1 || payload.day_of_month > 31) {
      message.value = "月度变更统计表调度日期必须在 1 到 31 之间。";
      return;
    }
    if (!Number.isInteger(payload.check_interval_sec) || payload.check_interval_sec <= 0) {
      message.value = "月度变更统计表调度检查间隔必须大于 0 秒。";
      return;
    }
    return guardedRun(
      ACTION_KEYS.changeSchedulerSave,
      async () => {
        try {
          monthlyChangeReportSchedulerQuickSaving.value = true;
          const data = await saveMonthlyChangeReportSchedulerConfigApi(payload);
          if (config.value?.handover_log?.monthly_change_report?.scheduler && data?.scheduler_config) {
            Object.assign(config.value.handover_log.monthly_change_report.scheduler, data.scheduler_config);
          }
          applySchedulerSnapshot(health?.monthly_change_report?.scheduler, data?.scheduler_status || data);
          triggerDashboardRefresh("monthly_change_scheduler_save");
          message.value = data?.message || "月度变更统计表调度配置已更新";
        } catch (err) {
          if (config.value?.handover_log?.monthly_change_report?.scheduler) {
            Object.assign(config.value.handover_log.monthly_change_report.scheduler, previousScheduler);
          }
          message.value = formatError(err, "月度变更统计表调度自动更新");
        } finally {
          monthlyChangeReportSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 0, queueLatest: true },
    );
  }

  return {
    runMonthlyEventReport,
    runMonthlyChangeReport,
    sendMonthlyReport,
    sendMonthlyReportTest,
    startMonthlyEventReportScheduler,
    stopMonthlyEventReportScheduler,
    saveMonthlyEventReportSchedulerQuickConfig,
    startMonthlyChangeReportScheduler,
    stopMonthlyChangeReportScheduler,
    saveMonthlyChangeReportSchedulerQuickConfig,
  };
}

