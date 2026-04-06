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
    fetchHealth,
    fetchJobs,
    runSingleFlight,
  } = ctx;

  function isInternalRole() {
    return String(config?.value?.deployment?.role_mode || "").trim().toLowerCase() === "internal";
  }

  function guardedRun(actionKey, taskFn, options = {}) {
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(actionKey, taskFn, options);
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
      message.value = "当前为内网端，本地管理页不提供月度统计表处理入口，请在外网端发起。";
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
      message.value = "当前为内网端，本地管理页不提供月度统计表处理入口，请在外网端发起。";
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
        try {
          const data = await startMonthlyEventReportSchedulerApi();
          applySchedulerSnapshot(health?.monthly_event_report?.scheduler, data);
          await fetchHealth();
          message.value = `月度事件统计表调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
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
        try {
          const data = await stopMonthlyEventReportSchedulerApi();
          applySchedulerSnapshot(health?.monthly_event_report?.scheduler, data);
          await fetchHealth();
          message.value = `月度事件统计表调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = formatError(err, "停止月度事件统计表调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveMonthlyEventReportSchedulerQuickConfig() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表调度入口，请在外网端发起。";
      return;
    }
    if (!config.value) return;
    const monthly = config.value.handover_log?.monthly_event_report || {};
    const scheduler = monthly.scheduler || {};
    const payload = {
      enabled: Boolean(scheduler.enabled),
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
      day_of_month: Number.parseInt(String(scheduler.day_of_month ?? 1), 10) || 1,
      run_time: String(scheduler.run_time || "").trim(),
      check_interval_sec: Number.parseInt(String(scheduler.check_interval_sec ?? 30), 10) || 30,
      state_file: String(scheduler.state_file || "").trim(),
    };
    if (!payload.run_time) {
      message.value = "月度事件统计表调度时间不能为空。";
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
          await fetchHealth();
          message.value = data?.message || "月度事件统计表调度配置已更新";
        } catch (err) {
          message.value = formatError(err, "保存月度事件统计表调度配置");
        } finally {
          monthlyEventReportSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 500 },
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
        try {
          const data = await startMonthlyChangeReportSchedulerApi();
          applySchedulerSnapshot(health?.monthly_change_report?.scheduler, data);
          await fetchHealth();
          message.value = `月度变更统计表调度启动结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
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
        try {
          const data = await stopMonthlyChangeReportSchedulerApi();
          applySchedulerSnapshot(health?.monthly_change_report?.scheduler, data);
          await fetchHealth();
          message.value = `月度变更统计表调度停止结果: ${formatSchedulerActionReason(data?.action?.reason)}`;
        } catch (err) {
          message.value = formatError(err, "停止月度变更统计表调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveMonthlyChangeReportSchedulerQuickConfig() {
    if (isInternalRole()) {
      message.value = "当前为内网端，本地管理页不提供月度统计表调度入口，请在外网端发起。";
      return;
    }
    if (!config.value) return;
    const monthly = config.value.handover_log?.monthly_change_report || {};
    const scheduler = monthly.scheduler || {};
    const payload = {
      enabled: Boolean(scheduler.enabled),
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
      day_of_month: Number.parseInt(String(scheduler.day_of_month ?? 1), 10) || 1,
      run_time: String(scheduler.run_time || "").trim(),
      check_interval_sec: Number.parseInt(String(scheduler.check_interval_sec ?? 30), 10) || 30,
      state_file: String(scheduler.state_file || "").trim(),
    };
    if (!payload.run_time) {
      message.value = "月度变更统计表调度时间不能为空。";
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
          await fetchHealth();
          message.value = data?.message || "月度变更统计表调度配置已更新";
        } catch (err) {
          message.value = formatError(err, "保存月度变更统计表调度配置");
        } finally {
          monthlyChangeReportSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 500 },
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
