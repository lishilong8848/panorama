import {
  saveChillerModeUploadSchedulerConfigApi,
  startChillerModeUploadJobApi,
  startChillerModeUploadSchedulerApi,
  stopChillerModeUploadSchedulerApi,
} from "./api_client.js";
import { cleanupChillerModeUploadCompat } from "./config_compat_cleanup.js";

const ACTION_KEYS = {
  run: "job:chiller_mode_upload",
  schedulerStart: "chiller_mode_upload_scheduler:start",
  schedulerStop: "chiller_mode_upload_scheduler:stop",
  schedulerSave: "chiller_mode_upload_scheduler:save",
};

function formatActionReason(reason) {
  const normalized = String(reason || "").trim().toLowerCase();
  if (!normalized || normalized === "ok") return "已完成";
  if (normalized === "started") return "已启动";
  if (normalized === "stopped") return "已停止";
  if (normalized === "already_running") return "已在运行";
  if (normalized === "disabled") return "未启用";
  if (normalized === "not_initialized") return "尚未初始化";
  return String(reason || "").trim() || "已完成";
}

function formatError(err, actionLabel) {
  const text = String(err || "").trim();
  if (text.includes('{"detail":"Not Found"}') || text.includes("HTTP 404") || text.includes("Not Found")) {
    return `${actionLabel}失败: 当前进程仍是更新前的旧版本，制冷模式参数上传接口尚未注册，请重启程序后再试。`;
  }
  return `${actionLabel}失败: ${text}`;
}

function syncLocalSchedulerAutoStart(targetScheduler, autoStart, options = {}) {
  if (!targetScheduler || typeof targetScheduler !== "object") return;
  targetScheduler.auto_start_in_gui = Boolean(autoStart);
  if (options.enableOnStart && autoStart) {
    targetScheduler.enabled = true;
  }
}

export function createDashboardChillerModeUploadActions(ctx) {
  const {
    canRun,
    config,
    health,
    message,
    currentJob,
    selectedJobId,
    streamController,
    fetchExternalDashboardSummary,
    scheduleExternalDashboardRefresh,
    fetchJobs,
    runSingleFlight,
    setSchedulerToggleState,
    chillerModeUploadSchedulerQuickSaving,
  } = ctx;

  function triggerDashboardRefresh(reason = "chiller_mode_upload_action") {
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
    setSchedulerToggleState("chiller_mode_upload", { mode, rememberedOverride });
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

  function applySchedulerSnapshotFromAction(data) {
    if (!data || typeof data !== "object" || !health?.chiller_mode_upload?.scheduler) return;
    Object.assign(health.chiller_mode_upload.scheduler, {
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
        : Boolean(health.chiller_mode_upload.scheduler.remembered_enabled),
      effective_auto_start_in_gui: Object.prototype.hasOwnProperty.call(data, "effective_auto_start_in_gui")
        ? Boolean(data.effective_auto_start_in_gui)
        : Boolean(health.chiller_mode_upload.scheduler.effective_auto_start_in_gui),
      memory_source: Object.prototype.hasOwnProperty.call(data, "memory_source")
        ? String(data.memory_source || "")
        : String(health.chiller_mode_upload.scheduler.memory_source || ""),
      display: data.display && typeof data.display === "object"
        ? { ...data.display }
        : (health.chiller_mode_upload.scheduler.display && typeof health.chiller_mode_upload.scheduler.display === "object"
          ? { ...health.chiller_mode_upload.scheduler.display }
          : {}),
    });
  }

  async function runChillerModeUpload() {
    if (!canRun.value) return;
    return guardedRun(
      ACTION_KEYS.run,
      async () => {
        try {
          message.value = "制冷模式参数上传任务已提交";
          const job = await startChillerModeUploadJobApi();
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
        } catch (err) {
          message.value = formatError(err, "制冷模式参数上传提交");
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function startChillerModeUploadScheduler() {
    return guardedRun(
      ACTION_KEYS.schedulerStart,
      async () => {
        markSchedulerToggle("starting", true);
        try {
          const data = await startChillerModeUploadSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.chiller_mode_upload?.scheduler, true, { enableOnStart: true });
          applySchedulerSnapshotFromAction(data);
          markSchedulerToggle("idle", true);
          triggerDashboardRefresh("chiller_mode_upload_scheduler_start");
          message.value = `制冷模式参数上传调度启动结果: ${formatActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("idle", null);
          message.value = formatError(err, "启动制冷模式参数上传调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function stopChillerModeUploadScheduler() {
    return guardedRun(
      ACTION_KEYS.schedulerStop,
      async () => {
        markSchedulerToggle("stopping", false);
        try {
          const data = await stopChillerModeUploadSchedulerApi();
          syncLocalSchedulerAutoStart(config.value?.chiller_mode_upload?.scheduler, false);
          applySchedulerSnapshotFromAction(data);
          markSchedulerToggle("idle", false);
          triggerDashboardRefresh("chiller_mode_upload_scheduler_stop");
          message.value = `制冷模式参数上传调度停止结果: ${formatActionReason(data?.action?.reason)}`;
        } catch (err) {
          markSchedulerToggle("idle", null);
          message.value = formatError(err, "停止制冷模式参数上传调度");
        }
      },
      { cooldownMs: 500 },
    );
  }

  async function saveChillerModeUploadSchedulerQuickConfig() {
    if (!config.value) return;
    const upload = config.value.chiller_mode_upload || {};
    const scheduler = upload.scheduler || {};
    const payload = {
      enabled: true,
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
      interval_minutes: Number.parseInt(String(scheduler.interval_minutes ?? 10), 10) || 10,
      check_interval_sec: Number.parseInt(String(scheduler.check_interval_sec ?? 30), 10) || 30,
      retry_failed_on_next_tick: Boolean(scheduler.retry_failed_on_next_tick),
      state_file: String(scheduler.state_file || "").trim(),
    };
    if (!payload.state_file) {
      message.value = "制冷模式参数上传状态文件名不能为空";
      return;
    }
    if (!Number.isInteger(payload.interval_minutes) || payload.interval_minutes <= 0) {
      message.value = "制冷模式参数上传执行间隔必须大于 0 分钟";
      return;
    }
    if (!Number.isInteger(payload.check_interval_sec) || payload.check_interval_sec <= 0) {
      message.value = "制冷模式参数上传检查间隔必须大于 0 秒";
      return;
    }
    return guardedRun(
      ACTION_KEYS.schedulerSave,
      async () => {
        try {
          chillerModeUploadSchedulerQuickSaving.value = true;
          const data = await saveChillerModeUploadSchedulerConfigApi(payload);
          if (config.value?.chiller_mode_upload?.scheduler && data?.scheduler_config) {
            const next = cleanupChillerModeUploadCompat({ scheduler: data.scheduler_config }).scheduler;
            Object.assign(config.value.chiller_mode_upload.scheduler, next || {});
          }
          applySchedulerSnapshotFromAction(data?.scheduler_status || data);
          triggerDashboardRefresh("chiller_mode_upload_scheduler_save");
          message.value = data?.message || "制冷模式参数上传调度配置已更新";
        } catch (err) {
          message.value = formatError(err, "制冷模式参数上传调度自动更新");
        } finally {
          chillerModeUploadSchedulerQuickSaving.value = false;
        }
      },
      { cooldownMs: 0, queueLatest: true },
    );
  }

  return {
    runChillerModeUpload,
    startChillerModeUploadScheduler,
    stopChillerModeUploadScheduler,
    saveChillerModeUploadSchedulerQuickConfig,
  };
}

