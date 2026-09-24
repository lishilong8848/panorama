import {
  saveAlarmRuleExportUploadSchedulerConfigApi,
  startAlarmRuleExportUploadSchedulerApi,
  stopAlarmRuleExportUploadSchedulerApi,
} from "./api_client.js";
import { normalizeRunTimeText } from "./config_helpers.js";

const KEYS = {
  start: "alarm_rule_export_upload_scheduler:start",
  stop: "alarm_rule_export_upload_scheduler:stop",
  save: "alarm_rule_export_upload_scheduler:save",
};

export function createDashboardAlarmRuleExportUploadActions(ctx) {
  const {
    health,
    message,
    config,
    alarmRuleExportUploadSchedulerQuickSaving,
    runSingleFlight,
    setSchedulerToggleState,
    scheduleExternalDashboardRefresh,
    fetchExternalDashboardSummary,
  } = ctx;

  const guardedRun = (key, task, options = {}) => (
    typeof runSingleFlight === "function"
      ? runSingleFlight(key, task, { ...options, onCooldown: () => { message.value = "请求处理中，请稍候"; } })
      : task()
  );

  function refresh(reason) {
    if (typeof scheduleExternalDashboardRefresh === "function") {
      scheduleExternalDashboardRefresh(reason, { force: true, delayMs: 0 });
    } else if (typeof fetchExternalDashboardSummary === "function") {
      void fetchExternalDashboardSummary({ silentMessage: true, force: true });
    }
  }

  function apply(data) {
    const target = health?.alarm_rule_export_upload?.scheduler;
    if (target && data && typeof data === "object") Object.assign(target, data);
  }

  function syncLocal(autoStart) {
    const scheduler = config.value?.alarm_rule_export_upload?.scheduler;
    if (!scheduler) return;
    scheduler.auto_start_in_gui = Boolean(autoStart);
    if (autoStart) scheduler.enabled = true;
    scheduler.catch_up_if_missed = false;
  }

  async function startAlarmRuleExportUploadScheduler() {
    return guardedRun(KEYS.start, async () => {
      setSchedulerToggleState?.("alarm_rule_export_upload", { mode: "starting", rememberedOverride: true });
      try {
        const data = await startAlarmRuleExportUploadSchedulerApi();
        syncLocal(true);
        apply(data);
        refresh("alarm_rule_export_upload_scheduler_start");
        message.value = "告警规则附件月度调度已启动";
      } catch (err) {
        message.value = `启动告警规则附件调度失败: ${String(err || "未知错误")}`;
      } finally {
        setSchedulerToggleState?.("alarm_rule_export_upload", { mode: "idle" });
      }
    }, { cooldownMs: 500 });
  }

  async function stopAlarmRuleExportUploadScheduler() {
    return guardedRun(KEYS.stop, async () => {
      setSchedulerToggleState?.("alarm_rule_export_upload", { mode: "stopping", rememberedOverride: false });
      try {
        const data = await stopAlarmRuleExportUploadSchedulerApi();
        syncLocal(false);
        apply(data);
        refresh("alarm_rule_export_upload_scheduler_stop");
        message.value = "告警规则附件月度调度已停止";
      } catch (err) {
        message.value = `停止告警规则附件调度失败: ${String(err || "未知错误")}`;
      } finally {
        setSchedulerToggleState?.("alarm_rule_export_upload", { mode: "idle" });
      }
    }, { cooldownMs: 500 });
  }

  async function saveAlarmRuleExportUploadSchedulerQuickConfig(overrides = {}) {
    const scheduler = config.value?.alarm_rule_export_upload?.scheduler;
    if (!scheduler) return;
    const payload = {
      enabled: true,
      auto_start_in_gui: Boolean(scheduler.auto_start_in_gui),
      day_of_month: Number.parseInt(String(overrides.day_of_month ?? scheduler.day_of_month ?? 3), 10),
      run_time: normalizeRunTimeText(overrides.run_time ?? scheduler.run_time),
      check_interval_sec: Number.parseInt(String(
        overrides.check_interval_sec ?? scheduler.check_interval_sec ?? 30
      ), 10),
      state_file: String(scheduler.state_file || "alarm_rule_export_upload_scheduler_state.json").trim(),
    };
    if (!Number.isInteger(payload.day_of_month) || payload.day_of_month < 1 || payload.day_of_month > 31) {
      message.value = "告警规则附件调度日期必须在1到31之间";
      return;
    }
    if (!payload.run_time) {
      message.value = "告警规则附件调度时间格式错误";
      return;
    }
    return guardedRun(KEYS.save, async () => {
      alarmRuleExportUploadSchedulerQuickSaving.value = true;
      try {
        const data = await saveAlarmRuleExportUploadSchedulerConfigApi(payload);
        if (data?.scheduler_config) Object.assign(scheduler, data.scheduler_config);
        apply(data?.scheduler_status || data);
        refresh("alarm_rule_export_upload_scheduler_save");
        message.value = data?.message || "告警规则附件调度配置已更新";
      } catch (err) {
        message.value = `告警规则附件调度配置更新失败: ${String(err || "未知错误")}`;
      } finally {
        alarmRuleExportUploadSchedulerQuickSaving.value = false;
      }
    }, { cooldownMs: 0, queueLatest: true });
  }

  return {
    startAlarmRuleExportUploadScheduler,
    stopAlarmRuleExportUploadScheduler,
    saveAlarmRuleExportUploadSchedulerQuickConfig,
  };
}

