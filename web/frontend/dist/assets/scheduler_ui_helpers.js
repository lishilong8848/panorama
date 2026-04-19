export function createSchedulerUiHelpers(options = {}) {
  const {
    health,
    config,
    schedulerToggleState,
    isActionLocked,
  } = options || {};

  function setSchedulerToggleState(key, patch = {}) {
    const name = String(key || "").trim();
    const entry = schedulerToggleState?.[name];
    if (!entry || typeof entry !== "object") return;
    if (Object.prototype.hasOwnProperty.call(patch, "mode")) {
      entry.mode = String(patch.mode || "idle").trim() || "idle";
    }
    if (Object.prototype.hasOwnProperty.call(patch, "rememberedOverride")) {
      entry.rememberedOverride = typeof patch.rememberedOverride === "boolean" ? patch.rememberedOverride : null;
    } else if (Object.prototype.hasOwnProperty.call(patch, "runningOverride")) {
      entry.rememberedOverride = typeof patch.runningOverride === "boolean" ? patch.runningOverride : null;
    }
  }

  function syncSchedulerToggleStateWithHealth(key, actualRememberedEnabled) {
    const name = String(key || "").trim();
    const entry = schedulerToggleState?.[name];
    if (!entry || typeof entry !== "object") return;
    const actual = Boolean(actualRememberedEnabled);
    if (typeof entry.rememberedOverride === "boolean" && entry.rememberedOverride === actual) {
      entry.rememberedOverride = null;
    }
    if (entry.mode !== "idle" && entry.rememberedOverride === null) {
      entry.mode = "idle";
    }
  }

  function getSchedulerToggleMode(key) {
    const entry = schedulerToggleState?.[String(key || "").trim()];
    return String(entry?.mode || "idle").trim() || "idle";
  }

  function getSchedulerSnapshotByKey(key) {
    const normalized = String(key || "").trim();
    if (normalized === "scheduler") return health.scheduler;
    if (normalized === "handover") return health.handover_scheduler;
    if (normalized === "wet_bulb") return health.wet_bulb_collection?.scheduler || {};
    if (normalized === "day_metric_upload") return health.day_metric_upload?.scheduler || {};
    if (normalized === "alarm_event_upload") return health.alarm_event_upload?.scheduler || {};
    if (normalized === "monthly_event_report") return health.monthly_event_report?.scheduler || {};
    if (normalized === "monthly_change_report") return health.monthly_change_report?.scheduler || {};
    return {};
  }

  function getSchedulerDisplayState(keyOrSnapshot) {
    const snapshot = keyOrSnapshot && typeof keyOrSnapshot === "object"
      ? keyOrSnapshot
      : getSchedulerSnapshotByKey(keyOrSnapshot);
    return snapshot?.display && typeof snapshot.display === "object" ? snapshot.display : {};
  }

  function getSchedulerAction(keyOrSnapshot, actionName) {
    const display = getSchedulerDisplayState(keyOrSnapshot);
    const actions = display?.actions;
    if (actions && typeof actions === "object" && actions[actionName] && typeof actions[actionName] === "object") {
      return actions[actionName];
    }
    return null;
  }

  function getSchedulerStatusTone(keyOrSnapshot) {
    const display = getSchedulerDisplayState(keyOrSnapshot);
    const explicit = String(display?.tone || "").trim();
    if (explicit) return explicit;
    return "neutral";
  }

  function getSchedulerStatusText(keyOrSnapshot) {
    const display = getSchedulerDisplayState(keyOrSnapshot);
    const explicit = String(display?.status_text || display?.statusText || "").trim();
    if (explicit) return explicit;
    return "等待后端状态";
  }

  function getSchedulerDisplayText(keyOrSnapshot, fieldName, fallback = "-") {
    const display = getSchedulerDisplayState(keyOrSnapshot);
    const explicit = String(display?.[fieldName] || "").trim();
    if (explicit) return explicit;
    return String(fallback || "-").trim() || "-";
  }

  function getSchedulerEffectiveRunning(key, actualRememberedEnabled) {
    const entry = schedulerToggleState?.[String(key || "").trim()];
    if (typeof entry?.rememberedOverride === "boolean") {
      return entry.rememberedOverride;
    }
    return Boolean(actualRememberedEnabled);
  }

  function getSchedulerEffectiveRemembered(key, actualRememberedEnabled) {
    return getSchedulerEffectiveRunning(key, actualRememberedEnabled);
  }

  function isSchedulerTogglePending(key) {
    const mode = getSchedulerToggleMode(key);
    return mode === "starting" || mode === "stopping";
  }

  function isSchedulerStartDisabled(key, actionKeyStart, actionKeyStop) {
    if (isActionLocked?.(actionKeyStart) || isActionLocked?.(actionKeyStop) || isSchedulerTogglePending(key)) {
      return true;
    }
    const action = getSchedulerAction(key, "start");
    if (action && typeof action === "object") {
      return !Boolean(action.allowed);
    }
    return true;
  }

  function isSchedulerStopDisabled(key, actionKeyStart, actionKeyStop) {
    if (isActionLocked?.(actionKeyStart) || isActionLocked?.(actionKeyStop) || isSchedulerTogglePending(key)) {
      return true;
    }
    const action = getSchedulerAction(key, "stop");
    if (action && typeof action === "object") {
      return !Boolean(action.allowed);
    }
    return true;
  }

  function getSchedulerStartButtonText(key) {
    const mode = getSchedulerToggleMode(key);
    if (mode === "starting") return "启动中...";
    if (mode === "stopping") return "处理中...";
    const action = getSchedulerAction(key, "start");
    const explicit = String(action?.label || "").trim();
    if (explicit) return explicit;
    return "等待状态";
  }

  function getSchedulerStopButtonText(key) {
    const mode = getSchedulerToggleMode(key);
    if (mode === "stopping") return "停止中...";
    if (mode === "starting") return "处理中...";
    const action = getSchedulerAction(key, "stop");
    const explicit = String(action?.label || "").trim();
    if (explicit) return explicit;
    return "等待状态";
  }

  function syncSchedulerDraftAutoStartFromRemembered(key, rememberedEnabled) {
    const remembered = Boolean(rememberedEnabled);
    const normalized = String(key || "").trim();
    if (normalized === "scheduler" && config?.value?.scheduler && typeof config.value.scheduler === "object") {
      config.value.scheduler.auto_start_in_gui = remembered;
      return;
    }
    if (
      normalized === "handover"
      && config?.value?.handover_log?.scheduler
      && typeof config.value.handover_log.scheduler === "object"
    ) {
      config.value.handover_log.scheduler.auto_start_in_gui = remembered;
      return;
    }
    if (
      normalized === "wet_bulb"
      && config?.value?.wet_bulb_collection?.scheduler
      && typeof config.value.wet_bulb_collection.scheduler === "object"
    ) {
      config.value.wet_bulb_collection.scheduler.auto_start_in_gui = remembered;
      return;
    }
    if (
      normalized === "day_metric_upload"
      && config?.value?.day_metric_upload?.scheduler
      && typeof config.value.day_metric_upload.scheduler === "object"
    ) {
      config.value.day_metric_upload.scheduler.auto_start_in_gui = remembered;
      return;
    }
    if (
      normalized === "alarm_event_upload"
      && config?.value?.alarm_export?.scheduler
      && typeof config.value.alarm_export.scheduler === "object"
    ) {
      config.value.alarm_export.scheduler.auto_start_in_gui = remembered;
      return;
    }
    if (
      normalized === "monthly_event_report"
      && config?.value?.handover_log?.monthly_event_report?.scheduler
      && typeof config.value.handover_log.monthly_event_report.scheduler === "object"
    ) {
      config.value.handover_log.monthly_event_report.scheduler.auto_start_in_gui = remembered;
      return;
    }
    if (
      normalized === "monthly_change_report"
      && config?.value?.handover_log?.monthly_change_report?.scheduler
      && typeof config.value.handover_log.monthly_change_report.scheduler === "object"
    ) {
      config.value.handover_log.monthly_change_report.scheduler.auto_start_in_gui = remembered;
    }
  }

  return {
    setSchedulerToggleState,
    syncSchedulerToggleStateWithHealth,
    getSchedulerToggleMode,
    getSchedulerSnapshotByKey,
    getSchedulerDisplayState,
    getSchedulerAction,
    getSchedulerStatusTone,
    getSchedulerStatusText,
    getSchedulerDisplayText,
    getSchedulerEffectiveRunning,
    getSchedulerEffectiveRemembered,
    isSchedulerStartDisabled,
    isSchedulerStopDisabled,
    getSchedulerStartButtonText,
    getSchedulerStopButtonText,
    isSchedulerTogglePending,
    syncSchedulerDraftAutoStartFromRemembered,
  };
}
