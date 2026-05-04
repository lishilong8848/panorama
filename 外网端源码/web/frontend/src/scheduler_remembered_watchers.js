const SCHEDULER_REMEMBERED_PATHS = [
  ["scheduler", () => "scheduler"],
  ["handover_scheduler", () => "handover"],
  ["wet_bulb_collection", () => "wet_bulb"],
  ["day_metric_upload", () => "day_metric_upload"],
  ["branch_power_upload", () => "branch_power_upload"],
  ["alarm_event_upload", () => "alarm_event_upload"],
  ["monthly_event_report", () => "monthly_event_report"],
  ["monthly_change_report", () => "monthly_change_report"],
];

function readRememberedEnabled(health, key) {
  const root = health?.[key];
  if (key === "scheduler" || key === "handover_scheduler") {
    return root?.remembered_enabled;
  }
  return root?.scheduler?.remembered_enabled;
}

export function registerSchedulerRememberedWatchers(options = {}) {
  const {
    watch,
    health,
    syncSchedulerToggleStateWithHealth,
    syncSchedulerDraftAutoStartFromRemembered,
  } = options;

  SCHEDULER_REMEMBERED_PATHS.forEach(([healthKey, resolveSchedulerKey]) => {
    const schedulerKey = resolveSchedulerKey();
    watch(
      () => readRememberedEnabled(health, healthKey),
      (value) => {
        syncSchedulerToggleStateWithHealth(schedulerKey, value);
        syncSchedulerDraftAutoStartFromRemembered(schedulerKey, value);
      },
      { immediate: true },
    );
  });
}
