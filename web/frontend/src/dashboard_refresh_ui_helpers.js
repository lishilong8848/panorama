import { buildNeutralTargetDisplay, mapBackendTargetDisplay } from "./backend_target_display_helpers.js";
import { buildSharedBridgeSelfCheckOverview } from "./shared_bridge_diagnostics_vm.js";

export function createDashboardRefreshUiHelpers(options = {}) {
  const {
    computed,
    currentJob,
    health,
    sharedBridgeSelfCheckResult,
    fetchExternalDashboardSummary,
    fetchHealth,
    fetchBridgeTasks,
    fetchPendingResumeRuns,
    shouldPollExternalDashboardSummary,
    shouldFetchHealth,
    shouldPollBridgeTasks,
    shouldFetchPendingResumeRuns,
    timerState,
  } = options;

  const wetBulbConfiguredTarget = computed(() => {
    const backendDisplay = mapBackendTargetDisplay(health.wet_bulb_collection?.target_display);
    if (backendDisplay) return backendDisplay;
    return buildNeutralTargetDisplay();
  });

  const wetBulbLatestRunTarget = computed(() => {
    const result = currentJob.value?.result;
    const target = result?.target;
    return {
      configuredAppToken: String(target?.configured_app_token || "").trim(),
      operationAppToken: String(target?.operation_app_token || target?.app_token || "").trim(),
      tableId: String(target?.table_id || "").trim(),
      targetKind: String(target?.target_kind || "").trim(),
      url: String(target?.display_url || target?.bitable_url || "").trim(),
      message: String(target?.message || "").trim(),
      resolvedAt: String(target?.resolved_at || "").trim(),
    };
  });

  function scheduleExternalDashboardRefresh(reason = "unknown", options = {}) {
    const includePendingResume = Boolean(options?.includePendingResume);
    const force = Boolean(options?.force);
    const delayMs = Math.max(0, Number.parseInt(String(options?.delayMs ?? 220), 10) || 220);
    if (timerState?.timer) {
      window.clearTimeout(timerState.timer);
    }
    timerState.timer = window.setTimeout(async () => {
      timerState.timer = null;
      if (shouldPollExternalDashboardSummary()) {
        await fetchExternalDashboardSummary({ silentMessage: true, force });
      }
      if (shouldFetchHealth()) {
        await fetchHealth({ silentTransientNetworkError: true, silentMessage: true });
      }
      if (shouldPollBridgeTasks()) {
        await fetchBridgeTasks({ silentMessage: true });
      }
      if (includePendingResume && shouldFetchPendingResumeRuns()) {
        await fetchPendingResumeRuns({ silentMessage: true });
      }
    }, delayMs);
    return reason;
  }

  const sharedBridgeSelfCheckOverview = computed(() =>
    buildSharedBridgeSelfCheckOverview(
      sharedBridgeSelfCheckResult.value,
      health.shared_bridge || {},
      health.deployment || {},
    ),
  );

  return {
    wetBulbConfiguredTarget,
    wetBulbLatestRunTarget,
    scheduleExternalDashboardRefresh,
    sharedBridgeSelfCheckOverview,
  };
}
