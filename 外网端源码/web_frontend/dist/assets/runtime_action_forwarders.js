function resolveRuntimeAction(runtimeActionsRef, actionName, fallback = null) {
  const fn = runtimeActionsRef?.current?.[actionName];
  if (typeof fn === "function") {
    return fn;
  }
  if (typeof fallback === "function") {
    return fallback;
  }
  return null;
}

export function createRuntimeActionForwarders(options = {}) {
  const {
    configLoaded,
    config,
    health,
  } = options;

  const runtimeActionsRef = { current: null };

  function startupRoleDraftSourceConfig() {
    if (configLoaded?.value && config?.value && typeof config.value === "object") {
      return config.value;
    }
    return {
      deployment: health?.deployment || {},
      shared_bridge: health?.startup_shared_bridge || {},
    };
  }

  function uploadAlarmSourceCacheFull(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "uploadAlarmSourceCacheFull", async () => null);
    return action(...args);
  }

  function uploadAlarmSourceCacheBuilding(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "uploadAlarmSourceCacheBuilding", async () => null);
    return action(...args);
  }

  function checkUpdaterNow(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "checkUpdaterNow", async () => null);
    return action(...args);
  }

  function applyUpdaterPatch(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "applyUpdaterPatch", async () => null);
    return action(...args);
  }

  function restartUpdaterApp(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "restartUpdaterApp", async () => null);
    return action(...args);
  }

  function refreshCurrentHourSourceCache(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "refreshCurrentHourSourceCache", async () => null);
    return action(...args);
  }

  function refreshManualAlarmSourceCache(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "refreshManualAlarmSourceCache", async () => null);
    return action(...args);
  }

  function getJobCancelActionKey(jobId = "") {
    const action = resolveRuntimeAction(
      runtimeActionsRef,
      "getJobCancelActionKey",
      (value = "") => `job:cancel:${String(value || "").trim() || "unknown"}`,
    );
    return action(jobId);
  }

  function getJobRetryActionKey(jobId = "") {
    const action = resolveRuntimeAction(
      runtimeActionsRef,
      "getJobRetryActionKey",
      (value = "") => `job:retry:${String(value || "").trim() || "unknown"}`,
    );
    return action(jobId);
  }

  function cancelCurrentJob(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "cancelCurrentJob", async () => null);
    return action(...args);
  }

  function retryCurrentJob(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "retryCurrentJob", async () => null);
    return action(...args);
  }

  function fetchJob(...args) {
    const action = resolveRuntimeAction(runtimeActionsRef, "fetchJob", async () => null);
    return action(...args);
  }

  return {
    runtimeActionsRef,
    startupRoleDraftSourceConfig,
    uploadAlarmSourceCacheFull,
    uploadAlarmSourceCacheBuilding,
    checkUpdaterNow,
    applyUpdaterPatch,
    restartUpdaterApp,
    refreshCurrentHourSourceCache,
    refreshManualAlarmSourceCache,
    getJobCancelActionKey,
    getJobRetryActionKey,
    cancelCurrentJob,
    retryCurrentJob,
    fetchJob,
  };
}
