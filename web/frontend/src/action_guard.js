export function createActionGuard(vueApi) {
  const { reactive } = vueApi;

  const lockMap = reactive({});
  const actionLastDoneAt = reactive({});
  const inflightPromiseMap = {};

  function isActionLocked(key) {
    const name = String(key || "").trim();
    if (!name) return false;
    return Boolean(lockMap[name]);
  }

  async function runSingleFlight(key, taskFn, options = {}) {
    const name = String(key || "").trim();
    if (!name) {
      if (typeof taskFn === "function") {
        return taskFn();
      }
      return undefined;
    }
    if (typeof taskFn !== "function") return undefined;

    const cooldownRaw = Number.parseInt(String(options.cooldownMs ?? 0), 10);
    const cooldownMs = Number.isFinite(cooldownRaw) && cooldownRaw > 0 ? cooldownRaw : 0;
    const now = Date.now();
    const lastDone = Number.parseInt(String(actionLastDoneAt[name] || 0), 10) || 0;
    if (lockMap[name]) return inflightPromiseMap[name];
    if (cooldownMs > 0 && now - lastDone < cooldownMs) return undefined;

    lockMap[name] = true;
    const runningPromise = (async () => taskFn())();
    inflightPromiseMap[name] = runningPromise;
    try {
      return await runningPromise;
    } finally {
      lockMap[name] = false;
      delete inflightPromiseMap[name];
      actionLastDoneAt[name] = Date.now();
    }
  }

  return {
    lockMap,
    actionLastDoneAt,
    inflightPromiseMap,
    isActionLocked,
    runSingleFlight,
  };
}
