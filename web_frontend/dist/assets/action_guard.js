export function createActionGuard(vueApi) {
  const { reactive } = vueApi;

  const lockMap = reactive({});
  const actionLastDoneAt = reactive({});
  const inflightPromiseMap = {};
  const queuedTaskMap = {};

  function isActionLocked(key) {
    const name = String(key || "").trim();
    if (!name) return false;
    return Boolean(lockMap[name]);
  }

  async function _runUnlocked(name, taskFn, options = {}) {
    const cooldownRaw = Number.parseInt(String(options.cooldownMs ?? 0), 10);
    const cooldownMs = Number.isFinite(cooldownRaw) && cooldownRaw > 0 ? cooldownRaw : 0;
    const onCooldown = typeof options.onCooldown === "function" ? options.onCooldown : null;
    const now = Date.now();
    const lastDone = Number.parseInt(String(actionLastDoneAt[name] || 0), 10) || 0;
    if (cooldownMs > 0 && now - lastDone < cooldownMs) {
      if (onCooldown) {
        try {
          onCooldown();
        } catch (_) {
          // ignore cooldown callback errors
        }
      }
      return false;
    }

    lockMap[name] = true;
    const runningPromise = (async () => taskFn())();
    inflightPromiseMap[name] = runningPromise;
    try {
      return await runningPromise;
    } finally {
      lockMap[name] = false;
      delete inflightPromiseMap[name];
      actionLastDoneAt[name] = Date.now();
      const queued = queuedTaskMap[name];
      if (queued && typeof queued.taskFn === "function") {
        delete queuedTaskMap[name];
        _runUnlocked(name, queued.taskFn, { ...queued.options, cooldownMs: 0, queueLatest: false }).then(
          queued.resolve,
          queued.reject,
        );
      }
    }
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

    if (lockMap[name]) {
      if (options.queueLatest) {
        const existingQueued = queuedTaskMap[name];
        if (existingQueued) {
          existingQueued.taskFn = taskFn;
          existingQueued.options = { ...options };
          return existingQueued.promise;
        }
        let resolveNext;
        let rejectNext;
        const queuedPromise = new Promise((resolve, reject) => {
          resolveNext = resolve;
          rejectNext = reject;
        });
        queuedTaskMap[name] = {
          taskFn,
          options: { ...options },
          resolve: resolveNext,
          reject: rejectNext,
          promise: queuedPromise,
        };
        return queuedPromise;
      }
      return inflightPromiseMap[name];
    }

    return _runUnlocked(name, taskFn, options);
  }

  return {
    lockMap,
    actionLastDoneAt,
    inflightPromiseMap,
    isActionLocked,
    runSingleFlight,
  };
}
