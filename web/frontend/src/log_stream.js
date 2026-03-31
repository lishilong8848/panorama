export function createLogStreamController({
  appendLog,
  setMessage,
  getSystemOffset,
  setSystemOffset,
  onJobDone,
  onJobReconnect,
  systemReconnectDelayMs = 1200,
  jobReconnectDelayMs = 1200,
}) {
  let jobEs = null;
  let activeJobId = "";
  let systemEs = null;
  let systemReconnectTimer = null;
  let jobReconnectTimer = null;
  let paused = false;
  let systemLastEventId = Number.isInteger(getSystemOffset?.()) ? getSystemOffset() : 0;
  const jobLastEventIds = new Map();

  function closeJobStream() {
    if (jobReconnectTimer) {
      clearTimeout(jobReconnectTimer);
      jobReconnectTimer = null;
    }
    if (!jobEs) return;
    jobEs.close();
    jobEs = null;
  }

  function scheduleSystemReconnect() {
    if (paused || systemReconnectTimer) return;
    systemReconnectTimer = setTimeout(() => {
      systemReconnectTimer = null;
      attachSystemStream();
    }, systemReconnectDelayMs);
  }

  function scheduleJobReconnect(jobId) {
    if (paused || !jobId || jobReconnectTimer) return;
    jobReconnectTimer = setTimeout(async () => {
      jobReconnectTimer = null;
      attachJobStream(jobId);
      try {
        await onJobReconnect?.(jobId);
      } catch (_) {
        // ignore reconnect refresh errors
      }
    }, jobReconnectDelayMs);
  }

  function closeSystemStream() {
    if (systemReconnectTimer) {
      clearTimeout(systemReconnectTimer);
      systemReconnectTimer = null;
    }
    if (!systemEs) return;
    systemEs.close();
    systemEs = null;
  }

  function attachSystemStream() {
    if (paused) return;
    closeSystemStream();
    const offset = Number.isInteger(getSystemOffset?.()) ? getSystemOffset() : systemLastEventId;
    systemLastEventId = Math.max(0, Number.parseInt(String(offset || 0), 10) || 0);
    const es = new EventSource(`/api/logs/system?last_event_id=${Math.max(0, systemLastEventId)}`);
    const handleSystemEvent = (e) => {
      try {
        const payload = JSON.parse(e.data);
        appendLog(payload);
        const nextId = Number.parseInt(String(e.lastEventId || payload?.id || 0), 10);
        if (Number.isInteger(nextId) && nextId >= 0) {
          systemLastEventId = nextId;
          setSystemOffset?.(nextId);
        }
      } catch (_) {
        // keep stream alive
      }
    };
    es.onmessage = handleSystemEvent;
    es.addEventListener("log", handleSystemEvent);
    es.onerror = () => {
      closeSystemStream();
      scheduleSystemReconnect();
    };
    systemEs = es;
  }

  function attachJobStream(jobId) {
    const normalizedJobId = String(jobId || "").trim();
    if (!normalizedJobId) return;
    activeJobId = normalizedJobId;
    if (paused) return;
    closeJobStream();
    const lastEventId = Number.parseInt(String(jobLastEventIds.get(normalizedJobId) || 0), 10);
    const es = new EventSource(`/api/jobs/${normalizedJobId}/logs?last_event_id=${Math.max(0, lastEventId || 0)}`);
    const handleJobEvent = (e) => {
      const nextId = Number.parseInt(String(e.lastEventId || 0), 10);
      if (Number.isInteger(nextId) && nextId > 0) {
        jobLastEventIds.set(normalizedJobId, nextId);
      }
      try {
        JSON.parse(e.data);
      } catch (err) {
        setMessage?.(`日志解析失败: ${err}`);
      }
    };
    es.onmessage = handleJobEvent;
    ["log", "raw_stdout_log", "raw_stderr_log", "stage_status", "progress", "result", "heartbeat"].forEach((eventName) => {
      es.addEventListener(eventName, handleJobEvent);
    });
    es.addEventListener("done", async (e) => {
      const nextId = Number.parseInt(String(e.lastEventId || 0), 10);
      if (Number.isInteger(nextId) && nextId > 0) {
        jobLastEventIds.set(normalizedJobId, nextId);
      }
      try {
        await onJobDone?.(normalizedJobId);
      } finally {
        if (activeJobId === normalizedJobId) {
          closeJobStream();
        }
      }
    });
    es.onerror = () => {
      closeJobStream();
      if (activeJobId === normalizedJobId) {
        scheduleJobReconnect(normalizedJobId);
      }
    };
    jobEs = es;
  }

  function dispose() {
    closeJobStream();
    closeSystemStream();
  }

  function pauseAll() {
    paused = true;
    closeJobStream();
    closeSystemStream();
  }

  function resumeAll() {
    if (!paused) return;
    paused = false;
    attachSystemStream();
    if (activeJobId) {
      attachJobStream(activeJobId);
    }
  }

  return {
    attachJobStream,
    attachSystemStream,
    closeJobStream,
    closeSystemStream,
    pauseAll,
    resumeAll,
    dispose,
  };
}
