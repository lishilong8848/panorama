import {
  deleteResumeRunApi,
  getPendingResumeRunsApi,
  startJsonJobApi,
} from "./api_client.js";
import { isTransientNetworkError } from "./config_helpers.js";

const PENDING_RESUME_FETCH_COOLDOWN_MS = 5000;

function isBusyJob(job) {
  const status = String(job?.status || "").trim().toLowerCase();
  return status === "running" || status === "queued";
}

function normalizeRunId(raw) {
  const text = String(raw == null ? "" : raw).trim();
  if (!text) return "";
  const lowered = text.toLowerCase();
  if (lowered === "none" || lowered === "null" || text === "-") return "";
  return text;
}

function normalizeDateList(rawList) {
  const out = [];
  const seen = new Set();
  if (!Array.isArray(rawList)) return out;
  rawList.forEach((item) => {
    const text = String(item || "").trim();
    if (!text) return;
    if (seen.has(text)) return;
    seen.add(text);
    out.push(text);
  });
  out.sort();
  return out;
}

function isResumeConflictError(err) {
  if (Number.parseInt(String(err?.httpStatus || 0), 10) === 409) return true;
  const text = String(err?.message || err || "").trim().toLowerCase();
  return text.includes("409") || text.includes("conflict");
}

export function createRuntimeResumeActions(ctx) {
  const {
    health,
    bootstrapReady,
    config,
    message,
    currentJob,
    selectedJobId,
    selectedBridgeTaskId,
    bridgeTaskDetail,
    pendingResumeRuns,
    autoResumeState,
    fetchBridgeTasks,
    fetchBridgeTaskDetail,
    canRun,
    streamController,
    runSingleFlight,
    shouldPauseRuntimeRequests,
  } = ctx;
  let lastPendingResumeFetchAt = 0;

  function isRuntimeTrafficPaused() {
    return Boolean(
      (typeof shouldPauseRuntimeRequests === "function" && shouldPauseRuntimeRequests())
      || Boolean(shouldPauseRuntimeRequests?.value)
      || !Boolean(bootstrapReady?.value)
      || !Boolean(health?.runtime_activated)
      || !Boolean(health?.startup_role_confirmed)
    );
  }

  function getResumeRunId(run) {
    return normalizeRunId(run?.run_id);
  }

  function getResumeRunActionKey(runId = "") {
    const normalized = normalizeRunId(runId);
    return `resume:run:${normalized || "default"}`;
  }

  function getResumeDeleteActionKey(runId = "") {
    const normalized = normalizeRunId(runId);
    return `resume:delete:${normalized || "invalid"}`;
  }

  function formatResumeDateFull(run) {
    const dates = normalizeDateList(run?.selected_dates);
    if (!dates.length) return "-";
    return dates.join(", ");
  }

  function formatResumeDateSummary(run) {
    const dates = normalizeDateList(run?.selected_dates);
    if (!dates.length) return "-";
    if (dates.length === 1) return dates[0];
    return `${dates[0]} ~ ${dates[dates.length - 1]}（${dates.length}天）`;
  }

  async function fetchPendingResumeRuns(options = {}) {
    if (isRuntimeTrafficPaused()) return false;
    const force = Boolean(options?.force);
    const now = Date.now();
    if (!force && lastPendingResumeFetchAt > 0 && now - lastPendingResumeFetchAt < PENDING_RESUME_FETCH_COOLDOWN_MS) {
      return true;
    }
    lastPendingResumeFetchAt = now;
    const silentMessage = Boolean(options?.silentMessage);
    try {
      const data = await getPendingResumeRunsApi();
      const rows = Array.isArray(data?.runs) ? data.runs : [];
      pendingResumeRuns.value = rows;
      return true;
    } catch (err) {
      if (isTransientNetworkError(err)) return false;
      pendingResumeRuns.value = [];
      if (isResumeConflictError(err)) {
        return true;
      }
      if (!silentMessage) {
        message.value = `读取续传任务失败: ${err}`;
      }
      return false;
    }
  }

  async function runResumeUpload(runId = "", autoTrigger = false) {
    const effectiveRunId = normalizeRunId(runId);
    if (!canRun.value) return;
    const actionKey = getResumeRunActionKey(effectiveRunId);
    const runner = async () => {
      try {
        const body = { auto: Boolean(autoTrigger) };
        if (effectiveRunId) body.run_id = effectiveRunId;
        const response = await startJsonJobApi("/api/jobs/resume-upload", body);
        const wrappedJob =
          response
          && typeof response === "object"
          && response.accepted
          && response.job
          && typeof response.job === "object"
            ? response.job
            : response;
        const isBridgeJob = String(wrappedJob?.kind || "").trim().toLowerCase() === "bridge";
        const isWaitingSharedBridge = String(wrappedJob?.wait_reason || "").trim().toLowerCase() === "waiting:shared_bridge";
        const bridgeTaskId = String(response?.bridge_task?.task_id || "").trim();
        if (isBridgeJob) {
          currentJob.value = null;
          if (selectedJobId) {
            selectedJobId.value = "";
          }
          if (selectedBridgeTaskId) {
            selectedBridgeTaskId.value = bridgeTaskId;
          }
          if (bridgeTaskDetail) {
            bridgeTaskDetail.value =
              response?.bridge_task && typeof response.bridge_task === "object" ? { ...response.bridge_task } : null;
          }
          if (typeof fetchBridgeTasks === "function") {
            await fetchBridgeTasks({ silentMessage: true });
          }
          if (bridgeTaskId && typeof fetchBridgeTaskDetail === "function") {
            await fetchBridgeTaskDetail(bridgeTaskId, { silentMessage: true });
          }
          message.value = "断点续传已进入内外网同步处理";
        } else {
          currentJob.value = wrappedJob;
          if (selectedJobId) {
            selectedJobId.value = String(wrappedJob?.job_id || "").trim();
          }
          if (wrappedJob?.job_id) {
            streamController.attachJobStream(wrappedJob.job_id);
          }
          if (isWaitingSharedBridge) {
            message.value = "断点续传已进入等待内网补采同步，共享文件到位后会自动继续";
          } else {
            message.value = autoTrigger ? "已自动触发断点续传" : "已提交断点续传任务";
          }
        }
        autoResumeState.lastRunId = effectiveRunId;
        autoResumeState.lastTryTs = Date.now();
      } catch (err) {
        message.value = `续传任务提交失败: ${err}`;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(actionKey, runner, { cooldownMs: 0 });
    }
    return runner();
  }

  async function deleteResumeRun(runId) {
    const effectiveRunId = normalizeRunId(runId);
    if (!effectiveRunId) {
      message.value = "run_id 无效，无法删除";
      return;
    }
    const actionKey = getResumeDeleteActionKey(effectiveRunId);
    const runner = async () => {
      try {
        const data = await deleteResumeRunApi(effectiveRunId);
        await fetchPendingResumeRuns();
        if (data?.ok && data?.deleted) {
          message.value = data?.message || `已删除续传任务 ${effectiveRunId}`;
          return;
        }
        message.value = data?.message || `未找到续传任务 ${effectiveRunId}`;
      } catch (err) {
        message.value = `删除续传任务失败: ${err}`;
      }
    };
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(actionKey, runner, { cooldownMs: 500 });
    }
    return runner();
  }

  async function tryAutoResume() {
    const resumeCfg = config.value?.download?.resume || {};
    if (!resumeCfg.enabled || !resumeCfg.auto_continue_when_external) return;
    if (autoResumeState.inProgress) return;
    if (!pendingResumeRuns.value.length) return;

    const now = Date.now();
    const pollSec = Number.parseInt(String(resumeCfg.auto_continue_poll_sec || 5), 10);
    const cooldownMs = Math.max(1, Number.isFinite(pollSec) ? pollSec : 5) * 1000;
    if (now - autoResumeState.lastTryTs < cooldownMs) return;

    const target = pendingResumeRuns.value.find((item) => getResumeRunId(item));
    if (!target) return;

    autoResumeState.inProgress = true;
    autoResumeState.lastTryTs = now;
    try {
      await runResumeUpload(getResumeRunId(target), true);
    } finally {
      autoResumeState.inProgress = false;
    }
  }

  return {
    fetchPendingResumeRuns,
    runResumeUpload,
    deleteResumeRun,
    getResumeRunId,
    getResumeRunActionKey,
    getResumeDeleteActionKey,
    formatResumeDateSummary,
    formatResumeDateFull,
    tryAutoResume,
  };
}


