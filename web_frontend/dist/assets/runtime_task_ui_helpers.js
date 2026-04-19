export function createRuntimeTaskUiHelpers(options = {}) {
  const {
    currentJob,
    jobsList,
    selectedJobId,
    bridgeTasks,
    selectedBridgeTaskId,
    bridgeTaskDetail,
    message,
    streamController,
    isActionLocked,
    getBridgeTaskCancelActionKey,
    getBridgeTaskRetryActionKey,
    getJobCancelActionKey,
    getJobRetryActionKey,
    cancelBridgeTask,
    retryBridgeTask,
    cancelCurrentJob,
    retryCurrentJob,
    fetchJob,
    fetchBridgeTaskDetail,
  } = options || {};

  function isWaitingResourceItemSelected(item) {
    const kind = String(item?.__waiting_kind || "job").trim().toLowerCase();
    if (kind === "bridge") {
      return String(selectedBridgeTaskId?.value || "").trim() === String(item?.task_id || "").trim();
    }
    return String(selectedJobId?.value || "").trim() === String(item?.job_id || "").trim();
  }

  async function focusWaitingResourceItem(item) {
    const kind = String(item?.__waiting_kind || "job").trim().toLowerCase();
    if (kind === "bridge") {
      await focusBridgeTask(item);
      return;
    }
    await focusJob(item);
  }

  async function focusJobInRuntimeLogs(jobLike) {
    await focusJob(jobLike);
  }

  async function focusBridgeTaskInRuntimeLogs(taskLike) {
    await focusBridgeTask(taskLike);
  }

  async function focusWaitingResourceItemInRuntimeLogs(item) {
    const kind = String(item?.__waiting_kind || "job").trim().toLowerCase();
    if (kind === "bridge") {
      await focusBridgeTaskInRuntimeLogs(item);
      return;
    }
    await focusJobInRuntimeLogs(item);
  }

  function getRuntimeTaskAction(item, actionName = "cancel") {
    const actions = item?.actions;
    if (
      actions
      && typeof actions === "object"
      && actions[actionName]
      && typeof actions[actionName] === "object"
    ) {
      return actions[actionName];
    }
    if (actionName === "cancel") {
      return item?.cancel_action && typeof item.cancel_action === "object" ? item.cancel_action : null;
    }
    if (actionName === "retry") {
      return item?.retry_action && typeof item.retry_action === "object" ? item.retry_action : null;
    }
    return null;
  }

  function getRuntimeTaskActionTarget(item, actionName = "cancel") {
    const action = getRuntimeTaskAction(item, actionName);
    const inferredKind = getRuntimeTaskKind(item);
    const fallbackTargetId = String(
      inferredKind === "bridge"
        ? (item?.task_id || item?.target_id || "")
        : (item?.job_id || item?.target_id || ""),
    ).trim();
    const targetKind = String(
      actionName === "cancel"
        ? (item?.cancel_target_kind || action?.target_kind || action?.targetKind || inferredKind || "")
        : (item?.retry_target_kind || action?.target_kind || action?.targetKind || inferredKind || ""),
    ).trim().toLowerCase();
    const targetId = String(
      actionName === "cancel"
        ? (item?.cancel_target_id || action?.target_id || action?.targetId || fallbackTargetId || "")
        : (item?.retry_target_id || action?.target_id || action?.targetId || fallbackTargetId || ""),
    ).trim();
    if (!["bridge", "job"].includes(targetKind) || !targetId) return null;
    return { targetKind, targetId, action };
  }

  function isRuntimeTaskActionVisible(item, actionName = "cancel") {
    const action = getRuntimeTaskAction(item, actionName);
    if (!action) return false;
    if (action.visible === false) return false;
    return Boolean(action.allowed || action.pending);
  }

  function isRuntimeTaskActionPending(item, actionName = "cancel") {
    return Boolean(getRuntimeTaskAction(item, actionName)?.pending);
  }

  function getRuntimeTaskActionLabel(item, actionName = "cancel", fallback = "") {
    const action = getRuntimeTaskAction(item, actionName);
    return String(action?.label || "").trim() || String(fallback || "等待后端动作").trim();
  }

  function getRuntimeTaskActionDisabledReason(item, actionName = "cancel") {
    const action = getRuntimeTaskAction(item, actionName);
    if (action?.pending) {
      return String(action?.disabled_reason || action?.disabledReason || "").trim() || "请求处理中，请稍候";
    }
    return String(action?.disabled_reason || action?.disabledReason || "").trim();
  }

  function getRuntimeTaskKind(item) {
    const taskKind = String(item?.task_kind || "").trim().toLowerCase();
    if (taskKind === "bridge" || taskKind === "job") return taskKind;
    return String(item?.task_id || "").trim() ? "bridge" : "job";
  }

  function getRuntimeTaskTitle(item) {
    return String(item?.display_title || "").trim() || "-";
  }

  function getRuntimeTaskMeta(item) {
    return String(item?.display_meta || "").trim() || "-";
  }

  function getRuntimeTaskDetail(item) {
    return String(item?.display_detail || "").trim();
  }

  function getJobRetryAction(job) {
    return job?.retry_action && typeof job.retry_action === "object" ? job.retry_action : null;
  }

  function getRuntimeTaskActionKey(item, actionName = "cancel") {
    const actionTarget = getRuntimeTaskActionTarget(item, actionName);
    if (actionTarget?.targetKind === "bridge") {
      return actionName === "retry"
        ? getBridgeTaskRetryActionKey?.(actionTarget.targetId)
        : getBridgeTaskCancelActionKey?.(actionTarget.targetId);
    }
    if (actionTarget?.targetKind === "job") {
      return actionName === "retry"
        ? getJobRetryActionKey?.(actionTarget.targetId)
        : getJobCancelActionKey?.(actionTarget.targetId);
    }
    const fallbackId = String(item?.__waiting_id || item?.task_id || item?.job_id || "unknown").trim() || "unknown";
    return `runtime-task:${actionName}:pending-backend:${fallbackId}`;
  }

  function getRuntimeTaskCancelActionKey(item) {
    return getRuntimeTaskActionKey(item, "cancel");
  }

  function isRuntimeTaskActionLocked(item, actionName = "cancel") {
    return Boolean(isActionLocked?.(getRuntimeTaskActionKey(item, actionName)) || isRuntimeTaskActionPending(item, actionName));
  }

  async function cancelRuntimeTask(item) {
    const cancelAction = getRuntimeTaskAction(item, "cancel");
    if (cancelAction?.pending) {
      if (message) message.value = getRuntimeTaskActionDisabledReason(item, "cancel") || "请求处理中，请稍候";
      return;
    }
    if (cancelAction && cancelAction.allowed === false) {
      if (message) message.value = getRuntimeTaskActionDisabledReason(item, "cancel") || "";
      return;
    }
    const actionTarget = getRuntimeTaskActionTarget(item, "cancel");
    if (actionTarget?.targetKind === "bridge") {
      await cancelBridgeTask?.(actionTarget.targetId);
      return;
    }
    if (actionTarget?.targetKind === "job") {
      await cancelCurrentJob?.(actionTarget.targetId);
      return;
    }
    if (message) message.value = getRuntimeTaskActionDisabledReason(item, "cancel") || "";
  }

  async function cancelJobItem(jobLike) {
    const job =
      jobLike && typeof jobLike === "object"
        ? jobLike
        : jobsList?.value?.find((item) => String(item?.job_id || "").trim() === String(jobLike || "").trim());
    const cancelAction = getRuntimeTaskAction(job, "cancel");
    if (cancelAction?.pending) {
      if (message) message.value = getRuntimeTaskActionDisabledReason(job, "cancel") || "请求处理中，请稍候";
      return;
    }
    if (cancelAction && cancelAction.allowed === false) {
      if (message) message.value = getRuntimeTaskActionDisabledReason(job, "cancel") || "";
      return;
    }
    const cancelTarget = getRuntimeTaskActionTarget(job, "cancel");
    if (cancelTarget?.targetKind !== "job" || !String(cancelTarget?.targetId || "").trim()) {
      if (message) message.value = getRuntimeTaskActionDisabledReason(job, "cancel") || "";
      return;
    }
    await cancelCurrentJob?.(String(cancelTarget.targetId || "").trim());
  }

  async function retryJobItem(jobLike) {
    const job =
      jobLike && typeof jobLike === "object"
        ? jobLike
        : jobsList?.value?.find((item) => String(item?.job_id || "").trim() === String(jobLike || "").trim());
    const retryAction = getJobRetryAction(job);
    if (retryAction?.pending) {
      if (message) message.value = String(retryAction?.disabled_reason || retryAction?.disabledReason || "").trim() || "请求处理中，请稍候";
      return;
    }
    if (retryAction && retryAction.allowed === false) {
      if (message) message.value = String(retryAction?.disabled_reason || retryAction?.disabledReason || "").trim() || "";
      return;
    }
    const retryTarget = getRuntimeTaskActionTarget(job, "retry");
    const retryKind = String(retryTarget?.targetKind || "").trim().toLowerCase();
    const targetId = String(retryTarget?.targetId || "").trim();
    if (!retryKind || !targetId) {
      if (message) message.value = String(retryAction?.disabled_reason || retryAction?.disabledReason || "").trim() || "";
      return;
    }
    if (retryKind === "bridge") {
      await retryBridgeTask?.(targetId);
      return;
    }
    if (retryKind !== "job") {
      if (message) message.value = String(retryAction?.disabled_reason || retryAction?.disabledReason || "").trim() || "";
      return;
    }
    if (currentJob) currentJob.value = { ...(currentJob.value || {}), ...job };
    if (selectedJobId) selectedJobId.value = targetId;
    await retryCurrentJob?.();
  }

  async function focusJob(jobLike) {
    const job =
      jobLike && typeof jobLike === "object"
        ? jobLike
        : jobsList?.value?.find((item) => String(item?.job_id || "").trim() === String(jobLike || "").trim());
    const jobId = String(job?.job_id || "").trim();
    if (!jobId) return;
    if (selectedJobId) selectedJobId.value = jobId;
    if (currentJob) currentJob.value = { ...(currentJob.value || {}), ...job };
    streamController?.attachJobStream?.(jobId);
    await fetchJob?.(jobId);
  }

  async function focusBridgeTask(taskLike) {
    const task =
      taskLike && typeof taskLike === "object"
        ? taskLike
        : bridgeTasks?.value?.find((item) => String(item?.task_id || "").trim() === String(taskLike || "").trim());
    const taskId = String(task?.task_id || "").trim();
    if (!taskId) return;
    if (selectedBridgeTaskId) selectedBridgeTaskId.value = taskId;
    if (bridgeTaskDetail) {
      if (bridgeTaskDetail.value && String(bridgeTaskDetail.value?.task_id || "").trim() === taskId) {
        bridgeTaskDetail.value = { ...bridgeTaskDetail.value, ...task };
      } else {
        bridgeTaskDetail.value = task;
      }
    }
    await fetchBridgeTaskDetail?.(taskId, { silentMessage: true });
  }

  return {
    isWaitingResourceItemSelected,
    focusWaitingResourceItem,
    focusJobInRuntimeLogs,
    focusBridgeTaskInRuntimeLogs,
    focusWaitingResourceItemInRuntimeLogs,
    getRuntimeTaskAction,
    getRuntimeTaskActionTarget,
    isRuntimeTaskActionVisible,
    isRuntimeTaskActionPending,
    getRuntimeTaskActionLabel,
    getRuntimeTaskActionDisabledReason,
    getRuntimeTaskKind,
    getRuntimeTaskTitle,
    getRuntimeTaskMeta,
    getRuntimeTaskDetail,
    getJobRetryAction,
    getRuntimeTaskActionKey,
    getRuntimeTaskCancelActionKey,
    isRuntimeTaskActionLocked,
    cancelRuntimeTask,
    cancelJobItem,
    retryJobItem,
    focusJob,
    focusBridgeTask,
  };
}
