export function buildSourceCachePlaceholderBuilding(building, bucketKey, sourceFamily = "") {
  const normalizedSourceFamily = String(sourceFamily || "").trim();
  return {
    building: String(building || "").trim() || "-",
    bucketKey: String(bucketKey || "").trim() || "-",
    sourceFamily: normalizedSourceFamily,
    source_family: normalizedSourceFamily,
    statusKey: "pending_backend",
    ready: false,
    downloadedAt: "",
    startedAt: "",
    lastError: "",
    blocked: false,
    blockedReason: "",
    nextProbeAt: "",
    relativePath: "",
    resolvedFilePath: "",
    tone: "neutral",
    stateText: "等待中",
    detailText: "等待共享文件就绪",
    metaLines: [],
    actions: {},
    usingFallback: false,
    versionGap: null,
    backfillRunning: false,
    backfillText: "",
    backfillScopeText: "",
    backfillTaskId: "",
  };
}

export function mapBackendActionState(action) {
  const payload = action && typeof action === "object" ? action : {};
  const targetKind = String(payload.target_kind || payload.targetKind || "").trim().toLowerCase();
  const targetId = String(payload.target_id || payload.targetId || "").trim();
  return {
    id: String(payload.id || "").trim(),
    allowed: payload.allowed !== false,
    pending: Boolean(payload.pending),
    label: String(payload.label || "").trim(),
    disabledReason: String(payload.disabled_reason || payload.disabledReason || "").trim(),
    reasonCode: String(payload.reason_code || payload.reasonCode || "").trim().toLowerCase(),
    targetKind,
    targetId,
    target_kind: targetKind,
    target_id: targetId,
  };
}

export function mapBackendActionsState(actions) {
  const payload = actions && typeof actions === "object" ? actions : {};
  return Object.fromEntries(
    Object.entries(payload)
      .filter(([key, action]) => String(key || "").trim() && action && typeof action === "object")
      .map(([key, action]) => [String(key || "").trim(), mapBackendActionState(action)]),
  );
}

export function mapBackendActionListById(actions) {
  const rows = Array.isArray(actions) ? actions : [];
  return Object.fromEntries(
    rows
      .filter((action) => action && typeof action === "object" && String(action.id || "").trim())
      .map((action) => {
        const mapped = mapBackendActionState(action);
        return [
          mapped.id,
          {
            ...mapped,
            desc: String(action.desc || "").trim(),
            visible: action.visible !== false,
          },
        ];
      }),
  );
}

export function normalizeBackendVisibleAction(raw, fallback = {}) {
  const payload = raw && typeof raw === "object" ? raw : {};
  const mapped = mapBackendActionState(payload);
  const hasAllowed = Object.prototype.hasOwnProperty.call(payload, "allowed");
  const hasPending = Object.prototype.hasOwnProperty.call(payload, "pending");
  const hasVisible = Object.prototype.hasOwnProperty.call(payload, "visible");
  return {
    ...mapped,
    allowed: hasAllowed ? mapped.allowed : fallback.allowed !== false,
    pending: hasPending ? mapped.pending : Boolean(fallback.pending),
    visible: hasVisible ? (payload.visible !== false) : (fallback.visible !== false),
    label: String(mapped.label || fallback.label || "").trim(),
    disabledReason: String(mapped.disabledReason || fallback.disabledReason || "").trim(),
    reasonCode: String(mapped.reasonCode || fallback.reasonCode || "").trim().toLowerCase(),
  };
}

export function normalizeBackendTaskItem(raw, fallbackKind = "job") {
  const payload = raw && typeof raw === "object" ? raw : {};
  const itemKind = String(payload.item_kind || payload.itemKind || fallbackKind || "").trim().toLowerCase() || fallbackKind;
  const waitingKind = String(payload.__waiting_kind || payload.waiting_kind || itemKind).trim().toLowerCase() || itemKind;
  const actions = mapBackendActionsState(payload.actions);
  const cancelAction = actions.cancel && typeof actions.cancel === "object" ? actions.cancel : null;
  const retryAction = actions.retry && typeof actions.retry === "object" ? actions.retry : null;
  const cancelTargetKind = String(cancelAction?.target_kind || cancelAction?.targetKind || "").trim().toLowerCase();
  const cancelTargetId = String(cancelAction?.target_id || cancelAction?.targetId || "").trim();
  const retryTargetKind = String(retryAction?.target_kind || retryAction?.targetKind || "").trim().toLowerCase();
  const retryTargetId = String(retryAction?.target_id || retryAction?.targetId || "").trim();
  const taskKind =
    ["bridge", "job"].includes(cancelTargetKind)
      ? cancelTargetKind
      : (["bridge", "job"].includes(waitingKind)
        ? waitingKind
        : (["bridge", "job"].includes(itemKind)
          ? itemKind
          : (String(payload.task_id || "").trim() ? "bridge" : "job")));
  return {
    ...payload,
    item_kind: itemKind,
    __waiting_kind: waitingKind,
    task_kind: taskKind,
    display_title: String(payload.display_title || payload.displayTitle || "").trim() || "-",
    display_meta: String(payload.display_meta || payload.displayMeta || "").trim() || "-",
    display_detail: String(payload.display_detail || payload.displayDetail || "").trim(),
    status_text: String(payload.status_text || payload.statusText || "").trim(),
    tone: String(payload.tone || "").trim() || "neutral",
    actions,
    cancel_action: cancelAction,
    retry_action: retryAction,
    cancel_target_kind: cancelTargetKind,
    cancel_target_id: cancelTargetId,
    retry_target_kind: retryTargetKind,
    retry_target_id: retryTargetId,
  };
}
