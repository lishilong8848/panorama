const HANDOVER_DUTY_CONTEXT_STORAGE_KEY = "handover_duty_context";
const APP_BOOT_OVERLAY_ID = "app-boot-overlay";
const STARTUP_ROLE_RESTART_PENDING_KEY = "startup_role_restart_pending_v1";
const STARTUP_ROLE_RESTART_RESUME_KEY = "startup_role_restart_resume_v1";
const STARTUP_ROLE_SESSION_KEY = "startup_role_session_v1";
const STARTUP_RUNTIME_RECOVERY_KEY = "startup_runtime_recovery_v1";
const UPDATER_RECOVERY_INTENT_KEY = "updater_recovery_intent_v1";
const STARTUP_ROLE_RESTART_PENDING_TTL_MS = 5 * 60 * 1000;

export function normalizeBrowserPathname(pathname) {
  const raw = String(pathname || "").trim() || "/";
  if (raw === "/") return "/";
  return raw.endsWith("/") ? raw.slice(0, -1) : raw;
}

export function parseAppBrowserRoute(pathname) {
  const normalizedPath = normalizeBrowserPathname(pathname).toLowerCase();
  if (normalizedPath === "/" || normalizedPath === "/index.html" || normalizedPath === "/login") {
    return { kind: "login", role_mode: "", view: "" };
  }
  if (normalizedPath === "/internal" || normalizedPath === "/internal/status") {
    return { kind: "app", role_mode: "internal", view: "status" };
  }
  if (normalizedPath === "/internal/config") {
    return { kind: "app", role_mode: "internal", view: "config" };
  }
  if (normalizedPath === "/external" || normalizedPath === "/external/dashboard") {
    return { kind: "app", role_mode: "external", view: "dashboard" };
  }
  if (normalizedPath === "/external/status") {
    return { kind: "app", role_mode: "external", view: "status" };
  }
  if (normalizedPath === "/external/config") {
    return { kind: "app", role_mode: "external", view: "config" };
  }
  if (normalizedPath === "/status") {
    return { kind: "app", role_mode: "", view: "status" };
  }
  if (normalizedPath === "/dashboard") {
    return { kind: "app", role_mode: "", view: "dashboard" };
  }
  if (normalizedPath === "/config") {
    return { kind: "app", role_mode: "", view: "config" };
  }
  return { kind: "unknown", role_mode: "", view: "" };
}

export function normalizeDeploymentRoleMode(value) {
  const text = String(value || "").trim().toLowerCase();
  if (["internal", "external"].includes(text)) return text;
  return "";
}

export function buildAppBrowserRoutePath(roleMode, view, selectorVisible = false) {
  if (selectorVisible || !normalizeDeploymentRoleMode(roleMode)) {
    return "/";
  }
  const normalizedRole = normalizeDeploymentRoleMode(roleMode);
  const normalizedView = String(view || "").trim().toLowerCase();
  if (normalizedRole === "internal") {
    if (normalizedView === "config") return "/internal/config";
    return "/internal/status";
  }
  if (normalizedView === "config") return "/external/config";
  if (normalizedView === "status") return "/external/status";
  return "/external/dashboard";
}

export function formatDeploymentRoleLabel(value) {
  const role = normalizeDeploymentRoleMode(value);
  if (role === "internal") return "采集端";
  if (role === "external") return "外网端";
  return "待选择角色";
}

export function formatDateTimeFromEpoch(value) {
  const timestamp = Number.parseInt(String(value || 0), 10);
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "";
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return "";
  const pad = (num) => String(num).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}

export function normalizeReceiveIdsText(value) {
  const sourceItems = Array.isArray(value) ? value : [value];
  const values = [];
  for (const item of sourceItems) {
    const text = String(item || "").trim();
    if (!text) continue;
    const parts = text.split(/[\s,，;；\r\n]+/).map((segment) => String(segment || "").trim()).filter(Boolean);
    values.push(...parts);
  }
  return values.filter((item, index, list) => list.indexOf(item) === index);
}

export const STARTUP_BRIDGE_DEFAULTS = Object.freeze({
  root_dir: "",
  poll_interval_sec: 2,
  heartbeat_interval_sec: 5,
  claim_lease_sec: 30,
  stale_task_timeout_sec: 1800,
  artifact_retention_days: 7,
  sqlite_busy_timeout_ms: 15000,
});

export function resolveSharedBridgeRoleRoot(config, roleMode) {
  const role = normalizeDeploymentRoleMode(roleMode);
  const sharedBridge = config && typeof config.shared_bridge === "object" ? config.shared_bridge : {};
  const legacyRoot = String(sharedBridge.root_dir || "").trim();
  if (role === "internal") {
    return String(sharedBridge.internal_root_dir || legacyRoot || "").trim();
  }
  if (role === "external") {
    return String(sharedBridge.external_root_dir || legacyRoot || "").trim();
  }
  return legacyRoot;
}

export function normalizePositiveInteger(value, fallback) {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

export function buildStartupBridgeDraft(config, roleMode = "") {
  const deployment = config && typeof config.deployment === "object" ? config.deployment : {};
  const sharedBridge = config && typeof config.shared_bridge === "object" ? config.shared_bridge : {};
  const effectiveRole = normalizeDeploymentRoleMode(roleMode || deployment.role_mode || "");
  return {
    root_dir: resolveSharedBridgeRoleRoot(config, effectiveRole),
    poll_interval_sec: normalizePositiveInteger(
      sharedBridge.poll_interval_sec,
      STARTUP_BRIDGE_DEFAULTS.poll_interval_sec,
    ),
    heartbeat_interval_sec: normalizePositiveInteger(
      sharedBridge.heartbeat_interval_sec,
      STARTUP_BRIDGE_DEFAULTS.heartbeat_interval_sec,
    ),
    claim_lease_sec: normalizePositiveInteger(
      sharedBridge.claim_lease_sec,
      STARTUP_BRIDGE_DEFAULTS.claim_lease_sec,
    ),
    stale_task_timeout_sec: normalizePositiveInteger(
      sharedBridge.stale_task_timeout_sec,
      STARTUP_BRIDGE_DEFAULTS.stale_task_timeout_sec,
    ),
    artifact_retention_days: normalizePositiveInteger(
      sharedBridge.artifact_retention_days,
      STARTUP_BRIDGE_DEFAULTS.artifact_retention_days,
    ),
    sqlite_busy_timeout_ms: normalizePositiveInteger(
      sharedBridge.sqlite_busy_timeout_ms,
      STARTUP_BRIDGE_DEFAULTS.sqlite_busy_timeout_ms,
    ),
  };
}

export function validateStartupBridgeDraft(roleMode, draft) {
  const role = normalizeDeploymentRoleMode(roleMode);
  if (!["internal", "external"].includes(role)) return "";
  if (!String(draft?.root_dir || "").trim()) {
    return "请先填写共享目录后再切换。";
  }
  const numericRules = [
    ["poll_interval_sec", "轮询间隔", 1],
    ["heartbeat_interval_sec", "心跳间隔", 1],
    ["claim_lease_sec", "阶段租约", 5],
    ["stale_task_timeout_sec", "任务超时", 60],
    ["artifact_retention_days", "产物保留天数", 1],
    ["sqlite_busy_timeout_ms", "SQLite 忙等待", 1000],
  ];
  for (const [field, label, minValue] of numericRules) {
    const value = Number.parseInt(String(draft?.[field] ?? "").trim(), 10);
    if (!Number.isFinite(value) || value < minValue) {
      return `${label}必须大于等于 ${minValue}。`;
    }
  }
  return "";
}

export function isStartupBridgeDraftChanged(config, draft, roleMode = "") {
  const current = buildStartupBridgeDraft(config, roleMode);
  return (
    current.root_dir !== String(draft?.root_dir || "").trim()
    || current.poll_interval_sec !== normalizePositiveInteger(draft?.poll_interval_sec, current.poll_interval_sec)
    || current.heartbeat_interval_sec !== normalizePositiveInteger(draft?.heartbeat_interval_sec, current.heartbeat_interval_sec)
    || current.claim_lease_sec !== normalizePositiveInteger(draft?.claim_lease_sec, current.claim_lease_sec)
    || current.stale_task_timeout_sec !== normalizePositiveInteger(draft?.stale_task_timeout_sec, current.stale_task_timeout_sec)
    || current.artifact_retention_days !== normalizePositiveInteger(draft?.artifact_retention_days, current.artifact_retention_days)
    || current.sqlite_busy_timeout_ms !== normalizePositiveInteger(draft?.sqlite_busy_timeout_ms, current.sqlite_busy_timeout_ms)
  );
}

export function buildRoleNodeIdPreview(currentNodeId, currentRole, targetRole) {
  const runtimeNodeId = String(currentNodeId || "").trim();
  const normalizedCurrentRole = normalizeDeploymentRoleMode(currentRole);
  const normalizedTargetRole = normalizeDeploymentRoleMode(targetRole);
  if (!runtimeNodeId) {
    return "切换后自动生成并长期固定";
  }
  if (normalizedCurrentRole === normalizedTargetRole) {
    return runtimeNodeId;
  }
  const autoPrefix = `${normalizedCurrentRole}-`;
  if (runtimeNodeId.startsWith(autoPrefix) && runtimeNodeId.length > autoPrefix.length) {
    return `${normalizedTargetRole}-${runtimeNodeId.slice(autoPrefix.length)}`;
  }
  return "切换后自动生成并长期固定";
}

function dismissBootOverlay() {
  if (typeof window === "undefined" || typeof document === "undefined") return;
  document.body?.classList.remove("app-boot-pending");
  const overlay = document.getElementById(APP_BOOT_OVERLAY_ID);
  if (!overlay) return;
  overlay.classList.add("is-hidden");
  window.setTimeout(() => {
    if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
  }, 260);
}

export function finishAppBoot() {
  if (typeof window === "undefined") return;
  const run = () => {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(dismissBootOverlay);
    });
  };
  if (document.readyState === "complete") {
    run();
    return;
  }
  window.addEventListener("load", run, { once: true });
}

export function persistHandoverDutyContext(dutyDate, dutyShift) {
  if (typeof window === "undefined" || !window.localStorage) return;
  const nextDutyDate = String(dutyDate || "").trim();
  const nextDutyShift = String(dutyShift || "").trim().toLowerCase();
  if (!nextDutyDate || !["day", "night"].includes(nextDutyShift)) return;
  try {
    window.localStorage.setItem(
      HANDOVER_DUTY_CONTEXT_STORAGE_KEY,
      JSON.stringify({
        duty_date: nextDutyDate,
        duty_shift: nextDutyShift,
        updated_at: Date.now(),
      }),
    );
  } catch (_) {
    // ignore localStorage errors
  }
}

export function readStartupRoleRestartPending() {
  if (typeof window === "undefined" || !window.sessionStorage) return null;
  try {
    const raw = window.sessionStorage.getItem(STARTUP_ROLE_RESTART_PENDING_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const role = normalizeDeploymentRoleMode(parsed?.role_mode);
    const requestedAt = Number.parseInt(String(parsed?.requested_at || 0), 10);
    if (!role || !Number.isFinite(requestedAt) || requestedAt <= 0) {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_PENDING_KEY);
      return null;
    }
    if (Date.now() - requestedAt > STARTUP_ROLE_RESTART_PENDING_TTL_MS) {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_PENDING_KEY);
      return null;
    }
    return {
      role_mode: role,
      requested_at: requestedAt,
      source_startup_token: String(parsed?.source_startup_token || "").trim(),
    };
  } catch (_) {
    try {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_PENDING_KEY);
    } catch (_) {
      // ignore sessionStorage errors
    }
    return null;
  }
}

export function writeStartupRoleRestartPending(roleMode, sourceStartupToken = "") {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  const role = normalizeDeploymentRoleMode(roleMode);
  if (!role) return;
  try {
    window.sessionStorage.setItem(
      STARTUP_ROLE_RESTART_PENDING_KEY,
      JSON.stringify({
        role_mode: role,
        requested_at: Date.now(),
        source_startup_token: String(sourceStartupToken || "").trim(),
      }),
    );
  } catch (_) {
    // ignore sessionStorage errors
  }
}

export function readStartupRoleRestartResume() {
  if (typeof window === "undefined" || !window.sessionStorage) return null;
  try {
    const raw = window.sessionStorage.getItem(STARTUP_ROLE_RESTART_RESUME_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const role = normalizeDeploymentRoleMode(parsed?.role_mode);
    const requestedAt = Number.parseInt(String(parsed?.requested_at || 0), 10);
    const sourceStartupToken = String(parsed?.source_startup_token || "").trim();
    if (!role || !Number.isFinite(requestedAt) || requestedAt <= 0 || !sourceStartupToken) {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_RESUME_KEY);
      return null;
    }
    if (Date.now() - requestedAt > STARTUP_ROLE_RESTART_PENDING_TTL_MS) {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_RESUME_KEY);
      return null;
    }
    return {
      role_mode: role,
      requested_at: requestedAt,
      source_startup_token: sourceStartupToken,
    };
  } catch (_) {
    try {
      window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_RESUME_KEY);
    } catch (_) {
      // ignore sessionStorage errors
    }
    return null;
  }
}

export function clearStartupRoleRestartPending() {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  try {
    window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_PENDING_KEY);
  } catch (_) {
    // ignore sessionStorage errors
  }
}

export function clearStartupRoleRestartResume() {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  try {
    window.sessionStorage.removeItem(STARTUP_ROLE_RESTART_RESUME_KEY);
  } catch (_) {
    // ignore sessionStorage errors
  }
}

export function readStartupRoleSession() {
  if (typeof window === "undefined" || !window.localStorage) return null;
  try {
    const raw = window.localStorage.getItem(STARTUP_ROLE_SESSION_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const role = normalizeDeploymentRoleMode(parsed?.role_mode);
    const startupToken = String(parsed?.startup_token || "").trim();
    const nodeId = String(parsed?.node_id || "").trim();
    if (!role || !startupToken) {
      window.localStorage.removeItem(STARTUP_ROLE_SESSION_KEY);
      return null;
    }
    return {
      role_mode: role,
      startup_token: startupToken,
      node_id: nodeId,
    };
  } catch (_) {
    try {
      window.localStorage.removeItem(STARTUP_ROLE_SESSION_KEY);
    } catch (_) {
      // ignore localStorage errors
    }
    return null;
  }
}

export function writeStartupRoleSession(roleMode, startupToken = "", nodeId = "") {
  if (typeof window === "undefined" || !window.localStorage) return;
  const role = normalizeDeploymentRoleMode(roleMode);
  const token = String(startupToken || "").trim();
  if (!role || !token) return;
  try {
    window.localStorage.setItem(
      STARTUP_ROLE_SESSION_KEY,
      JSON.stringify({
        role_mode: role,
        startup_token: token,
        node_id: String(nodeId || "").trim(),
        confirmed_at: Date.now(),
      }),
    );
  } catch (_) {
    // ignore localStorage errors
  }
}

export function clearStartupRoleSession() {
  if (typeof window === "undefined" || !window.localStorage) return;
  try {
    window.localStorage.removeItem(STARTUP_ROLE_SESSION_KEY);
  } catch (_) {
    // ignore localStorage errors
  }
}

export function readMatchingStartupRoleSession(startupToken = "", nodeId = "") {
  const session = readStartupRoleSession();
  if (!session) return null;
  const currentStartupToken = String(startupToken || "").trim();
  const currentNodeId = String(nodeId || "").trim();
  if (!currentStartupToken) {
    return null;
  }
  if (session.startup_token !== currentStartupToken) {
    clearStartupRoleSession();
    return null;
  }
  if (currentNodeId && session.node_id && session.node_id !== currentNodeId) {
    clearStartupRoleSession();
    return null;
  }
  return session;
}

export function readStartupRuntimeRecovery() {
  if (typeof window === "undefined" || !window.sessionStorage) return null;
  try {
    const raw = window.sessionStorage.getItem(STARTUP_RUNTIME_RECOVERY_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const role = normalizeDeploymentRoleMode(parsed?.role_mode);
    const requestedAt = Number.parseInt(String(parsed?.requested_at || 0), 10);
    const path = normalizeBrowserPathname(parsed?.path || "/");
    if (!role || !Number.isFinite(requestedAt) || requestedAt <= 0) {
      window.sessionStorage.removeItem(STARTUP_RUNTIME_RECOVERY_KEY);
      return null;
    }
    if (Date.now() - requestedAt > STARTUP_ROLE_RESTART_PENDING_TTL_MS) {
      window.sessionStorage.removeItem(STARTUP_RUNTIME_RECOVERY_KEY);
      return null;
    }
    return {
      role_mode: role,
      requested_at: requestedAt,
      path,
    };
  } catch (_) {
    try {
      window.sessionStorage.removeItem(STARTUP_RUNTIME_RECOVERY_KEY);
    } catch (_) {
      // ignore sessionStorage errors
    }
    return null;
  }
}

export function writeStartupRuntimeRecovery(roleMode, path = "") {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  const role = normalizeDeploymentRoleMode(roleMode);
  if (!role) return;
  try {
    window.sessionStorage.setItem(
      STARTUP_RUNTIME_RECOVERY_KEY,
      JSON.stringify({
        role_mode: role,
        requested_at: Date.now(),
        path: normalizeBrowserPathname(path || "/"),
      }),
    );
  } catch (_) {
    // ignore sessionStorage errors
  }
}

export function clearStartupRuntimeRecovery() {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  try {
    window.sessionStorage.removeItem(STARTUP_RUNTIME_RECOVERY_KEY);
  } catch (_) {
    // ignore sessionStorage errors
  }
}

export function normalizeUpdaterRecoveryStage(value) {
  const stage = String(value || "").trim().toLowerCase();
  if (["queued", "applying", "restarting", "reloading"].includes(stage)) {
    return stage;
  }
  return "applying";
}

export function readUpdaterRecoveryIntent() {
  if (typeof window === "undefined" || !window.sessionStorage) return null;
  try {
    const raw = window.sessionStorage.getItem(UPDATER_RECOVERY_INTENT_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const requestedAt = Number.parseInt(String(parsed?.requested_at || 0), 10);
    if (!Number.isFinite(requestedAt) || requestedAt <= 0) {
      window.sessionStorage.removeItem(UPDATER_RECOVERY_INTENT_KEY);
      return null;
    }
    if (Date.now() - requestedAt > STARTUP_ROLE_RESTART_PENDING_TTL_MS) {
      window.sessionStorage.removeItem(UPDATER_RECOVERY_INTENT_KEY);
      return null;
    }
    return {
      role_mode: normalizeDeploymentRoleMode(parsed?.role_mode),
      path: normalizeBrowserPathname(parsed?.path || "/"),
      requested_at: requestedAt,
      stage: normalizeUpdaterRecoveryStage(parsed?.stage),
      source: String(parsed?.source || "").trim().toLowerCase(),
      startup_token: String(parsed?.startup_token || "").trim(),
    };
  } catch (_) {
    try {
      window.sessionStorage.removeItem(UPDATER_RECOVERY_INTENT_KEY);
    } catch (_) {
      // ignore sessionStorage errors
    }
    return null;
  }
}

export function writeUpdaterRecoveryIntent(payload = {}) {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  const requestedAt = Number.parseInt(String(payload?.requested_at || Date.now()), 10);
  try {
    window.sessionStorage.setItem(
      UPDATER_RECOVERY_INTENT_KEY,
      JSON.stringify({
        role_mode: normalizeDeploymentRoleMode(payload?.role_mode),
        path: normalizeBrowserPathname(payload?.path || "/"),
        requested_at: Number.isFinite(requestedAt) && requestedAt > 0 ? requestedAt : Date.now(),
        stage: normalizeUpdaterRecoveryStage(payload?.stage),
        source: String(payload?.source || "").trim().toLowerCase(),
        startup_token: String(payload?.startup_token || "").trim(),
      }),
    );
  } catch (_) {
    // ignore sessionStorage errors
  }
}

export function clearUpdaterRecoveryIntent() {
  if (typeof window === "undefined" || !window.sessionStorage) return;
  try {
    window.sessionStorage.removeItem(UPDATER_RECOVERY_INTENT_KEY);
  } catch (_) {
    // ignore sessionStorage errors
  }
}
