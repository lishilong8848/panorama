import { normalizeBrowserPathname } from "./startup_runtime_storage_helpers.js";

export function createStartupRoleUiHelpers(options = {}) {
  const {
    normalizeDeploymentRoleMode,
    parseAppBrowserRoute,
    buildAppBrowserRoutePath,
    formatDeploymentRoleLabel,
    normalizePositiveInteger,
    STARTUP_BRIDGE_DEFAULTS,
    buildStartupBridgeDraft,
    validateStartupBridgeDraft,
    startupRoleSelectorVisible,
    startupRoleSelectorBusy,
    startupRoleSelectorMessage,
    startupRoleDecisionReady,
    startupRoleSelectorHandled,
    startupRoleFlowState,
    startupRoleLoadingVisible,
    startupRoleLoadingTitle,
    startupRoleLoadingSubtitle,
    startupRoleLoadingStage,
    startupRoleActivationInFlight,
    startupRoleCurrentMode,
    startupRoleCurrentToken,
    startupRoleCurrentNodeId,
    startupRoleSuppressedHandoffNonce,
    startupRoleSelectorSelection,
    startupRoleBridgeDraft,
    startupRoleAdvancedVisible,
    startupRoleAutoActivationKey,
    browserRouteLastPath,
    browserRouteReady,
    currentView,
    configLoaded,
    config,
    deploymentRoleMode,
    health,
    message,
    activateStartupRuntime,
    exitCurrentRuntime,
    fetchBootstrapHealth,
    fetchHealth,
    clearStartupRoleRestartPending,
    clearStartupRoleRestartResume,
    clearStartupRoleSession,
    clearStartupRuntimeRecovery,
    clearUpdaterRecoveryIntent,
    writeStartupRoleSession,
    writeStartupRuntimeRecovery,
    onRuntimeExited,
  } = options || {};
  const STARTUP_ACTIVATION_LOCK_KEY = "qjpt_startup_activation_lock_v1";
  const STARTUP_ACTIVATION_LOCK_TTL_MS = 90 * 1000;

  function closeStartupRoleSelector({ handled = false } = {}) {
    startupRoleSelectorVisible.value = false;
    startupRoleSelectorBusy.value = false;
    startupRoleSelectorMessage.value = "";
    startupRoleDecisionReady.value = true;
    if (handled) {
      startupRoleSelectorHandled.value = true;
      startupRoleFlowState.value = "activated";
    }
  }

  function showStartupRoleSelector(messageText = "") {
    startupRoleLoadingVisible.value = false;
    startupRoleLoadingTitle.value = "";
    startupRoleLoadingSubtitle.value = "";
    startupRoleLoadingStage.value = "";
    startupRoleSelectorMessage.value = String(messageText || "").trim();
    startupRoleDecisionReady.value = true;
    startupRoleSelectorVisible.value = true;
    startupRoleSelectorHandled.value = false;
    startupRoleSelectorBusy.value = false;
    startupRoleFlowState.value = "selecting";
  }

  function showStartupRoleLoading({ title = "", subtitle = "", stage = "" } = {}) {
    startupRoleSelectorVisible.value = false;
    startupRoleLoadingVisible.value = true;
    startupRoleLoadingTitle.value = String(title || "").trim();
    startupRoleLoadingSubtitle.value = String(subtitle || "").trim();
    startupRoleLoadingStage.value = String(stage || "").trim();
    const normalizedStage = String(stage || "").trim().toLowerCase();
    if (normalizedStage === "restarting") {
      startupRoleFlowState.value = "restarting";
    } else if (normalizedStage === "reloading" || normalizedStage === "recovering") {
      startupRoleFlowState.value = "recovering";
    } else {
      startupRoleFlowState.value = "activating";
    }
  }

  function hideStartupRoleLoading() {
    startupRoleLoadingVisible.value = false;
    startupRoleLoadingTitle.value = "";
    startupRoleLoadingSubtitle.value = "";
    startupRoleLoadingStage.value = "";
  }

  function clearStartupRoleRestartPendingState() {
    clearStartupRoleRestartPending?.();
  }

  function clearStartupRoleRestartResumeState() {
    clearStartupRoleRestartResume?.();
  }

  function clearLegacyStartupRoleRestartState() {
    clearStartupRoleRestartPendingState();
    clearStartupRoleRestartResumeState();
  }

  function persistStartupRoleSession(roleMode = "") {
    const confirmedRole = normalizeDeploymentRoleMode(roleMode || startupRoleCurrentMode.value);
    const startupToken = String(health.startup_time || startupRoleCurrentToken.value || "").trim();
    const nodeId = String(health.deployment?.node_id || startupRoleCurrentNodeId.value || "").trim();
    if (!confirmedRole || !startupToken) return;
    writeStartupRoleSession?.(confirmedRole, startupToken, nodeId);
  }

  function sleep(ms) {
    return new Promise((resolve) => {
      window.setTimeout(resolve, Math.max(0, Number(ms) || 0));
    });
  }

  function readStartupActivationLock() {
    if (typeof window === "undefined" || !window.localStorage) return null;
    try {
      const raw = window.localStorage.getItem(STARTUP_ACTIVATION_LOCK_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") return null;
      const expiresAt = Number.parseInt(String(parsed.expires_at || 0), 10) || 0;
      if (expiresAt && expiresAt < Date.now()) {
        window.localStorage.removeItem(STARTUP_ACTIVATION_LOCK_KEY);
        return null;
      }
      return parsed;
    } catch (_) {
      try {
        window.localStorage.removeItem(STARTUP_ACTIVATION_LOCK_KEY);
      } catch (_) {
        // ignore storage cleanup errors
      }
      return null;
    }
  }

  function writeStartupActivationLock(lock) {
    if (typeof window === "undefined" || !window.localStorage) return false;
    try {
      window.localStorage.setItem(STARTUP_ACTIVATION_LOCK_KEY, JSON.stringify(lock || {}));
      return true;
    } catch (_) {
      return false;
    }
  }

  function clearStartupActivationLock(owner = "") {
    if (typeof window === "undefined" || !window.localStorage) return;
    try {
      const current = readStartupActivationLock();
      const ownerText = String(owner || "").trim();
      if (ownerText && current && String(current.owner || "").trim() !== ownerText) return;
      window.localStorage.removeItem(STARTUP_ACTIVATION_LOCK_KEY);
    } catch (_) {
      // ignore storage cleanup errors
    }
  }

  function buildStartupActivationLockKey(roleMode) {
    return [
      String(startupRoleCurrentToken.value || "").trim(),
      normalizeDeploymentRoleMode(roleMode) || "",
    ].join("|");
  }

  function resolveActivationProgressSubtitle(defaultText = "正在连接后台运行时，请稍候。") {
    const step = String(health.activation_step || "").trim();
    const phase = String(health.activation_phase || "").trim().toLowerCase();
    if (phase === "failed") {
      return String(health.activation_error || "").trim() || "后台运行时激活失败。";
    }
    if (step === "queued") {
      return "后台运行时激活请求已受理，正在排队启动。";
    }
    if (step === "starting_runtime_services") {
      return "正在启动调度、共享桥接和后台运行组件。";
    }
    if (step === "initializing_handover_daily_report_auth") {
      return "正在初始化交接班日报截图登录态。";
    }
    if (step === "probing_handover_review_access") {
      return "正在检查审核页访问地址与审核链接补发状态。";
    }
    if (step === "activated") {
      return "后台运行时已就绪，正在进入页面。";
    }
    return defaultText;
  }

  async function waitForStartupActivationCompletion(targetRole) {
    const deadline = Date.now() + 120000;
    while (Date.now() < deadline) {
      await fetchBootstrapHealth?.({ silentMessage: true });
      const phase = String(health.activation_phase || "").trim().toLowerCase();
      if (Boolean(health.runtime_activated) && Boolean(health.startup_role_confirmed)) {
        return true;
      }
      if (phase === "failed") {
        const errorText = String(health.activation_error || "").trim() || "后台运行时激活失败。";
        hideStartupRoleLoading();
        startupRoleFlowState.value = "selecting";
        if (message) message.value = errorText;
        return false;
      }
      showStartupRoleLoading({
        title: `正在加载${formatDeploymentRoleLabel(targetRole || "internal")}`,
        subtitle: resolveActivationProgressSubtitle(),
        stage: "activating",
      });
      await sleep(3000);
    }
    hideStartupRoleLoading();
    startupRoleFlowState.value = "selecting";
    if (message) {
      const step = String(health.activation_step || "").trim();
      message.value = `后台运行时启动超时${step ? `（当前阶段：${step}）` : ""}。`;
    }
    return false;
  }

  function persistRuntimeRecoveryIntent(roleMode = "") {
    const confirmedRole = normalizeDeploymentRoleMode(
      roleMode || startupRoleCurrentMode.value || deploymentRoleMode.value,
    );
    if (!confirmedRole || typeof window === "undefined") return;
    writeStartupRuntimeRecovery?.(confirmedRole, window.location.pathname);
  }

  function applyBrowserRoute(route, options = {}) {
    const nextRoute = route && typeof route === "object" ? route : parseAppBrowserRoute(typeof window !== "undefined" ? window.location.pathname : "/");
    const forceLogin = Boolean(options?.forceLogin);
    const routeRole = normalizeDeploymentRoleMode(nextRoute.role_mode);
    const routeView = String(nextRoute.view || "").trim().toLowerCase();
    if (forceLogin || nextRoute.kind === "login") {
      browserRouteLastPath.value = "/";
      return;
    }
    if (routeView === "config") {
      currentView.value = "config";
    } else if (routeView === "status") {
      currentView.value = "status";
    } else if (routeView === "dashboard") {
      currentView.value = routeRole === "internal" ? "status" : "dashboard";
    } else if (routeRole === "internal") {
      currentView.value = "status";
    } else if (routeRole === "external") {
      currentView.value = "dashboard";
    }
    browserRouteLastPath.value = buildAppBrowserRoutePath(
      routeRole || deploymentRoleMode.value,
      currentView.value,
      false,
    );
  }

  function syncBrowserRoute(options = {}) {
    if (typeof window === "undefined" || !window.history) return;
    const replace = Boolean(options?.replace);
    const selectorVisible = Boolean(options?.selectorVisible ?? startupRoleSelectorVisible.value);
    const targetPath = buildAppBrowserRoutePath(
      deploymentRoleMode.value,
      currentView.value,
      selectorVisible,
    );
    const currentPath = normalizeBrowserPathname(window.location.pathname);
    if (currentPath === targetPath) {
      browserRouteLastPath.value = targetPath;
      browserRouteReady.value = true;
      return;
    }
    const method = replace || !browserRouteReady.value ? "replaceState" : "pushState";
    window.history[method]({}, "", targetPath);
    browserRouteLastPath.value = targetPath;
    browserRouteReady.value = true;
  }

  function currentStartupHandoff() {
    const raw = health.startup_handoff;
    if (!raw || typeof raw !== "object") {
      return {
        active: false,
        mode: "",
        target_role_mode: "",
        requested_at: "",
        reason: "",
        nonce: "",
      };
    }
    return {
      active: Boolean(raw.active),
      mode: String(raw.mode || "").trim(),
      target_role_mode: normalizeDeploymentRoleMode(raw.target_role_mode),
      requested_at: String(raw.requested_at || "").trim(),
      reason: String(raw.reason || "").trim(),
      nonce: String(raw.nonce || "").trim(),
    };
  }

  function startupRoleDraftSourceConfig() {
    if (configLoaded.value && config.value && typeof config.value === "object") {
      return config.value;
    }
    return {
      deployment: health.deployment || {},
      shared_bridge: health.startup_shared_bridge || {},
    };
  }

  function suppressCurrentStartupHandoff() {
    const nonce = String(health.startup_handoff?.nonce || "").trim();
    if (nonce) {
      startupRoleSuppressedHandoffNonce.value = nonce;
    }
    if (health.startup_handoff && typeof health.startup_handoff === "object") {
      Object.assign(health.startup_handoff, {
        active: false,
        mode: "",
        target_role_mode: "",
        requested_at: "",
        reason: "",
        nonce: "",
      });
    }
  }

  function syncStartupRoleBridgeDraft() {
    const role = normalizeDeploymentRoleMode(startupRoleSelectorSelection.value || startupRoleCurrentMode.value || "internal") || "internal";
    startupRoleBridgeDraft.value = buildStartupBridgeDraft(startupRoleDraftSourceConfig(), role);
    startupRoleAdvancedVisible.value = false;
  }

  function selectStartupRole(value) {
    const normalized = normalizeDeploymentRoleMode(value);
    startupRoleSelectorSelection.value = normalized || "internal";
    startupRoleSelectorMessage.value = "";
    syncStartupRoleBridgeDraft();
  }

  async function activateStartupRuntimeAfterSelection(source, options = {}) {
    if (startupRoleActivationInFlight?.value) {
      return false;
    }
    const targetRole = normalizeDeploymentRoleMode(
      options?.targetRoleMode || startupRoleSelectorSelection.value || config.value?.deployment?.role_mode || startupRoleCurrentMode.value,
    );
    const activationLockKey = buildStartupActivationLockKey(targetRole);
    const existingLock = readStartupActivationLock();
    const existingLockAgeMs = existingLock
      ? Math.max(0, Date.now() - (Number.parseInt(String(existingLock.started_at || 0), 10) || Date.now()))
      : 0;
    const backendActivationBusy = ["activating", "recovering", "restarting"].includes(
      String(health.activation_phase || "").trim().toLowerCase(),
    );
    if (
      existingLock
      && String(existingLock.key || "").trim() === activationLockKey
      && !Boolean(health.runtime_activated)
      && (backendActivationBusy || existingLockAgeMs < 8000)
    ) {
      showStartupRoleLoading({
        title: `正在加载${formatDeploymentRoleLabel(targetRole || "internal")}`,
        subtitle: "已有启动请求正在处理中，正在等待后台运行时就绪。",
        stage: "activating",
      });
      return waitForStartupActivationCompletion(targetRole);
    }
    const lockOwner = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    writeStartupActivationLock({
      key: activationLockKey,
      owner: lockOwner,
      role_mode: targetRole,
      source: String(source || "").trim(),
      started_at: Date.now(),
      expires_at: Date.now() + STARTUP_ACTIVATION_LOCK_TTL_MS,
    });
    startupRoleActivationInFlight.value = true;
    const bridgeRoot = String(startupRoleBridgeDraft.value?.root_dir || "").trim();
    const roleRootKey = targetRole === "internal" ? "internal_root_dir" : "external_root_dir";
    const sharedBridgePayload = {
      enabled: true,
      poll_interval_sec: normalizePositiveInteger(
        startupRoleBridgeDraft.value?.poll_interval_sec,
        STARTUP_BRIDGE_DEFAULTS.poll_interval_sec,
      ),
      heartbeat_interval_sec: normalizePositiveInteger(
        startupRoleBridgeDraft.value?.heartbeat_interval_sec,
        STARTUP_BRIDGE_DEFAULTS.heartbeat_interval_sec,
      ),
      claim_lease_sec: normalizePositiveInteger(
        startupRoleBridgeDraft.value?.claim_lease_sec,
        STARTUP_BRIDGE_DEFAULTS.claim_lease_sec,
      ),
      stale_task_timeout_sec: normalizePositiveInteger(
        startupRoleBridgeDraft.value?.stale_task_timeout_sec,
        STARTUP_BRIDGE_DEFAULTS.stale_task_timeout_sec,
      ),
      artifact_retention_days: normalizePositiveInteger(
        startupRoleBridgeDraft.value?.artifact_retention_days,
        STARTUP_BRIDGE_DEFAULTS.artifact_retention_days,
      ),
      sqlite_busy_timeout_ms: normalizePositiveInteger(
        startupRoleBridgeDraft.value?.sqlite_busy_timeout_ms,
        STARTUP_BRIDGE_DEFAULTS.sqlite_busy_timeout_ms,
      ),
    };
    if (bridgeRoot && roleRootKey) {
      sharedBridgePayload.root_dir = bridgeRoot;
      sharedBridgePayload[roleRootKey] = bridgeRoot;
    }
    showStartupRoleLoading({
      title: `正在加载${formatDeploymentRoleLabel(targetRole || "internal")}`,
      subtitle: "正在连接后台运行时，请稍候。",
      stage: "activating",
    });
    try {
      const activationResult = await activateStartupRuntime?.({
        source,
        roleMode: targetRole,
        sharedBridge: sharedBridgePayload,
        startupHandoffNonce: String(options?.startupHandoffNonce || "").trim(),
      });
      if (activationResult?.ok === false) {
        hideStartupRoleLoading();
        startupRoleFlowState.value = "selecting";
        if (message) message.value = String(activationResult?.error || "").trim() || "后台运行时激活失败。";
        return false;
      }
      if (activationResult?.pending) {
        health.activation_phase = String(activationResult?.phase || "activating").trim();
        health.activation_step = String(activationResult?.step || "queued").trim();
        const completed = await waitForStartupActivationCompletion(targetRole);
        if (!completed) {
          return false;
        }
      } else {
        health.runtime_activated = true;
        health.startup_role_confirmed = true;
        health.role_selection_required = false;
        health.startup_role_user_exited = false;
        if (targetRole) {
          Object.assign(health.deployment, {
            ...(health.deployment || {}),
            role_mode: targetRole,
            last_started_role_mode: targetRole,
            node_label: formatDeploymentRoleLabel(targetRole),
          });
        }
        if (activationResult?.savedRole && typeof activationResult.savedRole === "object") {
          Object.assign(health.deployment, {
            role_mode: String(activationResult.savedRole.role_mode || targetRole || "").trim(),
            node_label: String(activationResult.savedRole.node_label || formatDeploymentRoleLabel(targetRole)).trim(),
          });
        }
        await fetchBootstrapHealth?.({ silentMessage: true });
      }
      if (health.startup_handoff && typeof health.startup_handoff === "object") {
        Object.assign(health.startup_handoff, {
          active: false,
          mode: "",
          target_role_mode: "",
          requested_at: "",
          reason: "",
          nonce: "",
        });
      }
      startupRoleSuppressedHandoffNonce.value = "";
      persistStartupRoleSession(targetRole);
      hideStartupRoleLoading();
      startupRoleFlowState.value = "activated";
      return true;
    } finally {
      startupRoleActivationInFlight.value = false;
      clearStartupActivationLock(lockOwner);
    }
  }

  async function exitCurrentSystemToRoleSelector() {
    if (startupRoleSelectorBusy.value || startupRoleLoadingVisible.value) return;
    startupRoleSelectorBusy.value = true;
    showStartupRoleLoading({
      title: "正在退出当前系统",
      subtitle: "正在停止当前角色的调度、共享桥接和后台运行组件。",
      stage: "activating",
    });
    const result = await exitCurrentRuntime?.({ source: "用户退出当前系统" });
    startupRoleSelectorBusy.value = false;
    if (!result?.ok) {
      hideStartupRoleLoading();
      if (message) message.value = String(result?.error || "").trim() || "退出当前系统失败。";
      return;
    }
    onRuntimeExited?.();
    clearStartupRoleSession?.();
    clearStartupRuntimeRecovery?.();
    clearUpdaterRecoveryIntent?.();
    clearLegacyStartupRoleRestartState();
    startupRoleAutoActivationKey.value = "";
    health.runtime_activated = false;
    health.startup_role_confirmed = false;
    health.role_selection_required = true;
    health.startup_role_user_exited = true;
    selectStartupRole(
      startupRoleCurrentMode.value
      || deploymentRoleMode.value
      || startupRoleSelectorSelection.value
      || "internal",
    );
    syncStartupRoleBridgeDraft();
    showStartupRoleSelector("已退出当前系统，请重新选择角色。");
    if (message) message.value = "已退出当前系统，请重新选择角色。";
    await fetchBootstrapHealth?.({ silentMessage: true });
  }

  async function confirmStartupRoleSelection() {
    if (startupRoleSelectorBusy.value) return;
    const targetRole = normalizeDeploymentRoleMode(startupRoleSelectorSelection.value);
    startupRoleSelectorBusy.value = true;
    startupRoleSelectorMessage.value = "";
    showStartupRoleLoading({
      title: `正在准备${formatDeploymentRoleLabel(targetRole || "internal")}`,
      subtitle: "正在校验启动参数，请稍候。",
      stage: "validating",
    });

    const draftValidationMessage = validateStartupBridgeDraft?.(targetRole, startupRoleBridgeDraft.value);
    if (draftValidationMessage) {
      hideStartupRoleLoading();
      startupRoleSelectorVisible.value = true;
      startupRoleSelectorBusy.value = false;
      startupRoleSelectorMessage.value = draftValidationMessage;
      return;
    }

    const activated = await activateStartupRuntimeAfterSelection("startup_role_confirm", {
      targetRoleMode: targetRole,
    });
    startupRoleSelectorBusy.value = false;
    if (!activated) {
      showStartupRoleSelector("后台运行时激活失败。");
      return;
    }
    closeStartupRoleSelector({ handled: true });
    clearLegacyStartupRoleRestartState();
    syncStartupRoleBridgeDraft();
    if (message) message.value = `已进入${formatDeploymentRoleLabel(targetRole)}。`;
  }

  return {
    closeStartupRoleSelector,
    showStartupRoleSelector,
    showStartupRoleLoading,
    hideStartupRoleLoading,
    clearStartupRoleRestartPendingState,
    clearStartupRoleRestartResumeState,
    clearLegacyStartupRoleRestartState,
    persistStartupRoleSession,
    persistRuntimeRecoveryIntent,
    applyBrowserRoute,
    syncBrowserRoute,
    currentStartupHandoff,
    startupRoleDraftSourceConfig,
    suppressCurrentStartupHandoff,
    syncStartupRoleBridgeDraft,
    selectStartupRole,
    activateStartupRuntimeAfterSelection,
    exitCurrentSystemToRoleSelector,
    confirmStartupRoleSelection,
  };
}
