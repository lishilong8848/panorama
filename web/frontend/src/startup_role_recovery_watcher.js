export function registerStartupRoleRecoveryWatcher(options = {}) {
  const {
    watch,
    parseAppBrowserRoute,
    normalizeDeploymentRoleMode,
    clearStartupRouteFallbackTimer,
    readMatchingStartupRoleSession,
    readStartupRuntimeRecovery,
    currentStartupHandoff,
    formatDeploymentRoleLabel,
    bootstrapReady,
    configLoaded,
    startupRoleCurrentMode,
    startupRoleCurrentToken,
    startupRoleCurrentNodeId,
    startupRoleFlowState,
    updaterUiOverlayVisible,
    startupRoleSelectorVisible,
    startupRoleSelectorBusy,
    startupRoleLoadingVisible,
    startupRoleSelectorHandled,
    startupRoleDecisionReady,
    startupRoleSuppressedHandoffNonce,
    startupRoleSelectorSelection,
    startupRoleAutoActivationKey,
    health,
    selectStartupRole,
    syncStartupRoleBridgeDraft,
    showStartupRoleLoading,
    activateStartupRuntimeAfterSelection,
    suppressCurrentStartupHandoff,
    clearLegacyStartupRoleRestartState,
    clearUpdaterRecoveryIntent,
    message,
    showStartupRoleSelector,
    closeStartupRoleSelector,
    hideStartupRoleLoading,
    persistStartupRoleSession,
    clearStartupRuntimeRecovery,
    clearStartupRoleSession,
  } = options || {};

  watch(
    () => ({
      bootstrapReady: bootstrapReady.value,
      configLoaded: configLoaded.value,
      currentRole: startupRoleCurrentMode.value,
      currentStartupToken: startupRoleCurrentToken.value,
      currentNodeId: startupRoleCurrentNodeId.value,
      flowState: startupRoleFlowState.value,
      overlayVisible: updaterUiOverlayVisible.value,
      selectorVisible: startupRoleSelectorVisible.value,
      selectorBusy: startupRoleSelectorBusy.value,
      loadingVisible: startupRoleLoadingVisible.value,
      startupRoleConfirmed: Boolean(health.startup_role_confirmed),
      runtimeActivated: Boolean(health.runtime_activated),
      roleSelectionRequired: Boolean(health.role_selection_required),
      startupRoleUserExited: Boolean(health.startup_role_user_exited),
      startupHandoffActive: Boolean(health.startup_handoff?.active),
      startupHandoffRole: normalizeDeploymentRoleMode(health.startup_handoff?.target_role_mode),
      startupHandoffNonce: String(health.startup_handoff?.nonce || "").trim(),
    }),
    (state) => {
      const routeRole = parseAppBrowserRoute(typeof window !== "undefined" ? window.location.pathname : "/").role_mode;
      if (!state.bootstrapReady) {
        clearStartupRouteFallbackTimer();
        return;
      }
      clearStartupRouteFallbackTimer();
      if (state.overlayVisible) return;
      const normalizedFlowState = String(state.flowState || "").trim().toLowerCase();
      const activationFlowInProgress = Boolean(
        (state.loadingVisible || state.selectorBusy || ["activating", "recovering", "restarting"].includes(normalizedFlowState))
        && !state.runtimeActivated
      );
      if (activationFlowInProgress) return;
      const savedRole = normalizeDeploymentRoleMode(state.currentRole || routeRole);
      if (state.startupRoleUserExited) {
        startupRoleDecisionReady.value = true;
        startupRoleAutoActivationKey.value = "";
        if (state.selectorVisible || state.selectorBusy) return;
        selectStartupRole(savedRole || startupRoleSelectorSelection.value || "internal");
        syncStartupRoleBridgeDraft();
        hideStartupRoleLoading();
        clearStartupRoleSession();
        clearStartupRuntimeRecovery();
        clearUpdaterRecoveryIntent();
        showStartupRoleSelector("已退出当前系统，请重新选择角色。");
        return;
      }
      const storedStartupRoleSession = readMatchingStartupRoleSession(
        state.currentStartupToken,
        state.currentNodeId,
      );
      const runtimeRecoveryIntent = readStartupRuntimeRecovery();
      const startupHandoff = currentStartupHandoff();
      const canResumeAfterRestart =
        Boolean(startupHandoff.active)
        && Boolean(startupHandoff.target_role_mode)
        && Boolean(startupHandoff.nonce)
        && startupHandoff.nonce !== startupRoleSuppressedHandoffNonce.value;
      if (canResumeAfterRestart && !state.runtimeActivated) {
        const resumeRole = startupHandoff.target_role_mode || savedRole || startupRoleSelectorSelection.value || "internal";
        const activationKey = `${state.currentStartupToken || ""}|${resumeRole}|${startupHandoff.nonce}|restart_resume`;
        if (startupRoleSelectorBusy.value || (startupRoleLoadingVisible.value && startupRoleFlowState.value !== "recovering")) return;
        if (startupRoleAutoActivationKey.value === activationKey) return;
        selectStartupRole(resumeRole);
        syncStartupRoleBridgeDraft();
        startupRoleDecisionReady.value = true;
        startupRoleSelectorHandled.value = true;
        startupRoleSelectorVisible.value = false;
        startupRoleAutoActivationKey.value = activationKey;
        startupRoleSelectorBusy.value = true;
        showStartupRoleLoading({
          title: `正在继续启动${formatDeploymentRoleLabel(resumeRole || "internal")}`,
          subtitle: "服务已恢复，正在继续连接后台运行时，请稍候。",
          stage: "restarting",
        });
        void (async () => {
          const activated = await activateStartupRuntimeAfterSelection("startup_role_resume_after_restart", {
            targetRoleMode: resumeRole,
            startupHandoffNonce: startupHandoff.nonce,
          });
          startupRoleSelectorBusy.value = false;
          if (!activated) {
            suppressCurrentStartupHandoff();
            clearLegacyStartupRoleRestartState();
            clearUpdaterRecoveryIntent();
            message.value = "服务已恢复，但后台运行时启动失败，请重新确认启动角色。";
            showStartupRoleSelector("后台运行时激活失败，请重新确认启动角色。");
            return;
          }
          clearLegacyStartupRoleRestartState();
          closeStartupRoleSelector({ handled: true });
        })();
        return;
      }
      if (state.runtimeActivated) {
        const activatedRole =
          savedRole || normalizeDeploymentRoleMode(startupRoleSelectorSelection.value) || "internal";
        const activationKey = `${state.currentStartupToken || ""}|${activatedRole}`;
        startupRoleDecisionReady.value = true;
        startupRoleSelectorHandled.value = true;
        startupRoleSelectorVisible.value = false;
        startupRoleAutoActivationKey.value = activationKey;
        hideStartupRoleLoading();
        startupRoleSelectorBusy.value = false;
        persistStartupRoleSession(activatedRole);
        clearStartupRuntimeRecovery();
        clearUpdaterRecoveryIntent();
        clearLegacyStartupRoleRestartState();
        startupRoleSuppressedHandoffNonce.value = "";
        return;
      }
      if (runtimeRecoveryIntent && savedRole && runtimeRecoveryIntent.role_mode === savedRole) {
        const activationKey = `${state.currentStartupToken || ""}|${savedRole}|runtime_recovery`;
        if (startupRoleSelectorBusy.value || (startupRoleLoadingVisible.value && startupRoleFlowState.value !== "recovering")) return;
        if (startupRoleAutoActivationKey.value === activationKey) return;
        selectStartupRole(savedRole);
        syncStartupRoleBridgeDraft();
        startupRoleDecisionReady.value = true;
        startupRoleSelectorHandled.value = true;
        startupRoleSelectorVisible.value = false;
        startupRoleAutoActivationKey.value = activationKey;
        startupRoleSelectorBusy.value = true;
        showStartupRoleLoading({
          title: `正在恢复${formatDeploymentRoleLabel(savedRole)}`,
          subtitle: "检测到更新后的自动恢复请求，正在重新进入对应页面。",
          stage: "recovering",
        });
        void (async () => {
          const activated = await activateStartupRuntimeAfterSelection("startup_role_runtime_recovery", {
            targetRoleMode: savedRole,
          });
          startupRoleSelectorBusy.value = false;
          if (!activated) {
            clearStartupRuntimeRecovery();
            clearUpdaterRecoveryIntent();
            showStartupRoleSelector("后台运行时激活失败，请重新确认启动角色。");
            return;
          }
          closeStartupRoleSelector({ handled: true });
        })();
        return;
      }
      if (storedStartupRoleSession && savedRole && storedStartupRoleSession.role_mode === savedRole) {
        const activationKey = `${state.currentStartupToken || ""}|${savedRole}|session_resume`;
        if (startupRoleSelectorBusy.value || (startupRoleLoadingVisible.value && startupRoleFlowState.value !== "recovering")) return;
        if (startupRoleAutoActivationKey.value === activationKey) return;
        selectStartupRole(savedRole);
        syncStartupRoleBridgeDraft();
        startupRoleDecisionReady.value = true;
        startupRoleSelectorHandled.value = true;
        startupRoleSelectorVisible.value = false;
        startupRoleAutoActivationKey.value = activationKey;
        startupRoleSelectorBusy.value = true;
        showStartupRoleLoading({
          title: `正在恢复${formatDeploymentRoleLabel(savedRole)}`,
          subtitle: "检测到本次启动已确认角色，正在恢复对应页面。",
          stage: "recovering",
        });
        void (async () => {
          const activated = await activateStartupRuntimeAfterSelection("startup_role_session_resume", {
            targetRoleMode: savedRole,
          });
          startupRoleSelectorBusy.value = false;
          if (!activated) {
            clearStartupRoleSession();
            showStartupRoleSelector("后台运行时激活失败，请重新确认启动角色。");
            return;
          }
          closeStartupRoleSelector({ handled: true });
        })();
        return;
      }
      if (
        routeRole
        && savedRole
        && routeRole === savedRole
        && state.startupRoleConfirmed
        && !state.roleSelectionRequired
      ) {
        const activationKey = `${state.currentStartupToken || ""}|${routeRole}|route_resume`;
        if (startupRoleSelectorBusy.value || (startupRoleLoadingVisible.value && startupRoleFlowState.value !== "recovering")) return;
        if (startupRoleAutoActivationKey.value === activationKey) return;
        selectStartupRole(routeRole);
        syncStartupRoleBridgeDraft();
        startupRoleDecisionReady.value = true;
        startupRoleSelectorHandled.value = true;
        startupRoleSelectorVisible.value = false;
        startupRoleAutoActivationKey.value = activationKey;
        startupRoleSelectorBusy.value = true;
        showStartupRoleLoading({
          title: `正在恢复${formatDeploymentRoleLabel(routeRole)}`,
          subtitle: "检测到当前地址对应已确认角色，正在恢复对应页面。",
          stage: "recovering",
        });
        void (async () => {
          const activated = await activateStartupRuntimeAfterSelection("startup_role_route_resume", {
            targetRoleMode: routeRole,
          });
          startupRoleSelectorBusy.value = false;
          if (!activated) {
            showStartupRoleSelector("后台运行时激活失败，请重新确认启动角色。");
            return;
          }
          closeStartupRoleSelector({ handled: true });
        })();
        return;
      }
      if (
        !routeRole
        && savedRole
        && state.startupRoleConfirmed
        && !state.roleSelectionRequired
      ) {
        const activationKey = `${state.currentStartupToken || ""}|${savedRole}|saved_config_resume`;
        if (startupRoleSelectorBusy.value || (startupRoleLoadingVisible.value && startupRoleFlowState.value !== "recovering")) return;
        if (startupRoleAutoActivationKey.value === activationKey) return;
        selectStartupRole(savedRole);
        syncStartupRoleBridgeDraft();
        startupRoleDecisionReady.value = true;
        startupRoleSelectorHandled.value = true;
        startupRoleSelectorVisible.value = false;
        startupRoleAutoActivationKey.value = activationKey;
        startupRoleSelectorBusy.value = true;
        showStartupRoleLoading({
          title: `正在恢复${formatDeploymentRoleLabel(savedRole)}`,
          subtitle: "检测到已保存的启动角色和共享目录，正在自动恢复对应页面。",
          stage: "recovering",
        });
        void (async () => {
          const activated = await activateStartupRuntimeAfterSelection("startup_role_saved_config_resume", {
            targetRoleMode: savedRole,
          });
          startupRoleSelectorBusy.value = false;
          if (!activated) {
            showStartupRoleSelector("后台运行时激活失败，请重新确认启动角色。");
            return;
          }
          closeStartupRoleSelector({ handled: true });
        })();
        return;
      }
      if (state.selectorVisible || state.selectorBusy) return;
      if (savedRole && !state.roleSelectionRequired) {
        return;
      }
      clearLegacyStartupRoleRestartState();
      startupRoleAutoActivationKey.value = "";
      selectStartupRole(savedRole || startupRoleSelectorSelection.value || "internal");
      syncStartupRoleBridgeDraft();
      hideStartupRoleLoading();
      clearStartupRoleSession();
      clearStartupRuntimeRecovery();
      clearUpdaterRecoveryIntent();
      showStartupRoleSelector(savedRole ? "" : "请先选择有效角色。");
    },
    { immediate: true, deep: false },
  );
}
