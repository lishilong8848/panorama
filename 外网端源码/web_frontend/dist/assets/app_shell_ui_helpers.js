export function createAppShellUiHelpers(options = {}) {
  const {
    computed,
    health,
    config,
    bootstrapReady,
    startupRoleSelectorVisible,
    startupRoleSelectorBusy,
    startupRoleDecisionReady,
    startupRoleSelectorSelection,
    startupRoleBridgeDraft,
    normalizeDeploymentRoleMode,
    formatDeploymentRoleLabel,
    buildRoleNodeIdPreview,
    validateStartupBridgeDraft,
    resolveSharedBridgeRoleRoot,
    startupRoleDraftSourceConfig,
    isStartupBridgeDraftChanged,
  } = options;

  const effectiveRoleMode = computed(() =>
    normalizeDeploymentRoleMode(
      health.deployment?.role_mode || config.value?.deployment?.role_mode || "",
    ),
  );
  const deploymentRoleMode = computed(() => effectiveRoleMode.value);
  const configRoleMode = computed(() =>
    normalizeDeploymentRoleMode(config.value?.deployment?.role_mode || deploymentRoleMode.value),
  );
  const showCommonPathsConfigTab = computed(() => true);
  const showCommonSchedulerConfigTab = computed(() => true);
  const showNotifyConfigTab = computed(() => true);
  const showFeishuAuthConfigTab = computed(() => true);
  const showConsoleConfigTab = computed(() => true);
  const showFeatureMonthlyConfigTab = computed(() => true);
  const showFeatureHandoverConfigTab = computed(() => true);
  const showFeatureDayMetricUploadConfigTab = computed(() => true);
  const showFeatureWetBulbCollectionConfigTab = computed(() => true);
  const showFeatureAlarmExportConfigTab = computed(() => true);
  const showSheetImportConfigTab = computed(() => true);
  const showManualFeatureConfigTab = computed(() => true);
  const showRuntimeNetworkPanel = computed(() => false);
  const showDashboardPageNav = computed(() => true);

  const appShellTitle = computed(() => "外网业务控制台");
  const statusNavLabel = computed(() => "状态总览");
  const dashboardNavLabel = computed(() => "业务控制台");
  const configNavLabel = computed(() => "配置中心");
  const configShellTitle = computed(() => "配置中心（公共 + 功能分组）");
  const configShellDescription = computed(() => "左侧切换配置分组，右侧仅显示当前分组内容；外网端不展示采集端下载细节配置。");
  const configReturnButtonText = computed(() => "返回业务控制台");
  const statusHeroTitle = computed(() => "外网业务状态与共享任务");
  const statusHeroDescription = computed(() => "这一页负责查看外网业务运行状态，并保留共享任务、审核与后续上传入口。");
  const bridgeExecutionHint = computed(() => "当前为外网端，默认优先读取共享文件；缺失时再等待采集端补采。");
  const externalExecutionHint = computed(() => "当前为外网端，按当前网络直接执行。");
  const resumeExecutionHint = computed(() => "外网端会从共享文件继续上传，不重新触发共享文件准备。");

  const startupRoleCurrentMode = computed(() => effectiveRoleMode.value);
  const startupRoleCurrentToken = computed(() => String(health.startup_time || "").trim());
  const startupRoleCurrentNodeId = computed(() => String(health.deployment?.node_id || "").trim());
  const startupRoleCurrentLabel = computed(() => formatDeploymentRoleLabel(startupRoleCurrentMode.value));
  const startupRoleSelectedLabel = computed(() => formatDeploymentRoleLabel(startupRoleSelectorSelection.value));
  const startupRoleNodeIdDisplayText = computed(() =>
    buildRoleNodeIdPreview(
      startupRoleCurrentNodeId.value,
      startupRoleCurrentMode.value,
      startupRoleSelectorSelection.value,
    ),
  );
  const startupRoleNodeIdDisplayHint = computed(() =>
    startupRoleNodeIdDisplayText.value === "切换后自动生成并长期固定"
      ? "当前角色变更后会按本机自动生成并长期固定。"
      : normalizeDeploymentRoleMode(startupRoleSelectorSelection.value) === startupRoleCurrentMode.value
        ? "当前生效节点 ID"
        : "按当前机器推导出的目标角色节点 ID"
  );
  const startupRoleRequiresBridgeConfig = computed(() =>
    ["internal", "external"].includes(normalizeDeploymentRoleMode(startupRoleSelectorSelection.value)),
  );
  const startupRoleBridgeValidationMessage = computed(() =>
    validateStartupBridgeDraft(startupRoleSelectorSelection.value, startupRoleBridgeDraft.value),
  );
  const startupRoleCurrentHasBridgeConfig = computed(() =>
    Boolean(resolveSharedBridgeRoleRoot(startupRoleDraftSourceConfig(), startupRoleSelectorSelection.value)),
  );
  const startupRoleBridgeNoticeText = computed(() => {
    if (!startupRoleRequiresBridgeConfig.value) return "";
    if (startupRoleBridgeValidationMessage.value) {
      return startupRoleBridgeValidationMessage.value;
    }
    if (startupRoleCurrentHasBridgeConfig.value) {
      return "已检测到现有共享桥接配置，请确认后继续。";
    }
    if (String(startupRoleBridgeDraft.value?.root_dir || "").trim()) {
      return "共享目录已填写，确认后将自动启用共享桥接并加载对应角色页面。";
    }
    return "请先填写共享目录。节点名称会自动使用角色中文名，节点 ID 也会自动生成并长期固定。";
  });
  const startupRoleHasDraftChanges = computed(() =>
    isStartupBridgeDraftChanged(
      startupRoleDraftSourceConfig(),
      startupRoleBridgeDraft.value,
      startupRoleSelectorSelection.value,
    ),
  );
  const startupRoleHasRelevantDraftChanges = computed(() =>
    startupRoleRequiresBridgeConfig.value && startupRoleHasDraftChanges.value,
  );
  const startupRoleWillSaveChanges = computed(() => {
    const targetRole = normalizeDeploymentRoleMode(startupRoleSelectorSelection.value);
    return targetRole !== startupRoleCurrentMode.value || startupRoleHasRelevantDraftChanges.value;
  });
  const startupRoleActionButtonText = computed(() =>
    startupRoleSelectorBusy.value
      ? "处理中..."
      : startupRoleWillSaveChanges.value
        ? "保存并加载"
        : "按此角色进入",
  );
  const startupRoleConfirmDisabled = computed(() =>
    Boolean(startupRoleSelectorBusy.value || startupRoleBridgeValidationMessage.value),
  );
  const startupRoleGateReady = computed(() =>
    Boolean(bootstrapReady.value && String(health.startup_time || "").trim()),
  );
  const startupRoleGateVisible = computed(() => false);
  const shouldRenderAppShell = computed(() => true);
  const deploymentNodeIdDisplayText = computed(() =>
    buildRoleNodeIdPreview(
      String(health.deployment?.node_id || "").trim(),
      deploymentRoleMode.value,
      configRoleMode.value,
    ),
  );
  const deploymentNodeIdDisplayHint = computed(() =>
    deploymentNodeIdDisplayText.value === "切换后自动生成并长期固定"
      ? "保存后会按当前机器自动生成并长期固定。"
      : configRoleMode.value === deploymentRoleMode.value
        ? "当前生效节点 ID"
        : "保存后将使用该节点 ID"
  );

  return {
    effectiveRoleMode,
    deploymentRoleMode,
    configRoleMode,
    showCommonPathsConfigTab,
    showCommonSchedulerConfigTab,
    showNotifyConfigTab,
    showFeishuAuthConfigTab,
    showConsoleConfigTab,
    showFeatureMonthlyConfigTab,
    showFeatureHandoverConfigTab,
    showFeatureDayMetricUploadConfigTab,
    showFeatureWetBulbCollectionConfigTab,
    showFeatureAlarmExportConfigTab,
    showSheetImportConfigTab,
    showManualFeatureConfigTab,
    showRuntimeNetworkPanel,
    showDashboardPageNav,
    appShellTitle,
    statusNavLabel,
    dashboardNavLabel,
    configNavLabel,
    configShellTitle,
    configShellDescription,
    configReturnButtonText,
    statusHeroTitle,
    statusHeroDescription,
    bridgeExecutionHint,
    externalExecutionHint,
    resumeExecutionHint,
    startupRoleCurrentMode,
    startupRoleCurrentToken,
    startupRoleCurrentNodeId,
    startupRoleCurrentLabel,
    startupRoleSelectedLabel,
    startupRoleNodeIdDisplayText,
    startupRoleNodeIdDisplayHint,
    startupRoleRequiresBridgeConfig,
    startupRoleBridgeValidationMessage,
    startupRoleCurrentHasBridgeConfig,
    startupRoleBridgeNoticeText,
    startupRoleHasDraftChanges,
    startupRoleHasRelevantDraftChanges,
    startupRoleWillSaveChanges,
    startupRoleActionButtonText,
    startupRoleConfirmDisabled,
    startupRoleGateReady,
    startupRoleGateVisible,
    shouldRenderAppShell,
    deploymentNodeIdDisplayText,
    deploymentNodeIdDisplayHint,
  };
}
