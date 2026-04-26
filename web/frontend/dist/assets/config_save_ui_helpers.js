export function createConfigSaveUiHelpers(options = {}) {
  const {
    computed,
    clone,
    config,
    configLoaded,
    currentView,
    activeConfigTab,
    handoverConfigBuilding,
    handoverConfigBuildingRevision,
    configSaveStatus,
    configSaveSuspendDepth,
    message,
    isActionLocked,
    getPreparedConfigPayloadState,
    saveConfig,
    saveHandoverCommonConfig,
    saveHandoverBuildingConfig,
    fetchHandoverBuildingConfigSegment,
    sendHandoverReviewLinkAction,
    actionKeyConfigSave,
    actionKeyHandoverConfigCommonSave,
    actionKeyHandoverConfigBuildingSave,
  } = options || {};

  let lastSavedHandoverCommonSignature = "";
  const lastSavedHandoverBuildingSignatures = Object.create(null);
  const lastSavedHandoverBuildingMetaSignatures = Object.create(null);
  const lastSavedHandoverReviewRecipientSignatures = Object.create(null);
  const handoverBuildingRevisionByBuilding = Object.create(null);
  const skipNextHandoverBuildingSignatureSync = Object.create(null);

  const HANDOVER_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"];

  function updateConfigSaveStatus(patch = {}) {
    if (!configSaveStatus || typeof configSaveStatus !== "object") return;
    Object.assign(configSaveStatus, patch);
  }

  function currentConfigSaveTimestamp() {
    try {
      return new Date().toLocaleString("zh-CN", { hour12: false });
    } catch (_err) {
      return new Date().toISOString();
    }
  }

  function markConfigDraftDirty() {
    if (!configLoaded?.value) return;
    if ((configSaveSuspendDepth?.value || 0) > 0) return;
    updateConfigSaveStatus({
      mode: String(configSaveStatus.mode || "").trim() === "saving" ? "saving" : "idle",
      draft_dirty: true,
    });
  }

  function serializeCurrentHandoverCommonDraft() {
    const handover = config?.value?.handover_log && typeof config.value.handover_log === "object"
      ? clone(config.value.handover_log)
      : {};
    handover.cell_rules = handover.cell_rules && typeof handover.cell_rules === "object" ? handover.cell_rules : {};
    handover.cloud_sheet_sync = handover.cloud_sheet_sync && typeof handover.cloud_sheet_sync === "object" ? handover.cloud_sheet_sync : {};
    handover.review_ui = handover.review_ui && typeof handover.review_ui === "object" ? handover.review_ui : {};
    handover.cell_rules.building_rows = handover.cell_rules.building_rows && typeof handover.cell_rules.building_rows === "object"
      ? handover.cell_rules.building_rows
      : {};
    handover.cloud_sheet_sync.sheet_names = handover.cloud_sheet_sync.sheet_names && typeof handover.cloud_sheet_sync.sheet_names === "object"
      ? handover.cloud_sheet_sync.sheet_names
      : {};
    handover.review_ui.cabinet_power_defaults_by_building =
      handover.review_ui.cabinet_power_defaults_by_building && typeof handover.review_ui.cabinet_power_defaults_by_building === "object"
        ? handover.review_ui.cabinet_power_defaults_by_building
        : {};
    handover.review_ui.footer_inventory_defaults_by_building =
      handover.review_ui.footer_inventory_defaults_by_building && typeof handover.review_ui.footer_inventory_defaults_by_building === "object"
        ? handover.review_ui.footer_inventory_defaults_by_building
        : {};
    handover.review_ui.review_link_recipients_by_building =
      handover.review_ui.review_link_recipients_by_building && typeof handover.review_ui.review_link_recipients_by_building === "object"
        ? handover.review_ui.review_link_recipients_by_building
        : {};
    for (const building of HANDOVER_BUILDINGS) {
      delete handover.cell_rules.building_rows[building];
      delete handover.cloud_sheet_sync.sheet_names[building];
      delete handover.review_ui.cabinet_power_defaults_by_building[building];
      delete handover.review_ui.footer_inventory_defaults_by_building[building];
      delete handover.review_ui.review_link_recipients_by_building[building];
    }
    return JSON.stringify(handover);
  }

  function serializeCurrentHandoverBuildingDraft(building = handoverConfigBuilding?.value) {
    const buildingText = String(building || "").trim() || "A楼";
    const handover = config?.value?.handover_log && typeof config.value.handover_log === "object"
      ? config.value.handover_log
      : {};
    const cellRules = handover.cell_rules && typeof handover.cell_rules === "object" ? handover.cell_rules : {};
    const cloudSheetSync = handover.cloud_sheet_sync && typeof handover.cloud_sheet_sync === "object" ? handover.cloud_sheet_sync : {};
    const reviewUi = handover.review_ui && typeof handover.review_ui === "object" ? handover.review_ui : {};
    return JSON.stringify({
      building: buildingText,
      building_rows: cellRules.building_rows?.[buildingText] || [],
      sheet_name: String(cloudSheetSync.sheet_names?.[buildingText] || "").trim(),
      cabinet_defaults: reviewUi.cabinet_power_defaults_by_building?.[buildingText] || null,
      footer_defaults: reviewUi.footer_inventory_defaults_by_building?.[buildingText] || null,
      review_link_recipients: reviewUi.review_link_recipients_by_building?.[buildingText] || [],
    });
  }

  function serializeCurrentHandoverBuildingMetaDraft(building = handoverConfigBuilding?.value) {
    const buildingText = String(building || "").trim() || "A楼";
    const handover = config?.value?.handover_log && typeof config.value.handover_log === "object"
      ? config.value.handover_log
      : {};
    const cellRules = handover.cell_rules && typeof handover.cell_rules === "object" ? handover.cell_rules : {};
    const cloudSheetSync = handover.cloud_sheet_sync && typeof handover.cloud_sheet_sync === "object" ? handover.cloud_sheet_sync : {};
    const reviewUi = handover.review_ui && typeof handover.review_ui === "object" ? handover.review_ui : {};
    return JSON.stringify({
      building: buildingText,
      building_rows: cellRules.building_rows?.[buildingText] || [],
      sheet_name: String(cloudSheetSync.sheet_names?.[buildingText] || "").trim(),
      cabinet_defaults: reviewUi.cabinet_power_defaults_by_building?.[buildingText] || null,
      footer_defaults: reviewUi.footer_inventory_defaults_by_building?.[buildingText] || null,
    });
  }

  function serializeCurrentHandoverReviewRecipientDraft(building = handoverConfigBuilding?.value) {
    const buildingText = String(building || "").trim() || "A楼";
    const handover = config?.value?.handover_log && typeof config.value.handover_log === "object"
      ? config.value.handover_log
      : {};
    const reviewUi = handover.review_ui && typeof handover.review_ui === "object" ? handover.review_ui : {};
    return JSON.stringify({
      building: buildingText,
      review_link_recipients: reviewUi.review_link_recipients_by_building?.[buildingText] || [],
    });
  }

  function serializeAllHandoverReviewRecipientDrafts() {
    const payload = {};
    for (const building of HANDOVER_BUILDINGS) {
      payload[building] = serializeCurrentHandoverReviewRecipientDraft(building);
    }
    return JSON.stringify(payload);
  }

  function buildHandoverConfigDraftSignature(building = handoverConfigBuilding?.value) {
    const buildingText = String(building || "").trim() || "A楼";
    return JSON.stringify({
      common: serializeCurrentHandoverCommonDraft(),
      building: buildingText,
      buildingDraft: serializeCurrentHandoverBuildingDraft(buildingText),
      reviewRecipientDrafts: serializeAllHandoverReviewRecipientDrafts(),
    });
  }

  function buildCurrentConfigDraftSignature() {
    if (currentView?.value === "config" && String(activeConfigTab?.value || "").trim() === "feature_handover") {
      return buildHandoverConfigDraftSignature();
    }
    const payloadState = typeof getPreparedConfigPayloadState === "function"
      ? getPreparedConfigPayloadState()
      : null;
    if (!payloadState?.ok) return "";
    return String(payloadState.signature || "");
  }

  function syncConfigSaveSavedSignature(signature = "") {
    const normalizedSignature = String(signature || "");
    updateConfigSaveStatus({
      saved_signature: normalizedSignature,
      draft_dirty: false,
    });
  }

  function markCurrentConfigDraftDirty() {
    if (!config?.value) return;
    if (String(currentView?.value || "").trim() !== "config") return;
    markConfigDraftDirty();
  }

  function syncSavedHandoverCommonSignature() {
    lastSavedHandoverCommonSignature = serializeCurrentHandoverCommonDraft();
  }

  function syncSavedHandoverBuildingSignature(building = handoverConfigBuilding?.value, options = {}) {
    const buildingText = String(building || "").trim() || "A楼";
    const allowSkip = options?.allowSkip !== false;
    if (allowSkip && skipNextHandoverBuildingSignatureSync[buildingText]) {
      delete skipNextHandoverBuildingSignatureSync[buildingText];
      return;
    }
    lastSavedHandoverBuildingSignatures[buildingText] = serializeCurrentHandoverBuildingDraft(buildingText);
    lastSavedHandoverBuildingMetaSignatures[buildingText] = serializeCurrentHandoverBuildingMetaDraft(buildingText);
    lastSavedHandoverReviewRecipientSignatures[buildingText] = serializeCurrentHandoverReviewRecipientDraft(buildingText);
    if (String(handoverConfigBuilding?.value || "").trim() === buildingText && handoverConfigBuildingRevision) {
      handoverBuildingRevisionByBuilding[buildingText] =
        Number.parseInt(String(handoverConfigBuildingRevision.value || 0), 10) || 0;
    }
  }

  function hasPendingHandoverCommonChanges() {
    return serializeCurrentHandoverCommonDraft() !== lastSavedHandoverCommonSignature;
  }

  function hasPendingHandoverBuildingMetaChanges(building = handoverConfigBuilding?.value) {
    const buildingText = String(building || "").trim() || "A楼";
    if (!Object.prototype.hasOwnProperty.call(lastSavedHandoverBuildingMetaSignatures, buildingText)) {
      return false;
    }
    return serializeCurrentHandoverBuildingMetaDraft(buildingText)
      !== String(lastSavedHandoverBuildingMetaSignatures[buildingText] || "");
  }

  function hasPendingHandoverReviewRecipientChanges(building = handoverConfigBuilding?.value) {
    const buildingText = String(building || "").trim() || "A楼";
    if (!Object.prototype.hasOwnProperty.call(lastSavedHandoverReviewRecipientSignatures, buildingText)) {
      return false;
    }
    return serializeCurrentHandoverReviewRecipientDraft(buildingText)
      !== String(lastSavedHandoverReviewRecipientSignatures[buildingText] || "");
  }

  function collectDirtyHandoverReviewRecipientBuildings() {
    return HANDOVER_BUILDINGS.filter((building) => hasPendingHandoverReviewRecipientChanges(building));
  }

  function hasPendingHandoverConfigChanges(building = handoverConfigBuilding?.value) {
    const currentBuilding = String(building || "").trim() || "A楼";
    return hasPendingHandoverCommonChanges()
      || hasPendingHandoverBuildingMetaChanges(currentBuilding)
      || hasPendingHandoverReviewRecipientChanges(currentBuilding);
  }

  async function savePendingHandoverConfigChanges(options = {}) {
    if (!config?.value) return null;
    if (!configLoaded?.value) return null;
    if (!options?.force && currentView?.value !== "config") return null;
    if (!options?.force && String(activeConfigTab?.value || "").trim() !== "feature_handover") return null;
    if ((configSaveSuspendDepth?.value || 0) > 0) return null;
    const currentBuilding = String(handoverConfigBuilding?.value || "").trim() || "A楼";
    const commonDirty = hasPendingHandoverCommonChanges();
    const buildingMetaDirty = hasPendingHandoverBuildingMetaChanges(currentBuilding);
    const dirtyRecipientBuildings = collectDirtyHandoverReviewRecipientBuildings();
    const buildingSaveSet = new Set(dirtyRecipientBuildings);
    if (buildingMetaDirty) {
      buildingSaveSet.add(currentBuilding);
    }
    if (!commonDirty && !buildingSaveSet.size) {
      return { saved: true, reason: "unchanged" };
    }
    const saveTasks = [];
    if (commonDirty) {
      saveTasks.push(
        Promise.resolve(saveHandoverCommonConfig?.({
          silentSuccess: true,
          silentConflictMessage: false,
          silentErrorMessage: true,
          skipConfigRefresh: true,
        }))
          .then((result) => ({ target: "common", result }))
          .catch((err) => ({
            target: "common",
            result: { saved: false, reason: "error", error: String(err || "") },
          })),
      );
    }
    for (const building of buildingSaveSet) {
      const baseRevision = Object.prototype.hasOwnProperty.call(handoverBuildingRevisionByBuilding, building)
        ? handoverBuildingRevisionByBuilding[building]
        : (building === currentBuilding
          ? Number.parseInt(String(handoverConfigBuildingRevision?.value || 0), 10) || 0
          : 0);
      saveTasks.push(
        Promise.resolve(saveHandoverBuildingConfig?.(building, {
          silentSuccess: true,
          silentConflictMessage: false,
          silentErrorMessage: true,
          skipConfigRefresh: true,
          preserveSelection: building !== currentBuilding,
          preserveDraftOnConflict: true,
          skipSingleFlight: true,
          baseRevision,
        }))
          .then((result) => ({ target: "building", building, result }))
          .catch((err) => ({
            target: "building",
            building,
            result: { saved: false, reason: "error", error: String(err || "") },
          })),
      );
    }
    const saveResults = await Promise.all(saveTasks);
    if (saveResults.some((item) => item.target === "common" && item.result?.saved)) {
      syncSavedHandoverCommonSignature();
    }
    for (const item of saveResults.filter((entry) => entry.target === "building" && entry.result?.saved)) {
      if (item.result?.data?.revision !== undefined) {
        handoverBuildingRevisionByBuilding[item.building] =
          Number.parseInt(String(item.result.data.revision || 0), 10) || 0;
      }
      syncSavedHandoverBuildingSignature(item.building, { allowSkip: false });
    }
    const failedResult = saveResults.find((item) => !item.result?.saved);
    if (failedResult) {
      const failedPayload = failedResult.result || {
        saved: false,
        reason: "missing_save_result",
        target: failedResult.target,
      };
      if (failedResult.building && failedPayload && typeof failedPayload === "object") {
        failedPayload.building = failedResult.building;
        failedPayload.error = `${failedResult.building}保存失败：${String(failedPayload.error || failedPayload.reason || "未知错误")}`;
      }
      return failedPayload;
    }
    if (!options?.silentSuccess && message) {
      message.value = "交接班配置已保存";
    }
    return {
      saved: true,
      reason: "saved",
      commonDirty,
      buildingDirty: Boolean(buildingSaveSet.size),
      buildingMetaDirty,
      recipientDirtyBuildings: dirtyRecipientBuildings,
      building: currentBuilding,
    };
  }

  async function savePreparedConfigDraft(options = {}) {
    if (options?.handoverOnly || (currentView?.value === "config" && String(activeConfigTab?.value || "").trim() === "feature_handover")) {
      const result = await savePendingHandoverConfigChanges({
        force: true,
        silentSuccess: Boolean(options?.silentSuccess),
      });
      if (result?.saved !== false) {
        syncConfigSaveSavedSignature(buildHandoverConfigDraftSignature(options?.building || handoverConfigBuilding?.value));
        updateConfigSaveStatus({
          mode: "idle",
          last_error: "",
          last_saved_at: result?.reason === "saved" ? currentConfigSaveTimestamp() : configSaveStatus.last_saved_at,
        });
      }
      return result;
    }
    const payloadState = typeof getPreparedConfigPayloadState === "function" ? getPreparedConfigPayloadState() : null;
    if (!payloadState?.ok) {
      updateConfigSaveStatus({
        mode: "error",
        last_error: String(payloadState?.error || "配置校验失败"),
      });
      if (message) message.value = payloadState?.error || "配置校验失败";
      return { saved: false, reason: "invalid", error: payloadState?.error || "配置校验失败" };
    }
    if (String(payloadState.signature || "") === String(configSaveStatus.saved_signature || "")) {
      updateConfigSaveStatus({ mode: "idle", last_error: "", draft_dirty: false });
      return { saved: true, reason: "unchanged", signature: payloadState.signature };
    }
    updateConfigSaveStatus({
      mode: "saving",
      last_error: "",
    });
    const result = await saveConfig?.();
    if (result?.saved) {
      syncConfigSaveSavedSignature(String(result.signature || payloadState.signature || ""));
      updateConfigSaveStatus({
        mode: "idle",
        last_error: "",
        last_saved_at: currentConfigSaveTimestamp(),
      });
    } else if (result?.saved === false) {
      updateConfigSaveStatus({
        mode: "error",
        last_error: String(result?.error || "保存失败"),
      });
    }
    return result;
  }

  const configSaveStateText = computed(() => {
    if (configSaveStatus.mode === "error") return "保存失败";
    if (configSaveStatus.mode === "saving") return "正在保存...";
    if (configSaveStatus.draft_dirty) return "未保存修改";
    if (configSaveStatus.last_saved_at) return "已保存";
    return "";
  });

  const configSaveStateDetail = computed(() => {
    if (configSaveStatus.mode === "error") {
      return String(configSaveStatus.last_error || "").trim();
    }
    if (configSaveStateText.value === "已保存") {
      return String(configSaveStatus.last_saved_at || "").trim();
    }
    return "";
  });

  const configSaveButtonLocked = computed(() => String(configSaveStatus.mode || "").trim() === "saving");

  const isConfigSaveLocked = computed(() => {
    if (String(activeConfigTab?.value || "").trim() === "feature_handover") {
      return configSaveButtonLocked.value
        || isActionLocked?.(actionKeyHandoverConfigCommonSave)
        || isActionLocked?.(actionKeyHandoverConfigBuildingSave);
    }
    return configSaveButtonLocked.value || isActionLocked?.(actionKeyConfigSave);
  });

  const configSaveButtonText = computed(() => (isConfigSaveLocked.value ? "保存中..." : "保存配置"));

  async function saveActiveConfig() {
    if (String(activeConfigTab?.value || "").trim() === "feature_handover") {
      const result = await savePreparedConfigDraft({
        handoverOnly: true,
        silentSuccess: false,
      });
      if (!result) {
        if (message) message.value = "当前没有可保存的交接班配置变更";
        return { saved: false, reason: "missing" };
      }
      if (result.saved === false) {
        return result;
      }
      if (result.reason === "unchanged" && message) {
        message.value = "交接班配置已是最新";
      }
      return result;
    }
    const result = await savePreparedConfigDraft();
    if (result?.reason === "unchanged" && message) {
      message.value = "配置已是最新";
    }
    return result;
  }

  async function sendHandoverReviewLink(building, options = {}) {
    const targetBuilding = String(building || "").trim() || String(handoverConfigBuilding?.value || "").trim() || "A楼";
    const shouldCheckPendingConfig =
      String(currentView?.value || "").trim() === "config"
      && String(activeConfigTab?.value || "").trim() === "feature_handover";
    if (shouldCheckPendingConfig && hasPendingHandoverConfigChanges(targetBuilding)) {
      if (message) message.value = "当前交接班配置有未保存修改，请先点击保存配置";
      return {
        accepted: false,
        reason: "pending_manual_save",
        error: "当前交接班配置有未保存修改，请先点击保存配置",
      };
    }
    if (typeof sendHandoverReviewLinkAction !== "function") {
      if (message) message.value = "审核链接发送入口未初始化，请刷新页面后重试";
      return {
        accepted: false,
        reason: "action_unavailable",
        error: "审核链接发送入口未初始化，请刷新页面后重试",
      };
    }
    if (message) message.value = `${targetBuilding}审核链接测试发送中...`;
    return sendHandoverReviewLinkAction(targetBuilding, options);
  }

  async function onHandoverConfigBuildingChange(nextBuilding) {
    const targetBuilding = String(nextBuilding || "").trim() || String(handoverConfigBuilding?.value || "").trim() || "A楼";
    if (hasPendingHandoverConfigChanges(handoverConfigBuilding?.value)) {
      if (message) message.value = "当前交接班配置有未保存修改，请先点击保存配置";
      return;
    }
    await fetchHandoverBuildingConfigSegment?.(targetBuilding);
  }

  async function onHandoverReviewRecipientBuildingChange(nextBuilding) {
    const currentBuilding = String(handoverConfigBuilding?.value || "").trim() || "A楼";
    const targetBuilding = String(nextBuilding || "").trim() || currentBuilding;
    if (targetBuilding === currentBuilding) return;
    if (hasPendingHandoverCommonChanges() || hasPendingHandoverBuildingMetaChanges(currentBuilding)) {
      if (message) message.value = "当前交接班配置有未保存修改，请先点击保存配置";
      return;
    }
    if (hasPendingHandoverReviewRecipientChanges(targetBuilding)) {
      skipNextHandoverBuildingSignatureSync[targetBuilding] = true;
      if (handoverConfigBuilding) {
        handoverConfigBuilding.value = targetBuilding;
      }
      setTimeout(() => {
        delete skipNextHandoverBuildingSignatureSync[targetBuilding];
      }, 0);
      if (handoverConfigBuildingRevision && Object.prototype.hasOwnProperty.call(handoverBuildingRevisionByBuilding, targetBuilding)) {
        handoverConfigBuildingRevision.value = handoverBuildingRevisionByBuilding[targetBuilding];
      }
      return;
    }
    const data = await fetchHandoverBuildingConfigSegment?.(targetBuilding);
    if (data?.revision !== undefined) {
      handoverBuildingRevisionByBuilding[targetBuilding] =
        Number.parseInt(String(data.revision || 0), 10) || 0;
      syncSavedHandoverBuildingSignature(targetBuilding, { allowSkip: false });
    }
  }

  async function runSchedulerConfigQuickSave(taskFn) {
    if (typeof taskFn !== "function") return;
    if (configSaveSuspendDepth) {
      configSaveSuspendDepth.value += 1;
    }
    try {
      return await taskFn();
    } finally {
      if (configSaveSuspendDepth) {
        configSaveSuspendDepth.value = Math.max(0, configSaveSuspendDepth.value - 1);
      }
    }
  }

  return {
    updateConfigSaveStatus,
    currentConfigSaveTimestamp,
    markConfigDraftDirty,
    buildCurrentConfigDraftSignature,
    buildHandoverConfigDraftSignature,
    syncConfigSaveSavedSignature,
    markCurrentConfigDraftDirty,
    serializeCurrentHandoverCommonDraft,
    serializeCurrentHandoverBuildingDraft,
    serializeCurrentHandoverBuildingMetaDraft,
    serializeCurrentHandoverReviewRecipientDraft,
    syncSavedHandoverCommonSignature,
    syncSavedHandoverBuildingSignature,
    hasPendingHandoverConfigChanges,
    savePendingHandoverConfigChanges,
    savePreparedConfigDraft,
    configSaveStateText,
    configSaveStateDetail,
    configSaveButtonLocked,
    isConfigSaveLocked,
    configSaveButtonText,
    saveActiveConfig,
    sendHandoverReviewLink,
    onHandoverConfigBuildingChange,
    onHandoverReviewRecipientBuildingChange,
    runSchedulerConfigQuickSave,
  };
}
