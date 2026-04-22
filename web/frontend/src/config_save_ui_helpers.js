export function createConfigSaveUiHelpers(options = {}) {
  const {
    computed,
    clone,
    config,
    configLoaded,
    currentView,
    activeConfigTab,
    handoverConfigBuilding,
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
    for (const building of ["A楼", "B楼", "C楼", "D楼", "E楼"]) {
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

  function buildHandoverConfigDraftSignature(building = handoverConfigBuilding?.value) {
    const buildingText = String(building || "").trim() || "A楼";
    return JSON.stringify({
      common: serializeCurrentHandoverCommonDraft(),
      building: buildingText,
      buildingDraft: serializeCurrentHandoverBuildingDraft(buildingText),
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

  function syncSavedHandoverBuildingSignature(building = handoverConfigBuilding?.value) {
    const buildingText = String(building || "").trim() || "A楼";
    lastSavedHandoverBuildingSignatures[buildingText] = serializeCurrentHandoverBuildingDraft(buildingText);
  }

  function hasPendingHandoverConfigChanges(building = handoverConfigBuilding?.value) {
    const currentBuilding = String(building || "").trim() || "A楼";
    const currentCommonSignature = serializeCurrentHandoverCommonDraft();
    const currentBuildingSignature = serializeCurrentHandoverBuildingDraft(currentBuilding);
    const savedBuildingSignature = String(lastSavedHandoverBuildingSignatures[currentBuilding] || "");
    return currentCommonSignature !== lastSavedHandoverCommonSignature
      || currentBuildingSignature !== savedBuildingSignature;
  }

  async function savePendingHandoverConfigChanges(options = {}) {
    if (!config?.value) return null;
    if (!configLoaded?.value) return null;
    if (!options?.force && currentView?.value !== "config") return null;
    if (!options?.force && String(activeConfigTab?.value || "").trim() !== "feature_handover") return null;
    if ((configSaveSuspendDepth?.value || 0) > 0) return null;
    const currentBuilding = String(handoverConfigBuilding?.value || "").trim() || "A楼";
    const currentCommonSignature = serializeCurrentHandoverCommonDraft();
    const currentBuildingSignature = serializeCurrentHandoverBuildingDraft(currentBuilding);
    const commonDirty = currentCommonSignature !== lastSavedHandoverCommonSignature;
    const buildingDirty =
      currentBuildingSignature !== String(lastSavedHandoverBuildingSignatures[currentBuilding] || "");
    if (!commonDirty && !buildingDirty) {
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
    if (buildingDirty) {
      saveTasks.push(
        Promise.resolve(saveHandoverBuildingConfig?.(currentBuilding, {
          silentSuccess: true,
          silentConflictMessage: false,
          silentErrorMessage: true,
          skipConfigRefresh: true,
        }))
          .then((result) => ({ target: "building", result }))
          .catch((err) => ({
            target: "building",
            result: { saved: false, reason: "error", error: String(err || "") },
          })),
      );
    }
    const saveResults = await Promise.all(saveTasks);
    if (saveResults.some((item) => item.target === "common" && item.result?.saved)) {
      syncSavedHandoverCommonSignature();
    }
    if (saveResults.some((item) => item.target === "building" && item.result?.saved)) {
      syncSavedHandoverBuildingSignature(currentBuilding);
    }
    const failedResult = saveResults.find((item) => !item.result?.saved);
    if (failedResult) {
      return failedResult.result || {
        saved: false,
        reason: "missing_save_result",
        target: failedResult.target,
      };
    }
    if (!options?.silentSuccess && message) {
      message.value = "交接班配置已保存";
    }
    return {
      saved: true,
      reason: "saved",
      commonDirty,
      buildingDirty,
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
    if (hasPendingHandoverConfigChanges(targetBuilding)) {
      if (message) message.value = "当前交接班配置有未保存修改，请先点击保存配置";
      return {
        accepted: false,
        reason: "pending_manual_save",
        error: "当前交接班配置有未保存修改，请先点击保存配置",
      };
    }
    if (message) message.value = `${targetBuilding}审核链接测试发送中...`;
    return sendHandoverReviewLinkAction?.(targetBuilding, options);
  }

  async function onHandoverConfigBuildingChange(nextBuilding) {
    const targetBuilding = String(nextBuilding || "").trim() || String(handoverConfigBuilding?.value || "").trim() || "A楼";
    if (hasPendingHandoverConfigChanges(handoverConfigBuilding?.value)) {
      if (message) message.value = "当前交接班配置有未保存修改，请先点击保存配置";
      return;
    }
    await fetchHandoverBuildingConfigSegment?.(targetBuilding);
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
    runSchedulerConfigQuickSave,
  };
}
