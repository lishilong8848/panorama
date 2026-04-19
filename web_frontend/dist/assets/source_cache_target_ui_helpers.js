import { buildNeutralTargetDisplay, mapBackendTargetDisplay } from "./backend_target_display_helpers.js";

export function createSourceCacheTargetUiHelpers(options = {}) {
  const {
    computed,
    health,
    config,
    sharedSourceCacheReadinessOverview,
    externalAlarmUploadBuilding,
    isActionLocked,
    actionKeySourceCacheRefreshCurrentHour = "",
    actionKeySourceCacheRefreshAlarmManual = "",
    actionKeySourceCacheDeleteAlarmManual = "",
    actionKeySourceCacheUploadAlarmFull = "",
    actionKeySourceCacheUploadAlarmBuildingPrefix = "",
    monthlyEventReportLastRun,
    monthlyChangeReportLastRun,
    monthlyEventReportDeliveryStatus,
    monthlyChangeReportDeliveryStatus,
    monthlyEventReportRecipientStatusByBuilding,
    monthlyChangeReportRecipientStatusByBuilding,
    uploadAlarmSourceCacheFull,
    uploadAlarmSourceCacheBuilding,
  } = options;

  const isSourceCacheRefreshCurrentHourLocked = computed(() => isActionLocked(actionKeySourceCacheRefreshCurrentHour));
  const isSourceCacheRefreshAlarmManualLocked = computed(() => isActionLocked(actionKeySourceCacheRefreshAlarmManual));
  const isSourceCacheDeleteAlarmManualLocked = computed(() => isActionLocked(actionKeySourceCacheDeleteAlarmManual));

  const externalAlarmReadinessFamily = computed(() => {
    const families = Array.isArray(sharedSourceCacheReadinessOverview.value?.families)
      ? sharedSourceCacheReadinessOverview.value.families
      : [];
    return (
      families.find((item) => String(item?.key || "").trim() === "alarm_event_family") || {
        key: "alarm_event_family",
        title: "告警信息源文件",
        tone: "neutral",
        statusText: "暂无状态",
        summaryText: "当前还没有告警文件状态。",
        detailText: "",
        metaLines: [],
        items: [],
        buildings: [],
        uploadLastRunAt: "",
        uploadLastSuccessAt: "",
        uploadLastError: "",
        uploadRecordCount: 0,
        uploadFileCount: 0,
        uploadRunning: false,
        uploadStartedAt: "",
        uploadCurrentMode: "",
        uploadCurrentScope: "",
        uploadRunningText: "",
      }
    );
  });

  const isAlarmSourceCacheUploadRunning = computed(() => Boolean(externalAlarmReadinessFamily.value?.uploadRunning));
  const isSourceCacheUploadAlarmFullLocked = computed(() =>
    isAlarmSourceCacheUploadRunning.value || isActionLocked(actionKeySourceCacheUploadAlarmFull),
  );
  const isSourceCacheUploadAlarmBuildingLocked = computed(() =>
    isAlarmSourceCacheUploadRunning.value ||
    isActionLocked(`${String(actionKeySourceCacheUploadAlarmBuildingPrefix || "").trim()}${String(externalAlarmUploadBuilding.value || "").trim()}`),
  );
  const isSourceCacheUploadAlarmSelectedLocked = computed(() => {
    const buildingText = String(externalAlarmUploadBuilding.value || "").trim();
    if (buildingText === "全部楼栋") {
      return isSourceCacheUploadAlarmFullLocked.value;
    }
    return isAlarmSourceCacheUploadRunning.value
      || isActionLocked(`${String(actionKeySourceCacheUploadAlarmBuildingPrefix || "").trim()}${buildingText}`);
  });

  const currentHourRefreshButtonText = computed(() =>
    isSourceCacheRefreshCurrentHourLocked.value ? "下载中..." : "立即下载当前小时全部文件",
  );
  const manualAlarmRefreshButtonText = computed(() =>
    isSourceCacheRefreshAlarmManualLocked.value ? "拉取中..." : "一键拉取告警文件",
  );
  const manualAlarmDeleteButtonText = computed(() =>
    isSourceCacheDeleteAlarmManualLocked.value ? "删除中..." : "删除手动告警文件",
  );
  const externalAlarmUploadActionButtonText = computed(() => {
    if (isAlarmSourceCacheUploadRunning.value) return "上传进行中...";
    const buildingText = String(externalAlarmUploadBuilding.value || "").trim();
    if (buildingText === "全部楼栋") {
      return isActionLocked(actionKeySourceCacheUploadAlarmFull) ? "上传中..." : "使用共享文件上传60天";
    }
    return isActionLocked(`${String(actionKeySourceCacheUploadAlarmBuildingPrefix || "").trim()}${buildingText}`)
      ? "上传中..."
      : "使用共享文件上传60天";
  });

  async function uploadSelectedAlarmSourceCache() {
    const buildingText = String(externalAlarmUploadBuilding.value || "").trim();
    if (!buildingText || buildingText === "全部楼栋") {
      return uploadAlarmSourceCacheFull();
    }
    return uploadAlarmSourceCacheBuilding(buildingText);
  }

  const monthlyEventReportOutputDir = computed(() =>
    String(config.value?.handover_log?.monthly_event_report?.template?.output_dir || "").trim()
    || String(monthlyEventReportLastRun.value?.output_dir || "").trim()
    || "-"
  );
  const monthlyChangeReportOutputDir = computed(() =>
    String(config.value?.handover_log?.monthly_change_report?.template?.output_dir || "").trim()
    || String(monthlyChangeReportLastRun.value?.output_dir || "").trim()
    || "-"
  );
  const monthlyEventReportSendReadyCount = computed(() =>
    Number.isFinite(Number(monthlyEventReportDeliveryStatus.value?.sendReadyCount))
      ? Number(monthlyEventReportDeliveryStatus.value?.sendReadyCount)
      : monthlyEventReportRecipientStatusByBuilding.value.filter((item) => item.sendReady).length,
  );
  const monthlyChangeReportSendReadyCount = computed(() =>
    Number.isFinite(Number(monthlyChangeReportDeliveryStatus.value?.sendReadyCount))
      ? Number(monthlyChangeReportDeliveryStatus.value?.sendReadyCount)
      : monthlyChangeReportRecipientStatusByBuilding.value.filter((item) => item.sendReady).length,
  );

  const handoverEngineerDirectoryTarget = computed(() => {
    const backendDisplay = mapBackendTargetDisplay(health.handover?.engineer_directory?.target_display);
    if (backendDisplay) return backendDisplay;
    return buildNeutralTargetDisplay();
  });
  const alarmEventUploadTarget = computed(() => {
    const backendDisplay = mapBackendTargetDisplay(health.alarm_event_upload?.target_display);
    if (backendDisplay) return backendDisplay;
    return buildNeutralTargetDisplay();
  });
  const dayMetricUploadTarget = computed(() => {
    const backendDisplay = mapBackendTargetDisplay(health.day_metric_upload?.target_display);
    if (backendDisplay) return backendDisplay;
    return buildNeutralTargetDisplay();
  });

  return {
    isSourceCacheRefreshCurrentHourLocked,
    isSourceCacheRefreshAlarmManualLocked,
    isSourceCacheDeleteAlarmManualLocked,
    externalAlarmReadinessFamily,
    isAlarmSourceCacheUploadRunning,
    isSourceCacheUploadAlarmFullLocked,
    isSourceCacheUploadAlarmBuildingLocked,
    isSourceCacheUploadAlarmSelectedLocked,
    currentHourRefreshButtonText,
    manualAlarmRefreshButtonText,
    manualAlarmDeleteButtonText,
    externalAlarmUploadActionButtonText,
    uploadSelectedAlarmSourceCache,
    monthlyEventReportOutputDir,
    monthlyChangeReportOutputDir,
    monthlyEventReportSendReadyCount,
    monthlyChangeReportSendReadyCount,
    handoverEngineerDirectoryTarget,
    alarmEventUploadTarget,
    dayMetricUploadTarget,
  };
}
