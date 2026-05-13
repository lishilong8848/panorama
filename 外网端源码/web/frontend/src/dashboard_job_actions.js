import {
  cancelJobApi,
  getJobApi,
  postHandoverFromFilesJob,
  postManualUploadJob,
  postSheetImportJob,
  retryJobApi,
  startJsonJobApi,
  submitBranchPowerFromDownloadJob,
  submitBranchPowerPowerAlertSyncJob,
  submitDayMetricFromDownloadJob,
  submitDayMetricFromFileJob,
  submitHandoverFollowupContinueJob,
  submitDayMetricRetryFailedJob,
  submitDayMetricRetryUnitJob,
} from "./api_client.js";
import { parseDateText } from "./config_helpers.js";

const ACTION_KEYS = {
  autoOnce: "job:auto_once",
  multiDate: "job:multi_date",
  manualUpload: "job:manual_upload",
  sheetImport: "job:sheet_import",
  handoverFromFile: "job:handover_from_file",
  handoverFromDownload: "job:handover_from_download",
  dayMetricFromDownload: "job:day_metric_from_download",
  branchPowerFromDownload: "job:branch_power_from_download",
  branchPowerPowerAlertSync: "job:branch_power_power_alert_sync",
  dayMetricFromFile: "job:day_metric_from_file",
  dayMetricRetryUnit: "job:day_metric_retry_unit",
  dayMetricRetryFailed: "job:day_metric_retry_failed",
  handoverFollowupContinue: "job:handover_followup_continue",
};

export function createDashboardJobActions(ctx) {
  const {
    canRun,
    message,
    currentJob,
    selectedJobId,
    selectedBridgeTaskId,
    bridgeTaskDetail,
    applyJobPanelSummary,
    patchJobPanelActionState,
    config,
    selectedDates,
    manualBuilding,
    manualFile,
    manualUploadDate,
    sheetFile,
    handoverFilesByBuilding,
    handoverConfiguredBuildings,
    handoverDutyDate,
    handoverDutyShift,
    handoverDownloadScope,
    handoverDutyAutoFollow,
    dayMetricUploadScope,
    dayMetricUploadBuilding,
    dayMetricSelectedDates,
    dayMetricLocalBuilding,
    dayMetricLocalDate,
    dayMetricLocalFile,
    branchPowerBusinessDate,
    streamController,
    fetchHealth,
    fetchJobs,
    fetchBridgeTasks,
    fetchBridgeTaskDetail,
    scheduleExternalDashboardRefresh,
    fetchExternalDashboardSummary,
    syncHandoverDutyFromNow,
    runSingleFlight,
  } = ctx;
async function guardedRun(actionKey, taskFn, options = {}) {
    if (typeof runSingleFlight === "function") {
      return runSingleFlight(actionKey, taskFn, {
        ...options,
        onCooldown: () => {
          message.value = "请求处理中，请稍候";
        },
      });
    }
    return taskFn();
  }

  function triggerDashboardRefresh(reason = "job_action", options = {}) {
    if (typeof scheduleExternalDashboardRefresh === "function") {
      scheduleExternalDashboardRefresh(reason, options);
      return;
    }
    if (typeof fetchExternalDashboardSummary === "function") {
      void fetchExternalDashboardSummary({ silentMessage: true, force: Boolean(options?.force) });
    }
  }

  async function startJobByJson(url, body, title, actionKey) {
if (!canRun.value) return;
    return guardedRun(
      actionKey || `job:${url}`,
      async () => {
        try {
          message.value = `${title} 已提交`;
          const response = await startJsonJobApi(url, body || {});
          await applyAcceptedExecutionResponse(response, title);
        } catch (err) {
          message.value = `${title} 提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function runAutoOnce() {
    await startJobByJson("/api/jobs/auto-once", {}, "立即执行自动流程", ACTION_KEYS.autoOnce);
  }

  async function runMultiDate() {
    if (!selectedDates.value.length) {
      message.value = "请先选择至少一个日期";
      return;
    }
    await startJobByJson(
      "/api/jobs/multi-date",
      { dates: selectedDates.value },
      "多日用电明细自动流程",
      ACTION_KEYS.multiDate,
    );
  }

  async function runManualUpload() {
    if (!manualBuilding.value) {
      message.value = "请选择楼栋";
      return;
    }
    if (!manualFile.value) {
      message.value = "请选择月报表格文件（xlsx）";
      return;
    }
    const uploadDate = String(manualUploadDate.value || "").trim();
    if (!parseDateText(uploadDate)) {
      message.value = "上传日期格式错误，请使用 YYYY-MM-DD";
      return;
    }
    if (!canRun.value) return;

    const form = new FormData();
    form.append("building", manualBuilding.value);
    form.append("upload_date", uploadDate);
    form.append("file", manualFile.value);

    return guardedRun(
      ACTION_KEYS.manualUpload,
      async () => {
        try {
          const job = await postManualUploadJob(form);
          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          streamController.attachJobStream(job.job_id);
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = `手动补传提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function runSheetImport() {
    if (!sheetFile.value) {
      message.value = "请选择表格文件";
      return;
    }
    if (!canRun.value) return;

    const form = new FormData();
    form.append("file", sheetFile.value);

    return guardedRun(
      ACTION_KEYS.sheetImport,
      async () => {
        try {
          const job = await postSheetImportJob(form);
          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          streamController.attachJobStream(job.job_id);
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = `5Sheet 导表提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function fetchJob(jobId) {
    const targetJobId = String(jobId || "").trim();
    if (!targetJobId) return;
    try {
      const data = await getJobApi(targetJobId);
      currentJob.value = data;
      if (selectedJobId) {
        selectedJobId.value = String(data?.job_id || targetJobId || "").trim();
      }
      const status = String(data?.status || "").trim().toLowerCase();
      if (status === "success") {
        message.value = "任务执行成功";
      } else if (status === "failed") {
        message.value = `任务失败: ${data.error || data.summary}`;
      } else if (status === "cancelled") {
        message.value = "任务已取消";
      } else if (status === "interrupted") {
        message.value = `任务已中断: ${data.error || data.summary || "请重试"}`;
      } else if (status === "partial_failed") {
        message.value = `任务部分失败: ${data.error || data.summary || "请查看日志"}`;
      }
    } catch (err) {
      message.value = `任务状态读取失败: ${err}`;
    }
  }

  function getJobCancelActionKey(jobId) {
    return `job:cancel:${String(jobId || "").trim()}`;
  }

  function getJobRetryActionKey(jobId) {
    return `job:retry:${String(jobId || "").trim()}`;
  }

  async function cancelCurrentJob(targetJobId = "") {
    const jobId = String(targetJobId || currentJob.value?.job_id || selectedJobId?.value || "").trim();
    if (!jobId) {
      message.value = "等待后端返回任务动作能力";
      return;
    }
    return guardedRun(
      getJobCancelActionKey(jobId),
      async () => {
        try {
          if (typeof patchJobPanelActionState === "function") {
            patchJobPanelActionState(jobId, "cancel", {
              allowed: false,
              pending: true,
              label: "取消中...",
              disabled_reason: "取消请求已提交",
            });
          }
          const data = await cancelJobApi(jobId);
          if (data?.job && typeof data.job === "object" && currentJob?.value) {
            const currentSelectedJobId = String(selectedJobId?.value || currentJob.value?.job_id || "").trim();
            if (!currentSelectedJobId || currentSelectedJobId === jobId) {
              currentJob.value = { ...(currentJob.value || {}), ...data.job };
            }
          }
          let summaryApplied = false;
          if (typeof applyJobPanelSummary === "function" && data?.job_panel_summary && typeof data.job_panel_summary === "object") {
            applyJobPanelSummary(data.job_panel_summary);
            summaryApplied = true;
          }
          triggerDashboardRefresh("job_cancel", { force: true, delayMs: 0 });
          if (!summaryApplied && typeof fetchJobs === "function") {
            void fetchJobs({ silentMessage: true });
          }
          const bridgeCancelError = String(data?.bridge_cancel_error || data?.bridgeCancelError || "").trim();
          message.value = bridgeCancelError || "任务取消请求已提交";
        } catch (err) {
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
          message.value = `任务取消失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function applyAcceptedExecutionResponse(response, title) {
    const hasBridgeTask = Boolean(response?.bridge_task && typeof response.bridge_task === "object");
    const wrappedJob =
      response
      && typeof response === "object"
      && response.accepted
      && response.job
      && typeof response.job === "object"
        ? response.job
        : response;
    const isBridgeJob = hasBridgeTask || String(wrappedJob?.kind || "").trim().toLowerCase() === "bridge";
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
        bridgeTaskDetail.value = response?.bridge_task && typeof response.bridge_task === "object" ? { ...response.bridge_task } : null;
      }
      if (typeof fetchBridgeTasks === "function") {
        await fetchBridgeTasks({ silentMessage: true });
      }
      if (bridgeTaskId && typeof fetchBridgeTaskDetail === "function") {
        await fetchBridgeTaskDetail(bridgeTaskId, { silentMessage: true });
      }
      message.value = `${title} 已进入共享补采处理`;
      return;
    }

    const job = wrappedJob;
    currentJob.value = job;
    if (selectedJobId) {
      selectedJobId.value = String(job?.job_id || "").trim();
    }
    if (job?.job_id) {
      streamController.attachJobStream(job.job_id);
    }
    if (typeof fetchJobs === "function") {
      await fetchJobs({ silentMessage: true });
    }
    triggerDashboardRefresh("job_accepted");
    if (isWaitingSharedBridge) {
      message.value = `${title} 已进入等待采集端补采，共享文件到位后会自动继续`;
    }
  }

  async function retryCurrentJob() {
    const jobId = String(currentJob.value?.job_id || selectedJobId?.value || "").trim();
    if (!jobId) {
      message.value = "等待后端返回任务动作能力";
      return;
    }
    return guardedRun(
      getJobRetryActionKey(jobId),
      async () => {
        try {
          if (typeof patchJobPanelActionState === "function") {
            patchJobPanelActionState(jobId, "retry", {
              allowed: false,
              pending: true,
              label: "重试中...",
              disabled_reason: "重试请求已提交",
            });
          }
          message.value = "任务重试已提交";
          const response = await retryJobApi(jobId);
          const job = response?.job && typeof response.job === "object" ? response.job : response;
          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          streamController.attachJobStream(job.job_id);
          let summaryApplied = false;
          if (typeof applyJobPanelSummary === "function" && response?.job_panel_summary && typeof response.job_panel_summary === "object") {
            applyJobPanelSummary(response.job_panel_summary);
            summaryApplied = true;
          }
          if (!summaryApplied && typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
          triggerDashboardRefresh("job_retry");
        } catch (err) {
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
          message.value = `任务重试失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function runHandoverFromFile() {
    const selectedEntries = (handoverConfiguredBuildings.value || [])
      .map((building) => [building, handoverFilesByBuilding[building]])
      .filter(([building, file]) => String(building || "").trim() && file);
    if (!selectedEntries.length) {
      message.value = "请先为至少一个楼选择已有数据表文件";
      return;
    }
    if (!canRun.value) return;
    if (handoverDutyAutoFollow.value) {
      syncHandoverDutyFromNow(true);
    }

    const dutyDate = String(handoverDutyDate.value || "").trim();
    const dutyShift = String(handoverDutyShift.value || "").trim().toLowerCase();
    if (!parseDateText(dutyDate)) {
      message.value = "交接班日期格式错误，请使用 YYYY-MM-DD";
      return;
    }
    if (!["day", "night"].includes(dutyShift)) {
      message.value = "班次仅支持白班/夜班";
      return;
    }

    const form = new FormData();
    form.append("duty_date", dutyDate);
    form.append("duty_shift", dutyShift);
    for (const [building, file] of selectedEntries) {
      form.append("buildings", String(building || "").trim());
      form.append("files", file);
    }

    return guardedRun(
      ACTION_KEYS.handoverFromFile,
      async () => {
        try {
          const job = await postHandoverFromFilesJob(form);
          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          streamController.attachJobStream(job.job_id);
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = `交接班任务提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function runHandoverFromDownload(overridePayload = null) {
    if (!canRun.value) return;
    if (handoverDutyAutoFollow.value && !overridePayload) {
      syncHandoverDutyFromNow(true);
    }

    const scope = String(handoverDownloadScope.value || "single").trim();
    const usingLatestMode = !overridePayload && Boolean(handoverDutyAutoFollow.value);
    const payload =
      overridePayload && typeof overridePayload === "object"
        ? { ...overridePayload }
        : {};
    if (!Array.isArray(payload.buildings) || !payload.buildings.length) {
      if (scope === "single") {
        if (!manualBuilding.value) {
          message.value = "请选择楼栋";
          return;
        }
        payload.buildings = [manualBuilding.value];
      }
    }
    if (!usingLatestMode) {
      const dutyDate = String(handoverDutyDate.value || "").trim();
      const dutyShift = String(handoverDutyShift.value || "").trim().toLowerCase();
      if (!parseDateText(dutyDate)) {
        message.value = "交接班日期格式错误，请使用 YYYY-MM-DD";
        return;
      }
      if (!["day", "night"].includes(dutyShift)) {
        message.value = "班次仅支持白班/夜班";
        return;
      }
      payload.duty_date = dutyDate;
      payload.duty_shift = dutyShift;
    }
    await startJobByJson(
      "/api/jobs/handover/from-download",
      payload,
      "交接班日志使用共享文件生成",
      ACTION_KEYS.handoverFromDownload,
    );
  }

  async function runDayMetricFromDownload() {
    const dates = Array.isArray(dayMetricSelectedDates.value)
      ? dayMetricSelectedDates.value.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    if (!dates.length) {
      message.value = "请先选择至少一个日期";
      return;
    }
    const buildingScope = String(dayMetricUploadScope.value || "single").trim();
    if (!["single", "all_enabled"].includes(buildingScope)) {
      message.value = "楼栋范围仅支持单楼或全部启用楼栋";
      return;
    }

    const payload = {
      dates,
      building_scope: buildingScope,
    };
    if (buildingScope === "single") {
      const building = String(dayMetricUploadBuilding.value || "").trim();
      if (!building) {
        message.value = "请选择楼栋";
        return;
      }
      payload.building = building;
    }

    if (!canRun.value) return;
    return guardedRun(
      ACTION_KEYS.dayMetricFromDownload,
      async () => {
        try {
          message.value = "12项使用共享文件上传任务已提交";
          const response = await submitDayMetricFromDownloadJob(payload);
          await applyAcceptedExecutionResponse(response, "12项独立上传");
        } catch (err) {
          message.value = `12项使用共享文件上传提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function runBranchPowerFromDownload() {
    const businessDate = String(branchPowerBusinessDate?.value || "").trim();
    if (!parseDateText(businessDate)) {
      message.value = "请选择有效的支路三源表业务日期";
      return;
    }
    if (!canRun.value) return;
    return guardedRun(
      ACTION_KEYS.branchPowerFromDownload,
      async () => {
        try {
          const payload = {
            business_date: businessDate,
            target_business_date: businessDate,
            building_scope: "all_enabled",
          };
          message.value = `支路三源表整日直传任务已提交: ${businessDate}`;
          const response = await submitBranchPowerFromDownloadJob(payload);
          await applyAcceptedExecutionResponse(response, "支路三源表整日直传");
        } catch (err) {
          message.value = `支路三源表整日直传提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function runBranchPowerPowerAlertSync() {
    const businessDate = String(branchPowerBusinessDate?.value || "").trim();
    if (!parseDateText(businessDate)) {
      message.value = "请选择有效的动环统计业务日期";
      return;
    }
    if (!canRun.value) return;
    return guardedRun(
      ACTION_KEYS.branchPowerPowerAlertSync,
      async () => {
        try {
          const payload = {
            business_date: businessDate,
            target_business_date: businessDate,
          };
          message.value = `动环功率统计同步任务已提交: ${businessDate}`;
          const response = await submitBranchPowerPowerAlertSyncJob(payload);
          await applyAcceptedExecutionResponse(response, "动环功率统计同步");
        } catch (err) {
          message.value = `动环功率统计同步提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function runDayMetricFromFile() {
    const building = String(dayMetricLocalBuilding.value || "").trim();
    const dutyDate = String(dayMetricLocalDate.value || "").trim();
    if (!building) {
      message.value = "请选择补录楼栋";
      return;
    }
    if (!parseDateText(dutyDate)) {
      message.value = "补录日期格式错误，请使用 YYYY-MM-DD";
      return;
    }
    if (!dayMetricLocalFile.value) {
      message.value = "请选择补录 Excel 文件";
      return;
    }
    if (!canRun.value) return;

    const form = new FormData();
    form.append("building", building);
    form.append("duty_date", dutyDate);
    form.append("file", dayMetricLocalFile.value);

    return guardedRun(
      ACTION_KEYS.dayMetricFromFile,
      async () => {
        try {
          message.value = "12项补录任务已提交";
          const job = await submitDayMetricFromFileJob(form);
          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          streamController.attachJobStream(job.job_id);
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = `12项补录提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function retryDayMetricUnit(row) {
    const item = row && typeof row === "object" ? row : {};
    const dutyDate = String(item.duty_date || "").trim();
    const building = String(item.building || "").trim();
    const mode = String(item.mode || "from_download").trim().toLowerCase();
    if (!dutyDate || !building) {
      message.value = "失败单元缺少日期或楼栋，无法重试";
      return;
    }
    if (item.retryable === false) {
      message.value = String(item.retry_hint || item.error || "当前失败单元暂不支持重试").trim();
      return;
    }
    if (!canRun.value) return;
    const payload = {
      mode,
      duty_date: dutyDate,
      building,
      stage: String(item.stage_key || "").trim().toLowerCase(),
      source_file: String(item.source_file || "").trim(),
    };
    return guardedRun(
      `${ACTION_KEYS.dayMetricRetryUnit}:${mode}:${dutyDate}:${building}`,
      async () => {
        try {
          message.value = `12项失败单元重试已提交：${dutyDate} / ${building}`;
          const job = await submitDayMetricRetryUnitJob(payload);
          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          streamController.attachJobStream(job.job_id);
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = `12项失败单元重试提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function retryFailedDayMetricUnits(mode = "from_download") {
    const normalizedMode = String(mode || "from_download").trim().toLowerCase() || "from_download";
    if (!canRun.value) return;
    return guardedRun(
      `${ACTION_KEYS.dayMetricRetryFailed}:${normalizedMode}`,
      async () => {
        try {
          message.value = "12项失败单元批量重试已提交";
          const job = await submitDayMetricRetryFailedJob({ mode: normalizedMode });
          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          streamController.attachJobStream(job.job_id);
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = `12项失败单元批量重试提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  async function continueHandoverFollowupUpload(batchKey) {
    const targetBatchKey = String(batchKey || "").trim();
    if (!targetBatchKey) {
      message.value = "当前没有可继续执行的交接班批次";
      return;
    }
    if (!canRun.value) return;
    return guardedRun(
      ACTION_KEYS.handoverFollowupContinue,
      async () => {
        try {
          message.value = "继续后续上传任务已提交";
          const job = await submitHandoverFollowupContinueJob({ batch_key: targetBatchKey });
          currentJob.value = job;
          if (selectedJobId) {
            selectedJobId.value = String(job?.job_id || "").trim();
          }
          streamController.attachJobStream(job.job_id);
          if (typeof fetchJobs === "function") {
            await fetchJobs({ silentMessage: true });
          }
        } catch (err) {
          message.value = `继续后续上传提交失败: ${err}`;
        }
      },
      { cooldownMs: 0 },
    );
  }

  return {
    startJobByJson,
    runAutoOnce,
    runMultiDate,
    runManualUpload,
    runSheetImport,
    fetchJob,
    runHandoverFromFile,
    runHandoverFromDownload,
    runDayMetricFromDownload,
    runBranchPowerFromDownload,
    runBranchPowerPowerAlertSync,
    runDayMetricFromFile,
    retryDayMetricUnit,
    retryFailedDayMetricUnits,
    continueHandoverFollowupUpload,
    cancelCurrentJob,
    retryCurrentJob,
    getJobCancelActionKey,
    getJobRetryActionKey,
  };
}



