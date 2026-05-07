import {
  mapBackendActionState,
  mapBackendActionsState,
  normalizeBackendVisibleAction,
} from "./backend_action_display_helpers.js";

export function mapBackendOverviewCard(raw, defaults = {}) {
  const payload = raw && typeof raw === "object" ? raw : null;
  if (!payload) return null;
  return {
    kicker: String(payload.kicker || payload.eyebrow || "").trim() || defaults.kicker || "",
    title: String(payload.title || "").trim() || defaults.title || "",
    reasonCode: String(payload.reason_code || payload.reasonCode || "").trim().toLowerCase() || defaults.reasonCode || "",
    tone: String(payload.tone || "").trim() || defaults.tone || "neutral",
    statusText: String(payload.status_text || payload.statusText || "").trim() || defaults.statusText || "",
    summaryText: String(payload.summary_text || payload.summaryText || "").trim() || defaults.summaryText || "",
    detailText: String(payload.detail_text || payload.detailText || "").trim() || defaults.detailText || "",
    nextActionText: String(payload.next_action_text || payload.nextActionText || payload.action_text || payload.actionText || "").trim() || defaults.nextActionText || defaults.actionText || "",
    actionText: String(payload.action_text || payload.actionText || payload.next_action_text || payload.nextActionText || "").trim() || defaults.actionText || defaults.nextActionText || "",
    reasonText: String(payload.reason_text || payload.reasonText || "").trim() || defaults.reasonText || "",
    focusTitle: String(payload.focus_title || payload.focusTitle || "").trim() || defaults.focusTitle || "",
    focusMeta: String(payload.focus_meta || payload.focusMeta || "").trim() || defaults.focusMeta || "",
    runningCount: Number.parseInt(String(payload.running_count ?? payload.runningCount ?? defaults.runningCount ?? 0), 10) || 0,
    waitingCount: Number.parseInt(String(payload.waiting_count ?? payload.waitingCount ?? defaults.waitingCount ?? 0), 10) || 0,
    bridgeActiveCount: Number.parseInt(String(payload.bridge_active_count ?? payload.bridgeActiveCount ?? defaults.bridgeActiveCount ?? 0), 10) || 0,
    activeCount: Number.parseInt(String(payload.active_count ?? payload.activeCount ?? defaults.activeCount ?? 0), 10) || 0,
    finishedCount: Number.parseInt(String(payload.finished_count ?? payload.finishedCount ?? defaults.finishedCount ?? 0), 10) || 0,
    recentFailureTitle: String(payload.recent_failure_title ?? payload.recentFailureTitle ?? defaults.recentFailureTitle ?? "").trim(),
    restartImpactText: String(payload.restart_impact_text || payload.restartImpactText || defaults.restartImpactText || "").trim(),
    items: Array.isArray(payload.items) ? payload.items : [],
    actions: Array.isArray(payload.actions)
      ? payload.actions
        .filter((action) => action && typeof action === "object")
        .map((action) => ({
          ...mapBackendActionState(action),
          desc: String(action.desc || "").trim(),
          visible: action.visible !== false,
        }))
      : [],
  };
}

export function createEmptyOverviewCard(defaults = {}) {
  return {
    kicker: String(defaults.kicker || "").trim(),
    title: String(defaults.title || "").trim(),
    reasonCode: String(defaults.reasonCode || "idle").trim().toLowerCase() || "idle",
    tone: String(defaults.tone || "neutral").trim() || "neutral",
    statusText: String(defaults.statusText || "").trim(),
    summaryText: String(defaults.summaryText || "").trim(),
    detailText: String(defaults.detailText || "").trim(),
    nextActionText: String(defaults.nextActionText || "").trim(),
    actionText: String(defaults.actionText || "").trim(),
    reasonText: String(defaults.reasonText || "").trim(),
    focusTitle: String(defaults.focusTitle || "").trim(),
    focusMeta: String(defaults.focusMeta || "").trim(),
    runningCount: Number.parseInt(String(defaults.runningCount ?? 0), 10) || 0,
    waitingCount: Number.parseInt(String(defaults.waitingCount ?? 0), 10) || 0,
    bridgeActiveCount: Number.parseInt(String(defaults.bridgeActiveCount ?? 0), 10) || 0,
    activeCount: Number.parseInt(String(defaults.activeCount ?? 0), 10) || 0,
    finishedCount: Number.parseInt(String(defaults.finishedCount ?? 0), 10) || 0,
    recentFailureTitle: String(defaults.recentFailureTitle || "").trim(),
    restartImpactText: String(defaults.restartImpactText || "").trim(),
    items: Array.isArray(defaults.items) ? defaults.items : [],
    actions: Array.isArray(defaults.actions) ? defaults.actions : [],
  };
}

export function resolveBackendOverviewCard(primaryRaw, fallbackRaw, defaults = {}) {
  return (
    mapBackendOverviewCard(primaryRaw, defaults)
    || mapBackendOverviewCard(fallbackRaw, defaults)
    || createEmptyOverviewCard(defaults)
  );
}

export function normalizeMonthlyReportLastRunDisplay(raw, fallbackLabel = "月报") {
  const payload = raw && typeof raw === "object" ? raw : {};
  const display = payload.display && typeof payload.display === "object" ? payload.display : {};
  const successfulBuildings = Array.isArray(display.successful_buildings)
    ? display.successful_buildings
    : (Array.isArray(payload.successful_buildings) ? payload.successful_buildings : []);
  const failedBuildings = Array.isArray(display.failed_buildings)
    ? display.failed_buildings
    : (Array.isArray(payload.failed_buildings) ? payload.failed_buildings : []);
  return {
    ...payload,
    tone: String(display.tone || payload.tone || "").trim() || "neutral",
    statusText: String(
      display.status_text
      || display.statusText
      || payload.status_text
      || payload.statusText
      || payload.status
      || "",
    ).trim() || "尚未执行",
    summaryText: String(display.summary_text || display.summaryText || "").trim() || `${String(fallbackLabel || "月报").trim()}最近执行状态由后端返回。`,
    detailText: String(display.detail_text || display.detailText || "").trim(),
    reasonCode: String(display.reason_code || display.reasonCode || "").trim().toLowerCase(),
    started_at: String(display.started_at || payload.started_at || "").trim(),
    finished_at: String(display.finished_at || payload.finished_at || payload.started_at || "").trim(),
    target_month: String(display.target_month || payload.target_month || "").trim(),
    generated_files: Number.parseInt(String(display.generated_files ?? payload.generated_files ?? 0), 10) || 0,
    successful_buildings: successfulBuildings.map((item) => String(item || "").trim()).filter(Boolean),
    failed_buildings: failedBuildings.map((item) => String(item || "").trim()).filter(Boolean),
    output_dir: String(display.output_dir || payload.output_dir || "").trim(),
    error: String(display.error || display.error_text || payload.error || "").trim(),
  };
}

export function normalizeMonthlyReportDeliveryLastRun(raw, fallbackLabel = "月报") {
  const payload = raw && typeof raw === "object" ? raw : {};
  return {
    tone: String(payload.tone || "").trim() || "neutral",
    statusText: String(payload.status_text || payload.statusText || "").trim() || "尚未发送",
    summaryText: String(payload.summary_text || payload.summaryText || "").trim() || `${String(fallbackLabel || "月报").trim()}发送状态由后端返回。`,
    detailText: String(payload.detail_text || payload.detailText || "").trim(),
    reasonCode: String(payload.reason_code || payload.reasonCode || "").trim().toLowerCase(),
    started_at: String(payload.started_at || "").trim(),
    finished_at: String(payload.finished_at || payload.started_at || "").trim(),
    target_month: String(payload.target_month || payload.targetMonth || "").trim(),
    successful_buildings: (Array.isArray(payload.successful_buildings) ? payload.successful_buildings : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean),
    failed_buildings: (Array.isArray(payload.failed_buildings) ? payload.failed_buildings : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean),
    test_mode: Boolean(payload.test_mode ?? payload.testMode),
    test_receive_ids: (Array.isArray(payload.test_receive_ids) ? payload.test_receive_ids : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean),
    test_receive_id_type: String(payload.test_receive_id_type || payload.testReceiveIdType || "").trim(),
    test_successful_receivers: (Array.isArray(payload.test_successful_receivers) ? payload.test_successful_receivers : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean),
    test_failed_receivers: (Array.isArray(payload.test_failed_receivers) ? payload.test_failed_receivers : [])
      .map((item) => String(item || "").trim())
      .filter(Boolean),
    test_file_name: String(payload.test_file_name || payload.testFileName || "").trim(),
    test_file_building: String(payload.test_file_building || payload.testFileBuilding || "").trim(),
    error: String(payload.error || payload.error_text || payload.errorText || "").trim(),
  };
}

export function normalizeMonthlyReportDeliveryRow(raw, building = "") {
  const payload = raw && typeof raw === "object" ? raw : {};
  return {
    building: String(payload.building || building || "").trim() || "-",
    supervisor: String(payload.supervisor || "").trim(),
    position: String(payload.position || "").trim(),
    recipientId: String(payload.recipient_id || payload.recipientId || "").trim(),
    receiveIdType: String(payload.receive_id_type || payload.receiveIdType || "").trim() || "user_id",
    sendReady: Boolean(payload.send_ready ?? payload.sendReady),
    reason: String(payload.reason || "").trim(),
    fileName: String(payload.file_name || payload.fileName || "").trim(),
    filePath: String(payload.file_path || payload.filePath || "").trim(),
    fileExists: Boolean(payload.file_exists ?? payload.fileExists),
    tone: String(payload.tone || "").trim() || "neutral",
    statusText: String(payload.status_text || payload.statusText || "").trim() || "等待后端状态",
    detailText: String(payload.detail_text || payload.detailText || "").trim() || "等待后端状态",
  };
}

export function createNeutralMonthlyReportDeliveryRow(building) {
  return normalizeMonthlyReportDeliveryRow({}, building);
}

export function normalizeMonthlyReportDeliveryOverview(raw, reportLabel = "月报") {
  const payload = raw && typeof raw === "object" ? raw : {};
  return {
    tone: String(payload.tone || "").trim() || "neutral",
    statusText: String(payload.status_text || payload.statusText || "").trim() || "等待后端状态",
    summaryText: String(payload.summary_text || payload.summaryText || "").trim() || `${String(reportLabel || "月报").trim()}发送状态由后端返回。`,
    detailText: String(payload.detail_text || payload.detailText || "").trim(),
    reasonCode: String(payload.reason_code || payload.reasonCode || "").trim().toLowerCase() || "pending_backend",
    sendReadyCount: Number.parseInt(String(payload.send_ready_count ?? payload.sendReadyCount ?? 0), 10) || 0,
    targetMonth: String(payload.target_month || payload.targetMonth || "").trim(),
    reportType: String(payload.report_type || payload.reportType || "").trim(),
    reportLabel: String(payload.report_label || payload.reportLabel || reportLabel || "").trim(),
  };
}

export function createEmptyJobPanelDisplay() {
  return {
    running_jobs: [],
    waiting_resource_items: [],
    recent_finished_jobs: [],
    overview: {
      ...createEmptyOverviewCard({
        reasonCode: "pending_backend",
        tone: "neutral",
        statusText: "等待后端状态",
        summaryText: "任务状态由后端聚合后返回。",
        detailText: "",
        focusTitle: "等待后端任务状态",
        focusMeta: "",
      }),
      handover_generation_busy: false,
      handover_generation_status_text: "等待后端任务状态",
    },
  };
}

export function normalizeJobPanelDisplayPayload(raw) {
  const display = raw && typeof raw === "object" ? raw : {};
  const defaults = createEmptyJobPanelDisplay();
  const normalizedOverview = {
    ...defaults.overview,
    ...(mapBackendOverviewCard(display.overview, defaults.overview) || {}),
    handover_generation_busy: Boolean(display?.overview?.handover_generation_busy),
    handover_generation_status_text: String(
      display?.overview?.handover_generation_status_text || defaults.overview.handover_generation_status_text,
    ).trim() || defaults.overview.handover_generation_status_text,
  };
  return {
    running_jobs: Array.isArray(display.running_jobs) ? display.running_jobs : [],
    waiting_resource_items: Array.isArray(display.waiting_resource_items) ? display.waiting_resource_items : [],
    recent_finished_jobs: Array.isArray(display.recent_finished_jobs) ? display.recent_finished_jobs : [],
    overview: normalizedOverview,
  };
}

export function createEmptyBridgeTasksDisplay() {
  return {
    active_tasks: [],
    waiting_resource_items: [],
    recent_finished_tasks: [],
    active_count: 0,
    waiting_count: 0,
    finished_count: 0,
    overview: createEmptyOverviewCard({
      reasonCode: "pending_backend",
      tone: "neutral",
      statusText: "等待后端状态",
      summaryText: "共享桥接任务状态由后端聚合后返回。",
      detailText: "",
      focusTitle: "等待后端桥接任务状态",
      focusMeta: "",
      activeCount: 0,
      waitingCount: 0,
      finishedCount: 0,
    }),
  };
}

export function normalizeBridgeTasksDisplayPayload(raw) {
  const display = raw && typeof raw === "object" ? raw : {};
  const defaults = createEmptyBridgeTasksDisplay();
  return {
    active_tasks: Array.isArray(display.active_tasks) ? display.active_tasks : [],
    waiting_resource_items: Array.isArray(display.waiting_resource_items) ? display.waiting_resource_items : [],
    recent_finished_tasks: Array.isArray(display.recent_finished_tasks) ? display.recent_finished_tasks : [],
    active_count: Number.parseInt(String(display.active_count ?? defaults.active_count), 10) || 0,
    waiting_count: Number.parseInt(String(display.waiting_count ?? defaults.waiting_count), 10) || 0,
    finished_count: Number.parseInt(String(display.finished_count ?? defaults.finished_count), 10) || 0,
    overview: mapBackendOverviewCard(display.overview, defaults.overview) || defaults.overview,
  };
}

export function normalizeReviewCloudSheetSyncBrief(raw) {
  const payload = raw && typeof raw === "object" ? raw : {};
  return {
    status: String(payload.status || "").trim().toLowerCase(),
    text: String(payload.text || "").trim(),
    tone: String(payload.tone || "").trim() || "neutral",
    url: String(payload.url || "").trim(),
    error: String(payload.error || "").trim(),
  };
}

export function normalizeReviewLinkDeliveryBrief(raw) {
  const payload = raw && typeof raw === "object" ? raw : {};
  return {
    status: String(payload.status || "").trim().toLowerCase(),
    text: String(payload.text || "").trim(),
    tone: String(payload.tone || "").trim() || "neutral",
    error: String(payload.error || "").trim(),
    lastSentAt: String(payload.last_sent_at || payload.lastSentAt || "").trim(),
    lastAttemptAt: String(payload.last_attempt_at || payload.lastAttemptAt || "").trim(),
  };
}

export function normalizeReviewRecipientStatus(raw) {
  const payload = raw && typeof raw === "object" ? raw : {};
  return {
    text: String(payload.text || "").trim(),
    reason: String(payload.reason || "").trim(),
    recipientCount: Number(payload.recipient_count || payload.recipientCount || 0),
    enabledCount: Number(payload.enabled_count || payload.enabledCount || 0),
    disabledCount: Number(payload.disabled_count || payload.disabledCount || 0),
    invalidCount: Number(payload.invalid_count || payload.invalidCount || 0),
  };
}

export function normalizeReviewBoardRow(raw) {
  const row = raw && typeof raw === "object" ? raw : {};
  const actions = row.actions && typeof row.actions === "object" ? row.actions : {};
  return {
    building: String(row.building || "").trim(),
    status: String(row.status || "").trim().toLowerCase(),
    text: String(row.text || "").trim(),
    tone: String(row.tone || "").trim() || "neutral",
    code: String(row.code || "").trim().toLowerCase(),
    url: String(row.url || "").trim(),
    sessionId: String(row.session_id || row.sessionId || "").trim(),
    revision: Number(row.revision || 0),
    updatedAt: String(row.updated_at || row.updatedAt || "").trim(),
    cloudSheetSync: normalizeReviewCloudSheetSyncBrief(row.cloud_sheet_sync || row.cloudSheetSync),
    reviewLinkDelivery: normalizeReviewLinkDeliveryBrief(row.review_link_delivery || row.reviewLinkDelivery),
    reviewLinkRecipientStatus: normalizeReviewRecipientStatus(
      row.review_link_recipient_status || row.reviewLinkRecipientStatus,
    ),
    actions: {
      reviewLinkSend: normalizeBackendVisibleAction(
        actions.review_link_send || actions.reviewLinkSend,
        {
          allowed: false,
          pending: false,
          visible: false,
          label: "手动发送审核链接",
          disabledReason: "",
        },
      ),
    },
  };
}

export function createEmptyHandoverReviewOverview() {
  return {
    batchKey: "",
    dutyDate: "",
    dutyShift: "",
    dutyText: "",
    hasAnySession: false,
    required: 0,
    confirmed: 0,
    pending: 0,
    allConfirmed: false,
    readyForFollowupUpload: false,
    cloudRetryFailureCount: 0,
    followupFailedCount: 0,
    followupPendingCount: 0,
    reviewBoardRows: [],
    followupProgress: {
      status: "idle",
      canResumeFollowup: false,
      pendingCount: 0,
      failedCount: 0,
      attachmentPendingCount: 0,
      cloudPendingCount: 0,
      dailyReportStatus: "idle",
      tone: "neutral",
      statusText: "等待后端交接班状态",
      summaryText: "已清空",
    },
    tone: "neutral",
    summaryText: "等待后端交接班状态",
    actions: {
      confirmAll: normalizeBackendVisibleAction(null, {
        allowed: false,
        pending: false,
        visible: false,
        label: "一键全确认",
        disabledReason: "",
      }),
      retryCloudSyncAll: normalizeBackendVisibleAction(null, {
        allowed: false,
        pending: false,
        visible: false,
        label: "一键全部重试云表上传",
        disabledReason: "",
      }),
      continueFollowup: normalizeBackendVisibleAction(null, {
        allowed: false,
        pending: false,
        visible: false,
        label: "继续后续上传",
        disabledReason: "",
      }),
    },
  };
}

export function normalizeHandoverReviewOverview(raw) {
  const backendOverview = raw && typeof raw === "object" ? raw : {};
  const rawActions = backendOverview.actions && typeof backendOverview.actions === "object"
    ? backendOverview.actions
    : {};
  const rawFollowup = backendOverview.followup_progress && typeof backendOverview.followup_progress === "object"
    ? backendOverview.followup_progress
    : (backendOverview.followupProgress && typeof backendOverview.followupProgress === "object"
      ? backendOverview.followupProgress
      : {});
  const defaults = createEmptyHandoverReviewOverview();
  return {
    batchKey: String(backendOverview.batch_key || backendOverview.batchKey || "").trim(),
    dutyDate: String(backendOverview.duty_date || backendOverview.dutyDate || "").trim(),
    dutyShift: String(backendOverview.duty_shift || backendOverview.dutyShift || "").trim().toLowerCase(),
    dutyText: String(backendOverview.duty_text || backendOverview.dutyText || "").trim(),
    hasAnySession: Boolean(backendOverview.has_any_session ?? backendOverview.hasAnySession),
    required: Number(backendOverview.required || 0),
    confirmed: Number(backendOverview.confirmed || 0),
    pending: Number(backendOverview.pending || 0),
    allConfirmed: Boolean(backendOverview.all_confirmed ?? backendOverview.allConfirmed),
    readyForFollowupUpload: Boolean(backendOverview.ready_for_followup_upload ?? backendOverview.readyForFollowupUpload),
    cloudRetryFailureCount: Number(backendOverview.cloud_retry_failure_count || backendOverview.cloudRetryFailureCount || 0),
    followupFailedCount: Number(backendOverview.followup_failed_count || backendOverview.followupFailedCount || 0),
    followupPendingCount: Number(backendOverview.followup_pending_count || backendOverview.followupPendingCount || 0),
    reviewBoardRows: Array.isArray(backendOverview.review_board_rows || backendOverview.reviewBoardRows)
      ? (backendOverview.review_board_rows || backendOverview.reviewBoardRows).map((row) => normalizeReviewBoardRow(row))
      : [],
    followupProgress: {
      status: String(rawFollowup.status || "").trim().toLowerCase() || defaults.followupProgress.status,
      canResumeFollowup: Boolean(rawFollowup.can_resume_followup ?? rawFollowup.canResumeFollowup),
      pendingCount: Number(
        rawFollowup.pending_count ??
        rawFollowup.pendingCount ??
        backendOverview.followup_pending_count ??
        backendOverview.followupPendingCount ??
        0,
      ),
      failedCount: Number(
        rawFollowup.failed_count ??
        rawFollowup.failedCount ??
        backendOverview.followup_failed_count ??
        backendOverview.followupFailedCount ??
        0,
      ),
      attachmentPendingCount: Number(rawFollowup.attachment_pending_count ?? rawFollowup.attachmentPendingCount ?? 0),
      cloudPendingCount: Number(rawFollowup.cloud_pending_count ?? rawFollowup.cloudPendingCount ?? 0),
      dailyReportStatus: String(rawFollowup.daily_report_status || rawFollowup.dailyReportStatus || "").trim().toLowerCase() || defaults.followupProgress.dailyReportStatus,
      tone: String(rawFollowup.tone || "").trim() || defaults.followupProgress.tone,
      statusText: String(rawFollowup.status_text || rawFollowup.statusText || "").trim() || defaults.followupProgress.statusText,
      summaryText: String(rawFollowup.summary_text || rawFollowup.summaryText || "").trim() || defaults.followupProgress.summaryText,
    },
    tone: String(backendOverview.tone || "").trim() || defaults.tone,
    summaryText: String(
      backendOverview.summary_text || backendOverview.summaryText || backendOverview.status_text || backendOverview.statusText || "",
    ).trim() || defaults.summaryText,
    actions: {
      confirmAll: normalizeBackendVisibleAction(rawActions.confirm_all, defaults.actions.confirmAll),
      retryCloudSyncAll: normalizeBackendVisibleAction(rawActions.retry_cloud_sync_all, defaults.actions.retryCloudSyncAll),
      continueFollowup: normalizeBackendVisibleAction(rawActions.continue_followup, defaults.actions.continueFollowup),
    },
  };
}

export function mapCurrentHourRefreshOverview(raw) {
  const payload = raw && typeof raw === "object" ? raw : {};
  return {
    reasonCode: String(payload.reason_code || payload.reasonCode || "").trim().toLowerCase(),
    tone: String(payload.tone || "").trim() || "neutral",
    statusText: String(payload.status_text || payload.statusText || "").trim() || "尚未触发",
    summaryText: String(payload.summary_text || payload.summaryText || "").trim(),
    detailText: String(payload.detail_text || payload.detailText || "").trim(),
    lastRunAt: String(payload.last_run_at || payload.lastRunAt || "").trim(),
    lastSuccessAt: String(payload.last_success_at || payload.lastSuccessAt || "").trim(),
    lastError: String(payload.last_error || payload.lastError || "").trim(),
    failedBuildings: Array.isArray(payload.failed_buildings) ? payload.failed_buildings : (Array.isArray(payload.failedBuildings) ? payload.failedBuildings : []),
    blockedBuildings: Array.isArray(payload.blocked_buildings) ? payload.blocked_buildings : (Array.isArray(payload.blockedBuildings) ? payload.blockedBuildings : []),
    runningBuildings: Array.isArray(payload.running_buildings) ? payload.running_buildings : (Array.isArray(payload.runningBuildings) ? payload.runningBuildings : []),
    completedBuildings: Array.isArray(payload.completed_buildings) ? payload.completed_buildings : (Array.isArray(payload.completedBuildings) ? payload.completedBuildings : []),
    items: Array.isArray(payload.items) ? payload.items : [],
    actions: mapBackendActionsState(payload.actions),
  };
}

export function mapBackendSchedulerOverviewPart(raw) {
  const part = raw && typeof raw === "object" ? raw : {};
  return {
    label: String(part.label || "").trim() || "-",
    runTimeText: String(part.run_time_text || part.runTimeText || "").trim() || "-",
    nextRunText: String(part.next_run_text || part.nextRunText || "").trim() || "-",
    lastTriggerText: String(part.last_trigger_text || part.lastTriggerText || "").trim() || "-",
    resultText: String(part.result_text || part.resultText || "").trim() || "-",
  };
}

export function mapBackendSchedulerOverviewItem(raw) {
  const item = raw && typeof raw === "object" ? raw : {};
  return {
    key: String(item.key || "").trim(),
    title: String(item.title || "").trim() || "-",
    moduleId: String(item.module_id || item.moduleId || "").trim(),
    focusKey: String(item.focus_key || item.focusKey || "").trim(),
    statusText: String(item.status_text || item.statusText || "").trim() || "等待后端状态",
    summaryText: String(item.summary_text || item.summaryText || "").trim() || "",
    tone: String(item.tone || "").trim() || "neutral",
    parts: Array.isArray(item.parts) ? item.parts.map(mapBackendSchedulerOverviewPart) : [],
  };
}

export function mapBackendSchedulerOverviewSummary(raw) {
  const summary = raw && typeof raw === "object" ? raw : {};
  return {
    runningCount: Number(summary.running_count ?? summary.runningCount ?? 0),
    stoppedCount: Number(summary.stopped_count ?? summary.stoppedCount ?? 0),
    attentionCount: Number(summary.attention_count ?? summary.attentionCount ?? 0),
    statusText: String(summary.status_text || summary.statusText || "").trim() || "等待后端状态",
    reasonCode: String(summary.reason_code || summary.reasonCode || "").trim().toLowerCase(),
    tone: String(summary.tone || "").trim() || "neutral",
    nextSchedulerLabel: String(summary.next_scheduler_label || summary.nextSchedulerLabel || "").trim() || "-",
    nextSchedulerText: String(summary.next_scheduler_text || summary.nextSchedulerText || "").trim() || "-",
    attentionText: String(summary.attention_text || summary.attentionText || "").trim() || "-",
    summaryText: String(summary.summary_text || summary.summaryText || "").trim() || "调度状态由后端聚合后返回。",
    detailText: String(summary.detail_text || summary.detailText || "").trim() || "",
    items: Array.isArray(summary.items) ? summary.items : [],
    actions: Array.isArray(summary.actions) ? summary.actions : [],
  };
}

export function mapBackendUpdaterMirrorOverview(raw) {
  const overview = raw && typeof raw === "object" ? raw : null;
  if (!overview) return null;
  const statusText = String(overview.status_text || overview.statusText || "").trim();
  const items = Array.isArray(overview.items)
    ? overview.items.map((item) => ({
      label: String(item?.label || "").trim() || "-",
      value: String(item?.value || "").trim() || "-",
      tone: String(item?.tone || "").trim() || "neutral",
    }))
    : [];
  if (!statusText && !items.length) return null;
  const syncRaw = overview.sync && typeof overview.sync === "object" ? overview.sync : {};
  const businessActionsRaw = overview.business_actions && typeof overview.business_actions === "object"
    ? overview.business_actions
    : (overview.businessActions && typeof overview.businessActions === "object" ? overview.businessActions : {});
  return {
    tone: String(overview.tone || "").trim() || "neutral",
    kicker: String(overview.kicker || "").trim() || "代码同步",
    title: String(overview.title || "").trim() || "本机更新状态",
    statusText: statusText || "尚未发布到共享目录",
    badgeText: String(overview.badge_text || overview.badgeText || statusText || "").trim() || "等待后端更新状态",
    summaryText: String(overview.summary_text || overview.summaryText || "").trim() || "",
    manifestPath: String(overview.manifest_path || overview.manifestPath || "").trim(),
    manifestLabel: String(overview.manifest_label || overview.manifestLabel || "").trim() || "源码包清单",
    errorText: String(overview.error_text || overview.errorText || "").trim(),
    items,
    sync: {
      mode: String(syncRaw.mode || "").trim(),
      localCommit: String(syncRaw.local_commit || syncRaw.localCommit || "").trim(),
      remoteCommit: String(syncRaw.remote_commit || syncRaw.remoteCommit || "").trim(),
      publishedCommit: String(syncRaw.published_commit || syncRaw.publishedCommit || "").trim(),
      pendingSyncCommit: String(syncRaw.pending_sync_commit || syncRaw.pendingSyncCommit || "").trim(),
      deferredCommit: String(syncRaw.deferred_commit || syncRaw.deferredCommit || "").trim(),
    },
    actions: mapBackendActionsState(overview.actions),
    businessActions: {
      allowed: businessActionsRaw.allowed !== false,
      reasonCode: String(businessActionsRaw.reason_code || businessActionsRaw.reasonCode || "").trim().toLowerCase(),
      disabledReason: String(businessActionsRaw.disabled_reason || businessActionsRaw.disabledReason || "").trim(),
      statusText: String(businessActionsRaw.status_text || businessActionsRaw.statusText || "").trim(),
    },
  };
}
