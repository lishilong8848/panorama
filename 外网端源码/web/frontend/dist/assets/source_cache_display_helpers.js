import {
  buildSourceCachePlaceholderBuilding,
  mapBackendActionsState,
} from "./backend_action_display_helpers.js";

function resolveSourceCacheFamilyTitle(key) {
  const text = String(key || "").trim().toLowerCase();
  if (text === "handover_log_family") return "交接班日志源文件";
  if (text === "handover_capacity_report_family") return "交接班容量报表源文件";
  if (text === "monthly_report_family") return "全景平台月报源文件";
  if (text === "branch_power_family") return "支路功率源文件";
  if (text === "branch_current_family") return "支路电流源文件";
  if (text === "branch_switch_family") return "支路开关源文件";
  if (text === "chiller_mode_switch_family") return "制冷单元模式切换参数源文件";
  if (text === "alarm_event_family") return "告警信息源文件";
  return "";
}

function resolveSourceCacheFamilyKeyByTitle(title) {
  const text = String(title || "").trim();
  if (text === "交接班日志源文件") return "handover_log_family";
  if (text === "交接班容量报表源文件") return "handover_capacity_report_family";
  if (text === "全景平台月报源文件") return "monthly_report_family";
  if (text === "支路功率源文件") return "branch_power_family";
  if (text === "支路电流源文件") return "branch_current_family";
  if (text === "支路开关源文件") return "branch_switch_family";
  if (text === "制冷单元模式切换参数源文件") return "chiller_mode_switch_family";
  if (text === "告警信息源文件") return "alarm_event_family";
  return "";
}

function parseOptionalCount(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  if (!text) return null;
  const parsed = Number.parseInt(text, 10);
  return Number.isFinite(parsed) ? Math.max(0, parsed) : null;
}

function formatAlarmSourceKindText(value) {
  const text = String(value || "").trim().toLowerCase();
  if (text === "manual") return "手动";
  if (text === "latest") return "定时";
  return "";
}

function formatAlarmSelectionScopeText(value) {
  const text = String(value || "").trim().toLowerCase();
  if (text === "today") return "今天最新";
  if (text === "yesterday_fallback") return "昨天回退";
  if (text === "missing") return "今天和昨天都缺文件";
  return "";
}

function normalizeSourceCacheBuildingRow(raw, fallbackBucket, formatSharedBridgeRuntimeError, formatInternalDownloadPoolError) {
  const building = String(raw?.building || "").trim() || "-";
  const bucketKey = String(raw?.bucket_key || raw?.bucketKey || "").trim() || String(fallbackBucket || "").trim() || "-";
  const rawStatus = String(raw?.status || raw?.statusKey || "").trim().toLowerCase();
  const supportedStatuses = ["ready", "failed", "downloading", "consumed", "blocked", "waiting", "pending_backend"];
  return {
    building,
    bucket_key: bucketKey,
    status: supportedStatuses.includes(rawStatus) ? rawStatus : "pending_backend",
    ready: Boolean(raw?.ready),
    downloaded_at: String(raw?.downloaded_at || raw?.downloadedAt || "").trim(),
    last_error: formatSharedBridgeRuntimeError(raw?.last_error || raw?.lastError),
    relative_path: String(raw?.relative_path || raw?.relativePath || "").trim(),
    resolved_file_path: String(raw?.resolved_file_path || raw?.resolvedFilePath || "").trim(),
    started_at: String(raw?.started_at || raw?.startedAt || "").trim(),
    blocked: Boolean(raw?.blocked),
    blocked_reason: formatInternalDownloadPoolError(raw?.blocked_reason || raw?.blockedReason || raw?.last_error || raw?.lastError),
    next_probe_at: String(raw?.next_probe_at || raw?.nextProbeAt || "").trim(),
  };
}

function normalizeSourceCacheBuildingStatus(raw, fallbackBucket, formatSharedBridgeRuntimeError, formatInternalDownloadPoolError) {
  const row = normalizeSourceCacheBuildingRow(raw, fallbackBucket, formatSharedBridgeRuntimeError, formatInternalDownloadPoolError);
  const building = row.building;
  const bucketKey = row.bucket_key;
  const sourceFamily = String(raw?.source_family || raw?.sourceFamily || "").trim();
  const downloadedAt = row.downloaded_at;
  const startedAt = row.started_at;
  const lastError = row.last_error;
  const blocked = Boolean(row.blocked);
  const blockedReason = row.blocked_reason;
  const nextProbeAt = row.next_probe_at;
  const relativePath = row.relative_path;
  const resolvedFilePath = row.resolved_file_path;
  const explicitStatusKey = String(raw?.status_key || raw?.statusKey || raw?.status || row.status || "").trim().toLowerCase() || "pending_backend";
  const explicitReady = typeof raw?.ready === "boolean" ? raw.ready : null;
  const usingFallback = Boolean(raw?.using_fallback ?? raw?.usingFallback);
  const rawVersionGap = Number.parseInt(String(raw?.version_gap ?? raw?.versionGap ?? ""), 10);
  const versionGap = Number.isFinite(rawVersionGap) ? Math.max(0, rawVersionGap) : null;
  const backendStatusText = String(raw?.status_text || raw?.statusText || "").trim();
  const backendDetailText = String(raw?.detail_text || raw?.detailText || "").trim();
  const backendTone = String(raw?.tone || "").trim();
  const backendMetaLines = Array.isArray(raw?.meta_lines)
    ? raw.meta_lines.map((item) => String(item || "").trim()).filter(Boolean)
    : Array.isArray(raw?.metaLines)
      ? raw.metaLines.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
  const mappedActions = mapBackendActionsState(raw?.actions);
  const hasMeaningfulRawState = (
    explicitStatusKey !== "pending_backend"
    || explicitReady === true
    || Boolean(downloadedAt)
    || Boolean(startedAt)
    || Boolean(lastError)
    || Boolean(relativePath)
    || Boolean(resolvedFilePath)
    || blocked
    || Boolean(blockedReason)
  );
  if (!backendStatusText && !backendDetailText && !backendMetaLines.length && !Object.keys(mappedActions).length) {
    if (hasMeaningfulRawState) {
      let tone = "neutral";
      let stateText = "等待中";
      let detailText = blocked
      ? (blockedReason || "等待采集端恢复")
        : (lastError || downloadedAt || startedAt || resolvedFilePath || relativePath || "等待共享文件就绪");
      if (explicitStatusKey === "ready" && (explicitReady !== false)) {
        if (usingFallback) {
          tone = "warning";
          stateText = "使用上一版共享文件";
          detailText = versionGap && versionGap > 0
            ? `当前楼仍在使用上一版共享文件，落后 ${versionGap} 版。`
            : "当前楼仍在使用上一版共享文件，等待最新共享文件就绪。";
        } else {
          tone = "success";
          stateText = "已就绪";
        }
      } else if (explicitStatusKey === "downloading") {
        tone = "info";
        stateText = "下载中";
      } else if (explicitStatusKey === "consumed") {
        tone = "info";
        stateText = "已消费";
      } else if (explicitStatusKey === "stale") {
        tone = "warning";
        stateText = "版本过旧，等待更新";
        detailText = versionGap && versionGap > 0
          ? `当前共享文件已落后 ${versionGap} 版，等待最新文件就绪。`
          : "当前共享文件版本过旧，等待更新。";
      } else if (explicitStatusKey === "failed") {
        tone = "danger";
        stateText = "失败";
      } else if (blocked) {
        tone = "warning";
      stateText = "等待采集端恢复";
      }
      return {
        building,
        bucketKey,
        sourceFamily,
        source_family: sourceFamily,
        statusKey: blocked ? "blocked" : explicitStatusKey,
        reasonCode: blocked ? "blocked" : explicitStatusKey,
        ready: explicitReady === null ? explicitStatusKey === "ready" : explicitReady,
        downloadedAt,
        startedAt,
        lastError,
        blocked,
        blockedReason,
        nextProbeAt,
        relativePath,
        resolvedFilePath,
        tone,
        stateText,
        detailText,
        metaLines: [],
        actions: mappedActions,
      };
    }
    return {
      ...buildSourceCachePlaceholderBuilding(building, bucketKey, sourceFamily),
      building,
      bucketKey,
      sourceFamily,
      source_family: sourceFamily,
      downloadedAt,
      startedAt,
      lastError,
      blocked,
      blockedReason,
      nextProbeAt,
      relativePath,
      resolvedFilePath,
    };
  }
  return {
    building,
    bucketKey,
    sourceFamily,
    source_family: sourceFamily,
    statusKey: explicitStatusKey,
    reasonCode: String(raw?.reason_code || raw?.reasonCode || explicitStatusKey || "").trim().toLowerCase() || "unknown",
    ready: explicitReady === null ? explicitStatusKey === "ready" : explicitReady,
    downloadedAt,
    startedAt,
    lastError,
    blocked,
    blockedReason,
    nextProbeAt,
    relativePath,
    resolvedFilePath,
    tone: backendTone || "neutral",
    stateText: backendStatusText || "等待后端状态",
    detailText: backendDetailText || "等待后端状态",
    metaLines: backendMetaLines,
    actions: mappedActions,
  };
}

function normalizeAlarmEventReadinessBuilding(raw, fallbackBucket, formatSharedBridgeRuntimeError, formatInternalDownloadPoolError) {
  const building = String(raw?.building || "").trim() || "-";
  const bucketKey = String(raw?.bucket_key || raw?.bucketKey || "").trim() || String(fallbackBucket || "").trim() || "-";
  const sourceFamily = String(raw?.source_family || raw?.sourceFamily || "").trim();
  const downloadedAt = String(raw?.downloaded_at || raw?.downloadedAt || "").trim();
  const selectedDownloadedAt = String(raw?.selected_downloaded_at || raw?.selectedDownloadedAt || downloadedAt || "").trim();
  const lastError = formatSharedBridgeRuntimeError(raw?.last_error || raw?.lastError);
  const relativePath = String(raw?.relative_path || raw?.relativePath || "").trim();
  const resolvedFilePath = String(raw?.resolved_file_path || raw?.resolvedFilePath || "").trim();
  const blocked = Boolean(raw?.blocked);
  const blockedReason = formatInternalDownloadPoolError(raw?.blocked_reason || raw?.blockedReason || raw?.last_error || raw?.lastError);
  const sourceKind = String(raw?.source_kind || raw?.sourceKind || "").trim().toLowerCase();
  const sourceKindText = String(raw?.source_kind_text || raw?.sourceKindText || "").trim() || formatAlarmSourceKindText(sourceKind);
  const selectionScope = String(raw?.selection_scope || raw?.selectionScope || "").trim().toLowerCase();
  const selectionScopeText = String(raw?.selection_scope_text || raw?.selectionScopeText || "").trim() || formatAlarmSelectionScopeText(selectionScope);
  const explicitStatusKey = String(raw?.status_key || raw?.statusKey || raw?.status || (blocked ? "blocked" : "")).trim().toLowerCase() || "pending_backend";
  const backendStatusText = String(raw?.status_text || raw?.statusText || "").trim();
  const backendDetailText = String(raw?.detail_text || raw?.detailText || "").trim();
  const backendTone = String(raw?.tone || "").trim();
  const backendMetaLines = Array.isArray(raw?.meta_lines)
    ? raw.meta_lines.map((item) => String(item || "").trim()).filter(Boolean)
    : Array.isArray(raw?.metaLines)
      ? raw.metaLines.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
  const mappedActions = mapBackendActionsState(raw?.actions);
  const hasMeaningfulRawState = (
    explicitStatusKey !== "pending_backend"
    || Boolean(downloadedAt)
    || Boolean(lastError)
    || Boolean(relativePath)
    || Boolean(resolvedFilePath)
    || blocked
    || Boolean(blockedReason)
    || Boolean(sourceKind)
    || Boolean(selectionScope)
    || Boolean(selectedDownloadedAt)
  );
  if (!backendStatusText && !backendDetailText && !backendMetaLines.length && !Object.keys(mappedActions).length) {
    if (hasMeaningfulRawState) {
      let tone = "neutral";
      let stateText = "等待中";
      if (explicitStatusKey === "ready") {
        tone = "success";
        stateText = "已就绪";
      } else if (explicitStatusKey === "downloading") {
        tone = "info";
        stateText = "下载中";
      } else if (explicitStatusKey === "consumed") {
        tone = "info";
        stateText = "已消费";
      } else if (explicitStatusKey === "failed") {
        tone = "danger";
        stateText = "失败";
      } else if (blocked) {
        tone = "warning";
      stateText = "等待采集端恢复";
      } else if (selectionScope === "missing") {
        tone = "warning";
        stateText = "今天和昨天都缺文件";
      }
      return {
        building,
        bucketKey,
        sourceFamily,
        source_family: sourceFamily,
        statusKey: blocked ? "blocked" : explicitStatusKey,
        reasonCode: blocked ? "blocked" : explicitStatusKey,
        usingFallback: false,
        versionGap: null,
        downloadedAt,
        lastError,
        blocked,
        blockedReason,
        relativePath,
        resolvedFilePath,
        tone,
        stateText,
        sourceKind,
        sourceKindText,
        selectionScope,
        selectionScopeText,
        selectedDownloadedAt,
        detailText: blocked
      ? (blockedReason || "等待采集端恢复")
          : (selectionScope === "missing"
            ? "今天和昨天都缺文件"
            : (lastError || selectedDownloadedAt || "等待共享文件就绪")),
        metaLines: [],
        actions: mappedActions,
      };
    }
    return {
      ...buildSourceCachePlaceholderBuilding(building, bucketKey, sourceFamily),
      building,
      bucketKey,
      sourceFamily,
      source_family: sourceFamily,
      statusKey: "pending_backend",
      reasonCode: "pending_backend",
      downloadedAt,
      lastError,
      blocked,
      blockedReason,
      relativePath,
      resolvedFilePath,
      tone: "neutral",
      stateText: "暂无状态",
      sourceKind,
      sourceKindText,
      selectionScope,
      selectionScopeText,
      selectedDownloadedAt,
      detailText: "等待后端状态",
      metaLines: [],
      actions: {},
    };
  }
  return {
    building,
    bucketKey,
    sourceFamily,
    source_family: sourceFamily,
    statusKey: explicitStatusKey,
    reasonCode: String(raw?.reason_code || raw?.reasonCode || explicitStatusKey || "").trim().toLowerCase() || "unknown",
    usingFallback: false,
    versionGap: null,
    downloadedAt,
    lastError,
    blocked,
    blockedReason,
    relativePath,
    resolvedFilePath,
    tone: backendTone || "neutral",
    stateText: backendStatusText || "等待后端状态",
    sourceKind,
    sourceKindText,
    selectionScope,
    selectionScopeText,
    selectedDownloadedAt,
    detailText: backendDetailText || "等待后端状态",
    metaLines: backendMetaLines,
    actions: mappedActions,
  };
}

export function normalizeInternalDownloadPoolSlot(slot, { formatInternalDownloadPoolError }) {
  const building = String(slot?.building || "").trim() || "-";
  const backendStatusText = String(slot?.status_text || slot?.statusText || "").trim();
  const backendDetailText = String(slot?.detail_text || slot?.detailText || "").trim();
  const backendTone = String(slot?.tone || "").trim();
  const backendLoginText = String(slot?.login_text || slot?.loginText || "").trim();
  const backendLoginTone = String(slot?.login_tone || slot?.loginTone || "").trim();
  const pageReady = Boolean(slot?.page_ready);
  const inUse = Boolean(slot?.in_use);
  const suspended = Boolean(slot?.suspended);
  const suspendReason = formatInternalDownloadPoolError(slot?.suspend_reason || slot?.pending_issue_summary);
  const failureKind = String(slot?.failure_kind || "").trim().toLowerCase();
  const recoveryAttempts = Number.parseInt(String(slot?.recovery_attempts || 0), 10) || 0;
  const nextProbeAt = String(slot?.next_probe_at || "").trim();
  const lastUsedAt = String(slot?.last_used_at || "").trim();
  const lastLoginAt = String(slot?.last_login_at || "").trim();
  const lastResult = String(slot?.last_result || "").trim().toLowerCase();
  const lastError = formatInternalDownloadPoolError(slot?.last_error);
  const loginError = formatInternalDownloadPoolError(slot?.login_error);
  const loginState = String(slot?.login_state || "").trim().toLowerCase();
  if (!backendStatusText && !backendDetailText && !backendLoginText) {
    let tone = "neutral";
    let stateText = "未建页";
    let loginTone = "neutral";
    let loginText = "待初始化";
    let detailText = "";
    if (suspended) {
      tone = "warning";
      stateText = "已暂停等待恢复";
      loginTone = "warning";
      loginText = "页面异常";
      detailText = suspendReason || lastError || loginError || "等待自动恢复";
      if (nextProbeAt) {
        detailText = detailText ? `${detailText}；下次自动检测：${nextProbeAt}` : `下次自动检测：${nextProbeAt}`;
      }
    } else if (inUse || lastResult === "running") {
      tone = "info";
      stateText = "使用中";
    } else if (pageReady && (loginState === "ready" || lastResult === "ready")) {
      tone = "success";
      stateText = "待命";
      loginTone = "success";
      loginText = "已登录";
    } else if (loginState === "failed" || lastResult === "failed" || lastError || loginError) {
      tone = "warning";
      stateText = pageReady ? "最近失败" : "未建页";
      loginTone = "warning";
      loginText = failureKind === "page_unreachable" ? "页面异常" : "登录失败";
      detailText = lastError || loginError || "登录失败，请稍后重试";
    } else if (loginState === "logging_in") {
      tone = "info";
      stateText = pageReady ? "待命" : "未建页";
      loginTone = "info";
      loginText = "登录中";
    }
    return {
      building,
      pageReady,
      inUse,
      lastUsedAt,
      lastLoginAt,
      lastResult,
      lastError,
      suspended,
      suspendReason,
      failureKind,
      recoveryAttempts,
      nextProbeAt,
      loginState,
      loginTone,
      loginText,
      loginError,
      tone,
      stateText,
      detailText,
    };
  }
  return {
    building,
    pageReady,
    inUse,
    lastUsedAt,
    lastLoginAt,
    lastResult,
    lastError,
    suspended,
    suspendReason,
    failureKind,
    recoveryAttempts,
    nextProbeAt,
    loginState,
    loginTone: backendLoginTone || "neutral",
    loginText: backendLoginText || "等待后端状态",
    loginError,
    tone: backendTone || "neutral",
    stateText: backendStatusText || "等待后端状态",
    detailText: backendDetailText || "等待后端状态",
  };
}

export function mapPresentedSourceCacheFamilyOverview(
  payload,
  {
    fallbackBucket = "",
    internalBuildings = [],
    formatSharedBridgeRuntimeError,
    formatInternalDownloadPoolError,
  } = {},
) {
  const family = payload && typeof payload === "object" ? payload : {};
  const rawBuildings = Array.isArray(family.buildings) ? family.buildings : [];
  const rawTitle = String(family.title || family.display_title || family.displayTitle || "").trim();
  const inferredBuildingSourceFamily = rawBuildings
    .map((item) => String(item?.source_family || item?.sourceFamily || "").trim())
    .find(Boolean) || "";
  const key = String(
    family.key
    || family.source_family
    || family.sourceFamily
    || inferredBuildingSourceFamily
    || resolveSourceCacheFamilyKeyByTitle(rawTitle)
    || "",
  ).trim();
  const title = rawTitle || resolveSourceCacheFamilyTitle(key);
  const currentBucket =
    String(
      family.current_bucket
      || family.currentBucket
      || family.best_bucket_key
      || family.bestBucketKey
      || fallbackBucket
      || "",
    ).trim();
  const mapBuilding = (item) => {
    const base = key === "alarm_event_family"
      ? normalizeAlarmEventReadinessBuilding(item, currentBucket, formatSharedBridgeRuntimeError, formatInternalDownloadPoolError)
      : normalizeSourceCacheBuildingStatus(item, currentBucket, formatSharedBridgeRuntimeError, formatInternalDownloadPoolError);
    const usingFallback = key === "alarm_event_family"
      ? Boolean(base?.usingFallback)
      : Boolean(item?.using_fallback ?? item?.usingFallback ?? base?.usingFallback);
    const rawVersionGap = key === "alarm_event_family"
      ? base?.versionGap
      : Number.isInteger(item?.version_gap)
        ? item.version_gap
        : Number.isInteger(item?.versionGap)
          ? item.versionGap
          : Number.parseInt(String(item?.version_gap ?? item?.versionGap ?? base?.versionGap ?? ""), 10);
    const buildingSourceFamily = String(item?.source_family || item?.sourceFamily || base?.source_family || base?.sourceFamily || key).trim();
    return {
      ...base,
      sourceFamily: buildingSourceFamily,
      source_family: buildingSourceFamily,
      usingFallback,
      versionGap: Number.isFinite(rawVersionGap) ? Math.max(0, rawVersionGap) : null,
      backfillRunning: Boolean(item?.backfill_running ?? item?.backfillRunning),
      backfillText: String(item?.backfill_text || item?.backfillText || "").trim(),
      backfillScopeText: String(item?.backfill_scope_text || item?.backfillScopeText || "").trim(),
      backfillTaskId: String(item?.backfill_task_id || item?.backfillTaskId || "").trim(),
    };
  };
  const normalizedBuildings = Array.isArray(family.buildings)
    ? family.buildings.map((item) => mapBuilding(item))
    : [];
  const buildingMap = new Map(
    normalizedBuildings
      .filter((item) => item && typeof item === "object")
      .map((item) => [String(item.building || "").trim(), item]),
  );
  const buildings = Array.isArray(internalBuildings) && internalBuildings.length > 0
    ? internalBuildings.map((building) =>
      buildingMap.get(building)
      || buildSourceCachePlaceholderBuilding(building, currentBucket, key),
    )
    : normalizedBuildings;
  const readyCount = parseOptionalCount(family.ready_count ?? family.readyCount);
  const liveReadyCount = parseOptionalCount(
    family.live_ready_count ?? family.liveReadyCount ?? family.ready_count ?? family.readyCount,
  );
  const liveDownloadingCount = parseOptionalCount(family.live_downloading_count ?? family.liveDownloadingCount);
  const liveFailedCount = parseOptionalCount(family.live_failed_count ?? family.liveFailedCount);
  const liveBlockedCount = parseOptionalCount(family.live_blocked_count ?? family.liveBlockedCount);
  const manualRefreshRaw = family.manual_refresh && typeof family.manual_refresh === "object"
    ? family.manual_refresh
    : (family.manualRefresh && typeof family.manualRefresh === "object" ? family.manualRefresh : {});
  const fallbackBuildings = Array.isArray(family.fallback_buildings)
    ? family.fallback_buildings.map((item) => String(item || "").trim()).filter(Boolean)
    : Array.isArray(family.fallbackBuildings)
      ? family.fallbackBuildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
  const missingBuildings = Array.isArray(family.missing_buildings)
    ? family.missing_buildings.map((item) => String(item || "").trim()).filter(Boolean)
    : Array.isArray(family.missingBuildings)
      ? family.missingBuildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
  const staleBuildings = Array.isArray(family.stale_buildings)
    ? family.stale_buildings.map((item) => String(item || "").trim()).filter(Boolean)
    : Array.isArray(family.staleBuildings)
      ? family.staleBuildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
  const rawStatusText = String(family.status_text || family.statusText || "").trim();
  const rawSummaryText = String(family.summary_text || family.summaryText || "").trim();
  const hasDerivedReady = buildings.some((item) => String(item?.statusKey || "").trim().toLowerCase() === "ready");
  let derivedTone = "neutral";
  let derivedStatusText = "等待后端状态";
  let derivedSummaryText = "";
  if (!rawStatusText && !rawSummaryText) {
    if (key === "alarm_event_family") {
      const missingTodayBuildings = Array.isArray(family.missing_today_buildings)
        ? family.missing_today_buildings.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      const missingBothDaysBuildings = Array.isArray(family.missing_both_days_buildings)
        ? family.missing_both_days_buildings.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      const usedPreviousDayFallback = Array.isArray(family.used_previous_day_fallback)
        ? family.used_previous_day_fallback.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      derivedSummaryText = "当前策略：当天最新一份，缺失则回退昨天最新。";
      if (missingBothDaysBuildings.length || missingTodayBuildings.length) {
        derivedTone = "warning";
        derivedStatusText = "存在缺失楼栋";
      } else if (usedPreviousDayFallback.length) {
        derivedTone = "warning";
        derivedStatusText = "已回退昨天最新";
      } else if (hasDerivedReady) {
        derivedTone = "success";
        derivedStatusText = "当天最新文件已就绪";
      }
    } else if (staleBuildings.length) {
      derivedTone = "warning";
      derivedStatusText = "存在过旧楼栋";
      derivedSummaryText = "部分楼栋共享文件版本过旧，等待更新后会自动重试默认入口。";
    } else if (fallbackBuildings.length) {
      derivedTone = "warning";
      derivedStatusText = "已允许回退";
      derivedSummaryText = "部分楼栋暂时使用上一版共享文件。";
    } else if (missingBuildings.length) {
      derivedTone = "warning";
      derivedStatusText = "等待共享文件就绪";
      derivedSummaryText = "仍有楼栋等待共享文件就绪。";
    } else if (hasDerivedReady) {
      derivedTone = "success";
      derivedStatusText = "共享文件已就绪";
      derivedSummaryText = "当前参考桶的共享文件已准备完成。";
    }
  }
  return {
    ...family,
    key,
    title,
    metaLines: Array.isArray(family.meta_lines)
      ? family.meta_lines.map((item) => String(item || "").trim()).filter(Boolean)
      : Array.isArray(family.metaLines)
        ? family.metaLines.map((item) => String(item || "").trim()).filter(Boolean)
        : [],
    currentBucket,
    bestBucketKey: String(family.best_bucket_key || family.bestBucketKey || "").trim(),
    bestBucketAgeHours: Number.isFinite(Number.parseFloat(String(family.best_bucket_age_hours ?? family.bestBucketAgeHours ?? "")))
      ? Number.parseFloat(String(family.best_bucket_age_hours ?? family.bestBucketAgeHours ?? ""))
      : null,
    bestBucketAgeText: String(family.best_bucket_age_text || family.bestBucketAgeText || "").trim(),
    isBestBucketTooOld: Boolean(family.is_best_bucket_too_old ?? family.isBestBucketTooOld),
    fallbackBuildings,
    missingBuildings,
    staleBuildings,
    readyCount: readyCount ?? 0,
    failedBuildings: Array.isArray(family.failed_buildings)
      ? family.failed_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : Array.isArray(family.failedBuildings)
        ? family.failedBuildings.map((item) => String(item || "").trim()).filter(Boolean)
        : [],
    blockedBuildings: Array.isArray(family.blocked_buildings)
      ? family.blocked_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : Array.isArray(family.blockedBuildings)
        ? family.blockedBuildings.map((item) => String(item || "").trim()).filter(Boolean)
        : [],
    lastSuccessAt: String(family.last_success_at || family.lastSuccessAt || "").trim(),
    hasFailures: typeof (family.has_failures ?? family.hasFailures) === "boolean"
      ? Boolean(family.has_failures ?? family.hasFailures)
      : false,
    hasBlocked: typeof (family.has_blocked ?? family.hasBlocked) === "boolean"
      ? Boolean(family.has_blocked ?? family.hasBlocked)
      : false,
    hasDownloading: typeof (family.has_downloading ?? family.hasDownloading) === "boolean"
      ? Boolean(family.has_downloading ?? family.hasDownloading)
      : false,
    allReady: typeof (family.all_ready ?? family.allReady) === "boolean"
      ? Boolean(family.all_ready ?? family.allReady)
      : false,
    reasonCode: String(family.reason_code || family.reasonCode || "").trim().toLowerCase() || "unknown",
    tone: String(family.tone || "").trim() || derivedTone || "neutral",
    statusText: rawStatusText || derivedStatusText || "等待后端状态",
    summaryText: rawSummaryText || derivedSummaryText,
    detailText: String(family.detail_text || family.detailText || "").trim(),
    items: Array.isArray(family.items)
      ? family.items
        .filter((item) => item && typeof item === "object")
        .map((item) => ({
          label: String(item.label || "").trim(),
          value: String(item.value ?? "").trim(),
          tone: String(item.tone || "").trim() || "neutral",
        }))
      : [],
    backfillRunning: Boolean(family.backfill_running ?? family.backfillRunning),
    backfillText: String(family.backfill_text || family.backfillText || "").trim(),
    backfillScopeText: String(family.backfill_scope_text || family.backfillScopeText || "").trim(),
    backfillTaskId: String(family.backfill_task_id || family.backfillTaskId || "").trim(),
    backfillLabel: String(family.backfill_label || family.backfillLabel || "").trim(),
    backfillScopeLabel: String(family.backfill_scope_label || family.backfillScopeLabel || "").trim(),
    displayNoteText: String(family.display_note_text || family.displayNoteText || "").trim(),
    errorText: String(family.error_text || family.errorText || "").trim(),
    buildings,
    canProceed: typeof (family.can_proceed ?? family.canProceed) === "boolean"
      ? Boolean(family.can_proceed ?? family.canProceed)
      : false,
    liveReadyCount: liveReadyCount ?? 0,
    liveDownloadingCount: liveDownloadingCount ?? 0,
    liveFailedCount: liveFailedCount ?? 0,
    liveBlockedCount: liveBlockedCount ?? 0,
    uploadLastRunAt: String(family.upload_last_run_at || family.uploadLastRunAt || "").trim(),
    uploadLastSuccessAt: String(family.upload_last_success_at || family.uploadLastSuccessAt || "").trim(),
    uploadLastError: String(family.upload_last_error || family.uploadLastError || "").trim(),
    uploadRecordCount: Number.parseInt(String(family.upload_record_count ?? family.uploadRecordCount ?? 0), 10) || 0,
    uploadFileCount: Number.parseInt(String(family.upload_file_count ?? family.uploadFileCount ?? 0), 10) || 0,
    uploadRunning: Boolean(family.upload_running ?? family.uploadRunning),
    uploadStartedAt: String(family.upload_started_at || family.uploadStartedAt || "").trim(),
    uploadCurrentMode: String(family.upload_current_mode || family.uploadCurrentMode || "").trim(),
    uploadCurrentScope: String(family.upload_current_scope || family.uploadCurrentScope || "").trim(),
    uploadRunningText: String(family.upload_running_text || family.uploadRunningText || "").trim(),
    uploadStatus: family.upload_status && typeof family.upload_status === "object"
      ? {
        tone: String(family.upload_status.tone || "").trim(),
        statusText: String(family.upload_status.status_text || family.upload_status.statusText || "").trim(),
        summaryText: String(family.upload_status.summary_text || family.upload_status.summaryText || "").trim(),
      }
      : {},
    manualRefresh: {
      running: Boolean(manualRefreshRaw.running),
      lastRunAt: String(manualRefreshRaw.last_run_at || manualRefreshRaw.lastRunAt || "").trim(),
      lastSuccessAt: String(manualRefreshRaw.last_success_at || manualRefreshRaw.lastSuccessAt || "").trim(),
      lastError: String(manualRefreshRaw.last_error || manualRefreshRaw.lastError || "").trim(),
      bucketKey: String(manualRefreshRaw.bucket_key || manualRefreshRaw.bucketKey || "").trim(),
      successfulBuildings: Array.isArray(manualRefreshRaw.successful_buildings)
        ? manualRefreshRaw.successful_buildings.map((item) => String(item || "").trim()).filter(Boolean)
        : Array.isArray(manualRefreshRaw.successfulBuildings)
          ? manualRefreshRaw.successfulBuildings.map((item) => String(item || "").trim()).filter(Boolean)
          : [],
      failedBuildings: Array.isArray(manualRefreshRaw.failed_buildings)
        ? manualRefreshRaw.failed_buildings.map((item) => String(item || "").trim()).filter(Boolean)
        : Array.isArray(manualRefreshRaw.failedBuildings)
          ? manualRefreshRaw.failedBuildings.map((item) => String(item || "").trim()).filter(Boolean)
          : [],
      blockedBuildings: Array.isArray(manualRefreshRaw.blocked_buildings)
        ? manualRefreshRaw.blocked_buildings.map((item) => String(item || "").trim()).filter(Boolean)
        : Array.isArray(manualRefreshRaw.blockedBuildings)
          ? manualRefreshRaw.blockedBuildings.map((item) => String(item || "").trim()).filter(Boolean)
          : [],
      totalRowCount: Number.parseInt(String(manualRefreshRaw.total_row_count ?? manualRefreshRaw.totalRowCount ?? 0), 10) || 0,
      buildingRowCounts: manualRefreshRaw.building_row_counts && typeof manualRefreshRaw.building_row_counts === "object"
        ? manualRefreshRaw.building_row_counts
        : (manualRefreshRaw.buildingRowCounts && typeof manualRefreshRaw.buildingRowCounts === "object" ? manualRefreshRaw.buildingRowCounts : {}),
      queryStart: String(manualRefreshRaw.query_start || manualRefreshRaw.queryStart || "").trim(),
      queryEnd: String(manualRefreshRaw.query_end || manualRefreshRaw.queryEnd || "").trim(),
    },
    actions: mapBackendActionsState(family.actions),
    selectionPolicy: String(family.selection_policy || family.selectionPolicy || "").trim(),
    selectionReferenceDate: String(family.selection_reference_date || family.selectionReferenceDate || "").trim(),
    usedPreviousDayFallback: Array.isArray(family.used_previous_day_fallback)
      ? family.used_previous_day_fallback.map((item) => String(item || "").trim()).filter(Boolean)
      : [],
    missingTodayBuildings: Array.isArray(family.missing_today_buildings)
      ? family.missing_today_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [],
    missingBothDaysBuildings: Array.isArray(family.missing_both_days_buildings)
      ? family.missing_both_days_buildings.map((item) => String(item || "").trim()).filter(Boolean)
      : [],
    todaySelectedCount: Number.parseInt(String(family.today_selected_count ?? family.todaySelectedCount ?? 0), 10) || 0,
  };
}

export function mapPresentedInternalAlertOverview(payload, { internalBuildings = [] } = {}) {
  const overview = payload && typeof payload === "object" ? payload : {};
  const rawBuildings = Array.isArray(overview.buildings) ? overview.buildings : [];
  const buildingMap = new Map(
    rawBuildings
      .filter((item) => item && typeof item === "object")
      .map((item) => [String(item.building || "").trim(), item]),
  );
  const buildings = internalBuildings.map((building) => {
    const raw = buildingMap.get(building) || { building };
    return {
      building,
      tone: String(raw.tone || "").trim() || "neutral",
      statusText: String(raw.status_text || raw.statusText || "").trim() || "未知",
      summaryText: String(raw.summary_text || raw.summaryText || "").trim(),
      detailText: String(raw.detail_text || raw.detailText || "").trim(),
      timeText: String(raw.time_text || raw.timeText || "").trim(),
      activeCount: Number.parseInt(String(raw.active_count ?? raw.activeCount ?? 0), 10) || 0,
    };
  });
  return {
    tone: String(overview.tone || "").trim() || "neutral",
    statusText: String(overview.status_text || overview.statusText || "").trim() || "未知",
    summaryText: String(overview.summary_text || overview.summaryText || "").trim(),
    detailText: String(overview.detail_text || overview.detailText || "").trim(),
    items: Array.isArray(overview.items)
      ? overview.items
        .filter((item) => item && typeof item === "object")
        .map((item) => ({
          label: String(item.label || "").trim(),
          value: String(item.value ?? "").trim(),
          tone: String(item.tone || "").trim() || "neutral",
        }))
      : [],
    buildings,
  };
}
