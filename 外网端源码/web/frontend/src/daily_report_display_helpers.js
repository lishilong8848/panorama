import { mapBackendActionState } from "./backend_action_display_helpers.js";

function basenameFromPath(input) {
  const text = String(input || "").trim();
  if (!text) return "";
  const parts = text.split(/[\\/]/).filter(Boolean);
  return parts.length ? parts[parts.length - 1] : text;
}

export function getDailyReportBrowserLabel(rawAuth, fallback = "系统浏览器") {
  const label = String(rawAuth?.browser_label || "").trim();
  return label || fallback;
}

export function formatSharedBridgeRuntimeError(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const normalized = text.toLowerCase();
  if (normalized === "shared_bridge_disabled") return "共享桥接未启用";
  if (normalized === "shared_bridge_service_unavailable") return "共享桥接服务不可用";
  if (normalized === "disabled_or_switching" || normalized === "disabled_or_unselected") return "当前未启用共享桥接";
  if (normalized === "misconfigured") return "共享桥接目录未配置";
  if (normalized === "database is locked") return "共享桥接数据库正忙，请稍后重试";
  if (normalized === "unable to open database file") return "无法打开共享桥接数据库文件";
  if (normalized === "cannot operate on a closed database" || normalized === "cannot operate on a closed database.") {
    return "共享桥接数据库连接已关闭";
  }
  if (normalized.includes("permissionerror") || normalized.includes("winerror 5")) {
    return "共享桥接目录无写入权限";
  }
  if (normalized.includes("no such table")) {
    return "共享桥接数据库结构未初始化";
  }
  return text;
}

export function formatInternalDownloadPoolError(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const normalized = text.toLowerCase();
  if (normalized.includes("net::err_empty_response")) {
    return "页面无响应，请检查楼栋页面服务或网络";
  }
  if (normalized.includes("net::err_connection_refused")) {
    return "页面拒绝连接，请检查楼栋页面服务是否启动";
  }
  if (normalized.includes("net::err_connection_timed_out") || normalized.includes("net::err_timed_out")) {
    return "页面访问超时，请检查楼栋网络或站点状态";
  }
  if (normalized.includes("net::err_name_not_resolved")) {
    return "页面地址无法解析，请检查楼栋地址配置";
  }
  if (normalized.includes("net::err_internet_disconnected")) {
    return "网络未连接，请检查当前网络";
  }
  if (normalized.includes("browser_context_missing")) {
    return "浏览器上下文不可用，系统会自动重建";
  }
  if (normalized.includes("browser_unavailable")) {
    return "浏览器不可用，请检查系统浏览器和 Playwright";
  }
  if (normalized.includes("login_required")) {
    return "登录态已失效，任务开始前会自动重登";
  }
  if (normalized.includes("page.goto:")) {
    return "页面访问失败，请检查楼栋页面是否可达";
  }
  return formatSharedBridgeRuntimeError(text);
}

export function mapDailyReportScreenshotTestVm(raw, options = {}) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const rawError = String(raw?.error || "").trim();
  const rawErrorMessage = String(raw?.error_message || "").trim();
  const browserLabel = String(options.browserLabel || "").trim() || "系统浏览器";
  let error = rawErrorMessage || rawError;
  if (rawError === "target_page_not_open") {
    error = `目标网页当前没有在${browserLabel}中打开，请先打开对应页面后再测试截图。`;
  } else if (rawError === "target_page_mismatch") {
    error = "当前打开页面与目标页面不一致，请重新打开对应飞书页面后重试。";
  } else if (rawError === "login_required") {
    error = `当前${browserLabel}中的飞书登录态未就绪，请先完成扫码登录。`;
  } else if (rawError === "timeout") {
    error = "截图操作超时，请查看系统错误日志后重试。";
  }
  const path = String(raw?.path || raw?.stored_path || "").trim();
  const capturedAt = String(raw?.captured_at || "").trim();
  const fallbackExists = Boolean(options.fallbackExists);
  const fallbackPath = String(options.fallbackPath || "").trim();
  const fallbackCapturedAt = String(options.fallbackCapturedAt || "").trim();
  const skippedText = String(options.skippedText || "本次测试已跳过").trim() || "本次测试已跳过";
  if (status === "ok") {
    return {
      text: "最近测试成功",
      tone: "success",
      error: "",
      detail: capturedAt || basenameFromPath(path) || "",
    };
  }
  if (status === "skipped") {
    return {
      text: skippedText,
      tone: "neutral",
      error,
      detail: "",
    };
  }
  if (status === "failed" || status === "error") {
    return {
      text: "最近测试失败",
      tone: "danger",
      error: error || "截图测试失败",
      detail: "",
    };
  }
  if (fallbackExists) {
    return {
      text: "已有截图文件",
      tone: "info",
      error: "",
      detail: fallbackCapturedAt || basenameFromPath(fallbackPath) || "",
    };
  }
  return { text: "尚未测试", tone: "neutral", error: "", detail: "" };
}

export function emptyDailyReportAssetVariant() {
  return {
    exists: false,
    stored_path: "",
    captured_at: "",
    preview_url: "",
    thumbnail_url: "",
    full_image_url: "",
  };
}

export function buildDailyReportAssetDownloadName(dutyDate, dutyShift, title) {
  const dutyDateText = String(dutyDate || "").trim() || "unknown-date";
  const dutyShiftText = String(dutyShift || "").trim().toLowerCase() || "unknown-shift";
  const titleText = String(title || "").trim() || "日报截图";
  return `${dutyDateText}_${dutyShiftText}_${titleText}.png`;
}

export function mapDailyReportLastWrittenSourceVm(source) {
  const text = String(source || "").trim().toLowerCase();
  if (text === "manual") {
    return { text: "上次入库：手工图", tone: "warning", exists: true };
  }
  if (text === "auto") {
    return { text: "上次入库：自动图", tone: "info", exists: true };
  }
  return { text: "尚未写入日报", tone: "neutral", exists: false };
}

export function normalizeDailyReportAssetCard(raw, title, options = {}) {
  const asset = raw && typeof raw === "object" ? raw : {};
  const auto = asset.auto && typeof asset.auto === "object" ? asset.auto : emptyDailyReportAssetVariant();
  const manual = asset.manual && typeof asset.manual === "object" ? asset.manual : emptyDailyReportAssetVariant();
  const source = String(asset.source || "").trim().toLowerCase();
  const effectiveExists = Boolean(asset.exists);
  const effectiveThumbnailUrl = String(asset.thumbnail_url || asset.preview_url || "").trim();
  const effectiveFullImageUrl = String(asset.full_image_url || asset.preview_url || "").trim();
  const effectiveCapturedAt = String(asset.captured_at || "").trim();
  const dutyDate = String(options.dutyDate || "").trim();
  const dutyShift = String(options.dutyShift || "").trim().toLowerCase();
  const lastWrittenSourceVm = mapDailyReportLastWrittenSourceVm(options.lastWrittenSource || "");
  const sourceText =
    source === "manual" ? "手工" : source === "auto" ? "自动" : "未生成";
  return {
    title,
    exists: effectiveExists,
    source,
    sourceText,
    stored_path: String(asset.stored_path || "").trim(),
    captured_at: effectiveCapturedAt,
    preview_url: effectiveThumbnailUrl,
    thumbnail_url: effectiveThumbnailUrl,
    full_image_url: effectiveFullImageUrl,
    auto,
    manual,
    hasManual: Boolean(manual.exists),
    hasAuto: Boolean(auto.exists),
    downloadName: buildDailyReportAssetDownloadName(dutyDate, dutyShift, title),
    lastWrittenSource: String(options.lastWrittenSource || "").trim().toLowerCase(),
    lastWrittenSourceText: lastWrittenSourceVm.text,
    lastWrittenSourceTone: lastWrittenSourceVm.tone,
    hasLastWrittenSource: lastWrittenSourceVm.exists,
    actions: {
      preview: mapBackendActionState(asset.actions?.preview || {
        allowed: effectiveExists,
        label: "放大查看",
        disabled_reason: effectiveExists ? "" : `当前还没有${title}`,
        reason_code: effectiveExists ? "" : "missing_asset",
      }),
      recapture: mapBackendActionState(asset.actions?.recapture || { allowed: true, label: "重新截图" }),
      upload: mapBackendActionState(asset.actions?.upload || { allowed: true, label: "上传/粘贴替换" }),
      restore_auto: mapBackendActionState(asset.actions?.restore_auto || {
        allowed: Boolean(manual.exists),
        label: "恢复自动图",
        disabled_reason: manual.exists ? "" : "当前没有手工替换图",
        reason_code: manual.exists ? "" : "manual_asset_missing",
      }),
    },
  };
}

export function mapBackendDailyReportAssetCard(raw, fallbackTitle = "日报截图") {
  const payload = raw && typeof raw === "object" ? raw : {};
  const title = String(payload.title || "").trim() || fallbackTitle;
  const auto = payload.auto && typeof payload.auto === "object" ? payload.auto : emptyDailyReportAssetVariant();
  const manual = payload.manual && typeof payload.manual === "object" ? payload.manual : emptyDailyReportAssetVariant();
  return {
    title,
    exists: Boolean(payload.exists),
    source: String(payload.source || "").trim().toLowerCase(),
    sourceText: String(payload.source_text || payload.sourceText || "").trim() || "未生成",
    stored_path: String(payload.stored_path || payload.storedPath || "").trim(),
    captured_at: String(payload.captured_at || payload.capturedAt || "").trim(),
    preview_url: String(payload.preview_url || payload.previewUrl || payload.thumbnail_url || payload.thumbnailUrl || "").trim(),
    thumbnail_url: String(payload.thumbnail_url || payload.thumbnailUrl || payload.preview_url || payload.previewUrl || "").trim(),
    full_image_url: String(payload.full_image_url || payload.fullImageUrl || payload.preview_url || payload.previewUrl || "").trim(),
    auto,
    manual,
    hasManual: Boolean(payload.has_manual ?? payload.hasManual ?? manual.exists),
    hasAuto: Boolean(payload.has_auto ?? payload.hasAuto ?? auto.exists),
    downloadName: String(payload.download_name || payload.downloadName || "").trim() || buildDailyReportAssetDownloadName("", "", title),
    lastWrittenSource: String(payload.last_written_source || payload.lastWrittenSource || "").trim().toLowerCase(),
    lastWrittenSourceText: String(payload.last_written_source_text || payload.lastWrittenSourceText || "").trim() || "尚未写入日报",
    lastWrittenSourceTone: String(payload.last_written_source_tone || payload.lastWrittenSourceTone || "").trim() || "neutral",
    hasLastWrittenSource: Boolean(payload.has_last_written_source ?? payload.hasLastWrittenSource),
    actions: {
      preview: mapBackendActionState(payload.actions?.preview || {
        allowed: Boolean(payload.exists),
        label: "放大查看",
        disabled_reason: payload.exists ? "" : `当前还没有${title}`,
      }),
      recapture: mapBackendActionState(payload.actions?.recapture || { allowed: true, label: "重新截图" }),
      upload: mapBackendActionState(payload.actions?.upload || { allowed: true, label: "上传/粘贴替换" }),
      restore_auto: mapBackendActionState(payload.actions?.restore_auto || {
        allowed: Boolean(payload.has_manual ?? payload.hasManual ?? manual.exists),
        label: "恢复自动图",
        disabled_reason: Boolean(payload.has_manual ?? payload.hasManual ?? manual.exists) ? "" : "当前没有手工替换图",
      }),
    },
  };
}
