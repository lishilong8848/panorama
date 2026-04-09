function normalizeText(value, fallback = "") {
  const text = String(value || "").trim();
  return text || fallback;
}

function normalizeTone(value, fallback = "neutral") {
  const text = normalizeText(value, fallback).toLowerCase();
  return ["success", "warning", "danger", "info", "neutral"].includes(text) ? text : fallback;
}

function buildFamilyItems(rawFamilies) {
  const rows = Array.isArray(rawFamilies) ? rawFamilies : [];
  return rows
    .filter((item) => item && typeof item === "object")
    .map((item) => ({
      key: normalizeText(item.key),
      title: normalizeText(item.title, "-"),
      tone: normalizeTone(item.tone, "neutral"),
      statusText: normalizeText(item.status_text, "未检查"),
      summaryText: normalizeText(item.summary_text),
      pathText: normalizeText(item.path, "-"),
      readyEntryCount: Number.parseInt(String(item.ready_entry_count || 0), 10) || 0,
      accessibleReadyCount: Number.parseInt(String(item.accessible_ready_count || 0), 10) || 0,
      missingReadyCount: Number.parseInt(String(item.missing_ready_count || 0), 10) || 0,
      latestDownloadedAt: normalizeText(item.latest_downloaded_at, "-"),
      sampleReadyPath: normalizeText(item.sample_ready_path),
      sampleMissingPath: normalizeText(item.sample_missing_path),
      queryError: normalizeText(item.query_error),
    }));
}

export function buildSharedBridgeSelfCheckOverview(raw, healthSharedBridge = {}, deployment = {}) {
  const payload = raw && typeof raw === "object" ? raw : {};
  const summary = payload.summary && typeof payload.summary === "object" ? payload.summary : {};
  const familyItems = buildFamilyItems(payload.families);
  const roleMode = normalizeText(payload.role_mode || deployment.role_mode).toLowerCase();
  const roleLabel = normalizeText(
    payload.role_label,
    roleMode === "internal" ? "内网端" : roleMode === "external" ? "外网端" : "当前角色",
  );
  const rootDirText = normalizeText(payload.root_dir || healthSharedBridge.root_dir, "未配置");
  const dbPathText = normalizeText(
    payload.db_path,
    rootDirText && rootDirText !== "未配置" ? `${rootDirText}\\bridge.db` : "未配置",
  );
  const checkedAtText = normalizeText(payload.checked_at);
  const errorText = normalizeText(payload.error);
  const readyEntryCount = Number.parseInt(String(summary.ready_entry_count || 0), 10) || 0;
  const accessibleReadyCount = Number.parseInt(String(summary.accessible_ready_count || 0), 10) || 0;
  const missingReadyCount = Number.parseInt(String(summary.missing_ready_count || 0), 10) || 0;
  const initializedCount = Number.parseInt(String(summary.initialized_count || 0), 10) || 0;
  const statusText = normalizeText(
    payload.status_text,
    rootDirText === "未配置" ? "共享目录未配置" : "尚未执行自检",
  );
  const tone = normalizeTone(
    payload.tone,
    rootDirText === "未配置" ? "danger" : familyItems.length ? "info" : "neutral",
  );
  let summaryText = normalizeText(payload.message);
  if (!summaryText) {
    if (rootDirText === "未配置") {
      summaryText = "请先在配置中心填写当前角色对应的共享目录。";
    } else {
      summaryText = "按钮会补齐共享桥接、源文件缓存和临时目录，并检查当前角色能否真实看到 ready 文件。";
    }
  }
  return {
    hasResult: Boolean(checkedAtText || familyItems.length || errorText),
    tone,
    statusText,
    summaryText,
    roleLabel,
    rootDirText,
    dbPathText,
    checkedAtText,
    errorText,
    readyEntryCount,
    accessibleReadyCount,
    missingReadyCount,
    initializedCount,
    familyItems,
  };
}
