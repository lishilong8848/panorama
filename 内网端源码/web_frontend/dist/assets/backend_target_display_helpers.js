export function mapBackendTargetDisplay(raw, fallback = {}) {
  const payload = raw && typeof raw === "object" ? raw : null;
  if (!payload) return null;
  const configuredAppToken = String(payload.configured_app_token || payload.configuredAppToken || fallback.configuredAppToken || fallback.appToken || "").trim();
  const operationAppToken = String(payload.operation_app_token || payload.operationAppToken || fallback.operationAppToken || "").trim();
  const tableId = String(payload.table_id || payload.tableId || fallback.tableId || "").trim();
  const baseUrl = String(payload.base_url || payload.baseUrl || fallback.baseUrl || "").trim();
  const wikiUrl = String(payload.wiki_url || payload.wikiUrl || fallback.wikiUrl || "").trim();
  const displayUrl = String(payload.display_url || payload.displayUrl || payload.bitable_url || payload.bitableUrl || fallback.displayUrl || "").trim();
  const bitableUrl = String(payload.bitable_url || payload.bitableUrl || payload.display_url || payload.displayUrl || fallback.bitableUrl || "").trim();
  return {
    appToken: configuredAppToken,
    configuredAppToken,
    operationAppToken,
    tableId,
    baseUrl,
    wikiUrl,
    displayUrl,
    bitableUrl,
    targetKind: String(payload.target_kind || payload.targetKind || fallback.targetKind || "").trim(),
    configured: typeof payload.configured === "boolean" ? payload.configured : Boolean(fallback.configured),
    replaceExistingOnFull:
      typeof payload.replace_existing_on_full === "boolean"
        ? payload.replace_existing_on_full
        : (typeof payload.replaceExistingOnFull === "boolean"
          ? payload.replaceExistingOnFull
          : Boolean(fallback.replaceExistingOnFull)),
    statusText: String(payload.status_text || payload.statusText || fallback.statusText || "").trim(),
    hintText: String(payload.hint_text || payload.hintText || fallback.hintText || "").trim(),
    message: String(payload.message || fallback.message || "").trim(),
    resolvedAt: String(payload.resolved_at || payload.resolvedAt || fallback.resolvedAt || "").trim(),
    url: String(payload.display_url || payload.displayUrl || payload.bitable_url || payload.bitableUrl || payload.url || fallback.url || "").trim(),
  };
}

export function buildNeutralTargetDisplay(fallback = {}) {
  return {
    appToken: String(fallback.appToken || fallback.configuredAppToken || "").trim(),
    configuredAppToken: String(fallback.configuredAppToken || fallback.appToken || "").trim(),
    operationAppToken: String(fallback.operationAppToken || "").trim(),
    tableId: String(fallback.tableId || "").trim(),
    baseUrl: String(fallback.baseUrl || "").trim(),
    wikiUrl: String(fallback.wikiUrl || "").trim(),
    displayUrl: String(fallback.displayUrl || fallback.url || "").trim(),
    bitableUrl: String(fallback.bitableUrl || fallback.displayUrl || fallback.url || "").trim(),
    targetKind: String(fallback.targetKind || "").trim(),
    configured: Boolean(fallback.configured),
    replaceExistingOnFull:
      typeof fallback.replaceExistingOnFull === "boolean"
        ? fallback.replaceExistingOnFull
        : true,
    statusText: "等待后端状态",
    hintText: "目标状态由后端聚合后返回。",
    message: String(fallback.message || "").trim(),
    resolvedAt: String(fallback.resolvedAt || "").trim(),
    url: String(fallback.url || fallback.displayUrl || "").trim(),
  };
}
