function normalizeLegacyNetworkSide(side) {
  return String(side || "").trim().toLowerCase();
}

function normalizeLegacyNetworkMode(mode) {
  return String(mode || "").trim().toLowerCase();
}

export function formatNetworkWindowSide(side) {
  const normalized = normalizeLegacyNetworkSide(side);
  if (normalized === "internal") return "采集端网络";
  if (normalized === "external") return "外网";
  if (normalized === "pipeline") return "流水线";
  return "空闲";
}

export function formatDetectedNetworkSide(side) {
  const normalized = normalizeLegacyNetworkSide(side);
  if (normalized === "internal") return "当前在采集端网络";
  if (normalized === "external") return "当前在外网";
  if (normalized === "other") return "当前不在目标网络";
  if (normalized === "none") return "当前未连接 WiFi";
  return "当前网络未知";
}

export function formatSsidSide(side) {
  const normalized = String(side || "").trim().toLowerCase();
  if (normalized === "internal") return "采集端网络";
  if (normalized === "external") return "外网";
  if (normalized === "other") return "其他";
  if (normalized === "none") return "未连接";
  return "-";
}

export function formatNetworkMode(mode) {
  const normalized = normalizeLegacyNetworkMode(mode);
  if (normalized === "internal_only") return "仅采集端网络可达";
  if (normalized === "external_only") return "仅外网可达";
  if (normalized === "none_reachable") return "当前均不可达";
  return String(mode || "").trim() || "-";
}

export function formatBooleanReachability(value) {
  return value ? "是" : "否";
}

export function formatWetBulbTargetKind(kind) {
  const normalized = String(kind || "").trim().toLowerCase();
  if (normalized === "base_token_pair") return "Base";
  if (normalized === "wiki_token_pair") return "Wiki";
  if (normalized === "probe_error") return "探测失败";
  if (normalized === "invalid") return "配置无效";
  return "-";
}
