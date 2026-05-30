export function formatSharedBridgeRuntimeError(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.includes("timed out")) return "内网端 HTTP 请求超时，请检查内网端服务和端口连通性。";
  if (text.includes("Connection refused") || text.includes("actively refused")) {
    return "内网端 HTTP 端口未开放或服务未启动。";
  }
  return text;
}

export function formatInternalDownloadPoolError(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.includes("浏览器池")) return text;
  if (text.includes("not initialized") || text.includes("未初始化")) {
    return "内网端浏览器池尚未就绪。";
  }
  return text;
}
