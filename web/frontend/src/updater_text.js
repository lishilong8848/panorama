export const UPDATER_RESULT_TEXT_MAP = {
  "": "-",
  disabled: "已禁用",
  up_to_date: "已经是最新版本",
  update_available: "发现可用更新",
  downloading_patch: "补丁下载中",
  applying_patch: "补丁应用中",
  dependency_checking: "运行依赖检查中",
  dependency_syncing: "运行依赖同步中",
  dependency_rollback: "依赖同步失败，正在回滚",
  updated: "补丁已应用",
  updated_restart_scheduled: "更新后将自动重启",
  queued_busy: "任务结束后自动更新",
  restart_pending: "等待重启生效",
  ahead_of_remote: "本地版本高于远端正式版本",
  ahead_of_mirror: "本地版本高于共享目录批准版本",
  mirror_pending_publish: "等待外网端发布批准版本",
  failed: "更新失败",
};

export function mapUpdaterResultText(raw) {
  const key = String(raw || "").trim();
  return UPDATER_RESULT_TEXT_MAP[key] || key || "-";
}

export function buildUpdaterApplyMessage(raw) {
  const payload = raw && typeof raw === "object" ? raw : {};
  const explicitMessage = String(payload?.message || "").trim();
  if (explicitMessage) return explicitMessage;

  const key = String((payload && payload.last_result) || raw || "").trim();
  if (key === "updated_restart_scheduled") {
    return "补丁已应用并完成运行依赖同步，程序将自动重启。";
  }
  if (key === "queued_busy") {
    return "当前仍有任务在运行，更新已排队，任务结束后会自动执行。";
  }
  if (key === "restart_pending") {
    return "补丁已应用并完成运行依赖同步，重启程序后即可生效。";
  }
  if (key === "updated") {
    return "补丁已应用完成。";
  }
  if (key === "ahead_of_remote") {
    return "检测到本地版本高于远端正式版本，如需覆盖回远端正式版本，可继续执行更新。";
  }
  if (key === "ahead_of_mirror") {
    return "当前内网端本地版本高于共享目录批准版本，不会自动回退。";
  }
  if (key === "mirror_pending_publish") {
    return "共享目录中还没有已批准的更新版本，等待外网端发布后会自动跟随更新。";
  }
  if (key === "failed" && String(payload?.dependency_sync_status || "").trim() === "rolled_back") {
    return "更新失败：运行依赖同步失败，已自动回滚到旧版本。";
  }
  return `更新处理完成：${mapUpdaterResultText(key)}`;
}
