export const SCHEDULER_DECISION_TEXT_MAP = {
  "skip:not_started": "未启动",
  "skip:disabled": "已禁用",
  "skip:before_schedule_time": "未到执行时间",
  "skip:before_next_run": "未到下次执行时间",
  "skip:already_success_today": "今日已成功执行",
  "skip:missed_and_no_catchup": "已错过时间且未启用补跑",
  "skip:already_attempted_no_retry": "今日已尝试且不重试",
  "skip:retry_already_done": "今日重试已执行",
  "skip:busy": "执行时任务占用",
  "skip:skip_busy": "执行时任务占用",
  "skip:stopped": "已停止",
  "run:due": "满足触发条件",
};

export const SCHEDULER_TRIGGER_TEXT_MAP = {
  success: "成功",
  failed: "失败",
  skip_busy: "任务占用已跳过",
};

export function mapSchedulerDecisionText(raw) {
  const key = String(raw || "").trim();
  return SCHEDULER_DECISION_TEXT_MAP[key] || key || "-";
}

export function mapSchedulerTriggerText(raw) {
  const key = String(raw || "").trim();
  return SCHEDULER_TRIGGER_TEXT_MAP[key] || key || "-";
}
