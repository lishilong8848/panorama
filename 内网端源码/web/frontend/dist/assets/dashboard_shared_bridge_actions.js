import { runSharedBridgeSelfCheckApi } from "./api_client.js";

const ACTION_KEYS = {
  sharedBridgeSelfCheck: "bridge:shared_root_self_check",
};

export function createDashboardSharedBridgeActions(ctx) {
  const {
    message,
    fetchExternalDashboardSummary,
    scheduleExternalDashboardRefresh,
    runSingleFlight,
    sharedBridgeSelfCheckResult,
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

  async function runSharedBridgeSelfCheck() {
    return guardedRun(
      ACTION_KEYS.sharedBridgeSelfCheck,
      async () => {
        try {
          const data = await runSharedBridgeSelfCheckApi();
          if (sharedBridgeSelfCheckResult?.value !== undefined) {
            sharedBridgeSelfCheckResult.value = data;
          }
          if (typeof scheduleExternalDashboardRefresh === "function") {
            scheduleExternalDashboardRefresh("shared_bridge_self_check");
          } else if (typeof fetchExternalDashboardSummary === "function") {
            await fetchExternalDashboardSummary({ silentMessage: true });
          }
          message.value = String(data?.message || "").trim() || "共享目录自检已完成";
        } catch (err) {
          message.value = `共享目录自检失败: ${err}`;
        }
      },
      { cooldownMs: 500 },
    );
  }

  return {
    runSharedBridgeSelfCheck,
  };
}
