import { createDashboardJobActions } from "./dashboard_job_actions.js";
import { createDashboardMonthlyEventReportActions } from "./dashboard_monthly_event_report_actions.js";
import { createDashboardSchedulerActions } from "./dashboard_scheduler_actions.js";
import { createDashboardSharedBridgeActions } from "./dashboard_shared_bridge_actions.js";
import { createDashboardWetBulbCollectionActions } from "./dashboard_wet_bulb_collection_actions.js";

export function createDashboardActions(ctx) {
  const jobActions = createDashboardJobActions(ctx);
  const schedulerActions = createDashboardSchedulerActions(ctx);
  const sharedBridgeActions = createDashboardSharedBridgeActions(ctx);
  const wetBulbActions = createDashboardWetBulbCollectionActions(ctx);
  const monthlyEventReportActions = createDashboardMonthlyEventReportActions(ctx);
  return {
    ...jobActions,
    ...schedulerActions,
    ...sharedBridgeActions,
    ...wetBulbActions,
    ...monthlyEventReportActions,
  };
}
