import { createDashboardJobActions } from "./dashboard_job_actions.js";
import { createDashboardSchedulerActions } from "./dashboard_scheduler_actions.js";
import { createDashboardWetBulbCollectionActions } from "./dashboard_wet_bulb_collection_actions.js";

export function createDashboardActions(ctx) {
  const jobActions = createDashboardJobActions(ctx);
  const schedulerActions = createDashboardSchedulerActions(ctx);
  const wetBulbActions = createDashboardWetBulbCollectionActions(ctx);
  return {
    ...jobActions,
    ...schedulerActions,
    ...wetBulbActions,
  };
}
