import { createRuntimeHealthConfigActions } from "./runtime_health_config_actions.js";
import { createRuntimeResumeActions } from "./runtime_resume_actions.js";

export function createRuntimeActions(ctx) {
  const healthConfigActions = createRuntimeHealthConfigActions(ctx);
  const resumeActions = createRuntimeResumeActions(ctx);
  return {
    ...healthConfigActions,
    ...resumeActions,
  };
}
