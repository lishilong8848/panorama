import { CONFIG_COMMON_PATHS_TAB_TEMPLATE } from './app_config_common_paths_tab.js';
import { CONFIG_COMMON_DEPLOYMENT_TAB_TEMPLATE } from './app_config_common_deployment_tab.js';
import { CONFIG_COMMON_SCHEDULER_TAB_TEMPLATE } from './app_config_common_scheduler_tab.js';
import { CONFIG_COMMON_NOTIFY_TAB_TEMPLATE } from './app_config_common_notify_tab.js';
import { CONFIG_COMMON_FEISHU_AUTH_TAB_TEMPLATE } from './app_config_common_feishu_auth_tab.js';
import { CONFIG_COMMON_CONSOLE_TAB_TEMPLATE } from './app_config_common_console_tab.js';

export const CONFIG_COMMON_TABS_TEMPLATE = `
${CONFIG_COMMON_PATHS_TAB_TEMPLATE}

${CONFIG_COMMON_DEPLOYMENT_TAB_TEMPLATE}

${CONFIG_COMMON_SCHEDULER_TAB_TEMPLATE}

${CONFIG_COMMON_NOTIFY_TAB_TEMPLATE}

${CONFIG_COMMON_FEISHU_AUTH_TAB_TEMPLATE}

${CONFIG_COMMON_CONSOLE_TAB_TEMPLATE}
`;
