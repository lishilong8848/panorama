import { CONFIG_FEATURE_MONTHLY_TAB_TEMPLATE } from './app_config_feature_monthly_tab.js';
import { CONFIG_FEATURE_SHEET_TAB_TEMPLATE } from './app_config_feature_sheet_tab.js';
import { CONFIG_FEATURE_HANDOVER_TAB_TEMPLATE } from './app_config_feature_handover_tab.js';
import { CONFIG_FEATURE_DAY_METRIC_UPLOAD_TAB_TEMPLATE } from './app_config_feature_day_metric_upload_tab.js';
import { CONFIG_FEATURE_ALARM_EXPORT_TAB_TEMPLATE } from './app_config_feature_alarm_export_tab.js';
import { CONFIG_FEATURE_MANUAL_TAB_TEMPLATE } from './app_config_feature_manual_tab.js';
import { CONFIG_FEATURE_WET_BULB_COLLECTION_TAB_TEMPLATE } from './app_config_feature_wet_bulb_collection_tab.js';

export const CONFIG_FEATURE_TABS_TEMPLATE = `
${CONFIG_FEATURE_MONTHLY_TAB_TEMPLATE}

${CONFIG_FEATURE_SHEET_TAB_TEMPLATE}

${CONFIG_FEATURE_HANDOVER_TAB_TEMPLATE}

${CONFIG_FEATURE_DAY_METRIC_UPLOAD_TAB_TEMPLATE}

${CONFIG_FEATURE_WET_BULB_COLLECTION_TAB_TEMPLATE}

${CONFIG_FEATURE_ALARM_EXPORT_TAB_TEMPLATE}

${CONFIG_FEATURE_MANUAL_TAB_TEMPLATE}
`;
