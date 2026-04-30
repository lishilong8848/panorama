// Frontend helper facade. Keep export names stable for existing modules.

export {
  todayText,
  formatDateObj,
  parseDateText,
  isValidHms,
  normalizeRunTimeText,
  normalizeDatetimeLocalToApi,
  apiDatetimeToLocal,
  expandDateRange,
} from "./config_date_utils.js";

export { clone } from "./config_common_utils.js";

export {
  normalizeSiteHost,
  normalizeSheetRules,
  buildSheetRulesObject,
} from "./config_sheet_rules.js";

export {
  convertV3ConfigToLegacy,
  convertLegacyConfigToV3,
  ensureConfigShape,
} from "./config_runtime_shape.js";

export { apiJson, isTransientNetworkError } from "./config_api_utils.js";
