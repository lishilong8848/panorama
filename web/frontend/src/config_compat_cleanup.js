import { clone } from "./config_common_utils.js";

function ensurePlainObject(raw) {
  return raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
}

export function cleanupDayMetricUploadCompat(raw, options = {}) {
  const cfg = options.cloneInput ? clone(raw || {}) : ensurePlainObject(raw);
  delete cfg.enabled;
  delete cfg.manual_button_enabled;
  cfg.behavior = ensurePlainObject(cfg.behavior);
  delete cfg.behavior.only_day_shift;
  delete cfg.behavior.rewrite_existing;
  delete cfg.behavior.local_import_enabled;
  delete cfg.behavior.failure_policy;
  delete cfg.behavior.local_import_scope;
  cfg.target = ensurePlainObject(cfg.target);
  cfg.target.source = ensurePlainObject(cfg.target.source);
  delete cfg.target.source.base_url;
  delete cfg.target.source.wiki_url;
  delete cfg.target.types;
  return cfg;
}

export function cleanupAlarmExportCompat(raw, options = {}) {
  const cfg = options.cloneInput ? clone(raw || {}) : ensurePlainObject(raw);
  delete cfg.manual_button_enabled;
  cfg.shared_source_upload = ensurePlainObject(cfg.shared_source_upload);
  delete cfg.shared_source_upload.target;
  return cfg;
}

export function cleanupWetBulbCollectionCompat(raw, options = {}) {
  const cfg = options.cloneInput ? clone(raw || {}) : ensurePlainObject(raw);
  delete cfg.manual_button_enabled;
  cfg.source = ensurePlainObject(cfg.source);
  delete cfg.source.switch_to_internal_before_download;
  cfg.target = ensurePlainObject(cfg.target);
  delete cfg.target.base_url;
  delete cfg.target.wiki_url;
  return cfg;
}
