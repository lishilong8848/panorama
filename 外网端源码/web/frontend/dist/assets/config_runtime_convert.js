import { clone } from "./config_common_utils.js";
import {
  cleanupAlarmExportCompat,
  cleanupDayMetricUploadCompat,
  cleanupWetBulbCollectionCompat,
} from "./config_compat_cleanup.js";

export function convertV3ConfigToLegacy(v3) {
  const raw = clone(v3 || {});
  if (!raw || typeof raw !== "object") return {};
  if (!raw.common || !raw.features) return raw;

  const common = raw.common || {};
  const features = raw.features || {};
  const monthly = features.monthly_report || {};
  const upload = monthly.upload || {};
  const pathRoot =
    String(common.paths?.business_root_dir || "").trim() ||
    String(common.paths?.download_save_dir || "").trim() ||
    String(common.paths?.excel_dir || "").trim() ||
    "";

  return {
    version: 3,
    paths: {
      business_root_dir: pathRoot,
    },
    deployment: clone(common.deployment || {}),
    shared_bridge: {
      ...clone(common.shared_bridge || {}),
      internal_root_dir: String(common.shared_bridge?.internal_root_dir || common.shared_bridge?.root_dir || "").trim(),
      external_root_dir: String(common.shared_bridge?.external_root_dir || common.shared_bridge?.root_dir || "").trim(),
    },
    internal_source_sites: clone(common.internal_source_sites || []),
    input: {
      excel_dir: pathRoot,
      buildings: Array.isArray(monthly.buildings) ? clone(monthly.buildings) : [],
      file_glob_template: monthly.file_glob_template || "{building}_*.xlsx",
    },
    output: {
      save_json: false,
      json_dir: "",
    },
    download: {
      save_dir: pathRoot,
      time_range_mode: monthly.time_range_mode,
      custom_window_mode: monthly.custom_window_mode,
      start_time: monthly.start_time,
      end_time: monthly.end_time,
      daily_custom_window: clone(monthly.daily_custom_window || {}),
      run_subdir_mode: monthly.run_subdir_mode,
      run_subdir_prefix: monthly.run_subdir_prefix,
      max_retries: monthly.max_retries,
      retry_wait_sec: monthly.retry_wait_sec,
      site_start_delay_sec: monthly.site_start_delay_sec,
      only_process_downloaded_this_run: monthly.only_process_downloaded_this_run,
      browser_headless: monthly.browser_headless,
      browser_channel: monthly.browser_channel,
      playwright_browsers_path: monthly.playwright_browsers_path,
      sites: clone(monthly.sites || []),
      multi_date: clone(monthly.multi_date || {}),
      resume: clone(monthly.resume || {}),
      performance: clone(monthly.performance || {}),
    },
    scheduler: clone(common.scheduler || {}),
    updater: clone(common.updater || {}),
    notify: clone(common.notify || {}),
    feishu: {
      app_id: common.feishu_auth?.app_id || "",
      app_secret: common.feishu_auth?.app_secret || "",
      request_retry_count: common.feishu_auth?.request_retry_count,
      request_retry_interval_sec: common.feishu_auth?.request_retry_interval_sec,
      timeout: common.feishu_auth?.timeout,
      enable_upload: upload.enable_upload,
      skip_zero_records: upload.skip_zero_records,
      date_field_mode: upload.date_field_mode,
      date_field_day: upload.date_field_day,
      date_tz_offset_hours: upload.date_tz_offset_hours,
      app_token: upload.app_token,
      calc_table_id: upload.calc_table_id,
      attachment_table_id: upload.attachment_table_id,
      report_type: upload.report_type,
    },
    feishu_sheet_import: clone(features.sheet_import || {}),
    handover_log: clone(features.handover_log || {}),
    day_metric_upload: cleanupDayMetricUploadCompat(features.day_metric_upload || {}, { cloneInput: true }),
    branch_power_upload: clone(features.branch_power_upload || {}),
    wet_bulb_collection: cleanupWetBulbCollectionCompat(features.wet_bulb_collection || {}, { cloneInput: true }),
    alarm_export: cleanupAlarmExportCompat(features.alarm_export || {}, { cloneInput: true }),
    manual_upload_gui: clone(features.manual_upload_gui || {}),
    web: clone(common.console || {}),
  };
}

export function convertLegacyConfigToV3(legacy) {
  const src = clone(legacy || {});
  const monthlyUpload = src.feishu || {};
  const pathRoot =
    String(src.download?.save_dir || "").trim() ||
    String(src.input?.excel_dir || "").trim() ||
    "";
  return {
    version: 3,
    common: {
      paths: {
        business_root_dir: pathRoot,
      },
      deployment: clone(src.deployment || {}),
      shared_bridge: {
        ...clone(src.shared_bridge || {}),
        internal_root_dir: String(src.shared_bridge?.internal_root_dir || src.shared_bridge?.root_dir || "").trim(),
        external_root_dir: String(src.shared_bridge?.external_root_dir || src.shared_bridge?.root_dir || "").trim(),
      },
      internal_source_sites: clone(src.internal_source_sites || src.download?.sites || src.handover_log?.sites || []),
      scheduler: clone(src.scheduler || {}),
      updater: clone(src.updater || {}),
      notify: clone(src.notify || {}),
      feishu_auth: {
        app_id: monthlyUpload.app_id || "",
        app_secret: monthlyUpload.app_secret || "",
        request_retry_count: monthlyUpload.request_retry_count,
        request_retry_interval_sec: monthlyUpload.request_retry_interval_sec,
        timeout: monthlyUpload.timeout,
      },
      console: clone(src.web || {}),
    },
    features: {
      monthly_report: {
        buildings: Array.isArray(src.input?.buildings) ? clone(src.input.buildings) : [],
        file_glob_template: src.input?.file_glob_template || "{building}_*.xlsx",
        time_range_mode: src.download?.time_range_mode,
        custom_window_mode: src.download?.custom_window_mode,
        start_time: src.download?.start_time,
        end_time: src.download?.end_time,
        daily_custom_window: clone(src.download?.daily_custom_window || {}),
        run_subdir_mode: src.download?.run_subdir_mode,
        run_subdir_prefix: src.download?.run_subdir_prefix,
        max_retries: src.download?.max_retries,
        retry_wait_sec: src.download?.retry_wait_sec,
        site_start_delay_sec: src.download?.site_start_delay_sec,
        only_process_downloaded_this_run: src.download?.only_process_downloaded_this_run,
        browser_headless: src.download?.browser_headless,
        browser_channel: src.download?.browser_channel,
        playwright_browsers_path: src.download?.playwright_browsers_path,
        sites: clone(src.download?.sites || []),
        multi_date: clone(src.download?.multi_date || {}),
        resume: clone(src.download?.resume || {}),
        performance: clone(src.download?.performance || {}),
        upload: {
          enable_upload: monthlyUpload.enable_upload,
          skip_zero_records: monthlyUpload.skip_zero_records,
          date_field_mode: monthlyUpload.date_field_mode,
          date_field_day: monthlyUpload.date_field_day,
          date_tz_offset_hours: monthlyUpload.date_tz_offset_hours,
          app_token: monthlyUpload.app_token,
          calc_table_id: monthlyUpload.calc_table_id,
          attachment_table_id: monthlyUpload.attachment_table_id,
          report_type: monthlyUpload.report_type,
        },
      },
      sheet_import: clone(src.feishu_sheet_import || {}),
      handover_log: clone(src.handover_log || {}),
      day_metric_upload: cleanupDayMetricUploadCompat(src.day_metric_upload || {}, { cloneInput: true }),
      branch_power_upload: clone(src.branch_power_upload || {}),
      wet_bulb_collection: cleanupWetBulbCollectionCompat(src.wet_bulb_collection || {}, { cloneInput: true }),
      alarm_export: cleanupAlarmExportCompat(src.alarm_export || {}, { cloneInput: true }),
      manual_upload_gui: clone(src.manual_upload_gui || {}),
    },
  };
}
