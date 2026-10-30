"""Offline frontend checks: no server, lifecycle activation, or network access."""
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node.js is required")


def run_node(directory, script):
    result = subprocess.run(
        [NODE, "--input-type=module", "-e", "try {\n" + script + "\n} catch(e) { console.error(e.stack); process.exitCode=1; }"],
        cwd=directory, capture_output=True, text=True, encoding="utf-8", timeout=30,
    )
    assert result.returncode == 0, result.stderr + result.stdout


@pytest.mark.parametrize("directory", ["web/frontend/src", "web/frontend/dist/assets", "web_frontend/dist/assets"])
def test_actual_app_setup_exports_and_template_compile(directory):
    run_node(ROOT / directory, r'''
      const fs = await import('node:fs');
      const vm = await import('node:vm');
      const assert = (await import('node:assert/strict')).default;
      const noop = () => {};
      const storage = {getItem: () => null, setItem: noop, removeItem: noop};
      globalThis.window = globalThis;
      globalThis.location = {pathname: '/', search: '', hash: '', origin: 'http://static.invalid'};
      globalThis.localStorage = storage;
      globalThis.sessionStorage = storage;
      globalThis.history = {replaceState: noop, pushState: noop};
      globalThis.addEventListener = globalThis.removeEventListener = noop;
      globalThis.matchMedia = () => ({matches: false, addEventListener: noop, removeEventListener: noop});
      globalThis.document = {
        createElement: () => ({style: {}, content: {}, setAttribute: noop, removeAttribute: noop}),
        getElementById: () => null, querySelector: () => null, addEventListener: noop,
        documentElement: {classList: {add: noop, remove: noop, toggle: noop}, style: {}},
        body: {classList: {add: noop, remove: noop}},
      };
      globalThis.fetch = () => {throw Error('Network forbidden');};
      globalThis.setInterval = () => {throw Error('Polling forbidden');};
      vm.runInThisContext(fs.readFileSync('./vue.global.prod.js', 'utf8'));
      Vue.watch = () => noop;
      Vue.onMounted = Vue.onBeforeUnmount = noop;
      Vue.createApp = options => ({mount() {globalThis.bindings = options.setup();}});
      await import('./app.js');
      for (const key of [
        'inspectionDataUploadDate', 'inspectionDataUploadDryRun', 'inspectionDataUploadRunTimeDraft',
        'inspectionDataUploadSchedulerQuickSaving', 'runInspectionDataUpload',
        'getInspectionDataUploadResult', 'getInspectionDataUploadStatusText', 'getInspectionDataUploadStatusTone',
        'startInspectionDataUploadScheduler', 'stopInspectionDataUploadScheduler',
        'saveInspectionDataUploadSchedulerQuickConfig', 'addInspectionDataUploadRunTime',
        'actionKeyInspectionDataUploadRun', 'actionKeyInspectionDataUploadSchedulerStart',
        'actionKeyInspectionDataUploadSchedulerStop', 'actionKeyInspectionDataUploadSchedulerSave',
      ]) assert.notEqual(bindings[key], undefined, key);
      const {DASHBOARD_INSPECTION_DATA_UPLOAD_SECTION: section} = await import('./dashboard_template_sections/dashboard_inspection_data_upload_section.js');
      assert.equal(typeof Vue.compile(section), 'function');
      for (const phrase of ['source_family', 'inspection_data_family', 'executor_bound', '资源锁', '验证源文件与字段']) {
        assert.equal(section.includes(phrase), false, phrase);
      }
      assert.ok(section.includes('仅检查文件内容'));
      bindings.currentJob.value = {feature: 'inspection_data_upload', status: 'success', result: {
        dry_run: true, buildings: ['D楼', 'E楼'], results: {}, planned_count: 979, uploaded_count: 0, source_file_count: 4,
      }};
      assert.equal(bindings.getInspectionDataUploadResult().planned_count, 979);
      assert.equal(bindings.getInspectionDataUploadStatusTone(), 'success');
    ''')


def test_scheduler_actions_use_existing_api_without_automatic_enable():
    run_node(ROOT / "web/frontend/src", r'''
      const assert = (await import('node:assert/strict')).default;
      globalThis.window = globalThis;
      const calls = [];
      globalThis.fetch = async (url, options) => {
        const payload = JSON.parse(options.body);
        calls.push({url, payload});
        return {ok: true, json: async () => ({
          ok: true, scheduler_config: payload, scheduler_status: {running: false},
          action: {reason: url.endsWith('/start') ? 'started' : 'stopped'},
        })};
      };
      const {createDashboardSchedulerActions} = await import('./dashboard_scheduler_actions.js');
      const ctx = {
        config: {value: {inspection_data_upload: {scheduler: {enabled: false, auto_start_in_gui: false, run_times: []}}}},
        health: {inspection_data_upload: {scheduler: {}}}, message: {value: ''},
        inspectionDataUploadSchedulerQuickSaving: {value: false}, inspectionDataUploadRunTimeDraft: {value: ''},
      };
      const actions = createDashboardSchedulerActions(ctx);
      await actions.saveInspectionDataUploadSchedulerQuickConfig({run_times: ['13:10:00','01:10','01:10:00']});
      assert.deepEqual(calls[0].payload, {run_times: ['01:10:00','13:10:00']});
      assert.equal(ctx.config.value.inspection_data_upload.scheduler.enabled, false);
      await actions.saveInspectionDataUploadSchedulerQuickConfig({run_times: ['25:00:00']});
      assert.equal(calls.length, 1);
      ctx.inspectionDataUploadRunTimeDraft.value = '18:30';
      await actions.addInspectionDataUploadRunTime();
      assert.deepEqual(calls[1].payload.run_times, ['01:10:00','13:10:00','18:30:00']);
      assert.equal(ctx.inspectionDataUploadRunTimeDraft.value, '');
      await actions.saveInspectionDataUploadSchedulerQuickConfig({run_times: []});
      assert.deepEqual(calls[2].payload, {run_times: []});
      await actions.startInspectionDataUploadScheduler();
      await actions.stopInspectionDataUploadScheduler();
      assert.ok(calls[3].url.endsWith('/inspection-data-upload/start'));
      assert.ok(calls[4].url.endsWith('/inspection-data-upload/stop'));
      assert.equal(ctx.inspectionDataUploadSchedulerQuickSaving.value, false);
    ''')


def test_default_config_and_roundtrip():
    run_node(ROOT / "web/frontend/src", r'''
      const assert = (await import('node:assert/strict')).default;
      const {ensureConfigShape} = await import('./config_runtime_defaults.js');
      const {convertV3ConfigToLegacy, convertLegacyConfigToV3} = await import('./config_runtime_convert.js');
      const cfg = ensureConfigShape({});
      const feature = cfg.inspection_data_upload;
      assert.deepEqual(feature.buildings, ['D楼','E楼']);
      assert.deepEqual(feature.source_families, {
        '电气': 'electrical_inspection_family', '暖通': 'hvac_inspection_family',
      });
      assert.equal(feature.source_family, undefined);
      assert.equal(feature.enabled, true);
      assert.deepEqual(feature.scheduler.run_times, []);
      assert.equal(feature.scheduler.enabled, false);
      assert.equal(feature.scheduler.auto_start_in_gui, false);
      feature.scheduler.run_times = ['01:10:00','14:10:00'];
      const restored = convertV3ConfigToLegacy(convertLegacyConfigToV3(cfg));
      assert.deepEqual(restored.inspection_data_upload, feature);
      const legacy = ensureConfigShape({inspection_data_upload: {source_family: 'inspection_data_family'}});
      assert.equal(legacy.inspection_data_upload.source_family, undefined);
      assert.deepEqual(legacy.inspection_data_upload.source_families, feature.source_families);
    ''')


def test_both_frontend_builds_match_source():
    source = ROOT / "web/frontend/src"
    files = ["app.js", "app_state.js", "app_action_keys.js", "app_dashboard_template.js", "api_client.js",
             "config_runtime_convert.js", "config_runtime_defaults.js", "dashboard_job_actions.js",
             "dashboard_scheduler_actions.js", "dashboard_menu_config.js", "runtime_health_config_actions.js",
             "scheduler_remembered_watchers.js", "scheduler_ui_helpers.js",
             "dashboard_template_sections/dashboard_inspection_data_upload_section.js"]
    for name in files:
        expected = (source / name).read_bytes()
        for output in ("web/frontend/dist/assets", "web_frontend/dist/assets"):
            assert (ROOT / output / name).read_bytes() == expected, (output, name)
