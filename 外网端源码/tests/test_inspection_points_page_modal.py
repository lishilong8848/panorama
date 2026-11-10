"""测点修改悬浮窗（居中模态）离线前端自检：无服务器、无网络、无生命周期激活。

覆盖用户诉求：
- 修改时悬浮窗捕获并展示该测点完整只读信息（"信息要显示全"）。
- 有未保存修改时关闭需二次确认；无修改时直接关闭（取消/遮罩/ESC 共用 requestCloseDraft）。
- 添加测点无只读信息头。
- 修改后保存仍通过 PUT 持久化并关闭悬浮窗（"修改后保存好"）。
参数化覆盖 src 与两个 dist，确保构建产物同步。
"""
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node.js is required")


def run_node(directory, script):
    result = subprocess.run(
        [NODE, "--input-type=module", "-e",
         "try {\n" + script + "\n} catch(e) { console.error(e.stack); process.exitCode=1; }"],
        cwd=directory, capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stderr + result.stdout


@pytest.mark.parametrize("directory", ["web/frontend/src", "web/frontend/dist/assets", "web_frontend/dist/assets"])
def test_inspection_point_modal_info_dirty_close_and_save(directory):
    run_node(ROOT / directory, r'''
      const fs = await import('node:fs');
      const vm = await import('node:vm');
      const assert = (await import('node:assert/strict')).default;
      const noop = () => {};
      const storage = {getItem: () => null, setItem: noop, removeItem: noop};
      globalThis.window = globalThis;
      globalThis.location = {pathname: '/handover/review/d/inspection-points/', search: '', hash: '', origin: 'http://static.invalid'};
      globalThis.localStorage = storage;
      globalThis.sessionStorage = storage;
      globalThis.history = {replaceState: noop, pushState: noop};
      globalThis.addEventListener = globalThis.removeEventListener = noop;
      globalThis.matchMedia = () => ({matches: false, addEventListener: noop, removeEventListener: noop});
      // Vue 浏览器版编译器用 decodeHtmlBrowser 解码属性值：
      //   decoder.innerHTML = '<div foo="RAW">'; decoder.children[0].getAttribute('foo')
      //   decoder.innerHTML = RAW; decoder.textContent
      // 因此 createElement 桩件必须支持 innerHTML/textContent/children[0].getAttribute，否则编译报 undefined[0]。
      function fakeDecoderElement() {
        let html = '';
        return {
          style: {}, content: {},
          setAttribute: noop, removeAttribute: noop,
          set innerHTML(value) { html = String(value); },
          get innerHTML() { return html; },
          get textContent() { return html; },
          children: [{
            getAttribute: () => {
              const match = /^<div foo="([\s\S]*)">$/.exec(html);
              return match ? match[1].replace(/&quot;/g, '"') : html;
            },
          }],
        };
      }
      globalThis.document = {
        createElement: fakeDecoderElement,
        getElementById: () => null, querySelector: () => null, addEventListener: noop,
        documentElement: {classList: {add: noop, remove: noop, toggle: noop}, style: {}},
        body: {classList: {add: noop, remove: noop}},
      };
      let confirmAnswer = true;
      let confirmCalls = 0;
      globalThis.confirm = () => { confirmCalls += 1; return confirmAnswer; };

      const puts = [];
      const initialSettings = {electrical_inspection_family: {
        points: {'7_0_1': '旧备注'}, remove: [], baseline: {'7_0_1': {sampleTime: '09:00'}},
        targets: {}, status_rules: {}, correction_rules: {}, explicitly_enabled: [], explicitly_disabled: [],
      }};
      const savedSettings = {electrical_inspection_family: {
        points: {'7_0_1': '新备注'}, remove: [], baseline: {}, targets: {},
        status_rules: {}, correction_rules: {}, explicitly_enabled: [], explicitly_disabled: [],
      }};
      globalThis.fetch = async (url, options = {}) => {
        const method = options.method || 'GET';
        if (method === 'PUT') {
          puts.push({url, body: JSON.parse(options.body)});
          return {ok: true, json: async () => ({settings: savedSettings, message: '已保存'})};
        }
        if (url.endsWith('/latest')) return {ok: true, json: async () => ({datasets: {}})};
        return {ok: true, json: async () => ({settings: initialSettings, target_options: {}})};
      };

      vm.runInThisContext(fs.readFileSync('./vue.global.prod.js', 'utf8'));
      Vue.onMounted = Vue.onBeforeUnmount = noop;
      // 编译模板可捕获悬浮窗模板语法错误；setup 返回绑定供断言。
      Vue.createApp = options => ({mount() { Vue.compile(options.template); globalThis.bindings = options.setup(); }});

      const {mountInspectionPointsPage} = await import('./inspection_points_page.js');
      mountInspectionPointsPage(Vue);
      const b = globalThis.bindings;

      // settings/datasets 是内部 ref，不在绑定中暴露；通过真实 loadSettings 流程注入设置数据。
      await b.loadSettings();
      assert.equal(b.settingsReady.value, true);

      const row = {id: '7_0_1', name: '测点甲', path: '暖通/空调/1#', displayValue: '24.5',
        rawDisplay: '24.8', status: '√', sampleTime: '09:30', correctionCount: 2, target: null, note: '旧备注'};

      // 1) 修改：悬浮窗捕获完整只读信息，标题为"修改测点"。
      b.editPoint(row);
      assert.equal(b.draftTitle.value, '修改测点');
      const info = b.draft.value.info;
      assert.equal(info.id, '7_0_1');
      assert.equal(info.name, '测点甲');
      assert.equal(info.path, '暖通/空调/1#');
      assert.equal(info.displayValue, '24.5');
      assert.equal(info.rawDisplay, '24.8');
      assert.equal(info.status, '√');
      assert.equal(info.sampleTime, '09:30');
      assert.equal(info.correctionCount, 2);
      assert.equal(info.targetText, '未设置位置');

      // 2) 未改动 → 不脏：即使 confirm 返回 false 也应直接关闭，且不触发 confirm。
      confirmAnswer = false;
      b.requestCloseDraft();
      assert.equal(confirmCalls, 0, '无未保存修改时不应二次确认');
      assert.equal(b.draft.value, null, '无修改应直接关闭');

      // 3) 改动备注 → 脏：confirm 返回 false 时不得关闭。
      b.editPoint(row);
      b.draft.value.note = '新备注';
      confirmAnswer = false;
      b.requestCloseDraft();
      assert.equal(confirmCalls, 1, '有未保存修改时应二次确认');
      assert.notEqual(b.draft.value, null, '用户拒绝放弃时不得关闭');

      // 4) 确认放弃 → 关闭。
      confirmAnswer = true;
      b.requestCloseDraft();
      assert.equal(b.draft.value, null);

      // 5) 添加测点：无只读信息头，标题为"添加测点"。
      b.editPoint();
      assert.equal(b.draftTitle.value, '添加测点');
      assert.equal(b.draft.value.info, null);
      b.requestCloseDraft();
      assert.equal(b.draft.value, null);

      // 6) 修改后保存：saveDraft 仍 PUT 持久化并关闭悬浮窗。
      b.editPoint(row);
      b.draft.value.note = '新备注';
      await b.saveDraft();
      assert.equal(puts.length, 1, 'saveDraft 应发起一次 PUT');
      assert.equal(puts[0].url, '/api/scheduler/inspection-data-upload/points/d');
      assert.equal(puts[0].body.family, 'electrical_inspection_family');
      assert.deepEqual(puts[0].body.points, {'7_0_1': '新备注'});
      assert.equal(b.draft.value, null, '保存成功后关闭悬浮窗');
      assert.equal(b.message.value, '已保存');
    ''')
