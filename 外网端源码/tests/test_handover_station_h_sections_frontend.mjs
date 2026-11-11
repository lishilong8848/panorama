// H楼审核页“业务分类填写区”前端离线自检：模板可编译、setup 正常、分类 normalizer 行为正确。
// 直接导入 dist 构建产物（须先跑 build_frontend.py）；不触网、不激活生命周期。
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const APP_URL = new URL("../web/frontend/dist/assets/handover_review_app.js", import.meta.url);
const VUE_URL = new URL("../web/frontend/dist/assets/vue.global.prod.js", import.meta.url);
const noop = () => {};

// Vue 浏览器版编译器用 decodeHtmlBrowser 解码属性值：innerHTML='<div foo="RAW">' 后读 children[0].getAttribute('foo')。
function fakeDecoderElement() {
  let html = "";
  return {
    style: {}, content: {}, setAttribute: noop, removeAttribute: noop,
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

globalThis.window = {
  location: { pathname: "/handover/review/h", search: "" },
  setTimeout: (cb) => setTimeout(cb, 1), clearTimeout, setInterval: () => 1, clearInterval: noop,
  addEventListener: noop, removeEventListener: noop,
  matchMedia: () => ({ matches: false, addEventListener: noop, removeEventListener: noop }),
};
globalThis.location = globalThis.window.location;
globalThis.document = {
  createElement: fakeDecoderElement, getElementById: () => null, querySelector: () => null,
  addEventListener: noop, title: "",
  documentElement: { classList: { add: noop, remove: noop, toggle: noop }, style: {} },
  body: { classList: { add: noop, remove: noop } },
};
globalThis.localStorage = globalThis.sessionStorage = { getItem: () => null, setItem: noop, removeItem: noop };
globalThis.history = { replaceState: noop, pushState: noop };
globalThis.URL = { createObjectURL: () => "blob:x", revokeObjectURL: noop };
globalThis.fetch = () => new Promise(() => {});

vm.runInThisContext(readFileSync(VUE_URL, "utf8"));
Vue.onMounted = Vue.onBeforeUnmount = noop;
let page;
Vue.createApp = ({ template, setup }) => ({ mount() { Vue.compile(template); page = setup(); } });

const { mountHandoverReviewApp } = await import(APP_URL.href);
mountHandoverReviewApp(Vue);

// 1) H 模板可编译、setup 正常运行，空输入下分类 schema/数据为空。
assert.ok(page, "H 组件 setup 应返回 bindings");
assert.deepEqual(page.state.value.section_schema, []);
assert.deepEqual(page.form.value.sections, {});

// 2) 模板确实包含分类填写区（渲染 state.section_schema + form.sections，嵌套 v-model）。
const source = readFileSync(APP_URL, "utf8");
assert.ok(source.includes("station-h-sections-area"), "模板应含分类填写区容器");
assert.ok(source.includes('v-for="section in state.section_schema"'), "模板应遍历 section_schema");
assert.ok(source.includes("form.sections[section.name]"), "模板应绑定 form.sections");
assert.ok(source.includes("sections: form.value.sections"), "保存载荷应包含 sections");
assert.ok(source.includes("sectionsChanged ? { sections:"), "人员保存不应提交未编辑的分类");
assert.ok(source.includes("base_sections_revision: state.value.sections_revision"), "分类保存必须携带读取时的版本");

// 3) 用源码切片直接测试未导出的分类 normalizer（沿用 110 测试范式）。
const start = source.indexOf("function normalizeStationHSectionSchema(");
const end = source.indexOf("function normalizeStationHState(", start);
assert.ok(start > 0 && end > start, "应能切出分类 normalizer 源码");
const factory = new Function(`${source.slice(start, end)} return { normalizeStationHSectionSchema, normalizeStationHSections };`);
const { normalizeStationHSectionSchema, normalizeStationHSections } = factory();

const schema = normalizeStationHSectionSchema([
  { name: "新事件处理", columns: [{ key: "B", label: "事件等级" }, { key: "H", label: "跟进人" }], rows: [19] },
  { name: "三、注意事项交接", columns: [{ key: "B", label: "注意事项描述" }, { key: "H", label: "执行人" }], rows: [40, 41, 42] },
  { name: "", columns: [{ key: "B" }], rows: [1] },                 // 无名 → 丢弃
  { name: "空列", columns: [], rows: [1] },                          // 无列 → 丢弃
]);
assert.deepEqual(schema.map((s) => s.name), ["新事件处理", "三、注意事项交接"]);

// 已保存行数不受模板限制；缺列补空；值转字符串。
const sections = normalizeStationHSections(
  {
    "新事件处理": [{ cells: { B: "一级", H: 42 } }, { cells: { B: "越界行" } }],
    "三、注意事项交接": [{ cells: { B: "常备1" } }],
    "不存在的分类": [{ cells: { B: "x" } }],
  },
  schema,
);
assert.equal(sections["新事件处理"].length, 2);
assert.deepEqual(sections["新事件处理"][0].cells, { B: "一级", H: "42" });
assert.equal(sections["三、注意事项交接"].length, 1);
assert.deepEqual(sections["三、注意事项交接"][0].cells, { B: "常备1", H: "" });
assert.deepEqual(normalizeStationHSections({ "三、注意事项交接": [] }, schema)["三、注意事项交接"], []);
assert.equal("不存在的分类" in sections, false, "schema 外的分类不纳入");

// 空/非法输入 → 按 schema 产出全空行结构。
const empty = normalizeStationHSections(null, schema);
assert.deepEqual(empty["新事件处理"], [{ cells: { B: "", H: "" } }]);

const requests = [];
globalThis.fetch = async (_url, options) => {
  requests.push(JSON.parse(options.body));
  return new Response(JSON.stringify({ detail: "H楼分类已被更新" }), {
    status: 409, headers: { "Content-Type": "application/json" },
  });
};
page.state.value.sections = structuredClone(empty);
page.state.value.sections_revision = 7;
page.form.value.sections = structuredClone(empty);
page.addSectionRow(schema[0]);
const added = page.form.value.sections["新事件处理"][1];
page.toggleSectionPerson(added, "H", "张三");
assert.equal(added.cells.H, "张三");
assert.equal(page.sectionPersonActive(added, "H", "张三"), true);
page.removeSectionRow("新事件处理", 1);
assert.deepEqual(page.form.value.sections, empty);
await page.saveSelection();
assert.equal("sections" in requests[0], false, "未编辑的分类不得随其他内容提交");
page.form.value.sections["新事件处理"][0].cells.B = "未保存内容";
assert.equal(await page.saveSelection(), false);
assert.equal(requests[1].base_sections_revision, 7);
assert.equal(requests[1].sections["新事件处理"][0].cells.B, "未保存内容");
assert.equal(page.form.value.sections["新事件处理"][0].cells.B, "未保存内容");
assert.match(page.errorText.value, /分类已被更新/);

const batch = { batch_key: "2026-11-08|night", duty_date: "2026-11-08", duty_shift: "night" };
page.loading.value = false;
page.state.value.batch = batch;
page.dutyDate.value = batch.duty_date;
page.dutyShift.value = batch.duty_shift;
assert.equal(page.canUploadAttachment.value, true);
const calls = [];
globalThis.fetch = async (url, options) => {
  calls.push(url);
  let payload;
  if (url === "/api/handover/review/station-h") {
    const saved = JSON.parse(options.body);
    assert.equal(saved.sections["新事件处理"][0].cells.B, "未保存内容");
    payload = { saved: true, batch, sections: saved.sections, sections_revision: 8 };
  } else if (url === "/api/handover/review/station-h/upload") {
    assert.deepEqual(JSON.parse(options.body), { duty_date: batch.duty_date, duty_shift: batch.duty_shift });
    payload = { job: { job_id: "job-h" } };
  } else if (url === "/api/jobs/job-h") {
    payload = { status: "success", result: { status: "success", record_url: "https://example.test/record-h" } };
  } else {
    assert.fail(`Unexpected request: ${url}`);
  }
  return new Response(JSON.stringify(payload), { headers: { "Content-Type": "application/json" } });
};
await page.uploadStationHAttachment();
assert.deepEqual(calls, ["/api/handover/review/station-h", "/api/handover/review/station-h/upload", "/api/jobs/job-h"]);
assert.equal(page.attachmentUploading.value, false);
assert.equal(page.attachmentRecordUrl.value, "https://example.test/record-h");
assert.match(page.statusText.value, /已上传/);

const automatic = { row_id: "training:auto-1", cells: { B: "新培训", D: "", E: "", F: "", G: "" } };
const recorded = { batch, saved: true, training_rows: [] };
const previewCalls = [];
globalThis.fetch = async (url, options) => {
  const path = url.split("?")[0];
  previewCalls.push(path);
  let payload;
  if (path.endsWith("/status")) {
    payload = { ...recorded, training_rows: [automatic], training_preview_row_ids: [automatic.row_id] };
  } else if (path.endsWith("/signatures/refresh")) {
    payload = { ...recorded, signature_refresh: { count: 1 } };
  } else if (path === "/api/handover/review/station-h") {
    const body = JSON.parse(options.body);
    assert.deepEqual(body.training_rows, [automatic]);
    assert.equal(body.training_preview_only, true);
    assert.equal("current_people_text" in body, false);
    assert.equal("sections" in body, false);
    payload = { ...recorded, training_rows: [automatic] };
  } else if (path.endsWith("/upload")) {
    payload = { job: { job_id: "job-preview" } };
  } else if (path === "/api/jobs/job-preview") {
    payload = { status: "success", result: { status: "success", record_url: "https://example.test/record-preview" } };
  } else {
    assert.fail(`Unexpected request: ${path}`);
  }
  return new Response(JSON.stringify(payload), { headers: { "Content-Type": "application/json" } });
};
await page.refreshStatus();
assert.deepEqual(page.form.value.training_rows, [automatic]);
await page.refreshSignatures();
assert.deepEqual(page.form.value.training_rows, [automatic], "签名刷新不能清掉尚未保存的培训预览");
await page.uploadStationHAttachment();
assert.deepEqual(previewCalls, [
  "/api/handover/review/station-h/status",
  "/api/handover/review/station-h/signatures/refresh",
  "/api/handover/review/station-h",
  "/api/handover/review/station-h/upload",
  "/api/jobs/job-preview",
]);
assert.deepEqual(page.state.value.training_preview_row_ids, []);
assert.equal(page.attachmentRecordUrl.value, "https://example.test/record-preview");

console.log("H station sections frontend checks passed");
