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

// 行数严格等于模板数据行数；越界行丢弃；缺列补空；值转字符串。
const sections = normalizeStationHSections(
  {
    "新事件处理": [{ cells: { B: "一级", H: 42 } }, { cells: { B: "越界行" } }],
    "三、注意事项交接": [{ cells: { B: "常备1" } }],
    "不存在的分类": [{ cells: { B: "x" } }],
  },
  schema,
);
assert.equal(sections["新事件处理"].length, 1, "新事件处理模板只有 1 行");
assert.deepEqual(sections["新事件处理"][0].cells, { B: "一级", H: "42" });
assert.equal(sections["三、注意事项交接"].length, 3, "注意事项模板有 3 行");
assert.deepEqual(sections["三、注意事项交接"][0].cells, { B: "常备1", H: "" });
assert.deepEqual(sections["三、注意事项交接"][1].cells, { B: "", H: "" });
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
await page.saveSelection();
assert.equal("sections" in requests[0], false, "未编辑的分类不得随其他内容提交");
page.form.value.sections["新事件处理"][0].cells.B = "未保存内容";
assert.equal(await page.saveSelection(), false);
assert.equal(requests[1].base_sections_revision, 7);
assert.equal(requests[1].sections["新事件处理"][0].cells.B, "未保存内容");
assert.equal(page.form.value.sections["新事件处理"][0].cells.B, "未保存内容");
assert.match(page.errorText.value, /分类已被更新/);

console.log("H station sections frontend checks passed");
