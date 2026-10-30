const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const root = path.resolve(__dirname, "..");
const source = fs.readFileSync(path.join(root, "web/frontend/src/app.js"), "utf8");
const context = vm.createContext({
  URL,
  location: { pathname: "/" },
  window: { location: { origin: "http://inspection.test" } },
});
vm.runInContext(source.split('document.addEventListener("click", handleAction);')[0], context);

async function main() {
  vm.runInContext('state.config = { features: { inspection_data: { enabled: true } } };', context);
  assert.equal(vm.runInContext("INSPECTION_FAMILIES.every(key => HOURLY_SOURCE_FAMILIES.has(key))", context), true);
  assert.equal(vm.runInContext('INSPECTION_FAMILIES.every(key => SOURCE_FILTERS.find(row => row.key === "hourly").families.includes(key))', context), true);
  vm.runInContext(`state.summary = { source_cache: {
    electrical_inspection_family: { buildings: [
      { building: "D楼", status_text: "已就绪", ready: true },
      { building: "E楼", status_text: "下载失败", last_error: "未上线" }
    ] }
  } };`, context);
  const html = vm.runInContext("renderSourceMatrix()", context);
  const buttons = [...html.matchAll(/<button\b[\s\S]*?<\/button>/g)].map(match => match[0]);
  const inspectionButtons = buttons.filter(button => /data-family="(?:electrical|hvac)_inspection_family"/.test(button));
  assert.equal(inspectionButtons.length, 4);
  assert(inspectionButtons.some(button => button.includes('data-building="D楼"')));
  assert(inspectionButtons.some(button => button.includes('data-building="E楼"')));
  assert.equal((html.match(/不适用/g) || []).length, 6);
  assert.equal(buttons.filter(button => button.includes('data-family="handover_log_family"')).length, 5);
  vm.runInContext(`
    render = () => {};
    refreshAll = async () => {};
    setMessage = () => {};
    const requests = [];
    api = async (url, options) => { requests.push({url, options}); return {}; };
  `, context);
  for (const family of ["electrical_inspection_family", "hvac_inspection_family"]) {
  for (const building of ["A楼", "D楼", "E楼"]) {
    await vm.runInContext(`handleAction({ target: { closest: () => ({ dataset: {
      action: "refresh-building", family: ${JSON.stringify(family)}, building: ${JSON.stringify(building)}
    } }) } })`, context);
  }
  }
  const requests = JSON.parse(vm.runInContext("JSON.stringify(requests)", context));
  assert.equal(requests.length, 4);
  assert.deepEqual(requests.map(row => new URL(row.url, "http://inspection.test").searchParams.get("building")), ["D楼", "E楼", "D楼", "E楼"]);
  assert(requests.every(row => row.options.method === "POST"));
  vm.runInContext('state.config.features.inspection_data.enabled = false;', context);
  const disabledHtml = vm.runInContext('renderSourceMatrix()', context);
  assert(disabledHtml.includes('未启用'));
  assert(vm.runInContext('renderConfig()', context).includes('name="inspectionEnabled"'));
  await vm.runInContext(`handleAction({ target: { closest: () => ({ dataset: {
    action: "refresh-building", family: "electrical_inspection_family", building: "D楼"
  } }) } })`, context);
  assert.equal(vm.runInContext('requests.length', context), 4);
  vm.runInContext(`
    const messages = [];
    setMessage = (message, error) => messages.push({message, error});
    api = async () => ({ok: true, accepted: false, ready: true, running: false,
      reason: "already_ready", message: "E楼已有可用文件，无需重复下载"});
  `, context);
  await vm.runInContext(`handleAction({ target: { closest: () => ({ dataset: {
    action: "refresh-building", family: "monthly_report_family", building: "E楼"
  } }) } })`, context);
  assert.equal(vm.runInContext('messages[0].message', context), "E楼已有可用文件，无需重复下载");
  assert.equal(vm.runInContext('!!messages[0].error', context), false);
  assert.equal(vm.runInContext('state.busy.size', context), 0);
  for (const target of ["web/frontend/dist/assets/app.js", "web_frontend/dist/assets/app.js"]) {
    assert.equal(fs.readFileSync(path.join(root, target), "utf8"), source);
  }
  console.log("Inspection frontend: hourly group, D/E actions, other families, and both build outputs verified.");
}

main().catch(error => { console.error(error); process.exitCode = 1; });
