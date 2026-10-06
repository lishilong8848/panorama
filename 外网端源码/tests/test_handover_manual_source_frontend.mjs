import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createHandoverReviewActionHelpers } from "../web/frontend/dist/assets/handover_review_action_helpers.js";

const ref = (value) => ({ value });
const options = {
  session: ref({ session_id: "A|2026-10-06|day", duty_date: "2026-10-06", duty_shift: "day" }),
  buildingCode: "a",
  reviewClientId: "review-test",
  regenerateActionBase: ref({ allowed: true }),
  regenerateActionVm: ref({ disabledReason: "" }),
  activeRouteSelection: ref({}),
  clearSaveTimers() {},
  shouldPreferBootstrapLoad: () => true,
  loadReviewData: async () => {},
  regenerateHandoverReviewApi: () => { throw new Error("must use uploaded sources"); },
};
for (const name of ["dirty", "saving", "confirming", "cloudSyncBusy", "downloading", "capacityDownloading", "capacityImageSending", "syncingRemoteRevision", "regenerating"]) {
  options[name] = ref(false);
}
options.statusText = ref("");
options.errorText = ref("");
let requests = 0;
options.regenerateHandoverReviewFromFilesApi = async (building, form) => {
  requests += 1;
  assert.equal(building, "a");
  assert.equal(form.get("session_id"), "A|2026-10-06|day");
  assert.equal(form.get("client_id"), "review-test");
  assert.equal(await form.get("handover_source_file").text(), "handover");
  assert.equal(await form.get("capacity_source_file").text(), "capacity");
  return { job: { job_id: "generated" } };
};
const actions = createHandoverReviewActionHelpers(options);
const sources = { handover: new Blob(["handover"]), capacity: new Blob(["capacity"]) };
await actions.regenerateCurrentHandover(async () => ({ status: "success" }), sources);
assert.equal(requests, 1);
assert.equal(options.regenerating.value, false);
assert.match(options.statusText.value, /本楼本班次自动生成已跳过/);
assert.equal(options.errorText.value, "");

await actions.regenerateCurrentHandover(async () => ({ status: "success" }), { handover: sources.handover });
assert.equal(requests, 1);
assert.match(options.errorText.value, /同时选择/);

await actions.regenerateCurrentHandover(async () => ({ status: "failed", error: "capacity failed" }), sources);
assert.equal(requests, 2);
assert.equal(options.errorText.value, "capacity failed");
assert.equal(options.regenerating.value, false);

const app = await readFile(new URL("../web/frontend/dist/assets/handover_review_app.js", import.meta.url), "utf8");
const template = await readFile(new URL("../web/frontend/dist/assets/handover_review_template.js", import.meta.url), "utf8");
assert.match(app, /uploadedHandoverSource,\s+uploadedCapacitySource,/);
assert.match(app, /generateFromUploadedSources:/);
assert.equal((template.match(/type="file" accept="\.xlsx,\.xlsm"/g) || []).length, 2);
console.log("Handover dual-source frontend checks passed");
