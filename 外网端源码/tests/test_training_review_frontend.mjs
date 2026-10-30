import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { appendTrainingRows } from "../web/frontend/src/handover_training_helpers.js";
import { createHandoverReviewActionHelpers } from "../web/frontend/src/handover_review_action_helpers.js";

const edited = [{ row_id: "training:1", cells: { B: "用户修改", G: "用户人员" } }, { row_id: "manual", cells: { B: "手工" } }];
const merged = appendTrainingRows(edited, [{ row_id: "training:1", cells: { B: "源更新" } }, { row_id: "training:2", cells: { B: "新增" } }]);
assert.deepEqual(merged.slice(0, 2), edited);
assert.equal(merged.length, 3);
assert.deepEqual(appendTrainingRows(merged, merged), merged);

const ref = (value) => ({ value });
const activeRouteSelection = ref({});
let loads = 0;
let saves = 0;
let allowSave = false;
const actions = createHandoverReviewActionHelpers({
  dirty: ref(true), selectedSessionId: ref("A楼|2026-10-27|day"), activeRouteSelection,
  needsRefresh: ref(false), staleRevisionConflict: ref(false), errorText: ref(""), statusText: ref(""),
  saveDocument: async () => { saves++; return allowSave; }, releaseCurrentLock: async () => {},
  clearSaveTimers() {}, syncReviewSelectionToUrl() {}, shouldPreferBootstrapLoad: () => true,
  loadReviewData: async () => { loads++; },
});
await actions.switchToSession("", { dutyDate: "2026-03-01", dutyShift: "night" });
assert.equal(loads, 0);
allowSave = true;
await actions.switchToSession("", { dutyDate: "2026-03-01", dutyShift: "night" });
assert.equal(loads, 1);
assert.equal(saves, 2);
assert.deepEqual(activeRouteSelection.value, { sessionId: "", dutyDate: "2026-03-01", dutyShift: "night" });

const source = readFileSync(new URL("../web/frontend/src/handover_review_app.js", import.meta.url), "utf8");
const historyApply = source.slice(source.indexOf("function applyHistoryPayload("), source.indexOf("async function ensureHistoryLoaded("));
assert.ok(!historyApply.includes("activeRouteSelection.value ="));
assert.ok(source.includes("if (requestSeq !== stationHRequestSeq) return;"));
assert.ok(source.includes("if (!stillSelected()) return false;"));
console.log("training/history frontend checks passed");
