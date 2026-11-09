import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { mountHandoverReviewApp } from "../web/frontend/dist/assets/handover_review_app.js";

const ref = (value) => ({ value });
const requests = [];
const mounted = [];
let page;
let fastTimeout = false;
globalThis.window = {
  location: { pathname: "/handover/review/110", search: "" },
  setTimeout: (callback, ms) => setTimeout(callback, fastTimeout ? 1 : ms),
  clearTimeout,
  setInterval: () => 1,
  clearInterval() {},
};
globalThis.fetch = (url, { signal }) => new Promise((resolve, reject) => {
  requests.push({ url, resolve });
  signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
});
mountHandoverReviewApp({
  ref,
  computed: (getter) => ({ get value() { return getter(); } }),
  createApp: ({ setup }) => ({ mount() { page = setup(); } }),
  onMounted: (callback) => mounted.push(callback),
  onBeforeUnmount() {},
});
const settle = () => new Promise((resolve) => setImmediate(resolve));
const reply = (index, day, power) => requests[index].resolve(new Response(JSON.stringify({
  batch: { duty_date: day, duty_shift: "day", batch_key: `${day}|day` },
  upload: { parsed_110kv_rows: [{ row_id: "incoming_akai", power_kw: power }] },
})));
mounted.forEach((callback) => callback());
assert.equal(requests.length, 1);
reply(0, "2026-11-09", "1100");
await settle();
assert.equal(page.loading.value, false);
assert.equal(page.parsedRows.value[0].power_kw, "1100");

const old = page.refreshStatus({ background: true });
page.dutyDate.value = "2026-11-08";
page.onStation110DutyChange();
assert.match(requests[2].url, /duty_date=2026-11-08&duty_shift=day/);
reply(2, "2026-11-08", "800");
await settle();
reply(1, "2026-11-09", "1100");
await old;
assert.equal(page.dutyDate.value, "2026-11-08");
assert.equal(page.parsedRows.value[0].power_kw, "800");
assert.equal(page.loading.value, false);

fastTimeout = true;
await page.refreshStatus();
assert.match(page.errorText.value, /请求超时/);
assert.equal(page.loading.value, false);
fastTimeout = false;
const retry = page.refreshStatus();
reply(4, "2026-11-08", "805");
await retry;
assert.equal(page.errorText.value, "");
assert.equal(page.parsedRows.value[0].power_kw, "805");

const source = await readFile(new URL("../web/frontend/dist/assets/handover_review_app.js", import.meta.url), "utf8");
const sharedState = ref({ substation_110kv: {} });
const locks = ref({ substation_110kv: {} });
const session = ref({ batch_key: "2026-11-09|day" });
const dirty = ref(false);
const noOp = () => {};
const bindings = {
  session, sharedBlocks: sharedState, sharedBlockLocks: locks,
  substation110kvDirty: dirty, substation110kvDirtyMarked: ref(false),
  dirty, statusText: ref(""), saving: ref(false),
  normalizeSubstation110kvBlock: (block = {}) => ({ batch_key: "", revision: 0, rows: [], ...block }),
  normalizeSharedLockPayload: (lock = {}) => lock,
  clearSubstation110kvAutoSaveTimer: noOp, clearSubstation110kvPreviewSyncTimer: noOp,
  clearSubstation110kvHeartbeat: noOp, clearSubstation110kvIdleReleaseTimer: noOp,
  restartSubstation110kvHeartbeat: noOp, scheduleSubstation110kvIdleRelease: noOp,
  stopSubstation110kvLocalEditing: () => assert.fail("must not stop editing for an older response"),
};
const mergeStart = source.indexOf("function applySharedBlockPayload(");
const mergeEnd = source.indexOf("function buildSubstation110kvPayload(", mergeStart);
const merge = new Function(...Object.keys(bindings), `${source.slice(mergeStart, mergeEnd)} return applySharedBlockPayload;`)(...Object.values(bindings));
const packet = (batch, revision, value) => ({
  shared_blocks: { substation_110kv: { batch_key: batch, revision, rows: [value] } },
});
merge({ ...packet("2026-11-09|day", 9, "day"), shared_block_locks: { substation_110kv: { client_holds_lock: true } } });
session.value = { batch_key: "2026-11-09|night" };
merge(packet("2026-11-09|night", 1, "night"));
assert.deepEqual(sharedState.value.substation_110kv.rows, ["night"]);
assert.equal(Boolean(locks.value.substation_110kv.client_holds_lock), false);
merge(packet("2026-11-09|day", 10, "stale day"));
assert.deepEqual(sharedState.value.substation_110kv.rows, ["night"]);
dirty.value = true;
merge(packet("2026-11-09|night", 0, "stale night"));
assert.deepEqual(sharedState.value.substation_110kv.rows, ["night"]);
assert.equal(dirty.value, true);
dirty.value = false;
session.value = { batch_key: "2026-11-08|day" };
merge({});
assert.deepEqual(sharedState.value.substation_110kv.rows, []);
assert.equal(sharedState.value.substation_110kv.batch_key, "2026-11-08|day");
console.log("110 station loading, stale response and shared batch checks passed");
