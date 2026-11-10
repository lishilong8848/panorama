// Directed tests for the "生成成品并提交" (prepare-delivery) attachment flow.
// Imports the frontend sources directly; no dist build and no network/service is touched.
import assert from "node:assert/strict";

const SRC = "../web/frontend/src";

const {
  ATTACHMENT_DELIVERY_STATUSES,
  createHandoverReviewActionHelpers,
  emptyAttachmentDelivery,
  normalizeAttachmentDelivery,
  resolveAttachmentDeliveryGate,
  resolveAttachmentDeliveryUi,
} = await import(`${SRC}/handover_review_action_helpers.js`);

const results = [];
async function test(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
    console.log(`PASS  ${name}`);
  } catch (error) {
    results.push({ name, ok: false, error });
    console.log(`FAIL  ${name}\n      ${error && error.message}`);
  }
}

// ---------------------------------------------------------------------------
// Minimal Vue-compatible ref/computed stand-ins.
function makeRef(value) {
  return {
    value,
    __isRef: true,
  };
}
function makeComputed(getter) {
  return {
    get value() {
      return getter();
    },
    __isRef: true,
  };
}

// ---------------------------------------------------------------------------
// Harness: builds action helpers with a fake API surface and a stub DOM.
function createHarness(overrides = {}) {
  const calls = { prepare: [], job: [], download: [], send: [], regenerate: [] };
  let payload = overrides.initialPayload || {};

  const session = makeRef({
    session_id: "S-1",
    revision: 7,
    confirmed: false,
    duty_date: "2026-11-08",
    duty_shift: "day",
    output_file: "交接班日志.xlsx",
    capacity_output_file: "交接班容量报表.xlsx",
  });

  const attachmentDelivery = makeRef(emptyAttachmentDelivery());
  const attachmentGenerationId = makeRef("");
  const preparingDelivery = makeRef(false);
  const statusText = makeRef("");
  const errorText = makeRef("");
  const dirty = makeRef(false);
  const regenerating = makeRef(false);

  const allowedBase = { allowed: true, visible: true, disabledReason: "" };
  const allowedVm = { allowed: true, visible: true, disabled: false, disabledReason: "", text: "x" };

  function applyPayloadMeta(next = {}) {
    payload = { ...payload, ...next };
    if (Object.prototype.hasOwnProperty.call(next, "attachment_delivery")) {
      attachmentDelivery.value = normalizeAttachmentDelivery(next.attachment_delivery);
    }
  }

  const helpers = createHandoverReviewActionHelpers({
    buildingCode: "H",
    building: makeRef("H"),
    reviewClientId: "client-abc",
    session,
    reviewContext: makeRef({ duty_date: "2026-11-08", duty_shift: "day" }),
    activeRouteSelection: makeRef({ sessionId: "", dutyDate: "", dutyShift: "" }),
    historyState: makeRef({}),
    isHistoryMode: makeRef(false),
    isSubstationBuilding: makeRef(false),
    loading: makeRef(false),
    saving: makeRef(false),
    downloading: makeRef(false),
    capacityDownloading: makeRef(false),
    capacityImageSending: makeRef(false),
    regenerating,
    attachmentDelivery,
    attachmentGenerationId,
    preparingDelivery,
    confirming: makeRef(false),
    retryingCloudSync: makeRef(false),
    updatingHistoryCloudSync: makeRef(false),
    dirty,
    syncingRemoteRevision: makeRef(false),
    needsRefresh: makeRef(false),
    staleRevisionConflict: makeRef(false),
    cloudSyncBusy: makeRef(false),
    statusText,
    errorText,

    refreshActionBase: makeRef(allowedBase),
    refreshActionVm: makeRef(allowedVm),
    saveActionBase: makeRef(allowedBase),
    saveActionVm: makeRef(allowedVm),
    downloadActionBase: makeRef(allowedBase),
    downloadActionVm: makeRef(allowedVm),
    capacityDownloadActionBase: makeRef(allowedBase),
    capacityDownloadActionVm: makeRef(allowedVm),
    capacityImageSendActionBase: makeRef(allowedBase),
    capacityImageSendActionVm: makeRef(allowedVm),
    regenerateActionBase: makeRef(allowedBase),
    regenerateActionVm: makeRef(allowedVm),
    confirmActionBase: makeRef(allowedBase),
    confirmActionVm: makeRef(allowedVm),
    retryCloudSyncActionBase: makeRef(allowedBase),
    retryCloudSyncActionVm: makeRef(allowedVm),
    updateHistoryCloudSyncActionBase: makeRef(allowedBase),
    updateHistoryCloudSyncActionVm: makeRef(allowedVm),

    ensureEditingLock: async () => true,
    // Keep the directed test fast; production keeps the 2.5s tick / 15min budget.
    attachmentDeliveryPollOptions: { intervalMs: 0, timeoutMs: 5000 },
    saveDocument: async () => true,
    clearSaveTimers: () => {},
    loadReviewData: async (options) => {
      if (overrides.pollPayload) assert.equal(options.deliveryPoll, true, "polling must bypass the confirming guard");
      applyPayloadMeta(overrides.pollPayload ? overrides.pollPayload() : {});
      return true;
    },
    shouldPreferBootstrapLoad: () => false,
    beginRemoteSaveRefresh: () => {},
    applyPayloadMeta,
    broadcastHandoverReviewStatusChange: () => {},
    resolveOperationFeedbackText: (_payload, fallback) => fallback,
    isRevisionConflictError: () => false,
    documentRef: makeRef({}),

    prepareHandoverReviewDeliveryApi: async (building, body) => {
      calls.prepare.push({ building, body });
      return {
        status: "accepted",
        job: { job_id: overrides.jobId || "job-1" },
        generation_id: overrides.generationId || "gen-1",
        attachment_delivery: {
          status: "queued",
          artifacts_ready: false,
          generation_id: overrides.generationId || "gen-1",
          error: "",
          record_url: "",
        },
      };
    },
    buildHandoverReviewDownloadUrl: (building, sessionId, params) => {
      calls.download.push({ kind: "legacy", building, sessionId, params });
      return "/legacy";
    },
    buildHandoverReviewCapacityDownloadUrl: (building, sessionId, params) => {
      calls.download.push({ kind: "legacy-capacity", building, sessionId, params });
      return "/legacy-capacity";
    },
    buildHandoverReviewDeliveryDownloadUrl: (building, sessionId, params) => {
      calls.download.push({ kind: "delivery", building, sessionId, params });
      return "/delivery";
    },
    triggerBrowserDownload: async (url) => {
      calls.download.push({ kind: "trigger", url });
      return { warning: "" };
    },
    prepareHandoverReviewCapacityImageApi: async (building, body) => {
      calls.send.push({ step: "prepare", building, body });
      return { send_token: "token-1" };
    },
    sendHandoverReviewCapacityImageApi: async (building, body) => {
      calls.send.push({ step: "send", building, body });
      return { ok: true, status: "success" };
    },
    regenerateHandoverReviewApi: async (building, body) => {
      calls.regenerate.push({ building, body });
      return { job: { job_id: "regen-1" } };
    },
    regenerateHandoverReviewFromFilesApi: async (building, form) => {
      calls.regenerate.push({ building, form });
      return { job: { job_id: "regen-2" } };
    },
    retryHandoverReviewCloudSyncApi: async () => ({ job: { job_id: "retry-1" } }),
    updateHandoverReviewCloudSyncApi: async () => ({ status: "ok" }),
    switchToSession: async () => {},
    window: undefined,
  });

  return {
    helpers,
    calls,
    refs: { session, attachmentDelivery, attachmentGenerationId, preparingDelivery, statusText, errorText, dirty, regenerating },
    getPayload: () => payload,
  };
}

// ---------------------------------------------------------------------------
await test("normalizeAttachmentDelivery keeps only known statuses and ready flags", () => {
  assert.deepEqual(normalizeAttachmentDelivery(null), emptyAttachmentDelivery());

  const busy = normalizeAttachmentDelivery({ status: "Preparing", generation_id: " gen-9 " });
  assert.equal(busy.status, "preparing");
  assert.equal(busy.generation_id, "gen-9");
  assert.equal(busy.artifacts_ready, false);

  const ready = normalizeAttachmentDelivery({ status: "ready", artifacts_ready: true, generation_id: "gen-9" });
  assert.equal(ready.artifacts_ready, true);

  // artifacts_ready must not be trusted for failed/preparing payloads.
  const lying = normalizeAttachmentDelivery({ status: "preparing", artifacts_ready: true, generation_id: "gen-9" });
  assert.equal(lying.artifacts_ready, false);

  const unknown = normalizeAttachmentDelivery({ status: "bogus", generation_id: "" });
  assert.equal(unknown.status, "");
  assert.equal(ATTACHMENT_DELIVERY_STATUSES.includes(unknown.status), false);
});

await test("gate locks download/send until artifacts_ready + matching generation", () => {
  const queued = resolveAttachmentDeliveryGate(
    { status: "queued", artifacts_ready: false, generation_id: "gen-1" },
    { expectedGenerationId: "gen-1" },
  );
  assert.equal(queued.is_busy, true);
  assert.equal(queued.can_download, false);
  assert.equal(queued.can_send, false);
  assert.equal(queued.can_regenerate, false);
  assert.equal(queued.can_refresh, true);

  const preparing = resolveAttachmentDeliveryGate(
    { status: "preparing", artifacts_ready: false, generation_id: "gen-1" },
    { expectedGenerationId: "gen-1" },
  );
  assert.equal(preparing.is_busy, true);
  assert.equal(preparing.can_download, false);

  const ready = resolveAttachmentDeliveryGate(
    { status: "ready", artifacts_ready: true, generation_id: "gen-1" },
    { expectedGenerationId: "gen-1" },
  );
  assert.equal(ready.is_ready, true);
  assert.equal(ready.can_download, true);
  assert.equal(ready.can_send, true);
  assert.equal(ready.can_regenerate, true);

  // Ready artifacts from a previous submission must stay locked.
  const superseded = resolveAttachmentDeliveryGate(
    { status: "ready", artifacts_ready: true, generation_id: "gen-0" },
    { expectedGenerationId: "gen-1" },
  );
  assert.equal(superseded.is_stale, true);
  assert.equal(superseded.can_download, false);
  assert.equal(superseded.can_send, false);
  assert.equal(superseded.show_retry, true);
});

await test("failed delivery reports its error and allows retry", () => {
  const failed = resolveAttachmentDeliveryGate(
    { status: "failed", artifacts_ready: false, generation_id: "gen-1", error: "模板缺失" },
    { expectedGenerationId: "gen-1" },
  );
  assert.equal(failed.is_failed, true);
  assert.equal(failed.show_retry, true);
  assert.equal(failed.can_download, false);
  assert.equal(failed.can_prepare, true);
  assert.match(failed.reason, /模板缺失/);

  const ui = resolveAttachmentDeliveryUi(
    { status: "failed", artifacts_ready: false, generation_id: "gen-1", error: "模板缺失" },
    { expectedGenerationId: "gen-1" },
  );
  assert.equal(ui.tone, "danger");
  assert.match(ui.text, /模板缺失/);
});

await test("ready UI surfaces the multi-dim record link", () => {
  const ui = resolveAttachmentDeliveryUi(
    { status: "uploaded", artifacts_ready: true, generation_id: "gen-1", record_url: "https://example/rec/1" },
    { expectedGenerationId: "gen-1" },
  );
  assert.equal(ui.tone, "success");
  assert.equal(ui.gate.delivery.record_url, "https://example/rec/1");
  assert.equal(ui.gate.can_download, true);
});

await test("prepare delivery posts session_id/base_revision/client_id and unlocks only for its generation", async () => {
  let emits = 0;
  const harness = createHarness({
    generationId: "gen-42",
    pollPayload: () => {
      emits += 1;
      return { attachment_delivery: {
        status: emits >= 3 ? "uploaded" : emits === 2 ? "ready" : "preparing",
        artifacts_ready: emits >= 2, generation_id: "gen-42",
      } };
    },
  });

  await harness.helpers.prepareAttachmentDelivery();

  assert.equal(harness.calls.prepare.length, 1);
  const [, body] = [harness.calls.prepare[0].building, harness.calls.prepare[0].body];
  assert.equal(harness.calls.prepare[0].building, "H");
  assert.deepEqual(
    { session_id: body.session_id, base_revision: body.base_revision, client_id: body.client_id },
    { session_id: "S-1", base_revision: 7, client_id: "client-abc" },
  );

  assert.equal(harness.refs.attachmentGenerationId.value, "gen-42");
  assert.equal(harness.refs.attachmentDelivery.value.status, "uploaded");
  assert.equal(harness.refs.attachmentDelivery.value.artifacts_ready, true);
  assert.equal(harness.refs.preparingDelivery.value, false, "preparing flag resets after completion");
  assert.match(harness.refs.statusText.value, /本楼班次附件已上传/);
  assert.equal(emits, 3, "the button stops when this floor uploads, without waiting for supplements");

  // Download is decoupled from 成品 submission: the header button always uses the raw
  // download endpoint; the backend serves the current 成品 artifact when ready, else the
  // force-synced raw file. The delivery-download endpoint is no longer used here.
  await harness.helpers.downloadCurrentReviewFile();
  const legacyUrl = harness.calls.download.find((item) => item.kind === "legacy");
  assert.ok(legacyUrl, "raw download endpoint used");
  assert.equal(harness.calls.download.some((item) => item.kind === "delivery"), false);
});

await test("download/send/regenerate are blocked while the building job is queued", async () => {
  const harness = createHarness();
  harness.refs.attachmentDelivery.value = normalizeAttachmentDelivery({
    status: "queued",
    artifacts_ready: false,
    generation_id: "gen-1",
  });
  harness.refs.attachmentGenerationId.value = "gen-1";

  await harness.helpers.downloadCurrentReviewFile();
  assert.equal(harness.calls.download.length, 0, "download blocked");
  assert.match(harness.refs.statusText.value, /成品/);

  await harness.helpers.sendCurrentCapacityImage(async () => null);
  assert.equal(harness.calls.send.length, 0, "send blocked");

  await harness.helpers.regenerateCurrentHandover(async () => null);
  assert.equal(harness.calls.regenerate.length, 0, "regenerate blocked");

  // Refresh stays available so the operator can re-read the job state.
  assert.equal(resolveAttachmentDeliveryGate(harness.refs.attachmentDelivery.value,
    { expectedGenerationId: "gen-1" }).can_refresh, true);
});

await test("stale generation keeps send locked but download falls back to the raw file", async () => {
  const harness = createHarness();
  harness.refs.attachmentDelivery.value = normalizeAttachmentDelivery({
    status: "ready",
    artifacts_ready: true,
    generation_id: "gen-old",
  });
  harness.refs.attachmentGenerationId.value = "gen-new";

  // 成品已过期：不再把旧成品当作可下载版本，下载回落到原始（强制补写最新保存版）通道。
  await harness.helpers.downloadCurrentReviewFile();
  assert.ok(
    harness.calls.download.some((item) => item.kind === "legacy"),
    "stale delivery falls back to the raw download endpoint",
  );
  assert.equal(
    harness.calls.download.some((item) => item.kind === "delivery"),
    false,
    "stale 成品 artifact is never served",
  );

  // 发送审核文本/容量表图片仍要求当前版本成品就绪，过期时保持锁定。
  await harness.helpers.sendCurrentCapacityImage(async () => null);
  assert.equal(harness.calls.send.length, 0);
});

await test("failed generation surfaces the error and permits a retry", async () => {
  let poll = 0;
  const harness = createHarness({
    generationId: "gen-7",
    pollPayload: () => {
      poll += 1;
      return poll >= 1
        ? { attachment_delivery: { status: "failed", artifacts_ready: false, generation_id: "gen-7", error: "云表写入失败" } }
        : {};
    },
  });

  await harness.helpers.prepareAttachmentDelivery();
  assert.match(harness.refs.errorText.value, /云表写入失败/);

  const gate = harness.helpers.resolveDeliveryGate();
  assert.equal(gate.show_retry, true);
  assert.equal(gate.can_prepare, true);
});

await test("review regeneration blocks both downloads and manual send until it ends", async () => {
  const harness = createHarness();
  harness.refs.attachmentDelivery.value = normalizeAttachmentDelivery({
    status: "ready",
    artifacts_ready: true,
    generation_id: "gen-5",
  });
  harness.refs.attachmentGenerationId.value = "gen-5";
  harness.refs.regenerating.value = true;
  await harness.helpers.downloadCurrentReviewFile();
  await harness.helpers.downloadCurrentCapacityReviewFile();
  await harness.helpers.sendCurrentCapacityImage(async () => null);
  assert.equal(harness.calls.download.length, 0);
  assert.equal(harness.calls.send.length, 0);

  harness.refs.regenerating.value = false;
  await harness.helpers.downloadCurrentReviewFile();
  await harness.helpers.downloadCurrentCapacityReviewFile();
  await harness.helpers.sendCurrentCapacityImage(async () => null);
  assert.deepEqual(harness.calls.download.filter((item) => item.kind === "trigger").map((item) => item.url),
    ["/legacy", "/legacy-capacity"]);
  assert.equal(harness.calls.send.length, 2);
});

await test("send uses this generation and never fires during generation", async () => {
  const harness = createHarness();
  harness.refs.attachmentDelivery.value = normalizeAttachmentDelivery({
    status: "ready",
    artifacts_ready: true,
    generation_id: "gen-5",
  });
  harness.refs.attachmentGenerationId.value = "gen-5";

  await harness.helpers.sendCurrentCapacityImage(async () => null);
  assert.equal(harness.calls.send.length, 2);
  assert.equal(harness.calls.send[0].body.generation_id, "gen-5");
  assert.equal(harness.calls.send[1].body.generation_id, "gen-5");

  // Generating never triggers a send by itself.
  const fresh = createHarness({
    generationId: "gen-8",
    pollPayload: () => ({ attachment_delivery: { status: "uploaded", artifacts_ready: true, generation_id: "gen-8" } }),
  });
  await fresh.helpers.prepareAttachmentDelivery();
  assert.equal(fresh.calls.send.length, 0, "no auto-send after generation");
});

await test("two source-file upload entry is preserved", async () => {
  const harness = createHarness();
  harness.refs.attachmentDelivery.value = normalizeAttachmentDelivery({
    status: "ready",
    artifacts_ready: true,
    generation_id: "gen-3",
  });
  harness.refs.attachmentGenerationId.value = "gen-3";

  const files = { handover: { name: "a.xlsx" }, capacity: { name: "b.xlsx" } };
  await harness.helpers.regenerateCurrentHandover(async () => ({ status: "success" }), files);
  assert.equal(harness.calls.regenerate.length, 1);
  assert.ok(harness.calls.regenerate[0].form, "upload-from-files API used");
});

// ---------------------------------------------------------------------------
const failed = results.filter((item) => !item.ok);
console.log(`\n${results.length - failed.length}/${results.length} passed`);
if (failed.length) {
  process.exitCode = 1;
}
