import {
  buildHandoverReviewCapacityDownloadUrl,
  buildHandoverReviewDownloadUrl,
  claimHandoverReviewLockApi,
  confirmHandoverReviewApi,
  getJobApi,
  getHandoverReviewApi,
  heartbeatHandoverReviewLockApi,
  releaseHandoverReviewLockApi,
  retryHandoverReviewCloudSyncApi,
  saveHandoverReviewApi,
  updateHandoverReviewCloudSyncApi,
  unconfirmHandoverReviewApi,
} from "./api_client.js";
import { HANDOVER_REVIEW_TEMPLATE } from "./handover_review_template.js";

const FALLBACK_COLUMN_LETTERS = ["B", "C", "D", "E", "F", "G", "H", "I"];
const DEFAULT_FOOTER_INVENTORY_COLUMNS = [
  { key: "B", label: "交接工具名称", source_cols: ["B"], span: 1 },
  { key: "C", label: "存放位置", source_cols: ["C", "D"], span: 2 },
  { key: "E", label: "数量", source_cols: ["E"], span: 1 },
  { key: "F", label: "是否存在损坏", source_cols: ["F"], span: 1 },
  { key: "G", label: "其他补充说明", source_cols: ["G"], span: 1 },
  { key: "H", label: "清点确认人（接班）", source_cols: ["H"], span: 1 },
];
const REVIEW_PATH_RE = /^\/handover\/review\/([a-e])\/?$/i;
const REVIEW_IDLE_AUTOSAVE_DELAY_MS = 8000;
const REVIEW_SAVE_FAILURE_RETRY_DELAY_MS = 30000;
const REVIEW_SAVE_MAX_IDLE_RETRY_AFTER_FAILURE = 1;
const DEFAULT_POLL_INTERVAL_MS = 5000;
const REVIEW_LOCK_HEARTBEAT_MS = 15000;
const REVIEW_CLIENT_ID_STORAGE_KEY = "handover_review_client_id";
const REVIEW_CLIENT_LABEL_STORAGE_KEY = "handover_review_client_label";
const HANDOVER_REVIEW_STATUS_BROADCAST_KEY = "handover_review_status_broadcast_v1";
const CAPACITY_SYNC_TRACKED_CELLS = ["H6", "F8", "B6", "D6", "F6", "B13", "D13"];

function shiftTextFromCode(shift) {
  const normalized = String(shift || "").trim().toLowerCase();
  if (normalized === "day") return "白班";
  if (normalized === "night") return "夜班";
  return String(shift || "").trim() || "-";
}

function syncReviewSelectionToUrl({ sessionId = "", isLatest = false } = {}) {
  if (typeof window === "undefined" || !window.history?.replaceState) return;
  const url = new URL(window.location.href);
  url.searchParams.delete("session_id");
  url.searchParams.delete("duty_date");
  url.searchParams.delete("duty_shift");
  if (sessionId && !isLatest) {
    url.searchParams.set("session_id", sessionId);
  }
  window.history.replaceState(window.history.state, "", `${url.pathname}${url.search}${url.hash}`);
}

function basenameFromPath(input) {
  const text = String(input || "").trim();
  if (!text) return "";
  const parts = text.split(/[\\/]/).filter(Boolean);
  return parts.length ? parts[parts.length - 1] : text;
}

function badgeVm(text, tone = "neutral", emphasis = "soft", icon = "dot") {
  return {
    text: String(text || "").trim() || "-",
    tone,
    emphasis,
    icon,
  };
}

function randomHex(size = 8) {
  let output = "";
  while (output.length < size) {
    output += Math.random().toString(16).slice(2);
  }
  return output.slice(0, size);
}

function ensureReviewClientIdentity() {
  const fallbackId = `review-${randomHex(8)}`;
  const buildLabel = (id) => `终端-${String(id || "").trim().slice(-4).toUpperCase() || "----"}`;
  if (typeof window === "undefined" || !window.sessionStorage) {
    return {
      clientId: fallbackId,
      holderLabel: buildLabel(fallbackId),
    };
  }
  try {
    let clientId = String(window.sessionStorage.getItem(REVIEW_CLIENT_ID_STORAGE_KEY) || "").trim();
    if (!clientId) {
      clientId = fallbackId;
      window.sessionStorage.setItem(REVIEW_CLIENT_ID_STORAGE_KEY, clientId);
    }
    let holderLabel = String(window.sessionStorage.getItem(REVIEW_CLIENT_LABEL_STORAGE_KEY) || "").trim();
    if (!holderLabel) {
      holderLabel = buildLabel(clientId);
      window.sessionStorage.setItem(REVIEW_CLIENT_LABEL_STORAGE_KEY, holderLabel);
    }
    return { clientId, holderLabel };
  } catch (_error) {
    return {
      clientId: fallbackId,
      holderLabel: buildLabel(fallbackId),
    };
  }
}

function broadcastHandoverReviewStatusChange(payload = {}) {
  if (typeof window === "undefined" || !window.localStorage) return;
  try {
    const sessionPayload = payload?.session && typeof payload.session === "object" ? payload.session : {};
    const batchPayload = payload?.batch_status && typeof payload.batch_status === "object" ? payload.batch_status : {};
    window.localStorage.setItem(
      HANDOVER_REVIEW_STATUS_BROADCAST_KEY,
      JSON.stringify({
        ts: Date.now(),
        building: String(sessionPayload.building || "").trim(),
        session_id: String(sessionPayload.session_id || "").trim(),
        batch_key: String(batchPayload.batch_key || sessionPayload.batch_key || "").trim(),
        revision: Number(sessionPayload.revision || 0),
        confirmed: Boolean(sessionPayload.confirmed),
      }),
    );
  } catch (_error) {
    // ignore cross-tab sync failures
  }
}

function mapReviewCloudSheetSync(raw) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const attempted = Boolean(raw?.attempted);
  const url = String(raw?.spreadsheet_url || "").trim();
  const error = String(raw?.error || "").trim();
  if (status === "success") return { text: "云表已同步", tone: "success", url, error };
  if (status === "failed") return { text: "云表最终上传失败", tone: "danger", url, error };
  if (status === "prepare_failed") return { text: "云表预建失败", tone: "danger", url, error };
  if (status === "pending_upload") return { text: "云表待最终上传", tone: "warning", url, error };
  if (status === "disabled") return { text: "云表未启用", tone: "neutral", url, error };
  if (status === "skipped") return { text: "云表未执行", tone: "neutral", url, error };
  if (attempted) return { text: "云表已尝试同步", tone: "info", url, error };
  return { text: "云表未执行", tone: "neutral", url, error };
}

function cloneDeep(value) {
  return JSON.parse(JSON.stringify(value ?? null));
}

function normalizeField(field) {
  const cell = String(field?.cell || "").trim().toUpperCase();
  return {
    cell,
    label: String(field?.label || cell || "字段"),
    value: String(field?.value ?? ""),
  };
}

function normalizeFixedBlock(block, index) {
  const blockId = String(block?.id || `block_${index}`).trim() || `block_${index}`;
  const title = String(block?.title || blockId).trim() || blockId;
  const fields = Array.isArray(block?.fields) ? block.fields.map(normalizeField) : [];
  return { id: blockId, title, fields };
}

function normalizeSectionColumn(column, index, fallbackHeader = "") {
  const key = String(column?.key || "").trim().toUpperCase() || FALLBACK_COLUMN_LETTERS[index] || `COL_${index}`;
  const sourceCols = Array.isArray(column?.source_cols)
    ? column.source_cols.map((item) => String(item || "").trim().toUpperCase()).filter(Boolean)
    : [];
  return {
    key,
    label: String(column?.label || fallbackHeader || key).trim() || key,
    source_cols: sourceCols.length ? sourceCols : [key],
    span: Math.max(1, Number.parseInt(column?.span ?? sourceCols.length ?? 1, 10) || 1),
  };
}

function resolveSectionColumns(section) {
  if (Array.isArray(section?.columns) && section.columns.length) {
    return section.columns.map((column, index) =>
      normalizeSectionColumn(column, index, Array.isArray(section?.header) ? section.header[index] : ""),
    );
  }

  const header = Array.isArray(section?.header) ? section.header : [];
  return FALLBACK_COLUMN_LETTERS.map((column, index) =>
    normalizeSectionColumn(
      {
        key: column,
        label: header[index] || column,
        source_cols: [column],
        span: 1,
      },
      index,
      header[index] || column,
    ),
  );
}

function hasSectionRowContent(row, columns) {
  if (!row || !row.cells || !Array.isArray(columns)) return false;
  return columns.some((column) => String(row.cells[column.key] || "").trim());
}

function blankRow(columns) {
  const cells = {};
  for (const column of columns || []) {
    cells[column.key] = "";
  }
  return {
    row_id: `tmp_${Date.now()}_${Math.random().toString(16).slice(2)}`,
    cells,
    is_placeholder_row: true,
  };
}

function normalizeSectionRow(row, columns) {
  const cells = {};
  const rawCells = row?.cells && typeof row.cells === "object" ? row.cells : {};
  for (const column of columns || []) {
    cells[column.key] = String(rawCells[column.key] ?? "");
  }
  const normalizedRow = {
    row_id: String(row?.row_id || `row_${Date.now()}_${Math.random().toString(16).slice(2)}`),
    cells,
    is_placeholder_row: Boolean(row?.is_placeholder_row),
  };
  normalizedRow.is_placeholder_row = !hasSectionRowContent(normalizedRow, columns);
  return normalizedRow;
}

function normalizeSection(section) {
  const columns = resolveSectionColumns(section);
  const rows = Array.isArray(section?.rows) ? section.rows.map((row) => normalizeSectionRow(row, columns)) : [];
  return {
    name: String(section?.name || "未命名分类"),
    columns,
    header: columns.map((column) => column.label || column.key),
    rows: rows.length ? rows : [blankRow(columns)],
  };
}

function normalizeFooterInventoryColumn(column, index) {
  const fallback = DEFAULT_FOOTER_INVENTORY_COLUMNS[index] || DEFAULT_FOOTER_INVENTORY_COLUMNS[0];
  const key = String(column?.key || fallback.key || "").trim().toUpperCase();
  const sourceCols = Array.isArray(column?.source_cols)
    ? column.source_cols.map((item) => String(item || "").trim().toUpperCase()).filter(Boolean)
    : fallback.source_cols;
  return {
    key,
    label: String(column?.label || fallback.label || key),
    source_cols: sourceCols.length ? sourceCols : [key],
    span: Math.max(1, Number.parseInt(column?.span ?? sourceCols.length ?? fallback.span ?? 1, 10) || 1),
  };
}

function footerRowHasContent(row, columns) {
  if (!row || !row.cells || !Array.isArray(columns)) return false;
  return columns.some((column) => String(row.cells[column.key] || "").trim());
}

function blankFooterInventoryRow(columns) {
  const cells = {};
  for (const column of columns || []) {
    cells[column.key] = "";
  }
  return {
    row_id: `inventory_${Date.now()}_${Math.random().toString(16).slice(2)}`,
    cells,
    is_placeholder_row: true,
  };
}

function blankFooterInventoryRowWithDefaults(columns, defaultCells = {}) {
  const cells = {};
  for (const column of columns || []) {
    cells[column.key] = String(defaultCells?.[column.key] ?? "");
  }
  const row = {
    row_id: `inventory_${Date.now()}_${Math.random().toString(16).slice(2)}`,
    cells,
    is_placeholder_row: true,
  };
  row.is_placeholder_row = !footerRowHasContent(row, columns);
  return row;
}

function resolveFooterAutoFillCells(block) {
  if (!block || block.type !== "inventory_table" || !Array.isArray(block.rows)) {
    return {};
  }

  for (let index = block.rows.length - 1; index >= 0; index -= 1) {
    const row = block.rows[index];
    const checker = String(row?.cells?.H ?? "").trim();
    if (checker) {
      return { H: checker };
    }
  }

  return {
    H: "",
  };
}

function normalizeFooterInventoryRow(row, columns) {
  const rawCells = row?.cells && typeof row.cells === "object" ? row.cells : {};
  const cells = {};
  for (const column of columns || []) {
    cells[column.key] = String(rawCells[column.key] ?? "");
  }
  const normalizedRow = {
    row_id: String(row?.row_id || `inventory_row_${Date.now()}_${Math.random().toString(16).slice(2)}`),
    cells,
    is_placeholder_row: Boolean(row?.is_placeholder_row),
  };
  normalizedRow.is_placeholder_row = !footerRowHasContent(normalizedRow, columns);
  return normalizedRow;
}

function normalizeReadonlyFooterBlock(block, index) {
  return {
    id: String(block?.id || `footer_${index}`),
    type: "readonly_grid",
    title: String(block?.title || "底部交接信息"),
    rows: Array.isArray(block?.rows)
      ? block.rows.map((row, rowIndex) => ({
          row_key: String(row?.row_key || `footer_row_${rowIndex}`),
          cells: Array.isArray(row?.cells)
            ? row.cells.map((cell, cellIndex) => ({
                column: String(cell?.column || ""),
                value: String(cell?.value ?? ""),
                colspan: Math.max(1, Number.parseInt(cell?.colspan ?? 1, 10) || 1),
                cell_key: `${row?.row_key || rowIndex}:${cell?.column || cellIndex}`,
              }))
            : [],
        }))
      : [],
  };
}

function normalizeFooterBlock(block, index) {
  const type = String(block?.type || "readonly_grid").trim();
  if (type === "inventory_table") {
    const columns = Array.isArray(block?.columns) && block.columns.length
      ? block.columns.map((column, columnIndex) => normalizeFooterInventoryColumn(column, columnIndex))
      : DEFAULT_FOOTER_INVENTORY_COLUMNS.map((column, columnIndex) => normalizeFooterInventoryColumn(column, columnIndex));
    const rows = Array.isArray(block?.rows)
      ? block.rows.map((row) => normalizeFooterInventoryRow(row, columns))
      : [];
    return {
      id: String(block?.id || `footer_inventory_${index}`),
      type: "inventory_table",
      title: String(block?.title || "交接确认"),
      group_title: String(block?.group_title || "工具及物品交接清点"),
      columns,
      rows: rows.length ? rows : [blankFooterInventoryRow(columns)],
    };
  }
  return normalizeReadonlyFooterBlock(block, index);
}

function normalizeDocument(document) {
  const fixedBlocks = Array.isArray(document?.fixed_blocks)
    ? document.fixed_blocks.map(normalizeFixedBlock)
    : [];
  const sections = Array.isArray(document?.sections)
    ? document.sections.map(normalizeSection)
    : [];
  const footerBlocks = Array.isArray(document?.footer_blocks)
    ? document.footer_blocks.map((block, index) => normalizeFooterBlock(block, index))
    : [];
  return {
    title: String(document?.title ?? ""),
    fixed_blocks: fixedBlocks,
    sections,
    footer_blocks: footerBlocks,
  };
}

function emptyDirtyRegions() {
  return {
    fixed_blocks: false,
    sections: false,
    footer_inventory: false,
  };
}

function cloneDirtyRegions(dirtyRegions) {
  return {
    fixed_blocks: Boolean(dirtyRegions?.fixed_blocks),
    sections: Boolean(dirtyRegions?.sections),
    footer_inventory: Boolean(dirtyRegions?.footer_inventory),
  };
}

function normalizeCapacitySync(raw) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const normalizedStatus = ["ready", "pending_input", "missing_file", "failed"].includes(status)
    ? status
    : "failed";
  const trackedCells = Array.isArray(raw?.tracked_cells) && raw.tracked_cells.length
    ? raw.tracked_cells
    : CAPACITY_SYNC_TRACKED_CELLS;
  return {
    status: normalizedStatus,
    updated_at: String(raw?.updated_at || "").trim(),
    error: String(raw?.error || "").trim(),
    tracked_cells: trackedCells.map((item) => String(item || "").trim().toUpperCase()).filter(Boolean),
    input_signature: String(raw?.input_signature || "").trim(),
  };
}

function hasInventoryFooterBlock(document) {
  const footerBlocks = Array.isArray(document?.footer_blocks) ? document.footer_blocks : [];
  return footerBlocks.some((block) => String(block?.type || "").trim() === "inventory_table");
}

function mergeInventoryFooterBlock(currentDocument, nextDocument) {
  if (!hasInventoryFooterBlock(currentDocument) || hasInventoryFooterBlock(nextDocument)) {
    return nextDocument;
  }
  const currentFooterBlocks = Array.isArray(currentDocument?.footer_blocks) ? currentDocument.footer_blocks : [];
  const nextFooterBlocks = Array.isArray(nextDocument?.footer_blocks) ? nextDocument.footer_blocks : [];
  const inventoryBlock = currentFooterBlocks.find((block) => String(block?.type || "").trim() === "inventory_table");
  if (!inventoryBlock) {
    return nextDocument;
  }
  return {
    ...nextDocument,
    footer_blocks: [cloneDeep(inventoryBlock), ...nextFooterBlocks],
  };
}

export function isHandoverReviewPath(pathname = window.location.pathname) {
  return REVIEW_PATH_RE.test(String(pathname || "").trim());
}

function resolveReviewBuildingCode(pathname = window.location.pathname) {
  const match = String(pathname || "").trim().match(REVIEW_PATH_RE);
  return match ? String(match[1] || "").toLowerCase() : "";
}

function resolveReviewSelection(search = window.location.search) {
  const params = new URLSearchParams(String(search || ""));
  const sessionId = String(params.get("session_id") || "").trim();
  const dutyDate = String(params.get("duty_date") || "").trim();
  const dutyShift = String(params.get("duty_shift") || "").trim().toLowerCase();
  if (sessionId) {
    return { sessionId, dutyDate: "", dutyShift: "" };
  }
  if (dutyDate && ["day", "night"].includes(dutyShift)) {
    return { sessionId: "", dutyDate, dutyShift };
  }
  return { sessionId: "", dutyDate: "", dutyShift: "" };
}

function normalizeHistoryPayload(raw, fallbackSession = null) {
  const sessionItems = Array.isArray(raw?.sessions)
    ? raw.sessions
        .filter((item) => item && typeof item === "object" && String(item.session_id || "").trim())
        .map((item) => ({
          session_id: String(item.session_id || "").trim(),
          building: String(item.building || "").trim(),
          duty_date: String(item.duty_date || "").trim(),
          duty_shift: String(item.duty_shift || "").trim().toLowerCase(),
          revision: Number(item.revision || 0),
          confirmed: Boolean(item.confirmed),
          updated_at: String(item.updated_at || "").trim(),
          output_file: String(item.output_file || "").trim(),
          has_output_file: Boolean(item.has_output_file),
          is_latest: Boolean(item.is_latest),
          label: String(item.label || "").trim(),
        }))
    : [];
  const fallbackSessionId = String(fallbackSession?.session_id || "").trim();
  const latestSessionId = String(raw?.latest_session_id || "").trim();
  const selectedSessionId = String(raw?.selected_session_id || fallbackSessionId).trim();
  const selectedIsLatest =
    typeof raw?.selected_is_latest === "boolean"
      ? raw.selected_is_latest
      : Boolean(selectedSessionId && latestSessionId && selectedSessionId === latestSessionId);
  const selectedInHistoryList =
    typeof raw?.selected_in_history_list === "boolean"
      ? raw.selected_in_history_list
      : sessionItems.some((item) => item.session_id === selectedSessionId);
  const selectedHistoryExcludedReason = String(raw?.selected_history_excluded_reason || "").trim();
  const historyLimit = Math.max(0, Number.parseInt(raw?.history_limit ?? 10, 10) || 10);
  const historyRule = String(raw?.history_rule || "").trim() || "cloud_success_only";
  return {
    latest_session_id: latestSessionId,
    selected_session_id: selectedSessionId,
    selected_is_latest: selectedIsLatest,
    selected_in_history_list: selectedInHistoryList,
    selected_history_excluded_reason: selectedHistoryExcludedReason,
    history_limit: historyLimit,
    history_rule: historyRule,
    sessions: sessionItems,
  };
}

function emptyConcurrencyState(revision = 0) {
  return {
    current_revision: Number.parseInt(String(revision || 0), 10) || 0,
    active_editor: null,
    lease_expires_at: "",
    is_editing_elsewhere: false,
    client_holds_lock: false,
  };
}

function normalizeConcurrencyPayload(raw, fallbackRevision = 0) {
  const activeEditor = raw?.active_editor && typeof raw.active_editor === "object"
    ? {
        holder_label: String(raw.active_editor.holder_label || "").trim(),
        claimed_at: String(raw.active_editor.claimed_at || "").trim(),
        last_heartbeat_at: String(raw.active_editor.last_heartbeat_at || "").trim(),
      }
    : null;
  return {
    current_revision: Number.parseInt(String(raw?.current_revision ?? fallbackRevision ?? 0), 10) || 0,
    active_editor: activeEditor && activeEditor.holder_label ? activeEditor : null,
    lease_expires_at: String(raw?.lease_expires_at || "").trim(),
    is_editing_elsewhere: Boolean(raw?.is_editing_elsewhere),
    client_holds_lock: Boolean(raw?.client_holds_lock),
  };
}

export function mountHandoverReviewApp(Vue) {
  const { createApp, ref, computed, onMounted, onBeforeUnmount, watch } = Vue;

  createApp({
    setup() {
      const buildingCode = resolveReviewBuildingCode();
      const initialSelection = resolveReviewSelection();
      const activeRouteSelection = ref({
        sessionId: String(initialSelection.sessionId || "").trim(),
        dutyDate: String(initialSelection.dutyDate || "").trim(),
        dutyShift: String(initialSelection.dutyShift || "").trim().toLowerCase(),
      });
      const loading = ref(true);
      const saving = ref(false);
      const downloading = ref(false);
      const capacityDownloading = ref(false);
      const confirming = ref(false);
      const retryingCloudSync = ref(false);
      const updatingHistoryCloudSync = ref(false);
      const dirty = ref(false);
      const dirtyRegions = ref(emptyDirtyRegions());
      const capacityLinkedDirty = ref(false);
      const needsRefresh = ref(false);
      const errorText = ref("");
      const statusText = ref("");
      const building = ref("");
      const session = ref(null);
      const historyState = ref(normalizeHistoryPayload({}, null));
      const documentRef = ref(normalizeDocument({}));
      const batchStatus = ref({
        batch_key: "",
        confirmed_count: 0,
        required_count: 5,
        all_confirmed: false,
        ready_for_followup_upload: false,
        buildings: [],
      });
      const suspendAutoSave = ref(true);
      const autosaveTimer = ref(null);
      const saveFailureRetryTimer = ref(null);
      const pendingFailureRetryCount = ref(0);
      const pollTimer = ref(null);
      const heartbeatTimer = ref(null);
      const lastSavedSnapshot = ref("");
      const pollIntervalMs = ref(DEFAULT_POLL_INTERVAL_MS);
      const reviewClientIdentity = ensureReviewClientIdentity();
      const reviewClientId = String(reviewClientIdentity.clientId || "").trim();
      const reviewHolderLabel = String(reviewClientIdentity.holderLabel || "").trim();
      const concurrency = ref(emptyConcurrencyState(0));
      const staleRevisionConflict = ref(false);
      const syncingRemoteRevision = ref(false);
      const heldLockSessionId = ref("");
      let latestLoadRequestSeq = 0;
      let activeLoadController = null;

      const cloudSyncBusy = computed(() => retryingCloudSync.value || updatingHistoryCloudSync.value);
      const capacitySync = computed(() => normalizeCapacitySync(session.value?.capacity_sync || {}));
      const capacityTrackedCellSet = computed(() => new Set(capacitySync.value.tracked_cells || CAPACITY_SYNC_TRACKED_CELLS));
      const selectedSessionId = computed(() => String(historyState.value?.selected_session_id || session.value?.session_id || "").trim());
      const latestSessionId = computed(() => String(historyState.value?.latest_session_id || "").trim());
      const isHistoryMode = computed(() => Boolean(session.value) && !Boolean(historyState.value?.selected_is_latest));
      const historySessions = computed(() => Array.isArray(historyState.value?.sessions) ? historyState.value.sessions : []);
      const selectedSessionInHistoryList = computed(() => Boolean(historyState.value?.selected_in_history_list));
      const selectedSessionIdInListOrEmpty = computed(() => (selectedSessionInHistoryList.value ? selectedSessionId.value : ""));
      const historySelectorHint = computed(() => {
        const limit = Math.max(1, Number.parseInt(historyState.value?.history_limit ?? 10, 10) || 10);
        const rows = [`仅显示最近 ${limit} 条已成功上云的交接班日志。`];
        if (session.value && !selectedSessionInHistoryList.value) {
          const excludedReason = String(historyState.value?.selected_history_excluded_reason || "").trim();
          if (excludedReason === "outside_limit") {
            rows.push(`当前查看记录已成功上云，但不在最近 ${limit} 条历史范围内。`);
          } else if (excludedReason === "not_cloud_success") {
            rows.push("当前查看记录尚未成功上云，因此不在历史列表中。");
          }
        }
        return rows.join(" ");
      });

      const sessionSummary = computed(() => {
        if (!session.value) return "暂无会话";
        const dutyDate = session.value.duty_date || "-";
        return `${dutyDate} / ${shiftTextFromCode(session.value.duty_shift || "")}`;
      });

      const saveStatusText = computed(() => {
        if (saving.value) return statusText.value || "正在保存...";
        if (dirty.value) return "待自动保存（空闲后保存）";
        return statusText.value || "已同步";
      });
      const reviewFileSummary = computed(() => basenameFromPath(session.value?.output_file || ""));
      const currentDutyDateText = computed(() => String(session.value?.duty_date || "").trim() || "-");
      const currentDutyShiftText = computed(() => shiftTextFromCode(session.value?.duty_shift || ""));
      const currentModeText = computed(() => (isHistoryMode.value ? "历史" : "最新"));
      const canReturnToLatest = computed(() => Boolean(session.value && latestSessionId.value && selectedSessionId.value && selectedSessionId.value !== latestSessionId.value));
      const canUpdateHistoryCloudSync = computed(() => Boolean(
        isHistoryMode.value
        && session.value
        && session.value.session_id
        && !loading.value
        && !saving.value
        && !confirming.value
        && !cloudSyncBusy.value
        && !needsRefresh.value
      ));
      const activeEditorLabel = computed(() => String(concurrency.value?.active_editor?.holder_label || "").trim());
      const remoteRevision = computed(() => Number.parseInt(String(concurrency.value?.current_revision || 0), 10) || 0);

      const reviewSaveBadge = computed(() => {
        if (errorText.value) return badgeVm("保存异常", "danger", "soft", "error");
        if (needsRefresh.value) return badgeVm("等待同步", "warning", "soft", "warn");
        if (saving.value) return badgeVm(statusText.value || "正在保存...", "info", "soft", "clock");
        if (dirty.value) return badgeVm("待保存", "warning", "soft", "warn");
        return badgeVm(statusText.value || "已自动保存", "success", "soft", "check");
      });
      const reviewConfirmBadge = computed(() =>
        session.value?.confirmed
          ? badgeVm("已确认", "success", "solid", "check")
          : badgeVm("待确认", "warning", "soft", "warn"),
      );

      const reviewCloudSheetVm = computed(() => mapReviewCloudSheetSync(session.value?.cloud_sheet_sync || {}));
      const reviewCloudSheetUrl = computed(() => String(reviewCloudSheetVm.value.url || "").trim());
      const canRetryCloudSync = computed(() => {
        const status = String(session.value?.cloud_sheet_sync?.status || "").trim().toLowerCase();
        return Boolean(
          session.value
          && !isHistoryMode.value
          && session.value.confirmed
          && batchStatus.value?.all_confirmed
          && ["failed", "prepare_failed"].includes(status),
        );
      });

      const reviewHeaderBadges = computed(() => {
        const badges = [
          badgeVm(
            isHistoryMode.value ? "历史记录" : "当前记录",
            isHistoryMode.value ? "warning" : "info",
            "outline",
            "clock",
          ),
          reviewSaveBadge.value,
          isHistoryMode.value ? badgeVm("可编辑", "neutral", "outline", "file") : reviewConfirmBadge.value,
        ];
        if (concurrency.value?.client_holds_lock) {
          badges.push(badgeVm("本端编辑中", "info", "outline", "warn"));
        } else if (activeEditorLabel.value) {
          badges.push(badgeVm("其他终端编辑中", "warning", "outline", "warn"));
        } else if (["warning", "danger"].includes(String(reviewCloudSheetVm.value.tone || ""))) {
          badges.push(badgeVm(reviewCloudSheetVm.value.text, reviewCloudSheetVm.value.tone, "outline", "link"));
        }
        return badges;
      });
      const capacityDownloadDisabled = computed(() => Boolean(
        syncingRemoteRevision.value
        || capacityDownloading.value
        || !session.value
        || !session.value.session_id
        || !String(session.value.capacity_output_file || "").trim()
        || String(capacitySync.value.status || "").trim().toLowerCase() !== "ready"
        || capacityLinkedDirty.value
      ));

      const reviewStatusBanners = computed(() => {
        const rows = [];
        if (isHistoryMode.value) {
          rows.push({
            text: "当前为历史模式：只更新当前历史记录，不改模板默认值；如需同步云文档，请手动点击“更新云文档”。",
            tone: "info",
          });
        }
        if (syncingRemoteRevision.value) {
          rows.push({
            text: "其他用户正在保存，请稍等，系统将自动刷新最新内容。",
            tone: "warning",
          });
        }
        if (staleRevisionConflict.value) {
          rows.push({
            text: "内容已有更新，请稍等，系统将自动同步最新内容。",
            tone: "warning",
          });
        }
        if (concurrency.value?.is_editing_elsewhere && activeEditorLabel.value) {
          rows.push({
            text: `其他用户正在编辑：${activeEditorLabel.value}。如需保存或确认，请稍等。`,
            tone: "warning",
          });
        }
        if (statusText.value && !saving.value && !dirty.value && !errorText.value && !staleRevisionConflict.value) {
          rows.push({ text: statusText.value, tone: "info" });
        }
        if (needsRefresh.value) {
          rows.push({
            text: "检测到新版本，请刷新后查看。",
            tone: "warning",
          });
        }
        if (errorText.value) {
          rows.push({ text: errorText.value, tone: "danger" });
        }
        if (reviewCloudSheetVm.value.error) {
          rows.push({ text: `云表同步失败: ${reviewCloudSheetVm.value.error}`, tone: "danger" });
        }
        return rows;
      });

      const confirmActionVm = computed(() => {
        const disabled = !session.value || saving.value || dirty.value || confirming.value || cloudSyncBusy.value || needsRefresh.value || staleRevisionConflict.value || syncingRemoteRevision.value;
        if (!session.value) {
          return { text: "暂无记录", variant: "secondary", disabled: true };
        }
        if (confirming.value) {
          return {
            text: "处理中...",
            variant: session.value?.confirmed ? "success" : "warning",
            disabled: true,
          };
        }
        if (session.value?.confirmed) {
          return { text: "已确认（可取消）", variant: "success", disabled };
        }
        return { text: "确认当前楼栋", variant: "warning", disabled };
      });

      function serializeDocument(document) {
        return JSON.stringify(document || {});
      }

      function clearAutosaveTimer() {
        if (autosaveTimer.value) {
          window.clearTimeout(autosaveTimer.value);
          autosaveTimer.value = null;
        }
      }

      function clearSaveFailureRetryTimer() {
        if (saveFailureRetryTimer.value) {
          window.clearTimeout(saveFailureRetryTimer.value);
          saveFailureRetryTimer.value = null;
        }
      }

      function clearSaveTimers() {
        clearAutosaveTimer();
        clearSaveFailureRetryTimer();
      }

      function clearHeartbeatTimer() {
        if (heartbeatTimer.value) {
          window.clearInterval(heartbeatTimer.value);
          heartbeatTimer.value = null;
        }
      }

      function applyConcurrencyState(raw, fallbackRevision = 0, sessionId = "") {
        const normalized = normalizeConcurrencyPayload(raw, fallbackRevision);
        const resolvedSessionId = String(sessionId || session.value?.session_id || "").trim();
        concurrency.value = normalized;
        if (normalized.client_holds_lock && resolvedSessionId) {
          heldLockSessionId.value = resolvedSessionId;
          restartHeartbeat();
          return;
        }
        heldLockSessionId.value = "";
        clearHeartbeatTimer();
      }

      function buildLockPayload(sessionId = "") {
        return {
          session_id: String(sessionId || session.value?.session_id || "").trim(),
          client_id: reviewClientId,
          holder_label: reviewHolderLabel,
        };
      }

      async function ensureEditingLock() {
        const sessionId = String(session.value?.session_id || "").trim();
        if (!buildingCode || !sessionId || !reviewClientId) return false;
        if (concurrency.value?.client_holds_lock && heldLockSessionId.value === sessionId) {
          restartHeartbeat();
          return true;
        }
        try {
          const response = await claimHandoverReviewLockApi(buildingCode, buildLockPayload(sessionId));
          applyConcurrencyState(response?.concurrency, session.value?.revision || 0, sessionId);
          return Boolean(concurrency.value?.client_holds_lock);
        } catch (_error) {
          return false;
        }
      }

      async function sendLockHeartbeat() {
        const sessionId = String(heldLockSessionId.value || session.value?.session_id || "").trim();
        if (!buildingCode || !sessionId || !reviewClientId) return;
        try {
          const response = await heartbeatHandoverReviewLockApi(buildingCode, {
            session_id: sessionId,
            client_id: reviewClientId,
          });
          applyConcurrencyState(response?.concurrency, session.value?.revision || 0, sessionId);
        } catch (_error) {
          clearHeartbeatTimer();
        }
      }

      function restartHeartbeat() {
        clearHeartbeatTimer();
        const sessionId = String(heldLockSessionId.value || "").trim();
        if (!sessionId || !concurrency.value?.client_holds_lock) return;
        heartbeatTimer.value = window.setInterval(() => {
          void sendLockHeartbeat();
        }, REVIEW_LOCK_HEARTBEAT_MS);
      }

      async function releaseCurrentLock({ keepalive = false } = {}) {
        const sessionId = String(heldLockSessionId.value || session.value?.session_id || "").trim();
        clearHeartbeatTimer();
        heldLockSessionId.value = "";
        if (!buildingCode || !sessionId || !reviewClientId) {
          applyConcurrencyState(null, session.value?.revision || 0, "");
          return;
        }
        const body = JSON.stringify({
          session_id: sessionId,
          client_id: reviewClientId,
        });
        if (keepalive && typeof window !== "undefined" && typeof window.fetch === "function") {
          try {
            void window.fetch(`/api/handover/review/${buildingCode}/lock/release`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body,
              keepalive: true,
            }).catch(() => {});
          } catch (_error) {
            // Ignore best-effort release failures during unload.
          }
          applyConcurrencyState(null, session.value?.revision || 0, "");
          return;
        }
        try {
          const response = await releaseHandoverReviewLockApi(buildingCode, {
            session_id: sessionId,
            client_id: reviewClientId,
          });
          applyConcurrencyState(response?.concurrency, session.value?.revision || 0, "");
        } catch (_error) {
          applyConcurrencyState(null, session.value?.revision || 0, "");
        }
      }

      function markRevisionConflict(message = "") {
        beginRemoteSaveRefresh(message || "其他用户正在保存，请稍等，系统将自动刷新最新内容。");
      }

      function isRevisionConflictError(error) {
        return Number.parseInt(String(error?.httpStatus || 0), 10) === 409;
      }

      function beginRemoteSaveRefresh(message = "其他用户正在保存，请稍等，系统将自动刷新最新内容。") {
        syncingRemoteRevision.value = true;
        staleRevisionConflict.value = false;
        needsRefresh.value = false;
        clearSaveTimers();
        errorText.value = "";
        statusText.value = message;
      }

      function touchEditingIntent() {
        if (!session.value) return;
        void ensureEditingLock();
      }

      function handleWindowBeforeUnload() {
        void releaseCurrentLock({ keepalive: true });
      }

      function buildLoadParams() {
        const params = {
          client_id: reviewClientId,
        };
        if (activeRouteSelection.value.sessionId) {
          params.session_id = activeRouteSelection.value.sessionId;
          return params;
        }
        if (activeRouteSelection.value.dutyDate && activeRouteSelection.value.dutyShift) {
          params.duty_date = activeRouteSelection.value.dutyDate;
          params.duty_shift = activeRouteSelection.value.dutyShift;
        }
        return params;
      }

      function syncRouteToCurrentSelection(nextHistory = historyState.value) {
        const selectedId = String(nextHistory?.selected_session_id || session.value?.session_id || "").trim();
        syncReviewSelectionToUrl({
          sessionId: selectedId,
          isLatest: Boolean(nextHistory?.selected_is_latest),
        });
      }

      function applyPayloadMeta(payload = {}) {
        const nextSession = payload?.session && typeof payload.session === "object" ? cloneDeep(payload.session) : null;
        if (nextSession) {
          session.value = nextSession;
        }
        batchStatus.value = payload?.batch_status && typeof payload.batch_status === "object"
          ? cloneDeep(payload.batch_status)
          : batchStatus.value;
        historyState.value = normalizeHistoryPayload(payload?.history || {}, nextSession || session.value);
        const selectedId = String(historyState.value?.selected_session_id || nextSession?.session_id || session.value?.session_id || "").trim();
        activeRouteSelection.value = {
          sessionId: historyState.value?.selected_is_latest ? "" : selectedId,
          dutyDate: "",
          dutyShift: "",
        };
        syncRouteToCurrentSelection(historyState.value);
        applyConcurrencyState(
          payload?.concurrency,
          nextSession?.revision ?? session.value?.revision ?? 0,
          nextSession?.session_id || session.value?.session_id || "",
        );
      }

      function isIncompleteJobStatus(status) {
        const normalized = String(status || "").trim().toLowerCase();
        return normalized === "queued" || normalized === "running" || normalized === "waiting_resource";
      }

      async function waitForBackgroundJob(jobId, { timeoutMs = 120000, intervalMs = 1500 } = {}) {
        const targetJobId = String(jobId || "").trim();
        if (!targetJobId) return null;
        const startedAt = Date.now();
        while (Date.now() - startedAt <= timeoutMs) {
          try {
            const job = await getJobApi(targetJobId);
            if (!isIncompleteJobStatus(job?.status)) {
              return job;
            }
          } catch (_error) {
            // Ignore transient polling failures and keep waiting.
          }
          await new Promise((resolve) => window.setTimeout(resolve, intervalMs));
        }
        return null;
      }

      function restartPollTimer() {
        if (pollTimer.value) {
          window.clearInterval(pollTimer.value);
          pollTimer.value = null;
        }
        pollTimer.value = window.setInterval(() => {
          loadReviewData({ background: true });
        }, pollIntervalMs.value);
      }

      function hydrateFromPayload(payload, { fromBackground = false } = {}) {
        const nextSession = payload?.session && typeof payload.session === "object" ? cloneDeep(payload.session) : null;
        const rawNextDocument = normalizeDocument(payload?.document || {});
        const nextDocument = fromBackground ? mergeInventoryFooterBlock(documentRef.value, rawNextDocument) : rawNextDocument;

        suspendAutoSave.value = true;
        building.value = String(payload?.building || nextSession?.building || "");
        documentRef.value = nextDocument;
        applyPayloadMeta(payload);
        dirtyRegions.value = emptyDirtyRegions();
        dirty.value = false;
        capacityLinkedDirty.value = false;
        staleRevisionConflict.value = false;
        errorText.value = "";
        if (!fromBackground) {
          needsRefresh.value = false;
          statusText.value = "";
        }
        lastSavedSnapshot.value = serializeDocument(nextDocument);
        syncingRemoteRevision.value = false;
        window.setTimeout(() => {
          suspendAutoSave.value = false;
        }, 0);
      }

      async function loadReviewData({ background = false } = {}) {
        if (!buildingCode) {
          loading.value = false;
          errorText.value = "无效的楼栋审核页面地址";
          return;
        }
        if (background && (dirty.value || saving.value || loading.value || confirming.value || cloudSyncBusy.value || syncingRemoteRevision.value || downloading.value || capacityDownloading.value)) {
          return;
        }
        const requestSeq = ++latestLoadRequestSeq;
        if (activeLoadController && typeof activeLoadController.abort === "function") {
          activeLoadController.abort();
        }
        activeLoadController = typeof AbortController === "function" ? new AbortController() : null;
        try {
          if (!background) loading.value = true;
          const payload = await getHandoverReviewApi(
            buildingCode,
            buildLoadParams(),
            activeLoadController
              ? { signal: activeLoadController.signal, retryTransientNetworkErrors: false }
              : {},
          );
          if (requestSeq !== latestLoadRequestSeq) {
            return;
          }
          const reviewUi = payload?.review_ui && typeof payload.review_ui === "object" ? payload.review_ui : {};

          pollIntervalMs.value = Math.max(
            1000,
            Number(reviewUi.poll_interval_sec || DEFAULT_POLL_INTERVAL_MS / 1000) * 1000,
          );
          restartPollTimer();
          if (!background || !session.value) {
            hydrateFromPayload(payload, { fromBackground: background });
            return;
          }

          const incomingSession = payload?.session && typeof payload.session === "object" ? payload.session : {};
          const currentSessionId = String(session.value?.session_id || "").trim();
          const incomingSessionId = String(incomingSession.session_id || "").trim();
          const incomingRevision = Number(incomingSession.revision || 0);
          const currentRevision = Number(session.value?.revision || 0);

          batchStatus.value = payload?.batch_status && typeof payload.batch_status === "object"
            ? cloneDeep(payload.batch_status)
            : batchStatus.value;
          historyState.value = normalizeHistoryPayload(payload?.history || {}, incomingSession || session.value);
          syncRouteToCurrentSelection(historyState.value);
          applyConcurrencyState(payload?.concurrency, incomingRevision || currentRevision, incomingSessionId || currentSessionId);

          if (incomingSessionId && currentSessionId && incomingSessionId !== currentSessionId) {
            applyPayloadMeta(payload);
            needsRefresh.value = true;
            statusText.value = "检测到新版本，请刷新后查看。";
            return;
          }

          if (incomingRevision !== currentRevision) {
            if (saving.value) {
              applyPayloadMeta(payload);
              return;
            }
            if (dirty.value) {
              clearSaveTimers();
              beginRemoteSaveRefresh();
              applyPayloadMeta(payload);
              return;
            }
            hydrateFromPayload(payload, { fromBackground: true });
            statusText.value = "已同步最新审核内容";
            return;
          }

          if (dirty.value) {
            applyPayloadMeta(payload);
            return;
          }

          if (!dirty.value && !saving.value) {
            session.value = {
              ...(session.value || {}),
              ...(incomingSession || {}),
            };
            staleRevisionConflict.value = false;
          }
        } catch (error) {
          if (error?.name === "AbortError") {
            return;
          }
          if (!background) {
            errorText.value = String(error?.message || error || "加载失败");
          }
        } finally {
          if (requestSeq === latestLoadRequestSeq) {
            activeLoadController = null;
          }
          if (!background) loading.value = false;
        }
      }

      function scheduleSaveRetryAfterFailure() {
        if (pendingFailureRetryCount.value >= REVIEW_SAVE_MAX_IDLE_RETRY_AFTER_FAILURE) {
          statusText.value = "保存失败，请稍后重试。";
          return;
        }
        clearSaveFailureRetryTimer();
        pendingFailureRetryCount.value += 1;
        statusText.value = "保存失败，30 秒后自动重试。";
        saveFailureRetryTimer.value = window.setTimeout(() => {
          saveFailureRetryTimer.value = null;
          saveDocument({ reason: "retry" });
        }, REVIEW_SAVE_FAILURE_RETRY_DELAY_MS);
      }

      function scheduleAutosave() {
        clearSaveTimers();
        autosaveTimer.value = window.setTimeout(() => {
          autosaveTimer.value = null;
          saveDocument({ reason: "autosave" });
        }, REVIEW_IDLE_AUTOSAVE_DELAY_MS);
      }

      async function saveDocument(options = {}) {
        const { reason = "autosave" } = options || {};
        if (saving.value || confirming.value || cloudSyncBusy.value || suspendAutoSave.value || syncingRemoteRevision.value || !session.value) return false;
        if (staleRevisionConflict.value) {
          beginRemoteSaveRefresh();
          return false;
        }
        const payloadSnapshot = serializeDocument(documentRef.value);
        if (payloadSnapshot === lastSavedSnapshot.value) {
          clearSaveTimers();
          pendingFailureRetryCount.value = 0;
          dirty.value = false;
          return true;
        }
        clearSaveTimers();
        saving.value = true;
        errorText.value = "";
        await ensureEditingLock();
        const payloadDirtyRegions = cloneDirtyRegions(dirtyRegions.value);
        if (reason === "confirm") {
          statusText.value = "正在保存...";
        } else if (reason === "retry") {
          statusText.value = "正在重新保存...";
        } else if (reason === "switch") {
          statusText.value = "正在保存并切换...";
        } else if (reason === "cloud_update") {
          statusText.value = "正在保存并更新云文档...";
        } else {
          statusText.value = "正在自动保存...";
        }
        try {
          const response = await saveHandoverReviewApi(buildingCode, {
            session_id: session.value.session_id,
            base_revision: session.value.revision,
            client_id: reviewClientId,
            document: cloneDeep(documentRef.value),
            dirty_regions: payloadDirtyRegions,
          });
          applyPayloadMeta(response || {});
          broadcastHandoverReviewStatusChange(response || {});
          lastSavedSnapshot.value = payloadSnapshot;
          if (serializeDocument(documentRef.value) === payloadSnapshot) {
            dirtyRegions.value = emptyDirtyRegions();
            capacityLinkedDirty.value = false;
          }
          dirty.value = serializeDocument(documentRef.value) !== lastSavedSnapshot.value;
          pendingFailureRetryCount.value = 0;
          clearSaveFailureRetryTimer();
          staleRevisionConflict.value = false;
          needsRefresh.value = false;
          statusText.value = isHistoryMode.value ? "历史交接班日志已保存" : "已自动保存";
          if (dirty.value) {
            scheduleAutosave();
          }
          return true;
        } catch (error) {
          if (isRevisionConflictError(error)) {
            beginRemoteSaveRefresh();
            await loadReviewData({ background: false });
            statusText.value = "已同步最新审核内容";
            return false;
          }
          errorText.value = String(error?.message || error || "保存失败");
          if (reason === "autosave") {
            scheduleSaveRetryAfterFailure();
          } else {
            statusText.value = "保存失败，请处理后重试。";
          }
          return false;
        } finally {
          saving.value = false;
        }
      }

      async function switchToSession(sessionId, { toLatest = false } = {}) {
        const nextSessionId = String(sessionId || "").trim();
        if (!toLatest && (!nextSessionId || nextSessionId === selectedSessionId.value)) return;
        if (dirty.value) {
          const saved = await saveDocument({ reason: "switch" });
          if (!saved) return;
        }
        await releaseCurrentLock();
        clearSaveTimers();
        needsRefresh.value = false;
        staleRevisionConflict.value = false;
        errorText.value = "";
        activeRouteSelection.value = {
          sessionId: toLatest ? "" : nextSessionId,
          dutyDate: "",
          dutyShift: "",
        };
        syncReviewSelectionToUrl({ sessionId: toLatest ? "" : nextSessionId, isLatest: toLatest });
        statusText.value = toLatest ? "正在切换到最新交接班日志..." : "正在切换历史交接班日志...";
        await loadReviewData({ background: false });
      }

      async function onHistorySelectionChange(nextSessionId) {
        const targetSessionId = String(nextSessionId || "").trim();
        if (!targetSessionId || targetSessionId === selectedSessionId.value) return;
        if (latestSessionId.value && targetSessionId === latestSessionId.value) {
          await switchToSession(latestSessionId.value, { toLatest: true });
          return;
        }
        await switchToSession(targetSessionId, { toLatest: false });
      }

      async function returnToLatestSession() {
        if (!canReturnToLatest.value) return;
        await switchToSession(latestSessionId.value, { toLatest: true });
      }

      async function toggleConfirm() {
        if (
          isHistoryMode.value ||
          !session.value ||
          saving.value ||
          confirming.value ||
          cloudSyncBusy.value ||
          syncingRemoteRevision.value ||
          needsRefresh.value ||
          staleRevisionConflict.value
        ) return;
        if (dirty.value) {
          statusText.value = "请先等待当前修改保存成功后再确认。";
          return;
        }
        confirming.value = true;
        errorText.value = "";
        try {
          const request = {
            session_id: session.value.session_id,
            base_revision: session.value.revision,
            client_id: reviewClientId,
          };
          const response = session.value.confirmed
            ? await unconfirmHandoverReviewApi(buildingCode, request)
            : await confirmHandoverReviewApi(buildingCode, request);
          applyPayloadMeta(response || {});
          broadcastHandoverReviewStatusChange(response || {});
          staleRevisionConflict.value = false;
          needsRefresh.value = false;
          statusText.value = session.value?.confirmed ? "已确认当前楼栋" : "已撤销确认";
        } catch (error) {
          if (isRevisionConflictError(error)) {
            beginRemoteSaveRefresh();
            await loadReviewData({ background: false });
            statusText.value = "已同步最新审核内容";
          } else {
            errorText.value = String(error?.message || error || "确认失败");
          }
        } finally {
          confirming.value = false;
        }
      }

      async function retryCloudSheetSync() {
        if (!buildingCode || !session.value || retryingCloudSync.value || !canRetryCloudSync.value) return;
        retryingCloudSync.value = true;
        errorText.value = "";
        statusText.value = "正在重试云表同步...";
        try {
          const response = await retryHandoverReviewCloudSyncApi(buildingCode, {
            session_id: session.value.session_id,
          });
          const jobId = String(response?.job?.job_id || response?.job_id || "").trim();
          if (!jobId) {
            throw new Error("云表重试任务提交失败");
          }
          statusText.value = "已提交云表同步任务，正在处理中...";
          void (async () => {
            const job = await waitForBackgroundJob(jobId, { timeoutMs: 10 * 60 * 1000 });
            if (!job) return;
            await loadReviewData({ background: true });
            if (job.status === "success") {
              const result = job?.result && typeof job.result === "object" ? job.result : {};
              applyPayloadMeta(result || {});
              broadcastHandoverReviewStatusChange(result || {});
              const retryStatus = String(result.status || "").trim().toLowerCase();
              if (retryStatus === "ok" || retryStatus === "success") {
                statusText.value = "云表上传成功";
                errorText.value = "";
              } else if (retryStatus === "blocked") {
                errorText.value = String(result?.cloud_sheet_sync?.blocked_reason || "") || "当前批次尚未全部确认，不能重试云表上传。";
              } else {
                const failedRows = Array.isArray(result?.cloud_sheet_sync?.failed_buildings)
                  ? result.cloud_sheet_sync.failed_buildings
                  : [];
                const failedItem = failedRows.find((item) => String(item?.building || "").trim() === String(building.value || "").trim());
                errorText.value = String(failedItem?.error || "云表上传失败");
                statusText.value = "云表上传失败";
              }
            } else {
              errorText.value = String(job?.error || "云表重试失败");
              statusText.value = "云表上传失败";
            }
          })();
        } catch (error) {
          errorText.value = String(error?.message || error || "云表重试失败");
          statusText.value = "云表上传失败";
        } finally {
          retryingCloudSync.value = false;
        }
      }

      async function updateHistoryCloudSync() {
        if (!buildingCode || !session.value || !canUpdateHistoryCloudSync.value) return;
        if (dirty.value) {
          const saved = await saveDocument({ reason: "cloud_update" });
          if (!saved) return;
        }
        updatingHistoryCloudSync.value = true;
        errorText.value = "";
        statusText.value = "正在更新历史云文档...";
        try {
          const response = await updateHandoverReviewCloudSyncApi(buildingCode, {
            session_id: session.value.session_id,
          });
          applyPayloadMeta(response || {});
          broadcastHandoverReviewStatusChange(response || {});
          const updateStatus = String(response.status || "").trim().toLowerCase();
          if (updateStatus === "ok" || updateStatus === "success") {
            statusText.value = "历史云文档已更新";
          } else {
            errorText.value = String(response?.cloud_sheet_sync?.failed_buildings?.[0]?.error || response?.status || "历史云文档更新失败");
            statusText.value = "历史云文档更新失败";
          }
        } catch (error) {
          errorText.value = String(error?.message || error || "历史云文档更新失败");
          statusText.value = "历史云文档更新失败";
        } finally {
          updatingHistoryCloudSync.value = false;
        }
      }

      async function downloadCurrentReviewFile() {
        if (saving.value || dirty.value || syncingRemoteRevision.value) {
          statusText.value = "请先等待当前修改保存成功后再下载。";
          return;
        }
        const sessionId = String(session.value?.session_id || "").trim();
        if (!buildingCode || !sessionId) {
          statusText.value = "当前没有可下载的交接班文件";
          return;
        }
        downloading.value = true;
        errorText.value = "";
        try {
          const url = buildHandoverReviewDownloadUrl(buildingCode, sessionId);
          const response = await fetch(url, { method: "GET" });
          if (!response.ok) {
            const payload = await response.json().catch(() => null);
            throw new Error(payload?.detail || `下载失败: HTTP ${response.status}`);
          }
          const blob = await response.blob();
          const objectUrl = window.URL.createObjectURL(blob);
          const anchor = document.createElement("a");
          anchor.href = objectUrl;
          anchor.download =
            basenameFromPath(session.value?.output_file || "") ||
            `${String(building.value || buildingCode || "handover").trim()}.xlsx`;
          document.body.appendChild(anchor);
          anchor.click();
          anchor.remove();
          window.setTimeout(() => window.URL.revokeObjectURL(objectUrl), 0);
          statusText.value = "交接班日志下载已开始";
        } catch (error) {
          const message = String(error?.message || error || "下载失败");
          errorText.value = message;
          if (message.includes("新生成")) {
            needsRefresh.value = true;
          }
        } finally {
          downloading.value = false;
        }
      }

      async function downloadCurrentCapacityReviewFile() {
        if (capacityDownloadDisabled.value) {
          const syncStatus = String(capacitySync.value?.status || "").trim().toLowerCase();
          if (syncingRemoteRevision.value) {
            statusText.value = "请先等待远端修订同步完成后再下载。";
          } else if (capacityLinkedDirty.value) {
            statusText.value = "容量关联字段已修改，请先保存并等待容量报表补写完成。";
          } else if (syncStatus && syncStatus !== "ready") {
            statusText.value = capacitySync.value?.error || "容量报表待补写完成后才能下载。";
          } else {
            statusText.value = "当前没有可下载的交接班容量报表";
          }
          return;
        }
        const sessionId = String(session.value?.session_id || "").trim();
        const capacityOutputFile = String(session.value?.capacity_output_file || "").trim();
        if (!buildingCode || !sessionId || !capacityOutputFile) {
          statusText.value = "当前没有可下载的交接班容量报表";
          return;
        }
        capacityDownloading.value = true;
        errorText.value = "";
        try {
          const url = buildHandoverReviewCapacityDownloadUrl(buildingCode, sessionId);
          const anchor = document.createElement("a");
          anchor.href = `${url}&ts=${Date.now()}`;
          anchor.style.display = "none";
          document.body.appendChild(anchor);
          anchor.click();
          anchor.remove();
          statusText.value = "交接班容量报表下载已开始";
        } catch (error) {
          errorText.value = String(error?.message || error || "下载失败");
        } finally {
          window.setTimeout(() => {
            capacityDownloading.value = false;
          }, 1500);
        }
      }

      function updateFixedField(blockIndex, fieldIndex, value) {
        const block = documentRef.value.fixed_blocks?.[blockIndex];
        const field = block?.fields?.[fieldIndex];
        if (!field) return;
        touchEditingIntent();
        dirtyRegions.value.fixed_blocks = true;
        const cellName = String(field.cell || "").trim().toUpperCase();
        if (capacityTrackedCellSet.value.has(cellName)) {
          capacityLinkedDirty.value = true;
        }
        field.value = String(value ?? "");
      }

      function updateSectionCell(sectionIndex, rowIndex, column, value) {
        const section = documentRef.value.sections?.[sectionIndex];
        const row = section?.rows?.[rowIndex];
        if (!section || !row || !row.cells) return;
        touchEditingIntent();
        dirtyRegions.value.sections = true;
        row.cells[column] = String(value ?? "");
        row.is_placeholder_row = !hasSectionRowContent(row, section.columns);
      }

      function addSectionRow(sectionIndex) {
        const section = documentRef.value.sections?.[sectionIndex];
        if (!section || !Array.isArray(section.rows)) return;
        touchEditingIntent();
        dirtyRegions.value.sections = true;
        section.rows.push(blankRow(section.columns));
      }

      function removeSectionRow(sectionIndex, rowIndex) {
        const section = documentRef.value.sections?.[sectionIndex];
        if (!section || !Array.isArray(section.rows)) return;
        touchEditingIntent();
        dirtyRegions.value.sections = true;
        section.rows.splice(rowIndex, 1);
        if (!section.rows.length) {
          section.rows.push(blankRow(section.columns));
        }
      }

      function updateFooterCell(blockIndex, rowIndex, column, value) {
        const block = documentRef.value.footer_blocks?.[blockIndex];
        if (!block || block.type !== "inventory_table") return;
        const row = block.rows?.[rowIndex];
        if (!row || !row.cells) return;
        touchEditingIntent();
        dirtyRegions.value.footer_inventory = true;
        row.cells[column] = String(value ?? "");
        row.is_placeholder_row = !footerRowHasContent(row, block.columns);
      }

      function addFooterRow(blockIndex) {
        const block = documentRef.value.footer_blocks?.[blockIndex];
        if (!block || block.type !== "inventory_table" || !Array.isArray(block.rows)) return;
        touchEditingIntent();
        dirtyRegions.value.footer_inventory = true;
        block.rows.push(blankFooterInventoryRowWithDefaults(block.columns, resolveFooterAutoFillCells(block)));
      }

      function removeFooterRow(blockIndex, rowIndex) {
        const block = documentRef.value.footer_blocks?.[blockIndex];
        if (!block || block.type !== "inventory_table" || !Array.isArray(block.rows)) return;
        touchEditingIntent();
        dirtyRegions.value.footer_inventory = true;
        if (block.rows.length <= 1) {
          const placeholder = blankFooterInventoryRow(block.columns);
          block.rows[0].cells = placeholder.cells;
          block.rows[0].is_placeholder_row = true;
          return;
        }
        block.rows.splice(rowIndex, 1);
      }

      async function refreshData() {
        clearSaveTimers();
        needsRefresh.value = false;
        await loadReviewData({ background: false });
      }

      watch(
        documentRef,
        () => {
          if (suspendAutoSave.value || !session.value) return;
          const nextSnapshot = serializeDocument(documentRef.value);
          if (nextSnapshot === lastSavedSnapshot.value) return;
          dirty.value = true;
          pendingFailureRetryCount.value = 0;
          clearSaveFailureRetryTimer();
          if (staleRevisionConflict.value) {
            clearSaveTimers();
            beginRemoteSaveRefresh();
            return;
          }
          if (!isHistoryMode.value && session.value?.confirmed) {
            statusText.value = "内容已修改，保存后需重新确认";
          } else if (isHistoryMode.value) {
            statusText.value = "历史记录待保存";
          } else {
            statusText.value = "待保存";
          }
          scheduleAutosave();
        },
        { deep: true },
      );

      onMounted(async () => {
        if (typeof window !== "undefined") {
          window.addEventListener("beforeunload", handleWindowBeforeUnload);
        }
        syncReviewSelectionToUrl({
          sessionId: activeRouteSelection.value.sessionId,
          isLatest: !activeRouteSelection.value.sessionId,
        });
        await loadReviewData({ background: false });
      });

      onBeforeUnmount(() => {
        clearSaveTimers();
        clearHeartbeatTimer();
        if (activeLoadController && typeof activeLoadController.abort === "function") {
          activeLoadController.abort();
          activeLoadController = null;
        }
        if (pollTimer.value) {
          window.clearInterval(pollTimer.value);
          pollTimer.value = null;
        }
        if (typeof window !== "undefined") {
          window.removeEventListener("beforeunload", handleWindowBeforeUnload);
        }
        void releaseCurrentLock();
      });

      return {
        loading,
        saving,
        downloading,
        capacityDownloading,
        confirming,
        retryingCloudSync,
        updatingHistoryCloudSync,
        cloudSyncBusy,
        dirty,
        needsRefresh,
        errorText,
        statusText,
        building,
        session,
        document: documentRef,
        batchStatus,
        historyState,
        historySessions,
        selectedSessionId,
        selectedSessionInHistoryList,
        selectedSessionIdInListOrEmpty,
        historySelectorHint,
        isHistoryMode,
        currentDutyDateText,
        currentDutyShiftText,
        currentModeText,
        canReturnToLatest,
        canUpdateHistoryCloudSync,
        sessionSummary,
        saveStatusText,
        reviewFileSummary,
        reviewSaveBadge,
        reviewConfirmBadge,
        reviewCloudSheetVm,
        reviewCloudSheetUrl,
        capacitySync,
        capacityDownloadDisabled,
        canRetryCloudSync,
        reviewHeaderBadges,
        reviewStatusBanners,
        confirmActionVm,
        syncingRemoteRevision,
        onHistorySelectionChange,
        returnToLatestSession,
        updateHistoryCloudSync,
        updateFixedField,
        updateSectionCell,
        addSectionRow,
        removeSectionRow,
        updateFooterCell,
        addFooterRow,
        removeFooterRow,
        toggleConfirm,
        retryCloudSheetSync,
        downloadCurrentReviewFile,
        downloadCurrentCapacityReviewFile,
        refreshData,
      };
    },
    template: HANDOVER_REVIEW_TEMPLATE,
  }).mount("#app");
}



