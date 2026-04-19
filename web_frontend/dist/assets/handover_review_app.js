import {
  buildHandoverReviewCapacityDownloadUrl,
  buildHandoverReviewDownloadUrl,
  claimHandoverReviewLockApi,
  confirmHandoverReviewApi,
  getJobApi,
  getHandoverReviewApi,
  getHandoverReviewBootstrapApi,
  getHandoverReviewHistoryApi,
  getHandoverReviewStatusApi,
  heartbeatHandoverReviewLockApi,
  releaseHandoverReviewLockApi,
  retryHandoverReviewCloudSyncApi,
  saveHandoverReviewApi,
  updateHandoverReviewCloudSyncApi,
  unconfirmHandoverReviewApi,
} from "./api_client.js";
import { HANDOVER_REVIEW_TEMPLATE } from "./handover_review_template.js";
import { createHandoverReviewDisplayUiHelpers } from "./handover_review_display_ui_helpers.js";
import { createHandoverReviewDocumentEditHelpers } from "./handover_review_document_edit_helpers.js";
import { createHandoverReviewActionHelpers } from "./handover_review_action_helpers.js";

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
const DEFAULT_POLL_INTERVAL_MS = 5000;
const REVIEW_LOCK_HEARTBEAT_MS = 15000;
const REVIEW_CLIENT_ID_STORAGE_KEY = "handover_review_client_id";
const REVIEW_CLIENT_LABEL_STORAGE_KEY = "handover_review_client_label";
const HANDOVER_REVIEW_STATUS_BROADCAST_KEY = "handover_review_status_broadcast_v1";
const CAPACITY_SYNC_TRACKED_CELLS = ["H6", "F8", "B6", "D6", "F6", "D8", "B13", "D13"];

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

function parseDownloadFilename(contentDisposition = "", fallbackName = "") {
  const header = String(contentDisposition || "").trim();
  if (header) {
    const utf8Match = header.match(/filename\*\s*=\s*UTF-8''([^;]+)/i);
    if (utf8Match && utf8Match[1]) {
      try {
        return decodeURIComponent(String(utf8Match[1] || "").trim());
      } catch (_error) {
        // ignore malformed utf8 filename and fall through
      }
    }
    const plainMatch = header.match(/filename\s*=\s*"?([^\";]+)"?/i);
    if (plainMatch && plainMatch[1]) {
      return String(plainMatch[1] || "").trim();
    }
  }
  return basenameFromPath(fallbackName) || "download.xlsx";
}

function triggerBrowserDownload(url, fallbackName = "") {
  const anchor = document.createElement("a");
  anchor.href = String(url || "");
  const downloadName = basenameFromPath(fallbackName);
  if (downloadName) {
    anchor.download = downloadName;
  }
  anchor.rel = "noopener";
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
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

function cloneDeep(value) {
  if (typeof structuredClone === "function") {
    try {
      return structuredClone(value ?? null);
    } catch (_error) {
      // Fall through to JSON clone for plain-document payloads.
    }
  }
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
  const normalizedStatus = ["ready", "pending", "pending_input", "missing_file", "failed"].includes(status)
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

function normalizeDisplayBadge(raw, fallback = {}) {
  return {
    code: String(raw?.code || fallback.code || "").trim(),
    text: String(raw?.text || fallback.text || "").trim(),
    tone: String(raw?.tone || fallback.tone || "neutral").trim() || "neutral",
    emphasis: String(raw?.emphasis || fallback.emphasis || "soft").trim() || "soft",
    icon: String(raw?.icon || fallback.icon || "dot").trim() || "dot",
  };
}

function normalizeDisplayAction(raw, fallback = {}) {
  return {
    allowed: Boolean(raw?.allowed ?? fallback.allowed),
    visible: Boolean(raw?.visible ?? fallback.visible ?? true),
    pending: Boolean(raw?.pending ?? fallback.pending),
    label: String(raw?.label || fallback.label || "").trim(),
    disabledReason: String(raw?.disabled_reason || fallback.disabledReason || "").trim(),
    reasonCode: String(raw?.reason_code || fallback.reasonCode || "").trim(),
    tone: String(raw?.tone || fallback.tone || "neutral").trim() || "neutral",
    variant: String(raw?.variant || fallback.variant || "secondary").trim() || "secondary",
  };
}

function normalizeDisplayItem(raw, fallback = {}) {
  return {
    status: String(raw?.status || fallback.status || "unknown").trim().toLowerCase() || "unknown",
    text: String(raw?.text || fallback.text || "").trim(),
    tone: String(raw?.tone || fallback.tone || "neutral").trim() || "neutral",
    reason_code: String(raw?.reason_code || fallback.reason_code || "").trim(),
    detail_text: String(raw?.detail_text || fallback.detail_text || "").trim(),
  };
}

function normalizeReviewDisplayState(raw = {}) {
  const actions = raw?.actions && typeof raw.actions === "object" ? raw.actions : {};
  const headerBadges = Array.isArray(raw?.header_badges)
    ? raw.header_badges
        .filter((item) => item && typeof item === "object")
        .map((item) => normalizeDisplayBadge(item))
        .filter((item) => item.text)
    : [];
  const statusBanners = Array.isArray(raw?.status_banners)
    ? raw.status_banners
        .filter((item) => item && typeof item === "object")
        .map((item) => ({
          code: String(item.code || "").trim(),
          text: String(item.text || "").trim(),
          tone: String(item.tone || "neutral").trim() || "neutral",
        }))
        .filter((item) => item.text)
    : [];
  const trackedCells = Array.isArray(raw?.capacity_sync?.tracked_cells)
    ? raw.capacity_sync.tracked_cells.map((item) => String(item || "").trim().toUpperCase()).filter(Boolean)
    : CAPACITY_SYNC_TRACKED_CELLS;
  return {
    mode: normalizeDisplayBadge(raw?.mode, {
      code: "unknown",
      text: "",
      tone: "neutral",
      emphasis: "soft",
      icon: "dot",
    }),
    header_badges: headerBadges,
    confirm_badge: normalizeDisplayBadge(raw?.confirm_badge, {
      code: "unknown",
      text: "",
      tone: "neutral",
      emphasis: "soft",
      icon: "dot",
    }),
    lock_state: {
      status: String(raw?.lock_state?.status || "unknown").trim() || "unknown",
      text: String(raw?.lock_state?.text || "").trim(),
      tone: String(raw?.lock_state?.tone || "neutral").trim() || "neutral",
      reason_code: String(raw?.lock_state?.reason_code || "").trim(),
    },
    history_hint: String(raw?.history_hint || "").trim(),
    save_state: normalizeDisplayItem(raw?.save_state, {
      status: "unknown",
      text: "",
      tone: "neutral",
      reason_code: "",
      detail_text: "",
    }),
    download_state: normalizeDisplayItem(raw?.download_state, {
      status: "unknown",
      text: "",
      tone: "neutral",
      reason_code: "",
      detail_text: "",
    }),
    capacity_download_state: normalizeDisplayItem(raw?.capacity_download_state, {
      status: "unknown",
      text: "",
      tone: "neutral",
      reason_code: "",
      detail_text: "",
    }),
    confirm_state: normalizeDisplayItem(raw?.confirm_state, {
      status: "unknown",
      text: "",
      tone: "neutral",
      reason_code: "",
      detail_text: "",
    }),
    cloud_sheet: {
      status: String(raw?.cloud_sheet?.status || "").trim().toLowerCase(),
      text: String(raw?.cloud_sheet?.text || "").trim(),
      tone: String(raw?.cloud_sheet?.tone || "neutral").trim() || "neutral",
      reason_code: String(raw?.cloud_sheet?.reason_code || "").trim(),
      url: String(raw?.cloud_sheet?.url || "").trim(),
      error: String(raw?.cloud_sheet?.error || "").trim(),
    },
    excel_sync: {
      status: String(raw?.excel_sync?.status || "unknown").trim().toLowerCase(),
      text: String(raw?.excel_sync?.text || "").trim(),
      tone: String(raw?.excel_sync?.tone || "neutral").trim() || "neutral",
      reason_code: String(raw?.excel_sync?.reason_code || "").trim(),
      error: String(raw?.excel_sync?.error || "").trim(),
      synced_revision: Number.parseInt(String(raw?.excel_sync?.synced_revision || 0), 10) || 0,
      pending_revision: Number.parseInt(String(raw?.excel_sync?.pending_revision || 0), 10) || 0,
    },
    capacity_sync: {
      status: String(raw?.capacity_sync?.status || "unknown").trim().toLowerCase(),
      text: String(raw?.capacity_sync?.text || "").trim(),
      tone: String(raw?.capacity_sync?.tone || "neutral").trim() || "neutral",
      reason_code: String(raw?.capacity_sync?.reason_code || "").trim(),
      error: String(raw?.capacity_sync?.error || "").trim(),
      tracked_cells: trackedCells,
      updated_at: String(raw?.capacity_sync?.updated_at || "").trim(),
    },
    document_state: {
      status: String(raw?.document_state?.status || "unknown").trim().toLowerCase(),
      text: String(raw?.document_state?.text || "").trim(),
      tone: String(raw?.document_state?.tone || "neutral").trim() || "neutral",
      reason_code: String(raw?.document_state?.reason_code || "").trim(),
      detail_text: String(raw?.document_state?.detail_text || "").trim(),
      should_reload_document: Boolean(raw?.document_state?.should_reload_document),
    },
    defaults_sync: normalizeDisplayItem(raw?.defaults_sync, {
      status: "unknown",
      text: "",
      tone: "neutral",
      reason_code: "",
      detail_text: "",
    }),
    status_banners: statusBanners,
    actions: {
      refresh: normalizeDisplayAction(actions.refresh, { allowed: false, visible: false, label: "刷新", tone: "neutral", variant: "secondary" }),
      save: normalizeDisplayAction(actions.save, { allowed: false, visible: false, label: "保存", tone: "primary", variant: "primary" }),
      download: normalizeDisplayAction(actions.download, { allowed: false, visible: false, label: "下载交接班日志", tone: "neutral", variant: "secondary" }),
      capacity_download: normalizeDisplayAction(actions.capacity_download, { allowed: false, visible: false, label: "下载交接班容量报表", tone: "neutral", variant: "secondary" }),
      confirm: normalizeDisplayAction(actions.confirm, { allowed: false, visible: false, label: "确认当前楼栋", tone: "warning", variant: "warning" }),
      retry_cloud_sync: normalizeDisplayAction(actions.retry_cloud_sync, { allowed: false, visible: false, label: "重试云表上传", tone: "warning", variant: "warning" }),
      update_history_cloud_sync: normalizeDisplayAction(actions.update_history_cloud_sync, { allowed: false, visible: false, label: "更新云文档", tone: "warning", variant: "warning" }),
      return_to_latest: normalizeDisplayAction(actions.return_to_latest, { allowed: false, visible: false, label: "返回最新", tone: "neutral", variant: "secondary" }),
    },
  };
}

function resolveReviewActionDisabledReasonStrict(baseAction, pendingText = "请求处理中，请稍候") {
  const action = baseAction && typeof baseAction === "object" ? baseAction : {};
  if (action.allowed === false) {
    return String(action.disabledReason || "").trim();
  }
  if (action.pending) {
    return String(pendingText || "").trim() || "请求处理中，请稍候";
  }
  return "";
}

function resolveOperationFeedbackText(payload, fallback = "") {
  const feedback = payload?.operation_feedback;
  if (feedback && typeof feedback === "object") {
    const text = String(feedback.text || feedback.detail_text || "").trim();
    if (text) {
      return text;
    }
  }
  return String(fallback || "").trim();
}

function badgeVmFromDisplayItem(item, fallback = {}) {
  const normalized = item && typeof item === "object" ? item : {};
  const text = String(normalized.text || fallback.text || "").trim();
  if (!text) {
    return null;
  }
  return badgeVm(
    text,
    String(normalized.tone || fallback.tone || "neutral").trim() || "neutral",
    String(fallback.emphasis || "soft").trim() || "soft",
    String(fallback.icon || "dot").trim() || "dot",
  );
}

function buildReviewActionVmBase({
  baseAction,
  fallbackLabel = "",
  inFlight = false,
  inFlightText = "",
  disabled = false,
}) {
  const action = baseAction && typeof baseAction === "object" ? baseAction : {};
  return {
    text: (inFlight || action.pending)
      ? (String(inFlightText || "").trim() || String(action.label || "").trim() || fallbackLabel)
      : (String(action.label || "").trim() || fallbackLabel),
    disabled: Boolean(disabled),
    disabledReason: resolveReviewActionDisabledReasonStrict(action),
  };
}

function emptyReviewCloudSheetVm() {
  return {
    status: "unknown",
    text: "",
    tone: "neutral",
    reason_code: "",
    url: "",
    error: "",
  };
}

export function mountHandoverReviewApp(Vue) {
  const { createApp, ref, computed, onMounted, onBeforeUnmount } = Vue;

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
      const reviewDisplayState = ref(normalizeReviewDisplayState({}));
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
      const documentHydrating = ref(true);
      const pollTimer = ref(null);
      const heartbeatTimer = ref(null);
      const documentMutationVersion = ref(0);
      const pollIntervalMs = ref(DEFAULT_POLL_INTERVAL_MS);
      const reviewClientIdentity = ensureReviewClientIdentity();
      const reviewClientId = String(reviewClientIdentity.clientId || "").trim();
      const reviewHolderLabel = String(reviewClientIdentity.holderLabel || "").trim();
      const concurrency = ref(emptyConcurrencyState(0));
      const historyLoading = ref(false);
      const historyLoaded = ref(false);
      const historyCacheKey = ref("");
      const staleRevisionConflict = ref(false);
      const syncingRemoteRevision = ref(false);
      const heldLockSessionId = ref("");
      let latestLoadRequestSeq = 0;
      let activeLoadController = null;
      let activeMetaControllers = [];

      const cloudSyncBusy = computed(() => retryingCloudSync.value || updatingHistoryCloudSync.value);
      const capacitySync = computed(() => {
        const backendCapacity = reviewDisplayState.value?.capacity_sync;
        if (backendCapacity && backendCapacity.status) {
          return {
            ...normalizeCapacitySync(session.value?.capacity_sync || {}),
            status: String(backendCapacity.status || "").trim().toLowerCase() || "failed",
            error: String(backendCapacity.error || "").trim(),
            tracked_cells: Array.isArray(backendCapacity.tracked_cells) && backendCapacity.tracked_cells.length
              ? backendCapacity.tracked_cells
              : normalizeCapacitySync(session.value?.capacity_sync || {}).tracked_cells,
            updated_at: String(backendCapacity.updated_at || "").trim(),
          };
        }
        return normalizeCapacitySync(session.value?.capacity_sync || {});
      });
      const capacityTrackedCellSet = computed(() => new Set(capacitySync.value.tracked_cells || CAPACITY_SYNC_TRACKED_CELLS));
      const {
        selectedSessionId,
        latestSessionId,
        isHistoryMode,
        historySessions,
        selectedSessionInHistoryList,
        selectedSessionIdInListOrEmpty,
        historySelectorHint,
        sessionSummary,
        currentDutyDateText,
        currentDutyShiftText,
        currentModeText,
        refreshActionBase,
        saveActionBase,
        downloadActionBase,
        capacityDownloadActionBase,
        confirmActionBase,
        retryCloudSyncActionBase,
        updateHistoryCloudSyncActionBase,
        returnToLatestActionBase,
        showRefreshAction,
        showSaveAction,
        showDownloadAction,
        showCapacityDownloadAction,
        showConfirmAction,
        showReturnToLatestAction,
        reviewSaveBadge,
        reviewCloudSheetVm,
        reviewCloudSheetUrl,
        reviewHeaderBadges,
        refreshActionVm,
        downloadActionVm,
        capacityDownloadActionVm,
        capacityDownloadDisabled,
        reviewStatusBanners,
        confirmActionVm,
        saveActionVm,
        retryCloudSyncActionVm,
        showRetryCloudSyncAction,
        updateHistoryCloudSyncActionVm,
        showUpdateHistoryCloudSyncAction,
        returnToLatestActionVm,
      } = createHandoverReviewDisplayUiHelpers({
        computed,
        session,
        reviewDisplayState,
        historyState,
        historyLoading,
        activeRouteSelection,
        loading,
        saving,
        downloading,
        capacityDownloading,
        confirming,
        retryingCloudSync,
        updatingHistoryCloudSync,
        dirty,
        syncingRemoteRevision,
        cloudSyncBusy,
        errorText,
        statusText,
        badgeVm,
        badgeVmFromDisplayItem,
        normalizeDisplayBadge,
        buildReviewActionVmBase,
        emptyReviewCloudSheetVm,
        resolveReviewActionDisabledReasonStrict,
        shiftTextFromCode,
      });

      function clearSaveTimers() {
        // 审核页已改为显式保存，这里保留空实现，兼容现有调用点。
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

      function handleWindowBeforeUnload(event) {
        if (dirty.value || saving.value) {
          if (event && typeof event.preventDefault === "function") {
            event.preventDefault();
          }
          if (event) {
            event.returnValue = "";
          }
          return "";
        }
        void releaseCurrentLock({ keepalive: true });
        return undefined;
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

      function buildStatusParams() {
        const params = buildLoadParams();
        const currentSessionId = String(session.value?.session_id || "").trim();
        const currentRevision = Number.parseInt(String(session.value?.revision || 0), 10) || 0;
        if (currentSessionId) {
          params.client_session_id = currentSessionId;
        }
        if (currentRevision > 0) {
          params.client_revision = currentRevision;
        }
        return params;
      }

      function shouldPreferBootstrapLoad({ forceLatest = false } = {}) {
        if (forceLatest) {
          return true;
        }
        const explicitSessionId = String(activeRouteSelection.value.sessionId || "").trim();
        const hasExplicitDutyContext = Boolean(
          activeRouteSelection.value.dutyDate && activeRouteSelection.value.dutyShift,
        );
        if (explicitSessionId || hasExplicitDutyContext) {
          return false;
        }
        const backendMode = String(reviewDisplayState.value?.mode?.code || "").trim().toLowerCase();
        return backendMode !== "history";
      }

      function syncRouteToCurrentSelection(nextHistory = historyState.value) {
        const selectedId = String(nextHistory?.selected_session_id || session.value?.session_id || "").trim();
        syncReviewSelectionToUrl({
          sessionId: selectedId,
          isLatest: Boolean(nextHistory?.selected_is_latest),
        });
      }

      function buildHistoryCacheKey(sessionPayload = session.value, historyPayload = historyState.value) {
        const currentSessionId = String(sessionPayload?.session_id || "").trim();
        const latestId = String(historyPayload?.latest_session_id || currentSessionId || "").trim();
        return `${buildingCode || ""}|${latestId}|${currentSessionId}`;
      }

      function applyPayloadMeta(payload = {}) {
        const nextSession = payload?.session && typeof payload.session === "object" ? cloneDeep(payload.session) : null;
        if (nextSession) {
          session.value = nextSession;
        }
        reviewDisplayState.value = normalizeReviewDisplayState(payload?.display_state || reviewDisplayState.value);
        batchStatus.value = payload?.batch_status && typeof payload.batch_status === "object"
          ? cloneDeep(payload.batch_status)
          : batchStatus.value;
        const hasHistoryPayload = Object.prototype.hasOwnProperty.call(payload || {}, "history");
        if (hasHistoryPayload) {
          applyHistoryPayload(payload);
        } else {
          const currentHistory = historyState.value && typeof historyState.value === "object"
            ? cloneDeep(historyState.value)
            : {};
          const selectedId = String(nextSession?.session_id || currentHistory.selected_session_id || session.value?.session_id || "").trim();
          const latestId = String(
            payload?.latest_session_id
            || currentHistory.latest_session_id
            || nextSession?.session_id
            || "",
          ).trim();
          historyState.value = normalizeHistoryPayload(
            {
              ...currentHistory,
              latest_session_id: latestId,
              selected_session_id: selectedId,
              selected_is_latest: selectedId
                ? Boolean(latestId && selectedId === latestId)
                : (!activeRouteSelection.value.sessionId
                  && !(activeRouteSelection.value.dutyDate && activeRouteSelection.value.dutyShift)),
              sessions: Array.isArray(currentHistory.sessions) ? currentHistory.sessions : [],
            },
            nextSession || session.value,
          );
          if (!historyLoaded.value) {
            historyCacheKey.value = "";
          }
        }
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

      function clearMetaControllers() {
        activeMetaControllers.forEach((controller) => {
          if (controller && typeof controller.abort === "function") {
            controller.abort();
          }
        });
        activeMetaControllers = [];
      }

      function createRequestController() {
        return typeof AbortController === "function" ? new AbortController() : null;
      }

      function buildRequestOptions(controller = null) {
        if (!controller) {
          return {};
        }
        return {
          signal: controller.signal,
          retryTransientNetworkErrors: false,
        };
      }

      function resolvePollInterval(reviewUi = {}) {
        return Math.max(
          1000,
          Number(reviewUi.poll_interval_sec || DEFAULT_POLL_INTERVAL_MS / 1000) * 1000,
        );
      }

      function applyHistoryPayload(payload = {}) {
        const normalizedHistory = normalizeHistoryPayload(payload?.history || {}, session.value);
        historyState.value = normalizedHistory;
        historyLoaded.value = true;
        historyCacheKey.value = buildHistoryCacheKey(session.value, normalizedHistory);
        const selectedId = String(normalizedHistory?.selected_session_id || session.value?.session_id || "").trim();
        activeRouteSelection.value = {
          sessionId: normalizedHistory?.selected_is_latest ? "" : selectedId,
          dutyDate: "",
          dutyShift: "",
        };
        syncRouteToCurrentSelection(normalizedHistory);
      }

      async function ensureHistoryLoaded({ force = false } = {}) {
        if (!buildingCode || !session.value || !String(session.value?.session_id || "").trim()) {
          return;
        }
        const targetKey = buildHistoryCacheKey();
        if (!force && historyLoaded.value && historyCacheKey.value === targetKey) {
          return;
        }
        if (historyLoading.value) {
          return;
        }
        historyLoading.value = true;
        try {
          const payload = await getHandoverReviewHistoryApi(
            buildingCode,
            buildLoadParams(),
          );
          applyHistoryPayload(payload || {});
        } catch (error) {
          if (!errorText.value) {
            statusText.value = String(error?.message || error || "历史交接班日志加载失败");
          }
        } finally {
          historyLoading.value = false;
        }
      }

      async function applyStatusPayload(payload = {}, { background = false } = {}) {
        const reviewUi = payload?.review_ui && typeof payload.review_ui === "object" ? payload.review_ui : {};
        pollIntervalMs.value = resolvePollInterval(reviewUi);
        restartPollTimer();
        const nextDisplayState = normalizeReviewDisplayState(payload?.display_state || reviewDisplayState.value);
        reviewDisplayState.value = nextDisplayState;

        const incomingSession = payload?.session && typeof payload.session === "object" ? cloneDeep(payload.session) : {};
        const currentSessionId = String(session.value?.session_id || "").trim();
        const incomingSessionId = String(incomingSession.session_id || "").trim();
        const incomingRevision = Number(incomingSession.revision || 0);
        const currentRevision = Number(session.value?.revision || 0);

        batchStatus.value = payload?.batch_status && typeof payload.batch_status === "object"
          ? cloneDeep(payload.batch_status)
          : batchStatus.value;
        const incomingLatestSessionId = String(payload?.latest_session_id || "").trim();
        if (incomingLatestSessionId) {
          const currentHistory = historyState.value && typeof historyState.value === "object"
            ? cloneDeep(historyState.value)
            : {};
          historyState.value = normalizeHistoryPayload(
            {
              ...currentHistory,
              latest_session_id: incomingLatestSessionId,
              selected_session_id: String(currentHistory.selected_session_id || incomingSession.session_id || session.value?.session_id || "").trim(),
              sessions: Array.isArray(currentHistory.sessions) ? currentHistory.sessions : [],
            },
            incomingSession && Object.keys(incomingSession).length ? incomingSession : session.value,
          );
        }
        applyConcurrencyState(payload?.concurrency, incomingRevision || currentRevision, incomingSessionId || currentSessionId);

        if (!session.value) {
          if (incomingSession && Object.keys(incomingSession).length) {
            session.value = incomingSession;
          }
          return;
        }

        if (nextDisplayState.document_state?.should_reload_document) {
          const reloadMessage = String(
            nextDisplayState.document_state.detail_text
            || nextDisplayState.document_state.text
            || "检测到审核内容更新，正在同步最新内容...",
          ).trim();
          if (saving.value) {
            session.value = {
              ...(session.value || {}),
              ...(incomingSession || {}),
            };
            return;
          }
          if (dirty.value) {
            clearSaveTimers();
            beginRemoteSaveRefresh(reloadMessage);
            session.value = {
              ...(session.value || {}),
              ...(incomingSession || {}),
            };
            return;
          }
          if (background) {
            statusText.value = reloadMessage;
            await loadReviewData({
              background: false,
              mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full",
            });
            return;
          }
        }

        if (!dirty.value && !saving.value) {
          session.value = {
            ...(session.value || {}),
            ...(incomingSession || {}),
          };
          staleRevisionConflict.value = false;
        }
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

        documentHydrating.value = true;
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
        syncingRemoteRevision.value = false;
        window.setTimeout(() => {
          documentHydrating.value = false;
        }, 0);
      }

      async function loadReviewData({ background = false, mode = "auto" } = {}) {
        if (!buildingCode) {
          loading.value = false;
          errorText.value = "无效的楼栋审核页面地址";
          return;
        }
        if (background && (saving.value || loading.value || confirming.value || cloudSyncBusy.value || syncingRemoteRevision.value || downloading.value || capacityDownloading.value)) {
          return;
        }
        const resolvedMode = background ? "status" : (mode === "bootstrap" ? "bootstrap" : "full");
        const requestSeq = ++latestLoadRequestSeq;
        if (activeLoadController && typeof activeLoadController.abort === "function") {
          activeLoadController.abort();
        }
        clearMetaControllers();
        activeLoadController = createRequestController();
        try {
          if (!background) {
            loading.value = true;
          }
          if (resolvedMode === "bootstrap") {
            statusText.value = "正在加载最新交接班内容...";
            errorText.value = "";
            let payload;
            try {
              payload = await getHandoverReviewBootstrapApi(
                buildingCode,
                buildLoadParams(),
                buildRequestOptions(activeLoadController),
              );
            } catch (error) {
              if (error?.name === "AbortError") {
                return;
              }
              payload = await getHandoverReviewApi(
                buildingCode,
                buildLoadParams(),
                buildRequestOptions(activeLoadController),
              );
            }
            if (requestSeq !== latestLoadRequestSeq) {
              return;
            }
            pollIntervalMs.value = resolvePollInterval(payload?.review_ui && typeof payload.review_ui === "object" ? payload.review_ui : {});
            restartPollTimer();
            hydrateFromPayload(payload, { fromBackground: false });
            statusText.value = "正在补充审核状态信息...";

            const statusController = createRequestController();
            if (statusController) {
              activeMetaControllers.push(statusController);
            }
            await Promise.allSettled([
              getHandoverReviewStatusApi(
                buildingCode,
                buildStatusParams(),
                buildRequestOptions(statusController),
              ).then(async (statusPayload) => {
                if (requestSeq !== latestLoadRequestSeq) {
                  return;
                }
                await applyStatusPayload(statusPayload, { background: false });
              }),
            ]);
            activeMetaControllers = [];
            if (
              requestSeq === latestLoadRequestSeq
              && !dirty.value
              && !saving.value
              && !syncingRemoteRevision.value
              && !errorText.value
              && statusText.value === "正在补充审核状态信息..."
            ) {
              statusText.value = "";
            }
            return;
          }
          const payload = await (resolvedMode === "status" ? getHandoverReviewStatusApi : getHandoverReviewApi)(
            buildingCode,
            resolvedMode === "status" ? buildStatusParams() : buildLoadParams(),
            buildRequestOptions(activeLoadController),
          );
          if (requestSeq !== latestLoadRequestSeq) {
            return;
          }
          if (resolvedMode === "status") {
            await applyStatusPayload(payload, { background: true });
            return;
          }
          pollIntervalMs.value = resolvePollInterval(payload?.review_ui && typeof payload.review_ui === "object" ? payload.review_ui : {});
          restartPollTimer();
          hydrateFromPayload(payload, { fromBackground: false });
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
          if (!background) {
            loading.value = false;
          }
        }
      }

      async function saveDocument(options = {}) {
        const { reason = "manual" } = options || {};
        if (saving.value || confirming.value || cloudSyncBusy.value || documentHydrating.value || syncingRemoteRevision.value || !session.value) return false;
        if (staleRevisionConflict.value) {
          beginRemoteSaveRefresh();
          return false;
        }
        if (!dirty.value) {
          clearSaveTimers();
          dirty.value = false;
          statusText.value = isHistoryMode.value ? "历史交接班日志已保存" : "已保存";
          return true;
        }
        const payloadVersion = documentMutationVersion.value;
        clearSaveTimers();
        saving.value = true;
        errorText.value = "";
        await ensureEditingLock();
        const payloadDirtyRegions = cloneDirtyRegions(dirtyRegions.value);
        statusText.value = "正在保存审核内容...";
        try {
          const response = await saveHandoverReviewApi(buildingCode, {
            session_id: session.value.session_id,
            base_revision: session.value.revision,
            client_id: reviewClientId,
            document: documentRef.value,
            dirty_regions: payloadDirtyRegions,
          });
          applyPayloadMeta(response || {});
          broadcastHandoverReviewStatusChange(response || {});
          if (documentMutationVersion.value === payloadVersion) {
            dirtyRegions.value = emptyDirtyRegions();
            capacityLinkedDirty.value = false;
            dirty.value = false;
          } else {
            dirty.value = true;
          }
          staleRevisionConflict.value = false;
          needsRefresh.value = false;
          const saveStatus = response?.save_status && typeof response.save_status === "object"
            ? response.save_status
            : null;
          statusText.value = String(saveStatus?.state_text || "").trim()
            || (isHistoryMode.value ? "历史交接班日志已保存" : "已保存");
          return true;
        } catch (error) {
          if (isRevisionConflictError(error)) {
            beginRemoteSaveRefresh();
            await loadReviewData({
              background: false,
              mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full",
            });
            statusText.value = "已同步最新审核内容";
            return false;
          }
          errorText.value = String(error?.message || error || "保存失败");
          statusText.value = "保存失败，请处理后重试。";
          return false;
        } finally {
          saving.value = false;
        }
      }

      const {
        markDocumentDirty,
        updateFixedField,
        updateSectionCell,
        addSectionRow,
        removeSectionRow,
        updateFooterCell,
        addFooterRow,
        removeFooterRow,
      } = createHandoverReviewDocumentEditHelpers({
        documentRef,
        session,
        dirtyRegions,
        capacityTrackedCellSet,
        capacityLinkedDirty,
        documentMutationVersion,
        dirty,
        staleRevisionConflict,
        clearSaveTimers,
        beginRemoteSaveRefresh,
        isHistoryMode,
        statusText,
        touchEditingIntent,
        blankRow,
        hasSectionRowContent,
        footerRowHasContent,
        blankFooterInventoryRowWithDefaults,
        resolveFooterAutoFillCells,
        blankFooterInventoryRow,
      });

      const {
        switchToSession,
        onHistorySelectionChange,
        returnToLatestSession,
        toggleConfirm,
        retryCloudSheetSync,
        updateHistoryCloudSync,
        downloadCurrentReviewFile,
        downloadCurrentCapacityReviewFile,
        refreshData,
        saveCurrentReview,
      } = createHandoverReviewActionHelpers({
        session,
        building,
        buildingCode,
        dirty,
        saving,
        confirming,
        cloudSyncBusy,
        syncingRemoteRevision,
        needsRefresh,
        staleRevisionConflict,
        downloading,
        capacityDownloading,
        retryingCloudSync,
        updatingHistoryCloudSync,
        activeRouteSelection,
        selectedSessionId,
        latestSessionId,
        isHistoryMode,
        statusText,
        errorText,
        reviewClientId,
        returnToLatestActionBase,
        confirmActionBase,
        confirmActionVm,
        retryCloudSyncActionBase,
        retryCloudSyncActionVm,
        updateHistoryCloudSyncActionBase,
        updateHistoryCloudSyncActionVm,
        downloadActionBase,
        downloadActionVm,
        capacityDownloadActionBase,
        capacityDownloadActionVm,
        refreshActionBase,
        refreshActionVm,
        clearSaveTimers,
        saveDocument,
        releaseCurrentLock,
        loadReviewData,
        shouldPreferBootstrapLoad,
        beginRemoteSaveRefresh,
        isRevisionConflictError,
        applyPayloadMeta,
        broadcastHandoverReviewStatusChange,
        resolveOperationFeedbackText,
        syncReviewSelectionToUrl,
        confirmHandoverReviewApi,
        unconfirmHandoverReviewApi,
        retryHandoverReviewCloudSyncApi,
        updateHandoverReviewCloudSyncApi,
        buildHandoverReviewDownloadUrl,
        buildHandoverReviewCapacityDownloadUrl,
        triggerBrowserDownload,
      });

      onMounted(async () => {
        if (typeof window !== "undefined") {
          window.addEventListener("beforeunload", handleWindowBeforeUnload);
        }
        syncReviewSelectionToUrl({
          sessionId: activeRouteSelection.value.sessionId,
          isLatest: !activeRouteSelection.value.sessionId,
        });
        const useBootstrap = shouldPreferBootstrapLoad();
        await loadReviewData({ background: false, mode: useBootstrap ? "bootstrap" : "full" });
      });

      onBeforeUnmount(() => {
        clearSaveTimers();
        clearHeartbeatTimer();
        clearMetaControllers();
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
        historyLoading,
        selectedSessionId,
        selectedSessionInHistoryList,
        selectedSessionIdInListOrEmpty,
        historySelectorHint,
        isHistoryMode,
        currentDutyDateText,
        currentDutyShiftText,
        currentModeText,
        showReturnToLatestAction,
        showRefreshAction,
        showSaveAction,
        showDownloadAction,
        showCapacityDownloadAction,
        showConfirmAction,
        showRetryCloudSyncAction,
        showUpdateHistoryCloudSyncAction,
        sessionSummary,
        reviewSaveBadge,
        refreshActionVm,
        saveActionVm,
        downloadActionVm,
        capacityDownloadActionVm,
        retryCloudSyncActionVm,
        updateHistoryCloudSyncActionVm,
        returnToLatestActionVm,
        reviewCloudSheetVm,
        reviewCloudSheetUrl,
        capacitySync,
        capacityDownloadDisabled,
        reviewHeaderBadges,
        reviewStatusBanners,
        confirmActionVm,
        syncingRemoteRevision,
        onHistorySelectionChange,
        returnToLatestSession,
        ensureHistoryLoaded,
        updateHistoryCloudSync,
        updateFixedField,
        updateSectionCell,
        addSectionRow,
        removeSectionRow,
        updateFooterCell,
        addFooterRow,
        removeFooterRow,
        saveCurrentReview,
        toggleConfirm,
        retryCloudSheetSync: () => retryCloudSheetSync(getJobApi),
        downloadCurrentReviewFile,
        downloadCurrentCapacityReviewFile,
        refreshData,
      };
    },
    template: HANDOVER_REVIEW_TEMPLATE,
  }).mount("#app");
}



