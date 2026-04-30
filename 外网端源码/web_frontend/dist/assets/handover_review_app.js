import {
  buildHandoverReviewCapacityDownloadUrl,
  buildHandoverReviewDownloadUrl,
  claimHandoverReview110kvLockApi,
  claimHandoverReviewLockApi,
  confirmHandoverReviewApi,
  getJobApi,
  getHandoverReviewApi,
  getHandoverReviewBootstrapApi,
  getHandoverReviewHistoryApi,
  getHandoverReviewStatusApi,
  heartbeatHandoverReview110kvLockApi,
  heartbeatHandoverReviewLockApi,
  markHandoverReview110kvDirtyApi,
  releaseHandoverReview110kvLockApi,
  releaseHandoverReviewLockApi,
  retryHandoverReviewCloudSyncApi,
  saveHandoverReviewApi,
  saveHandoverReview110kvApi,
  sendHandoverReviewCapacityImageApi,
  regenerateHandoverReviewApi,
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
const SUBSTATION_110KV_AUTO_SAVE_DEBOUNCE_MS = 350;
const SUBSTATION_110KV_PREVIEW_SYNC_DEBOUNCE_MS = 150;
const SUBSTATION_110KV_LOCK_RELEASE_IDLE_MS = 10000;
const REVIEW_CLIENT_ID_STORAGE_KEY = "handover_review_client_id";
const REVIEW_CLIENT_LABEL_STORAGE_KEY = "handover_review_client_label";
const HANDOVER_REVIEW_STATUS_BROADCAST_KEY = "handover_review_status_broadcast_v1";
const CAPACITY_SYNC_TRACKED_CELLS = ["H6", "F8", "B6", "D6", "F6", "D8", "B13", "D13"];
const SUBSTATION_110KV_ROWS = [
  { row_id: "incoming_akai", label: "阿开", group: "incoming" },
  { row_id: "incoming_ajia", label: "阿家", group: "incoming" },
  { row_id: "transformer_1", label: "1#主变", group: "transformer" },
  { row_id: "transformer_2", label: "2#主变", group: "transformer" },
  { row_id: "transformer_3", label: "3#主变", group: "transformer" },
  { row_id: "transformer_4", label: "4#主变", group: "transformer" },
];
const SUBSTATION_110KV_VALUE_KEYS = ["line_voltage", "current", "power_kw", "power_factor", "load_rate"];

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

async function readDownloadError(response) {
  const fallback = `下载失败（HTTP ${response?.status || "?"}）`;
  const contentType = String(response?.headers?.get("content-type") || "").toLowerCase();
  try {
    if (contentType.includes("application/json")) {
      const payload = await response.json();
      if (payload && typeof payload === "object") {
        const detail = payload.detail || payload.error || payload.message;
        if (typeof detail === "string" && detail.trim()) return detail.trim();
        if (detail && typeof detail === "object") return JSON.stringify(detail);
      }
    }
    const text = await response.text();
    return String(text || "").trim() || fallback;
  } catch (_error) {
    return fallback;
  }
}

async function triggerBrowserDownload(url, fallbackName = "") {
  const response = await window.fetch(String(url || ""), {
    method: "GET",
    cache: "no-store",
  });
  if (!response.ok) {
    const detail = await readDownloadError(response);
    const error = new Error(detail);
    error.httpStatus = response.status;
    error.responseText = detail;
    error.responseRawText = detail;
    throw error;
  }
  const warningBase64 = String(response.headers.get("x-handover-download-warning-base64") || "").trim();
  let warning = "";
  if (warningBase64) {
    try {
      const binary = window.atob(warningBase64);
      const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
      warning = new TextDecoder("utf-8").decode(bytes).trim();
    } catch (_error) {
      warning = "";
    }
  }
  const blob = await response.blob();
  const objectUrl = window.URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  const downloadName = parseDownloadFilename(response.headers.get("content-disposition"), fallbackName);
  if (downloadName) {
    anchor.download = downloadName;
  }
  anchor.rel = "noopener";
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => window.URL.revokeObjectURL(objectUrl), 1000);
  return { warning };
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

function normalizeCoolingPumpPressures(raw = {}) {
  const rawTanks = raw?.tanks && typeof raw.tanks === "object" ? raw.tanks : {};
  const tanks = {};
  ["west", "east"].forEach((zone) => {
    const tank = rawTanks?.[zone] && typeof rawTanks[zone] === "object" ? rawTanks[zone] : {};
    tanks[zone] = {
      zone,
      zone_label: String(tank.zone_label || (zone === "east" ? "东区" : "西区")).trim(),
      temperature: String(tank.temperature ?? ""),
      level: String(tank.level ?? ""),
    };
  });
  const rows = Array.isArray(raw?.rows)
    ? raw.rows
        .filter((row) => row && typeof row === "object")
        .map((row) => {
          const zone = String(row.zone || "").trim().toLowerCase();
          const unit = Number.parseInt(String(row.unit || 0), 10) || 0;
          return {
            row_id: String(row.row_id || `${zone}:${unit}`).trim(),
            zone,
            zone_label: String(row.zone_label || (zone === "east" ? "东区" : "西区")).trim(),
            unit,
            unit_label: String(row.unit_label || (unit ? `${unit}#制冷单元` : "制冷单元")).trim(),
            position: Number.parseInt(String(row.position || 0), 10) || 0,
            mode_text: String(row.mode_text || "").trim(),
            inlet_pressure: String(row.inlet_pressure ?? ""),
            outlet_pressure: String(row.outlet_pressure ?? ""),
            cooling_tower_level: String(row.cooling_tower_level ?? ""),
          };
        })
        .filter((row) => row.zone && row.unit > 0)
    : [];
  return { rows, tanks };
}

function normalizeSubstation110kvBlock(raw = {}) {
  const sourceRows = Array.isArray(raw?.rows) ? raw.rows : [];
  const byId = new Map();
  const byLabel = new Map();
  sourceRows.forEach((row) => {
    if (!row || typeof row !== "object") return;
    const rowId = String(row.row_id || "").trim();
    const label = String(row.label || "").trim();
    if (rowId) byId.set(rowId, row);
    if (label) byLabel.set(label, row);
  });
  return {
    block_id: "substation_110kv",
    batch_key: String(raw?.batch_key || "").trim(),
    revision: Number.parseInt(String(raw?.revision || 0), 10) || 0,
    updated_at: String(raw?.updated_at || "").trim(),
    updated_by_building: String(raw?.updated_by_building || "").trim(),
    updated_by_client: String(raw?.updated_by_client || "").trim(),
    columns: [
      { key: "line_voltage", label: "线电压" },
      { key: "current", label: "电流/输出电流" },
      { key: "power_kw", label: "当前功率KW" },
      { key: "power_factor", label: "功率因数" },
      { key: "load_rate", label: "负载率" },
    ],
    rows: SUBSTATION_110KV_ROWS.map((base) => {
      const source = byId.get(base.row_id) || byLabel.get(base.label) || {};
      const row = { ...base };
      SUBSTATION_110KV_VALUE_KEYS.forEach((key) => {
        row[key] = String(source[key] ?? "");
      });
      return row;
    }),
  };
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
  const contentRows = rows.filter((row) => hasSectionRowContent(row, columns));
  return {
    name: String(section?.name || "未命名分类"),
    columns,
    header: columns.map((column) => column.label || column.key),
    rows: contentRows.length ? contentRows : [blankRow(columns)],
  };
}

function compactDocumentSectionRows(document) {
  if (!document || typeof document !== "object" || !Array.isArray(document.sections)) {
    return document;
  }
  document.sections.forEach((section) => {
    if (!section || typeof section !== "object") return;
    const columns = resolveSectionColumns(section);
    const rows = Array.isArray(section.rows)
      ? section.rows.map((row) => normalizeSectionRow(row, columns))
      : [];
    const contentRows = rows.filter((row) => hasSectionRowContent(row, columns));
    section.columns = columns;
    section.header = columns.map((column) => column.label || column.key);
    section.rows = contentRows.length ? contentRows : [blankRow(columns)];
  });
  return document;
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
    cooling_pump_pressures: normalizeCoolingPumpPressures(document?.cooling_pump_pressures || {}),
  };
}

function emptyDirtyRegions() {
  return {
    fixed_blocks: false,
    sections: false,
    footer_inventory: false,
    cooling_pump_pressures: false,
  };
}

function cloneDirtyRegions(dirtyRegions) {
  return {
    fixed_blocks: Boolean(dirtyRegions?.fixed_blocks),
    sections: Boolean(dirtyRegions?.sections),
    footer_inventory: Boolean(dirtyRegions?.footer_inventory),
    cooling_pump_pressures: Boolean(dirtyRegions?.cooling_pump_pressures),
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

function normalizeSharedLockPayload(raw, fallbackRevision = 0) {
  const base = normalizeConcurrencyPayload(raw, fallbackRevision);
  if (base.active_editor && raw?.active_editor?.holder_building) {
    base.active_editor.holder_building = String(raw.active_editor.holder_building || "").trim();
  }
  base.dirty = Boolean(raw?.dirty);
  base.dirty_at = String(raw?.dirty_at || "").trim();
  base.dirty_by_building = String(raw?.dirty_by_building || "").trim();
  base.dirty_by_client = String(raw?.dirty_by_client || "").trim();
  return base;
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
      capacity_image_send: normalizeDisplayAction(actions.capacity_image_send, { allowed: false, visible: false, label: "发送容量表图片", tone: "neutral", variant: "secondary" }),
      regenerate: normalizeDisplayAction(actions.regenerate, { allowed: false, visible: false, label: "重新生成交接班及容量表", tone: "warning", variant: "warning" }),
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
      const capacityImageSending = ref(false);
      const regenerating = ref(false);
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
      const sharedBlocks = ref({
        substation_110kv: normalizeSubstation110kvBlock({}),
      });
      const sharedBlockLocks = ref({
        substation_110kv: normalizeSharedLockPayload({}, 0),
      });
      const substation110kvDirty = ref(false);
      const substation110kvHeartbeatTimer = ref(null);
      const substation110kvAutoSaveTimer = ref(null);
      const substation110kvPreviewSyncTimer = ref(null);
      const substation110kvIdleReleaseTimer = ref(null);
      const substation110kvDirtyMarked = ref(false);
      const historyLoading = ref(false);
      const historyLoaded = ref(false);
      const historyCacheKey = ref("");
      const staleRevisionConflict = ref(false);
      const syncingRemoteRevision = ref(false);
      const heldLockSessionId = ref("");
      let latestLoadRequestSeq = 0;
      let latestSubstation110kvSaveSeq = 0;
      let substation110kvLocalVersion = 0;
      let substation110kvAutoSavePromise = null;
      let substation110kvDirtyMarkPromise = Promise.resolve(true);
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
      const substation110kvBlock = computed(() => sharedBlocks.value.substation_110kv || normalizeSubstation110kvBlock({}));
      const substation110kvLock = computed(() => sharedBlockLocks.value.substation_110kv || normalizeSharedLockPayload({}, substation110kvBlock.value.revision));
      const substation110kvLockedByOther = computed(() => Boolean(substation110kvLock.value?.is_editing_elsewhere));
      const substation110kvReadonly = computed(() => loading.value || saving.value || regenerating.value || substation110kvLockedByOther.value);
      const substation110kvMetaText = computed(() => {
        const block = substation110kvBlock.value;
        const updatedAt = String(block.updated_at || "").trim();
        const updatedBy = String(block.updated_by_building || "").trim();
        if (!updatedAt && !updatedBy) return "本班尚未填写";
        return `最新修改：${updatedAt || "-"} ${updatedBy || ""}`.trim();
      });
      const substation110kvLockText = computed(() => {
        const lock = substation110kvLock.value;
        if (lock?.dirty) {
          const owner = lock.dirty_by_building || lock?.active_editor?.holder_building || "其他楼栋";
          return `${owner} 正在编辑并自动保存中`;
        }
        if (lock?.client_holds_lock) return "本端编辑中";
        const active = lock?.active_editor || {};
        if (lock?.is_editing_elsewhere) {
          return `${active.holder_building || "其他楼栋"} ${active.holder_label || ""} 正在查看/编辑`;
        }
        return "";
      });
      function hasReviewDocumentDirty() {
        return Boolean(
          dirtyRegions.value.fixed_blocks
          || dirtyRegions.value.sections
          || dirtyRegions.value.footer_inventory
          || dirtyRegions.value.cooling_pump_pressures,
        );
      }

      function refreshDirtyFlagFromRegions() {
        dirty.value = hasReviewDocumentDirty() || substation110kvDirty.value;
      }
      const coolingPumpPressureRows = computed(() => documentRef.value?.cooling_pump_pressures?.rows || []);
      const coolingTankRows = computed(() => {
        const tanks = documentRef.value?.cooling_pump_pressures?.tanks || {};
        return ["west", "east"].map((zone) => {
          const row = tanks?.[zone] && typeof tanks[zone] === "object" ? tanks[zone] : {};
          return {
            zone,
            zone_label: String(row.zone_label || (zone === "east" ? "东区" : "西区")).trim(),
            temperature: String(row.temperature ?? ""),
            level: String(row.level ?? ""),
          };
        });
      });
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
        capacityImageSendActionBase,
        regenerateActionBase,
        confirmActionBase,
        retryCloudSyncActionBase,
        updateHistoryCloudSyncActionBase,
        returnToLatestActionBase,
        showRefreshAction,
        showSaveAction,
        showDownloadAction,
        showCapacityDownloadAction,
        showCapacityImageSendAction,
        showRegenerateAction,
        showConfirmAction,
        showReturnToLatestAction,
        reviewSaveBadge,
        reviewCloudSheetVm,
        reviewCloudSheetUrl,
        reviewHeaderBadges,
        refreshActionVm,
        downloadActionVm,
        capacityDownloadActionVm,
        capacityImageSendActionVm,
        regenerateActionVm,
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
        capacityImageSending,
        regenerating,
        confirming,
        retryingCloudSync,
        updatingHistoryCloudSync,
        dirty,
        syncingRemoteRevision,
        needsRefresh,
        staleRevisionConflict,
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
            void window.fetch(`/api/handover/review/${encodeURIComponent(buildingCode)}/lock/release`, {
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
        if (Number.parseInt(String(error?.httpStatus || 0), 10) !== 409) return false;
        const text = String(
          error?.message
          || error?.responseText
          || error?.responseRawText
          || "",
        ).trim().toLowerCase();
        if (!text) return false;
        return text.includes("revision conflict")
          || text.includes("revision落后")
          || text.includes("无法同步excel")
          || text.includes("版本冲突")
          || text.includes("已被其他人修改")
          || text.includes("内容已被其他")
          || text.includes("审核内容已被更新")
          || text.includes("已被其他楼栋更新");
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
        void releaseSubstation110kvLock({ keepalive: true });
        return undefined;
      }

      function handleReviewStatusBroadcast(event) {
        if (!event || event.key !== HANDOVER_REVIEW_STATUS_BROADCAST_KEY) return;
        if (saving.value || loading.value || syncingRemoteRevision.value) return;
        let payload = {};
        try {
          payload = JSON.parse(String(event.newValue || "{}"));
        } catch (_error) {
          return;
        }
        const incomingBatchKey = String(payload?.batch_key || "").trim();
        const currentBatchKey = String(session.value?.batch_key || batchStatus.value?.batch_key || "").trim();
        if (incomingBatchKey && currentBatchKey && incomingBatchKey !== currentBatchKey) return;
        void loadReviewData({ background: true });
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
        applySharedBlockPayload(payload || {});
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

      function applySharedBlockPayload(payload = {}) {
        const shared = payload?.shared_blocks && typeof payload.shared_blocks === "object" ? payload.shared_blocks : {};
        const locks = payload?.shared_block_locks && typeof payload.shared_block_locks === "object" ? payload.shared_block_locks : {};
        const hasIncomingSubstation = Object.prototype.hasOwnProperty.call(shared, "substation_110kv");
        let incomingBlock = normalizeSubstation110kvBlock(hasIncomingSubstation ? shared.substation_110kv : sharedBlocks.value.substation_110kv);
        const incomingLock = normalizeSharedLockPayload(
          locks.substation_110kv || sharedBlockLocks.value.substation_110kv,
          incomingBlock.revision,
        );
        const currentRevision = Number(sharedBlocks.value?.substation_110kv?.revision || 0);
        const incomingRevision = Number(incomingBlock.revision || 0);
        if (hasIncomingSubstation && currentRevision > 0 && incomingRevision > 0 && incomingRevision < currentRevision) {
          incomingBlock = normalizeSubstation110kvBlock(sharedBlocks.value.substation_110kv || {});
        }
        const serverRevisionChanged = hasIncomingSubstation && incomingRevision !== currentRevision;
        const preserveLocalRows = Boolean(substation110kvDirty.value);
        if (preserveLocalRows && hasIncomingSubstation) {
          const currentBlock = normalizeSubstation110kvBlock(sharedBlocks.value.substation_110kv || {});
          sharedBlocks.value = {
            ...sharedBlocks.value,
            substation_110kv: {
              ...incomingBlock,
              rows: currentBlock.rows,
            },
          };
        } else if (!substation110kvDirty.value || serverRevisionChanged) {
          sharedBlocks.value = {
            ...sharedBlocks.value,
            substation_110kv: incomingBlock,
          };
          if (serverRevisionChanged && substation110kvDirty.value) {
            substation110kvDirty.value = false;
            substation110kvDirtyMarked.value = false;
            refreshDirtyFlagFromRegions();
            if (!dirty.value && !saving.value) {
              statusText.value = "110KV变电站已同步最新内容";
            }
          }
        }
        sharedBlockLocks.value = {
          ...sharedBlockLocks.value,
          substation_110kv: incomingLock,
        };
        if (sharedBlockLocks.value.substation_110kv?.is_editing_elsewhere && pollIntervalMs.value > 1000) {
          pollIntervalMs.value = 1000;
          restartPollTimer();
        }
        if (sharedBlockLocks.value.substation_110kv?.client_holds_lock) {
          restartSubstation110kvHeartbeat();
          scheduleSubstation110kvIdleRelease();
        } else {
          clearSubstation110kvHeartbeat();
          clearSubstation110kvIdleReleaseTimer();
        }
      }

      function buildSubstation110kvPayload() {
        return {
          session_id: String(session.value?.session_id || "").trim(),
          client_id: reviewClientId,
          holder_label: reviewHolderLabel,
        };
      }

      function clearSubstation110kvAutoSaveTimer() {
        if (substation110kvAutoSaveTimer.value) {
          window.clearTimeout(substation110kvAutoSaveTimer.value);
          substation110kvAutoSaveTimer.value = null;
        }
      }

      function clearSubstation110kvPreviewSyncTimer() {
        if (substation110kvPreviewSyncTimer.value) {
          window.clearTimeout(substation110kvPreviewSyncTimer.value);
          substation110kvPreviewSyncTimer.value = null;
        }
      }

      function clearSubstation110kvIdleReleaseTimer() {
        if (substation110kvIdleReleaseTimer.value) {
          window.clearTimeout(substation110kvIdleReleaseTimer.value);
          substation110kvIdleReleaseTimer.value = null;
        }
      }

      function scheduleSubstation110kvIdleRelease() {
        clearSubstation110kvIdleReleaseTimer();
        if (!sharedBlockLocks.value.substation_110kv?.client_holds_lock) return;
        if (substation110kvDirty.value || substation110kvAutoSavePromise) return;
        substation110kvIdleReleaseTimer.value = window.setTimeout(() => {
          if (substation110kvDirty.value || substation110kvAutoSavePromise) return;
          void releaseSubstation110kvLock();
        }, SUBSTATION_110KV_LOCK_RELEASE_IDLE_MS);
      }

      async function flushSubstation110kvAutoSave() {
        clearSubstation110kvAutoSaveTimer();
        if (!buildingCode || !session.value || !reviewClientId) return true;
        if (!substation110kvDirty.value) return true;
        if (!sharedBlockLocks.value.substation_110kv?.client_holds_lock) {
          const locked = await ensureSubstation110kvLock();
          if (!locked) return false;
        }
        clearSubstation110kvPreviewSyncTimer();
        const marked = await markSubstation110kvServerDirty();
        if (!marked) return false;
        if (substation110kvAutoSavePromise) {
          try {
            await substation110kvAutoSavePromise;
          } catch (_error) {
            // The in-flight save path already surfaced the error state.
          }
          if (!substation110kvDirty.value) return true;
        }
        clearSubstation110kvAutoSaveTimer();
        const requestSeq = ++latestSubstation110kvSaveSeq;
        const saveVersion = substation110kvLocalVersion;
        const block = cloneDeep(substation110kvBlock.value);
        substation110kvAutoSavePromise = (async () => {
          try {
            const response = await saveHandoverReview110kvApi(buildingCode, {
              session_id: session.value.session_id,
              client_id: reviewClientId,
              base_revision: block.revision,
              rows: block.rows,
            });
            if (requestSeq !== latestSubstation110kvSaveSeq) return !substation110kvDirty.value;
            const savedBlock = normalizeSubstation110kvBlock(response?.shared_blocks?.substation_110kv || block);
            if (saveVersion === substation110kvLocalVersion) {
              applySharedBlockPayload(response || {});
              substation110kvDirty.value = false;
              substation110kvDirtyMarked.value = false;
              refreshDirtyFlagFromRegions();
              statusText.value = dirty.value ? "110KV变电站已自动保存" : "已保存";
              scheduleSubstation110kvIdleRelease();
            } else {
              const currentBlock = normalizeSubstation110kvBlock(sharedBlocks.value.substation_110kv || {});
              sharedBlocks.value = {
                ...sharedBlocks.value,
                substation_110kv: {
                  ...savedBlock,
                  rows: currentBlock.rows,
                },
              };
              sharedBlockLocks.value = {
                ...sharedBlockLocks.value,
                substation_110kv: normalizeSharedLockPayload(
                  response?.shared_block_locks?.substation_110kv || sharedBlockLocks.value.substation_110kv,
                  savedBlock.revision,
                ),
              };
              substation110kvDirtyMarked.value = false;
              scheduleSubstation110kvAutoSave();
            }
            broadcastHandoverReviewStatusChange(response || {});
            return saveVersion === substation110kvLocalVersion;
          } finally {
            if (requestSeq === latestSubstation110kvSaveSeq) {
              substation110kvAutoSavePromise = null;
            }
          }
        })();
        try {
          return await substation110kvAutoSavePromise;
        } catch (error) {
          if (isRevisionConflictError(error)) {
            const message = String(error?.message || "110KV变电站内容已被其他楼栋更新，请刷新后重试");
            if (message.includes("正在其他楼栋") || message.includes("正在其他")) {
              statusText.value = message;
              void loadReviewData({ background: true });
              return;
            }
            beginRemoteSaveRefresh(message);
            void loadReviewData({ background: true });
            return false;
          }
          errorText.value = String(error?.message || error || "110KV变电站自动保存失败");
          statusText.value = "110KV变电站自动保存失败，请处理后重试。";
          return false;
        } finally {
          substation110kvAutoSavePromise = null;
        }
      }

      function scheduleSubstation110kvAutoSave() {
        clearSubstation110kvAutoSaveTimer();
        if (!sharedBlockLocks.value.substation_110kv?.client_holds_lock) return;
        substation110kvAutoSaveTimer.value = window.setTimeout(() => {
          void flushSubstation110kvAutoSave();
        }, SUBSTATION_110KV_AUTO_SAVE_DEBOUNCE_MS);
      }

      async function flushSubstation110kvPreviewSync() {
        clearSubstation110kvPreviewSyncTimer();
        if (!buildingCode || !session.value || !reviewClientId || !substation110kvDirty.value) return true;
        const locked = await ensureSubstation110kvLock();
        if (!locked) {
          statusText.value = "110KV变电站锁定失败，请刷新后重试。";
          void loadReviewData({ background: true });
          return false;
        }
        const marked = await markSubstation110kvServerDirty(
          cloneDeep(substation110kvBlock.value),
          { force: true },
        );
        if (marked && substation110kvDirty.value) {
          scheduleSubstation110kvAutoSave();
        }
        return marked;
      }

      function scheduleSubstation110kvPreviewSync() {
        clearSubstation110kvPreviewSyncTimer();
        if (!substation110kvDirty.value) return;
        substation110kvPreviewSyncTimer.value = window.setTimeout(() => {
          void flushSubstation110kvPreviewSync();
        }, SUBSTATION_110KV_PREVIEW_SYNC_DEBOUNCE_MS);
      }

      function clearSubstation110kvHeartbeat() {
        if (substation110kvHeartbeatTimer.value) {
          window.clearInterval(substation110kvHeartbeatTimer.value);
          substation110kvHeartbeatTimer.value = null;
        }
      }

      async function sendSubstation110kvHeartbeat() {
        if (!buildingCode || !session.value || !reviewClientId) return;
        try {
          const response = await heartbeatHandoverReview110kvLockApi(buildingCode, {
            session_id: session.value.session_id,
            client_id: reviewClientId,
          });
          applySharedBlockPayload(response || {});
        } catch (_error) {
          clearSubstation110kvHeartbeat();
        }
      }

      function restartSubstation110kvHeartbeat() {
        clearSubstation110kvHeartbeat();
        if (!sharedBlockLocks.value.substation_110kv?.client_holds_lock) return;
        substation110kvHeartbeatTimer.value = window.setInterval(() => {
          void sendSubstation110kvHeartbeat();
        }, REVIEW_LOCK_HEARTBEAT_MS);
      }

      async function ensureSubstation110kvLock() {
        if (!buildingCode || !session.value || !reviewClientId) return false;
        if (sharedBlockLocks.value.substation_110kv?.client_holds_lock) {
          restartSubstation110kvHeartbeat();
          return true;
        }
        try {
          const response = await claimHandoverReview110kvLockApi(buildingCode, buildSubstation110kvPayload());
          applySharedBlockPayload(response || {});
          return Boolean(sharedBlockLocks.value.substation_110kv?.client_holds_lock);
        } catch (error) {
          errorText.value = String(error?.message || error || "110KV变电站锁定失败");
          return false;
        }
      }

      async function markSubstation110kvServerDirty(blockOverride = null, { force = false } = {}) {
        if (!buildingCode || !session.value || !reviewClientId) return false;
        if (!force && substation110kvDirtyMarked.value && sharedBlockLocks.value.substation_110kv?.dirty) {
          return true;
        }
        const block = normalizeSubstation110kvBlock(blockOverride || substation110kvBlock.value);
        const markRequest = async () => {
          const response = await markHandoverReview110kvDirtyApi(buildingCode, {
            session_id: session.value.session_id,
            client_id: reviewClientId,
            rows: block.rows,
          });
          applySharedBlockPayload(response || {});
          substation110kvDirtyMarked.value = true;
          return true;
        };
        try {
          const nextMarkPromise = substation110kvDirtyMarkPromise.catch(() => true).then(markRequest);
          substation110kvDirtyMarkPromise = nextMarkPromise.catch(() => true);
          return await nextMarkPromise;
        } catch (error) {
          errorText.value = String(error?.message || error || "110KV变电站dirty状态标记失败");
          statusText.value = "110KV变电站锁定或标记失败，请重试。";
          return false;
        }
      }

      async function releaseSubstation110kvLock({ keepalive = false } = {}) {
        clearSubstation110kvHeartbeat();
        clearSubstation110kvIdleReleaseTimer();
        clearSubstation110kvAutoSaveTimer();
        clearSubstation110kvPreviewSyncTimer();
        if (!buildingCode || !session.value || !reviewClientId) return;
        const body = JSON.stringify({
          session_id: session.value.session_id,
          client_id: reviewClientId,
        });
        if (keepalive && typeof window !== "undefined" && typeof window.fetch === "function") {
          try {
            void window.fetch(`/api/handover/review/${encodeURIComponent(buildingCode)}/shared-blocks/110kv/lock/release`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body,
              keepalive: true,
            }).catch(() => {});
          } catch (_error) {
            // Ignore best-effort release failures during unload.
          }
          return;
        }
        try {
          const response = await releaseHandoverReview110kvLockApi(buildingCode, {
            session_id: session.value.session_id,
            client_id: reviewClientId,
          });
          applySharedBlockPayload(response || {});
        } catch (_error) {
          sharedBlockLocks.value = {
            ...sharedBlockLocks.value,
            substation_110kv: normalizeSharedLockPayload({}, sharedBlocks.value.substation_110kv?.revision || 0),
          };
        }
      }

      async function applyStatusPayload(payload = {}, { background = false } = {}) {
        const reviewUi = payload?.review_ui && typeof payload.review_ui === "object" ? payload.review_ui : {};
        pollIntervalMs.value = resolvePollInterval(reviewUi);
        restartPollTimer();
        const nextDisplayState = normalizeReviewDisplayState(payload?.display_state || reviewDisplayState.value);
        applySharedBlockPayload(payload || {});
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
          if (!background) {
            syncingRemoteRevision.value = false;
          }
          return false;
        }
        if (background && (saving.value || loading.value || regenerating.value || confirming.value || cloudSyncBusy.value || syncingRemoteRevision.value || downloading.value || capacityDownloading.value || capacityImageSending.value)) {
          return false;
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
                return false;
              }
              payload = await getHandoverReviewApi(
                buildingCode,
                buildLoadParams(),
                buildRequestOptions(activeLoadController),
              );
            }
            if (requestSeq !== latestLoadRequestSeq) {
              return false;
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
            return true;
          }
          const payload = await (resolvedMode === "status" ? getHandoverReviewStatusApi : getHandoverReviewApi)(
            buildingCode,
            resolvedMode === "status" ? buildStatusParams() : buildLoadParams(),
            buildRequestOptions(activeLoadController),
          );
          if (requestSeq !== latestLoadRequestSeq) {
            return false;
          }
          if (resolvedMode === "status") {
            await applyStatusPayload(payload, { background: true });
            return true;
          }
          pollIntervalMs.value = resolvePollInterval(payload?.review_ui && typeof payload.review_ui === "object" ? payload.review_ui : {});
          restartPollTimer();
          hydrateFromPayload(payload, { fromBackground: false });
          return true;
        } catch (error) {
          if (error?.name === "AbortError") {
            return false;
          }
          if (!background) {
            errorText.value = String(error?.message || error || "加载失败");
            syncingRemoteRevision.value = false;
          }
          return false;
        } finally {
          if (requestSeq === latestLoadRequestSeq) {
            activeLoadController = null;
          }
          if (!background) {
            loading.value = false;
          }
        }
      }

      function markSubstation110kvDirty() {
        if (!session.value) return;
        clearSubstation110kvIdleReleaseTimer();
        substation110kvLocalVersion += 1;
        substation110kvDirty.value = true;
        dirty.value = true;
        statusText.value = "110KV变电站待自动保存";
        scheduleSubstation110kvAutoSave();
      }

      function updateSubstation110kvCell(rowIndex, key, value) {
        if (substation110kvReadonly.value) return;
        const fieldKey = String(key || "");
        if (!SUBSTATION_110KV_VALUE_KEYS.includes(fieldKey)) return;
        const currentRow = substation110kvBlock.value?.rows?.[rowIndex];
        const nextValue = String(value ?? "");
        if (!currentRow || String(currentRow[fieldKey] ?? "") === nextValue) return;
        const block = cloneDeep(substation110kvBlock.value);
        const row = block.rows?.[rowIndex];
        if (!row || String(row[fieldKey] ?? "") === nextValue) return;
        row[fieldKey] = nextValue;
        sharedBlocks.value = { ...sharedBlocks.value, substation_110kv: block };
        markSubstation110kvDirty();
        scheduleSubstation110kvPreviewSync();
      }

      function valuesAfterRowLabel(cells, label) {
        const labelIndex = cells.findIndex((cell) => String(cell || "").trim() === label);
        if (labelIndex < 0) return [];
        return cells
          .slice(labelIndex + 1)
          .map((cell) => String(cell ?? "").trim())
          .filter((cell) => cell !== "");
      }

      function pasteSubstation110kvTable(event) {
        if (substation110kvReadonly.value) return;
        const text = String(event?.clipboardData?.getData("text/plain") || "").trim();
        if (!text) return;
        const lines = text.split(/\r?\n/).map((line) => line.split("\t"));
        const nextBlock = cloneDeep(substation110kvBlock.value);
        let changed = false;
        let recognized = false;
        nextBlock.rows = nextBlock.rows.map((row) => {
          const matchedLine = lines.find((cells) => cells.some((cell) => String(cell || "").trim() === row.label));
          if (!matchedLine) return row;
          recognized = true;
          const values = valuesAfterRowLabel(matchedLine, row.label);
          if (!values.length) return row;
          const nextRow = { ...row };
          let rowChanged = false;
          SUBSTATION_110KV_VALUE_KEYS.forEach((key, index) => {
            const nextValue = values[index] ?? "";
            if (String(nextRow[key] ?? "") !== nextValue) {
              nextRow[key] = nextValue;
              rowChanged = true;
            }
          });
          if (!rowChanged) return row;
          changed = true;
          return nextRow;
        });
        if (!changed) {
          statusText.value = recognized ? "110KV变电站内容无变化" : "未识别到110KV变电站表格行";
          return;
        }
        sharedBlocks.value = { ...sharedBlocks.value, substation_110kv: nextBlock };
        markSubstation110kvDirty();
        scheduleSubstation110kvPreviewSync();
      }

      function updateCoolingPumpPressure(rowIndex, key, value) {
        const rows = documentRef.value?.cooling_pump_pressures?.rows;
        if (!Array.isArray(rows) || !rows[rowIndex]) return;
        if (!["inlet_pressure", "outlet_pressure"].includes(String(key || ""))) return;
        const nextValue = String(value ?? "");
        if (String(rows[rowIndex][key] ?? "") === nextValue) return;
        rows[rowIndex][key] = nextValue;
        markDocumentDirty({ region: "cooling_pump_pressures" });
      }

      function updateCoolingTowerLevel(rowIndex, value) {
        const rows = documentRef.value?.cooling_pump_pressures?.rows;
        if (!Array.isArray(rows) || !rows[rowIndex]) return;
        const nextValue = String(value ?? "");
        if (String(rows[rowIndex].cooling_tower_level ?? "") === nextValue) return;
        rows[rowIndex].cooling_tower_level = nextValue;
        markDocumentDirty({ region: "cooling_pump_pressures" });
      }

      function updateCoolingTankValue(zone, key, value) {
        const normalizedZone = String(zone || "").trim().toLowerCase();
        if (!["west", "east"].includes(normalizedZone)) return;
        if (!["temperature", "level"].includes(String(key || ""))) return;
        const pressures = documentRef.value?.cooling_pump_pressures;
        if (!pressures || typeof pressures !== "object") return;
        pressures.tanks = pressures.tanks && typeof pressures.tanks === "object" ? pressures.tanks : {};
        const current = pressures.tanks[normalizedZone] && typeof pressures.tanks[normalizedZone] === "object"
          ? pressures.tanks[normalizedZone]
          : {
              zone: normalizedZone,
              zone_label: normalizedZone === "east" ? "东区" : "西区",
              temperature: "",
              level: "",
            };
        const nextValue = String(value ?? "");
        if (String(current[key] ?? "") === nextValue) return;
        pressures.tanks[normalizedZone] = {
          ...current,
          zone: normalizedZone,
          zone_label: current.zone_label || (normalizedZone === "east" ? "东区" : "西区"),
          [key]: nextValue,
        };
        markDocumentDirty({ region: "cooling_pump_pressures" });
      }

      async function saveSubstation110kvIfNeeded() {
        if (!substation110kvDirty.value) return true;
        const locked = await ensureSubstation110kvLock();
        if (!locked) return false;
        const saved = await flushSubstation110kvAutoSave();
        if (!saved || substation110kvDirty.value) {
          return false;
        }
        try {
          await releaseSubstation110kvLock();
          return true;
        } catch (error) {
          errorText.value = String(error?.message || error || "110KV变电站锁释放失败");
          statusText.value = "保存失败，请处理后重试。";
          return false;
        }
      }

      async function saveDocument(options = {}) {
        const { reason = "manual" } = options || {};
        if (saving.value || regenerating.value || confirming.value || cloudSyncBusy.value || capacityImageSending.value || documentHydrating.value || syncingRemoteRevision.value || !session.value) return false;
        if (staleRevisionConflict.value) {
          beginRemoteSaveRefresh();
          return false;
        }
        const hasDocumentDirty = hasReviewDocumentDirty();
        if (!dirty.value && !substation110kvDirty.value) {
          clearSaveTimers();
          dirty.value = false;
          statusText.value = isHistoryMode.value ? "历史交接班日志已保存" : "已保存";
          return true;
        }
        const payloadVersion = documentMutationVersion.value;
        clearSaveTimers();
        saving.value = true;
        errorText.value = "";
        if (hasDocumentDirty) {
          await ensureEditingLock();
        }
        const payloadDirtyRegions = cloneDirtyRegions(dirtyRegions.value);
        statusText.value = "正在保存审核内容...";
        try {
          const sharedSaved = await saveSubstation110kvIfNeeded();
          if (!sharedSaved) {
            return false;
          }
          if (!hasDocumentDirty) {
            if (documentMutationVersion.value === payloadVersion) {
              dirty.value = false;
              dirtyRegions.value = emptyDirtyRegions();
            }
            statusText.value = "审核内容已保存";
            return true;
          }
          compactDocumentSectionRows(documentRef.value);
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
        canTransferSectionRowToOtherImportantWork,
        transferSectionRowToOtherImportantWork,
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
        sendCurrentCapacityImage,
        regenerateCurrentHandover,
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
        capacityImageSending,
        regenerating,
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
        capacityImageSendActionBase,
        capacityImageSendActionVm,
        regenerateActionBase,
        regenerateActionVm,
        refreshActionBase,
        refreshActionVm,
        clearSaveTimers,
        saveDocument,
        ensureEditingLock,
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
        sendHandoverReviewCapacityImageApi,
        regenerateHandoverReviewApi,
        buildHandoverReviewDownloadUrl,
        buildHandoverReviewCapacityDownloadUrl,
        triggerBrowserDownload,
      });

      onMounted(async () => {
        if (typeof window !== "undefined") {
          window.addEventListener("beforeunload", handleWindowBeforeUnload);
          window.addEventListener("storage", handleReviewStatusBroadcast);
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
        clearSubstation110kvIdleReleaseTimer();
        if (!substation110kvDirty.value) {
          clearSubstation110kvAutoSaveTimer();
        }
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
          window.removeEventListener("storage", handleReviewStatusBroadcast);
        }
        void releaseCurrentLock();
        if (!substation110kvDirty.value) {
          void releaseSubstation110kvLock();
        }
      });

      return {
        loading,
        saving,
        downloading,
        capacityDownloading,
        capacityImageSending,
        regenerating,
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
        showCapacityImageSendAction,
        showRegenerateAction,
        showConfirmAction,
        showRetryCloudSyncAction,
        showUpdateHistoryCloudSyncAction,
        sessionSummary,
        reviewSaveBadge,
        refreshActionVm,
        saveActionVm,
        downloadActionVm,
        capacityDownloadActionVm,
        capacityImageSendActionVm,
        regenerateActionVm,
        retryCloudSyncActionVm,
        updateHistoryCloudSyncActionVm,
        returnToLatestActionVm,
        reviewCloudSheetVm,
        reviewCloudSheetUrl,
        capacitySync,
        capacityDownloadDisabled,
        substation110kvBlock,
        substation110kvReadonly,
        substation110kvLockedByOther,
        substation110kvMetaText,
        substation110kvLockText,
        coolingPumpPressureRows,
        coolingTankRows,
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
        canTransferSectionRowToOtherImportantWork,
        transferSectionRowToOtherImportantWork,
        updateFooterCell,
        addFooterRow,
        removeFooterRow,
        ensureSubstation110kvLock,
        updateSubstation110kvCell,
        pasteSubstation110kvTable,
        updateCoolingPumpPressure,
        updateCoolingTowerLevel,
        updateCoolingTankValue,
        saveCurrentReview,
        toggleConfirm,
        retryCloudSheetSync: () => retryCloudSheetSync(getJobApi),
        downloadCurrentReviewFile,
        downloadCurrentCapacityReviewFile,
        sendCurrentCapacityImage: () => sendCurrentCapacityImage(getJobApi),
        regenerateCurrentReview: () => regenerateCurrentHandover(getJobApi),
        refreshData,
      };
    },
    template: HANDOVER_REVIEW_TEMPLATE,
  }).mount("#app");
}



