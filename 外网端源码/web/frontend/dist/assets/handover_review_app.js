import {
  buildHandoverReviewCapacityDownloadUrl,
  buildHandoverReviewDownloadUrl,
  claimHandoverReview110kvLockApi,
  claimHandoverReviewLockApi,
  confirmHandoverReviewApi,
  getHandoverEngineerDirectoryApi,
  getJobApi,
  getHandoverReviewApi,
  getHandoverReview110StationStatusApi,
  getHandoverReviewBootstrapApi,
  getHandoverReviewHistoryApi,
  getHandoverReviewStatusApi,
  heartbeatHandoverReview110kvLockApi,
  parseHandoverReview110StationFileApi,
  heartbeatHandoverReviewLockApi,
  markHandoverReview110kvDirtyApi,
  releaseHandoverReview110kvLockApi,
  releaseHandoverReviewLockApi,
  retryHandoverReviewCloudSyncApi,
  refreshHandoverReviewEventSectionsApi,
  saveHandoverReviewApi,
  saveHandoverReview110kvApi,
  sendHandoverReviewCapacityImageApi,
  regenerateHandoverReviewApi,
  retryHandoverReview110StationCloudSyncApi,
  uploadHandoverReview110StationFileApi,
  updateHandoverReviewCloudSyncApi,
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
const REVIEW_PATH_RE = /^\/handover\/review\/([a-e]|110)\/?$/i;
const DEFAULT_POLL_INTERVAL_MS = 5000;
const REVIEW_LOCK_HEARTBEAT_MS = 15000;
const SUBSTATION_110KV_AUTO_SAVE_DEBOUNCE_MS = 350;
const SUBSTATION_110KV_PREVIEW_SYNC_DEBOUNCE_MS = 150;
const SUBSTATION_110KV_LOCK_RELEASE_IDLE_MS = 10000;
const REVIEW_CLIENT_ID_STORAGE_KEY = "handover_review_client_id";
const REVIEW_CLIENT_LABEL_STORAGE_KEY = "handover_review_client_label";
const HANDOVER_REVIEW_STATUS_BROADCAST_KEY = "handover_review_status_broadcast_v1";
const CAPACITY_ROOM_TRACKED_CELLS = [
  "Z69", "AA69", "AC69", "Z79", "AA79", "AC79", "Z89", "AA89", "AC89",
  "Z103", "AA103", "AC103", "Z109", "AA109", "AC109", "Z117", "AA117", "AC117",
  "Z127", "AA127", "AC127", "Z129", "AA129", "AC129", "Z149", "AA149", "AC149",
  "Z169", "AA169", "AC169",
];
const CAPACITY_SYNC_TRACKED_CELLS = [
  "C3", "G3", "B4", "F4", "H6", "F8", "B6", "D6", "F6", "D8", "B7", "D7",
  "B10", "D10", "B15", "D15", "F15", "B13", "D13",
  ...CAPACITY_ROOM_TRACKED_CELLS,
];
const OUTDOOR_TEMPERATURE_CELLS = ["B7", "D7"];
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

function normalizePersonName(value) {
  return String(value ?? "").replace(/\s+/g, "").trim();
}

function splitPersonNames(value) {
  const seen = new Set();
  return String(value ?? "")
    .split(/[、,，;；/\\\s]+/)
    .map((item) => item.trim())
    .filter(Boolean)
    .filter((item) => {
      const key = normalizePersonName(item);
      if (!key || seen.has(key)) return false;
      seen.add(key);
      return true;
    });
}

function joinPersonNames(names) {
  return (Array.isArray(names) ? names : [])
    .map((item) => String(item ?? "").trim())
    .filter(Boolean)
    .join("、");
}

function togglePersonNameValue(value, name, { max = 0 } = {}) {
  const target = String(name ?? "").trim();
  if (!target) return String(value ?? "");
  const targetKey = normalizePersonName(target);
  const names = splitPersonNames(value);
  const existingIndex = names.findIndex((item) => normalizePersonName(item) === targetKey);
  if (existingIndex >= 0) {
    names.splice(existingIndex, 1);
    return joinPersonNames(names);
  }
  names.push(target);
  const capped = max > 0 && names.length > max ? names.slice(names.length - max) : names;
  return joinPersonNames(capped);
}

function hasPersonNameValue(value, name) {
  const target = normalizePersonName(name);
  if (!target) return false;
  return splitPersonNames(value).some((item) => normalizePersonName(item) === target);
}

function normalizeHeaderLabel(value) {
  return String(value ?? "").replace(/\s+/g, "").trim();
}

function syncReviewSelectionToUrl({ sessionId = "", isLatest = false, dutyDate = "", dutyShift = "" } = {}) {
  if (typeof window === "undefined" || !window.history?.replaceState) return;
  const url = new URL(window.location.href);
  url.searchParams.delete("session_id");
  url.searchParams.delete("duty_date");
  url.searchParams.delete("duty_shift");
  if (sessionId && !isLatest) {
    url.searchParams.set("session_id", sessionId);
  } else if (dutyDate && ["day", "night"].includes(String(dutyShift || "").trim().toLowerCase())) {
    url.searchParams.set("duty_date", String(dutyDate || "").trim());
    url.searchParams.set("duty_shift", String(dutyShift || "").trim().toLowerCase());
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

const HANDOVER_REVIEW_110_STATION_TEMPLATE = `
  <div class="review-shell">
    <header class="review-header review-header-sticky">
      <div class="review-header-main">
        <div class="review-header-copy">
          <h1 class="review-title">110站</h1>
          <p class="review-subtitle">{{ batchText }}</p>
        </div>
        <div class="review-header-actions">
          <button class="btn btn-secondary btn-mini" @click="refreshStatus" :disabled="loading || parsing || uploading || retrying">刷新</button>
          <button class="btn btn-warning btn-mini" @click="retryCloudSync" :disabled="!canRetryCloudSync">
            {{ retrying ? "正在重试..." : "重试写入110云文档" }}
          </button>
          <a v-if="cloudUrl" class="btn btn-secondary btn-mini" :href="cloudUrl" target="_blank" rel="noopener noreferrer">打开云文档</a>
        </div>
      </div>
      <div class="review-meta-row review-meta-row-rich">
        <span class="status-badge status-badge-soft tone-info icon-dot">{{ uploadStatusText }}</span>
        <span class="status-badge status-badge-soft" :class="'tone-' + cloudTone">{{ cloudStatusText }}</span>
      </div>
      <div v-if="statusText" class="review-status-line review-status-info">{{ statusText }}</div>
      <div v-if="errorText" class="review-status-line review-status-danger">{{ errorText }}</div>
    </header>

    <section class="review-current-view-section">
      <article class="review-card">
        <div class="review-card-head"><h2>上传110站交接班文件</h2></div>
        <div class="review-fixed-fields review-current-view-fields">
          <label class="review-field">
            <span class="review-field-label">日期</span>
            <input class="review-input" type="date" v-model="dutyDate" :disabled="loading || parsing || uploading || retrying" @change="onStation110DutyChange" />
          </label>
          <label class="review-field">
            <span class="review-field-label">班次</span>
            <select class="review-input" v-model="dutyShift" :disabled="loading || parsing || uploading || retrying" @change="onStation110DutyChange">
              <option value="day">白班</option>
              <option value="night">夜班</option>
            </select>
          </label>
          <label class="review-field review-field-wide">
            <span class="review-field-label">110站 Excel</span>
            <input class="review-input" type="file" accept=".xlsx,.xlsm" :disabled="loading || parsing || uploading || retrying" @change="onFileChange" />
            <small class="review-field-hint">第1个sheet写入云文档“110”页；白班取第2个sheet，夜班取第3个sheet的110KV数据回填各楼审核页。</small>
          </label>
          <button class="btn btn-secondary btn-mini" @click="parseFile" :disabled="!selectedFile || parsing || uploading || retrying">
            {{ parsing ? "正在解析..." : "解析预览" }}
          </button>
          <button class="btn btn-primary btn-mini" @click="uploadFile" :disabled="!selectedFile || parsing || uploading || retrying">
            {{ uploading ? "正在上传..." : "上传并写入云文档" }}
          </button>
        </div>
      </article>
    </section>

    <section v-if="loading" class="review-empty-card">正在加载110站状态...</section>
    <template v-else>
      <section class="review-shared-grid station-110-preview-grid">
        <article class="review-card">
          <div class="review-card-head"><h2>文件状态</h2></div>
          <div class="review-fixed-fields review-current-view-fields">
            <label class="review-field">
              <span class="review-field-label">文件名</span>
              <input class="review-input" :value="upload.original_filename || '-'" readonly />
            </label>
            <label class="review-field">
              <span class="review-field-label">上传时间</span>
              <input class="review-input" :value="upload.uploaded_at || '-'" readonly />
            </label>
            <label class="review-field">
              <span class="review-field-label">第1个sheet识别行数</span>
              <input class="review-input" :value="sourceSheetText" readonly />
            </label>
            <label class="review-field">
              <span class="review-field-label">110KV数据sheet</span>
              <input class="review-input" :value="substationSheetText" readonly />
            </label>
          </div>
        </article>

        <article class="review-card">
          <div class="review-card-head"><h2>110KV识别结果</h2></div>
          <div v-if="!parsedRows.length" class="review-empty-inline">暂无已解析的110KV数据</div>
          <div v-else class="review-table-wrap station-110-table-wrap">
            <table class="review-table review-substation-table station-110-preview-table">
              <thead>
                <tr>
                  <th>进线/主变</th>
                  <th>线电压</th>
                  <th>电流/输出电流</th>
                  <th>当前功率KW</th>
                  <th>功率因数</th>
                  <th>负载率</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="row in parsedRows" :key="row.row_id">
                  <th>{{ row.label }}</th>
                  <td>{{ row.line_voltage || "-" }}</td>
                  <td>{{ row.current || "-" }}</td>
                  <td>{{ row.power_kw || "-" }}</td>
                  <td>{{ row.power_factor || "-" }}</td>
                  <td>{{ row.load_rate || "-" }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </article>
      </section>
    </template>
  </div>
`;

function normalizeOutdoorTemperatureBlock(raw = {}) {
  const rawCells = raw?.cells && typeof raw.cells === "object" ? raw.cells : {};
  const cells = {};
  OUTDOOR_TEMPERATURE_CELLS.forEach((cell) => {
    cells[cell] = String(rawCells[cell] ?? "");
  });
  return {
    block_id: "outdoor_temperature",
    batch_key: String(raw?.batch_key || "").trim(),
    revision: Number.parseInt(String(raw?.revision || 0), 10) || 0,
    updated_at: String(raw?.updated_at || "").trim(),
    updated_by_building: String(raw?.updated_by_building || "").trim(),
    updated_by_client: String(raw?.updated_by_client || "").trim(),
    cells,
    fields: OUTDOOR_TEMPERATURE_CELLS.map((cell) => ({
      cell,
      label: cell === "B7" ? "室外干球温度" : "室外湿球温度",
      value: cells[cell],
    })),
  };
}

function applyOutdoorTemperatureCellsToDocument(document, cells = {}) {
  if (!document || typeof document !== "object" || !Array.isArray(document.fixed_blocks)) return false;
  let changed = false;
  document.fixed_blocks.forEach((block) => {
    if (!block || typeof block !== "object" || !Array.isArray(block.fields)) return;
    block.fields.forEach((field) => {
      if (!field || typeof field !== "object") return;
      const cell = String(field.cell || "").trim().toUpperCase();
      if (!OUTDOOR_TEMPERATURE_CELLS.includes(cell)) return;
      const nextValue = String(cells[cell] ?? "");
      if (String(field.value ?? "") !== nextValue) {
        field.value = nextValue;
        changed = true;
      }
    });
  });
  return changed;
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
  return columns.some((column) => {
    const text = String(row.cells[column.key] || "").trim();
    return Boolean(text && text !== "/");
  });
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

function normalizeCapacityRoomRow(row, index) {
  const room = String(row?.room || `M${index + 1}`).trim() || `M${index + 1}`;
  const rowNumber = Number.parseInt(String(row?.row || 0), 10) || 0;
  return {
    room,
    label: String(row?.label || `${room}包间`).trim() || `${room}包间`,
    row: rowNumber,
    total_cell: String(row?.total_cell || "").trim().toUpperCase(),
    powered_cell: String(row?.powered_cell || "").trim().toUpperCase(),
    aircon_cell: String(row?.aircon_cell || "").trim().toUpperCase(),
    total_cabinets: String(row?.total_cabinets ?? ""),
    powered_cabinets: String(row?.powered_cabinets ?? ""),
    aircon_started: String(row?.aircon_started ?? ""),
  };
}

function normalizeCapacityRoomInputs(raw = {}) {
  const rows = Array.isArray(raw?.rows) ? raw.rows.map(normalizeCapacityRoomRow) : [];
  return {
    title: String(raw?.title || "M1-M6包间机柜与空调启动台数").trim() || "M1-M6包间机柜与空调启动台数",
    rows,
  };
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
    capacity_room_inputs: normalizeCapacityRoomInputs(document?.capacity_room_inputs || {}),
  };
}

function normalizeStation110State(raw = {}) {
  const upload = raw?.upload && typeof raw.upload === "object" ? raw.upload : {};
  const cloudSync = upload.cloud_sync && typeof upload.cloud_sync === "object" ? upload.cloud_sync : {};
  const batch = raw?.batch && typeof raw.batch === "object" ? raw.batch : {};
  const cloudBatch = raw?.cloud_batch && typeof raw.cloud_batch === "object" ? raw.cloud_batch : {};
  return {
    batch: {
      batch_key: String(batch.batch_key || "").trim(),
      duty_date: String(batch.duty_date || "").trim(),
      duty_shift: String(batch.duty_shift || "").trim().toLowerCase(),
      duty_shift_text: String(batch.duty_shift_text || "").trim(),
    },
    upload: {
      status: String(upload.status || "").trim().toLowerCase(),
      error: String(upload.error || "").trim(),
      original_filename: String(upload.original_filename || "").trim(),
      uploaded_at: String(upload.uploaded_at || "").trim(),
      source_sheet: upload.source_sheet && typeof upload.source_sheet === "object" ? upload.source_sheet : {},
      substation_sheet: upload.substation_sheet && typeof upload.substation_sheet === "object" ? upload.substation_sheet : {},
      parsed_110kv_rows: Array.isArray(upload.parsed_110kv_rows) ? upload.parsed_110kv_rows : [],
      cloud_sync: {
        status: String(cloudSync.status || "").trim().toLowerCase(),
        spreadsheet_url: String(cloudSync.spreadsheet_url || "").trim(),
        spreadsheet_title: String(cloudSync.spreadsheet_title || "").trim(),
        sheet_title: String(cloudSync.sheet_title || "110").trim() || "110",
        error: String(cloudSync.error || "").trim(),
        synced_row_count: Number.parseInt(String(cloudSync.synced_row_count || 0), 10) || 0,
        synced_column_count: Number.parseInt(String(cloudSync.synced_column_count || 0), 10) || 0,
        updated_at: String(cloudSync.updated_at || "").trim(),
      },
    },
    cloud_batch: {
      status: String(cloudBatch.status || "").trim().toLowerCase(),
      spreadsheet_url: String(cloudBatch.spreadsheet_url || "").trim(),
      spreadsheet_title: String(cloudBatch.spreadsheet_title || "").trim(),
      error: String(cloudBatch.error || "").trim(),
    },
  };
}

function station110StatusText(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "success") return "已上传";
  if (normalized === "parsed") return "已解析";
  if (normalized === "failed") return "解析失败";
  if (normalized === "pending") return "待同步";
  return "未上传";
}

function station110CloudStatusText(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "success") return "云文档已同步";
  if (normalized === "failed") return "云文档同步失败";
  if (normalized === "pending") return "云文档待同步";
  if (normalized === "skipped") return "云文档已跳过";
  return "云文档未同步";
}

function emptyDirtyRegions() {
  return {
    fixed_blocks: false,
    sections: false,
    footer_inventory: false,
    cooling_pump_pressures: false,
    capacity_room_inputs: false,
  };
}

function cloneDirtyRegions(dirtyRegions) {
  return {
    fixed_blocks: Boolean(dirtyRegions?.fixed_blocks),
    sections: Boolean(dirtyRegions?.sections),
    footer_inventory: Boolean(dirtyRegions?.footer_inventory),
    cooling_pump_pressures: Boolean(dirtyRegions?.cooling_pump_pressures),
    capacity_room_inputs: Boolean(dirtyRegions?.capacity_room_inputs),
  };
}

function normalizeCapacityTrackedCells(rawTrackedCells) {
  const seen = new Set();
  return [
    ...(Array.isArray(rawTrackedCells) ? rawTrackedCells : []),
    ...CAPACITY_SYNC_TRACKED_CELLS,
  ]
    .map((item) => String(item || "").trim().toUpperCase())
    .filter(Boolean)
    .filter((cell) => {
      if (seen.has(cell)) return false;
      seen.add(cell);
      return true;
    });
}

function normalizeCapacitySync(raw) {
  const status = String(raw?.status || "").trim().toLowerCase();
  const normalizedStatus = ["ready", "pending", "pending_input", "missing_file", "failed"].includes(status)
    ? status
    : "failed";
  return {
    status: normalizedStatus,
    updated_at: String(raw?.updated_at || "").trim(),
    error: String(raw?.error || "").trim(),
    tracked_cells: normalizeCapacityTrackedCells(raw?.tracked_cells),
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

function normalizeReviewContext(raw = {}, fallback = {}) {
  const fallbackDutyDate = String(fallback?.dutyDate || fallback?.duty_date || "").trim();
  const fallbackDutyShift = String(fallback?.dutyShift || fallback?.duty_shift || "").trim().toLowerCase();
  const dutyDate = String(raw?.duty_date || fallbackDutyDate || "").trim();
  const dutyShift = String(raw?.duty_shift || fallbackDutyShift || "").trim().toLowerCase();
  const normalizedShift = ["day", "night"].includes(dutyShift) ? dutyShift : "";
  const readyValue = typeof raw?.ready === "boolean" ? raw.ready : Boolean(raw?.status === "ready");
  return {
    status: String(raw?.status || (readyValue ? "ready" : "")).trim(),
    ready: readyValue,
    duty_date: dutyDate,
    duty_shift: normalizedShift,
    duty_shift_text: String(raw?.duty_shift_text || shiftTextFromCode(normalizedShift)).trim(),
    batch_key: String(raw?.batch_key || "").trim(),
    message: String(raw?.message || "").trim(),
  };
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
  const trackedCells = normalizeCapacityTrackedCells(raw?.capacity_sync?.tracked_cells);
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
      capacity_image_send: normalizeDisplayAction(actions.capacity_image_send, { allowed: false, visible: false, label: "发送审核文本和容量表图片", tone: "neutral", variant: "secondary" }),
      regenerate: normalizeDisplayAction(actions.regenerate, { allowed: false, visible: false, label: "重新生成交接班及容量表", tone: "warning", variant: "warning" }),
      confirm: normalizeDisplayAction(actions.confirm, { allowed: false, visible: false, label: "确认并上传本楼云文档", tone: "warning", variant: "warning" }),
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

function mountHandover110StationApp(Vue) {
  const { createApp, ref, computed, onMounted, onBeforeUnmount } = Vue;
  createApp({
    setup() {
      const initialSelection = resolveReviewSelection();
      const hasExplicitDutySelection = Boolean(initialSelection.dutyDate && initialSelection.dutyShift);
      const loading = ref(true);
      const parsing = ref(false);
      const uploading = ref(false);
      const retrying = ref(false);
      const errorText = ref("");
      const statusText = ref("");
      const dutyDate = ref(String(initialSelection.dutyDate || "").trim());
      const dutyShift = ref(["day", "night"].includes(String(initialSelection.dutyShift || "").trim().toLowerCase())
        ? String(initialSelection.dutyShift || "").trim().toLowerCase()
        : "");
      const station110ManualDutySelection = ref(hasExplicitDutySelection);
      const selectedFile = ref(null);
      const state = ref(normalizeStation110State({}));
      let station110RefreshTimer = null;

      const batchText = computed(() => {
        const date = dutyDate.value || state.value.batch.duty_date || "-";
        const shiftValue = dutyShift.value || state.value.batch.duty_shift || "";
        const shift = shiftValue === "night" ? "夜班" : shiftValue === "day" ? "白班" : "自动判断";
        return `${date} ${shift}`;
      });
      const upload = computed(() => state.value.upload || normalizeStation110State({}).upload);
      const cloudUrl = computed(() => upload.value.cloud_sync.spreadsheet_url || state.value.cloud_batch.spreadsheet_url || "");
      const uploadStatusText = computed(() => station110StatusText(upload.value.status));
      const cloudStatusText = computed(() => station110CloudStatusText(upload.value.cloud_sync.status));
      const cloudTone = computed(() => {
        const status = upload.value.cloud_sync.status;
        if (status === "success") return "success";
        if (status === "failed") return "danger";
        if (status === "pending") return "warning";
        return "neutral";
      });
      const canRetryCloudSync = computed(() => Boolean(upload.value.status === "success" && !loading.value && !parsing.value && !uploading.value && !retrying.value));
      const parsedRows = computed(() => Array.isArray(upload.value.parsed_110kv_rows) ? upload.value.parsed_110kv_rows : []);
      const sourceSheetText = computed(() => {
        const sheet = upload.value.source_sheet || {};
        const rowCount = Number.parseInt(String(sheet.recognized_row_count ?? sheet.data_row_count ?? sheet.max_row ?? 0), 10) || 0;
        if (!sheet.title && rowCount <= 0) return "-";
        return `${rowCount}行已识别`;
      });
      const substationSheetText = computed(() => {
        const sheet = upload.value.substation_sheet || {};
        const sheetIndex = Number.parseInt(String(sheet.sheet_index || 0), 10) || 0;
        const title = String(sheet.title || "").trim();
        const rowCount = Number.parseInt(String(sheet.parsed_row_count || 0), 10) || 0;
        if (!title && rowCount <= 0) return "-";
        const prefix = sheetIndex > 0 ? `第${sheetIndex}个sheet` : "目标sheet";
        return `${prefix}${title ? `：${title}` : ""} (${rowCount}行已识别)`;
      });

      function applyState(raw) {
        const normalized = normalizeStation110State(raw || {});
        state.value = normalized;
        if (normalized.batch.duty_date) dutyDate.value = normalized.batch.duty_date;
        if (["day", "night"].includes(normalized.batch.duty_shift)) dutyShift.value = normalized.batch.duty_shift;
      }

      function buildStation110ContextPayload() {
        if (!station110ManualDutySelection.value || !dutyDate.value || !dutyShift.value) {
          return {};
        }
        return {
          duty_date: dutyDate.value,
          duty_shift: dutyShift.value,
        };
      }

      function onStation110DutyChange() {
        station110ManualDutySelection.value = true;
        void refreshStatus();
      }

      function appendStation110Context(form) {
        const context = buildStation110ContextPayload();
        form.append("duty_date", context.duty_date || "");
        form.append("duty_shift", context.duty_shift || "");
      }

      async function refreshStatus({ background = false } = {}) {
        if (!background) {
          loading.value = true;
          errorText.value = "";
        }
        try {
          const response = await getHandoverReview110StationStatusApi({
            ...buildStation110ContextPayload(),
            _t: Date.now(),
          });
          applyState(response);
          if (!background) {
            statusText.value = "110站状态已刷新";
          }
        } catch (error) {
          if (!background) {
            errorText.value = String(error?.message || error || "读取110站状态失败");
          }
        } finally {
          if (!background) {
            loading.value = false;
          }
        }
      }

      function onFileChange(event) {
        selectedFile.value = event?.target?.files?.[0] || null;
      }

      async function uploadFile() {
        if (!selectedFile.value) return;
        uploading.value = true;
        errorText.value = "";
        statusText.value = "正在上传110站文件...";
        try {
          if (!hasExplicitDutySelection) {
            await refreshStatus({ background: true });
          }
          const form = new FormData();
          appendStation110Context(form);
          form.append("file", selectedFile.value);
          const response = await uploadHandoverReview110StationFileApi(form);
          applyState(response);
          if (response?.ok === false) {
            errorText.value = String(response?.error || response?.upload?.error || "110站文件解析失败");
            statusText.value = "";
          } else {
            const cloudStatus = String(response?.upload?.cloud_sync?.status || "").trim().toLowerCase();
            if (cloudStatus === "success") {
              statusText.value = "110站文件已上传，审核页数据和云文档均已同步。";
            } else if (cloudStatus === "failed") {
              statusText.value = "110站文件已上传，审核页数据已回填，云文档同步失败，可重试。";
              errorText.value = String(response?.upload?.cloud_sync?.error || "110站云文档同步失败");
            } else {
              statusText.value = "110站文件已上传，审核页数据已回填，云文档待同步。";
            }
          }
        } catch (error) {
          errorText.value = String(error?.message || error || "110站文件上传失败");
          statusText.value = "";
        } finally {
          uploading.value = false;
        }
      }

      async function parseFile() {
        if (!selectedFile.value) return;
        parsing.value = true;
        errorText.value = "";
        statusText.value = "正在解析110站文件...";
        try {
          if (!hasExplicitDutySelection) {
            await refreshStatus({ background: true });
          }
          const form = new FormData();
          appendStation110Context(form);
          form.append("file", selectedFile.value);
          const response = await parseHandoverReview110StationFileApi(form);
          applyState(response);
          if (response?.ok === false) {
            errorText.value = String(response?.error || response?.upload?.error || "110站文件解析失败");
            statusText.value = "";
          } else {
            const sourceRows = Number.parseInt(
              String(response?.upload?.source_sheet?.recognized_row_count ?? response?.upload?.source_sheet?.max_row ?? 0),
              10,
            ) || 0;
            const kvRows = Number.parseInt(String(response?.upload?.substation_sheet?.parsed_row_count ?? 0), 10) || 0;
            const sheetIndex = Number.parseInt(String(response?.upload?.substation_sheet?.sheet_index ?? 0), 10) || 0;
            const sheetText = sheetIndex > 0 ? `第${sheetIndex}个sheet` : "110KV数据sheet";
            statusText.value = `解析完成：第1个sheet识别${sourceRows}行，${sheetText}识别${kvRows}行。`;
          }
        } catch (error) {
          errorText.value = String(error?.message || error || "110站文件解析失败");
          statusText.value = "";
        } finally {
          parsing.value = false;
        }
      }

      async function retryCloudSync() {
        retrying.value = true;
        errorText.value = "";
        statusText.value = "正在重试110站云文档同步...";
        try {
          const response = await retryHandoverReview110StationCloudSyncApi({
            ...buildStation110ContextPayload(),
          });
          applyState(response);
          if (response?.ok === false) {
            errorText.value = String(response?.error || response?.upload?.cloud_sync?.error || "110站云文档同步失败");
            statusText.value = "";
          } else {
            statusText.value = "110站云文档同步完成。";
          }
        } catch (error) {
          errorText.value = String(error?.message || error || "110站云文档同步失败");
          statusText.value = "";
        } finally {
          retrying.value = false;
        }
      }

      onMounted(() => {
        void refreshStatus();
        if (!hasExplicitDutySelection) {
          station110RefreshTimer = window.setInterval(() => {
            if (!loading.value && !parsing.value && !uploading.value && !retrying.value) {
              void refreshStatus({ background: true });
            }
          }, 60 * 1000);
        }
      });

      onBeforeUnmount(() => {
        if (station110RefreshTimer) {
          window.clearInterval(station110RefreshTimer);
          station110RefreshTimer = null;
        }
      });

      return {
        loading,
        parsing,
        uploading,
        retrying,
        errorText,
        statusText,
        dutyDate,
        dutyShift,
        selectedFile,
        upload,
        batchText,
        cloudUrl,
        uploadStatusText,
        cloudStatusText,
        cloudTone,
        canRetryCloudSync,
        parsedRows,
        sourceSheetText,
        substationSheetText,
        refreshStatus,
        onStation110DutyChange,
        onFileChange,
        parseFile,
        uploadFile,
        retryCloudSync,
      };
    },
    template: HANDOVER_REVIEW_110_STATION_TEMPLATE,
  }).mount("#app");
}

export function mountHandoverReviewApp(Vue) {
  if (resolveReviewBuildingCode() === "110") {
    mountHandover110StationApp(Vue);
    return;
  }
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
      const eventSectionsRefreshing = ref(false);
      const dirty = ref(false);
      const dirtyRegions = ref(emptyDirtyRegions());
      const capacityLinkedDirty = ref(false);
      const needsRefresh = ref(false);
      const errorText = ref("");
      const statusText = ref("");
      const building = ref("");
      const session = ref(null);
      const reviewContext = ref(normalizeReviewContext({}, activeRouteSelection.value));
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
      const documentRevision = ref(0);
      const pollIntervalMs = ref(DEFAULT_POLL_INTERVAL_MS);
      const reviewClientIdentity = ensureReviewClientIdentity();
      const reviewClientId = String(reviewClientIdentity.clientId || "").trim();
      const reviewHolderLabel = String(reviewClientIdentity.holderLabel || "").trim();
      const concurrency = ref(emptyConcurrencyState(0));
      const sharedBlocks = ref({
        outdoor_temperature: normalizeOutdoorTemperatureBlock({}),
        substation_110kv: normalizeSubstation110kvBlock({}),
      });
      const sharedBlockLocks = ref({
        outdoor_temperature: normalizeSharedLockPayload({}, 0),
        substation_110kv: normalizeSharedLockPayload({}, 0),
      });
      const substation110kvDirty = ref(false);
      const substation110kvHeartbeatTimer = ref(null);
      const substation110kvAutoSaveTimer = ref(null);
      const substation110kvPreviewSyncTimer = ref(null);
      const substation110kvIdleReleaseTimer = ref(null);
      const substation110kvDirtyMarked = ref(false);
      const outdoorTemperatureDirty = ref(false);
      const historyLoading = ref(false);
      const historyLoaded = ref(false);
      const historyCacheKey = ref("");
      const engineerDirectoryRows = ref([]);
      const engineerDirectoryLoading = ref(false);
      const engineerDirectoryLoaded = ref(false);
      const engineerDirectoryError = ref("");
      const selectedSectionPeople = ref({});
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
      function stopSubstation110kvLocalEditing(message = "") {
        clearSubstation110kvAutoSaveTimer();
        clearSubstation110kvPreviewSyncTimer();
        clearSubstation110kvIdleReleaseTimer();
        latestSubstation110kvSaveSeq += 1;
        substation110kvLocalVersion += 1;
        substation110kvDirty.value = false;
        substation110kvDirtyMarked.value = false;
        refreshDirtyFlagFromRegions();
        if (message) {
          statusText.value = message;
          errorText.value = "";
        }
        void releaseSubstation110kvLock();
      }
      function hasReviewDocumentDirty() {
        return Boolean(
          dirtyRegions.value.fixed_blocks
          || dirtyRegions.value.sections
          || dirtyRegions.value.footer_inventory
          || dirtyRegions.value.cooling_pump_pressures
          || dirtyRegions.value.capacity_room_inputs,
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
        reviewContext,
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
      const reviewPendingTitle = computed(() => sessionSummary.value || "当前班次");
      const reviewPendingMessage = computed(() => {
        const backendMessage = String(reviewContext.value?.message || "").trim();
        if (backendMessage) return backendMessage;
        if (errorText.value) return errorText.value;
        const dateText = String(reviewContext.value?.duty_date || activeRouteSelection.value.dutyDate || "").trim();
        const shiftText = shiftTextFromCode(reviewContext.value?.duty_shift || activeRouteSelection.value.dutyShift || "");
        const prefix = dateText ? `${dateText} ${shiftText}` : "当前班次";
        return `${prefix}交接班数据尚未生成，请在数据生成后再来查看。`;
      });
      function fixedFieldValue(cellName) {
        const target = String(cellName || "").trim().toUpperCase();
        const blocks = Array.isArray(documentRef.value?.fixed_blocks) ? documentRef.value.fixed_blocks : [];
        for (const block of blocks) {
          const fields = Array.isArray(block?.fields) ? block.fields : [];
          for (const field of fields) {
            if (String(field?.cell || "").trim().toUpperCase() === target) {
              return String(field?.value ?? "").trim();
            }
          }
        }
        return "";
      }

      function personOption(name, source = "") {
        const text = String(name || "").trim();
        if (!text) return null;
        return {
          name: text,
          key: normalizePersonName(text),
          source: String(source || "").trim(),
          label: source ? `${text} · ${source}` : text,
        };
      }

      function dedupePersonOptions(options) {
        const seen = new Set();
        return (Array.isArray(options) ? options : [])
          .filter(Boolean)
          .filter((item) => {
            const key = normalizePersonName(item.name);
            if (!key || seen.has(key)) return false;
            seen.add(key);
            return true;
          });
      }

      const currentDutyPersonOptions = computed(() => splitPersonNames(fixedFieldValue("C3")).map((name) => personOption(name, "值班人")));
      const handoverPersonOptions = computed(() => splitPersonNames(fixedFieldValue("G3")).map((name) => personOption(name, "接班人")));
      const dutyPersonOptions = computed(() => dedupePersonOptions([
        ...currentDutyPersonOptions.value,
        ...handoverPersonOptions.value,
      ]));
      const engineerPersonOptions = computed(() => {
        const rows = Array.isArray(engineerDirectoryRows.value) ? engineerDirectoryRows.value : [];
        const currentBuilding = String(building.value || "").trim();
        const sameBuilding = rows.filter((row) => String(row?.building || "").trim() === currentBuilding);
        const sourceRows = sameBuilding.length ? sameBuilding : rows;
        return dedupePersonOptions(sourceRows.map((row) => {
          const name = String(row?.supervisor || "").trim();
          const specialty = String(row?.specialty || row?.position || "工程师").trim();
          return personOption(name, specialty || "工程师");
        }));
      });
      const sectionPersonOptions = computed(() => dedupePersonOptions([
        ...dutyPersonOptions.value,
        ...engineerPersonOptions.value,
      ]));

      async function ensureEngineerDirectoryLoaded() {
        if (engineerDirectoryLoaded.value || engineerDirectoryLoading.value) return;
        engineerDirectoryLoading.value = true;
        engineerDirectoryError.value = "";
        try {
          const response = await getHandoverEngineerDirectoryApi();
          engineerDirectoryRows.value = Array.isArray(response?.rows) ? response.rows : [];
          engineerDirectoryLoaded.value = true;
        } catch (error) {
          engineerDirectoryError.value = String(error?.message || error || "工程师目录读取失败");
          engineerDirectoryRows.value = [];
        } finally {
          engineerDirectoryLoading.value = false;
        }
      }

      function isEventRefreshSection(section) {
        const name = normalizeHeaderLabel(section?.name);
        return name.includes("新事件处理") || name.includes("历史事件跟进");
      }

      function isSectionPersonColumn(section, column) {
        const label = normalizeHeaderLabel(column?.label || column?.key);
        if (!label || label.includes("执行方")) return false;
        return label.includes("跟进人") || label.includes("执行人") || label.includes("随工人");
      }

      function findSectionPersonColumn(section) {
        const columns = Array.isArray(section?.columns) ? section.columns : [];
        return columns.find((column) => isSectionPersonColumn(section, column)) || null;
      }

      function selectedSectionPersonValues(sectionIndex) {
        const values = selectedSectionPeople.value?.[sectionIndex];
        return Array.isArray(values) ? values : [];
      }

      function updateSectionPersonSelection(sectionIndex, event) {
        const selected = Array.from(event?.target?.selectedOptions || [])
          .map((option) => String(option.value || "").trim())
          .filter(Boolean);
        selectedSectionPeople.value = {
          ...selectedSectionPeople.value,
          [sectionIndex]: selected,
        };
      }

      async function fillSectionPeople(sectionIndex) {
        const section = documentRef.value.sections?.[sectionIndex];
        const column = findSectionPersonColumn(section);
        const selected = selectedSectionPersonValues(sectionIndex);
        if (!section || !column || !selected.length) {
          statusText.value = "请选择要填入的人名";
          return;
        }
        const locked = await ensureEditingLock();
        if (!locked) {
          statusText.value = "当前审核页正在其他终端编辑，请等待或刷新后重试";
          return;
        }
        const rows = Array.isArray(section.rows) ? section.rows : [];
        const value = joinPersonNames(selected);
        let changed = 0;
        for (const row of rows) {
          if (!row || !row.cells || !hasSectionRowContent(row, section.columns)) continue;
          if (String(row.cells[column.key] ?? "") === value) continue;
          row.cells[column.key] = value;
          row.is_placeholder_row = !hasSectionRowContent(row, section.columns);
          changed += 1;
        }
        if (!changed) {
          statusText.value = "当前分类没有可填入的人名行";
          return;
        }
        markDocumentDirty({ region: "sections" });
        statusText.value = `已将${value}填入${section.name}，待保存`;
      }

      function toggleSectionPerson(sectionIndex, rowIndex, columnKey, personName) {
        const section = documentRef.value.sections?.[sectionIndex];
        const row = section?.rows?.[rowIndex];
        if (!section || !row || !row.cells) return;
        const nextValue = togglePersonNameValue(row.cells[columnKey], personName);
        updateSectionCell(sectionIndex, rowIndex, columnKey, nextValue);
      }

      function sectionPersonActive(row, columnKey, personName) {
        return hasPersonNameValue(row?.cells?.[columnKey], personName);
      }

      function isFooterHandoverPersonColumn(column) {
        const key = String(column?.key || "").trim().toUpperCase();
        const label = normalizeHeaderLabel(column?.label || "");
        return key === "H" || label.includes("清点确认人") || label.includes("接班");
      }

      function toggleFooterHandoverPerson(blockIndex, rowIndex, columnKey, personName) {
        const block = documentRef.value.footer_blocks?.[blockIndex];
        const row = block?.rows?.[rowIndex];
        if (!block || block.type !== "inventory_table" || !row || !row.cells) return;
        const nextValue = togglePersonNameValue(row.cells[columnKey], personName, { max: 2 });
        updateFooterCell(blockIndex, rowIndex, columnKey, nextValue);
      }

      function footerPersonActive(row, columnKey, personName) {
        return hasPersonNameValue(row?.cells?.[columnKey], personName);
      }

      function normalizeEventSectionRows(section, rows) {
        const columns = resolveSectionColumns(section);
        const normalizedRows = (Array.isArray(rows) ? rows : []).map((row) => normalizeSectionRow(row, columns));
        const contentRows = normalizedRows.filter((row) => hasSectionRowContent(row, columns));
        return contentRows.length ? contentRows : [blankRow(columns)];
      }

      function applyEventSectionRows(sectionName, rows) {
        const sections = Array.isArray(documentRef.value.sections) ? documentRef.value.sections : [];
        const targetName = String(sectionName || "").trim();
        const sectionIndex = sections.findIndex((section) => String(section?.name || "").trim() === targetName);
        if (sectionIndex < 0) return false;
        const section = sections[sectionIndex];
        section.rows = normalizeEventSectionRows(section, rows);
        return true;
      }

      async function refreshEventSectionFromBitable(sectionName) {
        const targetName = String(sectionName || "").trim();
        if (!session.value?.session_id || !targetName) return;
        if (dirtyRegions.value.sections && typeof window !== "undefined") {
          const confirmed = window.confirm("刷新会覆盖该事件分类当前未保存内容，是否继续？");
          if (!confirmed) return;
        }
        const locked = await ensureEditingLock();
        if (!locked) {
          statusText.value = "当前审核页正在其他终端编辑，请等待或刷新后重试";
          return;
        }
        eventSectionsRefreshing.value = true;
        errorText.value = "";
        statusText.value = `正在刷新${targetName}...`;
        try {
          const response = await refreshHandoverReviewEventSectionsApi(buildingCode, {
            session_id: session.value.session_id,
            client_id: reviewClientId,
            section_name: targetName,
          });
          const sections = response?.sections && typeof response.sections === "object" ? response.sections : {};
          const updated = applyEventSectionRows(targetName, sections[targetName]);
          if (!updated) {
            throw new Error(`未找到${targetName}分类`);
          }
          markDocumentDirty({ region: "sections" });
          statusText.value = `${targetName}已从多维刷新，正在保存...`;
          const saved = await saveDocument({ reason: "event_section_refresh" });
          statusText.value = saved
            ? `${targetName}已从多维刷新并保存`
            : `${targetName}已刷新，但保存未完成，请处理提示后手动保存`;
        } catch (error) {
          errorText.value = String(error?.message || error || "刷新事件分类失败");
          statusText.value = "刷新事件分类失败";
        } finally {
          eventSectionsRefreshing.value = false;
        }
      }

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
          applyConcurrencyState(response?.concurrency, currentDocumentRevision(), sessionId);
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
          applyConcurrencyState(response?.concurrency, currentDocumentRevision(), sessionId);
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
          applyConcurrencyState(null, currentDocumentRevision(), "");
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
          applyConcurrencyState(null, currentDocumentRevision(), "");
          return;
        }
        try {
          const response = await releaseHandoverReviewLockApi(buildingCode, {
            session_id: sessionId,
            client_id: reviewClientId,
          });
          applyConcurrencyState(response?.concurrency, currentDocumentRevision(), "");
        } catch (_error) {
          applyConcurrencyState(null, currentDocumentRevision(), "");
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

      function revisionNumber(value) {
        return Number.parseInt(String(value ?? 0), 10) || 0;
      }

      function resolveDocumentRevision(payload = {}, nextSession = null) {
        const hasDocumentPayload = Object.prototype.hasOwnProperty.call(payload || {}, "document");
        const candidates = [
          payload?.document_revision,
          payload?.snapshot_revision,
          payload?.session?.document_revision,
          nextSession?.document_revision,
        ];
        if (hasDocumentPayload) {
          candidates.push(nextSession?.revision);
        }
        for (const candidate of candidates) {
          const resolved = revisionNumber(candidate);
          if (resolved > 0) return resolved;
        }
        return 0;
      }

      function updateDocumentRevisionFromPayload(payload = {}, nextSession = null) {
        const resolved = resolveDocumentRevision(payload, nextSession);
        if (resolved > 0) {
          documentRevision.value = resolved;
        }
        return documentRevision.value;
      }

      function currentDocumentRevision() {
        return documentRevision.value
          || revisionNumber(session.value?.document_revision)
          || revisionNumber(session.value?.revision);
      }

      function buildStatusParams() {
        const params = buildLoadParams();
        const currentSessionId = String(session.value?.session_id || "").trim();
        const currentRevision = currentDocumentRevision();
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
        if (!selectedId && activeRouteSelection.value?.dutyDate && activeRouteSelection.value?.dutyShift) {
          syncReviewSelectionToUrl({
            dutyDate: activeRouteSelection.value.dutyDate,
            dutyShift: activeRouteSelection.value.dutyShift,
          });
          return;
        }
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
        const hasSessionPayload = Object.prototype.hasOwnProperty.call(payload || {}, "session");
        const nextSession = payload?.session && typeof payload.session === "object" ? cloneDeep(payload.session) : null;
        const nextDocumentRevision = updateDocumentRevisionFromPayload(payload, nextSession);
        if (hasSessionPayload) {
          session.value = nextSession;
          if (!nextSession) {
            documentRevision.value = 0;
          }
        }
        if (payload?.review_context && typeof payload.review_context === "object") {
          reviewContext.value = normalizeReviewContext(payload.review_context, activeRouteSelection.value);
        } else if (nextSession) {
          reviewContext.value = normalizeReviewContext({
            status: "ready",
            ready: true,
            duty_date: nextSession.duty_date,
            duty_shift: nextSession.duty_shift,
            batch_key: nextSession.batch_key,
          }, activeRouteSelection.value);
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
        if (!selectedId && reviewContext.value?.duty_date && reviewContext.value?.duty_shift) {
          const routeHadExplicitDuty = Boolean(activeRouteSelection.value.dutyDate && activeRouteSelection.value.dutyShift);
          activeRouteSelection.value = {
            sessionId: "",
            dutyDate: routeHadExplicitDuty ? reviewContext.value.duty_date : "",
            dutyShift: routeHadExplicitDuty ? reviewContext.value.duty_shift : "",
          };
          syncReviewSelectionToUrl(routeHadExplicitDuty
            ? { dutyDate: reviewContext.value.duty_date, dutyShift: reviewContext.value.duty_shift }
            : {});
        } else {
          activeRouteSelection.value = {
            sessionId: historyState.value?.selected_is_latest ? "" : selectedId,
            dutyDate: "",
            dutyShift: "",
          };
          syncRouteToCurrentSelection(historyState.value);
        }
        applyConcurrencyState(
          payload?.concurrency,
          nextDocumentRevision || nextSession?.document_revision || nextSession?.revision || session.value?.document_revision || session.value?.revision || 0,
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
        if (Object.prototype.hasOwnProperty.call(shared, "outdoor_temperature")) {
          const incomingOutdoor = normalizeOutdoorTemperatureBlock(shared.outdoor_temperature || {});
          const currentOutdoorRevision = Number(sharedBlocks.value?.outdoor_temperature?.revision || 0);
          const incomingOutdoorRevision = Number(incomingOutdoor.revision || 0);
          if (!currentOutdoorRevision || !incomingOutdoorRevision || incomingOutdoorRevision >= currentOutdoorRevision) {
            sharedBlocks.value = {
              ...sharedBlocks.value,
              outdoor_temperature: incomingOutdoor,
            };
            if (!dirtyRegions.value.fixed_blocks && !saving.value && !documentHydrating.value) {
              applyOutdoorTemperatureCellsToDocument(documentRef.value, incomingOutdoor.cells);
            }
          }
          sharedBlockLocks.value = {
            ...sharedBlockLocks.value,
            outdoor_temperature: normalizeSharedLockPayload(
              locks.outdoor_temperature || sharedBlockLocks.value.outdoor_temperature,
              incomingOutdoor.revision,
            ),
          };
        }
        const hasIncomingSubstation = Object.prototype.hasOwnProperty.call(shared, "substation_110kv");
        let incomingBlock = normalizeSubstation110kvBlock(hasIncomingSubstation ? shared.substation_110kv : sharedBlocks.value.substation_110kv);
        const incomingLock = normalizeSharedLockPayload(
          locks.substation_110kv || sharedBlockLocks.value.substation_110kv,
          incomingBlock.revision,
        );
        const currentRevision = Number(sharedBlocks.value?.substation_110kv?.revision || 0);
        const previousSubstationLock = normalizeSharedLockPayload(
          sharedBlockLocks.value.substation_110kv,
          currentRevision,
        );
        const incomingRevision = Number(incomingBlock.revision || 0);
        if (hasIncomingSubstation && currentRevision > 0 && incomingRevision > 0 && incomingRevision < currentRevision) {
          incomingBlock = normalizeSubstation110kvBlock(sharedBlocks.value.substation_110kv || {});
        }
        const serverRevisionChanged = hasIncomingSubstation && incomingRevision !== currentRevision;
        const preserveLocalRows = Boolean(substation110kvDirty.value) && !serverRevisionChanged;
        const localEditingInterrupted = serverRevisionChanged && (Boolean(substation110kvDirty.value) || Boolean(previousSubstationLock?.client_holds_lock));
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
          if (localEditingInterrupted) {
            stopSubstation110kvLocalEditing("110站已将数据修改，当前编辑已停止，请基于最新内容继续。");
          } else if (serverRevisionChanged && !dirty.value && !saving.value) {
            statusText.value = "110KV变电站已同步最新内容";
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

        const hasSessionPayload = Object.prototype.hasOwnProperty.call(payload || {}, "session");
        const incomingSession = payload?.session && typeof payload.session === "object" ? cloneDeep(payload.session) : {};
        const currentSessionId = String(session.value?.session_id || "").trim();
        const incomingSessionId = String(incomingSession.session_id || "").trim();
        const incomingRevision = updateDocumentRevisionFromPayload(payload, incomingSession);
        const currentRevision = currentDocumentRevision();
        if (payload?.review_context && typeof payload.review_context === "object") {
          reviewContext.value = normalizeReviewContext(payload.review_context, activeRouteSelection.value);
        } else if (incomingSessionId) {
          reviewContext.value = normalizeReviewContext({
            status: "ready",
            ready: true,
            duty_date: incomingSession.duty_date,
            duty_shift: incomingSession.duty_shift,
            batch_key: incomingSession.batch_key,
          }, activeRouteSelection.value);
        }

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
        if (hasSessionPayload && !incomingSessionId) {
          session.value = null;
          documentRef.value = normalizeDocument({});
          documentRevision.value = 0;
          dirtyRegions.value = emptyDirtyRegions();
          outdoorTemperatureDirty.value = false;
          dirty.value = false;
          capacityLinkedDirty.value = false;
          staleRevisionConflict.value = false;
          if (reviewContext.value?.duty_date && reviewContext.value?.duty_shift) {
            const routeHadExplicitDuty = Boolean(activeRouteSelection.value.dutyDate && activeRouteSelection.value.dutyShift);
            activeRouteSelection.value = {
              sessionId: "",
              dutyDate: routeHadExplicitDuty ? reviewContext.value.duty_date : "",
              dutyShift: routeHadExplicitDuty ? reviewContext.value.duty_shift : "",
            };
          }
          return;
        }

        if (!session.value) {
          if (incomingSession && Object.keys(incomingSession).length) {
            session.value = incomingSession;
            if (background) {
              statusText.value = "检测到当前班次交接班数据已生成，正在加载审核内容...";
              await loadReviewData({
                background: false,
                mode: shouldPreferBootstrapLoad() ? "bootstrap" : "full",
              });
            }
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
        outdoorTemperatureDirty.value = false;
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

      function applySavedDocumentPayload(payload = {}, payloadVersion = 0) {
        updateDocumentRevisionFromPayload(payload, payload?.session && typeof payload.session === "object" ? payload.session : null);
        if (documentMutationVersion.value !== payloadVersion) {
          return false;
        }
        const rawDocument = payload?.document && typeof payload.document === "object" ? payload.document : null;
        if (!rawDocument) {
          return false;
        }
        documentHydrating.value = true;
        documentRef.value = normalizeDocument(rawDocument);
        outdoorTemperatureDirty.value = false;
        window.setTimeout(() => {
          documentHydrating.value = false;
        }, 0);
        return true;
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

      function updateCapacityRoomInput(rowIndex, key, value) {
        const rows = documentRef.value?.capacity_room_inputs?.rows;
        if (!Array.isArray(rows) || !rows[rowIndex]) return;
        const normalizedKey = String(key || "").trim();
        const cellKeyByValueKey = {
          total_cabinets: "total_cell",
          powered_cabinets: "powered_cell",
          aircon_started: "aircon_cell",
        };
        if (!Object.prototype.hasOwnProperty.call(cellKeyByValueKey, normalizedKey)) return;
        const nextValue = String(value ?? "");
        if (String(rows[rowIndex][normalizedKey] ?? "") === nextValue) return;
        rows[rowIndex][normalizedKey] = nextValue;
        const cellName = String(rows[rowIndex][cellKeyByValueKey[normalizedKey]] || "").trim().toUpperCase();
        markDocumentDirty({ region: "capacity_room_inputs", capacityCell: cellName });
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
            base_revision: currentDocumentRevision(),
            client_id: reviewClientId,
            document: documentRef.value,
            dirty_regions: payloadDirtyRegions,
            shared_outdoor_temperature_dirty: outdoorTemperatureDirty.value,
          });
          applyPayloadMeta(response || {});
          applySavedDocumentPayload(response || {}, payloadVersion);
          broadcastHandoverReviewStatusChange(response || {});
          if (documentMutationVersion.value === payloadVersion) {
            dirtyRegions.value = emptyDirtyRegions();
            capacityLinkedDirty.value = false;
            outdoorTemperatureDirty.value = false;
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
        onFixedFieldChanged: ({ cell }) => {
          if (OUTDOOR_TEMPERATURE_CELLS.includes(String(cell || "").trim().toUpperCase())) {
            outdoorTemperatureDirty.value = true;
          }
        },
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
        reviewContext,
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
        eventSectionsRefreshing,
        cloudSyncBusy,
        dirty,
        needsRefresh,
        errorText,
        statusText,
        building,
        session,
        reviewContext,
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
        reviewPendingTitle,
        reviewPendingMessage,
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
        handoverPersonOptions,
        sectionPersonOptions,
        engineerDirectoryLoading,
        engineerDirectoryError,
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
        isEventRefreshSection,
        refreshEventSectionFromBitable,
        findSectionPersonColumn,
        isSectionPersonColumn,
        selectedSectionPersonValues,
        updateSectionPersonSelection,
        fillSectionPeople,
        toggleSectionPerson,
        sectionPersonActive,
        isFooterHandoverPersonColumn,
        toggleFooterHandoverPerson,
        footerPersonActive,
        ensureEngineerDirectoryLoaded,
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
        updateCapacityRoomInput,
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



