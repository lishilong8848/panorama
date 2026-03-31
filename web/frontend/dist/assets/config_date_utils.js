export function todayText() {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

export function formatDateObj(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

export function parseDateText(text) {
  const raw = String(text || "").trim();
  const parts = raw.split("-");
  if (parts.length !== 3) return null;
  const y = Number(parts[0]);
  const m = Number(parts[1]);
  const d = Number(parts[2]);
  if (!Number.isInteger(y) || !Number.isInteger(m) || !Number.isInteger(d)) return null;
  if (m < 1 || m > 12 || d < 1 || d > 31) return null;
  const dt = new Date(y, m - 1, d);
  if (dt.getFullYear() !== y || dt.getMonth() !== m - 1 || dt.getDate() !== d) return null;
  return dt;
}

export function isValidHms(text) {
  return /^\d{2}:\d{2}:\d{2}$/.test(String(text || "").trim());
}

export function normalizeRunTimeText(text) {
  const raw = String(text || "").trim();
  if (/^\d{2}:\d{2}:\d{2}$/.test(raw)) return raw;
  if (/^\d{2}:\d{2}$/.test(raw)) return `${raw}:00`;
  return "";
}

export function normalizeDatetimeLocalToApi(text) {
  const raw = String(text || "").trim();
  if (!raw) return "";
  const m = raw.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return "";
  const sec = m[4] || "00";
  return `${m[1]} ${m[2]}:${m[3]}:${sec}`;
}

export function apiDatetimeToLocal(text) {
  const raw = String(text || "").trim();
  if (!raw) return "";
  const m = raw.match(/^(\d{4}-\d{2}-\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return "";
  return `${m[1]}T${m[2]}:${m[3]}:${m[4]}`;
}

export function expandDateRange(startText, endText) {
  const start = parseDateText(startText);
  const end = parseDateText(endText);
  if (!start || !end) return [];
  if (start > end) return [];
  const out = [];
  const cursor = new Date(start.getTime());
  while (cursor <= end) {
    out.push(formatDateObj(cursor));
    cursor.setDate(cursor.getDate() + 1);
  }
  return out;
}
