export function normalizeSiteHost(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const candidate = /^https?:\/\//i.test(text) ? text : `http://${text}`;
  try {
    const u = new URL(candidate);
    return String(u.hostname || "").trim();
  } catch (_) {
    const withoutScheme = text.replace(/^https?:\/\//i, "");
    return withoutScheme.split("/")[0].trim();
  }
}

export function normalizeSheetRules(rawRules) {
  const rows = [];
  if (Array.isArray(rawRules)) {
    rawRules.forEach((item) => {
      if (!item || typeof item !== "object") return;
      rows.push({
        sheet_name: String(item.sheet_name || "").trim(),
        table_id: String(item.table_id || "").trim(),
        header_row: Number.parseInt(item.header_row ?? 1, 10) || 1,
      });
    });
  } else if (rawRules && typeof rawRules === "object") {
    Object.entries(rawRules).forEach(([sheetName, value]) => {
      if (value && typeof value === "object") {
        rows.push({
          sheet_name: String(sheetName || "").trim(),
          table_id: String(value.table_id || "").trim(),
          header_row: Number.parseInt(value.header_row ?? 1, 10) || 1,
        });
      } else if (typeof value === "string") {
        const parts = value.split("|").map((x) => x.trim());
        rows.push({
          sheet_name: String(sheetName || "").trim(),
          table_id: String(parts[0] || "").trim(),
          header_row: Number.parseInt(parts[1] || "1", 10) || 1,
        });
      }
    });
  }
  return rows;
}

export function buildSheetRulesObject(rows) {
  const obj = {};
  const seen = new Set();
  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i] || {};
    const sheetName = String(row.sheet_name || "").trim();
    const tableId = String(row.table_id || "").trim();
    const headerRow = Number.parseInt(row.header_row ?? 1, 10);
    const rowNo = i + 1;
    if (!sheetName && !tableId) continue;
    if (!sheetName) throw new Error(`第${rowNo}行的 Sheet 名称不能为空`);
    if (!tableId) throw new Error(`第${rowNo}行的 table_id 不能为空`);
    if (!Number.isInteger(headerRow) || headerRow < 1) {
      throw new Error(`第${rowNo}行的 header_row 必须是大于等于1的整数`);
    }
    const key = sheetName.toLowerCase();
    if (seen.has(key)) throw new Error(`存在重复 Sheet 名称: ${sheetName}`);
    seen.add(key);
    obj[sheetName] = { table_id: tableId, header_row: headerRow };
  }
  if (!Object.keys(obj).length) throw new Error("sheet_rules 不能为空");
  return obj;
}
