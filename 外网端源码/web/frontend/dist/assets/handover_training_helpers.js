export const TRAINING_COLUMNS = [
  { key: "B", label: "培训项目" }, { key: "D", label: "培训时间段" },
  { key: "E", label: "培训方式" }, { key: "F", label: "培训讲师" }, { key: "G", label: "培训参与人" },
];

export function appendTrainingRows(existing = [], incoming = []) {
  const rows = JSON.parse(JSON.stringify(Array.isArray(existing) ? existing : []));
  const seen = new Set(rows.map((row) => row.row_id));
  for (const row of Array.isArray(incoming) ? incoming : []) {
    if (!row?.row_id || seen.has(row.row_id)) continue;
    rows.push(JSON.parse(JSON.stringify(row)));
    seen.add(row.row_id);
  }
  return rows;
}
