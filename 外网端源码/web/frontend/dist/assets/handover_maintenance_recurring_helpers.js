export const MAINTENANCE_FIELDS = [
  { key: "maintenance_item", label: "维护总项", column: "B" },
  { key: "maintenance_party", label: "维护执行方", column: "C" },
  { key: "completion", label: "完成情况", column: "D" },
  { key: "executor", label: "执行人", column: "H" },
];

export function maintenanceValuesFromRow(section, row) {
  return Object.fromEntries(MAINTENANCE_FIELDS.map((field) => {
    const column = (section.columns || []).find((item) => String(item.label || "").replace(/\s/g, "") === field.label);
    return [field.key, String(row?.cells?.[column?.key || field.column] || "").trim()];
  }));
}

export function createMaintenanceRecurringHelpers(ctx) {
  const { ref, session, buildingCode, reviewClientId, ensureEditingLock, statusText, isBusy,
    getApi, saveApi, deleteApi } = ctx;
  const maintenanceRecurringLoading = ref(false);
  const maintenanceRecurringBusy = ref(false);
  const maintenanceRecurringError = ref("");
  const maintenanceRecurringRecords = ref([]);
  const drafts = ref({});
  const rowErrors = ref({});
  let bindings = new WeakMap();
  let contextId = "";
  let requestSequence = 0;
  const isMaintenanceSection = (section) => String(section?.name || "").replace(/\s/g, "") === "维护管理";
  const rowKey = (section, row) => `${session.value?.session_id}:${section.name}:${row.row_id}`;
  const itemKey = (text) => String(text || "").replace(/\s/g, "").toLowerCase();

  function recordFor(section, row) {
    if (bindings.has(row)) {
      return maintenanceRecurringRecords.value.find((record) => record.id === bindings.get(row));
    }
    const rowId = String(row.row_id || "");
    const id = rowId.startsWith("maintenance-recurring:") ? rowId.slice("maintenance-recurring:".length) : "";
    const record = id ? maintenanceRecurringRecords.value.find((item) => item.id === id)
      : maintenanceRecurringRecords.value.find((record) =>
      itemKey(record.values.maintenance_item) === itemKey(maintenanceValuesFromRow(section, row).maintenance_item));
    if (record) bindings.set(row, record.id);
    return record;
  }

  function maintenanceRecurringRowMode(section, row) {
    return drafts.value[rowKey(section, row)] ?? recordFor(section, row)?.mode ?? "none";
  }

  function setMaintenanceRecurringRowMode(section, row, mode) {
    if (maintenanceRecurringBusy.value || maintenanceRecurringLoading.value || isBusy()) return;
    if (!["none", "every_shift", "day_only"].includes(mode)) return;
    recordFor(section, row);
    drafts.value = { ...drafts.value, [rowKey(section, row)]: mode };
    rowErrors.value = { ...rowErrors.value, [rowKey(section, row)]: "" };
  }

  function maintenanceRecurringRowError(section, row) {
    return rowErrors.value[rowKey(section, row)] || maintenanceRecurringError.value;
  }

  function maintenanceRecurringCanSave(section, row) {
    return Boolean(recordFor(section, row)) || maintenanceRecurringRowMode(section, row) !== "none";
  }

  async function loadMaintenanceRecurring() {
    if (maintenanceRecurringBusy.value) return;
    const sequence = ++requestSequence;
    maintenanceRecurringLoading.value = true;
    maintenanceRecurringError.value = "";
    try {
      const response = await getApi(buildingCode);
      if (sequence === requestSequence) {
        maintenanceRecurringRecords.value = response.records || [];
        rowErrors.value = {};
      }
    } catch (error) {
      if (sequence === requestSequence) maintenanceRecurringError.value = error.message || "读取常驻记录失败";
    } finally {
      if (sequence === requestSequence) maintenanceRecurringLoading.value = false;
    }
  }

  function hydrateMaintenanceRecurring() {
    const nextId = session.value?.session_id || "";
    if (contextId !== nextId) {
      contextId = nextId;
      drafts.value = {};
      rowErrors.value = {};
      bindings = new WeakMap();
    }
    if (nextId && !maintenanceRecurringBusy.value) return loadMaintenanceRecurring();
  }

  async function saveMaintenanceRecurringRow(section, row) {
    if (!isMaintenanceSection(section) || !session.value?.session_id || maintenanceRecurringBusy.value
      || maintenanceRecurringLoading.value || maintenanceRecurringError.value || isBusy()) return;
    const sessionId = session.value.session_id;
    const key = rowKey(section, row);
    const record = recordFor(section, row);
    const mode = maintenanceRecurringRowMode(section, row);
    const values = maintenanceValuesFromRow(section, row);
    if (!values.maintenance_item && mode !== "none") {
      rowErrors.value = { ...rowErrors.value, [key]: "请先填写本条维护总项" };
      return;
    }
    if (mode === "none" && !record) return;
    if (mode === "none" && !window.confirm(`取消“${record.values.maintenance_item}”的常驻复用？已保存班次不变。`)) {
      const nextDrafts = { ...drafts.value };
      delete nextDrafts[key];
      drafts.value = nextDrafts;
      return;
    }
    maintenanceRecurringBusy.value = true;
    rowErrors.value = { ...rowErrors.value, [key]: "" };
    try {
      if (!await ensureEditingLock() || sessionId !== session.value?.session_id) throw Error("审核班次已切换或正在其他终端编辑，请重试");
      const response = mode === "none"
        ? await deleteApi(buildingCode, record.id, { session_id: sessionId, client_id: reviewClientId, revision: record.revision })
        : await saveApi(buildingCode, {
          session_id: sessionId, client_id: reviewClientId,
          record_id: record?.id || "", revision: record?.revision, mode, values,
        });
      maintenanceRecurringRecords.value = response.records || [];
      if (mode === "none") bindings.set(row, null);
      else {
        const saved = response.record || maintenanceRecurringRecords.value.find((r) => itemKey(r.values.maintenance_item) === itemKey(values.maintenance_item));
        if (saved) bindings.set(row, saved.id);
      }
      const nextDrafts = { ...drafts.value };
      delete nextDrafts[key];
      drafts.value = nextDrafts;
      if (sessionId === session.value?.session_id) statusText.value = mode === "none"
        ? "本条维护已取消常驻，后续不再复用" : "本条维护常驻设置已保存";
    } catch (error) {
      rowErrors.value = { ...rowErrors.value, [key]: error.message || "保存常驻设置失败" };
    } finally {
      maintenanceRecurringBusy.value = false;
    }
  }

  return {
    maintenanceRecurringLoading, maintenanceRecurringBusy, maintenanceRecurringError,
    isMaintenanceSection, loadMaintenanceRecurring, hydrateMaintenanceRecurring,
    maintenanceRecurringRowMode, setMaintenanceRecurringRowMode, maintenanceRecurringRowError,
    maintenanceRecurringCanSave, saveMaintenanceRecurringRow,
  };
}
