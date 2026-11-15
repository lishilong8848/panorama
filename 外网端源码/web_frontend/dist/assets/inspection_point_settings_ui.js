import { getInspectionPointSettingsApi, putInspectionPointSettingsApi } from "./api_client.js";

const isLocalPoint = id => /^local_[0-9a-f]{32}$/.test(id);

export function createInspectionPointSettingsUi(ref) {
  const inspectionPointEditor = ref({
    open: false, busy: false, loaded: false, building: "D楼", family: "electrical_inspection_family",
    settings: {}, points: "", remove: "", status: "", error: false,
  });

  function showInspectionPointGroup() {
    const e = inspectionPointEditor.value;
    const group = e.settings[e.building]?.[e.family] || {};
    e.points = Object.entries(group.points || {}).filter(([id]) => !isLocalPoint(id))
      .map(([id, note]) => note ? `${id} | ${note}` : id).join("\n");
    e.remove = (group.remove || []).filter(id => !isLocalPoint(id)).join("\n");
  }

  async function toggleInspectionPointEditor() {
    const e = inspectionPointEditor.value;
    if (e.busy) return;
    e.open = !e.open;
    if (!e.open || e.loaded) return;
    e.busy = true;
    try {
      e.settings = (await getInspectionPointSettingsApi()).settings || {};
      showInspectionPointGroup();
      e.loaded = true;
      e.status = "";
    } catch (error) {
      e.status = String(error.message || error);
      e.error = true;
    } finally {
      e.busy = false;
    }
  }

  async function saveInspectionPointGroup() {
    const e = inspectionPointEditor.value;
    if (!e.loaded || e.busy) return;
    e.busy = true;
    try {
      const group = e.settings[e.building]?.[e.family] || { points: {}, remove: [] };
      const points = Object.fromEntries(e.points.split(/\r?\n/).map(line => line.trim()).filter(Boolean).map(line => {
        const [id, ...note] = line.split("|");
        return [id.trim(), note.join("|").trim()];
      }));
      const remove = e.remove.split(/[\s,，]+/).filter(Boolean);
      if ([...Object.keys(points), ...remove].some(isLocalPoint)) {
        throw new Error("固定项和手工项请在楼栋巡检测点页编辑");
      }
      for (const [id, note] of Object.entries(group.points || {})) {
        if (isLocalPoint(id)) points[id] = note;
      }
      const response = await putInspectionPointSettingsApi({
        building: e.building, family: e.family, points, remove,
        expected_group: group,
      });
      e.settings = response.settings || {};
      showInspectionPointGroup();
      e.status = response.message || "已保存";
      e.error = response.synced === false || response.cloud_synced === false;
    } catch (error) {
      e.status = String(error.message || error);
      e.error = true;
    } finally {
      e.busy = false;
    }
  }

  return { inspectionPointEditor, showInspectionPointGroup, toggleInspectionPointEditor, saveInspectionPointGroup };
}
