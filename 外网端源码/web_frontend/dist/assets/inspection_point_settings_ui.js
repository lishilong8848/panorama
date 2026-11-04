import { getInspectionPointSettingsApi, putInspectionPointSettingsApi } from "./api_client.js";

export function createInspectionPointSettingsUi(ref) {
  const inspectionPointEditor = ref({
    open: false, busy: false, loaded: false, building: "D楼", family: "electrical_inspection_family",
    settings: {}, points: "", remove: "", status: "", error: false,
  });

  function showInspectionPointGroup() {
    const e = inspectionPointEditor.value;
    const group = e.settings[e.building]?.[e.family] || {};
    e.points = Object.entries(group.points || {}).map(([id, note]) => note ? `${id} | ${note}` : id).join("\n");
    e.remove = (group.remove || []).join("\n");
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
      const points = Object.fromEntries(e.points.split(/\r?\n/).map(line => line.trim()).filter(Boolean).map(line => {
        const [id, ...note] = line.split("|");
        return [id.trim(), note.join("|").trim()];
      }));
      const response = await putInspectionPointSettingsApi({
        building: e.building, family: e.family, points, remove: e.remove.split(/[\s,，]+/).filter(Boolean),
        expected_group: e.settings[e.building]?.[e.family] || { points: {}, remove: [] },
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
