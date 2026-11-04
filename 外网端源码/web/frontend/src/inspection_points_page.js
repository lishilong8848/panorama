import {
  getBuildingInspectionLatestPointsApi,
  getBuildingInspectionPointSettingsApi,
  putBuildingInspectionPointSettingsApi,
} from "./api_client.js";

const ROUTE = /^\/handover\/review\/(d|e)\/inspection-points\/?$/;
const FAMILIES = [
  { key: "electrical_inspection_family", label: "电气" },
  { key: "hvac_inspection_family", label: "暖通" },
];
const POINT_ID = /^[0-9]+(?:_[0-9]+){1,15}$/;
const ROUND_FIELDS = [
  { key: "day_1", label: "白1位置" }, { key: "day_2", label: "白2位置" },
  { key: "night_1", label: "夜1位置" }, { key: "night_2", label: "夜2位置" },
];
const COLD_SHEET = "暖通冷机系统巡检表";
const emptyRoundCells = () => Object.fromEntries(ROUND_FIELDS.map(item => [item.key, ""]));
const PAGE_SIZE = 30;

export function isInspectionPointsPage(pathname) {
  return ROUTE.test(pathname);
}

const TEMPLATE = `
  <main class="review-shell inspection-points-page">
    <header class="review-header">
      <div class="review-header-main">
        <div class="review-header-copy"><h1 class="review-title">{{ building }}巡检测点</h1></div>
        <div class="review-header-actions">
          <button class="btn btn-secondary btn-mini" type="button" :disabled="latestBusy" @click="loadLatest">{{ latestBusy ? '刷新中…' : '刷新最新值' }}</button>
          <a class="btn btn-secondary btn-mini" :href="returnUrl" @click="returnToReview">返回审核页</a>
        </div>
      </div>
    </header>

    <section class="inspection-points-content" aria-label="巡检测点">
      <div class="inspection-points-toolbar">
        <div class="btn-line" aria-label="巡检专业">
          <button v-for="item in families" :key="item.key" type="button" class="btn btn-mini"
            :class="family === item.key ? 'btn-primary' : 'btn-secondary'" :aria-pressed="family === item.key"
            :disabled="saving" @click="selectFamily(item.key)">{{ item.label }}</button>
        </div>
        <input class="review-input inspection-points-search" type="search" v-model="keyword" @input="page = 1"
          aria-label="搜索测点" placeholder="搜索测点名称、ID 或备注" />
        <button class="btn btn-primary btn-mini" type="button" :disabled="!settingsReady || saving" @click="editPoint()">添加测点</button>
      </div>
      <div class="inspection-points-summary">
        <span>测点 {{ filteredRows.length }} 项</span><span>最近采样：{{ sampleTime || '暂无采样' }}</span>
        <button v-if="settingsError" class="btn btn-secondary btn-mini" type="button" :disabled="settingsBusy || saving" @click="loadSettings">重新读取设置</button>
      </div>
      <p v-if="settingsError || latestError" role="alert" class="review-status-line review-status-danger">{{ settingsError || latestError }}</p>
      <p v-if="message" role="status" class="review-status-line review-status-info">{{ message }}</p>
      <form v-if="draft" class="inspection-point-form" @submit.prevent="saveDraft">
        <label class="review-field"><span class="review-field-label">测点 ID</span>
          <input class="review-input" v-model.trim="draft.id" :disabled="saving" required maxlength="100" />
        </label>
        <label class="review-field"><span class="review-field-label">测点备注</span>
          <input class="review-input" v-model="draft.note" :disabled="saving" maxlength="500" />
        </label>
        <label class="review-field"><span class="review-field-label">目标巡检表</span>
          <select class="review-input" v-model="draft.target.workbook" :disabled="saving" @change="draft.target.sheet = ''; draft.target.cell = ''; draft.target.round_cells = emptyRoundCells()">
            <option value="">不指定</option><option v-for="book in Object.keys(targetOptions)" :key="book" :value="book">{{ book }}</option>
          </select>
        </label>
        <label class="review-field"><span class="review-field-label">目标工作表</span>
          <select class="review-input" v-model="draft.target.sheet" :disabled="saving || !draft.target.workbook" @change="draft.target.cell = ''; draft.target.round_cells = emptyRoundCells()">
            <option value="">请选择</option><option v-for="sheet in (targetOptions[draft.target.workbook] || [])" :key="sheet">{{ sheet }}</option>
          </select>
        </label>
        <label v-if="draft.target.sheet === coldSheet" class="review-field"><span class="review-field-label">固定位置</span>
          <input class="review-input" v-model.trim="draft.target.round_cells.day_1" :disabled="saving || !draft.target.workbook" placeholder="B6" maxlength="10" />
        </label>
        <label v-for="round in (draft.target.sheet === coldSheet ? [] : roundFields)" :key="round.key" class="review-field"><span class="review-field-label">{{ round.label }}</span>
          <input class="review-input" v-model.trim="draft.target.round_cells[round.key]" :disabled="saving || !draft.target.workbook" placeholder="B6" maxlength="10" />
        </label>
        <div class="btn-line">
          <button class="btn btn-primary btn-mini" type="submit" :disabled="saving || !draft.id">{{ saving ? '保存中…' : '保存测点' }}</button>
          <button class="btn btn-secondary btn-mini" type="button" :disabled="saving" @click="draft = null">取消</button>
        </div>
      </form>

      <div v-if="!pageRows.length" class="review-empty-card">{{ latestBusy ? '正在读取最近采样…' : (keyword ? '没有匹配的测点' : '暂无测点') }}</div>
      <div v-else class="inspection-point-list">
        <div class="inspection-point-row inspection-point-columns" aria-hidden="true"><span>测点</span><span>备注</span><span>最新值</span><span>操作</span></div>
        <div v-for="row in pageRows" :key="row.id" class="inspection-point-row" :class="{'inspection-point-excluded': row.excluded}">
          <div class="inspection-point-name"><strong>{{ row.name || row.id }}</strong>
            <small>{{ row.id }}<template v-if="row.excluded"> · 已排除</template></small>
            <small v-if="row.path">{{ row.path }}</small>
          </div>
          <div class="inspection-point-note">{{ row.note || '—' }}<small v-if="row.target">{{ row.target.workbook }} · {{ row.target.sheet }} · {{ formatTarget(row.target) }}</small></div>
          <div class="inspection-point-value"><strong>{{ row.value || '—' }}</strong><small>{{ row.sampleTime || '暂无采样' }}</small></div>
          <div class="inspection-point-actions">
            <button v-if="!row.excluded" class="btn btn-secondary btn-mini" type="button" :disabled="!settingsReady || saving" @click="editPoint(row)">修改</button>
            <button v-if="!row.excluded" class="btn btn-ghost btn-mini" type="button" :disabled="!settingsReady || saving" @click="setExcluded(row, true)">删除</button>
            <button v-else class="btn btn-secondary btn-mini" type="button" :disabled="!settingsReady || saving" @click="setExcluded(row, false)">恢复查询</button>
          </div>
        </div>
      </div>
      <div v-if="pageCount > 1" class="inspection-points-pagination btn-line">
        <button class="btn btn-secondary btn-mini" type="button" :disabled="page <= 1" @click="page--">上一页</button>
        <span>{{ page }} / {{ pageCount }}</span>
        <button class="btn btn-secondary btn-mini" type="button" :disabled="page >= pageCount" @click="page++">下一页</button>
      </div>
    </section>
  </main>
`;

export function mountInspectionPointsPage(Vue) {
  const { createApp, computed, onMounted, onBeforeUnmount, ref } = Vue;
  const code = ROUTE.exec(window.location.pathname)[1];
  const building = `${code.toUpperCase()}楼`;
  const query = new URLSearchParams(window.location.search);
  const fromReview = query.get("from_review") === "1";
  query.delete("from_review");
  const returnUrl = `/handover/review/${code}${query.size ? `?${query}` : ""}`;
  document.title = `${building}巡检测点`;

  return createApp({
    template: TEMPLATE,
    setup() {
      const family = ref(FAMILIES[0].key), keyword = ref(""), page = ref(1);
      const settings = ref({}), datasets = ref({}), draft = ref(null), targetOptions = ref({});
      const latestBusy = ref(false), settingsBusy = ref(false), settingsReady = ref(false), saving = ref(false);
      const latestError = ref(""), settingsError = ref(""), message = ref("");
      const controller = new AbortController();
      const group = computed(() => settings.value[family.value] || { points: {}, remove: [] });
      const liveSampleTime = computed(() => datasets.value[family.value]?.sample_time || "");
      const sampleTime = computed(() => {
        const baselineTime = Object.values(group.value.baseline || {})[0]?.sampleTime || "";
        return liveSampleTime.value >= baselineTime ? liveSampleTime.value : baselineTime;
      });
      const filteredRows = computed(() => {
        const latest = datasets.value[family.value]?.points || [];
        const { baseline, points, remove } = group.value;
        const byId = new Map(Object.entries(baseline || {}).map(([id, point]) => [id, { id, ...point }]));
        for (const point of latest) {
          if ((!baseline || Object.hasOwn(baseline, point.id) || Object.hasOwn(points, point.id) || remove.includes(point.id))
              && liveSampleTime.value >= (byId.get(point.id)?.sampleTime || "")) {
            byId.set(point.id, { ...point, sampleTime: liveSampleTime.value });
          }
        }
        for (const id of [...Object.keys(points), ...remove]) {
          if (!byId.has(id)) byId.set(id, { id });
        }
        const q = keyword.value.trim().toLowerCase();
        return [...byId.values()].map(row => ({
          ...row, note: group.value.points[row.id] || "", target: group.value.targets?.[row.id], excluded: group.value.remove.includes(row.id),
        })).filter(row => !q || [row.id, row.name, row.path, row.note].some(value => String(value || "").toLowerCase().includes(q)));
      });
      const pageCount = computed(() => Math.max(1, Math.ceil(filteredRows.value.length / PAGE_SIZE)));
      const pageRows = computed(() => filteredRows.value.slice((page.value - 1) * PAGE_SIZE, page.value * PAGE_SIZE));

      async function loadSettings() {
        if (settingsBusy.value || saving.value) return;
        settingsBusy.value = true;
        settingsError.value = "";
        try {
          const response = await getBuildingInspectionPointSettingsApi(code, { signal: controller.signal });
          settings.value = response.settings;
          targetOptions.value = response.target_options || {};
          settingsReady.value = true;
        } catch (error) {
          if (!controller.signal.aborted) settingsError.value = error.message;
        } finally {
          settingsBusy.value = false;
        }
      }
      async function loadLatest() {
        if (latestBusy.value) return;
        latestBusy.value = true;
        latestError.value = "";
        try {
          const response = await getBuildingInspectionLatestPointsApi(code, { signal: controller.signal });
          datasets.value = response.datasets;
          page.value = Math.min(page.value, pageCount.value);
        } catch (error) {
          if (!controller.signal.aborted) latestError.value = `未能刷新采样：${error.message}`;
        } finally {
          latestBusy.value = false;
        }
      }
      function selectFamily(next) {
        if (next === family.value || saving.value || (draft.value && !window.confirm("放弃未保存的测点修改？"))) return;
        family.value = next;
        page.value = 1;
        draft.value = null;
        message.value = "";
      }
      function editPoint(row) {
        if (draft.value && !window.confirm("放弃未保存的测点修改？")) return;
        draft.value = { id: row?.id || "", note: row?.note || "", originalId: row?.id || "",
          target: { workbook: "", sheet: "", cell: "", ...row?.target,
            round_cells: { ...emptyRoundCells(), ...row?.target?.round_cells } } };
        message.value = "";
      }
      function formatTarget(target) {
        const cells = target.round_cells || {};
        const positions = ROUND_FIELDS.map(item => cells[item.key]).filter(Boolean);
        if (target.sheet === COLD_SHEET && positions.length) return `固定位置 ${positions[0]}`;
        if (positions.length) return ROUND_FIELDS.filter(item => cells[item.key]).map(item => `${item.label} ${cells[item.key]}`).join(" · ");
        return target.cell ? `历史位置 ${target.cell}` : "未设置位置";
      }
      async function persist(points, remove, targets) {
        if (!settingsReady.value || saving.value || settingsBusy.value) return false;
        saving.value = true;
        message.value = "";
        settingsError.value = "";
        try {
          const response = await putBuildingInspectionPointSettingsApi(code, {
            family: family.value, points, remove, targets, expected_group: group.value,
          });
          settings.value = response.settings;
          message.value = response.message;
          page.value = Math.min(page.value, pageCount.value);
          return true;
        } catch (error) {
          settingsError.value = error.message;
          return false;
        } finally {
          saving.value = false;
        }
      }
      async function saveDraft() {
        const row = draft.value;
        if (!row) return;
        if (!POINT_ID.test(row.id)) { settingsError.value = "测点 ID 格式无效"; return; }
        const roundCells = { ...row.target.round_cells };
        if (row.target.sheet === COLD_SHEET) {
          for (const item of ROUND_FIELDS) roundCells[item.key] = roundCells.day_1;
        }
        if (row.target.workbook && (!row.target.sheet || (!row.target.cell && !Object.values(roundCells).some(Boolean)))) {
          settingsError.value = "请选择工作表并填写目标单元格";
          return;
        }
        const points = { ...group.value.points }, targets = { ...group.value.targets };
        const remove = group.value.remove.filter(id => id !== row.id);
        if (row.originalId && row.id !== row.originalId) {
          if (Object.hasOwn(points, row.id)) { settingsError.value = "目标测点已有设置，请直接修改该测点"; return; }
          delete points[row.originalId];
          delete targets[row.originalId];
          if (!remove.includes(row.originalId)) remove.push(row.originalId);
        }
        points[row.id] = row.note;
        if (row.target.workbook) targets[row.id] = { ...row.target, round_cells: roundCells };
        else delete targets[row.id];
        if (await persist(points, remove, targets)) {
          draft.value = null;
        }
      }
      async function setExcluded(row, excluded) {
        if (excluded && !window.confirm(`停止查询 ${row.name || row.id}？`)) return;
        const points = { ...group.value.points };
        const targets = { ...group.value.targets };
        if (excluded) { delete points[row.id]; delete targets[row.id]; }
        else if (group.value.baseline && !Object.hasOwn(group.value.baseline, row.id)) points[row.id] = "";
        const remove = group.value.remove.filter(id => id !== row.id);
        if (excluded) remove.push(row.id);
        if (await persist(points, remove, targets) && draft.value?.id === row.id) draft.value = null;
      }
      function returnToReview(event) {
        if (saving.value || (draft.value && !window.confirm("放弃未保存的测点修改？"))) { event.preventDefault(); return; }
        if (fromReview) {
          event.preventDefault();
          window.close();
          window.setTimeout(() => window.location.assign(returnUrl), 100);
        }
      }
      onMounted(() => { void loadSettings(); void loadLatest(); });
      onBeforeUnmount(() => controller.abort());
      return { building, families: FAMILIES, family, keyword, page, pageCount, pageRows, filteredRows, sampleTime,
        roundFields: ROUND_FIELDS, coldSheet: COLD_SHEET, emptyRoundCells, formatTarget,
        draft, targetOptions, settingsReady, settingsBusy, latestBusy, saving, settingsError, latestError, message, returnUrl,
        loadSettings, loadLatest, selectFamily, editPoint, saveDraft, setExcluded, returnToReview };
    },
  }).mount("#app");
}
