export const inspectionLiveSettings = {
  data: () => ({
    settings: null, busy: false, message: "",
  }),
  methods: {
    async request(save = false) {
      if (this.busy) return;
      this.busy = true;
      this.message = "";
      try {
        const response = await fetch("/api/scheduler/inspection-data-upload/live-settings", {
          method: save ? "PUT" : "GET",
          cache: "no-store",
          headers: { "X-Inspection-Console": "1", ...(save ? { "Content-Type": "application/json" } : {}) },
          ...(save ? { body: JSON.stringify({ enabled: this.settings.enabled }) } : {}),
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || "暂时无法读取或保存设置");
        this.settings = data;
        if (save) this.message = "联动设置已保存。";
      } catch (error) {
        this.message = error.message;
      } finally {
        this.busy = false;
      }
    },
    toggle(event) {
      if (event.currentTarget.open) this.request();
    },
  },
  template: `
    <details class="form-row" @toggle="toggle">
      <summary>灯塔巡检联动</summary>
      <template v-if="settings">
        <label><input type="checkbox" v-model="settings.enabled" :disabled="busy"> 启用实时采集联动</label>
        <div class="btn-line">
          <button class="btn btn-primary" type="button" :disabled="busy" @click="request(true)">保存联动设置</button>
        </div>
        <small class="hint">灯塔通过外网端的局域网地址发起巡检采集。</small>
      </template>
      <button v-else-if="!busy" class="btn btn-ghost" type="button" @click="request()">读取联动设置</button>
      <div v-if="busy || message" class="hint" role="status">{{ busy ? '处理中…' : message }}</div>
    </details>`,
};
