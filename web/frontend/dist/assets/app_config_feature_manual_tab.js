export const CONFIG_FEATURE_MANUAL_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_manual'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">手动补传开关</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">功能状态</div>
        <div class="status-metric-value">{{ config.manual_upload_gui.enabled ? '已启用' : '未启用' }}</div>
      </div>
    </div>
    <div class="hint">启用后，控制台会显示手动补传相关入口；关闭后仅保留自动流程与共享桥接主链。</div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">功能开关</div>
      <div class="form-row"><label><input type="checkbox" v-model="config.manual_upload_gui.enabled" /> 启用手动补传功能</label></div>
    </div>
  </div>
</div>
`;
