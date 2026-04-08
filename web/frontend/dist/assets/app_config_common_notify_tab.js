export const CONFIG_COMMON_NOTIFY_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_notify'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">告警通知</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">Webhook 状态</div>
        <div class="status-metric-value">{{ config.notify.enable_webhook ? '已启用' : '未启用' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">关键字</div>
        <div class="status-metric-value monospace">{{ config.notify.keyword || '未设置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">超时时间</div>
        <div class="status-metric-value">{{ config.notify.timeout || 0 }} 秒</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">这里控制全局飞书机器人通知，不负责业务数据上传。</div>
      <div class="hint">关键字用于目标机器人安全校验，应与飞书机器人的设置保持一致。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">通知开关</div>
      <div class="form-row"><label><input type="checkbox" v-model="config.notify.enable_webhook" /> 启用飞书机器人告警</label></div>
      <div class="form-row"><label class="label">飞书机器人回调地址</label><input type="text" v-model="config.notify.feishu_webhook_url" /></div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">请求参数</div>
      <div class="form-row"><label class="label">告警关键字</label><input type="text" v-model="config.notify.keyword" /></div>
      <div class="form-row"><label class="label">请求超时（秒）</label><input type="number" v-model.number="config.notify.timeout" /></div>
    </div>
  </div>
</div>
`;
