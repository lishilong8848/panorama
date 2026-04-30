export const CONFIG_COMMON_FEISHU_AUTH_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_feishu_auth'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">飞书鉴权</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">应用编号</div>
        <div class="status-metric-value monospace">{{ config.feishu.app_id || '未设置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">重试次数</div>
        <div class="status-metric-value">{{ config.feishu.request_retry_count || 0 }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">重试间隔</div>
        <div class="status-metric-value">{{ config.feishu.request_retry_interval_sec || 0 }} 秒</div>
      </div>
    </div>
    <div class="hint">这里配置全局飞书应用鉴权信息，供多维表读取、上传和消息发送等链路复用。</div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">应用凭证</div>
      <div class="form-row"><label class="label">飞书应用编号</label><input type="text" v-model="config.feishu.app_id" /></div>
      <div class="form-row"><label class="label">飞书应用密钥</label><input type="text" v-model="config.feishu.app_secret" /></div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">重试策略</div>
      <div class="form-row"><label class="label">鉴权重试次数</label><input type="number" v-model.number="config.feishu.request_retry_count" /></div>
      <div class="form-row"><label class="label">鉴权重试间隔（秒）</label><input type="number" v-model.number="config.feishu.request_retry_interval_sec" /></div>
    </div>
  </div>
</div>
`;
