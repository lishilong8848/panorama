export const CONFIG_COMMON_SCHEDULER_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_scheduler'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">调度</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">调度状态</div>
        <div class="status-metric-value">{{ health.scheduler.status || '未启动' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">每日执行时间</div>
        <div class="status-metric-value monospace">{{ config.scheduler.run_time || '未设置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">检查间隔</div>
        <div class="status-metric-value">{{ config.scheduler.check_interval_sec || 0 }} 秒</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">这里只配置全局基础调度参数，具体业务模块仍可在各自模块内设置独立调度。</div>
      <div class="hint">若错过运行时间点，可按补跑策略在下一次检查时补执行。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">调度策略</div>
      <div class="form-row"><label><input type="checkbox" v-model="config.scheduler.catch_up_if_missed" /> 错过时点后补跑</label></div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">基础参数</div>
      <div class="form-row"><label class="label">每日执行时间</label><input type="time" step="1" v-model="config.scheduler.run_time" /></div>
      <div class="form-row"><label class="label">检查间隔（秒）</label><input type="number" v-model.number="config.scheduler.check_interval_sec" /></div>
    </div>
  </div>
</div>
`;
