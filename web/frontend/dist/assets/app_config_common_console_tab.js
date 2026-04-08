export const CONFIG_COMMON_CONSOLE_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_console' && showConsoleConfigTab" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">控制台</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">控制台状态</div>
        <div class="status-metric-value">{{ config.web.enabled ? '已启用' : '未启用' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">监听地址</div>
        <div class="status-metric-value monospace">{{ config.web.host || '未设置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">端口</div>
        <div class="status-metric-value">{{ config.web.port || '未设置' }}</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">控制台用于承载状态总览、业务控制台、配置中心和审核页面。</div>
      <div class="hint">自动打开浏览器只影响本机启动体验，不影响监听地址和网络访问策略。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">基础设置</div>
      <div class="form-row"><label><input type="checkbox" v-model="config.web.enabled" /> 启用网页控制台</label></div>
      <div class="form-row"><label class="label">主机地址</label><input type="text" v-model="config.web.host" /></div>
      <div class="form-row"><label class="label">端口</label><input type="number" v-model.number="config.web.port" /></div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">启动体验</div>
      <div class="form-row"><label><input type="checkbox" v-model="config.web.auto_open_browser" /> 启动后自动打开浏览器</label></div>
      <div class="form-row"><label class="label">日志缓存行数</label><input type="number" v-model.number="config.web.log_buffer_size" /></div>
      <div class="hint">日志缓存越大，页面能查看的历史日志越多，但内存占用也会随之增加。</div>
    </div>
  </div>
</div>
`;
