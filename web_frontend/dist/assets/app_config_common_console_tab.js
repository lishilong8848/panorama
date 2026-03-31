export const CONFIG_COMMON_CONSOLE_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_console' && showConsoleConfigTab">
  <div class="section-title">控制台</div>
  <div class="form-row"><label><input type="checkbox" v-model="config.web.enabled" /> 启用网页控制台</label></div>
  <div class="form-row"><label class="label">主机地址</label><input type="text" v-model="config.web.host" /></div>
  <div class="form-row"><label class="label">端口</label><input type="number" v-model.number="config.web.port" /></div>
  <div class="form-row"><label><input type="checkbox" v-model="config.web.auto_open_browser" /> 启动后自动打开浏览器</label></div>
  <div class="form-row"><label class="label">日志缓存行数</label><input type="number" v-model.number="config.web.log_buffer_size" /></div>
</div>
`;
