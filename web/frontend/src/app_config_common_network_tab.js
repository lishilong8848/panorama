export const CONFIG_COMMON_NETWORK_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_network' && showNetworkConfigTab">
  <div class="section-title">网络切换</div>
  <div class="hint">该页仅在“单机切网端”显示。</div>
  <div class="hint">当前版本不再提供“是否切网”开关；单机切网端会固定按切网流程执行。</div>

  <div class="form-row"><label class="label">内网 SSID</label><input type="text" v-model="config.network.internal_ssid" /></div>
  <div class="form-row"><label class="label">外网 SSID</label><input type="text" v-model="config.network.external_ssid" /></div>
  <div class="form-row"><label class="label">切网超时（秒）</label><input type="number" v-model.number="config.network.switch_timeout_sec" /></div>
  <div class="form-row"><label class="label">切网重试次数</label><input type="number" v-model.number="config.network.retry_count" /></div>
  <div class="form-row"><label class="label">切网轮询间隔（秒）</label><input type="number" step="0.1" v-model.number="config.network.connect_poll_interval_sec" /></div>
  <div class="form-row"><label class="label">切网后稳定等待（秒）</label><input type="number" step="0.1" v-model.number="config.network.post_switch_stabilize_sec" /></div>
  <div class="form-row"><label><input type="checkbox" v-model="config.network.post_switch_probe_enabled" /> 启用切网后连通性探测</label></div>
  <div class="form-row"><label class="label">内网探测目标（host[:port]）</label><input type="text" v-model="config.network.post_switch_probe_internal_host" /></div>
  <div class="form-row"><label class="label">外网探测目标（host[:port]）</label><input type="text" v-model="config.network.post_switch_probe_external_host" /></div>
  <div class="form-row"><label class="label">探测超时（秒）</label><input type="number" step="0.1" v-model.number="config.network.post_switch_probe_timeout_sec" /></div>
  <div class="form-row"><label class="label">探测重试次数</label><input type="number" v-model.number="config.network.post_switch_probe_retries" /></div>
  <div class="form-row"><label class="label">探测重试间隔（秒）</label><input type="number" step="0.1" v-model.number="config.network.post_switch_probe_interval_sec" /></div>
  <div class="form-row"><label><input type="checkbox" v-model="config.network.scan_before_connect" /> 连接前主动扫描 WiFi</label></div>
  <div class="form-row"><label class="label">扫描次数</label><input type="number" v-model.number="config.network.scan_attempts" /></div>
  <div class="form-row"><label class="label">扫描等待（秒）</label><input type="number" v-model.number="config.network.scan_wait_sec" /></div>
  <div class="form-row"><label class="label">内网 Profile 名称</label><input type="text" v-model="config.network.internal_profile_name" /></div>
  <div class="form-row"><label class="label">外网 Profile 名称</label><input type="text" v-model="config.network.external_profile_name" /></div>
</div>
`;
