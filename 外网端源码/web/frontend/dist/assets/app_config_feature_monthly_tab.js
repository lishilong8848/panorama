export const CONFIG_FEATURE_MONTHLY_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_monthly'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">月报流程</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">时间窗模式</div>
        <div class="status-metric-value">{{ config.download.time_range_mode === 'yesterday_to_today_start' ? '按天' : (config.download.time_range_mode === 'last_month_to_this_month_start' ? '按月' : '自定义') }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">飞书上传</div>
        <div class="status-metric-value">{{ config.feishu.enable_upload ? '已启用' : '未启用' }}</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">这里配置月报业务时间窗以及飞书月报上传目标。</div>
      <div class="hint">自定义时间窗只在手动运行或特定补采场景下使用，常规月报建议保持按天或按月。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">时间窗</div>
      <div class="form-row">
        <label class="label">时间窗模式</label>
        <select v-model="config.download.time_range_mode">
          <option value="yesterday_to_today_start">按天（昨天 00:00:00 到今天 00:00:00）</option>
          <option value="last_month_to_this_month_start">按月（上月 1 号 00:00:00 到本月 1 号 00:00:00）</option>
          <option value="custom">自定义时间</option>
        </select>
      </div>
      <div class="form-row" v-if="config.download.time_range_mode === 'custom'">
        <label class="label">自定义模式</label>
        <select v-model="config.download.custom_window_mode">
          <option value="absolute">固定绝对时间段</option>
          <option value="daily_relative">每日相对时间段</option>
        </select>
      </div>
      <template v-if="config.download.time_range_mode === 'custom' && config.download.custom_window_mode === 'absolute'">
        <div class="form-row"><label class="label">绝对开始时间</label><input type="datetime-local" step="1" v-model="customAbsoluteStartLocal" /></div>
        <div class="form-row"><label class="label">绝对结束时间</label><input type="datetime-local" step="1" v-model="customAbsoluteEndLocal" /></div>
      </template>
      <template v-if="config.download.time_range_mode === 'custom' && config.download.custom_window_mode === 'daily_relative'">
        <div class="form-row"><label class="label">每日开始时间</label><input type="text" v-model="config.download.daily_custom_window.start_time" /></div>
        <div class="form-row"><label class="label">每日结束时间</label><input type="text" v-model="config.download.daily_custom_window.end_time" /></div>
        <div class="form-row"><label><input type="checkbox" v-model="config.download.daily_custom_window.cross_day" /> 跨天区间</label></div>
      </template>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">飞书月报上传</div>
      <div class="config-form-grid two-col">
        <div class="form-row"><label><input type="checkbox" v-model="config.feishu.enable_upload" /> 启用月报上传</label></div>
        <div></div>
        <div class="form-row"><label class="label">飞书多维访问凭证</label><input type="text" v-model="config.feishu.app_token" /></div>
        <div class="form-row"><label class="label">数据表编号</label><input type="text" v-model="config.feishu.calc_table_id" /></div>
        <div class="form-row"><label class="label">附件表编号</label><input type="text" v-model="config.feishu.attachment_table_id" /></div>
      </div>
    </div>
  </div>
</div>
`;
