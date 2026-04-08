export const CONFIG_COMMON_ALARM_DB_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_alarm_db'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">告警数据库（仅内网交接班）</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">数据库名</div>
        <div class="status-metric-value monospace">{{ config.alarm_common_db.database || '未设置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">端口</div>
        <div class="status-metric-value">{{ config.alarm_common_db.port || '未设置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">主机来源</div>
        <div class="status-metric-value monospace">{{ config.alarm_common_db.host_source || '未设置' }}</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">本页只供当前内网端交接班告警查询链路使用，不再参与外网告警多维上传。</div>
      <div class="hint">字段名称应与数据库真实列名一致；如果数据库结构变更，需要同步更新这里的映射。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">连接参数</div>
      <div class="form-row"><label class="label">数据库端口</label><input type="number" v-model.number="config.alarm_common_db.port" /></div>
      <div class="form-row"><label class="label">数据库用户</label><input type="text" v-model="config.alarm_common_db.user" /></div>
      <div class="form-row"><label class="label">数据库密码</label><input type="text" v-model="config.alarm_common_db.password" /></div>
      <div class="form-row"><label class="label">数据库名</label><input type="text" v-model="config.alarm_common_db.database" /></div>
      <div class="form-row"><label class="label">表名规则</label><input type="text" v-model="config.alarm_common_db.table_pattern" placeholder="event_{year}_{month:02d}" /></div>
      <div class="form-row"><label class="label">字符集</label><input type="text" v-model="config.alarm_common_db.charset" /></div>
      <div class="form-row"><label class="label">主机来源</label><input type="text" v-model="config.alarm_common_db.host_source" /></div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">超时与时间字段</div>
      <div class="form-row"><label class="label">连接超时（秒）</label><input type="number" v-model.number="config.alarm_common_db.connect_timeout_sec" /></div>
      <div class="form-row"><label class="label">读取超时（秒）</label><input type="number" v-model.number="config.alarm_common_db.read_timeout_sec" /></div>
      <div class="form-row"><label class="label">写入超时（秒）</label><input type="number" v-model.number="config.alarm_common_db.write_timeout_sec" /></div>
      <div class="form-row"><label class="label">时间字段模式</label><input type="text" v-model="config.alarm_common_db.time_field_mode" /></div>
      <div class="form-row"><label class="label">时间字段</label><input type="text" v-model="config.alarm_common_db.time_field" /></div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">业务字段映射</div>
      <div class="config-form-grid three-col">
        <div class="form-row"><label class="label">屏蔽标记字段（masked）</label><input type="text" v-model="config.alarm_common_db.masked_field" /></div>
        <div class="form-row"><label class="label">恢复标记字段（is_recover）</label><input type="text" v-model="config.alarm_common_db.is_recover_field" /></div>
        <div class="form-row"><label class="label">受理描述字段（accept_description）</label><input type="text" v-model="config.alarm_common_db.accept_description_field" /></div>
      </div>
      <div class="hint">这些字段会参与交接班未恢复告警统计、受理描述提取和恢复状态判定。</div>
    </div>
  </div>
</div>
`;
