export const CONFIG_FEATURE_ALARM_EXPORT_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_alarm_export'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">告警信息上传</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">目标多维表</div>
        <div class="status-metric-value">{{ config.alarm_export.feishu.table_id ? '已配置' : '未配置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">全量覆盖策略</div>
        <div class="status-metric-value">{{ config.alarm_export.shared_source_upload.replace_existing_on_full ? '上传前清空旧记录' : '保留后覆盖写入' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">写入批次</div>
        <div class="status-metric-value">{{ config.alarm_export.feishu.create_batch_size || 0 }}</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">本页配置仅用于外网端消费共享告警文件并上传到目标多维表。</div>
      <div class="hint">外网按楼读取当天最新告警共享文件；缺失时回退到上一份可用文件，不再参与任何告警数据库查询链路。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">目标多维表</div>
      <div class="form-row">
        <label class="label">告警多维应用 Token</label>
        <input type="text" v-model="config.alarm_export.feishu.app_token" />
      </div>
      <div class="form-row">
        <label class="label">告警多维数据表 ID</label>
        <input type="text" v-model="config.alarm_export.feishu.table_id" />
      </div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">上传策略</div>
      <div class="form-row">
        <label><input type="checkbox" v-model="config.alarm_export.shared_source_upload.replace_existing_on_full" /> 使用共享文件上传 60 天（全部楼栋）前先清空旧记录</label>
      </div>
      <div class="hint">这个开关只影响“全部楼栋”场景；选择单楼上传时始终覆盖该楼最近 60 天数据。</div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">批量参数</div>
      <div class="status-metric-grid-compact">
        <div class="status-metric-card compact">
          <div class="status-metric-label">分页大小</div>
          <div class="status-metric-value">{{ config.alarm_export.feishu.page_size || 0 }}</div>
        </div>
        <div class="status-metric-card compact">
          <div class="status-metric-label">删除批次</div>
          <div class="status-metric-value">{{ config.alarm_export.feishu.delete_batch_size || 0 }}</div>
        </div>
        <div class="status-metric-card compact">
          <div class="status-metric-label">写入批次</div>
          <div class="status-metric-value">{{ config.alarm_export.feishu.create_batch_size || 0 }}</div>
        </div>
      </div>
      <div class="config-form-grid three-col">
        <div class="form-row">
          <label class="label">清表分页大小</label>
          <input type="number" min="1" v-model.number="config.alarm_export.feishu.page_size" />
        </div>
        <div class="form-row">
          <label class="label">清表删除批次</label>
          <input type="number" min="1" v-model.number="config.alarm_export.feishu.delete_batch_size" />
        </div>
        <div class="form-row">
          <label class="label">写入批次大小</label>
          <input type="number" min="1" v-model.number="config.alarm_export.feishu.create_batch_size" />
        </div>
      </div>
      <div class="hint">默认参数适合常规批量写入；只有目标表记录量明显增大时，才需要调整这些值。</div>
    </div>
  </div>
</div>
`;
