export const CONFIG_FEATURE_ALARM_EXPORT_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_alarm_export'">
  <div class="section-title">告警信息上传</div>
  <div class="hint">该配置仅用于外网端消费共享告警文件并上传到目标多维表。</div>
  <div class="hint">外网按楼读取当天最新一份告警文件，缺失则回退昨天最新；当前页面不再参与任何告警数据库查询链路。</div>

  <div class="section-title" style="margin-top:14px">目标多维表</div>
  <div class="form-row">
    <label class="label">告警多维 App Token</label>
    <input type="text" v-model="config.alarm_export.feishu.app_token" />
  </div>
  <div class="form-row">
    <label class="label">告警多维 Table ID</label>
    <input type="text" v-model="config.alarm_export.feishu.table_id" />
  </div>

  <div class="section-title" style="margin-top:14px">上传策略</div>
  <div class="form-row">
    <label><input type="checkbox" v-model="config.alarm_export.shared_source_upload.replace_existing_on_full" /> 全量上传前清空旧记录</label>
  </div>
  <div class="hint">该开关只影响“使用共享文件上传60天（全部楼栋）”；选择单楼时始终按覆盖该楼最近 60 天数据的方式处理。</div>

  <div class="section-title" style="margin-top:14px">批量参数</div>
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
  <div class="hint">默认参数适合常规批量写入；只有在目标表记录量明显增大时，才需要调整这些值。</div>
</div>
`;
