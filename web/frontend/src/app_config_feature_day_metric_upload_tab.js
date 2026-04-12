export const CONFIG_FEATURE_DAY_METRIC_UPLOAD_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_day_metric_upload'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">12项独立上传</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">目标多维表</div>
        <div class="status-metric-value">{{ dayMetricUploadTarget.configured ? '已配置' : '未配置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">写入方式</div>
        <div class="status-metric-value">同楼同日先删后写</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">本地补录</div>
        <div class="status-metric-value">默认启用</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">本页只维护 12 项独立上传本身的目标多维表和重试参数，不进入交接班审核链路。</div>
      <div class="hint">调度只在业务控制台配置；这里不再提供调度入口、功能开关或白夜班判断。</div>
      <div class="hint">12 项计算口径已内置到程序代码中，这里不再提供类型映射、规则 ID 或修复按钮。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card config-panel-card-wide config-editor-card">
      <div class="section-title">12项目标多维表</div>
      <div class="config-form-grid three-col">
        <div class="form-row">
          <label class="label">多维 App Token</label>
          <input type="text" v-model="config.day_metric_upload.target.source.app_token" />
        </div>
        <div class="form-row">
          <label class="label">多维 Table ID</label>
          <input type="text" v-model="config.day_metric_upload.target.source.table_id" />
        </div>
        <div class="form-row">
          <label class="label">批量写入大小</label>
          <input type="number" min="1" v-model.number="config.day_metric_upload.target.source.create_batch_size" />
        </div>
        <div class="form-row">
          <label class="label">字段：类型</label>
          <input type="text" v-model="config.day_metric_upload.target.fields.type" />
        </div>
        <div class="form-row">
          <label class="label">字段：楼栋</label>
          <input type="text" v-model="config.day_metric_upload.target.fields.building" />
        </div>
        <div class="form-row">
          <label class="label">字段：日期</label>
          <input type="text" v-model="config.day_metric_upload.target.fields.date" />
        </div>
        <div class="form-row">
          <label class="label">字段：数值</label>
          <input type="text" v-model="config.day_metric_upload.target.fields.value" />
        </div>
        <div class="form-row">
          <label class="label">字段：位置/编号</label>
          <input type="text" v-model="config.day_metric_upload.target.fields.position_code" />
        </div>
        <div class="form-row">
          <label class="label">缺失值策略</label>
          <input type="text" v-model="config.day_metric_upload.target.missing_value_policy" />
        </div>
      </div>
      <div class="hint">12 项目标多维表配置已从交接班日志中剥离，后续只在本页维护目标表、字段名和重试参数。</div>
      <div class="hint">固定上传项：总负荷、IT总负荷、室外湿球、冷水系统供水最高温度、蓄水池后备、蓄冷罐后备、供油可用时长、冷通道最高温湿、变压器负载率、UPS负载率、HVDC负载率。</div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">处理参数</div>
      <div class="config-form-grid three-col">
        <div class="form-row">
          <label class="label">基础重试次数</label>
          <input type="number" min="0" v-model.number="config.day_metric_upload.behavior.basic_retry_attempts" />
        </div>
        <div class="form-row">
          <label class="label">基础退避秒数</label>
          <input type="number" min="0" v-model.number="config.day_metric_upload.behavior.basic_retry_backoff_sec" />
        </div>
        <div class="form-row">
          <label class="label">网络重试次数</label>
          <input type="number" min="0" v-model.number="config.day_metric_upload.behavior.network_retry_attempts" />
        </div>
        <div class="form-row">
          <label class="label">网络退避秒数</label>
          <input type="number" min="0" v-model.number="config.day_metric_upload.behavior.network_retry_backoff_sec" />
        </div>
        <div class="form-row">
          <label class="label">触发告警阈值</label>
          <input type="number" min="0" v-model.number="config.day_metric_upload.behavior.alert_after_attempts" />
        </div>
      </div>
      <div class="hint">固定规则：不区分白班或夜班；默认按“同楼栋 + 同日期”先删旧记录，再写入新记录，避免重复累加。</div>
      <div class="hint">本地补录固定为单日期单楼，不再单独提供功能开关。</div>
    </div>
  </div>
</div>
`;
