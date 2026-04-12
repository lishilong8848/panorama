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
    </div>
    <div class="btn-line" style="margin-top:10px;">
      <button
        class="btn btn-secondary"
        @click="repairDayMetricUploadConfig"
        :disabled="isActionLocked(actionKeyDayMetricConfigRepair)"
      >
        {{ isActionLocked(actionKeyDayMetricConfigRepair) ? '修复中...' : '修复12项配置' }}
      </button>
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
      <div class="btn-line" style="margin:8px 0;">
        <button class="btn btn-secondary" @click="config.day_metric_upload.target.types.push({ name: '', source: 'cell', cell: '', metric_id: '' })">新增指标类型</button>
      </div>
      <div class="config-editor-scroll">
        <table class="site-table config-editor-table" style="margin-bottom:0;">
          <thead>
            <tr>
              <th style="width:220px;">类型名称</th>
              <th style="width:140px;">来源</th>
              <th style="width:140px;">单元格</th>
              <th style="width:200px;">规则ID(metric_id)</th>
              <th style="width:90px;">操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, idx) in (config.day_metric_upload.target.types || [])" :key="'day-metric-type-' + idx">
              <td><input type="text" v-model="row.name" /></td>
              <td>
                <select v-model="row.source">
                  <option value="cell">cell</option>
                  <option value="metric">metric</option>
                  <option value="cell_percent">cell_percent</option>
                  <option value="cell_min_pair">cell_min_pair</option>
                </select>
              </td>
              <td>
                <input
                  type="text"
                  v-model="row.cell"
                  :disabled="row.source === 'metric'"
                  placeholder="如 D6"
                />
              </td>
              <td>
                <input
                  type="text"
                  v-model="row.metric_id"
                  :disabled="row.source !== 'metric'"
                  placeholder="如 cold_temp_max"
                />
              </td>
              <td><button class="btn btn-danger" @click="config.day_metric_upload.target.types.splice(idx, 1)">删除</button></td>
            </tr>
            <tr v-if="!(config.day_metric_upload.target.types || []).length" class="config-editor-empty-row">
              <td colspan="5" class="hint">暂无指标类型，请点击“新增指标类型”。</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="hint">12 项目标多维表配置已从交接班日志中剥离，后续只在本页维护。</div>
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
