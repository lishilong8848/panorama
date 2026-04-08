export const CONFIG_FEATURE_SHEET_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_sheet'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">5Sheet 导入</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">功能状态</div>
        <div class="status-metric-value">{{ config.feishu_sheet_import.enabled ? '已启用' : '未启用' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">规则数量</div>
        <div class="status-metric-value">{{ sheetRuleRows.length }}</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">这里维护 5Sheet 文件导入的目标多维表和表头规则。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">基础开关</div>
      <div class="form-row"><label><input type="checkbox" v-model="config.feishu_sheet_import.enabled" /> 启用 5Sheet 导入</label></div>
      <div class="form-row"><label class="label">目标飞书多维访问凭证</label><input type="text" v-model="config.feishu_sheet_import.app_token" /></div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide config-editor-card">
      <div class="section-title">Sheet 映射规则</div>
      <div class="hint">统一使用表格编辑区维护导入规则，新增、删除和表头行号都在这里完成。</div>
      <div class="config-editor-scroll">
        <table class="site-table rule-table config-editor-table">
          <thead>
            <tr>
              <th>Sheet 名称</th>
              <th>目标表 ID</th>
              <th>表头行号</th>
              <th style="width: 90px;">操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(rule, idx) in sheetRuleRows" :key="'rule-' + idx">
              <td><input type="text" v-model="rule.sheet_name" /></td>
              <td><input type="text" v-model="rule.table_id" /></td>
              <td><input type="number" min="1" v-model.number="rule.header_row" /></td>
              <td><button class="btn btn-danger" @click="removeSheetRuleRow(idx)">删除</button></td>
            </tr>
            <tr v-if="!sheetRuleRows.length" class="config-editor-empty-row">
              <td colspan="4" class="hint">暂无导入规则，请点击“新增规则”。</td>
            </tr>
          </tbody>
        </table>
      </div>
      <div class="btn-line"><button class="btn btn-secondary" @click="addSheetRuleRow">新增规则</button></div>
    </div>
  </div>
</div>
`;
