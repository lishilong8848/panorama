export const CONFIG_FEATURE_SHEET_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_sheet'">
              <div class="section-title">5Sheet导入</div>
              <div class="form-row"><label><input type="checkbox" v-model="config.feishu_sheet_import.enabled" /> 启用5Sheet导入</label></div>
              <div class="form-row"><label class="label">目标飞书多维访问凭证</label><input type="text" v-model="config.feishu_sheet_import.app_token" /></div>
              <div class="form-row">
                <label class="label">Sheet映射规则</label>
                <table class="site-table rule-table">
                  <thead><tr><th>Sheet名称</th><th>目标表ID</th><th>表头行号</th><th>操作</th></tr></thead>
                  <tbody>
                    <tr v-for="(rule, idx) in sheetRuleRows" :key="'rule-' + idx">
                      <td><input type="text" v-model="rule.sheet_name" /></td>
                      <td><input type="text" v-model="rule.table_id" /></td>
                      <td><input type="number" min="1" v-model.number="rule.header_row" /></td>
                      <td><button class="btn btn-danger" @click="removeSheetRuleRow(idx)">删除</button></td>
                    </tr>
                  </tbody>
                </table>
                <div class="btn-line" style="margin-top:8px;"><button class="btn btn-secondary" @click="addSheetRuleRow">新增规则</button></div>
              </div>
            </div>

            
`;

