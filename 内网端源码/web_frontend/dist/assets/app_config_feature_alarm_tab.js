export const CONFIG_FEATURE_ALARM_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_alarm'">
              <div class="section-title">告警多维导出</div>
              <div class="form-row"><label><input type="checkbox" v-model="config.alarm_bitable_export.enabled" /> 启用告警多维上传</label></div>
              <div class="form-row"><label><input type="checkbox" v-model="config.alarm_bitable_export.run_with_scheduler" /> 每日调度时自动附带执行</label></div>
              <div class="form-row"><label><input type="checkbox" v-model="config.alarm_bitable_export.manual_button_enabled" /> 显示手动上传按钮</label></div>
              <div class="hint">当前告警多维上传范围固定为：上个月1日00:00:00 至任务执行时刻。</div>
              <div class="form-row"><label class="label">目标App Token</label><input type="text" v-model="config.alarm_bitable_export.feishu.app_token" /></div>
              <div class="form-row"><label class="label">目标表ID</label><input type="text" v-model="config.alarm_bitable_export.feishu.table_id" /></div>
              <div class="form-row"><label><input type="checkbox" v-model="config.alarm_bitable_export.feishu.clear_before_upload" /> 上传前先清空目标表</label></div>

              <div class="section-title" style="margin-top:14px">测试数据库（仅告警导出）</div>
              <div class="hint">开启后仅影响告警多维导出，不影响交接班告警查询。</div>
              <div class="form-row"><label><input type="checkbox" v-model="config.alarm_bitable_export.test_db.enabled" /> 启用测试数据库</label></div>
              <div class="form-row"><label class="label">测试库主机</label><input type="text" v-model="config.alarm_bitable_export.test_db.host" placeholder="127.0.0.1" /></div>
              <div class="form-row"><label class="label">端口</label><input type="number" v-model.number="config.alarm_bitable_export.test_db.port" /></div>
              <div class="form-row"><label class="label">用户</label><input type="text" v-model="config.alarm_bitable_export.test_db.user" /></div>
              <div class="form-row"><label class="label">密码</label><input type="text" v-model="config.alarm_bitable_export.test_db.password" /></div>
              <div class="form-row"><label class="label">数据库名</label><input type="text" v-model="config.alarm_bitable_export.test_db.database" /></div>
              <div class="form-row"><label class="label">表模式</label><input type="text" v-model="config.alarm_bitable_export.test_db.table_mode" placeholder="fixed" /></div>
              <div class="form-row"><label class="label">固定表名</label><input type="text" v-model="config.alarm_bitable_export.test_db.fixed_table" placeholder="event_2026_02" /></div>
              <div class="form-row"><label class="label">测试楼栋名</label><input type="text" v-model="config.alarm_bitable_export.test_db.building_label" placeholder="测试楼栋" /></div>
              <div class="form-row"><label class="label">时间字段模式</label><input type="text" v-model="config.alarm_bitable_export.test_db.time_field_mode" placeholder="auto" /></div>
            </div>

            
`;
