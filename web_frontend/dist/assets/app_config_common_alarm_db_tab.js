export const CONFIG_COMMON_ALARM_DB_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_alarm_db'">
              <div class="section-title">告警数据库（公共）</div>
              <div class="hint">本页配置同时用于“交接班告警查询”和“告警多维导出”。</div>
              <div class="form-row"><label class="label">数据库端口</label><input type="number" v-model.number="config.alarm_common_db.port" /></div>
              <div class="form-row"><label class="label">数据库用户</label><input type="text" v-model="config.alarm_common_db.user" /></div>
              <div class="form-row"><label class="label">数据库密码</label><input type="text" v-model="config.alarm_common_db.password" /></div>
              <div class="form-row"><label class="label">数据库名</label><input type="text" v-model="config.alarm_common_db.database" /></div>
              <div class="form-row"><label class="label">表格名规则</label><input type="text" v-model="config.alarm_common_db.table_pattern" placeholder="event_{year}_{month:02d}" /></div>
              <div class="form-row"><label class="label">字符集</label><input type="text" v-model="config.alarm_common_db.charset" /></div>
              <div class="form-row"><label class="label">连接超时（秒）</label><input type="number" v-model.number="config.alarm_common_db.connect_timeout_sec" /></div>
              <div class="form-row"><label class="label">读取超时（秒）</label><input type="number" v-model.number="config.alarm_common_db.read_timeout_sec" /></div>
              <div class="form-row"><label class="label">写入超时（秒）</label><input type="number" v-model.number="config.alarm_common_db.write_timeout_sec" /></div>
              <div class="form-row"><label class="label">时间字段模式</label><input type="text" v-model="config.alarm_common_db.time_field_mode" /></div>
              <div class="form-row"><label class="label">时间字段</label><input type="text" v-model="config.alarm_common_db.time_field" /></div>
              <div class="form-row"><label class="label">masked字段</label><input type="text" v-model="config.alarm_common_db.masked_field" /></div>
              <div class="form-row"><label class="label">is_recover字段</label><input type="text" v-model="config.alarm_common_db.is_recover_field" /></div>
              <div class="form-row"><label class="label">accept_description字段</label><input type="text" v-model="config.alarm_common_db.accept_description_field" /></div>
              <div class="form-row"><label class="label">主机来源</label><input type="text" v-model="config.alarm_common_db.host_source" /></div>
            </div>
`;
