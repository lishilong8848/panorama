export const DASHBOARD_ALARM_RULE_EXPORT_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'alarm_rule_export_upload'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">告警规则附件上传</h3>
              <div class="hint">读取内网端每月导出的 A-E 楼告警规则文件，上传到告警规则多维表，并按楼栋和月份替换同月旧附件。</div>
            </div>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">专项上传卡</div>
                    <h3 class="card-title">上传月度告警规则附件</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">A楼至E楼</span>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label">年份</label>
                    <input type="text" v-model="alarmRuleExportUploadYear" placeholder="2026" />
                  </div>
                  <div class="form-row">
                    <label class="label">月份</label>
                    <select v-model.number="alarmRuleExportUploadMonth">
                      <option v-for="month in 12" :key="'alarm-rule-export-upload-month-' + month" :value="month">{{ month }}月</option>
                    </select>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">写入目标</div>
                  <div class="ops-focus-card-title">tblNyGBGSCnWhWyL：楼栋 / 月份 / 附件</div>
                  <div class="ops-focus-card-meta">外网端不扫描共享目录，只通过内网端 HTTP 文件接口按精确文件名下载已导出的月度文件。</div>
                </div>
                <div class="btn-line" style="flex-wrap:wrap;">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || isActionLocked(actionKeyAlarmRuleExportUploadRun)"
                    @click="runAlarmRuleExportUpload"
                  >
                    {{ isActionLocked(actionKeyAlarmRuleExportUploadRun) ? '提交中...' : '上传附件' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">状态概览</div>
                    <h3 class="card-title">最近告警规则上传任务</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getAlarmRuleExportUploadStatusTone()">
                    {{ getAlarmRuleExportUploadStatusText() }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">上传楼栋</div>
                    <strong class="status-metric-value">{{ getAlarmRuleExportUploadResult().uploaded_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">删除旧记录</div>
                    <strong class="status-metric-value">{{ getAlarmRuleExportUploadResult().deleted_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">月份</div>
                    <strong class="status-metric-value">{{ getAlarmRuleExportUploadResult().month_value || getAlarmRuleExportUploadResult().period || '-' }}</strong>
                  </div>
                </div>
                <div class="hint">目标表：{{ getAlarmRuleExportUploadResult().table_id || 'tblNyGBGSCnWhWyL' }}</div>
                <div class="hint">当前任务：{{ currentJob && currentJob.feature === 'alarm_rule_export_upload' ? (currentJob.job_id || '-') : '-' }}</div>
                <div class="hint" v-if="getAlarmRuleExportUploadResult().delete_warning">旧记录删除警告：{{ getAlarmRuleExportUploadResult().delete_warning }}</div>
              </article>
            </div>
          </div>
        </section>`;
