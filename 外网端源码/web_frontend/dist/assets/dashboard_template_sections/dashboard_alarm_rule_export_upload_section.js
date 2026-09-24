export const DASHBOARD_ALARM_RULE_EXPORT_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'alarm_rule_export_upload'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">告警规则附件上传</h3>
              <div class="hint">读取内网端每月导出的 A-E 楼告警规则文件，上传到告警规则多维表，并按楼栋和月份替换同月旧附件。</div>
            </div>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block dashboard-module-scheduler-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">调度卡</div>
                    <h3 class="card-title">月度自动上传</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('alarm_rule_export_upload')">
                    {{ getSchedulerStatusText('alarm_rule_export_upload') }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">下次执行</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('alarm_rule_export_upload', 'next_run_text', '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">最近触发</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('alarm_rule_export_upload', 'last_trigger_text', '-') }}</strong>
                  </div>
                </div>
                <div class="task-grid two-col">
                  <div class="form-row">
                    <label class="label">每月几号</label>
                    <input type="number" min="1" max="31"
                      :value="config.alarm_rule_export_upload.scheduler.day_of_month"
                      :disabled="alarmRuleExportUploadSchedulerQuickSaving"
                      @change="saveAlarmRuleExportUploadSchedulerQuickConfig({ day_of_month: $event.target.value })" />
                  </div>
                  <div class="form-row">
                    <label class="label">执行时间</label>
                    <input type="time" step="1"
                      :value="config.alarm_rule_export_upload.scheduler.run_time"
                      :disabled="alarmRuleExportUploadSchedulerQuickSaving"
                      @change="saveAlarmRuleExportUploadSchedulerQuickConfig({ run_time: $event.target.value })" />
                  </div>
                </div>
                <div class="btn-line">
                  <button class="btn btn-success"
                    :disabled="alarmRuleExportUploadSchedulerQuickSaving || isSchedulerStartDisabled('alarm_rule_export_upload', actionKeyAlarmRuleExportUploadSchedulerStart, actionKeyAlarmRuleExportUploadSchedulerStop)"
                    @click="startAlarmRuleExportUploadScheduler">
                    {{ getSchedulerStartButtonText('alarm_rule_export_upload') }}
                  </button>
                  <button class="btn btn-danger"
                    :disabled="alarmRuleExportUploadSchedulerQuickSaving || isSchedulerStopDisabled('alarm_rule_export_upload', actionKeyAlarmRuleExportUploadSchedulerStart, actionKeyAlarmRuleExportUploadSchedulerStop)"
                    @click="stopAlarmRuleExportUploadScheduler">
                    {{ getSchedulerStopButtonText('alarm_rule_export_upload') }}
                  </button>
                </div>
                <div class="hint">默认每月3日09:00上传当月A-E楼附件；修改日期或时间后立即生效。</div>
              </article>

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
