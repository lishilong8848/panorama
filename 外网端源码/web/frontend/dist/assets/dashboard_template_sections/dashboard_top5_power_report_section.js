export const DASHBOARD_TOP5_POWER_REPORT_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'top5_power_report'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">TOP5功率文件生成</h3>
              <div class="hint">保留现有 TOP5 生成功能，并提供月度超功率/超功耗附件获取入口。两个任务独立提交，互不阻塞。</div>
            </div>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">手动触发卡</div>
                    <h3 class="card-title">生成TOP5功率文件</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">A楼至E楼</span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">设备组</div>
                    <strong class="status-metric-value">4组</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">汇总行</div>
                    <strong class="status-metric-value">25</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">源sheet</div>
                    <strong class="status-metric-value">10</strong>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">数据来源</div>
                  <div class="ops-focus-card-title">交接班容量报表源文件、支路功率源文件</div>
                  <div class="ops-focus-card-meta">通过现有共享缓存读取最新 ready 文件，生成后按下方年月覆盖上传到高功率 TOP5 多维附件记录。</div>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label">上传年份</label>
                    <input type="text" v-model="top5PowerReportYear" placeholder="2026" />
                  </div>
                  <div class="form-row">
                    <label class="label">上传月份</label>
                    <select v-model.number="top5PowerReportMonth">
                      <option v-for="month in 12" :key="'top5-power-report-month-' + month" :value="month">{{ month }}月</option>
                    </select>
                  </div>
                </div>
                <div class="btn-line" style="flex-wrap:wrap;">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || isActionLocked(actionKeyTop5PowerReportRun)"
                    @click="runTop5PowerReport"
                  >
                    {{ isActionLocked(actionKeyTop5PowerReportRun) ? '提交中...' : '生成文件' }}
                  </button>
                  <button
                    class="btn btn-success"
                    :disabled="!canDownloadTop5PowerReport()"
                    @click="downloadTop5PowerReportCurrentJob"
                  >
                    下载结果文件
                  </button>
                </div>
              </article>

              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">附件获取卡</div>
                    <h3 class="card-title">获取月度超功率/超功耗附件</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">飞书多维附件</span>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label">年份</label>
                    <input type="text" v-model="top5OverPowerYear" placeholder="2026" />
                  </div>
                  <div class="form-row">
                    <label class="label">月份</label>
                    <select v-model.number="top5OverPowerMonth">
                      <option v-for="month in 12" :key="'top5-over-power-month-' + month" :value="month">{{ month }}月</option>
                    </select>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">筛选规则</div>
                  <div class="ops-focus-card-title">附件名包含“超功率”或“超功耗”，并排除 TOP5 文件</div>
                  <div class="ops-focus-card-meta">使用现有飞书应用凭证与统一 token 管理，不保存独立密钥。</div>
                </div>
                <div class="btn-line" style="flex-wrap:wrap;">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || isActionLocked(actionKeyTop5OverPowerAttachmentRun)"
                    @click="runTop5OverPowerAttachment"
                  >
                    {{ isActionLocked(actionKeyTop5OverPowerAttachmentRun) ? '提交中...' : '获取附件' }}
                  </button>
                  <button
                    class="btn btn-success"
                    :disabled="!canDownloadTop5OverPowerAttachment()"
                    @click="downloadTop5OverPowerAttachmentCurrentJob"
                  >
                    下载附件包
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">状态概览</div>
                    <h3 class="card-title">最近TOP5任务</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getTop5PowerReportStatusTone()">
                    {{ getTop5PowerReportStatusText() }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">汇总行</div>
                    <strong class="status-metric-value">{{ getTop5PowerReportResult().summary_row_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">源sheet</div>
                    <strong class="status-metric-value">{{ getTop5PowerReportResult().source_sheet_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">完成时间</div>
                    <strong class="status-metric-value">{{ getTop5PowerReportResult().finished_at || '-' }}</strong>
                  </div>
                </div>
                <div class="hint">输出文件：{{ getTop5PowerReportResult().file_name || '-' }}</div>
                <div class="hint">输出目录：{{ getTop5PowerReportResult().output_dir || '-' }}</div>
                <div class="hint">多维上传：{{ getTop5PowerReportResult().bitable_upload ? ((getTop5PowerReportResult().bitable_upload.year || '-') + '-' + (getTop5PowerReportResult().bitable_upload.month || '-') + ' / ' + (getTop5PowerReportResult().bitable_upload.record_id || '-')) : '-' }}</div>
                <div class="hint">当前任务：{{ currentJob && currentJob.feature === 'top5_power_report' ? (currentJob.job_id || '-') : '-' }}</div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">状态概览</div>
                    <h3 class="card-title">最近附件获取任务</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getTop5OverPowerAttachmentStatusTone()">
                    {{ getTop5OverPowerAttachmentStatusText() }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">已下载</div>
                    <strong class="status-metric-value">{{ getTop5OverPowerAttachmentResult().downloaded_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">失败数</div>
                    <strong class="status-metric-value">{{ (getTop5OverPowerAttachmentResult().errors || []).length || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">年月</div>
                    <strong class="status-metric-value">{{ getTop5OverPowerAttachmentResult().year || '-' }}-{{ getTop5OverPowerAttachmentResult().month ? String(getTop5OverPowerAttachmentResult().month).padStart(2, '0') : '-' }}</strong>
                  </div>
                </div>
                <div class="hint">附件包：{{ getTop5OverPowerAttachmentResult().zip_file_name || '-' }}</div>
                <div class="hint">输出目录：{{ getTop5OverPowerAttachmentResult().output_dir || '-' }}</div>
                <div class="hint">当前任务：{{ currentJob && currentJob.feature === 'top5_over_power_attachment' ? (currentJob.job_id || '-') : '-' }}</div>
              </article>
            </div>
          </div>
        </section>`;
