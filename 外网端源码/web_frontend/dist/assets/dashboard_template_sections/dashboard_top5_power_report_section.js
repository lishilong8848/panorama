export const DASHBOARD_TOP5_POWER_REPORT_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'top5_power_report'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">TOP5功率文件生成</h3>
              <div class="hint">读取最新共享源文件，生成高功率设备TOP5汇总表，并把参与计算的源文件追加为独立 sheet。</div>
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
                  <div class="ops-focus-card-meta">通过现有共享缓存读取最新 ready 文件。</div>
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
                <div class="hint">当前任务：{{ currentJob && currentJob.feature === 'top5_power_report' ? (currentJob.job_id || '-') : '-' }}</div>
              </article>
            </div>
          </div>
        </section>`;
