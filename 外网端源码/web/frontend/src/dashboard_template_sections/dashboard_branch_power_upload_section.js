export const DASHBOARD_BRANCH_POWER_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'branch_power_upload'">
          <div class="dashboard-module-shell">
            <article class="task-block dashboard-module-scheduler-card">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">调度卡</div>
                  <h3 class="card-title">支路三源表整日直传调度</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('branch_power_upload')">
                  {{ getSchedulerStatusText('branch_power_upload') || '-' }}
                </span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">下次执行</div>
                  <strong class="status-metric-value">{{ getSchedulerDisplayText('branch_power_upload', 'next_run_text', '-') }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">最近触发</div>
                  <strong class="status-metric-value">{{ getSchedulerDisplayText('branch_power_upload', 'last_trigger_text', '-') }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">最近结果</div>
                  <strong class="status-metric-value">{{ branchPowerUploadSchedulerTriggerText || '-' }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">执行计划</div>
                  <strong class="status-metric-value">{{ branchPowerUploadScheduleText || '-' }}</strong>
                </div>
              </div>
              <div class="hint">每天 00:30 左右处理前一业务日，内网端按整日窗口下载支路功率、支路电流、支路开关，外网端解析 24 小时后直接清表整传多维表。</div>
              <div class="task-grid two-col">
                <div class="form-row">
                  <label class="label">调度口径</label>
                  <div class="readonly-inline-card">每日一次，处理前一业务日</div>
                </div>
                <div class="form-row">
                  <label class="label">最近决策</label>
                  <div class="readonly-inline-card">{{ branchPowerUploadSchedulerDecisionText || '-' }}</div>
                </div>
              </div>
              <div class="btn-line">
                <button
                  class="btn btn-success"
                  :disabled="branchPowerUploadSchedulerQuickSaving || isSchedulerStartDisabled('branch_power_upload', actionKeyBranchPowerUploadSchedulerStart, actionKeyBranchPowerUploadSchedulerStop)"
                  @click="startBranchPowerUploadScheduler"
                >
                  {{ getSchedulerStartButtonText('branch_power_upload') }}
                </button>
                <button
                  class="btn btn-danger"
                  :disabled="branchPowerUploadSchedulerQuickSaving || isSchedulerStopDisabled('branch_power_upload', actionKeyBranchPowerUploadSchedulerStart, actionKeyBranchPowerUploadSchedulerStop)"
                  @click="stopBranchPowerUploadScheduler"
                >
                  {{ getSchedulerStopButtonText('branch_power_upload') }}
                </button>
              </div>
            </article>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">执行入口</div>
                    <h3 class="card-title">处理指定业务日支路三源表</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">整日直传</span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">楼栋范围</div>
                    <strong class="status-metric-value">全部启用楼栋</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">查询窗口</div>
                    <strong class="status-metric-value">前日 23:50 至当日 23:50</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">目标字段</div>
                    <strong class="status-metric-value">功率/电流/开关</strong>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">当前策略</div>
                  <div class="ops-focus-card-title">读取整日共享三源表，解析完成后直接上传多维表</div>
                  <div class="ops-focus-card-meta">支路本地库不再参与新流程；源文件缺失时会创建内网整日补采任务。</div>
                </div>
                <div class="form-row" style="margin-top:10px;">
                  <label class="label">业务日期</label>
                  <input class="input" type="date" v-model="branchPowerBusinessDate" />
                  <div class="hint">手动执行只按这一天处理；调度仍在每天 00:30 左右处理前一业务日。</div>
                </div>
                <div class="hint">{{ bridgeExecutionHint }}</div>
                <div class="btn-stack" style="margin-top:8px;">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || isActionLocked(actionKeyBranchPowerFromDownload)"
                    @click="runBranchPowerFromDownload"
                  >
                    {{ isActionLocked(actionKeyBranchPowerFromDownload) ? '执行中...' : '执行该业务日整日直传' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">数据规则</div>
                    <h3 class="card-title">支路三源表写入说明</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-neutral">共享文件</span>
                </div>
                <div class="hint">采集端分别下载“支路功率 / 支路电流 / 支路开关”三张整日表，每楼每类每天一份源文件。</div>
                <div class="hint">外网端从第 4 行开始按行合并：功率表 A/B/C 给包间、机列、PDU编号，开关表 C 给支路编号，按 00:00 至 23:00 小时列读取对应值。</div>
                <div class="hint">目标多维表固定为 ASLxbfESPahdTKs0A9NccgbrnXc / tblT5KbsxGCK1SwA；整日解析全部成功后才清空并批量上传。</div>
              </article>
            </div>
          </div>
        </section>
`;
