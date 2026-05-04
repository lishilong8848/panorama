export const DASHBOARD_BRANCH_POWER_UPLOAD_SECTION = `        <section class="content-card" v-if="!isInternalDeploymentRole && dashboardActiveModule === 'branch_power_upload'">
          <div class="dashboard-module-shell">
            <article class="task-block dashboard-module-scheduler-card">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">调度卡</div>
                  <h3 class="card-title">自动上传支路功率调度</h3>
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
              </div>
              <div class="hint">每小时处理上一小时桶的支路功率共享文件；缺文件时进入内网补采同步。</div>
              <div class="task-grid two-col">
                <div class="form-row">
                  <label class="label">执行间隔（分钟）</label>
                  <input
                    type="number"
                    min="1"
                    step="1"
                    v-model.number="config.branch_power_upload.scheduler.interval_minutes"
                    :disabled="branchPowerUploadSchedulerQuickSaving"
                    @change="saveBranchPowerUploadSchedulerQuickConfig"
                  />
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
              <div class="hint">{{ branchPowerUploadSchedulerQuickSaving ? '自动上传支路功率调度配置同步中...' : '修改执行间隔后立即生效。' }}</div>
            </article>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">执行入口</div>
                    <h3 class="card-title">上传最新支路功率</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">按小时更新</span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">楼栋范围</div>
                    <strong class="status-metric-value">全部启用楼栋</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">匹配键</div>
                    <strong class="status-metric-value">机楼/包间/机列/PDU</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">目标字段</div>
                    <strong class="status-metric-value">当前小时</strong>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">当前策略</div>
                  <div class="ops-focus-card-title">读取最新共享支路功率文件，按小时字段更新多维表</div>
                  <div class="ops-focus-card-meta">0 点上传前会清空目标表一次；1-23 点只按匹配键更新对应小时字段。</div>
                </div>
                <div class="hint">{{ bridgeExecutionHint }}</div>
                <div class="btn-stack" style="margin-top:8px;">
                  <button
                    class="btn btn-primary"
                    :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyBranchPowerFromDownload)"
                    @click="runBranchPowerFromDownload"
                  >
                    {{ isActionLocked(actionKeyBranchPowerFromDownload) ? '执行中...' : '立即执行一次' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">数据规则</div>
                    <h3 class="card-title">支路功率写入说明</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-neutral">共享文件</span>
                </div>
                <div class="hint">内网端下载“列头柜支路电流”，sheet 名固定为“支路功率”，文件按数据所属上一小时进入共享桶。</div>
                <div class="hint">外网端从第 4 行开始读取 A-D 列：包间、机列、PDU、数值。</div>
                <div class="hint">目标多维表固定更新 ASLxbfESPahdTKs0A9NccgbrnXc / tblT5KbsxGCK1SwA。</div>
              </article>
            </div>
          </div>
        </section>
`;
