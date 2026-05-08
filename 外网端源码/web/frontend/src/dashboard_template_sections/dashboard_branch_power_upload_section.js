export const DASHBOARD_BRANCH_POWER_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'branch_power_upload'">
          <div class="dashboard-module-shell">
            <article class="task-block dashboard-module-scheduler-card">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">调度卡</div>
                  <h3 class="card-title">支路三源表日暂存调度</h3>
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
              <div class="hint">每小时约 30 分读取上一业务小时的支路功率、支路电流、支路开关三类共享文件写入本地库；0 点处理完前一天 23 点后整表上传多维。</div>
              <div class="task-grid two-col">
                <div class="form-row">
                  <label class="label">执行分钟（每小时）</label>
                  <input
                    type="number"
                    min="0"
                    max="59"
                    step="1"
                    v-model.number="config.branch_power_upload.scheduler.minute_offset"
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
              <div class="hint">{{ branchPowerUploadSchedulerQuickSaving ? '支路三源表调度配置同步中...' : '修改每小时执行分钟后立即生效。' }}</div>
            </article>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">执行入口</div>
                    <h3 class="card-title">处理最新支路三源表</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">每小时入库</span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">楼栋范围</div>
                    <strong class="status-metric-value">全部启用楼栋</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">匹配键</div>
                    <strong class="status-metric-value">机楼/包间/机列/支路/PDU</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">目标字段</div>
                    <strong class="status-metric-value">功率/电流/开关</strong>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">当前策略</div>
                  <div class="ops-focus-card-title">读取最新共享三源表，先写本地 SQLite，23 点后整日上传</div>
                  <div class="ops-focus-card-meta">日终上传成功后会清空本地当天暂存；已上传日期的指定小时处理会直接更新多维表，不再写库。</div>
                </div>
                <div class="hint">{{ bridgeExecutionHint }}</div>
                <div class="btn-stack" style="margin-top:8px;">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || isActionLocked(actionKeyBranchPowerFromDownload)"
                    @click="runBranchPowerFromDownload"
                  >
                    {{ isActionLocked(actionKeyBranchPowerFromDownload) ? '执行中...' : '立即执行一次' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">缺口总览</div>
                    <h3 class="card-title">小时入库状态</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">本地库</span>
                </div>
                <div class="task-grid two-col">
                  <div class="form-row">
                    <label class="label">日期</label>
                    <input
                      type="date"
                      v-model="branchPowerBackfillDate"
                      :disabled="branchPowerHourStatusLoading || isActionLocked(actionKeyBranchPowerBackfillMissing)"
                    />
                  </div>
                  <div class="form-row">
                    <label class="label">楼栋范围</label>
                    <select
                      v-model="branchPowerBackfillBuilding"
                      :disabled="branchPowerHourStatusLoading || isActionLocked(actionKeyBranchPowerBackfillMissing)"
                    >
                      <option value="">全部启用楼栋</option>
                      <option value="A楼">A楼</option>
                      <option value="B楼">B楼</option>
                      <option value="C楼">C楼</option>
                      <option value="D楼">D楼</option>
                      <option value="E楼">E楼</option>
                    </select>
                  </div>
                </div>
                <div class="status-metric-grid status-metric-grid-compact" v-if="branchPowerHourStatus">
                  <div class="status-metric">
                    <div class="status-metric-label">已齐全小时</div>
                    <strong class="status-metric-value">{{ branchPowerHourStatus.summary?.complete_hours || 0 }}/{{ branchPowerHourStatus.summary?.total_hours || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">缺口小时</div>
                    <strong class="status-metric-value">{{ branchPowerHourStatus.summary?.incomplete_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">楼栋数</div>
                    <strong class="status-metric-value">{{ branchPowerHourStatus.expected_building_count || 0 }}</strong>
                  </div>
                </div>
                <div class="branch-hour-status-grid" v-if="branchPowerHourStatus">
                  <div
                    v-for="item in branchPowerHourStatus.hours || []"
                    :key="item.bucket_key"
                    class="branch-hour-status-pill"
                    :class="'is-' + item.status"
                    :title="(item.missing_buildings || []).length ? '缺少：' + (item.missing_buildings || []).join('、') : ''"
                  >
                    <strong>{{ item.hour_text }}</strong>
                    <span>{{ item.status === 'complete' ? '已入库' : item.status === 'partial' ? '部分' : item.status === 'failed' ? '失败' : '未入库' }}</span>
                    <small>{{ item.success_count || 0 }}/{{ item.expected_count || 0 }}</small>
                  </div>
                </div>
                <div class="hint">{{ branchPowerHourStatusMessage || '可查看指定日期从 0 点到当前目标小时的入库缺口。' }}</div>
                <div class="btn-line">
                  <button
                    class="btn btn-secondary"
                    :disabled="branchPowerHourStatusLoading"
                    @click="refreshBranchPowerHourStatus"
                  >
                    {{ branchPowerHourStatusLoading ? '刷新中...' : '刷新入库状态' }}
                  </button>
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || branchPowerHourStatusLoading || isActionLocked(actionKeyBranchPowerBackfillMissing)"
                    @click="runBranchPowerBackfillMissing"
                  >
                    {{ isActionLocked(actionKeyBranchPowerBackfillMissing) ? '处理中...' : '一键补全部缺口' }}
                  </button>
                </div>
                <div class="hint">缺少三源文件时会自动创建共享补采任务；下载完成后按原小时继续写本地库或更新多维表。</div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">手动补处理</div>
                    <h3 class="card-title">指定小时三源表</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-warning">按小时</span>
                </div>
                <div class="task-grid two-col">
                  <div class="form-row">
                    <label class="label">日期</label>
                    <input
                      type="date"
                      v-model="branchPowerManualDate"
                      :disabled="isActionLocked(actionKeyBranchPowerManualHour)"
                    />
                  </div>
                  <div class="form-row">
                    <label class="label">小时</label>
                    <select v-model="branchPowerManualHour" :disabled="isActionLocked(actionKeyBranchPowerManualHour)">
                      <option
                        v-for="item in branchPowerManualHourOptions"
                        :key="item.value"
                        :value="item.value"
                      >
                        {{ item.label }}
                      </option>
                    </select>
                  </div>
                  <div class="form-row">
                    <label class="label">时间桶</label>
                    <div class="readonly-inline-card">{{ branchPowerManualBucketKey || '-' }}</div>
                  </div>
                  <div class="form-row">
                    <label class="label">楼栋范围</label>
                    <select v-model="branchPowerManualBuilding" :disabled="isActionLocked(actionKeyBranchPowerManualHour)">
                      <option value="">全部启用楼栋</option>
                      <option value="A楼">A楼</option>
                      <option value="B楼">B楼</option>
                      <option value="C楼">C楼</option>
                      <option value="D楼">D楼</option>
                      <option value="E楼">E楼</option>
                    </select>
                  </div>
                </div>
                <div class="hint">日期未整表上传时写入本地库；该日期已有日终上传记录时，直接把该小时三类数据更新到多维表。</div>
                <div class="btn-stack" style="margin-top:8px;">
                  <button
                    class="btn btn-secondary"
                    :disabled="!canRun || isActionLocked(actionKeyBranchPowerManualHour)"
                    @click="runBranchPowerManualHour"
                  >
                    {{ isActionLocked(actionKeyBranchPowerManualHour) ? '处理中...' : '处理指定小时' }}
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
                <div class="hint">采集端分别下载“支路功率 / 支路电流 / 支路开关”三张表，按同一小时桶保存。</div>
                <div class="hint">外网端从第 4 行开始按行合并：功率表 A/B/C 给包间、机列、PDU编号，开关表 C 给支路编号，三表 D 列或表头匹配小时列给对应值。</div>
                <div class="hint">目标多维表固定为 ASLxbfESPahdTKs0A9NccgbrnXc / tblT5KbsxGCK1SwA，日终整表上传前会清空目标表。</div>
              </article>
            </div>
          </div>
        </section>
`;

