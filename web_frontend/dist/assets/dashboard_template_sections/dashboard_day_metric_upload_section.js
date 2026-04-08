export const DASHBOARD_DAY_METRIC_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'day_metric_upload'">
          <div class="dashboard-module-shell">
          <article class="task-block dashboard-module-scheduler-card">
            <div class="task-block-head">
              <div>
                <div class="task-block-kicker">调度卡</div>
                <h3 class="card-title">12项独立上传调度</h3>
              </div>
              <span class="status-badge status-badge-soft" :class="health.day_metric_upload.scheduler.running ? 'tone-success' : 'tone-neutral'">
                {{ health.day_metric_upload.scheduler.status || '-' }}
              </span>
            </div>
            <div class="status-metric-grid status-metric-grid-compact">
              <div class="status-metric">
                <div class="status-metric-label">下次执行</div>
                <strong class="status-metric-value">{{ health.day_metric_upload.scheduler.next_run_time || '-' }}</strong>
              </div>
              <div class="status-metric">
                <div class="status-metric-label">最近触发</div>
                <strong class="status-metric-value">{{ health.day_metric_upload.scheduler.last_trigger_at || '-' }}</strong>
              </div>
              <div class="status-metric">
                <div class="status-metric-label">最近决策</div>
                <strong class="status-metric-value">{{ dayMetricUploadSchedulerDecisionText || '-' }}</strong>
              </div>
            </div>
            <div class="hint">调度固定处理当天、全部启用楼栋；缺共享文件时会显示等待内网补采同步。</div>
            <div class="task-grid two-col">
              <div class="form-row">
                <label class="label">每日执行时间</label>
                <input type="time" step="1" v-model="config.day_metric_upload.scheduler.run_time" @change="saveDayMetricUploadSchedulerQuickConfig" />
              </div>
              <div class="form-row">
                <label class="label">最近结果</label>
                <div class="readonly-inline-card">{{ dayMetricUploadSchedulerTriggerText || '-' }}</div>
              </div>
            </div>
            <div class="btn-line">
              <button
                class="btn btn-success"
                :disabled="isInternalDeploymentRole || health.day_metric_upload.scheduler.running || isActionLocked(actionKeyDayMetricUploadSchedulerStart)"
                @click="startDayMetricUploadScheduler"
              >
                {{
                  isActionLocked(actionKeyDayMetricUploadSchedulerStart)
                    ? '启动中...'
                    : (health.day_metric_upload.scheduler.running ? '已启动调度' : '启动调度')
                }}
              </button>
              <button
                class="btn btn-danger"
                :disabled="isInternalDeploymentRole || !health.day_metric_upload.scheduler.running || isActionLocked(actionKeyDayMetricUploadSchedulerStop)"
                @click="stopDayMetricUploadScheduler"
              >
                {{ isActionLocked(actionKeyDayMetricUploadSchedulerStop) ? '停止中...' : '停止调度' }}
              </button>
            </div>
            <div class="hint">{{ dayMetricUploadSchedulerQuickSaving ? '12项独立上传调度配置保存中...' : '修改每日执行时间后自动保存。' }}</div>
          </article>
          <div class="day-metric-shell">
            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">独立执行</div>
                    <h3 class="card-title">12项执行参数</h3>
                  </div>
                <div class="btn-line">
                    <span class="status-badge status-badge-soft tone-info">不区分班次</span>
                    <span
                      class="status-badge status-badge-soft"
                      :class="deploymentRoleMode === 'external' ? 'tone-success' : 'tone-neutral'"
                    >
                      {{
                        deploymentRoleMode === 'internal' ? '内网端' : '外网端'
                      }}
                    </span>
                  </div>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">楼栋范围</div>
                    <strong class="status-metric-value">{{ dayMetricUploadScope === 'all_enabled' ? '全部启用楼栋' : (dayMetricUploadBuilding || '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">已选日期</div>
                    <strong class="status-metric-value">{{ dayMetricSelectedDateCount }} 天</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">运行角色</div>
                    <strong class="status-metric-value">{{ deploymentRoleMode === 'internal' ? '内网端' : '外网端' }}</strong>
                  </div>
                </div>

                <div class="hint">该模块只上传 12 项，不生成交接班日志，不进入审核流程。</div>
                <div class="hint">
                  {{
                    deploymentRoleMode === 'internal'
                      ? '当前为内网端，请在外网端发起；内网端只负责准备共享文件。'
                      : '当前为外网端，默认优先读取共享文件；缺失时再等待内网端补采。'
                  }}
                </div>

                <div class="task-grid two-col" style="margin-top:8px;">
                  <div class="form-row">
                    <label class="label">楼栋范围</label>
                    <select v-model="dayMetricUploadScope">
                      <option value="single">单楼</option>
                      <option value="all_enabled">全部启用楼栋</option>
                    </select>
                  </div>
                  <div class="form-row" v-if="dayMetricUploadScope === 'single'">
                    <label class="label">楼栋</label>
                    <select v-model="dayMetricUploadBuilding">
                      <option v-for="b in config.input.buildings" :key="'day-metric-building-' + b" :value="b">{{ b }}</option>
                    </select>
                  </div>
                  <div class="form-row" v-else>
                    <label class="label">楼栋范围说明</label>
                    <div class="readonly-inline-card">将按配置中全部启用楼栋逐个执行</div>
                  </div>
                  <div class="form-row">
                    <label class="label">开始日期</label>
                    <input type="date" v-model="dayMetricRangeStartDate" />
                  </div>
                  <div class="form-row">
                    <label class="label">结束日期</label>
                    <input type="date" v-model="dayMetricRangeEndDate" />
                  </div>
                </div>

                <div class="btn-line">
                  <button class="btn btn-secondary" @click="addDayMetricDateRange">按区间添加</button>
                </div>

                <div class="task-grid two-col">
                  <div class="form-row">
                    <label class="label">单日快选</label>
                    <input type="date" v-model="dayMetricSelectedDate" />
                  </div>
                  <div class="form-row">
                    <label class="label">失败策略</label>
                    <div class="readonly-inline-card">继续其他日期 / 楼栋</div>
                  </div>
                </div>

                <div class="btn-line">
                  <button class="btn btn-secondary" @click="addDayMetricDate">添加单日</button>
                  <button class="btn btn-ghost" @click="clearDayMetricDates">清空已选</button>
                </div>

                <div class="form-row">
                  <div class="label">已选日期（共 {{ dayMetricSelectedDateCount }} 天）</div>
                  <div class="chips">
                    <span class="chip" v-for="d in dayMetricSelectedDates" :key="'day-metric-date-' + d">
                      {{ d }}
                      <button @click="removeDayMetricDate(d)">×</button>
                    </span>
                  </div>
                </div>
              </article>

              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">主流程</div>
                    <h3 class="card-title">使用共享文件上传12项</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-warning">删除旧记录后重写</span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">当前楼栋</div>
                    <strong class="status-metric-value">{{ dayMetricUploadScope === 'all_enabled' ? '全部启用楼栋' : (dayMetricUploadBuilding || '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">当前日期</div>
                    <strong class="status-metric-value">{{ dayMetricSelectedDateCount }} 天</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">写入方式</div>
                    <strong class="status-metric-value">先删后写</strong>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">执行说明</div>
                  <div class="ops-focus-card-title">优先使用共享文件上传 12 项，按同楼同日先删后写</div>
                  <div class="ops-focus-card-meta">会按日期升序、楼栋配置顺序逐个执行；失败单元不会阻断其余日期或楼栋。</div>
                </div>
                <div class="hint">执行顺序：按日期升序、楼栋配置顺序逐个执行。失败单元不会中断其他日期或楼栋。</div>
                <div class="hint">{{ bridgeExecutionHint }}</div>
                <div class="task-grid two-col" style="margin-top:8px;">
                  <div class="readonly-inline-card">App Token：{{ dayMetricUploadTarget.appToken || '-' }}</div>
                  <div class="readonly-inline-card">Table ID：{{ dayMetricUploadTarget.tableId || '-' }}</div>
                </div>
                <div class="btn-line" style="margin-top:10px;" v-if="dayMetricUploadTarget.displayUrl || dayMetricUploadTarget.bitableUrl">
                  <a
                    class="secondary-btn btn-compact"
                    :href="dayMetricUploadTarget.displayUrl || dayMetricUploadTarget.bitableUrl"
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    打开多维表
                  </a>
                </div>
                <div class="hint" style="margin-top:10px;">{{ dayMetricUploadTarget.hintText }}</div>
                <div class="btn-stack" style="margin-top:8px;">
                  <button
                    class="btn btn-primary"
                    :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyDayMetricFromDownload)"
                    @click="runDayMetricFromDownload"
                  >
                    {{ isActionLocked(actionKeyDayMetricFromDownload) ? '执行中...' : '使用共享文件上传12项' }}
                  </button>
                </div>
                <div class="hint">当前选择：{{ dayMetricUploadScope === 'all_enabled' ? '全部启用楼栋' : (dayMetricUploadBuilding || '-') }} / {{ dayMetricSelectedDateCount }} 天</div>
                <div class="hint" v-if="isInternalDeploymentRole">当前为内网端，12项任务请在外网端发起；内网端只负责共享桥接下载阶段。</div>
              </article>
            </div>

            <details class="module-advanced-section">
              <summary>查看本地文件补录</summary>
              <div class="module-advanced-section-body">
            <article class="task-block">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">补录入口</div>
                  <h3 class="card-title">本地文件补录</h3>
                </div>
                <span class="status-badge status-badge-soft tone-info">默认启用</span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">补录楼栋</div>
                  <strong class="status-metric-value">{{ dayMetricLocalBuilding || '-' }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">补录日期</div>
                  <strong class="status-metric-value">{{ dayMetricLocalDate || '-' }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">功能状态</div>
                  <strong class="status-metric-value">默认启用</strong>
                </div>
              </div>
              <div class="hint">仅用于单日期单楼补救，不走内网下载。{{ externalExecutionHint }}</div>
              <div class="task-grid day-metric-local-grid">
                <div class="form-row">
                  <label class="label">楼栋</label>
                  <select v-model="dayMetricLocalBuilding">
                    <option v-for="b in config.input.buildings" :key="'day-metric-local-' + b" :value="b">{{ b }}</option>
                  </select>
                </div>
                <div class="form-row">
                  <label class="label">日期</label>
                  <input type="date" v-model="dayMetricLocalDate" />
                </div>
                <div class="form-row day-metric-file-row">
                  <label class="label">Excel 文件</label>
                  <input type="file" accept=".xlsx,.xlsm,.xls" @change="onDayMetricLocalFileChange" />
                </div>
              </div>
              <div class="btn-line">
                <button
                  class="btn btn-secondary"
                  :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyDayMetricFromFile)"
                  @click="runDayMetricFromFile"
                >
                  {{ isActionLocked(actionKeyDayMetricFromFile) ? '补录中...' : '开始补录12项' }}
                </button>
              </div>
              <div class="hint" v-if="isInternalDeploymentRole">当前为内网端，本地文件补录也请在外网端执行。</div>
            </article>
              </div>
            </details>

            <details class="module-advanced-section" v-if="dayMetricCurrentPayload">
              <summary>查看执行结果汇总</summary>
              <div class="module-advanced-section-body">
            <article class="task-block">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">执行结果</div>
                  <h3 class="card-title">12项上传结果汇总</h3>
                </div>
                <div class="btn-line">
                  <button
                    v-if="dayMetricRetryAllMode === 'from_download' && dayMetricRetryableFailedCount > 0"
                    class="secondary-btn btn-compact"
                    :disabled="isActionLocked(actionKeyDayMetricRetryFailed + ':' + dayMetricRetryAllMode)"
                    @click="retryFailedDayMetricUnits(dayMetricRetryAllMode)"
                  >
                    重试全部失败单元（{{ dayMetricRetryableFailedCount }}）
                  </button>
                  <span class="status-badge status-badge-soft" :class="currentJob && currentJob.status === 'failed' ? 'tone-danger' : currentJob && currentJob.status === 'success' ? 'tone-success' : 'tone-warning'">
                    {{ currentJob ? (currentJob.status || 'running') : '-' }}
                  </span>
                </div>
              </div>

              <div class="day-metric-summary-grid">
                <div class="readonly-inline-card">模式：{{ dayMetricCurrentPayload.mode === 'from_file' ? '本地文件补录' : '多日期下载上传' }}</div>
                <div class="readonly-inline-card">执行网络：当前角色固定网络</div>
                <div class="readonly-inline-card">总单元：{{ dayMetricCurrentPayload.total_units || 0 }}</div>
                <div class="readonly-inline-card">成功：{{ dayMetricCurrentPayload.success_units || 0 }}</div>
                <div class="readonly-inline-card">失败：{{ dayMetricCurrentPayload.failed_units || 0 }}</div>
                <div class="readonly-inline-card">跳过：{{ dayMetricCurrentPayload.skipped_units || 0 }}</div>
                <div class="readonly-inline-card">删除旧记录：{{ dayMetricCurrentPayload.total_deleted_records || 0 }}</div>
                <div class="readonly-inline-card">创建新记录：{{ dayMetricCurrentPayload.total_created_records || 0 }}</div>
              </div>

              <div class="day-metric-result-table-wrap" v-if="dayMetricCurrentResultRows.length">
                <table class="day-metric-result-table">
                  <thead>
                    <tr>
                      <th>日期</th>
                      <th>楼栋</th>
                      <th>状态</th>
                      <th>失败阶段</th>
                      <th>网络模式</th>
                      <th>尝试次数</th>
                      <th>删除数</th>
                      <th>创建数</th>
                      <th>错误信息</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="row in dayMetricCurrentResultRows" :key="'day-metric-result-' + row.duty_date + '-' + row.building">
                      <td>{{ row.duty_date }}</td>
                      <td>{{ row.building }}</td>
                      <td>
                        <span class="status-badge status-badge-soft" :class="'tone-' + row.tone">{{ row.status }}</span>
                      </td>
                      <td>{{ row.stage || '-' }}</td>
                      <td>{{ row.network_mode || '-' }}</td>
                      <td>{{ row.attempts || 0 }}</td>
                      <td>{{ row.deleted_records }}</td>
                      <td>{{ row.created_records }}</td>
                      <td class="day-metric-error-cell">{{ row.error || '-' }}</td>
                      <td>
                        <button
                          v-if="row.status_key === 'failed'"
                          class="secondary-btn btn-compact"
                          :disabled="!row.retryable || isActionLocked(actionKeyDayMetricRetryUnit + ':' + row.mode + ':' + row.duty_date + ':' + row.building)"
                          :title="row.retryable ? '重试该失败单元' : (row.retry_hint || '当前失败单元暂不支持重试')"
                          @click="retryDayMetricUnit(row)"
                        >
                          重试
                        </button>
                        <span v-else>-</span>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </article>
              </div>
            </details>
          </div>
          </div>
        </section>
`;
