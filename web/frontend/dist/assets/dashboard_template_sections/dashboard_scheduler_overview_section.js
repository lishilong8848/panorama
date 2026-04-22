export const DASHBOARD_SCHEDULER_OVERVIEW_SECTION = `        <section class="content-card" v-if="!isInternalDeploymentRole && dashboardActiveModule === 'scheduler_overview'">
          <div class="dashboard-module-shell">
            <article class="task-block task-block-compact dashboard-scheduler-overview-summary-card">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">统一扫读</div>
                  <h3 class="card-title">全部调度状态</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="'tone-' + schedulerOverviewSummary.tone">
                  {{ schedulerOverviewSummary.statusText }}
                </span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">已启动调度</div>
                  <strong class="status-metric-value">{{ schedulerOverviewSummary.runningCount }} 项</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">最近即将执行</div>
                  <strong class="status-metric-value">{{ schedulerOverviewSummary.nextSchedulerLabel }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">待关注项</div>
                  <strong class="status-metric-value">{{ schedulerOverviewSummary.attentionCount }} 项</strong>
                </div>
              </div>
              <div class="hint">{{ schedulerOverviewSummary.summaryText }}</div>
            </article>

            <article class="task-block task-block-compact dashboard-shared-root-check-card">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">共享目录</div>
                  <h3 class="card-title">共享目录自检</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="'tone-' + sharedBridgeSelfCheckOverview.tone">
                  {{ sharedBridgeSelfCheckOverview.statusText }}
                </span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">就绪记录</div>
                  <strong class="status-metric-value">{{ sharedBridgeSelfCheckOverview.readyEntryCount }} 条</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">可访问文件</div>
                  <strong class="status-metric-value">{{ sharedBridgeSelfCheckOverview.accessibleReadyCount }} 条</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">不可见文件</div>
                  <strong class="status-metric-value">{{ sharedBridgeSelfCheckOverview.missingReadyCount }} 条</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">本次补齐目录</div>
                  <strong class="status-metric-value">{{ sharedBridgeSelfCheckOverview.initializedCount }} 个</strong>
                </div>
              </div>
              <div class="hint">当前角色：{{ sharedBridgeSelfCheckOverview.roleLabel }}</div>
              <div class="hint">共享目录：{{ sharedBridgeSelfCheckOverview.rootDirText }}</div>
              <div class="hint">数据库：{{ sharedBridgeSelfCheckOverview.dbPathText }}</div>
              <div class="hint">{{ sharedBridgeSelfCheckOverview.summaryText }}</div>
              <div class="hint" v-if="sharedBridgeSelfCheckOverview.checkedAtText">最近自检：{{ sharedBridgeSelfCheckOverview.checkedAtText }}</div>
              <div class="hint" v-if="sharedBridgeSelfCheckOverview.errorText">最近异常：{{ sharedBridgeSelfCheckOverview.errorText }}</div>
              <div class="btn-line dashboard-shared-root-check-action">
                <button
                  class="btn btn-secondary"
                  type="button"
                  :disabled="isActionLocked(actionKeySharedBridgeSelfCheck)"
                  @click="runSharedBridgeSelfCheck"
                >
                  {{ isActionLocked(actionKeySharedBridgeSelfCheck) ? '自检中...' : '共享目录自检并补齐必要目录' }}
                </button>
              </div>
              <div class="hint">只会补齐共享桥接、源文件缓存和临时目录，不会改动其他文件夹。</div>
              <div class="dashboard-shared-root-family-grid" v-if="sharedBridgeSelfCheckOverview.familyItems.length">
                <article
                  class="dashboard-shared-root-family-card"
                  v-for="family in sharedBridgeSelfCheckOverview.familyItems"
                  :key="'shared-root-family-' + family.key"
                >
                  <div class="dashboard-shared-root-family-card-head">
                    <span class="dashboard-shared-root-family-card-title">{{ family.title }}</span>
                    <span class="status-badge status-badge-soft" :class="'tone-' + family.tone">
                      {{ family.statusText }}
                    </span>
                  </div>
                  <div class="hint">{{ family.summaryText }}</div>
                  <div class="hint">目录：{{ family.pathText }}</div>
                  <div class="hint">ready 记录：{{ family.readyEntryCount }} 条，可访问：{{ family.accessibleReadyCount }} 条，不可见：{{ family.missingReadyCount }} 条</div>
                  <div class="hint" v-if="family.latestDownloadedAt && family.latestDownloadedAt !== '-'">最近就绪：{{ family.latestDownloadedAt }}</div>
                  <div class="hint" v-if="family.sampleMissingPath">示例缺失路径：{{ family.sampleMissingPath }}</div>
                  <div class="hint" v-else-if="family.sampleReadyPath">示例文件：{{ family.sampleReadyPath }}</div>
                  <div class="hint" v-if="family.queryError">查询异常：{{ family.queryError }}</div>
                </article>
              </div>
            </article>

            <div class="dashboard-scheduler-overview-grid">
              <article
                class="task-block task-block-compact dashboard-scheduler-overview-card"
                v-for="item in schedulerOverviewItems"
                :key="'scheduler-overview-' + item.key"
              >
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">调度项</div>
                    <h3 class="card-title">{{ item.title }}</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">
                    {{ item.statusText }}
                  </span>
                </div>
                <div class="hint">{{ item.summaryText }}</div>
                <div class="dashboard-scheduler-part-grid">
                  <div class="dashboard-scheduler-part-card" v-for="part in item.parts" :key="item.key + '-' + part.label">
                    <div class="dashboard-scheduler-part-title">{{ part.label }}</div>
                    <div class="hint">执行时间：{{ part.runTimeText }}</div>
                    <div class="hint">下次执行：{{ part.nextRunText }}</div>
                    <div class="hint">最近触发：{{ part.lastTriggerText }}</div>
                    <div class="hint">最近结果：{{ part.resultText }}</div>
                  </div>
                </div>
                <div class="btn-line dashboard-scheduler-overview-action">
                  <button
                    class="btn btn-secondary"
                    type="button"
                    @click="openDashboardSchedulerOverviewTarget(item.moduleId, item.focusKey || '')"
                  >
                    进入对应模块
                  </button>
                </div>
              </article>
            </div>
          </div>
        </section>

`;
