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
                  <div class="status-metric-label">未启动调度</div>
                  <strong class="status-metric-value">{{ schedulerOverviewSummary.stoppedCount }} 项</strong>
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
              <div class="hint">最近即将执行：{{ schedulerOverviewSummary.nextSchedulerText }}</div>
              <div class="hint">待关注：{{ schedulerOverviewSummary.attentionText }}</div>
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
