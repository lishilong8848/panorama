import { DASHBOARD_AUTO_FLOW_SECTION } from "./dashboard_template_sections/dashboard_auto_flow_section.js";
import { DASHBOARD_MULTI_DATE_SECTION } from "./dashboard_template_sections/dashboard_multi_date_section.js";
import { DASHBOARD_SCHEDULER_OVERVIEW_SECTION } from "./dashboard_template_sections/dashboard_scheduler_overview_section.js";
import { DASHBOARD_MANUAL_UPLOAD_SECTION } from "./dashboard_template_sections/dashboard_manual_upload_section.js";
import { DASHBOARD_SHEET_IMPORT_SECTION } from "./dashboard_template_sections/dashboard_sheet_import_section.js";
import { DASHBOARD_HANDOVER_LOG_SECTION } from "./dashboard_template_sections/dashboard_handover_log_section.js";
import { DASHBOARD_DAY_METRIC_UPLOAD_SECTION } from "./dashboard_template_sections/dashboard_day_metric_upload_section.js";
import { DASHBOARD_BRANCH_POWER_UPLOAD_SECTION } from "./dashboard_template_sections/dashboard_branch_power_upload_section.js";
import { DASHBOARD_TOP5_POWER_REPORT_SECTION } from "./dashboard_template_sections/dashboard_top5_power_report_section.js";
import { DASHBOARD_WET_BULB_COLLECTION_SECTION } from "./dashboard_template_sections/dashboard_wet_bulb_collection_section.js";
import { DASHBOARD_CHILLER_MODE_UPLOAD_SECTION } from "./dashboard_template_sections/dashboard_chiller_mode_upload_section.js";
import { DASHBOARD_MONTHLY_EVENT_REPORT_SECTION } from "./dashboard_template_sections/dashboard_monthly_event_report_section.js";
import { DASHBOARD_ALARM_EVENT_UPLOAD_SECTION } from "./dashboard_template_sections/dashboard_alarm_event_upload_section.js";

export const DASHBOARD_TEMPLATE = `<section v-if="showDashboardPageNav && isDashboardView" class="dashboard-layout">
      <aside class="content-card dashboard-menu" :class="{ 'is-open': dashboardModuleMenuOpen }">
        <div class="dashboard-menu-head">
          <span>业务模块</span>
        </div>
        <div v-for="group in dashboardMenuGroups" :key="group.id" class="dashboard-menu-group">
          <div class="dashboard-menu-group-title">{{ group.title }}</div>
          <button
            v-for="module in group.items"
            :key="module.id"
            :class="['btn', 'dashboard-menu-button', dashboardActiveModule === module.id ? 'btn-primary is-active' : 'btn-ghost']"
            @click="setDashboardActiveModule(module.id)"
          >
            <span class="dashboard-menu-item-title">{{ module.title }}</span>
            <small class="dashboard-menu-item-desc">{{ module.desc || '' }}</small>
          </button>
        </div>
      </aside>

      <div class="dashboard-drawer-mask" v-if="dashboardModuleMenuOpen" @click="closeDashboardMenuDrawer"></div>

      <div class="dashboard-main">
        <section class="content-card module-topbar module-hero">
          <button class="btn btn-secondary menu-toggle" @click="openDashboardMenuDrawer">模块菜单</button>
          <div class="module-hero-copy">
            <div class="module-kicker">{{ dashboardActiveModuleHero.eyebrow }}</div>
            <div class="module-title">{{ dashboardActiveModuleHero.title }}</div>
            <div class="module-hero-desc">{{ dashboardActiveModuleHero.description }}</div>
          </div>
          <div class="module-hero-metrics" v-if="dashboardActiveModuleHero.metrics && dashboardActiveModuleHero.metrics.length">
            <div class="module-hero-metric" v-for="metric in dashboardActiveModuleHero.metrics" :key="dashboardActiveModule + '-' + metric.label">
              <span class="module-hero-metric-label">{{ metric.label }}</span>
              <strong class="module-hero-metric-value">{{ metric.value }}</strong>
            </div>
          </div>
        </section>

        <section class="content-card" style="margin-bottom:12px;">
          <article class="task-block task-block-compact">
            <div class="task-block-head">
              <div>
                <div class="task-block-kicker">执行反馈</div>
                <h4 class="card-title">当前任务</h4>
              </div>
              <span class="status-badge status-badge-soft" :class="'tone-' + currentTaskOverview.tone">
                {{ currentTaskOverview.statusText }}
              </span>
            </div>
            <div class="hint">{{ currentTaskOverview.summaryText }}</div>
            <div class="hint" v-if="currentTaskOverview.focusTitle && currentTaskOverview.focusTitle !== '当前没有选中任务'">
              当前重点：{{ currentTaskOverview.focusTitle }}
            </div>
            <div class="hint" v-if="currentTaskOverview.focusMeta && currentTaskOverview.focusTitle && currentTaskOverview.focusTitle !== '当前没有选中任务'">
              {{ currentTaskOverview.focusMeta }}
            </div>
            <div class="hint" v-if="currentTaskOverview.nextActionText">{{ currentTaskOverview.nextActionText }}</div>
            <div class="status-metric-grid status-metric-grid-compact" v-if="currentTaskOverview.items && currentTaskOverview.items.length">
              <div
                class="status-metric"
                v-for="item in currentTaskOverview.items.slice(0, 3)"
                :key="'dashboard-task-overview-' + item.label"
              >
                <div class="status-metric-label">{{ item.label }}</div>
                <strong class="status-metric-value">{{ item.value }}</strong>
              </div>
            </div>
          </article>
        </section>

        <section class="content-card ops-job-panel">
          <div class="task-block-head">
            <div>
              <div class="task-block-kicker">执行引擎</div>
              <h3 class="card-title">任务与资源</h3>
            </div>
            <span class="status-badge status-badge-soft" :class="'tone-' + (taskPanelOverview.tone || 'neutral')">
              {{ taskPanelOverview.statusText || '当前空闲' }}
            </span>
          </div>
          <div class="hint">{{ taskPanelOverview.summaryText }}</div>
          <div
            class="status-metric-grid status-metric-grid-compact"
            v-if="taskPanelOverview.items && taskPanelOverview.items.length"
          >
            <div
              class="status-metric"
              v-for="item in taskPanelOverview.items.slice(0, 3)"
              :key="'task-panel-overview-' + item.label"
            >
              <div class="status-metric-label">{{ item.label }}</div>
              <strong class="status-metric-value">{{ item.value }}</strong>
            </div>
          </div>
          <div class="hint" v-else>等待后端任务摘要。</div>
          <div class="ops-job-grid">
            <article class="task-block task-block-compact">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">运行中</div>
                  <h4 class="card-title">运行中任务</h4>
                </div>
                <span class="status-badge status-badge-soft" :class="taskPanelOverview.runningCount ? 'tone-info' : 'tone-neutral'">
                  {{ taskPanelOverview.runningCount ? (taskPanelOverview.runningCount + ' 项') : '无' }}
                </span>
              </div>
              <div class="ops-job-list" v-if="runningJobs.length">
                <div
                  v-for="job in runningJobs"
                  :key="'running-' + job.job_id"
                  class="ops-job-list-item ops-job-list-row"
                  :class="{ 'is-selected': selectedJobId === job.job_id }"
                >
                  <button
                    class="btn btn-ghost ops-job-list-main"
                    type="button"
                    @click="focusJobInRuntimeLogs(job)"
                  >
                    <span class="ops-job-list-title">{{ getRuntimeTaskTitle(job) }}</span>
                    <span class="ops-job-list-meta">{{ getRuntimeTaskMeta(job) }}</span>
                    <span class="ops-job-list-meta" v-if="getRuntimeTaskDetail(job)">{{ getRuntimeTaskDetail(job) }}</span>
                  </button>
                  <div class="ops-job-inline-action-slot">
                    <button
                      v-if="isRuntimeTaskActionVisible(job, 'cancel')"
                      class="btn btn-secondary btn-mini ops-job-inline-action"
                      type="button"
                      :title="getRuntimeTaskActionDisabledReason(job, 'cancel')"
                      :disabled="isRuntimeTaskActionLocked(job, 'cancel')"
                      @click="cancelRuntimeTask(job)"
                    >
                      {{ getRuntimeTaskActionLabel(job, 'cancel') }}
                    </button>
                  </div>
                </div>
              </div>
              <div class="hint" v-else>当前没有运行中的任务。</div>
            </article>

            <article class="task-block task-block-compact">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">等待资源</div>
                  <h4 class="card-title">等待资源任务</h4>
                </div>
                <span class="status-badge status-badge-soft" :class="taskPanelOverview.waitingCount ? 'tone-warning' : 'tone-neutral'">
                  {{ taskPanelOverview.waitingCount ? (taskPanelOverview.waitingCount + ' 项') : '无' }}
                </span>
              </div>
              <div class="ops-job-list" v-if="waitingResourceJobs.length">
                <div
                  v-for="job in waitingResourceJobs"
                  :key="'waiting-' + (job.__waiting_id || job.job_id || job.task_id)"
                  class="ops-job-list-item ops-job-list-row"
                  :class="{ 'is-selected': isWaitingResourceItemSelected(job) }"
                >
                  <button
                    class="btn btn-ghost ops-job-list-main"
                    type="button"
                    @click="focusWaitingResourceItemInRuntimeLogs(job)"
                  >
                    <span class="ops-job-list-title">{{ getRuntimeTaskTitle(job) }}</span>
                    <span class="ops-job-list-meta">{{ getRuntimeTaskMeta(job) }}</span>
                    <span class="ops-job-list-meta" v-if="getRuntimeTaskDetail(job)">{{ getRuntimeTaskDetail(job) }}</span>
                  </button>
                  <div class="ops-job-inline-action-slot">
                    <button
                      v-if="isRuntimeTaskActionVisible(job, 'cancel')"
                      class="btn btn-secondary btn-mini ops-job-inline-action"
                      type="button"
                      :title="getRuntimeTaskActionDisabledReason(job, 'cancel')"
                      :disabled="isRuntimeTaskActionLocked(job, 'cancel')"
                      @click="cancelRuntimeTask(job)"
                    >
                      {{ getRuntimeTaskActionLabel(job, 'cancel') }}
                    </button>
                  </div>
                </div>
              </div>
              <div class="hint" v-else>当前没有等待资源的任务。</div>
            </article>

            <article class="task-block task-block-compact" v-if="showRuntimeNetworkPanel">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">公共资源</div>
                  <h4 class="card-title">资源状态</h4>
                </div>
              </div>
              <div class="ops-resource-grid">
                <div class="readonly-inline-card">
                  网络窗口：{{ formatNetworkWindowSide(resourceSnapshot.network?.current_side) }}
                </div>
                <div class="readonly-inline-card">
                  当前网络：{{ formatDetectedNetworkSide(resourceSnapshot.network?.current_detected_side) }}
                </div>
                <div class="readonly-inline-card">
                  当前 WiFi：{{ resourceSnapshot.network?.current_ssid || '-' }}
                </div>
                <div class="readonly-inline-card">
                  SSID 侧：{{ formatSsidSide(resourceSnapshot.network?.ssid_side) }}
                </div>
                <div class="readonly-inline-card">
                  采集端网络可达：{{ formatBooleanReachability(resourceSnapshot.network?.internal_reachable) }}
                </div>
                <div class="readonly-inline-card">
                  外网可达：{{ formatBooleanReachability(resourceSnapshot.network?.external_reachable) }}
                </div>
                <div class="readonly-inline-card">
                  网络模式：{{ formatNetworkMode(resourceSnapshot.network?.mode) }}
                </div>
                <div class="readonly-inline-card">
                  采集端队列：{{ resourceSnapshot.network?.queued_internal || 0 }}
                </div>
                <div class="readonly-inline-card">
                  外网队列：{{ resourceSnapshot.network?.queued_external || 0 }}
                </div>
                <div class="readonly-inline-card">
                  浏览器队列：{{ resourceSnapshot.controlled_browser?.queue_length || 0 }}
                </div>
              </div>
            </article>
          </div>
          <article class="task-block task-block-compact" v-if="bridgeTasksEnabled" style="margin-top:12px;">
            <div class="task-block-head">
              <div>
                <div class="task-block-kicker">共享补采</div>
                <h4 class="card-title">补采同步任务</h4>
              </div>
              <span class="status-badge status-badge-soft" :class="'tone-' + (bridgeTaskPanelOverview?.tone || 'neutral')">
                {{ bridgeTaskPanelOverview?.statusText || '等待后端摘要' }}
              </span>
            </div>
            <div class="hint">{{ bridgeTaskPanelOverview?.summaryText || '暂无共享桥接任务。' }}</div>
            <div class="hint">缺文件任务会先由采集端补采，再由原任务自动继续处理。</div>
            <div class="hint">这里只保留任务摘要和操作入口，详细排障信息不再占用首页区域。</div>
            <div class="ops-job-list" v-if="displayedBridgeTasks.length">
              <div
                v-for="task in displayedBridgeTasks"
                :key="'bridge-' + task.task_id"
                class="ops-job-list-item ops-job-list-row"
                :class="{ 'is-selected': selectedBridgeTaskId === task.task_id }"
              >
                <button
                  class="btn btn-ghost ops-job-list-main"
                  type="button"
                  @click="focusBridgeTaskInRuntimeLogs(task)"
                >
                  <span class="ops-job-list-title">{{ getRuntimeTaskTitle(task) }}</span>
                  <span class="ops-job-list-meta">{{ getRuntimeTaskMeta(task) }}</span>
                  <span class="ops-job-list-meta" v-if="getRuntimeTaskDetail(task)">{{ getRuntimeTaskDetail(task) }}</span>
                </button>
                <div class="ops-job-inline-action-slot">
                  <button
                    v-if="isRuntimeTaskActionVisible(task, 'cancel')"
                    class="btn btn-secondary btn-mini ops-job-inline-action"
                    type="button"
                    :title="getRuntimeTaskActionDisabledReason(task, 'cancel')"
                    :disabled="isRuntimeTaskActionLocked(task, 'cancel')"
                    @click="cancelRuntimeTask(task)"
                  >
                    {{ getRuntimeTaskActionLabel(task, 'cancel') }}
                  </button>
                </div>
              </div>
            </div>
            <div class="hint" v-else>当前还没有补采同步任务。</div>
          </article>
          <div class="hint" style="margin-top:10px;">这里只保留任务摘要和操作入口。</div>
        </section>

${DASHBOARD_SCHEDULER_OVERVIEW_SECTION}
${DASHBOARD_AUTO_FLOW_SECTION}
${DASHBOARD_MULTI_DATE_SECTION}
${DASHBOARD_MANUAL_UPLOAD_SECTION}
${DASHBOARD_SHEET_IMPORT_SECTION}
${DASHBOARD_HANDOVER_LOG_SECTION}
${DASHBOARD_DAY_METRIC_UPLOAD_SECTION}
${DASHBOARD_BRANCH_POWER_UPLOAD_SECTION}
${DASHBOARD_TOP5_POWER_REPORT_SECTION}
${DASHBOARD_WET_BULB_COLLECTION_SECTION}
${DASHBOARD_CHILLER_MODE_UPLOAD_SECTION}
${DASHBOARD_MONTHLY_EVENT_REPORT_SECTION}
${DASHBOARD_ALARM_EVENT_UPLOAD_SECTION}
      </div>
    </section>`;




