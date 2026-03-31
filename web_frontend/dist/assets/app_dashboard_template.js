export const DASHBOARD_TEMPLATE = `<section v-else-if="showDashboardPageNav && isDashboardView" class="dashboard-layout">
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

        <section class="content-card ops-job-panel">
          <div class="task-block-head">
            <div>
              <div class="task-block-kicker">执行引擎</div>
              <h3 class="card-title">任务与资源</h3>
            </div>
            <span class="status-badge status-badge-soft" :class="runningJobs.length ? 'tone-info' : 'tone-neutral'">
              运行中 {{ runningJobs.length }} / 等待 {{ waitingResourceJobs.length }}
            </span>
          </div>
          <div class="ops-job-grid">
            <article class="task-block task-block-compact">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">运行中</div>
                  <h4 class="card-title">运行中任务</h4>
                </div>
              </div>
              <div class="ops-job-list" v-if="runningJobs.length">
                <button
                  v-for="job in runningJobs"
                  :key="'running-' + job.job_id"
                  class="btn btn-ghost ops-job-list-item"
                  :class="{ 'is-selected': selectedJobId === job.job_id }"
                  @click="focusJob(job)"
                >
                  <span class="ops-job-list-title">{{ job.name || job.feature || job.job_id }}</span>
                  <span class="ops-job-list-meta">{{ formatJobKind(job) }} | #{{ job.job_id }} | {{ formatJobStatus(job.status || 'running') }}</span>
                </button>
              </div>
              <div class="hint" v-else>当前没有运行中的任务。</div>
            </article>

            <article class="task-block task-block-compact">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">等待资源</div>
                  <h4 class="card-title">等待资源任务</h4>
                </div>
              </div>
              <div class="ops-job-list" v-if="waitingResourceJobs.length">
                <button
                  v-for="job in waitingResourceJobs"
                  :key="'waiting-' + job.job_id"
                  class="btn btn-ghost ops-job-list-item"
                  :class="{ 'is-selected': selectedJobId === job.job_id }"
                  @click="focusJob(job)"
                >
                  <span class="ops-job-list-title">{{ job.name || job.feature || job.job_id }}</span>
                  <span class="ops-job-list-meta">
                    {{ formatJobKind(job) }} | #{{ job.job_id }} | {{ formatJobWaitReason(job) }}
                  </span>
                </button>
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
                  内网可达：{{ formatBooleanReachability(resourceSnapshot.network?.internal_reachable) }}
                </div>
                <div class="readonly-inline-card">
                  外网可达：{{ formatBooleanReachability(resourceSnapshot.network?.external_reachable) }}
                </div>
                <div class="readonly-inline-card">
                  网络模式：{{ formatNetworkMode(resourceSnapshot.network?.mode) }}
                </div>
                <div class="readonly-inline-card">
                  内网队列：{{ resourceSnapshot.network?.queued_internal || 0 }}
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
          <div class="ops-job-history" v-if="recentFinishedJobs.length">
            <div class="label">最近完成任务</div>
            <div class="chips">
              <button
                v-for="job in recentFinishedJobs"
                :key="'recent-' + job.job_id"
                class="btn btn-ghost chip-button"
                @click="focusJob(job)"
              >
                {{ job.name || job.feature || job.job_id }} / {{ formatJobKind(job) }} / {{ formatJobStatus(job.status) }}
              </button>
            </div>
          </div>
          <article class="task-block task-block-compact" v-if="bridgeTasksEnabled" style="margin-top:12px;">
            <div class="task-block-head">
              <div>
                <div class="task-block-kicker">共享桥接</div>
                <h4 class="card-title">共享协同任务</h4>
              </div>
              <span class="status-badge status-badge-soft" :class="activeBridgeTasks.length ? 'tone-warning' : 'tone-neutral'">
                处理中 {{ activeBridgeTasks.length }} / 历史 {{ bridgeTasks.length }}
              </span>
            </div>
            <div class="hint">仅内网端 / 外网端角色显示。外网发起后，内网执行前段，完成后再回到外网继续后段。</div>
            <div class="ops-job-list" v-if="bridgeTasks.length">
              <div
                v-for="task in bridgeTasks"
                :key="'bridge-' + task.task_id"
                class="ops-job-list-item ops-job-list-row"
                :class="{ 'is-selected': selectedBridgeTaskId === task.task_id }"
              >
                <button
                  class="btn btn-ghost ops-job-list-main"
                  type="button"
                  @click="focusBridgeTask(task)"
                >
                  <span class="ops-job-list-title">{{ task.feature_label || formatBridgeFeature(task.feature) }}</span>
                  <span class="ops-job-list-meta">
                    #{{ task.task_id }} | {{ formatBridgeTaskStatus(task.status) }} | {{ task.updated_at || '-' }}
                  </span>
                  <span class="ops-job-list-meta">
                    {{ task.current_stage_name || formatBridgeStageSummary(task) }}
                  </span>
                  <span class="ops-job-list-meta" v-if="formatBridgeTaskError(task) !== '-'">
                    错误：{{ formatBridgeTaskError(task) }}
                  </span>
                </button>
                <div v-if="isExternalDeploymentRole" class="ops-job-inline-action-slot">
                  <button
                    v-if="canCancelBridgeTask(task)"
                    class="btn btn-secondary btn-mini ops-job-inline-action"
                    type="button"
                    :disabled="isActionLocked(getBridgeTaskCancelActionKey(task.task_id))"
                    @click="cancelBridgeTask(task.task_id)"
                  >
                    {{ isActionLocked(getBridgeTaskCancelActionKey(task.task_id)) ? '取消提交中...' : '取消任务' }}
                  </button>
                </div>
              </div>
            </div>
            <div class="hint" v-else>当前还没有共享协同任务。</div>
            <div class="ops-job-history" v-if="recentFinishedBridgeTasks.length" style="margin-top:10px;">
              <div class="label">最近完成的共享任务</div>
              <div class="chips">
                <button
                  v-for="task in recentFinishedBridgeTasks"
                  :key="'bridge-recent-' + task.task_id"
                  class="btn btn-ghost chip-button"
                  :class="{ 'is-selected': selectedBridgeTaskId === task.task_id }"
                  @click="focusBridgeTask(task)"
                >
                  {{ task.feature_label || formatBridgeFeature(task.feature) }} / {{ formatBridgeTaskStatus(task.status) }}
                </button>
              </div>
            </div>
          </article>
          <article class="task-block task-block-compact ops-job-detail" v-if="bridgeTasksEnabled && currentBridgeTask">
            <div class="task-block-head">
              <div>
                <div class="task-block-kicker">共享详情</div>
                <h4 class="card-title">当前选中共享任务</h4>
              </div>
              <span
                class="status-badge status-badge-soft"
                :class="'tone-' + formatBridgeTaskTone(currentBridgeTask.status)"
              >
                {{ formatBridgeTaskStatus(currentBridgeTask.status) }}
              </span>
            </div>
            <div class="ops-resource-grid">
              <div class="readonly-inline-card">任务：{{ currentBridgeTask.feature_label || formatBridgeFeature(currentBridgeTask.feature) }}</div>
              <div class="readonly-inline-card">编号：{{ currentBridgeTask.task_id || '-' }}</div>
              <div class="readonly-inline-card">当前阶段：{{ formatBridgeStageSummary(currentBridgeTask) }}</div>
              <div class="readonly-inline-card">更新时间：{{ currentBridgeTask.updated_at || '-' }}</div>
            </div>
            <div
              class="hint"
              v-if="(String(currentBridgeTask.current_stage_error || '').trim() || formatBridgeTaskError(currentBridgeTask)) && formatBridgeTaskError(currentBridgeTask) !== '-'"
            >
              最近错误：{{ String(currentBridgeTask.current_stage_error || '').trim() || formatBridgeTaskError(currentBridgeTask) }}
            </div>
            <div class="btn-line" style="margin:8px 0 4px;">
              <button
                class="btn btn-secondary"
                :disabled="!currentBridgeTask || !canCancelBridgeTask(currentBridgeTask) || isActionLocked(getBridgeTaskCancelActionKey(currentBridgeTask.task_id))"
                @click="cancelBridgeTask(currentBridgeTask.task_id)"
              >
                {{ isActionLocked(getBridgeTaskCancelActionKey(currentBridgeTask?.task_id)) ? '取消提交中...' : '取消共享任务' }}
              </button>
              <button
                class="btn btn-secondary"
                :disabled="!currentBridgeTask || !['failed', 'partial_failed', 'cancelled', 'stale'].includes(String(currentBridgeTask.status || '').trim().toLowerCase()) || isActionLocked(getBridgeTaskRetryActionKey(currentBridgeTask.task_id))"
                @click="retryBridgeTask(currentBridgeTask.task_id)"
              >
                {{ isActionLocked(getBridgeTaskRetryActionKey(currentBridgeTask?.task_id)) ? '重试提交中...' : '重试共享任务' }}
              </button>
            </div>
          </article>
          <article class="task-block task-block-compact ops-job-detail" v-if="currentJob">
            <div class="task-block-head">
              <div>
                <div class="task-block-kicker">任务详情</div>
                <h4 class="card-title">当前选中任务</h4>
              </div>
              <span
                class="status-badge status-badge-soft"
                :class="'tone-' + formatJobTone(currentJob.status)"
              >
                {{ formatJobStatus(currentJob.status) }}
              </span>
            </div>
            <div class="btn-line" style="margin-bottom:8px;">
              <button
                class="btn btn-secondary"
                :disabled="!currentJob || !['queued', 'waiting_resource', 'running'].includes(String(currentJob.status || '').trim().toLowerCase()) || Boolean(currentJob.cancel_requested) || isActionLocked(getJobCancelActionKey(currentJob.job_id))"
                @click="cancelCurrentJob"
              >
                {{
                  Boolean(currentJob?.cancel_requested)
                    ? '取消请求中...'
                    : isActionLocked(getJobCancelActionKey(currentJob?.job_id))
                      ? '取消中...'
                      : '取消任务'
                }}
              </button>
              <button
                class="btn btn-ghost"
                :disabled="!currentJob || !['failed', 'cancelled', 'interrupted'].includes(String(currentJob.status || '').trim().toLowerCase()) || !Array.isArray(currentJob.stages) || !String(currentJob.stages[0]?.worker_handler || '').trim() || isActionLocked(getJobRetryActionKey(currentJob.job_id))"
                @click="retryCurrentJob"
              >
                {{ isActionLocked(getJobRetryActionKey(currentJob?.job_id)) ? '重试提交中...' : '重试任务' }}
              </button>
            </div>
            <div class="ops-resource-grid">
              <div class="readonly-inline-card">名称：{{ currentJob.name || currentJob.feature || '-' }}</div>
              <div class="readonly-inline-card">类型：{{ formatJobKind(currentJob) }}</div>
              <div class="readonly-inline-card">编号：{{ currentJob.job_id || '-' }}</div>
              <div class="readonly-inline-card">来源：{{ formatJobSubmittedBy(currentJob.submitted_by) }}</div>
              <div class="readonly-inline-card">优先级：{{ formatJobPriority(currentJob.priority) }}</div>
              <div class="readonly-inline-card">等待原因：{{ currentJob.wait_reason ? formatJobWaitReason(currentJob) : '-' }}</div>
              <div class="readonly-inline-card">提交时间：{{ currentJob.created_at || '-' }}</div>
              <div class="readonly-inline-card">开始时间：{{ currentJob.started_at || '-' }}</div>
              <div class="readonly-inline-card">结束时间：{{ currentJob.finished_at || '-' }}</div>
              <div class="readonly-inline-card">
                资源：{{ Array.isArray(currentJob.resource_keys) && currentJob.resource_keys.length ? currentJob.resource_keys.join(' / ') : '-' }}
              </div>
            </div>
            <div class="hint" v-if="currentJob.summary" style="margin-top:8px;">摘要：{{ currentJob.summary }}</div>
            <div class="hint" v-if="currentJob.error">错误：{{ currentJob.error }}</div>
            <div class="ops-stage-table-wrap" v-if="Array.isArray(currentJob.stages) && currentJob.stages.length">
              <table class="ops-stage-table">
                <thead>
                  <tr>
                    <th>阶段</th>
                    <th>状态</th>
                    <th>资源</th>
                    <th>恢复策略</th>
                    <th>心跳</th>
                    <th>开始</th>
                    <th>结束</th>
                    <th>摘要/错误</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="stage in currentJob.stages" :key="'stage-' + currentJob.job_id + '-' + stage.stage_id">
                    <td>{{ stage.name || stage.stage_id || '-' }}</td>
                    <td>
                      <span class="status-badge status-badge-soft" :class="'tone-' + formatJobStageTone(stage)">
                        {{ formatJobStageStatus(stage) }}
                      </span>
                    </td>
                    <td>{{ Array.isArray(stage.resource_keys) && stage.resource_keys.length ? stage.resource_keys.join(' / ') : '-' }}</td>
                    <td>{{ stage.resume_policy || '-' }}</td>
                    <td>{{ stage.last_heartbeat_at || '-' }}</td>
                    <td>{{ stage.started_at || '-' }}</td>
                    <td>{{ stage.finished_at || '-' }}</td>
                    <td>{{ stage.error || stage.summary || '-' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </article>
        </section>

        <section class="content-card" v-if="!isInternalDeploymentRole && dashboardActiveModule === 'auto_flow'">
          <h3 class="card-title">立即执行自动流程</h3>
          <div class="form-row hint" v-if="deploymentRoleMode === 'switching'">按当前配置时间窗执行：固定先切到内网下载，再切到外网计算并上传飞书。</div>
          <div class="form-row hint" v-else>{{ bridgeExecutionHint }}</div>
          <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyAutoOnce)" @click="runAutoOnce">
            {{ isActionLocked(actionKeyAutoOnce) ? '执行中...' : '立即执行自动流程' }}
          </button>
          <div class="hint" v-if="isInternalDeploymentRole" style="margin-top:8px;">
            当前为内网端，自动流程请在外网端发起；内网端只负责共享桥接前置下载阶段。
          </div>

          <div class="hr"></div>
          <div class="form-row"><label class="label">断点续传上传</label></div>
          <div class="hint">{{ resumeExecutionHint }}</div>
          <div class="btn-line" style="margin-top:8px;">
            <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || pendingResumeCount === 0 || isActionLocked(getResumeRunActionKey())" @click="runResumeUpload()">
              {{ isActionLocked(getResumeRunActionKey()) ? '处理中...' : '继续上传（不重下）' }}
            </button>
          </div>
          <div class="hint" v-if="isInternalDeploymentRole" style="margin-top:8px;">
            当前为内网端，断点续传请在外网端执行；外网端会从共享桥接产物继续上传。
          </div>
          <div class="form-row" style="margin-top:8px;" v-if="pendingResumeCount === 0">
            <div class="hint">当前没有待续传任务。</div>
          </div>
          <div class="resume-list" v-else>
            <div class="resume-item" v-for="run in pendingResumeRuns" :key="'auto_' + ((run.run_id || '-') + '_' + (run.run_save_dir || '-'))">
              <div class="resume-row">
                <span class="resume-key">run_id</span>
                <span class="resume-val">{{ getResumeRunId(run) || '-' }}</span>
              </div>
              <div class="resume-row">
                <span class="resume-key">待上传</span>
                <span class="resume-val">{{ run.pending_upload_count }} 项</span>
              </div>
              <div class="resume-row">
                <span class="resume-key">日期范围</span>
                <span class="resume-val" :title="formatResumeDateFull(run)">{{ formatResumeDateSummary(run) }}</span>
              </div>
              <div class="resume-row">
                <span class="resume-key">更新时间</span>
                <span class="resume-val">{{ run.updated_at || '-' }}</span>
              </div>
              <div class="btn-line" style="margin-top:6px;">
                <button
                  class="btn btn-secondary"
                  :disabled="isInternalDeploymentRole || !canRun || !getResumeRunId(run) || isActionLocked(getResumeRunActionKey(getResumeRunId(run)))"
                  @click="runResumeUpload(getResumeRunId(run), false)"
                >
                  {{ isActionLocked(getResumeRunActionKey(getResumeRunId(run))) ? '处理中...' : '继续该任务' }}
                </button>
                <button
                  class="btn btn-danger"
                  :disabled="isInternalDeploymentRole || !canRun || !getResumeRunId(run) || isActionLocked(getResumeDeleteActionKey(getResumeRunId(run)))"
                  @click="deleteResumeRun(getResumeRunId(run))"
                >
                  {{ isActionLocked(getResumeDeleteActionKey(getResumeRunId(run))) ? '删除中...' : '删除任务' }}
                </button>
              </div>
            </div>
          </div>

          <div class="hr"></div>
          <div class="form-row"><label class="label">调度设置</label></div>
          <div class="form-row">
            <label><input type="checkbox" v-model="config.scheduler.enabled" /> 启用调度</label>
            <label><input type="checkbox" v-model="config.scheduler.auto_start_in_gui" /> 启动后自动开启</label>
          </div>
          <div class="btn-line">
            <label class="label" style="min-width:unset;">每日执行时间</label>
            <input style="width:120px" type="time" step="1" v-model="config.scheduler.run_time" />
          </div>
          <div class="btn-line">
            <button class="btn btn-success" :disabled="isInternalDeploymentRole || isActionLocked(actionKeySchedulerStart)" @click="startScheduler">
              {{ isActionLocked(actionKeySchedulerStart) ? '启动中...' : '启动调度' }}
            </button>
            <button class="btn btn-danger" :disabled="isInternalDeploymentRole || isActionLocked(actionKeySchedulerStop)" @click="stopScheduler">
              {{ isActionLocked(actionKeySchedulerStop) ? '停止中...' : '停止调度' }}
            </button>
            <button class="btn btn-secondary" :disabled="schedulerQuickSaving || isActionLocked(actionKeySchedulerSave)" @click="saveSchedulerQuickConfig">
              {{ schedulerQuickSaving || isActionLocked(actionKeySchedulerSave) ? '保存中...' : '保存调度配置' }}
            </button>
          </div>
          <div class="hint" v-if="isInternalDeploymentRole" style="margin-top:8px;">
            当前为内网端，该调度请在外网端启用；内网端只消费共享桥接任务。
          </div>
        </section>

        <section class="content-card" v-if="dashboardActiveModule === 'multi_date'">
          <h3 class="card-title">多日期自动流程</h3>
          <div class="range-card">
            <div class="range-head">区间选择</div>
            <div class="range-grid">
              <div class="form-row">
                <label class="label">开始日期</label>
                <input type="date" v-model="rangeStartDate" />
              </div>
              <div class="form-row">
                <label class="label">结束日期</label>
                <input type="date" v-model="rangeEndDate" />
              </div>
            </div>
            <div class="hint">会自动展开成区间内每一天（包含开始和结束日期）。</div>
            <div class="btn-line" style="margin-top:8px;">
              <button class="btn btn-secondary" @click="addDateRange">按区间添加</button>
              <button class="btn btn-ghost" @click="quickRangeToday">区间设为今天</button>
            </div>
          </div>
          <div class="form-row" style="margin-top:10px;">
            <label class="label">单日快选</label>
            <input type="date" v-model="selectedDate" />
          </div>
          <div class="btn-line">
            <button class="btn btn-secondary" @click="addDate">添加单日</button>
            <button class="btn btn-ghost" @click="clearDates">清空已选</button>
            <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyMultiDate)" @click="runMultiDate">
              {{ isActionLocked(actionKeyMultiDate) ? '执行中...' : '执行多日期自动流程' }}
            </button>
          </div>
          <div class="hint" v-if="isInternalDeploymentRole">当前为内网端，多日期自动流程请在外网端发起。</div>
          <div class="form-row">
            <div class="label">已选日期（从左到右，共 {{ selectedDateCount }} 天）</div>
            <div class="chips">
              <span class="chip" v-for="d in selectedDates" :key="d">{{ d }}<button @click="removeDate(d)">×</button></span>
            </div>
          </div>
          <div class="hr"></div>
          <div class="form-row"><label class="label">断点续传上传</label></div>
          <div class="hint">{{ resumeExecutionHint }}</div>
          <div class="btn-line" style="margin-top:8px;">
            <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || pendingResumeCount === 0 || isActionLocked(getResumeRunActionKey())" @click="runResumeUpload()">
              {{ isActionLocked(getResumeRunActionKey()) ? '处理中...' : '继续上传（不重下）' }}
            </button>
          </div>
          <div class="hint" v-if="isInternalDeploymentRole" style="margin-top:8px;">当前为内网端，断点续传请在外网端执行。</div>
          <div class="form-row" style="margin-top:8px;" v-if="pendingResumeCount === 0">
            <div class="hint">当前没有待续传任务。</div>
          </div>
          <div class="resume-list" v-else>
            <div class="resume-item" v-for="run in pendingResumeRuns" :key="'multi_' + ((run.run_id || '-') + '_' + (run.run_save_dir || '-'))">
              <div class="resume-row">
                <span class="resume-key">run_id</span>
                <span class="resume-val">{{ getResumeRunId(run) || '-' }}</span>
              </div>
              <div class="resume-row">
                <span class="resume-key">待上传</span>
                <span class="resume-val">{{ run.pending_upload_count }} 项</span>
              </div>
              <div class="resume-row">
                <span class="resume-key">日期范围</span>
                <span class="resume-val" :title="formatResumeDateFull(run)">{{ formatResumeDateSummary(run) }}</span>
              </div>
              <div class="resume-row">
                <span class="resume-key">更新时间</span>
                <span class="resume-val">{{ run.updated_at || '-' }}</span>
              </div>
              <div class="btn-line" style="margin-top:6px;">
                <button
                  class="btn btn-secondary"
                  :disabled="isInternalDeploymentRole || !canRun || !getResumeRunId(run) || isActionLocked(getResumeRunActionKey(getResumeRunId(run)))"
                  @click="runResumeUpload(getResumeRunId(run), false)"
                >
                  {{ isActionLocked(getResumeRunActionKey(getResumeRunId(run))) ? '处理中...' : '继续该任务' }}
                </button>
                <button
                  class="btn btn-danger"
                  :disabled="isInternalDeploymentRole || !canRun || !getResumeRunId(run) || isActionLocked(getResumeDeleteActionKey(getResumeRunId(run)))"
                  @click="deleteResumeRun(getResumeRunId(run))"
                >
                  {{ isActionLocked(getResumeDeleteActionKey(getResumeRunId(run))) ? '删除中...' : '删除任务' }}
                </button>
              </div>
            </div>
          </div>
        </section>

        <section class="content-card" v-if="dashboardActiveModule === 'manual_upload'">
          <h3 class="card-title">手动补传（月报）</h3>
          <div class="hint">手动补传仅使用已选择文件，不执行内网下载。</div>
          <div class="form-row">
            <label class="label">楼栋</label>
            <select v-model="manualBuilding">
              <option v-for="b in config.input.buildings" :key="b" :value="b">{{ b }}</option>
            </select>
          </div>
          <div class="form-row">
            <label class="label">上传日期</label>
            <input type="date" v-model="manualUploadDate" />
          </div>
          <div class="form-row">
            <label class="label">表格文件</label>
            <input type="file" accept=".xlsx" @change="onManualFileChange" />
          </div>
          <div class="hint">{{ externalExecutionHint }}</div>
          <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyManualUpload)" @click="runManualUpload">
            {{ isActionLocked(actionKeyManualUpload) ? '提交中...' : '开始手动补传' }}
          </button>
          <div class="hint" v-if="isInternalDeploymentRole" style="margin-top:8px;">当前为内网端，手动补传请在外网端执行。</div>
        </section>

        <section class="content-card" v-if="dashboardActiveModule === 'sheet_import'">
          <h3 class="card-title">5Sheet 导入（清空后导入）</h3>
          <div class="form-row">
            <label class="label">5Sheet 文件</label>
            <input type="file" accept=".xlsx" @change="onSheetFileChange" />
          </div>
          <div class="hint">{{ externalExecutionHint }}</div>
          <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeySheetImport)" @click="runSheetImport">
            {{ isActionLocked(actionKeySheetImport) ? '提交中...' : '清空并上传 5 个工作表' }}
          </button>
          <div class="hint" v-if="isInternalDeploymentRole" style="margin-top:8px;">当前为内网端，5Sheet 导入请在外网端执行。</div>
        </section>

        <section class="content-card" v-if="dashboardActiveModule === 'handover_log'">
          <div class="handover-task-shell-redesign">
            <div class="handover-top-grid">
              <article class="task-block">
                <div class="task-block-head">
                  <div>
                    <h3 class="card-title">交接班日志生成参数</h3>
                    <div class="hint">默认自动判断班次：09:00 前为前一天夜班，09:00-18:00 为当天白班，18:00 后为当天夜班。</div>
                    <div class="hint">上方楼栋、日期、班次会优先读取共享文件；缺失时再提交历史补采，不影响下方“从已有数据表生成”。</div>
                  </div>
                  <span class="status-badge status-badge-soft" :class="handoverDutyAutoFollow ? 'tone-info' : 'tone-warning'">
                    {{ handoverDutyAutoLabel }}
                  </span>
                </div>

                <div class="task-grid two-col">
                  <div class="form-row">
                    <label class="label">下载范围</label>
                    <select v-model="handoverDownloadScope">
                      <option value="single">单楼栋</option>
                      <option value="all_enabled">全部启用楼栋</option>
                    </select>
                  </div>
                  <div class="form-row" v-if="handoverDownloadScope === 'single'">
                    <label class="label">楼栋</label>
                    <select v-model="manualBuilding">
                      <option v-for="b in config.input.buildings" :key="'handover-' + b" :value="b">{{ b }}</option>
                    </select>
                  </div>
                  <div class="form-row" v-else>
                    <label class="label">楼栋范围</label>
                    <div class="readonly-inline-card">将按配置中已启用楼栋批量执行</div>
                  </div>
                  <div class="form-row">
                    <label class="label">交接班日期</label>
                    <input type="date" v-model="handoverDutyDate" @change="onHandoverDutyDateManualChange" />
                  </div>
                  <div class="form-row">
                    <label class="label">班次</label>
                    <select v-model="handoverDutyShift" @change="onHandoverDutyShiftManualChange">
                      <option value="day">白班（08:00-17:00）</option>
                      <option value="night">夜班（17:00-次日08:00）</option>
                    </select>
                  </div>
                </div>

                <div class="btn-line">
                  <button class="btn btn-secondary" :disabled="handoverDutyAutoFollow" @click="restoreAutoHandoverDuty">
                    恢复自动
                  </button>
                </div>
              </article>

              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">推荐操作</div>
                    <h3 class="card-title">执行交接班流程</h3>
                  </div>
                  <span class="status-badge status-badge-solid" :class="'tone-' + handoverReviewOverview.tone">
                    {{ handoverReviewOverview.summaryText }}
                  </span>
                </div>
                <div class="btn-stack">
                  <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || handoverGenerationBusy || isActionLocked(actionKeyHandoverFromDownload) || isActionLocked(actionKeyHandoverFromFile)" @click="runHandoverFromDownload">
                    {{ handoverGenerationBusy || isActionLocked(actionKeyHandoverFromDownload) || isActionLocked(actionKeyHandoverFromFile) ? '执行中...' : '使用共享文件生成' }}
                  </button>
                  <button class="btn btn-secondary" :disabled="isInternalDeploymentRole || !canRun || handoverGenerationBusy || !hasSelectedHandoverFiles || isActionLocked(actionKeyHandoverFromFile) || isActionLocked(actionKeyHandoverFromDownload)" @click="runHandoverFromFile">
                    {{ handoverGenerationBusy || isActionLocked(actionKeyHandoverFromFile) || isActionLocked(actionKeyHandoverFromDownload) ? '执行中...' : '从已有数据表生成' }}
                  </button>
                </div>
                <div class="action-reason action-reason-warning" v-if="isInternalDeploymentRole">
                  当前为内网端，该模块请在外网端发起；内网端只消费共享桥接任务。
                </div>
                <div class="action-reason action-reason-warning" v-if="handoverGenerationBusy">
                  当前已有交接班日志生成任务在执行或排队，请等待任务完成后再发起新的交接班生成。
                </div>
                <div class="action-reason action-reason-warning" v-else-if="!hasSelectedHandoverFiles">
                  请先为至少一个楼选择已有数据表文件，再执行“从已有数据表生成”。
                </div>
                <div class="hint" v-else>
                  本次将生成 {{ handoverSelectedFileCount }} 个楼，未选择文件的楼将跳过。
                </div>
              </article>
            </div>

            <div class="handover-middle-stack">
              <article class="task-block file-state-panel">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">文件状态</div>
                    <h3 class="card-title">已有数据表文件</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="hasSelectedHandoverFiles ? 'tone-success' : 'tone-neutral'">
                    {{ hasSelectedHandoverFiles ? ('已选择 ' + handoverSelectedFileCount + ' 个楼') : '尚未选择文件' }}
                  </span>
                </div>
                <div class="hint">每个楼单独选择一个源数据表文件。选择了文件的楼才会参与本次生成，未选择的楼将跳过。</div>
                <div class="review-board-grid" style="margin-top:10px;">
                  <div
                    class="review-board-item"
                    v-for="building in handoverConfiguredBuildings"
                    :key="'handover-file-' + building"
                  >
                    <div class="review-board-item-top">
                      <strong>{{ building }}</strong>
                      <span
                        class="status-badge status-badge-soft"
                        :class="handoverFileStatesByBuilding[building] && handoverFileStatesByBuilding[building].state === 'selected' ? 'tone-success' : 'tone-neutral'"
                      >
                        {{ handoverFileStatesByBuilding[building] ? handoverFileStatesByBuilding[building].label : '未选择' }}
                      </span>
                    </div>
                    <div
                      class="file-state-name"
                      v-if="handoverFileStatesByBuilding[building] && handoverFileStatesByBuilding[building].filename"
                    >
                      {{ handoverFileStatesByBuilding[building].filename }}
                    </div>
                    <div class="file-state-name is-empty" v-else>尚未选择 Excel 文件</div>
                    <div class="hint">
                      {{ handoverFileStatesByBuilding[building] ? handoverFileStatesByBuilding[building].helper : '未选择文件时，该楼将跳过。' }}
                    </div>
                    <div class="form-row" style="margin-top:10px;">
                      <input type="file" accept=".xlsx" @change="onHandoverBuildingFileChange(building, $event)" />
                    </div>
                  </div>
                </div>
              </article>

              <article class="task-block handover-daily-report-panel">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">日报多维</div>
                    <h3 class="card-title">自动截图与日报记录</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + handoverDailyReportExportVm.tone">
                    {{ handoverDailyReportExportVm.text }}
                  </span>
                </div>
                <div class="hint">日报多维表记录只在“一键全确认”且本批次云文档全部成功后自动写入。</div>
                <div class="hint">两张截图默认自动截取。若图不正确，可放大查看、重新截图，或手工上传/粘贴替换后再手动重写日报记录。</div>
                <div class="handover-daily-report-meta">
                  <div class="form-row">
                    <label class="label">当前日期</label>
                    <div class="readonly-inline-card">{{ handoverDutyDate || '-' }}</div>
                  </div>
                  <div class="form-row">
                    <label class="label">当前班次</label>
                    <div class="readonly-inline-card">{{ handoverDutyShift === 'day' ? '白班' : '夜班' }}</div>
                  </div>
                  <div class="form-row">
                    <label class="label">截图登录态</label>
                    <div class="readonly-inline-card">
                      <span class="status-badge status-badge-soft" :class="'tone-' + handoverDailyReportAuthVm.tone">
                        {{ handoverDailyReportAuthVm.text }}
                      </span>
                    </div>
                  </div>
                  <div class="form-row">
                    <label class="label">最近写入</label>
                    <div class="readonly-inline-card">
                      {{ handoverDailyReportContext.daily_report_record_export.updated_at || '-' }}
                    </div>
                  </div>
                </div>
                <div class="hint" v-if="handoverDailyReportContext.screenshot_auth.last_checked_at">
                  最近检测：{{ handoverDailyReportContext.screenshot_auth.last_checked_at }}
                </div>
                <div class="hint" v-if="handoverDailyReportAuthVm.error">
                  {{ handoverDailyReportAuthVm.error }}
                </div>
                <div class="btn-line" style="margin-top:10px;">
                  <button
                    class="btn btn-secondary"
                    :disabled="isActionLocked(actionKeyHandoverDailyReportAuthOpen)"
                    @click="openHandoverDailyReportScreenshotAuth"
                  >
                    {{ isActionLocked(actionKeyHandoverDailyReportAuthOpen) ? '打开中...' : '初始化飞书截图登录态' }}
                  </button>
                  <button
                    class="btn btn-secondary"
                    :disabled="isActionLocked(actionKeyHandoverDailyReportScreenshotTest)"
                    @click="runHandoverDailyReportScreenshotTest"
                  >
                    {{ isActionLocked(actionKeyHandoverDailyReportScreenshotTest) ? '测试中...' : '截图测试' }}
                  </button>
                </div>
                <div class="form-row" style="margin-top:10px;">
                  <label class="label">云文档链接</label>
                  <a
                    v-if="handoverDailyReportSpreadsheetUrl"
                    class="handover-access-url"
                    :href="handoverDailyReportSpreadsheetUrl"
                    target="_blank"
                    rel="noopener noreferrer"
                  >{{ handoverDailyReportSpreadsheetUrl }}</a>
                  <div v-else class="readonly-inline-card">当前尚未记录云文档链接</div>
                </div>
                <div class="handover-daily-report-grid">
                  <div class="content-card handover-daily-report-card">
                    <div class="btn-line" style="justify-content:space-between; align-items:center;">
                      <strong>{{ handoverDailyReportCaptureAssets.summarySheetImage.title }}</strong>
                      <span class="status-badge status-badge-soft" :class="handoverDailyReportCaptureAssets.summarySheetImage.source === 'manual' ? 'tone-warning' : handoverDailyReportCaptureAssets.summarySheetImage.source === 'auto' ? 'tone-info' : 'tone-neutral'">
                        {{ handoverDailyReportCaptureAssets.summarySheetImage.sourceText }}
                      </span>
                    </div>
                    <div
                      v-if="handoverDailyReportCaptureAssets.summarySheetImage.exists"
                      style="margin-top:10px; border:1px solid rgba(255,255,255,.12); border-radius:10px; overflow:hidden; background:#0f172a; cursor:pointer;"
                      @click="openHandoverDailyReportPreview('summary_sheet')"
                    >
                      <img
                        :src="handoverDailyReportCaptureAssets.summarySheetImage.thumbnail_url || handoverDailyReportCaptureAssets.summarySheetImage.preview_url"
                        alt="今日航图截图"
                        style="display:block; width:100%; max-height:180px; object-fit:cover;"
                      />
                    </div>
                    <div v-else class="readonly-inline-card" style="margin-top:10px;">当前还没有今日航图截图</div>
                    <div class="hint" style="margin-top:8px;">
                      最近测试：
                      <span class="status-badge status-badge-soft" :class="'tone-' + handoverDailyReportSummaryTestVm.tone">
                        {{ handoverDailyReportSummaryTestVm.text }}
                      </span>
                    </div>
                    <div class="hint">
                      <span class="status-badge status-badge-soft" :class="'tone-' + handoverDailyReportCaptureAssets.summarySheetImage.lastWrittenSourceTone">
                        {{ handoverDailyReportCaptureAssets.summarySheetImage.lastWrittenSourceText }}
                      </span>
                    </div>
                    <div class="hint" v-if="handoverDailyReportCaptureAssets.summarySheetImage.captured_at">
                      生效时间：{{ handoverDailyReportCaptureAssets.summarySheetImage.captured_at }}
                    </div>
                    <div class="hint" v-if="handoverDailyReportSummaryTestVm.error">{{ handoverDailyReportSummaryTestVm.error }}</div>
                    <div class="hint">自动截取固定飞书页面并向下滚动拼接完整长图，卡片中仅显示低清预览。</div>
                    <div class="btn-line" style="margin-top:10px; flex-wrap:wrap;">
                      <button
                        class="btn btn-secondary"
                        :disabled="!handoverDailyReportCaptureAssets.summarySheetImage.exists"
                        @click="openHandoverDailyReportPreview('summary_sheet')"
                      >放大查看</button>
                      <button
                        class="btn btn-secondary"
                        :disabled="isActionLocked(getHandoverDailyReportRecaptureActionKey('summary_sheet'))"
                        @click="recaptureHandoverDailyReportAsset('summary_sheet')"
                      >{{ isActionLocked(getHandoverDailyReportRecaptureActionKey('summary_sheet')) ? '重截中...' : '重新截图' }}</button>
                      <button class="btn btn-secondary" @click="openHandoverDailyReportUploadDialog('summary_sheet')">上传/粘贴替换</button>
                      <button
                        v-if="handoverDailyReportCaptureAssets.summarySheetImage.hasManual"
                        class="btn btn-ghost"
                        :disabled="isActionLocked(getHandoverDailyReportRestoreActionKey('summary_sheet'))"
                        @click="restoreHandoverDailyReportAutoAsset('summary_sheet')"
                      >{{ isActionLocked(getHandoverDailyReportRestoreActionKey('summary_sheet')) ? '恢复中...' : '恢复自动图' }}</button>
                    </div>
                  </div>

                  <div class="content-card handover-daily-report-card">
                    <div class="btn-line" style="justify-content:space-between; align-items:center;">
                      <strong>{{ handoverDailyReportCaptureAssets.externalPageImage.title }}</strong>
                      <span class="status-badge status-badge-soft" :class="handoverDailyReportCaptureAssets.externalPageImage.source === 'manual' ? 'tone-warning' : handoverDailyReportCaptureAssets.externalPageImage.source === 'auto' ? 'tone-info' : 'tone-neutral'">
                        {{ handoverDailyReportCaptureAssets.externalPageImage.sourceText }}
                      </span>
                    </div>
                    <div
                      v-if="handoverDailyReportCaptureAssets.externalPageImage.exists"
                      style="margin-top:10px; border:1px solid rgba(255,255,255,.12); border-radius:10px; overflow:hidden; background:#0f172a; cursor:pointer;"
                      @click="openHandoverDailyReportPreview('external_page')"
                    >
                      <img
                        :src="handoverDailyReportCaptureAssets.externalPageImage.thumbnail_url || handoverDailyReportCaptureAssets.externalPageImage.preview_url"
                        alt="排班截图"
                        style="display:block; width:100%; max-height:180px; object-fit:cover;"
                      />
                    </div>
                    <div v-else class="readonly-inline-card" style="margin-top:10px;">当前还没有排班截图</div>
                    <div class="hint" style="margin-top:8px;">
                      最近测试：
                      <span class="status-badge status-badge-soft" :class="'tone-' + handoverDailyReportExternalTestVm.tone">
                        {{ handoverDailyReportExternalTestVm.text }}
                      </span>
                    </div>
                    <div class="hint">
                      <span class="status-badge status-badge-soft" :class="'tone-' + handoverDailyReportCaptureAssets.externalPageImage.lastWrittenSourceTone">
                        {{ handoverDailyReportCaptureAssets.externalPageImage.lastWrittenSourceText }}
                      </span>
                    </div>
                    <div class="hint" v-if="handoverDailyReportCaptureAssets.externalPageImage.captured_at">
                      生效时间：{{ handoverDailyReportCaptureAssets.externalPageImage.captured_at }}
                    </div>
                    <div class="hint" v-if="handoverDailyReportExternalTestVm.error">{{ handoverDailyReportExternalTestVm.error }}</div>
                    <div class="hint">卡片中仅显示低清预览，点击后查看完整截图。</div>
                    <div class="btn-line" style="margin-top:10px; flex-wrap:wrap;">
                      <button
                        class="btn btn-secondary"
                        :disabled="!handoverDailyReportCaptureAssets.externalPageImage.exists"
                        @click="openHandoverDailyReportPreview('external_page')"
                      >放大查看</button>
                      <button
                        class="btn btn-secondary"
                        :disabled="isActionLocked(getHandoverDailyReportRecaptureActionKey('external_page'))"
                        @click="recaptureHandoverDailyReportAsset('external_page')"
                      >{{ isActionLocked(getHandoverDailyReportRecaptureActionKey('external_page')) ? '重截中...' : '重新截图' }}</button>
                      <button class="btn btn-secondary" @click="openHandoverDailyReportUploadDialog('external_page')">上传/粘贴替换</button>
                      <button
                        v-if="handoverDailyReportCaptureAssets.externalPageImage.hasManual"
                        class="btn btn-ghost"
                        :disabled="isActionLocked(getHandoverDailyReportRestoreActionKey('external_page'))"
                        @click="restoreHandoverDailyReportAutoAsset('external_page')"
                      >{{ isActionLocked(getHandoverDailyReportRestoreActionKey('external_page')) ? '恢复中...' : '恢复自动图' }}</button>
                    </div>
                  </div>
                </div>
                <div class="hint" v-if="handoverDailyReportExportVm.error">
                  {{ handoverDailyReportExportVm.error }}
                </div>
                <div class="btn-line" style="margin-top:12px;">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRewriteHandoverDailyReportRecord || isActionLocked(actionKeyHandoverDailyReportRecordRewrite)"
                    @click="rewriteHandoverDailyReportRecord"
                  >
                    {{ isActionLocked(actionKeyHandoverDailyReportRecordRewrite) ? '重写中...' : '重新写入日报多维表' }}
                  </button>
                </div>
                <div class="hint" v-if="!canRewriteHandoverDailyReportRecord">
                  需要当前批次已有云文档链接，且两张截图都存在，才能重写日报多维表记录。
                </div>
              </article>
            </div>

            <div class="handover-bottom-grid">
              <article class="handover-access-panel review-board-panel">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">审核访问</div>
                    <div class="handover-access-title">当前 5 个楼页面访问地址</div>
                  </div>
                  <span class="status-inline-note" v-if="handoverReviewOverview.batchKey">批次 {{ handoverReviewOverview.batchKey }}</span>
                </div>
                <div class="btn-line" v-if="canShowHandoverCloudRetryAll" style="margin-bottom:8px;">
                  <button
                    class="btn btn-secondary"
                    @click="retryAllFailedHandoverCloudSync"
                    :disabled="isHandoverCloudRetryAllDisabled"
                  >
                    {{ handoverCloudRetryAllButtonText }}
                  </button>
                </div>
                <div class="hint" v-if="handoverReviewOverview.dutyText">
                  本次上传云文档批次：{{ handoverReviewOverview.dutyText }}
                </div>
                <div class="hint">以下地址可直接发给局域网内对应楼栋电脑访问。</div>
                <div class="hint" v-if="health.handover.review_base_url_effective">
                  当前生效地址（{{ health.handover.review_base_url_effective_source === 'manual' ? '手工指定' : '已缓存自动诊断结果' }}）：{{ health.handover.review_base_url_effective }}
                </div>
                <div class="hint" v-else-if="health.handover.review_base_url_error">
                  {{ health.handover.review_base_url_error }}
                </div>
                <div class="hint" v-else-if="health.handover.review_base_url_status === 'no_candidate'">
                  未检测到可用私网 IPv4 地址
                </div>
                <div class="handover-access-empty" v-if="!handoverReviewBoardRows.length">暂未获取到局域网访问地址。</div>
                <div class="review-board-grid" v-if="handoverReviewBoardRows.length">
                  <div class="review-board-item" v-for="row in handoverReviewBoardRows" :key="'handover-board-' + row.building">
                    <div class="review-board-item-top">
                      <strong>{{ row.building }}</strong>
                      <span class="status-badge status-badge-soft" :class="'tone-' + row.tone">{{ row.text }}</span>
                    </div>
                    <div class="hint" style="margin-top:4px;">
                      云表同步：
                      <span class="status-badge status-badge-soft" :class="'tone-' + row.cloudSheetSyncTone">{{ row.cloudSheetSyncText }}</span>
                    </div>
                    <a
                      v-if="row.hasUrl"
                      class="handover-access-url"
                      :href="row.url"
                      target="_blank"
                      rel="noopener noreferrer"
                    >{{ row.url }}</a>
                    <div class="handover-access-empty" v-else>当前无可访问地址</div>
                    <a
                      v-if="row.hasCloudSheetUrl"
                      class="handover-access-url"
                      :href="row.cloudSheetUrl"
                      target="_blank"
                      rel="noopener noreferrer"
                    >打开云文档</a>
                    <div class="hint" v-if="row.cloudSheetError">{{ row.cloudSheetError }}</div>
                  </div>
                </div>
              </article>

              <article class="task-block">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">调度入口</div>
                    <h3 class="card-title">交接班调度</h3>
                  </div>
                  <span
                    class="status-badge status-badge-soft"
                    :class="health.handover_scheduler.running ? 'tone-success' : 'tone-neutral'"
                  >
                    {{ health.handover_scheduler.status || '-' }}
                  </span>
                </div>
                <div class="hint">上午时间点用于补跑前一天夜班，下午时间点用于执行当天白班。</div>
                <div class="hint" v-if="!health.handover_scheduler.executor_bound">
                  当前调度执行器未绑定，保存配置后也不会自动执行。
                </div>

                <div class="task-grid two-col" style="margin-top:10px;">
                  <div class="form-row">
                    <label><input type="checkbox" v-model="config.handover_log.scheduler.enabled" /> 启用调度</label>
                  </div>
                  <div class="form-row">
                    <label><input type="checkbox" v-model="config.handover_log.scheduler.auto_start_in_gui" /> 启动后自动开启</label>
                  </div>
                  <div class="form-row">
                    <label class="label">上午时间</label>
                    <input type="time" step="1" v-model="config.handover_log.scheduler.morning_time" />
                  </div>
                  <div class="form-row">
                    <label class="label">下午时间</label>
                    <input type="time" step="1" v-model="config.handover_log.scheduler.afternoon_time" />
                  </div>
                </div>

                <div class="form-row" style="margin-top:6px;">
                  <label class="label">上午下次执行</label>
                  <div class="readonly-inline-card">
                    {{ (health.handover_scheduler.morning && health.handover_scheduler.morning.next_run_time) || '-' }}
                  </div>
                </div>
                <div class="form-row">
                  <label class="label">下午下次执行</label>
                  <div class="readonly-inline-card">
                    {{ (health.handover_scheduler.afternoon && health.handover_scheduler.afternoon.next_run_time) || '-' }}
                  </div>
                </div>

                <div class="btn-line" style="margin-top:10px;">
                  <button
                    class="btn btn-success"
                    :disabled="isInternalDeploymentRole || health.handover_scheduler.running || isActionLocked(actionKeyHandoverSchedulerStart)"
                    @click="startHandoverScheduler"
                  >
                    {{
                      isActionLocked(actionKeyHandoverSchedulerStart)
                        ? '启动中...'
                        : (health.handover_scheduler.running ? '已启动调度' : '启动调度')
                    }}
                  </button>
                  <button
                    class="btn btn-danger"
                    :disabled="isInternalDeploymentRole || !health.handover_scheduler.running || isActionLocked(actionKeyHandoverSchedulerStop)"
                    @click="stopHandoverScheduler"
                  >
                    {{ isActionLocked(actionKeyHandoverSchedulerStop) ? '停止中...' : '停止调度' }}
                  </button>
                  <button
                    class="btn btn-secondary"
                    :disabled="handoverSchedulerQuickSaving || isActionLocked(actionKeyHandoverSchedulerSave)"
                    @click="saveHandoverSchedulerQuickConfig"
                  >
                    {{ handoverSchedulerQuickSaving || isActionLocked(actionKeyHandoverSchedulerSave) ? '保存中...' : '保存调度配置' }}
                  </button>
                </div>
                <div class="hint" v-if="isInternalDeploymentRole" style="margin-top:8px;">
                  当前为内网端，交接班调度请在外网端启用；内网端只负责共享桥接下载阶段。
                </div>
              </article>
            </div>
          </div>
        </section>

        <section class="content-card" v-if="dashboardActiveModule === 'day_metric_upload'">
          <div class="day-metric-shell">
            <div class="day-metric-top-grid">
              <article class="task-block">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">独立执行</div>
                    <h3 class="card-title">12项执行参数</h3>
                  </div>
                  <div class="btn-line">
                    <span class="status-badge status-badge-soft tone-info">固定白班</span>
                    <span
                      class="status-badge status-badge-soft"
                      :class="deploymentRoleMode === 'switching' ? 'tone-success' : 'tone-neutral'"
                    >
                      {{
                        deploymentRoleMode === 'switching'
                          ? '单机切网端'
                          : (deploymentRoleMode === 'internal' ? '内网端' : '外网端')
                      }}
                    </span>
                  </div>
                </div>

                <div class="hint">该模块只上传 12 项，不生成交接班日志，不进入审核流程。</div>
                <div class="hint">
                  {{
                    deploymentRoleMode === 'switching'
                      ? '当前为单机切网端，固定按切网流程执行。'
                      : (deploymentRoleMode === 'internal'
                        ? '当前为内网端，请在外网端发起；内网端只负责准备共享文件。'
                        : '当前为外网端，默认优先读取共享文件；缺失时再等待内网端补采。')
                  }}
                </div>
                <div class="hint" v-if="!dayMetricUploadEnabled">当前配置已禁用 12 项独立上传，可在配置中心开启。</div>

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
                <div class="hint">执行顺序：按日期升序、楼栋配置顺序逐个执行。失败单元不会中断其他日期或楼栋。</div>
                <div class="hint">{{ bridgeExecutionHint }}</div>
                <div class="btn-stack" style="margin-top:8px;">
                  <button
                    class="btn btn-primary"
                    :disabled="isInternalDeploymentRole || !dayMetricUploadEnabled || !canRun || isActionLocked(actionKeyDayMetricFromDownload)"
                    @click="runDayMetricFromDownload"
                  >
                    {{ isActionLocked(actionKeyDayMetricFromDownload) ? '执行中...' : '使用共享文件上传12项' }}
                  </button>
                </div>
                <div class="hint">当前选择：{{ dayMetricUploadScope === 'all_enabled' ? '全部启用楼栋' : (dayMetricUploadBuilding || '-') }} / {{ dayMetricSelectedDateCount }} 天</div>
                <div class="hint" v-if="isInternalDeploymentRole">当前为内网端，12项任务请在外网端发起；内网端只负责共享桥接下载阶段。</div>
              </article>
            </div>

            <article class="task-block">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">补录入口</div>
                  <h3 class="card-title">本地文件补录</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="dayMetricLocalImportEnabled ? 'tone-info' : 'tone-neutral'">
                  {{ dayMetricLocalImportEnabled ? '已启用' : '已禁用' }}
                </span>
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
                  :disabled="isInternalDeploymentRole || !dayMetricLocalImportEnabled || !canRun || isActionLocked(actionKeyDayMetricFromFile)"
                  @click="runDayMetricFromFile"
                >
                  {{ isActionLocked(actionKeyDayMetricFromFile) ? '补录中...' : '开始补录12项' }}
                </button>
              </div>
              <div class="hint" v-if="isInternalDeploymentRole">当前为内网端，本地文件补录也请在外网端执行。</div>
            </article>

            <article class="task-block" v-if="dayMetricCurrentPayload">
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
                <div class="readonly-inline-card">执行网络：{{ dayMetricCurrentPayload.network_auto_switch_enabled ? '单机切网流程' : '当前角色固定网络' }}</div>
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
        </section>
        <section class="content-card" v-if="!isInternalDeploymentRole && dashboardActiveModule === 'wet_bulb_collection'">
          <h3 class="card-title">湿球温度定时采集</h3>
          <div class="hint">复用交接班日志规则引擎提取“天气湿球温度”和“冷源运行模式”，不读取源表物理 D7/F7。</div>
          <div class="hint">同一天同楼栋仅保留最新一条；冷源运行模式按全楼优先级归并后写入多维表。</div>
          <div class="hint">{{ bridgeExecutionHint }}</div>

          <div class="hr"></div>
          <div class="form-row"><label class="label">当前目标多维表</label></div>
          <div class="day-metric-summary-grid">
            <div class="readonly-inline-card">配置 Token：{{ wetBulbConfiguredTarget.configuredAppToken || '-' }}</div>
            <div class="readonly-inline-card">Table ID：{{ wetBulbConfiguredTarget.tableId || '-' }}</div>
            <div class="readonly-inline-card">目标类型：{{ formatWetBulbTargetKind(wetBulbConfiguredTarget.targetKind) }}</div>
          </div>
          <div class="hint" v-if="wetBulbConfiguredTarget.operationAppToken && wetBulbConfiguredTarget.operationAppToken !== wetBulbConfiguredTarget.configuredAppToken">
            实际上传 Token：{{ wetBulbConfiguredTarget.operationAppToken }}
          </div>
          <div class="hint" v-if="wetBulbConfiguredTarget.url">
            当前配置链接：
            <a :href="wetBulbConfiguredTarget.url" target="_blank" rel="noopener noreferrer">{{ wetBulbConfiguredTarget.url }}</a>
          </div>
          <div class="hint" v-else-if="wetBulbConfiguredTarget.message">当前配置状态：{{ wetBulbConfiguredTarget.message }}</div>
          <div class="hint" v-else>当前尚未配置湿球温度目标多维表的 App Token / Table ID。</div>
          <div class="hint" v-if="wetBulbConfiguredTarget.resolvedAt">最近解析时间：{{ wetBulbConfiguredTarget.resolvedAt }}</div>
          <div class="hint" v-if="wetBulbLatestRunTarget.url">
            最近一次执行目标：
            <a :href="wetBulbLatestRunTarget.url" target="_blank" rel="noopener noreferrer">{{ wetBulbLatestRunTarget.url }}</a>
          </div>
          <div class="hint" v-else-if="wetBulbLatestRunTarget.message">最近一次执行状态：{{ wetBulbLatestRunTarget.message }}</div>
          <div class="hint" v-if="wetBulbLatestRunTarget.targetKind">
            最近一次目标类型：{{ formatWetBulbTargetKind(wetBulbLatestRunTarget.targetKind) }}
          </div>

          <div class="hr"></div>
          <div class="form-row"><label class="label">运行参数</label></div>
          <div class="btn-line" style="margin-top:8px;">
            <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyWetBulbCollectionRun)" @click="runWetBulbCollection">
              {{ isActionLocked(actionKeyWetBulbCollectionRun) ? '执行中...' : '立即运行一次' }}
            </button>
          </div>
          <div class="hint" v-if="isInternalDeploymentRole">当前为内网端，湿球温度任务请在外网端发起；内网端只负责共享桥接前置下载。</div>

          <div class="hr"></div>
          <div class="form-row"><label class="label">调度状态</label></div>
          <div class="hint">状态：{{ health.wet_bulb_collection.scheduler.status || '-' }}</div>
          <div class="hint">下次执行：{{ health.wet_bulb_collection.scheduler.next_run_time || '-' }}</div>
          <div class="hint">最近触发：{{ health.wet_bulb_collection.scheduler.last_trigger_at || '-' }} / {{ wetBulbSchedulerTriggerText || '-' }}</div>

          <div class="hr"></div>
          <div class="form-row"><label class="label">调度参数</label></div>
          <div class="form-row">
            <label class="label">每 N 分钟运行一次</label>
            <input type="number" min="1" v-model.number="config.wet_bulb_collection.scheduler.interval_minutes" />
          </div>
          <div class="form-row">
            <label class="label">检查间隔（秒）</label>
            <input type="number" min="1" v-model.number="config.wet_bulb_collection.scheduler.check_interval_sec" />
          </div>
          <div class="btn-line">
            <button
              class="btn btn-success"
              :disabled="isInternalDeploymentRole || health.wet_bulb_collection.scheduler.running || isActionLocked(actionKeyWetBulbSchedulerStart)"
              @click="startWetBulbCollectionScheduler"
            >
              {{
                isActionLocked(actionKeyWetBulbSchedulerStart)
                  ? '启动中...'
                  : (health.wet_bulb_collection.scheduler.running ? '已启动调度' : '启动调度')
              }}
            </button>
            <button
              class="btn btn-danger"
              :disabled="isInternalDeploymentRole || !health.wet_bulb_collection.scheduler.running || isActionLocked(actionKeyWetBulbSchedulerStop)"
              @click="stopWetBulbCollectionScheduler"
            >
              {{ isActionLocked(actionKeyWetBulbSchedulerStop) ? '停止中...' : '停止调度' }}
            </button>
            <button class="btn btn-secondary" :disabled="wetBulbSchedulerQuickSaving || isActionLocked(actionKeyWetBulbSchedulerSave)" @click="saveWetBulbCollectionSchedulerQuickConfig">
              {{ wetBulbSchedulerQuickSaving || isActionLocked(actionKeyWetBulbSchedulerSave) ? '保存中...' : '保存调度配置' }}
            </button>
          </div>
          <div class="hint" v-if="isInternalDeploymentRole" style="margin-top:8px;">
            当前为内网端，湿球温度调度请在外网端启用；内网端只负责共享桥接前置阶段。
          </div>
        </section>

        <section class="content-card log-wrap" v-if="dashboardActiveModule === 'runtime_logs'">
          <div class="log-toolbar">
            <div>
              <h3 class="card-title" style="margin-bottom:4px;">运行日志</h3>
              <div class="hint">任务状态: {{ currentJob ? currentJob.status : '-' }} {{ currentJob ? ('| 任务编号=' + currentJob.job_id) : '' }}</div>
            </div>
            <div class="btn-line">
              <input type="text" v-model="logFilter" placeholder="关键字过滤日志" style="width:220px" />
              <button class="btn btn-secondary" @click="clearLogs">清空日志</button>
            </div>
          </div>
          <div id="logBox" class="log-box">{{ filteredLogs.join('\\n') }}</div>
        </section>

        <div
          v-if="handoverDailyReportPreviewModal.open"
          style="position:fixed; inset:0; background:rgba(15,23,42,.82); z-index:1200; display:flex; align-items:center; justify-content:center; padding:24px;"
          @click.self="closeHandoverDailyReportPreview"
        >
          <div class="content-card" style="width:min(1100px, 96vw); max-height:92vh; overflow:auto; padding:16px;">
            <div class="btn-line" style="justify-content:space-between; align-items:center; margin-bottom:12px;">
              <strong>{{ handoverDailyReportPreviewModal.title || '截图预览' }}</strong>
              <div class="btn-line">
                <a class="btn btn-secondary" :href="handoverDailyReportPreviewModal.imageUrl" :download="handoverDailyReportPreviewModal.downloadName || '日报截图.png'">下载图片</a>
                <button class="btn btn-ghost" @click="closeHandoverDailyReportPreview">关闭</button>
              </div>
            </div>
            <img
              :src="handoverDailyReportPreviewModal.imageUrl"
              alt="日报截图预览"
              style="display:block; width:100%; height:auto; border-radius:10px; background:#0f172a;"
            />
          </div>
        </div>

        <div
          v-if="handoverDailyReportUploadModal.open"
          style="position:fixed; inset:0; background:rgba(15,23,42,.82); z-index:1200; display:flex; align-items:center; justify-content:center; padding:24px;"
          @click.self="closeHandoverDailyReportUploadDialog"
        >
          <div class="content-card" style="width:min(560px, 94vw); padding:16px;">
            <div class="btn-line" style="justify-content:space-between; align-items:center; margin-bottom:12px;">
              <strong>{{ handoverDailyReportUploadModal.title || '上传截图' }}</strong>
              <button class="btn btn-ghost" @click="closeHandoverDailyReportUploadDialog">关闭</button>
            </div>
            <div class="hint">{{ handoverDailyReportUploadModal.hint }}</div>
            <div class="form-row" style="margin-top:12px;">
              <label class="label">选择图片文件</label>
              <input
                type="file"
                accept=".png,.jpg,.jpeg,.webp,image/png,image/jpeg,image/webp"
                @change="onHandoverDailyReportAssetFileChange(handoverDailyReportUploadModal.target, $event)"
              />
            </div>
            <div
              tabindex="0"
              class="readonly-inline-card"
              style="margin-top:12px; min-height:120px; display:flex; align-items:center; justify-content:center; text-align:center; white-space:normal; cursor:text;"
              @paste="onHandoverDailyReportUploadPaste"
            >
              点击此处后按 Ctrl+V，可直接粘贴剪贴板图片
            </div>
          </div>
        </div>
      </div>
    </section>`;


