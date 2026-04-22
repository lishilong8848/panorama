export const DASHBOARD_AUTO_FLOW_SECTION = `        <section class="content-card" v-if="!isInternalDeploymentRole && dashboardActiveModule === 'auto_flow'">
          <div class="dashboard-module-shell">
            <article class="task-block task-block-compact dashboard-module-scheduler-card">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">定时执行</div>
                  <h3 class="card-title">调度设置</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="health.scheduler.running ? 'tone-success' : 'tone-neutral'">
                  {{ health.scheduler.status || '未启动' }}
                </span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">执行间隔</div>
                  <strong class="status-metric-value">{{ Number(config.scheduler.interval_minutes || 0) > 0 ? ('每 ' + config.scheduler.interval_minutes + ' 分钟') : '-' }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">最近决策</div>
                  <strong class="status-metric-value">{{ schedulerDecisionText || '-' }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">当前状态</div>
                  <strong class="status-metric-value">{{ health.scheduler.status || '-' }}</strong>
                </div>
              </div>
              <div class="btn-line">
                <label class="label" style="min-width:unset;">执行间隔（分钟）</label>
                <input style="width:120px" type="number" min="1" step="1" v-model.number="config.scheduler.interval_minutes" @change="saveSchedulerQuickConfig" />
              </div>
              <div class="btn-line">
                <button class="btn btn-success" :disabled="getSchedulerEffectiveRunning('scheduler', health.scheduler.remembered_enabled) || isActionLocked(actionKeySchedulerStart) || isActionLocked(actionKeySchedulerStop) || isSchedulerTogglePending('scheduler')" @click="startScheduler">
                  {{ getSchedulerToggleMode('scheduler') === 'starting' ? '启动中...' : (getSchedulerToggleMode('scheduler') === 'stopping' ? '处理中...' : (getSchedulerEffectiveRunning('scheduler', health.scheduler.remembered_enabled) ? '已记住开启' : '启动调度')) }}
                </button>
                <button class="btn btn-danger" :disabled="!getSchedulerEffectiveRunning('scheduler', health.scheduler.remembered_enabled) || isActionLocked(actionKeySchedulerStop) || isActionLocked(actionKeySchedulerStart) || isSchedulerTogglePending('scheduler')" @click="stopScheduler">
                  {{ getSchedulerToggleMode('scheduler') === 'stopping' ? '停止中...' : (getSchedulerToggleMode('scheduler') === 'starting' ? '处理中...' : '停止调度') }}
                </button>
              </div>
              <div class="hint">{{ schedulerQuickSaving ? '调度配置保存中...' : '修改执行间隔后自动保存。' }}</div>
            </article>

            <div class="dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">执行入口</div>
                  <h3 class="card-title">立即执行自动流程</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="isActionLocked(actionKeyAutoOnce) ? 'tone-info' : 'tone-success'">
                  {{ isActionLocked(actionKeyAutoOnce) ? '执行中' : '可执行' }}
                </span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">当前网络</div>
                  <strong class="status-metric-value">{{ health.network.current_ssid || '-' }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">调度状态</div>
                  <strong class="status-metric-value">{{ health.scheduler.status || '-' }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">待续传任务</div>
                  <strong class="status-metric-value">{{ pendingResumeCount }}</strong>
                </div>
              </div>
              <div class="ops-focus-card">
                <div class="ops-focus-card-label">推荐路径</div>
                <div class="ops-focus-card-title">先执行标准自动流程，再按需继续断点续传</div>
                <div class="ops-focus-card-meta">{{ bridgeExecutionHint }}</div>
              </div>
              <div class="btn-line">
                <button class="btn btn-primary" :disabled="!canRun || isActionLocked(actionKeyAutoOnce)" @click="runAutoOnce">
                  {{ isActionLocked(actionKeyAutoOnce) ? '执行中...' : '立即执行自动流程' }}
                </button>
              </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">后续上传</div>
                    <h3 class="card-title">断点续传上传</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="pendingResumeCount > 0 ? 'tone-info' : 'tone-neutral'">
                    {{ pendingResumeCount > 0 ? ('待处理 ' + pendingResumeCount + ' 项') : '暂无待续传' }}
                  </span>
                </div>
                <div class="hint-stack">
                  <div class="hint">{{ resumeExecutionHint }}</div>
                  <div class="hint" v-if="pendingResumeRuns[0]">
                    最新任务：{{ formatResumeDateSummary(pendingResumeRuns[0]) }} / 待上传 {{ pendingResumeRuns[0].pending_upload_count }} 项
                  </div>
                </div>
                <div class="btn-line">
                  <button class="btn btn-primary" :disabled="!canRun || pendingResumeCount === 0 || isActionLocked(getResumeRunActionKey())" @click="runResumeUpload()">
                    {{ isActionLocked(getResumeRunActionKey()) ? '处理中...' : '继续上传（不重下）' }}
                  </button>
                  <button class="btn btn-danger" :disabled="pendingResumeCount === 0 || isActionLocked(getResumeDeleteAllActionKey())" @click="deleteAllResumeRuns">
                    {{ isActionLocked(getResumeDeleteAllActionKey()) ? '删除中...' : '删除全部待续传任务' }}
                  </button>
                </div>
              </article>
            </div>
          </div>

          <details class="module-advanced-section" v-if="pendingResumeCount > 0">
            <summary>查看待续传任务明细（{{ pendingResumeCount }} 项）</summary>
            <div class="module-advanced-section-body">
              <article class="task-block task-block-compact">
                <div class="resume-list">
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
                        :disabled="!canRun || !getResumeRunId(run) || isActionLocked(getResumeRunActionKey(getResumeRunId(run)))"
                        @click="runResumeUpload(getResumeRunId(run), false)"
                      >
                        {{ isActionLocked(getResumeRunActionKey(getResumeRunId(run))) ? '处理中...' : '继续该任务' }}
                      </button>
                      <button
                        class="btn btn-danger"
                        :disabled="!getResumeRunId(run) || isActionLocked(getResumeDeleteActionKey(getResumeRunId(run)))"
                        @click="deleteResumeRun(getResumeRunId(run))"
                      >
                        {{ isActionLocked(getResumeDeleteActionKey(getResumeRunId(run))) ? '删除中...' : '删除任务' }}
                      </button>
                    </div>
                  </div>
                </div>
              </article>
            </div>
          </details>
        </section>

`;

