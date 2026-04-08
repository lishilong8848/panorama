export const DASHBOARD_MULTI_DATE_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'multi_date'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-primary-grid">
              <article class="task-block">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">日期选择</div>
                  <h3 class="card-title">多日用电明细自动流程</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="selectedDateCount > 0 ? 'tone-info' : 'tone-neutral'">
                  {{ selectedDateCount > 0 ? ('已选 ' + selectedDateCount + ' 天') : '尚未选择日期' }}
                </span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">已选日期</div>
                  <strong class="status-metric-value">{{ selectedDateCount }} 天</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">待续传任务</div>
                  <strong class="status-metric-value">{{ pendingResumeCount }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">当前网络</div>
                  <strong class="status-metric-value">{{ health.network.current_ssid || '-' }}</strong>
                </div>
              </div>
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
              <div class="task-grid two-col">
                <div class="form-row">
                  <label class="label">单日快选</label>
                  <input type="date" v-model="selectedDate" />
                </div>
                <div class="form-row">
                  <label class="label">当前策略</label>
                  <div class="readonly-inline-card">按日期顺序自动下载并上传</div>
                </div>
              </div>
              <div class="btn-line">
                <button class="btn btn-secondary" @click="addDate">添加单日</button>
                <button class="btn btn-ghost" @click="clearDates">清空已选</button>
              </div>
              <div class="form-row">
                <div class="label">已选日期（从左到右，共 {{ selectedDateCount }} 天）</div>
                <div class="chips">
                  <span class="chip" v-for="d in selectedDates" :key="d">{{ d }}<button @click="removeDate(d)">×</button></span>
                </div>
              </div>
              </article>

              <article class="task-block task-block-accent dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">执行入口</div>
                    <h3 class="card-title">执行多日用电明细自动流程</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="isActionLocked(actionKeyMultiDate) ? 'tone-info' : 'tone-success'">
                    {{ isActionLocked(actionKeyMultiDate) ? '执行中' : '可执行' }}
                  </span>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">执行说明</div>
                  <div class="ops-focus-card-title">适合补跑连续日期，保持标准下载与上传链路</div>
                  <div class="ops-focus-card-meta">日期按升序执行。若某一天失败，不会中断其他日期。</div>
                </div>
                <div class="hint" v-if="isInternalDeploymentRole">当前为内网端，多日用电明细自动流程请在外网端发起。</div>
                <div class="btn-line">
                  <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyMultiDate)" @click="runMultiDate">
                    {{ isActionLocked(actionKeyMultiDate) ? '执行中...' : '执行多日用电明细自动流程' }}
                  </button>
                </div>
              </article>
            </div>

            <div class="dashboard-module-support-stack">
              <article class="task-block task-block-compact">
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
                  <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || pendingResumeCount === 0 || isActionLocked(getResumeRunActionKey())" @click="runResumeUpload()">
                    {{ isActionLocked(getResumeRunActionKey()) ? '处理中...' : '继续上传（不重下）' }}
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
              </article>
            </div>
          </details>
        </section>

`;
