export const DASHBOARD_MANUAL_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'manual_upload'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">补传入口</div>
                    <h3 class="card-title">手动补传（月报）</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="manualFile ? 'tone-info' : 'tone-neutral'">
                    {{ manualFile ? '已选择文件' : '待选择文件' }}
                  </span>
                </div>
                <div class="hint">手动补传仅使用已选择文件，不触发采集端下载。</div>
                <div class="task-grid two-col">
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
                </div>
                <div class="form-row">
                  <label class="label">表格文件</label>
                  <input type="file" accept=".xlsx" @change="onManualFileChange" />
                </div>
                <div class="hint">{{ externalExecutionHint }}</div>
                <div class="btn-line">
                  <button class="btn btn-primary" :disabled="!canRun || isActionLocked(actionKeyManualUpload)" @click="runManualUpload">
                    {{ isActionLocked(actionKeyManualUpload) ? '提交中...' : '开始手动补传' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">当前状态</div>
                    <h3 class="card-title">补传条件概览</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="manualFile ? 'tone-success' : 'tone-warning'">
                    {{ manualFile ? '可提交' : '待补文件' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">楼栋</div>
                    <strong class="status-metric-value">{{ manualBuilding || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">上传日期</div>
                    <strong class="status-metric-value">{{ manualUploadDate || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">文件状态</div>
                    <strong class="status-metric-value">{{ manualFile ? '已选择' : '未选择' }}</strong>
                  </div>
                </div>
                <div class="hint">当前文件：{{ manualFile ? manualFile.name : '尚未选择 Excel 文件' }}</div>
                <div class="hint">执行方式：只按当前楼栋和当前日期上传所选文件。</div>
              </article>

              <article class="task-block task-block-compact" style="grid-column:1 / -1;">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">本地数据初始化</div>
                    <h3 class="card-title">首次备份多维记录</h3>
                  </div>
                  <span
                    class="status-badge status-badge-soft"
                    :class="currentJob && currentJob.feature === 'monthly_mysql_initial_backup' && ['queued', 'running', 'waiting_resource'].includes(currentJob.status) ? 'tone-info' : 'tone-neutral'"
                  >
                    {{ currentJob && currentJob.feature === 'monthly_mysql_initial_backup' ? (currentJob.status_text || currentJob.status || '待执行') : '待执行' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">读取记录</div>
                    <strong class="status-metric-value">{{ currentJob && currentJob.feature === 'monthly_mysql_initial_backup' ? (currentJob.progress?.fetched_records || currentJob.result?.fetched_records || 0) : 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">记录总数</div>
                    <strong class="status-metric-value">{{ currentJob && currentJob.feature === 'monthly_mysql_initial_backup' ? (currentJob.progress?.total_records || currentJob.result?.total_records || 0) : 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">已写入</div>
                    <strong class="status-metric-value">{{ currentJob && currentJob.feature === 'monthly_mysql_initial_backup' ? (currentJob.progress?.written_records || currentJob.result?.written_records || 0) : 0 }}</strong>
                  </div>
                </div>
                <div class="form-row">
                  <label class="label">备份进度</label>
                  <progress
                    max="100"
                    style="width:100%; height:14px;"
                    :value="currentJob && currentJob.feature === 'monthly_mysql_initial_backup' ? (currentJob.progress?.progress || (currentJob.status === 'success' ? 100 : 0)) : 0"
                  ></progress>
                  <div class="hint">
                    {{ currentJob && currentJob.feature === 'monthly_mysql_initial_backup' ? (currentJob.progress?.message || currentJob.summary || '等待任务进度') : '等待执行' }}
                  </div>
                </div>
                <div class="btn-line">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || isActionLocked('job:monthly_mysql_initial_backup') || (currentJob && currentJob.feature === 'monthly_mysql_initial_backup' && ['queued', 'running', 'waiting_resource'].includes(currentJob.status))"
                    @click="runMonthlyMysqlInitialBackup"
                  >
                    {{ currentJob && currentJob.feature === 'monthly_mysql_initial_backup' && ['queued', 'running', 'waiting_resource'].includes(currentJob.status) ? '备份中...' : '首次备份多维记录' }}
                  </button>
                </div>
              </article>
            </div>
          </div>
        </section>

`;

