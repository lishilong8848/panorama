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
                <div class="hint">手动补传仅使用已选择文件，不执行内网下载。</div>
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
                  <button class="btn btn-primary" :disabled="isInternalDeploymentRole || !canRun || isActionLocked(actionKeyManualUpload)" @click="runManualUpload">
                    {{ isActionLocked(actionKeyManualUpload) ? '提交中...' : '开始手动补传' }}
                  </button>
                </div>
                <div class="hint" v-if="isInternalDeploymentRole">当前为内网端，手动补传请在外网端执行。</div>
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
            </div>
          </div>
        </section>

`;
