export const DASHBOARD_SYSTEM_SCREENSHOT_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'system_screenshot_upload'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">系统截图上传</h3>
              <div class="hint">触发内网端检查当天 5 张系统图截图，并上传到对应的多维表附件字段。</div>
            </div>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">专项上传卡</div>
                    <h3 class="card-title">上传每日系统截图</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">5张系统图</span>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label">截图日期</label>
                    <input type="date" v-model="systemScreenshotUploadDate" />
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">写入目标</div>
                  <div class="ops-focus-card-title">供配电、暖通、燃油、柴发、弱电 5 个多维表</div>
                  <div class="ops-focus-card-meta">外网端通过内网 HTTP 文件接口读取当天截图，不扫描共享目录。</div>
                </div>
                <div class="btn-line" style="flex-wrap:wrap;">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || isActionLocked(actionKeySystemScreenshotUploadRun)"
                    @click="runSystemScreenshotUpload"
                  >
                    {{ isActionLocked(actionKeySystemScreenshotUploadRun) ? '提交中...' : '上传截图' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">状态概览</div>
                    <h3 class="card-title">最近系统截图上传任务</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getSystemScreenshotUploadStatusTone()">
                    {{ getSystemScreenshotUploadStatusText() }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">上传图片</div>
                    <strong class="status-metric-value">{{ getSystemScreenshotUploadResult().uploaded_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">删除旧记录</div>
                    <strong class="status-metric-value">{{ getSystemScreenshotUploadResult().deleted_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">日期</div>
                    <strong class="status-metric-value">{{ getSystemScreenshotUploadResult().date_value || getSystemScreenshotUploadResult().capture_date || '-' }}</strong>
                  </div>
                </div>
                <div class="hint">当前任务：{{ currentJob && currentJob.feature === 'system_screenshot_upload' ? (currentJob.job_id || '-') : '-' }}</div>
                <div class="hint" v-if="getSystemScreenshotUploadResult().app_token">目标应用：{{ getSystemScreenshotUploadResult().app_token }}</div>
              </article>
            </div>
          </div>
        </section>`;
