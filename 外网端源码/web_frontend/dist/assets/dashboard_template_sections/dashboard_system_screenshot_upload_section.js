export const DASHBOARD_SYSTEM_SCREENSHOT_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'system_screenshot_upload'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">系统截图上传</h3>
              <div class="hint">读取内网端当天已生成的 5 楼 × 6 张系统图截图，并上传到对应多维表的“截图”附件字段。</div>
            </div>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">专项上传卡</div>
                    <h3 class="card-title">上传每日系统截图</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">30张截图</span>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label">截图日期</label>
                    <input type="date" v-model="systemScreenshotUploadDate" />
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">写入目标</div>
                  <div class="ops-focus-card-title">供配电、暖通A区、暖通B区、燃油、柴发、弱电，共 30 张截图</div>
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
                    <div class="task-block-kicker">每日调度</div>
                    <h3 class="card-title">自动上传当天截图</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('system_screenshot_upload')">
                    {{ getSchedulerStatusText('system_screenshot_upload') || '-' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">下次执行</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('system_screenshot_upload', 'next_run_text', '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">最近触发</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('system_screenshot_upload', 'last_trigger_text', '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">执行器</div>
                    <strong class="status-metric-value">{{ health.system_screenshot_upload.scheduler.executor_bound ? '已绑定' : '未绑定' }}</strong>
                  </div>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label">每日上传时间</label>
                    <input
                      type="time"
                      step="1"
                      :value="config.system_screenshot_upload.scheduler.run_time"
                      :disabled="systemScreenshotUploadSchedulerQuickSaving"
                      @change="saveSystemScreenshotUploadSchedulerQuickConfig({ run_time: $event.target.value })"
                    />
                  </div>
                </div>
                <div class="btn-line" style="flex-wrap:wrap;">
                  <button
                    class="btn btn-primary"
                    :disabled="systemScreenshotUploadSchedulerQuickSaving || isSchedulerStartDisabled('system_screenshot_upload', actionKeySystemScreenshotUploadSchedulerStart, actionKeySystemScreenshotUploadSchedulerStop)"
                    @click="startSystemScreenshotUploadScheduler"
                  >
                    {{ getSchedulerStartButtonText('system_screenshot_upload') }}
                  </button>
                  <button
                    class="btn btn-ghost"
                    :disabled="systemScreenshotUploadSchedulerQuickSaving || isSchedulerStopDisabled('system_screenshot_upload', actionKeySystemScreenshotUploadSchedulerStart, actionKeySystemScreenshotUploadSchedulerStop)"
                    @click="stopSystemScreenshotUploadScheduler"
                  >
                    {{ getSchedulerStopButtonText('system_screenshot_upload') }}
                  </button>
                </div>
                <div class="hint">{{ systemScreenshotUploadSchedulerQuickSaving ? '系统截图上传调度配置同步中...' : '调度会先触发内网端检查并补齐当天截图，再上传到多维表。' }}</div>
                <div class="hint">最近判断：{{ systemScreenshotUploadSchedulerDecisionText }}；最近结果：{{ systemScreenshotUploadSchedulerTriggerText }}</div>
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
