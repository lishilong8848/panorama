export const DASHBOARD_CHILLER_MODE_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'chiller_mode_upload'">
          <div class="dashboard-module-shell">
            <article class="task-block dashboard-module-scheduler-card">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">调度卡</div>
                  <h3 class="card-title">制冷模式参数上传调度</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('chiller_mode_upload')">
                  {{ getSchedulerStatusText('chiller_mode_upload') || '-' }}
                </span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">运行间隔</div>
                  <strong class="status-metric-value">{{ config.chiller_mode_upload.scheduler.interval_minutes || '-' }} 分钟</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">下次执行</div>
                  <strong class="status-metric-value">{{ getSchedulerDisplayText('chiller_mode_upload', 'next_run_text', '-') }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">最近结果</div>
                  <strong class="status-metric-value">{{ chillerModeUploadSchedulerTriggerText || '-' }}</strong>
                </div>
              </div>
              <div class="task-grid two-col">
                <div class="form-row">
                  <label class="label">每 N 分钟运行一次</label>
                  <input type="number" min="1" v-model.number="config.chiller_mode_upload.scheduler.interval_minutes" @change="saveChillerModeUploadSchedulerQuickConfig" />
                </div>
                <div class="form-row">
                  <label class="label">检查间隔（秒）</label>
                  <input type="number" min="1" v-model.number="config.chiller_mode_upload.scheduler.check_interval_sec" @change="saveChillerModeUploadSchedulerQuickConfig" />
                </div>
              </div>
              <div class="btn-line">
                <button
                  class="btn btn-success"
                  :disabled="isSchedulerStartDisabled('chiller_mode_upload', actionKeyChillerModeUploadSchedulerStart, actionKeyChillerModeUploadSchedulerStop)"
                  @click="startChillerModeUploadScheduler"
                >
                  {{ getSchedulerStartButtonText('chiller_mode_upload') }}
                </button>
                <button
                  class="btn btn-danger"
                  :disabled="isSchedulerStopDisabled('chiller_mode_upload', actionKeyChillerModeUploadSchedulerStart, actionKeyChillerModeUploadSchedulerStop)"
                  @click="stopChillerModeUploadScheduler"
                >
                  {{ getSchedulerStopButtonText('chiller_mode_upload') }}
                </button>
              </div>
              <div class="hint">{{ chillerModeUploadSchedulerQuickSaving ? '制冷模式参数上传调度配置同步中...' : '默认每10分钟读取最新共享源文件并清表重传目标多维表。' }}</div>
            </article>

            <div class="module-section-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">执行入口</div>
                    <h3 class="card-title">制冷模式参数上传</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="isActionLocked(actionKeyChillerModeUploadRun) ? 'tone-info' : 'tone-success'">
                    {{ isActionLocked(actionKeyChillerModeUploadRun) ? '执行中' : '可执行' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">调度状态</div>
                    <strong class="status-metric-value">{{ getSchedulerStatusText('chiller_mode_upload') || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">源文件</div>
                    <strong class="status-metric-value">制冷单元模式切换参数</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">目标表</div>
                    <strong class="status-metric-value">{{ health.chiller_mode_upload.target_preview.table_id || '-' }}</strong>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">处理口径</div>
                  <div class="ops-focus-card-title">按楼栋读取 C/D/E 列，C列支持合并单元格前向填充</div>
                  <div class="ops-focus-card-meta">解析全部成功后才清空目标多维表，失败时保留旧数据。</div>
                </div>
                <div class="btn-line">
                  <button class="btn btn-primary" :disabled="!canRun || isActionLocked(actionKeyChillerModeUploadRun)" @click="runChillerModeUpload">
                    {{ isActionLocked(actionKeyChillerModeUploadRun) ? '执行中...' : '立即运行一次' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">目标配置</div>
                    <h3 class="card-title">当前目标多维表</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="health.chiller_mode_upload.target_preview.configured_app_token && health.chiller_mode_upload.target_preview.table_id ? 'tone-success' : 'tone-warning'">
                    {{ health.chiller_mode_upload.target_preview.configured_app_token && health.chiller_mode_upload.target_preview.table_id ? '已配置' : '待配置' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">配置 Token</div>
                    <strong class="status-metric-value">{{ health.chiller_mode_upload.target_preview.configured_app_token || config.chiller_mode_upload.target.app_token || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">Table ID</div>
                    <strong class="status-metric-value">{{ health.chiller_mode_upload.target_preview.table_id || config.chiller_mode_upload.target.table_id || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">目标类型</div>
                    <strong class="status-metric-value">{{ formatWetBulbTargetKind(health.chiller_mode_upload.target_preview.target_kind) }}</strong>
                  </div>
                </div>
                <div class="hint" v-if="health.chiller_mode_upload.target_preview.operation_app_token && health.chiller_mode_upload.target_preview.operation_app_token !== health.chiller_mode_upload.target_preview.configured_app_token">
                  实际上传 Token：{{ health.chiller_mode_upload.target_preview.operation_app_token }}
                </div>
                <div class="hint" v-if="health.chiller_mode_upload.target_preview.display_url">
                  当前配置链接：
                  <a :href="health.chiller_mode_upload.target_preview.display_url" target="_blank" rel="noopener noreferrer">{{ health.chiller_mode_upload.target_preview.display_url }}</a>
                </div>
                <div class="hint" v-else-if="health.chiller_mode_upload.target_preview.message">当前配置状态：{{ health.chiller_mode_upload.target_preview.message }}</div>
                <div class="hint" v-else>当前尚未解析制冷模式参数目标多维表。</div>
              </article>
            </div>

            <div class="dashboard-module-support-stack">
              <article class="task-block task-block-compact">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">调度状态</div>
                    <h3 class="card-title">当前调度反馈</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('chiller_mode_upload')">
                    {{ getSchedulerStatusText('chiller_mode_upload') }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">最近触发</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('chiller_mode_upload', 'last_trigger_text', '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">触发结果</div>
                    <strong class="status-metric-value">{{ chillerModeUploadSchedulerTriggerText || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">最近决策</div>
                    <strong class="status-metric-value">{{ chillerModeUploadSchedulerDecisionText || '-' }}</strong>
                  </div>
                </div>
              </article>
            </div>
          </div>
        </section>

`;

