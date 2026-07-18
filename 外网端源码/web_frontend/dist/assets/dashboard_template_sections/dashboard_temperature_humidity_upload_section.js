export const DASHBOARD_TEMPERATURE_HUMIDITY_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'temperature_humidity_upload'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">空调温湿度专项上传</h3>
              <div class="hint">按日期读取内网端 A-E 楼空调温湿度源文件，完整校验后清空目标表，再上传温度、湿度和运行状态。</div>
            </div>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">专项上传卡</div>
                    <h3 class="card-title">上传空调温湿度数据</h3>
                  </div>
                  <span class="status-badge status-badge-soft tone-info">A楼至E楼</span>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label">源文件日期</label>
                    <input type="date" v-model="temperatureHumidityUploadDate" />
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">数据口径</div>
                  <div class="ops-focus-card-title">温湿度按位置合并，运行状态 1=开启、0=关闭</div>
                  <div class="ops-focus-card-meta">只使用内网端 source-index 返回的明确文件；5 楼全部解析及字段校验成功后，先清表再上传。失败时自动尝试恢复旧表快照。</div>
                </div>
                <div class="btn-line" style="flex-wrap:wrap;">
                  <button
                    class="btn btn-primary"
                    :disabled="!canRun || isActionLocked(actionKeyTemperatureHumidityUploadRun)"
                    @click="runTemperatureHumidityUpload"
                  >
                    {{ isActionLocked(actionKeyTemperatureHumidityUploadRun) ? '提交中...' : '上传温湿度数据' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">每日调度</div>
                    <h3 class="card-title">自动上传当天温湿度</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('temperature_humidity_upload')">
                    {{ getSchedulerStatusText('temperature_humidity_upload') || '-' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">下次执行</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('temperature_humidity_upload', 'next_run_text', '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">最近触发</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('temperature_humidity_upload', 'last_trigger_text', '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">执行器</div>
                    <strong class="status-metric-value">{{ health.temperature_humidity_upload.scheduler.executor_bound ? '已绑定' : '未绑定' }}</strong>
                  </div>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label">每日上传时间</label>
                    <input
                      type="time"
                      step="1"
                      :value="config.temperature_humidity_upload.scheduler.run_time"
                      :disabled="temperatureHumidityUploadSchedulerQuickSaving"
                      @change="saveTemperatureHumidityUploadSchedulerQuickConfig({ run_time: $event.target.value })"
                    />
                  </div>
                </div>
                <div class="btn-line" style="flex-wrap:wrap;">
                  <button
                    class="btn btn-primary"
                    :disabled="temperatureHumidityUploadSchedulerQuickSaving || isSchedulerStartDisabled('temperature_humidity_upload', actionKeyTemperatureHumidityUploadSchedulerStart, actionKeyTemperatureHumidityUploadSchedulerStop)"
                    @click="startTemperatureHumidityUploadScheduler"
                  >
                    {{ getSchedulerStartButtonText('temperature_humidity_upload') }}
                  </button>
                  <button
                    class="btn btn-ghost"
                    :disabled="temperatureHumidityUploadSchedulerQuickSaving || isSchedulerStopDisabled('temperature_humidity_upload', actionKeyTemperatureHumidityUploadSchedulerStart, actionKeyTemperatureHumidityUploadSchedulerStop)"
                    @click="stopTemperatureHumidityUploadScheduler"
                  >
                    {{ getSchedulerStopButtonText('temperature_humidity_upload') }}
                  </button>
                </div>
                <div class="hint">{{ temperatureHumidityUploadSchedulerQuickSaving ? '空调温湿度上传调度配置同步中...' : '默认每天 02:30 执行；可修改时间，调度和立即上传共用同一资源锁。' }}</div>
                <div class="hint">最近判断：{{ getSchedulerDisplayText('temperature_humidity_upload', 'decision_text', '暂无记录') }}；最近结果：{{ getSchedulerDisplayText('temperature_humidity_upload', 'trigger_text', '暂无记录') }}</div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">状态概览</div>
                    <h3 class="card-title">最近温湿度上传任务</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getTemperatureHumidityUploadStatusTone()">
                    {{ getTemperatureHumidityUploadStatusText() }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">上传记录</div>
                    <strong class="status-metric-value">{{ getTemperatureHumidityUploadResult().uploaded_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">温湿度位置</div>
                    <strong class="status-metric-value">{{ getTemperatureHumidityUploadResult().temperature_location_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">运行状态位置</div>
                    <strong class="status-metric-value">{{ getTemperatureHumidityUploadResult().status_location_count || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">删除旧记录</div>
                    <strong class="status-metric-value">{{ getTemperatureHumidityUploadResult().deleted_count || 0 }}</strong>
                  </div>
                </div>
                <div class="hint">源文件日期：{{ getTemperatureHumidityUploadResult().source_date || '-' }}</div>
                <div class="hint">目标表：{{ getTemperatureHumidityUploadResult().table_id || 'tblfnTbEWK9607zV' }}</div>
                <div class="hint">当前任务：{{ currentJob && currentJob.feature === 'temperature_humidity_upload' ? (currentJob.job_id || '-') : '-' }}</div>
              </article>
            </div>
          </div>
        </section>`;
