export const DASHBOARD_INSPECTION_DATA_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'inspection_data_upload'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">巡检数据处理</h3>
            </div>
            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <div class="dashboard-module-main-column">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <h3 class="card-title">手动处理</h3>
                  <span class="status-badge status-badge-soft tone-info">D楼 / E楼</span>
                </div>
                <div class="config-form-grid config-form-grid-compact">
                  <div class="form-row">
                    <label class="label" for="inspection-source-date">源文件日期</label>
                    <input id="inspection-source-date" type="date" v-model="inspectionDataUploadDate" />
                  </div>
                  <div class="form-row">
                    <label style="display:flex;align-items:center;gap:8px;">
                      <input type="checkbox" v-model="inspectionDataUploadDryRun" />
                      <span>仅检查文件内容</span>
                    </label>
                  </div>
                </div>
                <div class="btn-line">
                  <button class="btn btn-primary"
                    :disabled="!canRun || isActionLocked(actionKeyInspectionDataUploadRun)"
                    @click="runInspectionDataUpload">
                    {{ isActionLocked(actionKeyInspectionDataUploadRun) ? '提交中...' : (inspectionDataUploadDryRun ? '检查文件' : '上传巡检数据') }}
                  </button>
                </div>
              </article>
              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <h3 class="card-title">处理结果</h3>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getInspectionDataUploadStatusTone()">{{ getInspectionDataUploadStatusText() }}</span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">{{ getInspectionDataUploadResult().dry_run ? '检查设备数' : '上传记录' }}</div>
                    <strong class="status-metric-value">{{ (getInspectionDataUploadResult().dry_run ? getInspectionDataUploadResult().planned_count : getInspectionDataUploadResult().uploaded_count) || 0 }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">源文件数</div>
                    <strong class="status-metric-value">{{ getInspectionDataUploadResult().source_file_count || 0 }}</strong>
                  </div>
                </div>
                <div class="hint">源文件日期：{{ getInspectionDataUploadResult().source_date || '-' }}</div>
                <div v-for="building in (getInspectionDataUploadResult().buildings || [])" :key="building" class="form-row">
                  <strong>{{ building }}</strong>
                  <div class="hint" v-for="source in (getInspectionDataUploadResult().results?.[building]?.sources || {})" :key="source.report_name">
                    {{ source.report_name }}：{{ source.source_time || '未提供' }}
                  </div>
                  <div class="hint">上传记录：{{ (getInspectionDataUploadResult().results?.[building] || {}).uploaded_count || 0 }}</div>
                  <div class="hint" v-if="(getInspectionDataUploadResult().results?.[building] || {}).missing_devices">缺测设备：{{ getInspectionDataUploadResult().results[building].missing_devices }}</div>
                </div>
                <div class="hint" v-if="getInspectionDataUploadResult().dry_run">仅检查文件内容，未上传</div>
              </article>
              </div>
              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <h3 class="card-title">每日调度</h3>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('inspection_data_upload')">
                    {{ getSchedulerStatusText('inspection_data_upload') || '-' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">下次执行</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('inspection_data_upload', 'next_run_text', '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">最近执行</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('inspection_data_upload', 'last_trigger_text', '-') }}</strong>
                  </div>
                </div>
                <div class="form-row">
                  <label class="label">每日执行时间</label>
                  <div v-if="!config.inspection_data_upload.scheduler.run_times.length" class="hint">未设置时间</div>
                  <div v-for="(runTime, runTimeIndex) in config.inspection_data_upload.scheduler.run_times"
                    :key="'inspection-run-time-' + runTimeIndex" style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">
                    <input type="time" step="1" :value="runTime" :aria-label="'执行时间 ' + (runTimeIndex + 1)"
                      :disabled="inspectionDataUploadSchedulerQuickSaving"
                      @change="saveInspectionDataUploadSchedulerQuickConfig({ run_times: config.inspection_data_upload.scheduler.run_times.map((item, idx) => idx === runTimeIndex ? $event.target.value : item) })" />
                    <button class="btn btn-ghost" title="删除时间" aria-label="删除时间"
                      :disabled="inspectionDataUploadSchedulerQuickSaving"
                      @click="saveInspectionDataUploadSchedulerQuickConfig({ run_times: config.inspection_data_upload.scheduler.run_times.filter((item, idx) => idx !== runTimeIndex) })">−</button>
                  </div>
                  <div style="display:flex;align-items:center;gap:8px;">
                    <input type="time" step="1" v-model="inspectionDataUploadRunTimeDraft" aria-label="新增执行时间" :disabled="inspectionDataUploadSchedulerQuickSaving" />
                    <button class="btn btn-ghost" title="添加时间" aria-label="添加时间"
                      :disabled="inspectionDataUploadSchedulerQuickSaving || !inspectionDataUploadRunTimeDraft"
                      @click="addInspectionDataUploadRunTime">+</button>
                  </div>
                </div>
                <div class="btn-line" style="flex-wrap:wrap;">
                  <button class="btn btn-primary"
                    :disabled="!config.inspection_data_upload.scheduler.run_times.length || inspectionDataUploadSchedulerQuickSaving || isSchedulerStartDisabled('inspection_data_upload', actionKeyInspectionDataUploadSchedulerStart, actionKeyInspectionDataUploadSchedulerStop)"
                    @click="startInspectionDataUploadScheduler">{{ getSchedulerStartButtonText('inspection_data_upload') }}</button>
                  <button class="btn btn-ghost"
                    :disabled="inspectionDataUploadSchedulerQuickSaving || isSchedulerStopDisabled('inspection_data_upload', actionKeyInspectionDataUploadSchedulerStart, actionKeyInspectionDataUploadSchedulerStop)"
                    @click="stopInspectionDataUploadScheduler">{{ getSchedulerStopButtonText('inspection_data_upload') }}</button>
                </div>
                <div class="hint" aria-live="polite">{{ inspectionDataUploadSchedulerQuickSaving ? '保存中...' : '' }}</div>
                <div class="hint">最近结果：{{ getSchedulerDisplayText('inspection_data_upload', 'trigger_text', '暂无记录') }}</div>
              </article>
            </div>
          </div>
        </section>`;
