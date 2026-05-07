export const DASHBOARD_HANDOVER_LOG_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'handover_log'">
          <div class="dashboard-module-shell">
          <article class="task-block dashboard-module-scheduler-card">
            <div class="task-block-head">
              <div>
                <div class="task-block-kicker">调度卡</div>
                <h3 class="card-title">交接班调度</h3>
              </div>
              <span
                class="status-badge status-badge-soft"
                :class="'tone-' + getSchedulerStatusTone('handover')"
              >
                {{ getSchedulerStatusText('handover') || '-' }}
              </span>
            </div>
            <div class="status-metric-grid status-metric-grid-compact">
              <div class="status-metric">
                <div class="status-metric-label">上午下次执行</div>
                <strong class="status-metric-value">{{ getSchedulerDisplayText(health.handover_scheduler.morning, 'next_run_text', '-') }}</strong>
              </div>
              <div class="status-metric">
                <div class="status-metric-label">下午下次执行</div>
                <strong class="status-metric-value">{{ getSchedulerDisplayText(health.handover_scheduler.afternoon, 'next_run_text', '-') }}</strong>
              </div>
              <div class="status-metric">
                <div class="status-metric-label">最近决策</div>
                <strong class="status-metric-value">{{ handoverMorningDecisionText || handoverAfternoonDecisionText || '-' }}</strong>
              </div>
            </div>
            <div class="hint">上午时间点用于补跑前一天夜班，下午时间点用于执行当天白班。修改后立即生效。</div>
            <div class="task-grid two-col">
              <div class="form-row">
                <label class="label">上午时间</label>
                <input
                  type="time"
                  step="1"
                  :value="config.handover_log.scheduler.morning_time"
                  :disabled="handoverSchedulerQuickSaving"
                  @change="saveHandoverSchedulerQuickConfig({ morning_time: $event.target.value })"
                />
              </div>
              <div class="form-row">
                <label class="label">下午时间</label>
                <input
                  type="time"
                  step="1"
                  :value="config.handover_log.scheduler.afternoon_time"
                  :disabled="handoverSchedulerQuickSaving"
                  @change="saveHandoverSchedulerQuickConfig({ afternoon_time: $event.target.value })"
                />
              </div>
            </div>
            <div class="btn-line" style="margin-top:10px;">
              <button
                class="btn btn-success"
                :disabled="handoverSchedulerQuickSaving || isSchedulerStartDisabled('handover', actionKeyHandoverSchedulerStart, actionKeyHandoverSchedulerStop)"
                @click="startHandoverScheduler"
              >
                {{ getSchedulerStartButtonText('handover') }}
              </button>
              <button
                class="btn btn-danger"
                :disabled="handoverSchedulerQuickSaving || isSchedulerStopDisabled('handover', actionKeyHandoverSchedulerStart, actionKeyHandoverSchedulerStop)"
                @click="stopHandoverScheduler"
              >
                {{ getSchedulerStopButtonText('handover') }}
              </button>
            </div>
            <div class="hint" v-if="handoverSchedulerQuickSaving">交接班调度配置同步中...</div>
          </article>
          <div class="handover-task-shell-redesign">
            <div class="handover-top-grid dashboard-module-primary-grid">
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
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">执行范围</div>
                    <strong class="status-metric-value">{{ handoverDownloadScope === 'single' ? (manualBuilding || '-') : '全部启用楼栋' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">交接班日期</div>
                    <strong class="status-metric-value">{{ handoverDutyDate || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">当前班次</div>
                    <strong class="status-metric-value">{{ handoverDutyShift === 'day' ? '白班' : '夜班' }}</strong>
                  </div>
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
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">已选文件</div>
                    <strong class="status-metric-value">{{ handoverSelectedFileCount }} 个楼</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">已配置楼栋</div>
                    <strong class="status-metric-value">{{ handoverConfiguredBuildings.length }} 个楼</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">审核概况</div>
                    <strong class="status-metric-value">{{ handoverReviewOverview.summaryText }}</strong>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">推荐顺序</div>
                  <div class="ops-focus-card-title">优先使用共享文件生成，只有缺文件时再从已有数据表补生成</div>
                  <div class="ops-focus-card-meta">共享文件路径更适合标准班次流程；已有数据表适合单楼修复和历史回补。</div>
                </div>
                <div class="btn-stack">
                  <button class="btn btn-primary" :disabled="!canRun || handoverGenerationBusy || isActionLocked(actionKeyHandoverFromDownload) || isActionLocked(actionKeyHandoverFromFile)" @click="runHandoverFromDownload">
                    {{ handoverGenerationBusy || isActionLocked(actionKeyHandoverFromDownload) || isActionLocked(actionKeyHandoverFromFile) ? '执行中...' : '使用共享文件生成' }}
                  </button>
                  <button class="btn btn-secondary" :disabled="!canRun || handoverGenerationBusy || !hasSelectedHandoverFiles || isActionLocked(actionKeyHandoverFromFile) || isActionLocked(actionKeyHandoverFromDownload)" @click="runHandoverFromFile">
                    {{ handoverGenerationBusy || isActionLocked(actionKeyHandoverFromFile) || isActionLocked(actionKeyHandoverFromDownload) ? '执行中...' : '从已有数据表生成' }}
                  </button>
                </div>
                <div class="action-reason action-reason-warning" v-if="handoverGenerationBusy">
                  {{ handoverGenerationStatusText || '当前已有交接班日志生成任务在执行或排队，请等待任务完成后再发起新的交接班生成。' }}
                </div>
                <div class="action-reason action-reason-warning" v-else-if="!hasSelectedHandoverFiles">
                  请先为至少一个楼选择已有数据表文件，再执行“从已有数据表生成”。
                </div>
                <div class="hint" v-else>
                  本次将生成 {{ handoverSelectedFileCount }} 个楼，未选择文件的楼将跳过。
                </div>
              </article>
            </div>

            <div class="handover-middle-stack dashboard-module-support-stack">
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
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">已选楼栋</div>
                    <strong class="status-metric-value">{{ handoverSelectedFileCount }} 个</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">待补文件楼栋</div>
                    <strong class="status-metric-value">{{ handoverConfiguredBuildings.length - handoverSelectedFileCount }} 个</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">当前用途</div>
                    <strong class="status-metric-value">从已有数据表生成</strong>
                  </div>
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

              <details class="module-advanced-section">
                <summary>查看日报截图与日报记录</summary>
                <div class="module-advanced-section-body">
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
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">当前日期</div>
                    <strong class="status-metric-value">{{ handoverDutyDate || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">当前班次</div>
                    <strong class="status-metric-value">{{ handoverDutyShift === 'day' ? '白班' : '夜班' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">截图登录态</div>
                    <strong class="status-metric-value">{{ handoverDailyReportAuthVm.text }}</strong>
                  </div>
                </div>
                <div class="ops-focus-card">
                  <div class="ops-focus-card-label">执行条件</div>
                  <div class="ops-focus-card-title">两张截图与云文档链接齐全后，才能重写日报多维表</div>
                  <div class="ops-focus-card-meta">建议先检查登录态，再做截图测试；自动图异常时再手工替换。</div>
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
                <div class="hint" v-if="handoverDailyReportAuthVm.profileText">
                  {{ handoverDailyReportAuthVm.profileLabel || '当前目标浏览器' }}：{{ handoverDailyReportAuthVm.profileText }}
                </div>
                <div class="hint" v-if="handoverDailyReportAuthVm.error">
                  {{ handoverDailyReportAuthVm.error }}
                </div>
                <div class="btn-line" style="margin-top:10px;">
                  <button
                    class="btn btn-secondary"
                    :disabled="isActionLocked(actionKeyHandoverDailyReportAuthOpen) || isHandoverDailyReportActionDisabled('open_auth')"
                    @click="openHandoverDailyReportScreenshotAuth"
                  >
                    {{ isActionLocked(actionKeyHandoverDailyReportAuthOpen) ? '打开中...' : getHandoverDailyReportActionButtonText('open_auth', '初始化飞书截图登录态') }}
                  </button>
                  <button
                    class="btn btn-secondary"
                    :disabled="isActionLocked(actionKeyHandoverDailyReportScreenshotTest) || isHandoverDailyReportActionDisabled('screenshot_test')"
                    @click="runHandoverDailyReportScreenshotTest"
                  >
                    {{ isActionLocked(actionKeyHandoverDailyReportScreenshotTest) ? '测试中...' : getHandoverDailyReportActionButtonText('screenshot_test', '截图测试') }}
                  </button>
                </div>
                <div class="hint" v-if="!isActionLocked(actionKeyHandoverDailyReportAuthOpen) && getHandoverDailyReportActionDisabledReason('open_auth')">
                  {{ getHandoverDailyReportActionDisabledReason('open_auth') }}
                </div>
                <div class="hint" v-if="!isActionLocked(actionKeyHandoverDailyReportScreenshotTest) && getHandoverDailyReportActionDisabledReason('screenshot_test')">
                  {{ getHandoverDailyReportActionDisabledReason('screenshot_test') }}
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
                        :disabled="isHandoverDailyReportAssetActionDisabled(handoverDailyReportCaptureAssets.summarySheetImage, 'preview')"
                        @click="openHandoverDailyReportPreview('summary_sheet')"
                      >{{ getHandoverDailyReportAssetActionButtonText(handoverDailyReportCaptureAssets.summarySheetImage, 'preview', '放大查看') }}</button>
                      <button
                        class="btn btn-secondary"
                        :disabled="isActionLocked(getHandoverDailyReportRecaptureActionKey('summary_sheet')) || isHandoverDailyReportAssetActionDisabled(handoverDailyReportCaptureAssets.summarySheetImage, 'recapture')"
                        @click="recaptureHandoverDailyReportAsset('summary_sheet')"
                      >{{ isActionLocked(getHandoverDailyReportRecaptureActionKey('summary_sheet')) ? '重截中...' : getHandoverDailyReportAssetActionButtonText(handoverDailyReportCaptureAssets.summarySheetImage, 'recapture', '重新截图') }}</button>
                      <button
                        class="btn btn-secondary"
                        :disabled="isHandoverDailyReportAssetActionDisabled(handoverDailyReportCaptureAssets.summarySheetImage, 'upload')"
                        @click="openHandoverDailyReportUploadDialog('summary_sheet')"
                      >{{ getHandoverDailyReportAssetActionButtonText(handoverDailyReportCaptureAssets.summarySheetImage, 'upload', '上传/粘贴替换') }}</button>
                      <button
                        v-if="handoverDailyReportCaptureAssets.summarySheetImage.hasManual"
                        class="btn btn-ghost"
                        :disabled="isActionLocked(getHandoverDailyReportRestoreActionKey('summary_sheet')) || isHandoverDailyReportAssetActionDisabled(handoverDailyReportCaptureAssets.summarySheetImage, 'restore_auto')"
                        @click="restoreHandoverDailyReportAutoAsset('summary_sheet')"
                      >{{ isActionLocked(getHandoverDailyReportRestoreActionKey('summary_sheet')) ? '恢复中...' : getHandoverDailyReportAssetActionButtonText(handoverDailyReportCaptureAssets.summarySheetImage, 'restore_auto', '恢复自动图') }}</button>
                    </div>
                    <div class="hint" v-if="getHandoverDailyReportAssetActionDisabledReason(handoverDailyReportCaptureAssets.summarySheetImage, 'preview') && !handoverDailyReportCaptureAssets.summarySheetImage.exists">
                      {{ getHandoverDailyReportAssetActionDisabledReason(handoverDailyReportCaptureAssets.summarySheetImage, 'preview') }}
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
                        :disabled="isHandoverDailyReportAssetActionDisabled(handoverDailyReportCaptureAssets.externalPageImage, 'preview')"
                        @click="openHandoverDailyReportPreview('external_page')"
                      >{{ getHandoverDailyReportAssetActionButtonText(handoverDailyReportCaptureAssets.externalPageImage, 'preview', '放大查看') }}</button>
                      <button
                        class="btn btn-secondary"
                        :disabled="isActionLocked(getHandoverDailyReportRecaptureActionKey('external_page')) || isHandoverDailyReportAssetActionDisabled(handoverDailyReportCaptureAssets.externalPageImage, 'recapture')"
                        @click="recaptureHandoverDailyReportAsset('external_page')"
                      >{{ isActionLocked(getHandoverDailyReportRecaptureActionKey('external_page')) ? '重截中...' : getHandoverDailyReportAssetActionButtonText(handoverDailyReportCaptureAssets.externalPageImage, 'recapture', '重新截图') }}</button>
                      <button
                        class="btn btn-secondary"
                        :disabled="isHandoverDailyReportAssetActionDisabled(handoverDailyReportCaptureAssets.externalPageImage, 'upload')"
                        @click="openHandoverDailyReportUploadDialog('external_page')"
                      >{{ getHandoverDailyReportAssetActionButtonText(handoverDailyReportCaptureAssets.externalPageImage, 'upload', '上传/粘贴替换') }}</button>
                      <button
                        v-if="handoverDailyReportCaptureAssets.externalPageImage.hasManual"
                        class="btn btn-ghost"
                        :disabled="isActionLocked(getHandoverDailyReportRestoreActionKey('external_page')) || isHandoverDailyReportAssetActionDisabled(handoverDailyReportCaptureAssets.externalPageImage, 'restore_auto')"
                        @click="restoreHandoverDailyReportAutoAsset('external_page')"
                      >{{ isActionLocked(getHandoverDailyReportRestoreActionKey('external_page')) ? '恢复中...' : getHandoverDailyReportAssetActionButtonText(handoverDailyReportCaptureAssets.externalPageImage, 'restore_auto', '恢复自动图') }}</button>
                    </div>
                    <div class="hint" v-if="getHandoverDailyReportAssetActionDisabledReason(handoverDailyReportCaptureAssets.externalPageImage, 'preview') && !handoverDailyReportCaptureAssets.externalPageImage.exists">
                      {{ getHandoverDailyReportAssetActionDisabledReason(handoverDailyReportCaptureAssets.externalPageImage, 'preview') }}
                    </div>
                  </div>
                </div>
                <div class="hint" v-if="handoverDailyReportExportVm.error">
                  {{ handoverDailyReportExportVm.error }}
                </div>
                <div class="btn-line" style="margin-top:12px;">
                  <button
                    class="btn btn-primary"
                    :disabled="isActionLocked(actionKeyHandoverDailyReportRecordRewrite) || isHandoverDailyReportActionDisabled('rewrite_record')"
                    @click="rewriteHandoverDailyReportRecord"
                  >
                    {{ isActionLocked(actionKeyHandoverDailyReportRecordRewrite) ? '重写中...' : getHandoverDailyReportActionButtonText('rewrite_record', '重新写入日报多维表') }}
                  </button>
                </div>
                <div class="hint" v-if="!isActionLocked(actionKeyHandoverDailyReportRecordRewrite) && getHandoverDailyReportActionDisabledReason('rewrite_record')">
                  {{ getHandoverDailyReportActionDisabledReason('rewrite_record') }}
                </div>
                  </article>
                </div>
              </details>

            <details class="module-advanced-section">
              <summary>查看审核访问与调度</summary>
              <div class="module-advanced-section-body">
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
                <div class="hint">以下地址来自手工配置的审核页基地址，可直接发给局域网内对应楼栋电脑访问。</div>
                <div class="hint" v-if="health.handover.review_base_url_effective">
                  当前生效地址（手工指定）：{{ health.handover.review_base_url_effective }}
                </div>
                <div class="hint" v-else-if="health.handover.review_base_url_error">
                  {{ health.handover.review_base_url_error }}
                </div>
                <div class="hint" v-else-if="health.handover.review_base_url_status === 'manual_only'">
                  请先在配置中心手工填写审核页访问基地址
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
                    <div class="hint" style="margin-top:4px;">
                      审核链接发送：
                      <span class="status-badge status-badge-soft" :class="'tone-' + row.reviewLinkDeliveryTone">{{ row.reviewLinkDeliveryText }}</span>
                    </div>
                    <div class="hint" style="margin-top:4px;">
                      接收人状态：
                      <span class="status-badge status-badge-soft" :class="'tone-' + (row.actions.reviewLinkSend.allowed ? 'success' : 'neutral')">
                        {{ row.reviewLinkRecipientStatus.text || '等待后端接收人状态' }}
                      </span>
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
                    <div class="btn-line" style="margin-top:8px;">
                      <button
                        class="btn btn-secondary"
                        :disabled="isHandoverReviewLinkSendDisabled(row)"
                        @click="sendHandoverReviewLink(row.building, { batchKey: handoverReviewOverview.batchKey, force: true })"
                      >
                        {{ getHandoverReviewLinkSendButtonText(row) }}
                      </button>
                    </div>
                    <div class="hint" v-if="getHandoverReviewLinkSendDisabledReason(row)">{{ getHandoverReviewLinkSendDisabledReason(row) }}</div>
                    <div class="hint" v-if="row.reviewLinkDeliveryLastSentAt">最近发送：{{ row.reviewLinkDeliveryLastSentAt }}</div>
                    <div class="hint" v-else-if="row.reviewLinkDeliveryLastAttemptAt">最近尝试：{{ row.reviewLinkDeliveryLastAttemptAt }}</div>
                    <div class="hint" v-if="row.reviewLinkDeliveryError">{{ row.reviewLinkDeliveryError }}</div>
                    <div class="hint" v-if="row.cloudSheetError">{{ row.cloudSheetError }}</div>
                  </div>
                </div>
              </article>
                </div>
              </div>
            </details>
          </div>
          </div>
          </div>
        </section>

`;


