export const DASHBOARD_ALARM_EVENT_UPLOAD_SECTION = `        <section class="content-card" v-if="!isInternalDeploymentRole && dashboardActiveModule === 'alarm_event_upload'">
          <div class="dashboard-module-shell">
                      <article class="task-block dashboard-module-scheduler-card">
                        <div class="task-block-head">
                          <div>
                            <div class="task-block-kicker">调度卡</div>
                            <h3 class="card-title">告警信息上传调度</h3>
                          </div>
                          <span class="status-badge status-badge-soft" :class="health.alarm_event_upload.scheduler.running ? 'tone-success' : 'tone-neutral'">
                            {{ health.alarm_event_upload.scheduler.status || '-' }}
                          </span>
                        </div>
                        <div class="status-metric-grid status-metric-grid-compact">
                          <div class="status-metric">
                            <div class="status-metric-label">下次执行</div>
                            <strong class="status-metric-value">{{ health.alarm_event_upload.scheduler.next_run_time || '-' }}</strong>
                          </div>
                          <div class="status-metric">
                            <div class="status-metric-label">最近触发</div>
                            <strong class="status-metric-value">{{ health.alarm_event_upload.scheduler.last_trigger_at || '-' }}</strong>
                          </div>
                          <div class="status-metric">
                            <div class="status-metric-label">最近结果</div>
                            <strong class="status-metric-value">{{ alarmEventUploadSchedulerTriggerText || '-' }}</strong>
                          </div>
                        </div>
                        <div class="hint">调度固定执行“使用共享文件上传60天（全部楼栋）”，不提供单楼调度。</div>
                        <div class="task-grid two-col">
                          <div class="form-row">
                            <label class="label">每日执行时间</label>
                            <input type="time" step="1" v-model="config.alarm_export.scheduler.run_time" @change="saveAlarmEventUploadSchedulerQuickConfig" />
                          </div>
                          <div class="form-row">
                            <label class="label">最近决策</label>
                            <div class="readonly-inline-card">{{ alarmEventUploadSchedulerDecisionText || '-' }}</div>
                          </div>
                        </div>
                        <div class="btn-line">
                          <button
                            class="btn btn-success"
                            :disabled="getSchedulerEffectiveRunning('alarm_event_upload', health.alarm_event_upload.scheduler.running) || isActionLocked(actionKeyAlarmEventUploadSchedulerStart) || isActionLocked(actionKeyAlarmEventUploadSchedulerStop) || isSchedulerTogglePending('alarm_event_upload')"
                            @click="startAlarmEventUploadScheduler"
                          >
                            {{
                              getSchedulerToggleMode('alarm_event_upload') === 'starting'
                                ? '启动中...'
                                : (getSchedulerToggleMode('alarm_event_upload') === 'stopping' ? '处理中...' : (getSchedulerEffectiveRunning('alarm_event_upload', health.alarm_event_upload.scheduler.running) ? '已启动调度' : '启动调度'))
                            }}
                          </button>
                          <button
                            class="btn btn-danger"
                            :disabled="!getSchedulerEffectiveRunning('alarm_event_upload', health.alarm_event_upload.scheduler.running) || isActionLocked(actionKeyAlarmEventUploadSchedulerStop) || isActionLocked(actionKeyAlarmEventUploadSchedulerStart) || isSchedulerTogglePending('alarm_event_upload')"
                            @click="stopAlarmEventUploadScheduler"
                          >
                            {{ getSchedulerToggleMode('alarm_event_upload') === 'stopping' ? '停止中...' : (getSchedulerToggleMode('alarm_event_upload') === 'starting' ? '处理中...' : '停止调度') }}
                          </button>
                        </div>
                        <div class="hint">{{ alarmEventUploadSchedulerQuickSaving ? '告警信息上传调度配置保存中...' : '修改每日执行时间后自动保存。' }}</div>
                      </article>

            <div class="dashboard-module-intro">
              <h3 class="card-title">告警信息上传</h3>
              <div class="hint">状态总览只保留告警文件只读状态，所有告警上传入口统一收在这个专项模块里。</div>
              <div class="hint">外网端按楼读取当天最新一份告警文件，缺失则回退昨天最新，并只上传 60 天内的告警记录。</div>
            </div>

            <div class="day-metric-top-grid dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                            <div class="task-block-head">
                              <div>
                                <div class="task-block-kicker">执行入口</div>
                                <h3 class="card-title">上传到告警多维表</h3>
                              </div>
                              <span class="status-badge status-badge-soft" :class="'tone-' + externalAlarmUploadStatus.tone">
                                {{ externalAlarmUploadStatus.statusText }}
                              </span>
                            </div>
                            <div class="status-metric-grid status-metric-grid-compact">
                              <div class="status-metric">
                                <div class="status-metric-label">上传范围</div>
                                <strong class="status-metric-value">{{ externalAlarmUploadBuilding || '全部楼栋' }}</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">最近上传</div>
                                <strong class="status-metric-value">{{ externalAlarmReadinessFamily.uploadLastRunAt || '-' }}</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">最近成功</div>
                                <strong class="status-metric-value">{{ externalAlarmReadinessFamily.uploadLastSuccessAt || '-' }}</strong>
                              </div>
                            </div>
                            <div class="hint">当前按下拉选择决定上传范围；选择“全部楼栋”会上传全部楼栋最近 60 天数据，选择单楼则只上传该楼最近 60 天数据。</div>
                            <div class="hint">{{ externalAlarmUploadStatus.summaryText }}</div>
                            <div class="ops-focus-card">
                              <div class="ops-focus-card-label">当前策略</div>
                              <div class="ops-focus-card-title">{{ externalAlarmUploadBuilding === '全部楼栋' ? '使用共享文件上传 60 天（全部楼栋）' : ('使用共享文件上传 60 天（' + externalAlarmUploadBuilding + '）') }}</div>
                              <div class="ops-focus-card-meta">{{ alarmEventUploadTarget.replaceExistingOnFull ? '全部楼栋模式会按清表重传处理；单楼模式只覆盖该楼最近 60 天数据。' : '全部楼栋模式按增量写入处理；单楼模式仍只覆盖该楼最近 60 天数据。' }}</div>
                            </div>
                            <div class="task-grid two-col" style="margin-top:10px;">
                              <div class="form-row">
                                <label class="label">刷新楼栋</label>
                                <select v-model="externalAlarmUploadBuilding">
                                  <option value="全部楼栋">全部楼栋</option>
                                  <option value="A楼">A楼</option>
                                  <option value="B楼">B楼</option>
                                  <option value="C楼">C楼</option>
                                  <option value="D楼">D楼</option>
                                  <option value="E楼">E楼</option>
                                </select>
                              </div>
                              <div class="form-row">
                                <label class="label">执行策略</label>
                                <div class="readonly-inline-card">
                                  {{ alarmEventUploadTarget.replaceExistingOnFull ? '全部楼栋清表重传 / 单楼覆盖刷新' : '全部楼栋增量写入 / 单楼覆盖刷新' }}
                                </div>
                              </div>
                            </div>
                            <div class="btn-line" style="margin-top:10px;">
                              <button
                                class="btn btn-primary"
                                :disabled="!canRun || isSourceCacheUploadAlarmSelectedLocked"
                                @click="uploadSelectedAlarmSourceCache"
                              >
                                {{ externalAlarmUploadActionButtonText }}
                              </button>
                            </div>
                          </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">状态概览</div>
                    <h3 class="card-title">当前上传状态</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + externalAlarmReadinessFamily.tone">
                    {{ externalAlarmReadinessFamily.statusText }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">目标表</div>
                    <strong class="status-metric-value">{{ alarmEventUploadTarget.statusText }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">上传记录</div>
                    <strong class="status-metric-value">{{ externalAlarmReadinessFamily.uploadRecordCount || 0 }} 条</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">参与文件</div>
                    <strong class="status-metric-value">{{ externalAlarmReadinessFamily.uploadFileCount || 0 }} 份</strong>
                  </div>
                </div>
                <div class="hint">共享文件状态：{{ externalAlarmReadinessFamily.summaryText }}</div>
                <div class="hint" v-if="externalAlarmReadinessFamily.selectionReferenceDate">参考日期：{{ externalAlarmReadinessFamily.selectionReferenceDate }}</div>
                <div class="hint" v-if="externalAlarmReadinessFamily.uploadLastError">最近异常：{{ externalAlarmReadinessFamily.uploadLastError }}</div>
              </article>
            </div>

            <div class="dashboard-module-support-stack">
              <article class="task-block">
                          <div class="task-block-head">
                            <div>
                              <div class="task-block-kicker">共享文件</div>
                              <h3 class="card-title">当天最新告警文件就绪情况</h3>
                            </div>
                            <span class="status-badge status-badge-soft" :class="'tone-' + externalAlarmReadinessFamily.tone">
                              {{ externalAlarmReadinessFamily.statusText }}
                            </span>
                          </div>
                          <div class="hint">{{ externalAlarmReadinessFamily.summaryText }}</div>
                          <div class="hint" v-if="externalAlarmReadinessFamily.selectionReferenceDate">
                            选择策略：当天最新一份，缺失则回退昨天最新。参考日期：{{ externalAlarmReadinessFamily.selectionReferenceDate }}
                          </div>
                          <div class="source-cache-building-grid" v-if="externalAlarmReadinessFamily.buildings && externalAlarmReadinessFamily.buildings.length" style="margin-top:12px;">
                            <div
                              class="source-cache-building-card"
                              v-for="building in externalAlarmReadinessFamily.buildings"
                              :key="'alarm-upload-family-' + building.building"
                            >
                              <div class="source-cache-building-card-head">
                                <span class="source-cache-building-card-title">{{ building.building }}</span>
                                <span class="status-badge status-badge-soft" :class="'tone-' + building.tone">{{ building.stateText }}</span>
                              </div>
                              <div class="hint">同步日期：{{ building.selectionReferenceDate || externalAlarmReadinessFamily.selectionReferenceDate || '-' }}</div>
                              <div class="hint">选择文件时间：{{ building.selectedDownloadedAt || '-' }}</div>
                              <div class="hint">{{ building.detailText || building.selectionScopeText || building.sourceKindText || '-' }}</div>
                              <div class="hint" v-if="building.resolvedFilePath">共享路径：{{ building.resolvedFilePath }}</div>
                              <div class="hint" v-else-if="building.relativePath">缓存文件：{{ building.relativePath }}</div>
                            </div>
                          </div>
                          <div class="hint" v-else style="margin-top:10px;">当前没有可展示的楼栋告警文件状态。</div>
                        </article>
            </div>

            <details class="module-advanced-section">
                          <summary>查看目标多维表与上传记录</summary>
                          <div class="module-advanced-section-body">
                        <article class="task-block">
                          <div class="task-block-head">
                            <div>
                              <div class="task-block-kicker">目标配置</div>
                              <h3 class="card-title">当前告警多维表</h3>
                            </div>
                            <span class="status-badge status-badge-soft" :class="alarmEventUploadTarget.configured ? 'tone-success' : 'tone-warning'">
                              {{ alarmEventUploadTarget.statusText }}
                            </span>
                          </div>
                          <div class="status-metric-grid status-metric-grid-compact">
                            <div class="status-metric">
                              <div class="status-metric-label">上传记录</div>
                              <strong class="status-metric-value">{{ externalAlarmReadinessFamily.uploadRecordCount || 0 }} 条</strong>
                            </div>
                            <div class="status-metric">
                              <div class="status-metric-label">参与文件</div>
                              <strong class="status-metric-value">{{ externalAlarmReadinessFamily.uploadFileCount || 0 }} 份</strong>
                            </div>
                            <div class="status-metric">
                              <div class="status-metric-label">当前策略</div>
                              <strong class="status-metric-value">{{ alarmEventUploadTarget.replaceExistingOnFull ? '清表重传' : '增量写入' }}</strong>
                            </div>
                          </div>
                          <div class="day-metric-summary-grid">
                            <div class="readonly-token-card readonly-token-card-wide">
                              <div class="readonly-token-card-label">App Token</div>
                              <div class="readonly-token-card-value">{{ alarmEventUploadTarget.appToken || '-' }}</div>
                            </div>
                            <div class="readonly-token-card">
                              <div class="readonly-token-card-label">Table ID</div>
                              <div class="readonly-token-card-value">{{ alarmEventUploadTarget.tableId || '-' }}</div>
                            </div>
                            <div class="readonly-inline-card">最近上传：{{ externalAlarmReadinessFamily.uploadLastRunAt || '-' }}</div>
                            <div class="readonly-inline-card">最近成功：{{ externalAlarmReadinessFamily.uploadLastSuccessAt || '-' }}</div>
                            <div class="readonly-inline-card">上传记录：{{ externalAlarmReadinessFamily.uploadRecordCount || 0 }} 条</div>
                            <div class="readonly-inline-card">参与文件：{{ externalAlarmReadinessFamily.uploadFileCount || 0 }} 份</div>
                          </div>
                          <div class="btn-line" style="margin-top:10px;" v-if="alarmEventUploadTarget.displayUrl || alarmEventUploadTarget.bitableUrl">
                            <button
                              class="btn btn-secondary"
                              type="button"
                              @click="openAlarmEventUploadTarget"
                            >
                              打开多维表
                            </button>
                          </div>
                          <div class="hint" style="margin-top:10px;">{{ alarmEventUploadTarget.hintText }}</div>
                          <div class="hint" v-if="externalAlarmReadinessFamily.selectionReferenceDate">
                            选择参考日期：{{ externalAlarmReadinessFamily.selectionReferenceDate }}
                          </div>
                          <div class="hint" v-if="externalAlarmReadinessFamily.uploadRunning">
                            {{ externalAlarmReadinessFamily.uploadRunningText }}
                          </div>
                          <div class="hint" v-if="externalAlarmReadinessFamily.uploadLastError">
                            最近上传异常：{{ externalAlarmReadinessFamily.uploadLastError }}
                          </div>
                        </article>
                          </div>
                        </details>
                      </div>
        </section>

`;
