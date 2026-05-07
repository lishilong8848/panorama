export const DASHBOARD_ALARM_EVENT_UPLOAD_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'alarm_event_upload'">
          <div class="dashboard-module-shell">
                      <article class="task-block dashboard-module-scheduler-card">
                        <div class="task-block-head">
                          <div>
                            <div class="task-block-kicker">调度卡</div>
                            <h3 class="card-title">告警信息上传调度</h3>
                          </div>
                          <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('alarm_event_upload')">
                            {{ getSchedulerStatusText('alarm_event_upload') || '-' }}
                          </span>
                        </div>
                        <div class="status-metric-grid status-metric-grid-compact">
                        <div class="status-metric">
                            <div class="status-metric-label">下次执行</div>
                            <strong class="status-metric-value">{{ getSchedulerDisplayText('alarm_event_upload', 'next_run_text', '-') }}</strong>
                        </div>
                          <div class="status-metric">
                            <div class="status-metric-label">最近触发</div>
                            <strong class="status-metric-value">{{ getSchedulerDisplayText('alarm_event_upload', 'last_trigger_text', '-') }}</strong>
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
                            <input type="time" step="1" :value="config.alarm_export.scheduler.run_time" :disabled="alarmEventUploadSchedulerQuickSaving" @change="saveAlarmEventUploadSchedulerQuickConfig({ run_time: $event.target.value })" />
                          </div>
                          <div class="form-row">
                            <label class="label">最近决策</label>
                            <div class="readonly-inline-card">{{ alarmEventUploadSchedulerDecisionText || '-' }}</div>
                          </div>
                        </div>
                        <div class="btn-line">
                          <button
                            class="btn btn-success"
                            :disabled="alarmEventUploadSchedulerQuickSaving || isSchedulerStartDisabled('alarm_event_upload', actionKeyAlarmEventUploadSchedulerStart, actionKeyAlarmEventUploadSchedulerStop)"
                            @click="startAlarmEventUploadScheduler"
                          >
                            {{ getSchedulerStartButtonText('alarm_event_upload') }}
                          </button>
                          <button
                            class="btn btn-danger"
                            :disabled="alarmEventUploadSchedulerQuickSaving || isSchedulerStopDisabled('alarm_event_upload', actionKeyAlarmEventUploadSchedulerStart, actionKeyAlarmEventUploadSchedulerStop)"
                            @click="stopAlarmEventUploadScheduler"
                          >
                            {{ getSchedulerStopButtonText('alarm_event_upload') }}
                          </button>
                        </div>
                        <div class="hint">{{ alarmEventUploadSchedulerQuickSaving ? '告警信息上传调度配置同步中...' : '修改每日执行时间后立即生效。' }}</div>
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
                  <span class="status-badge status-badge-soft" :class="'tone-' + externalAlarmUploadStatus.tone">
                    {{ externalAlarmUploadStatus.statusText }}
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
                <div class="hint">{{ externalAlarmUploadStatus.summaryText }}</div>
                <template v-if="externalAlarmReadinessFamily.metaLines && externalAlarmReadinessFamily.metaLines.length">
                  <div
                    class="hint"
                    v-for="(line, idx) in externalAlarmReadinessFamily.metaLines"
                    :key="'alarm-upload-status-meta-' + idx"
                  >
                    {{ line }}
                  </div>
                </template>
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
                          <div class="status-list" v-if="externalAlarmReadinessFamily.items && externalAlarmReadinessFamily.items.length">
                            <div
                              v-for="(item, idx) in externalAlarmReadinessFamily.items"
                              :key="'alarm-upload-family-item-' + idx"
                              class="status-list-row"
                            >
                              <span class="status-list-label">{{ item.label }}</span>
                              <span class="status-list-value" :class="'tone-' + (item.tone || 'neutral')">{{ item.value }}</span>
                            </div>
                          </div>
                          <template v-if="externalAlarmReadinessFamily.metaLines && externalAlarmReadinessFamily.metaLines.length">
                            <div
                              class="hint"
                              v-for="(line, idx) in externalAlarmReadinessFamily.metaLines"
                              :key="'alarm-upload-family-meta-' + idx"
                            >
                              {{ line }}
                            </div>
                          </template>
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
                              <template v-if="building.metaLines && building.metaLines.length">
                                <div
                                  class="hint"
                                  v-for="(line, idx) in building.metaLines"
                                  :key="'alarm-upload-family-building-meta-' + building.building + '-' + idx"
                                >
                                  {{ line }}
                                </div>
                              </template>
                              <template v-else>
                                <div class="hint">等待后端明细</div>
                              </template>
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
                          <template v-if="externalAlarmReadinessFamily.metaLines && externalAlarmReadinessFamily.metaLines.length">
                            <div
                              class="hint"
                              v-for="(line, idx) in externalAlarmReadinessFamily.metaLines"
                              :key="'alarm-upload-advanced-meta-' + idx"
                            >
                              {{ line }}
                            </div>
                          </template>
                        </article>
                          </div>
                        </details>
                      </div>
        </section>

`;


