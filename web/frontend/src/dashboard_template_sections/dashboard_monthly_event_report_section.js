export const DASHBOARD_MONTHLY_EVENT_REPORT_SECTION = `        <section class="content-card" v-if="!isInternalDeploymentRole && dashboardActiveModule === 'monthly_event_report'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-intro">
              <h3 class="card-title">体系月度统计表</h3>
              <div class="hint">每次手动触发或调度执行时，固定读取上一个自然月的新事件处理数据，并生成本地 Excel 文件。</div>
              <div class="hint">本轮只实现“事件月度统计表”；“月度变更统计表”仅保留模板占位，不执行处理链路。</div>
            </div>

            <div
              class="dashboard-module-subshell"
              data-scheduler-overview-target="monthly_event"
              :class="{ 'dashboard-section-spotlight': dashboardSchedulerOverviewFocusKey === 'monthly_event' }"
            >
              <div class="day-metric-top-grid dashboard-module-primary-grid">
                            <article class="task-block dashboard-module-scheduler-card">
                              <div class="task-block-head">
                                <div>
                                  <div class="task-block-kicker">调度卡</div>
                                  <h3 class="card-title">月度事件统计调度</h3>
                                </div>
                                <span class="status-badge status-badge-soft" :class="'tone-' + (health.monthly_event_report.scheduler.running ? 'success' : 'neutral')">
                                  {{ health.monthly_event_report.scheduler.status || '-' }}
                                </span>
                              </div>
                              <div class="status-metric-grid status-metric-grid-compact">
                                <div class="status-metric">
                                  <div class="status-metric-label">下次执行</div>
                                  <strong class="status-metric-value">{{ health.monthly_event_report.scheduler.next_run_time || '-' }}</strong>
                                </div>
                                <div class="status-metric">
                                  <div class="status-metric-label">最近触发</div>
                                  <strong class="status-metric-value">{{ health.monthly_event_report.scheduler.last_trigger_at || '-' }}</strong>
                                </div>
                                <div class="status-metric">
                                  <div class="status-metric-label">最近决策</div>
                                  <strong class="status-metric-value">{{ monthlyEventReportSchedulerDecisionText || '-' }}</strong>
                                </div>
                              </div>
                              <div class="ops-focus-card">
                                <div class="ops-focus-card-label">调度说明</div>
                                <div class="ops-focus-card-title">固定读取上一个自然月，适合月初统一生成统计表</div>
                                <div class="ops-focus-card-meta">触发结果：{{ monthlyEventReportSchedulerTriggerText || '-' }}</div>
                              </div>
                              <div class="hint">下次执行：{{ health.monthly_event_report.scheduler.next_run_time || '-' }}</div>
                              <div class="hint">最近触发：{{ health.monthly_event_report.scheduler.last_trigger_at || '-' }} / {{ monthlyEventReportSchedulerTriggerText || '-' }}</div>
                              <div class="task-grid two-col">
                                <div class="form-row">
                                  <label class="label">每月几号</label>
                                  <input type="number" min="1" max="31" v-model.number="config.handover_log.monthly_event_report.scheduler.day_of_month" @change="saveMonthlyEventReportSchedulerQuickConfig" />
                                </div>
                                <div class="form-row">
                                  <label class="label">时间（HH:mm:ss）</label>
                                  <input type="time" step="1" v-model="config.handover_log.monthly_event_report.scheduler.run_time" @change="saveMonthlyEventReportSchedulerQuickConfig" />
                                </div>
                              </div>
                              <div class="form-row">
                                <label class="label">检查间隔（秒）</label>
                                <input type="number" min="1" v-model.number="config.handover_log.monthly_event_report.scheduler.check_interval_sec" @change="saveMonthlyEventReportSchedulerQuickConfig" />
                              </div>
                              <div class="btn-line">
                                <button
                                  class="btn btn-success"
                                  :disabled="getSchedulerEffectiveRunning('monthly_event_report', health.monthly_event_report.scheduler.running) || isActionLocked(actionKeyMonthlyEventReportSchedulerStart) || isActionLocked(actionKeyMonthlyEventReportSchedulerStop) || isSchedulerTogglePending('monthly_event_report')"
                                  @click="startMonthlyEventReportScheduler"
                                >
                                  {{
                                    getSchedulerToggleMode('monthly_event_report') === 'starting'
                                      ? '启动中...'
                                      : (getSchedulerToggleMode('monthly_event_report') === 'stopping' ? '处理中...' : (getSchedulerEffectiveRunning('monthly_event_report', health.monthly_event_report.scheduler.running) ? '已启动调度' : '启动调度'))
                                  }}
                                </button>
                                <button
                                  class="btn btn-danger"
                                  :disabled="!getSchedulerEffectiveRunning('monthly_event_report', health.monthly_event_report.scheduler.running) || isActionLocked(actionKeyMonthlyEventReportSchedulerStop) || isActionLocked(actionKeyMonthlyEventReportSchedulerStart) || isSchedulerTogglePending('monthly_event_report')"
                                  @click="stopMonthlyEventReportScheduler"
                                >
                                  {{ getSchedulerToggleMode('monthly_event_report') === 'stopping' ? '停止中...' : (getSchedulerToggleMode('monthly_event_report') === 'starting' ? '处理中...' : '停止调度') }}
                                </button>
                              </div>
                              <div class="hint">{{ monthlyEventReportSchedulerQuickSaving ? '事件月报调度配置保存中...' : '修改日期、时间或检查间隔后自动保存。' }}</div>
                            </article>

                            <article class="task-block task-block-accent">
                              <div class="task-block-head">
                                <div>
                                  <div class="task-block-kicker">手动触发卡</div>
                                  <h3 class="card-title">立即生成事件月度统计表</h3>
                                </div>
                                <span class="status-badge status-badge-soft tone-info">上月窗口</span>
                              </div>
                              <div class="status-metric-grid status-metric-grid-compact">
                                <div class="status-metric">
                                  <div class="status-metric-label">目标月份</div>
                                  <strong class="status-metric-value">{{ monthlyEventReportLastRun.target_month || '上一个自然月' }}</strong>
                                </div>
                                <div class="status-metric">
                                  <div class="status-metric-label">楼栋范围</div>
                                  <strong class="status-metric-value">A楼至E楼</strong>
                                </div>
                                <div class="status-metric">
                                  <div class="status-metric-label">输出目录</div>
                                  <strong class="status-metric-value">{{ monthlyEventReportOutputDir || '-' }}</strong>
                                </div>
                              </div>
                              <div class="ops-focus-card">
                                <div class="ops-focus-card-label">执行说明</div>
                                <div class="ops-focus-card-title">无论是否有数据，五个楼都会生成对应月度文件</div>
                                <div class="ops-focus-card-meta">适合月度补跑或单楼重生；默认窗口始终是上一个自然月。</div>
                              </div>
                              <div class="hint">全部楼栋固定为 A楼、B楼、C楼、D楼、E楼；即使某楼无数据，也会生成空表。</div>
                              <div class="btn-line" style="flex-wrap:wrap;">
                                <button
                                  class="btn btn-primary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyEventReportRunAll)"
                                  @click="runMonthlyEventReport('all')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyEventReportRunAll) ? '提交中...' : '全部楼栋' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'A楼')"
                                  @click="runMonthlyEventReport('building', 'A楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'A楼') ? '提交中...' : 'A楼' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'B楼')"
                                  @click="runMonthlyEventReport('building', 'B楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'B楼') ? '提交中...' : 'B楼' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'C楼')"
                                  @click="runMonthlyEventReport('building', 'C楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'C楼') ? '提交中...' : 'C楼' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'D楼')"
                                  @click="runMonthlyEventReport('building', 'D楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'D楼') ? '提交中...' : 'D楼' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'E楼')"
                                  @click="runMonthlyEventReport('building', 'E楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyEventReportRunBuildingPrefix + 'E楼') ? '提交中...' : 'E楼' }}
                                </button>
                              </div>
                            </article>

                <article class="task-block task-block-compact dashboard-module-status-card">
                  <div class="task-block-head">
                    <div>
                      <div class="task-block-kicker">状态概览</div>
                      <h3 class="card-title">当前事件月报状态</h3>
                    </div>
                    <span class="status-badge status-badge-soft" :class="'tone-' + (monthlyEventReportLastRun.status === 'ok' ? 'success' : monthlyEventReportLastRun.status === 'partial_failed' ? 'warning' : monthlyEventReportLastRun.status === 'failed' ? 'danger' : 'neutral')">
                      {{ monthlyEventReportLastRun.status || '尚未执行' }}
                    </span>
                  </div>
                  <div class="status-metric-grid status-metric-grid-compact">
                    <div class="status-metric">
                      <div class="status-metric-label">目标月份</div>
                      <strong class="status-metric-value">{{ monthlyEventReportLastRun.target_month || '上一个自然月' }}</strong>
                    </div>
                    <div class="status-metric">
                      <div class="status-metric-label">生成文件数</div>
                      <strong class="status-metric-value">{{ monthlyEventReportLastRun.generated_files || 0 }}</strong>
                    </div>
                    <div class="status-metric">
                      <div class="status-metric-label">最近发送</div>
                      <strong class="status-metric-value">{{ monthlyEventReportDeliveryLastRun.finished_at || monthlyEventReportDeliveryLastRun.started_at || '-' }}</strong>
                    </div>
                  </div>
                  <div class="hint">输出目录：{{ monthlyEventReportOutputDir || '-' }}</div>
                  <div class="hint">成功楼栋：{{ (monthlyEventReportLastRun.successful_buildings || []).join('、') || '-' }}</div>
                  <div class="hint">失败楼栋：{{ (monthlyEventReportLastRun.failed_buildings || []).join('、') || '-' }}</div>
                </article>
              </div>

                          <details class="module-advanced-section">
                            <summary>查看事件月报发送设置</summary>
                            <div class="module-advanced-section-body">
                          <article class="task-block">
                            <div class="task-block-head">
                              <div>
                                <div class="task-block-kicker">文件发送</div>
                                <h3 class="card-title">发送事件月度统计表到飞书</h3>
                              </div>
                              <span class="status-badge status-badge-soft" :class="'tone-' + monthlyEventReportDeliveryStatus.tone">
                                {{ monthlyEventReportDeliveryStatus.statusText }}
                              </span>
                            </div>
                            <div class="status-metric-grid status-metric-grid-compact">
                              <div class="status-metric">
                                <div class="status-metric-label">满足发送</div>
                                <strong class="status-metric-value">{{ monthlyEventReportSendReadyCount }}/5</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">测试接收人</div>
                                <strong class="status-metric-value">{{ monthlyReportTestReceiveCount }} 人</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">最近状态</div>
                                <strong class="status-metric-value">{{ monthlyEventReportDeliveryStatus.statusText }}</strong>
                              </div>
                            </div>
                            <div class="hint">{{ monthlyEventReportDeliveryStatus.summaryText }}</div>
                            <div class="hint">当前只发送事件月度统计表；收件人来自工程师目录中职位包含“设施运维主管”的唯一记录。</div>
                            <div class="hint">已满足发送条件：{{ monthlyEventReportSendReadyCount }}/5</div>
                            <div class="task-grid two-col" style="margin-top:10px;">
                              <div class="form-row">
                                <label class="label">测试 receive_id_type</label>
                                <select v-model="monthlyReportTestReceiveIdType">
                                  <option value="open_id">open_id</option>
                                  <option value="user_id">user_id</option>
                                  <option value="email">email</option>
                                  <option value="mobile">mobile</option>
                                </select>
                              </div>
                              <div class="form-row">
                                <label class="label">测试接收人数</label>
                                <div class="readonly-inline-card">{{ monthlyReportTestReceiveCount }} 人</div>
                              </div>
                            </div>
                            <div class="form-row" style="margin-top:10px;align-items:flex-start;">
                              <label class="label">新增测试接收人 ID</label>
                              <div style="display:flex;gap:8px;align-items:center;flex:1 1 auto;min-width:0;">
                                <input
                                  type="text"
                                  v-model="monthlyReportTestReceiveIdDraftEvent"
                                  placeholder="输入单个接收人 ID"
                                  @keydown.enter.prevent="addMonthlyReportTestReceiveId('event')"
                                />
                                <button class="btn btn-secondary" type="button" @click="addMonthlyReportTestReceiveId('event')">
                                  添加
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  type="button"
                                  :disabled="isActionLocked(actionKeyConfigSave)"
                                  @click="saveConfig"
                                >
                                  {{ isActionLocked(actionKeyConfigSave) ? '保存中...' : '保存测试配置' }}
                                </button>
                              </div>
                            </div>
                            <div class="hint">测试接收人通过“添加”按钮维护；点击“保存测试配置”后会写入配置文件，下次启动仍可直接使用。</div>
                            <table class="site-table" style="margin-top:10px;" v-if="monthlyReportTestReceiveIds.length">
                              <thead>
                                <tr>
                                  <th>测试接收人 ID</th>
                                  <th style="width:88px;">操作</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr v-for="receiveId in monthlyReportTestReceiveIds" :key="'monthly-test-receiver-' + receiveId">
                                  <td style="word-break:break-all;">{{ receiveId }}</td>
                                  <td>
                                    <button class="btn btn-danger" type="button" @click="removeMonthlyReportTestReceiveId(receiveId)">
                                      删除
                                    </button>
                                  </td>
                                </tr>
                              </tbody>
                            </table>
                            <div class="hint" v-else style="margin-top:10px;">暂未添加测试接收人 ID。</div>
                            <div class="hint">测试发送会复用一份已生成月报，同时向当前已添加的全部接收人发送同一个文件。</div>
                            <div class="btn-line" style="flex-wrap:wrap;margin-top:10px;">
                              <button
                                class="btn btn-primary"
                                :disabled="!canRun || !monthlyEventReportSendReadyCount || isActionLocked(monthlyEventReportSendAllActionKey)"
                                @click="sendMonthlyReport('event', 'all')"
                              >
                                {{ isActionLocked(monthlyEventReportSendAllActionKey) ? '提交中...' : '一键全部发送' }}
                              </button>
                              <button
                                class="btn btn-secondary"
                                :disabled="!canRun || !(monthlyEventReportLastRun.generated_files || 0) || !monthlyReportTestReceiveCount || isActionLocked(monthlyEventReportSendTestActionKey)"
                                @click="sendMonthlyReportTest('event')"
                              >
                                {{ isActionLocked(monthlyEventReportSendTestActionKey) ? '提交中...' : ('测试发送（' + (monthlyReportTestReceiveCount || 0) + '人）') }}
                              </button>
                              <button
                                v-for="row in monthlyEventReportRecipientStatusByBuilding"
                                :key="'monthly-send-' + row.building"
                                class="btn btn-secondary"
                                :title="row.detailText"
                                :disabled="!canRun || !row.sendReady || isActionLocked(getMonthlyReportSendBuildingActionKey('event', row.building))"
                                @click="sendMonthlyReport('event', 'building', row.building)"
                              >
                                {{ isActionLocked(getMonthlyReportSendBuildingActionKey('event', row.building)) ? '提交中...' : row.building }}
                              </button>
                            </div>
                            <table class="site-table" style="margin-top:12px;">
                              <thead>
                                <tr>
                                  <th style="width:72px;">楼栋</th>
                                  <th style="width:88px;">状态</th>
                                  <th style="width:120px;">主管</th>
                                  <th style="width:140px;">职位</th>
                                  <th style="width:110px;">ID 类型</th>
                                  <th>身份 ID</th>
                                  <th style="width:180px;">文件</th>
                                  <th>说明</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr
                                  v-for="row in monthlyEventReportRecipientStatusByBuilding"
                                  :key="'monthly-recipient-' + row.building"
                                >
                                  <td>{{ row.building }}</td>
                                  <td>
                                    <span class="status-badge status-badge-soft" :class="'tone-' + row.tone">{{ row.statusText }}</span>
                                  </td>
                                  <td>{{ row.supervisor || '-' }}</td>
                                  <td>{{ row.position || '-' }}</td>
                                  <td>{{ row.receiveIdType || '-' }}</td>
                                  <td style="word-break:break-all;">{{ row.recipientId || '-' }}</td>
                                  <td style="word-break:break-all;">{{ row.fileName || '-' }}</td>
                                  <td>{{ row.detailText }}</td>
                                </tr>
                              </tbody>
                            </table>
                          </article>
                            </div>
                          </details>

                          <details class="module-advanced-section">
                            <summary>查看事件月报最近结果</summary>
                            <div class="module-advanced-section-body">
                          <article class="task-block">
                            <div class="task-block-head">
                              <div>
                                <div class="task-block-kicker">最近结果卡</div>
                                <h3 class="card-title">最近一次事件体系月度统计表</h3>
                              </div>
                              <span class="status-badge status-badge-soft" :class="'tone-' + (monthlyEventReportLastRun.status === 'ok' ? 'success' : monthlyEventReportLastRun.status === 'partial_failed' ? 'warning' : monthlyEventReportLastRun.status === 'failed' ? 'danger' : 'neutral')">
                                {{ monthlyEventReportLastRun.status || '尚未执行' }}
                              </span>
                            </div>
                            <div class="status-metric-grid status-metric-grid-compact">
                              <div class="status-metric">
                                <div class="status-metric-label">目标月份</div>
                                <strong class="status-metric-value">{{ monthlyEventReportLastRun.target_month || '-' }}</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">生成文件数</div>
                                <strong class="status-metric-value">{{ monthlyEventReportLastRun.generated_files || 0 }}</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">最近发送</div>
                                <strong class="status-metric-value">{{ monthlyEventReportDeliveryLastRun.finished_at || monthlyEventReportDeliveryLastRun.started_at || '-' }}</strong>
                              </div>
                            </div>
                            <div class="hint">最近运行：{{ monthlyEventReportLastRun.finished_at || monthlyEventReportLastRun.started_at || '-' }}</div>
                            <div class="hint">目标月份：{{ monthlyEventReportLastRun.target_month || '-' }}</div>
                            <div class="hint">生成文件数：{{ monthlyEventReportLastRun.generated_files || 0 }}</div>
                            <div class="hint">成功楼栋：{{ (monthlyEventReportLastRun.successful_buildings || []).join('、') || '-' }}</div>
                            <div class="hint">失败楼栋：{{ (monthlyEventReportLastRun.failed_buildings || []).join('、') || '-' }}</div>
                            <div class="hint">输出目录：{{ monthlyEventReportOutputDir }}</div>
                            <div class="hint" v-if="monthlyEventReportLastRun.error">错误：{{ monthlyEventReportLastRun.error }}</div>
                            <div class="hr" style="margin:10px 0;"></div>
                            <div class="hint">最近发送：{{ monthlyEventReportDeliveryLastRun.finished_at || monthlyEventReportDeliveryLastRun.started_at || '-' }}</div>
                            <div class="hint">发送目标月份：{{ monthlyEventReportDeliveryLastRun.target_month || '-' }}</div>
                            <div class="hint">发送成功楼栋：{{ (monthlyEventReportDeliveryLastRun.successful_buildings || []).join('、') || '-' }}</div>
                            <div class="hint">发送失败楼栋：{{ (monthlyEventReportDeliveryLastRun.failed_buildings || []).join('、') || '-' }}</div>
                            <div class="hint" v-if="monthlyEventReportDeliveryLastRun.test_mode">最近发送类型：测试发送（多接收人）</div>
                            <div class="hint" v-if="(monthlyEventReportDeliveryLastRun.test_receive_ids || []).length">
                              测试接收人：{{ (monthlyEventReportDeliveryLastRun.test_receive_ids || []).join('、') }} / {{ monthlyEventReportDeliveryLastRun.test_receive_id_type || '-' }}
                            </div>
                            <div class="hint" v-if="(monthlyEventReportDeliveryLastRun.test_successful_receivers || []).length">
                              测试发送成功：{{ (monthlyEventReportDeliveryLastRun.test_successful_receivers || []).join('、') }}
                            </div>
                            <div class="hint" v-if="(monthlyEventReportDeliveryLastRun.test_failed_receivers || []).length">
                              测试发送失败：{{ (monthlyEventReportDeliveryLastRun.test_failed_receivers || []).join('、') }}
                            </div>
                            <div class="hint" v-if="monthlyEventReportDeliveryLastRun.test_file_name">测试发送文件：{{ monthlyEventReportDeliveryLastRun.test_file_building || '-' }} / {{ monthlyEventReportDeliveryLastRun.test_file_name }}</div>
                            <div class="hint" v-if="monthlyEventReportDeliveryLastRun.error">发送错误：{{ monthlyEventReportDeliveryLastRun.error }}</div>
                          </article>
                            </div>
                          </details>
                        </div>
            </div>

            <div class="hr dashboard-module-divider"></div>

            <div
              class="dashboard-module-subshell"
              data-scheduler-overview-target="monthly_change"
              :class="{ 'dashboard-section-spotlight': dashboardSchedulerOverviewFocusKey === 'monthly_change' }"
            >
              <div class="day-metric-top-grid dashboard-module-primary-grid">
                            <article class="task-block dashboard-module-scheduler-card">
                              <div class="task-block-head">
                                <div>
                                  <div class="task-block-kicker">调度卡</div>
                                  <h3 class="card-title">月度变更统计调度</h3>
                                </div>
                                <span class="status-badge status-badge-soft" :class="'tone-' + (health.monthly_change_report.scheduler.running ? 'success' : 'neutral')">
                                  {{ health.monthly_change_report.scheduler.status || '-' }}
                                </span>
                              </div>
                              <div class="status-metric-grid status-metric-grid-compact">
                                <div class="status-metric">
                                  <div class="status-metric-label">下次执行</div>
                                  <strong class="status-metric-value">{{ health.monthly_change_report.scheduler.next_run_time || '-' }}</strong>
                                </div>
                                <div class="status-metric">
                                  <div class="status-metric-label">最近触发</div>
                                  <strong class="status-metric-value">{{ health.monthly_change_report.scheduler.last_trigger_at || '-' }}</strong>
                                </div>
                                <div class="status-metric">
                                  <div class="status-metric-label">最近决策</div>
                                  <strong class="status-metric-value">{{ monthlyChangeReportSchedulerDecisionText || '-' }}</strong>
                                </div>
                              </div>
                              <div class="ops-focus-card">
                                <div class="ops-focus-card-label">调度说明</div>
                                <div class="ops-focus-card-title">按变更开始时间统计上一个自然月，适合月初统一归档</div>
                                <div class="ops-focus-card-meta">触发结果：{{ monthlyChangeReportSchedulerTriggerText || '-' }}</div>
                              </div>
                              <div class="hint">下次执行：{{ health.monthly_change_report.scheduler.next_run_time || '-' }}</div>
                              <div class="hint">最近触发：{{ health.monthly_change_report.scheduler.last_trigger_at || '-' }} / {{ monthlyChangeReportSchedulerTriggerText || '-' }}</div>
                              <div class="task-grid two-col">
                                <div class="form-row">
                                  <label class="label">每月几号</label>
                                  <input type="number" min="1" max="31" v-model.number="config.handover_log.monthly_change_report.scheduler.day_of_month" @change="saveMonthlyChangeReportSchedulerQuickConfig" />
                                </div>
                                <div class="form-row">
                                  <label class="label">时间（HH:mm:ss）</label>
                                  <input type="time" step="1" v-model="config.handover_log.monthly_change_report.scheduler.run_time" @change="saveMonthlyChangeReportSchedulerQuickConfig" />
                                </div>
                              </div>
                              <div class="form-row">
                                <label class="label">检查间隔（秒）</label>
                                <input type="number" min="1" v-model.number="config.handover_log.monthly_change_report.scheduler.check_interval_sec" @change="saveMonthlyChangeReportSchedulerQuickConfig" />
                              </div>
                              <div class="btn-line">
                                <button
                                  class="btn btn-success"
                                  :disabled="getSchedulerEffectiveRunning('monthly_change_report', health.monthly_change_report.scheduler.running) || isActionLocked(actionKeyMonthlyChangeReportSchedulerStart) || isActionLocked(actionKeyMonthlyChangeReportSchedulerStop) || isSchedulerTogglePending('monthly_change_report')"
                                  @click="startMonthlyChangeReportScheduler"
                                >
                                  {{
                                    getSchedulerToggleMode('monthly_change_report') === 'starting'
                                      ? '启动中...'
                                      : (getSchedulerToggleMode('monthly_change_report') === 'stopping' ? '处理中...' : (getSchedulerEffectiveRunning('monthly_change_report', health.monthly_change_report.scheduler.running) ? '已启动调度' : '启动调度'))
                                  }}
                                </button>
                                <button
                                  class="btn btn-danger"
                                  :disabled="!getSchedulerEffectiveRunning('monthly_change_report', health.monthly_change_report.scheduler.running) || isActionLocked(actionKeyMonthlyChangeReportSchedulerStop) || isActionLocked(actionKeyMonthlyChangeReportSchedulerStart) || isSchedulerTogglePending('monthly_change_report')"
                                  @click="stopMonthlyChangeReportScheduler"
                                >
                                  {{ getSchedulerToggleMode('monthly_change_report') === 'stopping' ? '停止中...' : (getSchedulerToggleMode('monthly_change_report') === 'starting' ? '处理中...' : '停止调度') }}
                                </button>
                              </div>
                              <div class="hint">{{ monthlyChangeReportSchedulerQuickSaving ? '变更月报调度配置保存中...' : '修改日期、时间或检查间隔后自动保存。' }}</div>
                            </article>

                            <article class="task-block task-block-accent">
                              <div class="task-block-head">
                                <div>
                                  <div class="task-block-kicker">手动触发卡</div>
                                  <h3 class="card-title">立即生成变更月度统计表</h3>
                                </div>
                                <span class="status-badge status-badge-soft tone-info">上月窗口</span>
                              </div>
                              <div class="status-metric-grid status-metric-grid-compact">
                                <div class="status-metric">
                                  <div class="status-metric-label">目标月份</div>
                                  <strong class="status-metric-value">{{ monthlyChangeReportLastRun.target_month || '上一个自然月' }}</strong>
                                </div>
                                <div class="status-metric">
                                  <div class="status-metric-label">楼栋范围</div>
                                  <strong class="status-metric-value">A楼至E楼</strong>
                                </div>
                                <div class="status-metric">
                                  <div class="status-metric-label">输出目录</div>
                                  <strong class="status-metric-value">{{ monthlyChangeReportOutputDir || '-' }}</strong>
                                </div>
                              </div>
                              <div class="ops-focus-card">
                                <div class="ops-focus-card-label">执行说明</div>
                                <div class="ops-focus-card-title">按变更开始时间回溯上月数据，无数据楼栋也会生成空表</div>
                                <div class="ops-focus-card-meta">适合按楼重生或月度补跑；默认窗口始终是上一个自然月。</div>
                              </div>
                              <div class="hint">全部楼栋固定为 A楼、B楼、C楼、D楼、E楼；按“变更开始时间”统计上一个自然月，无数据楼栋也会生成空表。</div>
                              <div class="btn-line" style="flex-wrap:wrap;">
                                <button
                                  class="btn btn-primary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyChangeReportRunAll)"
                                  @click="runMonthlyChangeReport('all')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyChangeReportRunAll) ? '提交中...' : '全部楼栋' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'A楼')"
                                  @click="runMonthlyChangeReport('building', 'A楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'A楼') ? '提交中...' : 'A楼' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'B楼')"
                                  @click="runMonthlyChangeReport('building', 'B楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'B楼') ? '提交中...' : 'B楼' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'C楼')"
                                  @click="runMonthlyChangeReport('building', 'C楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'C楼') ? '提交中...' : 'C楼' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'D楼')"
                                  @click="runMonthlyChangeReport('building', 'D楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'D楼') ? '提交中...' : 'D楼' }}
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  :disabled="!canRun || isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'E楼')"
                                  @click="runMonthlyChangeReport('building', 'E楼')"
                                >
                                  {{ isActionLocked(actionKeyMonthlyChangeReportRunBuildingPrefix + 'E楼') ? '提交中...' : 'E楼' }}
                                </button>
                              </div>
                            </article>

                <article class="task-block task-block-compact dashboard-module-status-card">
                  <div class="task-block-head">
                    <div>
                      <div class="task-block-kicker">状态概览</div>
                      <h3 class="card-title">当前变更月报状态</h3>
                    </div>
                    <span class="status-badge status-badge-soft" :class="'tone-' + (monthlyChangeReportLastRun.status === 'ok' ? 'success' : monthlyChangeReportLastRun.status === 'partial_failed' ? 'warning' : monthlyChangeReportLastRun.status === 'failed' ? 'danger' : 'neutral')">
                      {{ monthlyChangeReportLastRun.status || '尚未执行' }}
                    </span>
                  </div>
                  <div class="status-metric-grid status-metric-grid-compact">
                    <div class="status-metric">
                      <div class="status-metric-label">目标月份</div>
                      <strong class="status-metric-value">{{ monthlyChangeReportLastRun.target_month || '上一个自然月' }}</strong>
                    </div>
                    <div class="status-metric">
                      <div class="status-metric-label">生成文件数</div>
                      <strong class="status-metric-value">{{ monthlyChangeReportLastRun.generated_files || 0 }}</strong>
                    </div>
                    <div class="status-metric">
                      <div class="status-metric-label">最近发送</div>
                      <strong class="status-metric-value">{{ monthlyChangeReportDeliveryLastRun.finished_at || monthlyChangeReportDeliveryLastRun.started_at || '-' }}</strong>
                    </div>
                  </div>
                  <div class="hint">输出目录：{{ monthlyChangeReportOutputDir || '-' }}</div>
                  <div class="hint">成功楼栋：{{ (monthlyChangeReportLastRun.successful_buildings || []).join('、') || '-' }}</div>
                  <div class="hint">失败楼栋：{{ (monthlyChangeReportLastRun.failed_buildings || []).join('、') || '-' }}</div>
                </article>
              </div>

                          <details class="module-advanced-section">
                            <summary>查看变更月报发送设置</summary>
                            <div class="module-advanced-section-body">
                          <article class="task-block">
                            <div class="task-block-head">
                              <div>
                                <div class="task-block-kicker">文件发送</div>
                                <h3 class="card-title">发送变更月度统计表到飞书</h3>
                              </div>
                              <span class="status-badge status-badge-soft" :class="'tone-' + monthlyChangeReportDeliveryStatus.tone">
                                {{ monthlyChangeReportDeliveryStatus.statusText }}
                              </span>
                            </div>
                            <div class="status-metric-grid status-metric-grid-compact">
                              <div class="status-metric">
                                <div class="status-metric-label">满足发送</div>
                                <strong class="status-metric-value">{{ monthlyChangeReportSendReadyCount }}/5</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">测试接收人</div>
                                <strong class="status-metric-value">{{ monthlyReportTestReceiveCount }} 人</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">最近状态</div>
                                <strong class="status-metric-value">{{ monthlyChangeReportDeliveryStatus.statusText }}</strong>
                              </div>
                            </div>
                            <div class="hint">{{ monthlyChangeReportDeliveryStatus.summaryText }}</div>
                            <div class="hint">当前发送变更月度统计表；收件人来自工程师目录中职位包含“设施运维主管”的唯一记录。</div>
                            <div class="hint">已满足发送条件：{{ monthlyChangeReportSendReadyCount }}/5</div>
                            <div class="task-grid two-col" style="margin-top:10px;">
                              <div class="form-row">
                                <label class="label">测试 receive_id_type</label>
                                <select v-model="monthlyReportTestReceiveIdType">
                                  <option value="open_id">open_id</option>
                                  <option value="user_id">user_id</option>
                                  <option value="email">email</option>
                                  <option value="mobile">mobile</option>
                                </select>
                              </div>
                              <div class="form-row">
                                <label class="label">测试接收人数</label>
                                <div class="readonly-inline-card">{{ monthlyReportTestReceiveCount }} 人</div>
                              </div>
                            </div>
                            <div class="form-row" style="margin-top:10px;align-items:flex-start;">
                              <label class="label">新增测试接收人 ID</label>
                              <div style="display:flex;gap:8px;align-items:center;flex:1 1 auto;min-width:0;">
                                <input
                                  type="text"
                                  v-model="monthlyReportTestReceiveIdDraftChange"
                                  placeholder="输入单个接收人 ID"
                                  @keydown.enter.prevent="addMonthlyReportTestReceiveId('change')"
                                />
                                <button class="btn btn-secondary" type="button" @click="addMonthlyReportTestReceiveId('change')">
                                  添加
                                </button>
                                <button
                                  class="btn btn-secondary"
                                  type="button"
                                  :disabled="isActionLocked(actionKeyConfigSave)"
                                  @click="saveConfig"
                                >
                                  {{ isActionLocked(actionKeyConfigSave) ? '保存中...' : '保存测试配置' }}
                                </button>
                              </div>
                            </div>
                            <div class="hint">测试接收人通过“添加”按钮维护；点击“保存测试配置”后会写入配置文件，下次启动仍可直接使用。</div>
                            <table class="site-table" style="margin-top:10px;" v-if="monthlyReportTestReceiveIds.length">
                              <thead>
                                <tr>
                                  <th>测试接收人 ID</th>
                                  <th style="width:88px;">操作</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr v-for="receiveId in monthlyReportTestReceiveIds" :key="'monthly-change-test-receiver-' + receiveId">
                                  <td style="word-break:break-all;">{{ receiveId }}</td>
                                  <td>
                                    <button class="btn btn-danger" type="button" @click="removeMonthlyReportTestReceiveId(receiveId)">
                                      删除
                                    </button>
                                  </td>
                                </tr>
                              </tbody>
                            </table>
                            <div class="hint" v-else style="margin-top:10px;">暂未添加测试接收人 ID。</div>
                            <div class="hint">测试发送会复用一份已生成月报，同时向当前已添加的全部接收人发送同一个文件。</div>
                            <div class="btn-line" style="flex-wrap:wrap;margin-top:10px;">
                              <button
                                class="btn btn-primary"
                                :disabled="!canRun || !monthlyChangeReportSendReadyCount || isActionLocked(monthlyChangeReportSendAllActionKey)"
                                @click="sendMonthlyReport('change', 'all')"
                              >
                                {{ isActionLocked(monthlyChangeReportSendAllActionKey) ? '提交中...' : '一键全部发送' }}
                              </button>
                              <button
                                class="btn btn-secondary"
                                :disabled="!canRun || !(monthlyChangeReportLastRun.generated_files || 0) || !monthlyReportTestReceiveCount || isActionLocked(monthlyChangeReportSendTestActionKey)"
                                @click="sendMonthlyReportTest('change')"
                              >
                                {{ isActionLocked(monthlyChangeReportSendTestActionKey) ? '提交中...' : ('测试发送（' + (monthlyReportTestReceiveCount || 0) + '人）') }}
                              </button>
                              <button
                                v-for="row in monthlyChangeReportRecipientStatusByBuilding"
                                :key="'monthly-change-send-' + row.building"
                                class="btn btn-secondary"
                                :title="row.detailText"
                                :disabled="!canRun || !row.sendReady || isActionLocked(getMonthlyReportSendBuildingActionKey('change', row.building))"
                                @click="sendMonthlyReport('change', 'building', row.building)"
                              >
                                {{ isActionLocked(getMonthlyReportSendBuildingActionKey('change', row.building)) ? '提交中...' : row.building }}
                              </button>
                            </div>
                            <table class="site-table" style="margin-top:12px;">
                              <thead>
                                <tr>
                                  <th style="width:72px;">楼栋</th>
                                  <th style="width:88px;">状态</th>
                                  <th style="width:120px;">主管</th>
                                  <th style="width:140px;">职位</th>
                                  <th style="width:110px;">ID 类型</th>
                                  <th>身份 ID</th>
                                  <th style="width:180px;">文件</th>
                                  <th>说明</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr
                                  v-for="row in monthlyChangeReportRecipientStatusByBuilding"
                                  :key="'monthly-change-recipient-' + row.building"
                                >
                                  <td>{{ row.building }}</td>
                                  <td>
                                    <span class="status-badge status-badge-soft" :class="'tone-' + row.tone">{{ row.statusText }}</span>
                                  </td>
                                  <td>{{ row.supervisor || '-' }}</td>
                                  <td>{{ row.position || '-' }}</td>
                                  <td>{{ row.receiveIdType || '-' }}</td>
                                  <td style="word-break:break-all;">{{ row.recipientId || '-' }}</td>
                                  <td style="word-break:break-all;">{{ row.fileName || '-' }}</td>
                                  <td>{{ row.detailText }}</td>
                                </tr>
                              </tbody>
                            </table>
                          </article>
                            </div>
                          </details>

                          <details class="module-advanced-section">
                            <summary>查看变更月报最近结果</summary>
                            <div class="module-advanced-section-body">
                          <article class="task-block">
                            <div class="task-block-head">
                              <div>
                                <div class="task-block-kicker">最近结果卡</div>
                                <h3 class="card-title">最近一次变更体系月度统计表</h3>
                              </div>
                              <span class="status-badge status-badge-soft" :class="'tone-' + (monthlyChangeReportLastRun.status === 'ok' ? 'success' : monthlyChangeReportLastRun.status === 'partial_failed' ? 'warning' : monthlyChangeReportLastRun.status === 'failed' ? 'danger' : 'neutral')">
                                {{ monthlyChangeReportLastRun.status || '尚未执行' }}
                              </span>
                            </div>
                            <div class="status-metric-grid status-metric-grid-compact">
                              <div class="status-metric">
                                <div class="status-metric-label">目标月份</div>
                                <strong class="status-metric-value">{{ monthlyChangeReportLastRun.target_month || '-' }}</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">生成文件数</div>
                                <strong class="status-metric-value">{{ monthlyChangeReportLastRun.generated_files || 0 }}</strong>
                              </div>
                              <div class="status-metric">
                                <div class="status-metric-label">最近发送</div>
                                <strong class="status-metric-value">{{ monthlyChangeReportDeliveryLastRun.finished_at || monthlyChangeReportDeliveryLastRun.started_at || '-' }}</strong>
                              </div>
                            </div>
                            <div class="hint">最近运行：{{ monthlyChangeReportLastRun.finished_at || monthlyChangeReportLastRun.started_at || '-' }}</div>
                            <div class="hint">目标月份：{{ monthlyChangeReportLastRun.target_month || '-' }}</div>
                            <div class="hint">生成文件数：{{ monthlyChangeReportLastRun.generated_files || 0 }}</div>
                            <div class="hint">成功楼栋：{{ (monthlyChangeReportLastRun.successful_buildings || []).join('、') || '-' }}</div>
                            <div class="hint">失败楼栋：{{ (monthlyChangeReportLastRun.failed_buildings || []).join('、') || '-' }}</div>
                            <div class="hint">输出目录：{{ monthlyChangeReportOutputDir }}</div>
                            <div class="hint" v-if="monthlyChangeReportLastRun.error">错误：{{ monthlyChangeReportLastRun.error }}</div>
                            <div class="hr" style="margin:10px 0;"></div>
                            <div class="hint">最近发送：{{ monthlyChangeReportDeliveryLastRun.finished_at || monthlyChangeReportDeliveryLastRun.started_at || '-' }}</div>
                            <div class="hint">发送目标月份：{{ monthlyChangeReportDeliveryLastRun.target_month || '-' }}</div>
                            <div class="hint">发送成功楼栋：{{ (monthlyChangeReportDeliveryLastRun.successful_buildings || []).join('、') || '-' }}</div>
                            <div class="hint">发送失败楼栋：{{ (monthlyChangeReportDeliveryLastRun.failed_buildings || []).join('、') || '-' }}</div>
                            <div class="hint" v-if="monthlyChangeReportDeliveryLastRun.test_mode">最近发送类型：测试发送（多接收人）</div>
                            <div class="hint" v-if="(monthlyChangeReportDeliveryLastRun.test_receive_ids || []).length">
                              测试接收人：{{ (monthlyChangeReportDeliveryLastRun.test_receive_ids || []).join('、') }} / {{ monthlyChangeReportDeliveryLastRun.test_receive_id_type || '-' }}
                            </div>
                            <div class="hint" v-if="(monthlyChangeReportDeliveryLastRun.test_successful_receivers || []).length">
                              测试发送成功：{{ (monthlyChangeReportDeliveryLastRun.test_successful_receivers || []).join('、') }}
                            </div>
                            <div class="hint" v-if="(monthlyChangeReportDeliveryLastRun.test_failed_receivers || []).length">
                              测试发送失败：{{ (monthlyChangeReportDeliveryLastRun.test_failed_receivers || []).join('、') }}
                            </div>
                            <div class="hint" v-if="monthlyChangeReportDeliveryLastRun.test_file_name">测试发送文件：{{ monthlyChangeReportDeliveryLastRun.test_file_building || '-' }} / {{ monthlyChangeReportDeliveryLastRun.test_file_name }}</div>
                            <div class="hint" v-if="monthlyChangeReportDeliveryLastRun.error">发送错误：{{ monthlyChangeReportDeliveryLastRun.error }}</div>
                          </article>
                            </div>
                          </details>
                        </div>
                      </section>

`;
