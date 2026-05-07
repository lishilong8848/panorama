export const DASHBOARD_WET_BULB_COLLECTION_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'wet_bulb_collection'">
          <div class="dashboard-module-shell">
          <article class="task-block dashboard-module-scheduler-card">
            <div class="task-block-head">
              <div>
                <div class="task-block-kicker">调度卡</div>
                <h3 class="card-title">湿球温度定时采集调度</h3>
              </div>
              <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('wet_bulb')">
                {{ getSchedulerStatusText('wet_bulb') || '-' }}
              </span>
            </div>
            <div class="status-metric-grid status-metric-grid-compact">
              <div class="status-metric">
                <div class="status-metric-label">运行间隔</div>
                <strong class="status-metric-value">{{ config.wet_bulb_collection.scheduler.interval_minutes || '-' }} 分钟</strong>
              </div>
              <div class="status-metric">
                <div class="status-metric-label">下次执行</div>
                <strong class="status-metric-value">{{ getSchedulerDisplayText('wet_bulb', 'next_run_text', '-') }}</strong>
              </div>
              <div class="status-metric">
                <div class="status-metric-label">最近结果</div>
                <strong class="status-metric-value">{{ wetBulbSchedulerTriggerText || '-' }}</strong>
              </div>
            </div>
            <div class="task-grid two-col">
              <div class="form-row">
                <label class="label">每 N 分钟运行一次</label>
                <input type="number" min="1" v-model.number="config.wet_bulb_collection.scheduler.interval_minutes" @change="saveWetBulbCollectionSchedulerQuickConfig" />
              </div>
              <div class="form-row">
                <label class="label">检查间隔（秒）</label>
                <input type="number" min="1" v-model.number="config.wet_bulb_collection.scheduler.check_interval_sec" @change="saveWetBulbCollectionSchedulerQuickConfig" />
              </div>
            </div>
            <div class="btn-line">
              <button
                class="btn btn-success"
                :disabled="isSchedulerStartDisabled('wet_bulb', actionKeyWetBulbSchedulerStart, actionKeyWetBulbSchedulerStop)"
                @click="startWetBulbCollectionScheduler"
              >
                {{ getSchedulerStartButtonText('wet_bulb') }}
              </button>
              <button
                class="btn btn-danger"
                :disabled="isSchedulerStopDisabled('wet_bulb', actionKeyWetBulbSchedulerStart, actionKeyWetBulbSchedulerStop)"
                @click="stopWetBulbCollectionScheduler"
              >
                {{ getSchedulerStopButtonText('wet_bulb') }}
              </button>
            </div>
            <div class="hint">{{ wetBulbSchedulerQuickSaving ? '湿球温度定时采集调度配置同步中...' : '修改执行间隔后立即生效。' }}</div>
          </article>
          <div class="module-section-grid dashboard-module-primary-grid">
            <article class="task-block task-block-accent">
              <div class="task-block-head">
                <div>
                  <div class="task-block-kicker">执行入口</div>
                  <h3 class="card-title">湿球温度定时采集</h3>
                </div>
                <span class="status-badge status-badge-soft" :class="isActionLocked(actionKeyWetBulbCollectionRun) ? 'tone-info' : 'tone-success'">
                  {{ isActionLocked(actionKeyWetBulbCollectionRun) ? '执行中' : '可执行' }}
                </span>
              </div>
              <div class="status-metric-grid status-metric-grid-compact">
                <div class="status-metric">
                  <div class="status-metric-label">调度状态</div>
                  <strong class="status-metric-value">{{ getSchedulerStatusText('wet_bulb') || '-' }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">下次执行</div>
                  <strong class="status-metric-value">{{ getSchedulerDisplayText('wet_bulb', 'next_run_text', '-') }}</strong>
                </div>
                <div class="status-metric">
                  <div class="status-metric-label">目标类型</div>
                  <strong class="status-metric-value">{{ formatWetBulbTargetKind(wetBulbConfiguredTarget.targetKind) }}</strong>
                </div>
              </div>
              <div class="ops-focus-card">
                <div class="ops-focus-card-label">执行说明</div>
                <div class="ops-focus-card-title">提取天气湿球温度和冷源运行模式，按楼栋写入同一张多维表</div>
                <div class="ops-focus-card-meta">{{ bridgeExecutionHint }}</div>
              </div>
              <div class="hint">同一天同楼栋仅保留最新一条；冷源运行模式按全楼优先级归并后写入多维表。</div>
              <div class="btn-line">
                <button class="btn btn-primary" :disabled="!canRun || isActionLocked(actionKeyWetBulbCollectionRun)" @click="runWetBulbCollection">
                  {{ isActionLocked(actionKeyWetBulbCollectionRun) ? '执行中...' : '立即运行一次' }}
                </button>
              </div>
            </article>

            <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">目标配置</div>
                    <h3 class="card-title">当前目标多维表</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="wetBulbConfiguredTarget.configuredAppToken && wetBulbConfiguredTarget.tableId ? 'tone-success' : 'tone-warning'">
                    {{ wetBulbConfiguredTarget.configuredAppToken && wetBulbConfiguredTarget.tableId ? '已配置' : '待配置' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">配置 Token</div>
                    <strong class="status-metric-value">{{ wetBulbConfiguredTarget.configuredAppToken || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">Table ID</div>
                    <strong class="status-metric-value">{{ wetBulbConfiguredTarget.tableId || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">目标类型</div>
                    <strong class="status-metric-value">{{ formatWetBulbTargetKind(wetBulbConfiguredTarget.targetKind) }}</strong>
                  </div>
                </div>
                <div class="hint" v-if="wetBulbConfiguredTarget.operationAppToken && wetBulbConfiguredTarget.operationAppToken !== wetBulbConfiguredTarget.configuredAppToken">
                  实际上传 Token：{{ wetBulbConfiguredTarget.operationAppToken }}
                </div>
                <div class="hint" v-if="wetBulbConfiguredTarget.url">
                  当前配置链接：
                  <a :href="wetBulbConfiguredTarget.url" target="_blank" rel="noopener noreferrer">{{ wetBulbConfiguredTarget.url }}</a>
                </div>
                <div class="hint" v-else-if="wetBulbConfiguredTarget.message">当前配置状态：{{ wetBulbConfiguredTarget.message }}</div>
                <div class="hint" v-else>当前尚未配置湿球温度目标多维表的 App Token / Table ID。</div>
                <div class="hint" v-if="wetBulbConfiguredTarget.resolvedAt">最近解析时间：{{ wetBulbConfiguredTarget.resolvedAt }}</div>
                <div class="hint" v-if="wetBulbLatestRunTarget.url">
                  最近一次执行目标：
                  <a :href="wetBulbLatestRunTarget.url" target="_blank" rel="noopener noreferrer">{{ wetBulbLatestRunTarget.url }}</a>
                </div>
                <div class="hint" v-else-if="wetBulbLatestRunTarget.message">最近一次执行状态：{{ wetBulbLatestRunTarget.message }}</div>
              </article>
          </div>

          <div class="dashboard-module-support-stack">
              <article class="task-block task-block-compact">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">调度状态</div>
                    <h3 class="card-title">当前调度反馈</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="'tone-' + getSchedulerStatusTone('wet_bulb')">
                    {{ getSchedulerStatusText('wet_bulb') }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">最近触发</div>
                    <strong class="status-metric-value">{{ getSchedulerDisplayText('wet_bulb', 'last_trigger_text', '-') }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">触发结果</div>
                    <strong class="status-metric-value">{{ wetBulbSchedulerTriggerText || '-' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">最近决策</div>
                    <strong class="status-metric-value">{{ wetBulbSchedulerDecisionText || '-' }}</strong>
                  </div>
                </div>
              </article>
            </div>
          </div>
        </section>

`;


