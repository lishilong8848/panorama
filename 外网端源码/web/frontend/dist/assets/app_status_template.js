export const STATUS_TEMPLATE = `<section v-if="isStatusView" class="status-page">
      <section class="status-page-hero content-card">
        <div class="status-page-hero-copy">
          <div class="module-kicker">状态总览</div>
          <div class="module-title">{{ statusHeroTitle }}</div>
          <div class="module-hero-desc">{{ statusHeroDescription }}</div>
        </div>
        <div v-if="!isInternalDeploymentRole" class="status-page-hero-actions">
          <span class="status-badge status-badge-soft" :class="'tone-' + handoverReviewOverview.tone">
            {{ handoverReviewOverview.summaryText }}
          </span>
          <button
            class="btn btn-warning"
            @click="confirmAllHandoverReview"
            :disabled="isHandoverConfirmAllDisabled"
          >
            {{ handoverConfirmAllButtonText }}
          </button>
          <button
            v-if="canShowHandoverCloudRetryAll"
            class="btn btn-secondary"
            @click="retryAllFailedHandoverCloudSync"
            :disabled="isHandoverCloudRetryAllDisabled"
          >
            {{ handoverCloudRetryAllButtonText }}
          </button>
          <button
            v-if="canShowHandoverFollowupContinue"
            class="btn btn-secondary"
            @click="continueHandoverFollowupUpload(handoverFollowupBatchKey)"
            :disabled="isHandoverFollowupContinueDisabled"
          >
            {{ handoverFollowupContinueButtonText }}
          </button>
        </div>
      </section>

      <section class="status-page-grid">
        <article :class="['status-card', isInternalDeploymentRole ? 'status-card-wide' : 'status-card-featured']">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">诊断优先</span>
              <h2 class="status-panel-title">当前结论与下一步</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + statusDiagnosisOverview.tone">
              {{ statusDiagnosisOverview.statusText }}
            </span>
          </div>
          <div class="hint">{{ statusDiagnosisOverview.reasonText }}</div>
          <div class="hint" v-if="statusDiagnosisOverview.actionText">建议动作：{{ statusDiagnosisOverview.actionText }}</div>
          <div class="status-list" v-if="statusDiagnosisOverview.items && statusDiagnosisOverview.items.length">
            <div
              class="status-list-row"
              v-for="item in statusDiagnosisOverview.items"
              :key="'status-diagnosis-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="btn-line" style="margin-top:10px;" v-if="statusDiagnosisOverview.actions && statusDiagnosisOverview.actions.length">
            <button
              v-for="action in statusDiagnosisOverview.actions"
              :key="'status-diagnosis-action-' + action.id"
              class="btn"
              :class="action.id === 'refresh_current_hour' ? 'btn-warning' : action.id === 'refresh_manual_alarm' ? 'btn-secondary' : 'btn-ghost'"
              :disabled="isHomeQuickActionLocked(action)"
              :title="getHomeQuickActionDisabledReason(action)"
              @click="runHomeQuickAction(action)"
            >
              {{ getHomeQuickActionButtonText(action) || action.label }}
            </button>
          </div>
        </article>

        <article class="status-card">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">{{ dashboardSystemOverview.kicker || '系统与网络' }}</span>
              <h2 class="status-panel-title">{{ dashboardSystemOverview.title || '当前运行环境' }}</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + dashboardSystemOverview.tone">
              {{ dashboardSystemOverview.statusText }}
            </span>
          </div>
          <div class="hint" v-if="dashboardSystemOverview.summaryText">{{ dashboardSystemOverview.summaryText }}</div>
          <div class="hint" v-if="dashboardSystemOverview.detailText">{{ dashboardSystemOverview.detailText }}</div>
          <div class="status-metric-grid" v-if="dashboardSystemStatusItems && dashboardSystemStatusItems.length">
            <div class="status-metric" v-for="item in dashboardSystemStatusItems" :key="'status-system-' + item.label">
              <div class="status-metric-label">{{ item.label }}</div>
              <div class="status-badge status-badge-solid" :class="'tone-' + item.tone">{{ item.value }}</div>
            </div>
          </div>
          <div class="hint" v-else>等待后端系统概览。</div>
        </article>

        <article class="status-card">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">{{ sharedRootDiagnosticOverview.kicker || '共享目录诊断' }}</span>
              <h2 class="status-panel-title">{{ sharedRootDiagnosticOverview.title || '共享目录一致性' }}</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + sharedRootDiagnosticOverview.tone">
              {{ sharedRootDiagnosticOverview.statusText }}
            </span>
          </div>
          <div class="hint">{{ sharedRootDiagnosticOverview.summaryText }}</div>
          <div class="status-list" v-if="sharedRootDiagnosticOverview.items && sharedRootDiagnosticOverview.items.length">
            <div
              class="status-list-row"
              v-for="item in sharedRootDiagnosticOverview.items"
              :key="'status-shared-root-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
        </article>

        <article v-if="!isInternalDeploymentRole" class="status-card">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">{{ dashboardScheduleOverview.kicker || '调度状态' }}</span>
              <h2 class="status-panel-title">{{ dashboardScheduleOverview.title || '月报与交接班调度' }}</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + dashboardScheduleOverview.tone">
              {{ dashboardScheduleOverview.statusText }}
            </span>
          </div>
          <div class="hint" v-if="dashboardScheduleOverview.summaryText">{{ dashboardScheduleOverview.summaryText }}</div>
          <div class="hint" v-if="dashboardScheduleOverview.detailText">{{ dashboardScheduleOverview.detailText }}</div>
          <div class="status-list" v-if="dashboardScheduleStatusItems && dashboardScheduleStatusItems.length">
            <div class="status-list-row" v-for="item in dashboardScheduleStatusItems" :key="'status-schedule-' + item.label">
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="hint" v-else>等待后端调度概览。</div>
        </article>

        <article v-if="isInternalDeploymentRole" class="status-card status-card-wide">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">内网运行态</span>
              <h2 class="status-panel-title">5个楼实时状态与共享文件仓</h2>
            </div>
            <div class="status-page-hero-actions">
              <span class="status-badge status-badge-solid" :class="'tone-' + internalRuntimeOverview.tone">
                {{ internalRuntimeOverview.statusText }}
              </span>
              <button
                class="btn btn-warning"
                type="button"
                @click="runHomeQuickAction(getStatusQuickAction('refresh_current_hour'))"
                :disabled="isHomeQuickActionLocked(getStatusQuickAction('refresh_current_hour'))"
                :title="getHomeQuickActionDisabledReason(getStatusQuickAction('refresh_current_hour'))"
              >
                {{ getHomeQuickActionButtonText(getStatusQuickAction('refresh_current_hour')) || currentHourRefreshButtonText }}
              </button>
              <button
                class="btn btn-secondary"
                type="button"
                @click="runHomeQuickAction(getStatusQuickAction('refresh_manual_alarm'))"
                :disabled="isHomeQuickActionLocked(getStatusQuickAction('refresh_manual_alarm'))"
                :title="getHomeQuickActionDisabledReason(getStatusQuickAction('refresh_manual_alarm'))"
              >
                {{ getHomeQuickActionButtonText(getStatusQuickAction('refresh_manual_alarm')) || manualAlarmRefreshButtonText }}
              </button>
              <button
                class="btn btn-secondary"
                type="button"
                @click="deleteManualAlarmSourceCacheFiles"
                :disabled="isSourceCacheDeleteAlarmManualLocked"
              >
                {{ manualAlarmDeleteButtonText }}
              </button>
            </div>
          </div>
          <div class="hint">{{ internalRuntimeOverview.summaryText }}</div>
          <div class="hint">收到内网下载、共享桥接和楼栋浏览器相关事件时会即时刷新，并保留每 10 秒一次的兜底刷新。</div>
          <div class="status-list" v-if="internalRuntimeOverview.items && internalRuntimeOverview.items.length">
            <div
              class="status-list-row"
              v-for="item in internalRuntimeOverview.items"
              :key="'status-internal-runtime-source-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="hint" v-if="internalRuntimeOverview.cacheRoot">
            缓存目录：{{ internalRuntimeOverview.cacheRoot }}
          </div>
          <div class="hint" v-if="internalRuntimeOverview.errorText">
            最近异常：{{ internalRuntimeOverview.errorText }}
          </div>
          <div class="status-subsection-head">
            <span class="status-panel-kicker">5个楼实时状态</span>
            <span class="status-inline-note">{{ internalRuntimeOverview.poolStatusText }}</span>
          </div>
          <div class="hint">{{ internalRuntimeOverview.poolSummaryText }}</div>
          <div class="status-list" v-if="internalRuntimeOverview.poolItems && internalRuntimeOverview.poolItems.length">
            <div
              class="status-list-row"
              v-for="item in internalRuntimeOverview.poolItems"
              :key="'status-internal-runtime-pool-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="hint" v-if="internalRuntimeOverview.poolErrorText">
            最近异常：{{ internalRuntimeOverview.poolErrorText }}
          </div>
          <div class="internal-download-pool-grid">
            <div
              class="internal-runtime-slot"
              v-for="slot in internalRuntimeOverview.slots"
              :key="'status-download-slot-' + slot.building"
            >
              <div class="internal-runtime-slot-head">
                <span class="internal-runtime-slot-title">{{ slot.building }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + slot.tone">{{ slot.stateText }}</span>
              </div>
              <div class="internal-runtime-slot-meta">
                <span class="status-inline-note">页签：{{ slot.pageReady ? "已建页" : "未建页" }}</span>
                <span class="status-inline-note">占用：{{ slot.inUse ? "是" : "否" }}</span>
                <span class="status-inline-note">登录：<span class="status-badge status-badge-soft" :class="'tone-' + slot.loginTone">{{ slot.loginText }}</span></span>
              </div>
              <div class="hint" v-if="slot.lastLoginAt">最近登录：{{ slot.lastLoginAt }}</div>
              <div class="hint">{{ slot.detailText }}</div>
            </div>
          </div>
          <div class="status-subsection-head">
            <span class="status-panel-kicker">当前共享文件执行状态</span>
            <span class="status-badge status-badge-soft" :class="'tone-' + internalRuntimeOverview.currentHourRefresh.tone">
              {{ internalRuntimeOverview.currentHourRefresh.statusText }}
            </span>
          </div>
          <div class="hint">{{ internalRuntimeOverview.currentHourRefresh.summaryText }}</div>
          <div class="hint" v-if="internalRuntimeOverview.currentHourRefresh.detailText">
            {{ internalRuntimeOverview.currentHourRefresh.detailText }}
          </div>
          <div
            class="status-list"
            v-if="internalRuntimeOverview.currentHourRefresh.items && internalRuntimeOverview.currentHourRefresh.items.length"
          >
            <div
              class="status-list-row"
              v-for="item in internalRuntimeOverview.currentHourRefresh.items"
              :key="'status-internal-current-hour-item-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + (item.tone || 'neutral')">{{ item.value }}</span>
            </div>
          </div>
          <div class="hint" v-else>等待后端当前小时摘要。</div>
          <div class="status-subsection-head">
            <span class="status-panel-kicker">最新共享文件状态</span>
            <span class="status-inline-note">交接班源文件、交接班容量报表源文件、月报源文件、支路功率源文件和告警信息源文件实时同步显示</span>
          </div>
          <div class="source-cache-family-grid" v-if="internalRuntimeOverview.families && internalRuntimeOverview.families.length">
            <div
              class="source-cache-family-card"
              v-for="family in internalRuntimeOverview.families"
              :key="'status-internal-runtime-family-' + family.key"
            >
              <div class="source-cache-family-card-head">
                <span class="source-cache-family-card-title">{{ family.title }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + family.tone">{{ family.statusText }}</span>
              </div>
              <div class="source-cache-family-card-meta">
                <span class="status-inline-note">当前桶：{{ family.currentBucket }}</span>
                <span class="status-inline-note">最近成功：{{ family.lastSuccessAt || "-" }}</span>
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.manualRefresh && family.manualRefresh.running">
                最近一次手动拉取进行中：{{ family.manualRefresh.bucketKey || '-' }}
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.manualRefresh && family.manualRefresh.lastSuccessAt">
                最近一次手动拉取：{{ family.manualRefresh.lastSuccessAt }} / 桶 {{ family.manualRefresh.bucketKey || '-' }} / 总计 {{ family.manualRefresh.totalRowCount || 0 }} 条
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.manualRefresh && family.manualRefresh.queryStart">
                查询时间窗：{{ family.manualRefresh.queryStart }} ~ {{ family.manualRefresh.queryEnd || '-' }}
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.manualRefresh && family.manualRefresh.failedBuildings && family.manualRefresh.failedBuildings.length">
                手动拉取失败楼栋：{{ family.manualRefresh.failedBuildings.join(' / ') }}
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.manualRefresh && family.manualRefresh.blockedBuildings && family.manualRefresh.blockedBuildings.length">
                手动拉取等待恢复：{{ family.manualRefresh.blockedBuildings.join(' / ') }}
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.manualRefresh && family.manualRefresh.lastError">
                最近手动拉取错误：{{ family.manualRefresh.lastError }}
              </div>
              <div class="source-cache-building-grid">
                <div
                  class="source-cache-building-card"
                  v-for="building in family.buildings"
                  :key="'status-internal-runtime-building-' + family.key + '-' + building.building"
                >
                  <div class="source-cache-building-card-head">
                    <span class="source-cache-building-card-title">{{ building.building }}</span>
                    <span class="status-badge status-badge-soft" :class="'tone-' + building.tone">{{ building.stateText }}</span>
                  </div>
                  <div class="hint">时间桶：{{ building.bucketKey || family.currentBucket }}</div>
                  <div class="hint">{{ building.detailText }}</div>
                  <div class="hint" v-if="family.key === 'alarm_event_family' && family.manualRefresh && family.manualRefresh.buildingRowCounts && family.manualRefresh.buildingRowCounts[building.building] !== undefined">
                    最近手动拉取：{{ family.manualRefresh.buildingRowCounts[building.building] }} 条
                  </div>
                  <div class="hint" v-if="building.relativePath">缓存文件：{{ building.relativePath }}</div>
                  <div class="hint" v-if="getInternalSourceCacheRefreshDisabledReason(family, building)">
                    {{ getInternalSourceCacheRefreshDisabledReason(family, building) }}
                  </div>
                  <div class="source-cache-building-actions">
                    <button
                      class="btn btn-ghost source-cache-building-btn"
                      type="button"
                      @click="refreshBuildingLatestSourceCache(family.key || building.sourceFamily || building.source_family, building.building)"
                      :disabled="isInternalSourceCacheRefreshLocked(family, building)"
                      :title="getInternalSourceCacheRefreshDisabledReason(family, building) || ''"
                    >
                      {{ getInternalSourceCacheRefreshButtonText(family, building) }}
                    </button>
                  </div>
                </div>
              </div>
              <div class="hint" v-if="family.failedBuildings && family.failedBuildings.length">
                失败楼栋：{{ family.failedBuildings.join(' / ') }}
              </div>
              <div class="hint" v-if="family.blockedBuildings && family.blockedBuildings.length">
                等待恢复：{{ family.blockedBuildings.join(' / ') }}
              </div>
              <div class="hint" v-if="!family.failedBuildings.length && !family.blockedBuildings.length">
                已就绪楼栋数：{{ family.readyCount }}
              </div>
            </div>
          </div>
        </article>

        <article v-if="isExternalDeploymentRole" class="status-card status-card-wide">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">内网环境告警</span>
              <h2 class="status-panel-title">5个楼浏览器状态</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + externalInternalAlertOverview.tone">
              {{ externalInternalAlertOverview.statusText }}
            </span>
          </div>
          <div class="hint">{{ externalInternalAlertOverview.summaryText }}</div>
          <div class="status-list">
            <div
              class="status-list-row"
              v-for="item in externalInternalAlertOverview.items"
              :key="'status-external-alert-overview-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="status-building-grid">
            <div
              class="status-building-card"
              v-for="slot in externalInternalAlertOverview.buildings"
              :key="'status-external-internal-alert-' + slot.building"
            >
              <div class="status-building-card-head">
                <span class="status-building-card-title">{{ slot.building }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + slot.tone">{{ slot.statusText }}</span>
              </div>
              <div class="hint">{{ slot.summaryText }}</div>
              <div class="hint" v-if="slot.detailText">{{ slot.detailText }}</div>
              <div class="hint" v-if="slot.timeText">{{ slot.timeText }}</div>
            </div>
          </div>
        </article>

        <article v-if="isExternalDeploymentRole" class="status-card status-card-wide">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">共享文件</span>
              <h2 class="status-panel-title">最新共享文件就绪情况</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + sharedSourceCacheReadinessOverview.tone">
              {{ sharedSourceCacheReadinessOverview.statusText }}
            </span>
          </div>
          <div class="hint">{{ sharedSourceCacheReadinessOverview.summaryText }}</div>
          <div class="hint" v-if="sharedSourceCacheReadinessOverview.displayNoteText">
            {{ sharedSourceCacheReadinessOverview.displayNoteText }}
          </div>
          <div class="hint">当前共享参考标识：{{ sharedSourceCacheReadinessOverview.referenceBucketKey }}</div>
          <div class="status-list" v-if="sharedSourceCacheReadinessOverview.items && sharedSourceCacheReadinessOverview.items.length">
            <div
              v-for="(item, idx) in sharedSourceCacheReadinessOverview.items"
              :key="'status-external-cache-overview-item-' + idx"
              class="status-list-row"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-list-value" :class="'tone-' + (item.tone || 'neutral')">{{ item.value }}</span>
            </div>
          </div>
          <div v-else class="hint">等待后端共享文件摘要。</div>
          <div class="source-cache-family-grid" v-if="sharedSourceCacheReadinessOverview.families && sharedSourceCacheReadinessOverview.families.length">
            <div
              class="source-cache-family-card"
              v-for="family in sharedSourceCacheReadinessOverview.families"
              :key="'status-external-cache-family-' + family.key"
            >
              <div class="source-cache-family-card-head">
                <span class="source-cache-family-card-title">{{ family.title }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + family.tone">{{ family.statusText }}</span>
              </div>
              <template v-if="family.metaLines && family.metaLines.length">
                <div class="hint" v-for="(line, idx) in family.metaLines" :key="'status-external-cache-family-line-' + family.key + '-' + idx">
                  {{ line }}
                </div>
              </template>
              <template v-else>
                <div class="hint">等待后端明细</div>
              </template>
              <div class="hint">{{ family.summaryText }}</div>
              <div class="status-list" v-if="family.items && family.items.length">
                <div
                  v-for="(item, idx) in family.items"
                  :key="'status-external-cache-family-item-' + family.key + '-' + idx"
                  class="status-list-row"
                >
                  <span class="status-list-label">{{ item.label }}</span>
                  <span class="status-list-value" :class="'tone-' + (item.tone || 'neutral')">{{ item.value }}</span>
                </div>
              </div>
              <div class="hint" v-if="family.backfillRunning && family.backfillText">{{ family.backfillLabel || '当前补采' }}：{{ family.backfillText }}</div>
              <div class="hint" v-if="family.backfillRunning && family.backfillScopeText">{{ family.backfillScopeLabel || '补采范围' }}：{{ family.backfillScopeText }}</div>
              <div class="source-cache-building-grid" v-if="family.buildings && family.buildings.length">
                <div
                  class="source-cache-building-card"
                  v-for="building in family.buildings"
                  :key="'status-external-cache-building-' + family.key + '-' + building.building"
                >
                  <div class="source-cache-building-card-head">
                    <span class="source-cache-building-card-title">{{ building.building }}</span>
                    <span class="status-badge status-badge-soft" :class="'tone-' + building.tone">{{ building.stateText }}</span>
                  </div>
                  <template v-if="building.metaLines && building.metaLines.length">
                    <div class="hint" v-for="(line, idx) in building.metaLines" :key="'status-external-cache-building-line-' + family.key + '-' + building.building + '-' + idx">
                      {{ line }}
                    </div>
                  </template>
                  <template v-else>
                    <div class="hint">等待后端明细</div>
                  </template>
                  <div class="hint" v-if="building.backfillRunning && building.backfillText">{{ family.backfillLabel || '当前补采' }}：{{ building.backfillText }}</div>
                  <div class="hint" v-if="building.backfillRunning && building.backfillScopeText">{{ family.backfillScopeLabel || '补采范围' }}：{{ building.backfillScopeText }}</div>
                </div>
              </div>
              <div class="hint" v-else>暂无楼栋明细</div>
            </div>
          </div>
          <div class="hint" v-if="sharedSourceCacheReadinessOverview.errorText">
            最近异常：{{ sharedSourceCacheReadinessOverview.errorText }}
          </div>
        </article>

        <article v-if="bridgeTasksEnabled" class="status-card">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">共享桥接</span>
              <h2 class="status-panel-title">当前认领与等待队列</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + (bridgeTaskPanelOverview.tone || 'neutral')">
              {{ bridgeTaskPanelOverview.statusText || (bridgeTasksEnabled ? '桥接已启用' : '桥接未启用') }}
            </span>
          </div>
          <div class="hint">{{ bridgeTaskPanelOverview.summaryText || '暂无共享桥接任务。' }}</div>
          <div class="status-list" v-if="bridgeTaskPanelOverview?.items && bridgeTaskPanelOverview.items.length">
            <div
              class="status-list-row"
              v-for="item in bridgeTaskPanelOverview.items.slice(0, 2)"
              :key="'status-bridge-overview-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + (bridgeTaskPanelOverview.tone || 'neutral')">
                {{ item.value }}
              </span>
            </div>
          </div>
          <div class="hint" v-else>等待后端共享桥接摘要。</div>
          <div class="hint" v-if="bridgeTaskPanelOverview?.focusTitle || bridgeTaskPanelOverview?.focusMeta">
            当前认领：{{ bridgeTaskPanelOverview?.focusTitle || '-' }}<span v-if="bridgeTaskPanelOverview?.focusMeta"> | {{ bridgeTaskPanelOverview.focusMeta }}</span>
          </div>
          <div class="status-list" v-if="activeBridgeTasks && activeBridgeTasks.length">
            <div
              class="status-list-row"
              v-for="task in activeBridgeTasks.slice(0, 5)"
              :key="'status-internal-bridge-' + task.task_id"
            >
              <span class="status-list-label">{{ task.display_title || '-' }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + (task.tone || 'neutral')">
                {{ task.status_text || '-' }}
              </span>
            </div>
          </div>
          <div class="hint" v-if="bridgeTaskPanelOverview.nextActionText">
            {{ bridgeTaskPanelOverview.nextActionText }}
          </div>
        </article>

        <article v-if="isInternalDeploymentRole" class="status-card">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">运行日志</span>
              <h2 class="status-panel-title">内网代理、页池与镜像</h2>
            </div>
            <button class="btn btn-ghost" type="button" @click="clearLogs">清空日志</button>
          </div>
          <div class="hint">默认只展示内网相关日志；如需精确筛选，可在日志页使用关键字过滤。</div>
          <div class="status-list" v-if="internalOpsLogs && internalOpsLogs.length">
            <div
              class="status-list-row"
              v-for="(line, index) in internalOpsLogs.slice(0, 8)"
              :key="'status-internal-log-' + index"
            >
              <span class="status-list-label">日志 {{ index + 1 }}</span>
              <span class="status-inline-note">{{ line }}</span>
            </div>
          </div>
          <div class="hint" v-else>当前还没有内网代理、下载页池或镜像更新日志。</div>
        </article>

        <article v-if="isInternalDeploymentRole" class="status-card status-card-wide">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">下载历史</span>
              <h2 class="status-panel-title">最近调度与历史</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + internalSourceCacheHistoryOverview.tone">
              {{ internalSourceCacheHistoryOverview.statusText }}
            </span>
          </div>
          <div class="hint">{{ internalSourceCacheHistoryOverview.summaryText }}</div>
          <div class="status-list">
            <div
              class="status-list-row"
              v-for="item in internalSourceCacheHistoryOverview.items"
              :key="'status-source-cache-history-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="hint" v-if="internalSourceCacheHistoryOverview.lastError">
            最近错误：{{ internalSourceCacheHistoryOverview.lastError }}
          </div>
          <div class="hint" v-if="internalSourceCacheHistoryOverview.detailText">
            {{ internalSourceCacheHistoryOverview.detailText }}
          </div>
          <div class="hint" v-if="(!internalSourceCacheHistoryOverview.items || !internalSourceCacheHistoryOverview.items.length) && !internalSourceCacheHistoryOverview.detailText">
            当前还没有后端历史摘要。
          </div>
        </article>

        <article v-if="!isInternalDeploymentRole" class="status-card status-card-wide">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">交接确认</span>
              <h2 class="status-panel-title">交接确认与审核入口</h2>
            </div>
            <span class="status-inline-note" v-if="handoverReviewOverview.batchKey">
              批次 {{ handoverReviewOverview.batchKey }}
            </span>
          </div>
          <div class="status-panel-summary">
            <span class="status-badge status-badge-solid" :class="'tone-' + handoverReviewOverview.tone">
              {{ handoverReviewOverview.summaryText }}
            </span>
            <span class="status-inline-note">
              已确认 {{ handoverReviewOverview.confirmed }} / {{ handoverReviewOverview.required }}
            </span>
          </div>
          <div class="hint" v-if="handoverReviewOverview.dutyText">
            本次上传云文档批次：{{ handoverReviewOverview.dutyText }}
          </div>
          <div class="hint" v-if="handoverFollowupProgress.statusText && handoverFollowupProgress.summaryText !== '已清空'">
            {{ handoverFollowupProgress.statusText }}：{{ handoverFollowupProgress.summaryText }}
          </div>
          <div class="status-metric-grid status-metric-grid-compact">
            <div class="status-metric">
              <div class="status-metric-label">已确认</div>
              <strong class="status-metric-value">{{ handoverReviewOverview.confirmed }}</strong>
            </div>
            <div class="status-metric">
              <div class="status-metric-label">待确认</div>
              <strong class="status-metric-value">{{ handoverReviewOverview.pending }}</strong>
            </div>
            <div class="status-metric">
              <div class="status-metric-label">后续上传</div>
              <strong class="status-metric-value">{{ handoverFollowupProgress.summaryText || '已清空' }}</strong>
            </div>
          </div>
          <div class="hint" v-if="health.handover.review_base_url_effective">
            当前生效地址（手工指定）：{{ health.handover.review_base_url_effective }}
          </div>
          <div class="hint" v-else-if="health.handover.review_base_url_error">
            {{ health.handover.review_base_url_error }}
          </div>
          <div class="hint" v-else-if="health.handover.review_base_url_status === 'manual_only'">
            请先在配置中心手工填写审核页访问基地址
          </div>
          <div class="review-matrix review-matrix-detailed" v-if="handoverReviewBoardRows && handoverReviewBoardRows.length">
            <div
              class="review-matrix-item"
              v-for="row in handoverReviewBoardRows"
              :key="'status-board-' + row.building"
              :class="'tone-' + row.tone"
            >
              <div class="review-matrix-head">
                <span class="review-matrix-building">{{ row.building }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + row.tone">{{ row.text }}</span>
              </div>
              <div class="hint" style="margin:4px 0;">
                云表同步：
                <span class="status-badge status-badge-soft" :class="'tone-' + row.cloudSheetSyncTone">{{ row.cloudSheetSyncText }}</span>
              </div>
              <a
                v-if="row.hasUrl"
                class="review-matrix-link"
                :href="row.url"
                target="_blank"
                rel="noopener noreferrer"
              >
                打开审核页
              </a>
              <a
                v-if="row.hasCloudSheetUrl"
                class="review-matrix-link"
                :href="row.cloudSheetUrl"
                target="_blank"
                rel="noopener noreferrer"
              >
                打开云文档
              </a>
              <div class="hint" v-if="row.cloudSheetError">{{ row.cloudSheetError }}</div>
              <a
                v-if="row.hasUrl"
                class="handover-access-url"
                :href="row.url"
                target="_blank"
                rel="noopener noreferrer"
              >{{ row.url }}</a>
              <div class="handover-access-empty" v-else>当前没有可用的审核访问地址</div>
            </div>
          </div>
          <div class="handover-access-empty" v-else>当前没有可用的审核访问地址。</div>
        </article>
      </section>
    </section>`;
