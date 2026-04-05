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
            :disabled="isHandoverConfirmAllLocked || !health.handover.review_status.batch_key || !health.handover.review_status.has_any_session || health.handover.review_status.all_confirmed"
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
            @click="continueHandoverFollowupUpload(health.handover.review_status.batch_key)"
            :disabled="isHandoverFollowupContinueLocked"
          >
            {{ handoverFollowupContinueButtonText }}
          </button>
        </div>
      </section>

      <section class="status-page-grid">
        <article :class="['status-card', isInternalDeploymentRole ? 'status-card-wide' : '']">
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
              @click="runHomeQuickAction(action.id)"
            >
              {{ action.label }}
            </button>
          </div>
        </article>

        <article v-if="!isInternalDeploymentRole" class="status-card">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">系统与网络</span>
              <h2 class="status-panel-title">当前运行环境</h2>
            </div>
          </div>
          <div class="status-metric-grid">
            <div class="status-metric" v-for="item in dashboardSystemStatusItems" :key="'status-system-' + item.label">
              <div class="status-metric-label">{{ item.label }}</div>
              <div class="status-badge status-badge-solid" :class="'tone-' + item.tone">{{ item.value }}</div>
            </div>
          </div>
        </article>

        <article v-if="!isInternalDeploymentRole" class="status-card">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">更新镜像</span>
              <h2 class="status-panel-title">共享目录批准版本</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + updaterMirrorOverview.tone">
              {{ updaterMirrorOverview.statusText }}
            </span>
          </div>
          <div class="hint">{{ updaterMirrorOverview.summaryText }}</div>
          <div class="status-list">
            <div
              class="status-list-row"
              v-for="item in updaterMirrorOverview.items"
              :key="'status-updater-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="hint" v-if="updaterMirrorOverview.manifestPath">
            镜像清单：{{ updaterMirrorOverview.manifestPath }}
          </div>
          <div class="hint" v-if="updaterMirrorOverview.errorText">
            发布异常：{{ updaterMirrorOverview.errorText }}
          </div>
        </article>

        <article v-if="!isInternalDeploymentRole" class="status-card">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">调度状态</span>
              <h2 class="status-panel-title">月报与交接班调度</h2>
            </div>
          </div>
          <div class="status-list">
            <div class="status-list-row" v-for="item in dashboardScheduleStatusItems" :key="'status-schedule-' + item.label">
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
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
                @click="refreshCurrentHourSourceCache"
                :disabled="isSourceCacheRefreshCurrentHourLocked"
              >
                {{ currentHourRefreshButtonText }}
              </button>
              <button
                class="btn btn-secondary"
                type="button"
                @click="refreshManualAlarmSourceCache"
                :disabled="isSourceCacheRefreshAlarmManualLocked"
              >
                {{ manualAlarmRefreshButtonText }}
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
          <div class="hint">每 2 秒自动刷新一次，实时显示 A楼 / B楼 / C楼 / D楼 / E楼 的页面、占用和登录状态，以及三组共享文件状态。</div>
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
              class="internal-download-slot"
              v-for="slot in internalRuntimeOverview.slots"
              :key="'status-download-slot-' + slot.building"
            >
              <div class="internal-download-slot-head">
                <span class="internal-download-slot-title">{{ slot.building }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + slot.tone">{{ slot.stateText }}</span>
              </div>
              <div class="internal-download-slot-meta">
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
          <div class="hint" v-if="internalRuntimeOverview.currentHourRefresh.lastRunAt">最近触发：{{ internalRuntimeOverview.currentHourRefresh.lastRunAt }}</div>
          <div class="hint" v-if="internalRuntimeOverview.currentHourRefresh.lastSuccessAt">最近完成：{{ internalRuntimeOverview.currentHourRefresh.lastSuccessAt }}</div>
          <div class="hint" v-if="internalRuntimeOverview.currentHourRefresh.runningBuildings && internalRuntimeOverview.currentHourRefresh.runningBuildings.length">
            当前进行中：{{ internalRuntimeOverview.currentHourRefresh.runningBuildings.join(' / ') }}
          </div>
          <div class="hint" v-if="internalRuntimeOverview.currentHourRefresh.completedBuildings && internalRuntimeOverview.currentHourRefresh.completedBuildings.length">
            本轮完成：{{ internalRuntimeOverview.currentHourRefresh.completedBuildings.join(' / ') }}
          </div>
          <div class="hint" v-if="internalRuntimeOverview.currentHourRefresh.blockedBuildings && internalRuntimeOverview.currentHourRefresh.blockedBuildings.length">
            等待恢复：{{ internalRuntimeOverview.currentHourRefresh.blockedBuildings.join(' / ') }}
          </div>
          <div class="hint" v-if="internalRuntimeOverview.currentHourRefresh.failedBuildings && internalRuntimeOverview.currentHourRefresh.failedBuildings.length">
            失败项：{{ internalRuntimeOverview.currentHourRefresh.failedBuildings.join(' / ') }}
          </div>
          <div class="hint" v-if="internalRuntimeOverview.currentHourRefresh.lastError">最近错误：{{ internalRuntimeOverview.currentHourRefresh.lastError }}</div>
          <div class="status-subsection-head">
            <span class="status-panel-kicker">最新共享文件状态</span>
            <span class="status-inline-note">交接班源文件、月报源文件和告警信息源文件实时同步显示</span>
          </div>
          <div class="source-cache-family-grid" v-if="internalRuntimeOverview.families && internalRuntimeOverview.families.length">
            <div
              class="internal-download-slot"
              v-for="family in internalRuntimeOverview.families"
              :key="'status-internal-runtime-family-' + family.key"
            >
              <div class="internal-download-slot-head">
                <span class="internal-download-slot-title">{{ family.title }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + family.tone">{{ family.statusText }}</span>
              </div>
              <div class="internal-download-slot-meta">
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
                  class="internal-download-slot"
                  v-for="building in family.buildings"
                  :key="'status-internal-runtime-building-' + family.key + '-' + building.building"
                >
                  <div class="internal-download-slot-head">
                    <span class="internal-download-slot-title">{{ building.building }}</span>
                    <span class="status-badge status-badge-soft" :class="'tone-' + building.tone">{{ building.stateText }}</span>
                  </div>
                  <div class="hint">时间桶：{{ building.bucketKey || family.currentBucket }}</div>
                  <div class="hint">{{ building.detailText }}</div>
                  <div class="hint" v-if="family.key === 'alarm_event_family' && family.manualRefresh && family.manualRefresh.buildingRowCounts && family.manualRefresh.buildingRowCounts[building.building] !== undefined">
                    最近手动拉取：{{ family.manualRefresh.buildingRowCounts[building.building] }} 条
                  </div>
                  <div class="hint" v-if="building.relativePath">缓存文件：{{ building.relativePath }}</div>
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
          <div class="internal-download-pool-grid">
            <div
              class="internal-download-slot"
              v-for="slot in externalInternalAlertOverview.buildings"
              :key="'status-external-internal-alert-' + slot.building"
            >
              <div class="internal-download-slot-head">
                <span class="internal-download-slot-title">{{ slot.building }}</span>
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
          <div class="hint">本次最新时间桶：{{ sharedSourceCacheReadinessOverview.referenceBucketKey }}</div>
          <div class="source-cache-family-grid" v-if="sharedSourceCacheReadinessOverview.families && sharedSourceCacheReadinessOverview.families.length">
            <div
              class="internal-download-slot"
              v-for="family in sharedSourceCacheReadinessOverview.families"
              :key="'status-external-cache-family-' + family.key"
            >
              <div class="internal-download-slot-head">
                <span class="internal-download-slot-title">{{ family.title }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + family.tone">{{ family.statusText }}</span>
              </div>
              <div class="internal-download-slot-meta">
                <span class="status-inline-note" v-if="family.key === 'alarm_event_family'">选择策略：当天最新一份，缺失则回退昨天最新</span>
                <span class="status-inline-note" v-else>最新时间桶：{{ family.bestBucketKey || sharedSourceCacheReadinessOverview.referenceBucketKey }}</span>
                <span class="status-inline-note" v-if="family.key === 'alarm_event_family' && family.selectionReferenceDate">参考日期：{{ family.selectionReferenceDate }}</span>
                <span class="status-inline-note" v-else-if="family.bestBucketAgeText">距当前约 {{ family.bestBucketAgeText }}</span>
                <span class="status-inline-note">{{ family.summaryText }}</span>
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.uploadLastRunAt">
                最近上传：{{ family.uploadLastRunAt }} / 记录 {{ family.uploadRecordCount || 0 }} 条 / 文件 {{ family.uploadFileCount || 0 }} 份 / 源文件保留
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.uploadRunning">
                {{ family.uploadRunningText }}
              </div>
              <div class="hint" v-if="family.key === 'alarm_event_family' && family.uploadLastError">
                最近上传异常：{{ family.uploadLastError }}
              </div>
              <div class="source-cache-building-grid" v-if="family.buildings && family.buildings.length">
                <div
                  class="internal-download-slot"
                  v-for="building in family.buildings"
                  :key="'status-external-cache-building-' + family.key + '-' + building.building"
                >
                  <div class="internal-download-slot-head">
                    <span class="internal-download-slot-title">{{ building.building }}</span>
                    <span class="status-badge status-badge-soft" :class="'tone-' + building.tone">{{ building.stateText }}</span>
                  </div>
                  <div class="hint" v-if="family.key === 'alarm_event_family'">来源：{{ building.sourceKindText || '-' }}</div>
                  <div class="hint" v-if="family.key === 'alarm_event_family'">选择：{{ building.selectionScopeText || '-' }}</div>
                  <div class="hint" v-if="family.key === 'alarm_event_family'">选中文件时间：{{ building.selectedDownloadedAt || '-' }}</div>
                  <div class="hint" v-else>时间桶：{{ building.bucketKey || family.bestBucketKey || sharedSourceCacheReadinessOverview.referenceBucketKey }}</div>
                  <div class="hint" v-if="building.usingFallback && building.versionGap !== null">较最新版本落后 {{ building.versionGap }} 桶</div>
                  <div class="hint">{{ building.lastError ? ("最近错误：" + building.lastError) : ("最近成功：" + (building.downloadedAt || "-")) }}</div>
                  <div class="hint" v-if="building.resolvedFilePath">共享路径：{{ building.resolvedFilePath }}</div>
                  <div class="hint" v-else-if="building.relativePath">缓存文件：{{ building.relativePath }}</div>
                  <div class="hint" v-else-if="building.statusKey !== 'waiting'">共享文件未登记</div>
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
            <span class="status-badge status-badge-solid" :class="bridgeTasksEnabled ? 'tone-success' : 'tone-neutral'">
              {{ bridgeTasksEnabled ? '桥接已启用' : '桥接未启用' }}
            </span>
          </div>
          <div class="status-list">
            <div class="status-list-row">
              <span class="status-list-label">当前处理中</span>
              <span class="status-badge status-badge-soft" :class="(activeBridgeTasks && activeBridgeTasks.length) ? 'tone-warning' : 'tone-neutral'">
                {{ (activeBridgeTasks && activeBridgeTasks.length) ? activeBridgeTasks.length + ' 项' : '无' }}
              </span>
            </div>
            <div class="status-list-row">
              <span class="status-list-label">最近完成</span>
              <span class="status-badge status-badge-soft" :class="(recentFinishedBridgeTasks && recentFinishedBridgeTasks.length) ? 'tone-info' : 'tone-neutral'">
                {{ (recentFinishedBridgeTasks && recentFinishedBridgeTasks.length) ? recentFinishedBridgeTasks.length + ' 项' : '无' }}
              </span>
            </div>
          </div>
          <div class="hint" v-if="currentBridgeTask">
            当前认领：{{ formatBridgeStageSummary(currentBridgeTask) }}
          </div>
          <div class="status-list" v-if="activeBridgeTasks && activeBridgeTasks.length">
            <div
              class="status-list-row"
              v-for="task in activeBridgeTasks.slice(0, 5)"
              :key="'status-internal-bridge-' + task.task_id"
            >
              <span class="status-list-label">{{ formatBridgeFeature(task.feature) }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + formatBridgeTaskTone(task.status)">
                {{ formatBridgeTaskStatus(task.status) }}
              </span>
            </div>
          </div>
          <div class="hint" v-if="activeBridgeTasks && activeBridgeTasks.length">
            等待中的任务会在当前楼栋页签释放后自动接续，不需要重新发起。
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
          <div class="status-list" v-if="internalSourceCacheHistoryOverview.recentLogs && internalSourceCacheHistoryOverview.recentLogs.length">
            <div
              class="status-list-row"
              v-for="(line, index) in internalSourceCacheHistoryOverview.recentLogs"
              :key="'status-source-cache-history-log-' + index"
            >
              <span class="status-list-label">最近记录 {{ index + 1 }}</span>
              <span class="status-inline-note">{{ line }}</span>
            </div>
          </div>
          <div class="hint" v-else>当前还没有共享文件下载历史记录。</div>
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
          <div class="hint" v-if="handoverFollowupProgress.pendingCount || handoverFollowupProgress.failedCount">
            后续上传待处理 {{ handoverFollowupProgress.pendingCount }} 项，失败 {{ handoverFollowupProgress.failedCount }} 项
          </div>
          <div class="hint" v-if="health.handover.review_base_url_effective">
            当前生效地址（{{ health.handover.review_base_url_effective_source === 'manual' ? '手工指定' : '已缓存自动诊断结果' }}）：{{ health.handover.review_base_url_effective }}
          </div>
          <div class="hint" v-else-if="health.handover.review_base_url_error">
            {{ health.handover.review_base_url_error }}
          </div>
          <div class="hint" v-else-if="health.handover.review_base_url_status === 'no_candidate'">
            未检测到可用私网 IPv4 地址
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
