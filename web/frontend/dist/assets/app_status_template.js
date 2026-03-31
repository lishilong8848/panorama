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
              <span class="status-panel-kicker">内网下载页池</span>
              <h2 class="status-panel-title">5 个楼栋页签常驻复用</h2>
            </div>
            <span class="status-badge status-badge-solid" :class="'tone-' + internalDownloadPoolOverview.tone">
              {{ internalDownloadPoolOverview.statusText }}
            </span>
          </div>
          <div class="hint">{{ internalDownloadPoolOverview.summaryText }}</div>
          <div class="status-list">
            <div
              class="status-list-row"
              v-for="item in internalDownloadPoolOverview.items"
              :key="'status-internal-download-pool-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="hint" v-if="internalDownloadPoolOverview.errorText">
            最近异常：{{ internalDownloadPoolOverview.errorText }}
          </div>
          <div
            v-if="internalDownloadPoolOverview.slots && internalDownloadPoolOverview.slots.length"
            class="internal-download-pool-grid"
          >
            <div
              class="internal-download-slot"
              v-for="slot in internalDownloadPoolOverview.slots"
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
                <span class="status-inline-note">最新时间桶：{{ family.bestBucketKey || sharedSourceCacheReadinessOverview.referenceBucketKey }}</span>
                <span class="status-inline-note" v-if="family.bestBucketAgeText">距当前约 {{ family.bestBucketAgeText }}</span>
                <span class="status-inline-note">{{ family.summaryText }}</span>
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
                  <div class="hint">时间桶：{{ building.bucketKey || family.bestBucketKey || sharedSourceCacheReadinessOverview.referenceBucketKey }}</div>
                  <div class="hint" v-if="building.usingFallback && building.versionGap !== null">较最新版本落后 {{ building.versionGap }} 桶</div>
                  <div class="hint">{{ building.lastError ? ("最近错误：" + building.lastError) : ("最近成功：" + (building.downloadedAt || "-")) }}</div>
                  <div class="hint" v-if="building.resolvedFilePath">共享路径：{{ building.resolvedFilePath }}</div>
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

        <article v-if="isInternalDeploymentRole" class="status-card status-card-wide">
          <div class="status-card-head">
            <div>
              <span class="status-panel-kicker">共享文件仓</span>
              <h2 class="status-panel-title">双源文件小时缓存</h2>
            </div>
            <div class="status-page-hero-actions">
              <span class="status-badge status-badge-solid" :class="'tone-' + internalSourceCacheOverview.tone">
                {{ internalSourceCacheOverview.statusText }}
              </span>
              <button
                class="btn btn-warning"
                type="button"
                @click="refreshCurrentHourSourceCache"
                :disabled="isSourceCacheRefreshCurrentHourLocked"
              >
                {{ currentHourRefreshButtonText }}
              </button>
            </div>
          </div>
          <div class="hint">{{ internalSourceCacheOverview.summaryText }}</div>
          <div class="status-list">
            <div
              class="status-list-row"
              v-for="item in internalSourceCacheOverview.items"
              :key="'status-internal-source-cache-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <div class="hint" v-if="internalSourceCacheOverview.cacheRoot">
            缓存目录：{{ internalSourceCacheOverview.cacheRoot }}
          </div>
          <div class="hint" v-if="internalSourceCacheOverview.errorText">
            最近异常：{{ internalSourceCacheOverview.errorText }}
          </div>
          <div class="status-card-inline status-card-inline-stack">
            <div class="status-card-inline-item">
              <span class="status-list-label">当前小时下载</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + currentHourRefreshOverview.tone">
                {{ currentHourRefreshOverview.statusText }}
              </span>
            </div>
            <div class="hint">{{ currentHourRefreshOverview.summaryText }}</div>
            <div class="hint" v-if="currentHourRefreshOverview.lastRunAt">最近触发：{{ currentHourRefreshOverview.lastRunAt }}</div>
            <div class="hint" v-if="currentHourRefreshOverview.lastSuccessAt">最近完成：{{ currentHourRefreshOverview.lastSuccessAt }}</div>
          <div class="hint" v-if="currentHourRefreshOverview.failedBuildings && currentHourRefreshOverview.failedBuildings.length">
              失败项：{{ currentHourRefreshOverview.failedBuildings.join(' / ') }}
            </div>
            <div class="hint" v-if="currentHourRefreshOverview.lastError">最近错误：{{ currentHourRefreshOverview.lastError }}</div>
          </div>
          <div class="source-cache-family-grid" v-if="internalSourceCacheOverview.families && internalSourceCacheOverview.families.length">
            <div
              class="internal-download-slot"
              v-for="family in internalSourceCacheOverview.families"
              :key="'status-source-family-' + family.key"
            >
              <div class="internal-download-slot-head">
                <span class="internal-download-slot-title">{{ family.title }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + family.tone">{{ family.statusText }}</span>
              </div>
              <div class="internal-download-slot-meta">
                <span class="status-inline-note">当前桶：{{ family.currentBucket || internalSourceCacheOverview.currentHourBucket }}</span>
                <span class="status-inline-note">最近成功：{{ family.lastSuccessAt || "-" }}</span>
              </div>
              <div class="hint">{{ (family.failedBuildings && family.failedBuildings.length) ? ("失败楼栋：" + family.failedBuildings.join(" / ")) : ("当前小时已就绪楼栋数：" + family.readyCount) }}</div>
              <div class="source-cache-building-grid" v-if="family.buildings && family.buildings.length">
                <div
                  class="internal-download-slot"
                  v-for="building in family.buildings"
                  :key="'status-source-cache-building-' + family.key + '-' + building.building"
                >
                  <div class="internal-download-slot-head">
                    <span class="internal-download-slot-title">{{ building.building }}</span>
                    <span class="status-badge status-badge-soft" :class="'tone-' + building.tone">{{ building.stateText }}</span>
                  </div>
                  <div class="hint">{{ building.lastError ? ("最近错误：" + building.lastError) : ("最近成功：" + (building.downloadedAt || "-")) }}</div>
                  <div class="hint" v-if="building.relativePath">缓存文件：{{ building.relativePath }}</div>
                </div>
              </div>
              <div class="hint" v-else>暂无楼栋明细</div>
            </div>
          </div>
        </article>

        <article v-if="false" class="status-card">
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
              <h2 class="status-panel-title">每小时下载状态与历史</h2>
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
          <div class="source-cache-family-grid" v-if="internalSourceCacheHistoryOverview.families && internalSourceCacheHistoryOverview.families.length">
            <div
              class="internal-download-slot"
              v-for="family in internalSourceCacheHistoryOverview.families"
              :key="'status-source-cache-history-family-' + family.key"
            >
              <div class="internal-download-slot-head">
                <span class="internal-download-slot-title">{{ family.title }}</span>
                <span class="status-badge status-badge-soft" :class="'tone-' + family.tone">{{ family.statusText }}</span>
              </div>
              <div class="internal-download-slot-meta">
                <span class="status-inline-note">当前桶：{{ family.currentBucket }}</span>
                <span class="status-inline-note">最近成功：{{ family.lastSuccessAt }}</span>
              </div>
              <div class="hint" v-if="family.failedSummary">最近失败楼栋：{{ family.failedSummary }}</div>
              <div class="hint" v-else>{{ family.readyCountText }}</div>
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
