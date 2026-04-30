import { STATUS_TEMPLATE } from "./app_status_template.js";
import { DASHBOARD_TEMPLATE } from "./app_dashboard_template.js";
import { CONFIG_TEMPLATE } from "./app_config_template.js";

const APP_TEMPLATE_PREFIX = `
  <div class="app-root-shell">
    <div v-if="updaterUiOverlayVisible" class="app-runtime-overlay" aria-live="polite" aria-busy="true" role="status">
      <div class="app-runtime-overlay-card">
        <div class="app-runtime-overlay-kicker">
          {{
            updaterUiOverlayKicker || (
              updaterUiOverlayStage === 'restarting'
                ? '更新重启中'
                : updaterUiOverlayStage === 'reloading'
                  ? '页面恢复中'
                  : '更新中'
            )
          }}
        </div>
        <div class="app-runtime-overlay-title">{{ updaterUiOverlayTitle || '正在更新程序' }}</div>
        <div class="app-runtime-overlay-subtitle">{{ updaterUiOverlaySubtitle || '请保持当前页面打开。' }}</div>
        <div class="app-runtime-overlay-progress" aria-hidden="true"></div>
      </div>
    </div>

    <div v-if="startupRoleLoadingVisible" class="app-runtime-overlay" aria-live="polite" aria-busy="true" role="status">
      <div class="app-runtime-overlay-card">
        <div class="app-runtime-overlay-kicker">
          {{
            startupRoleLoadingStage === 'validating'
              ? '正在校验启动参数'
              : startupRoleLoadingStage === 'saving'
                ? '正在保存配置'
                : startupRoleLoadingStage === 'restarting'
                  ? '正在切换监听并重启'
                  : startupRoleLoadingStage === 'recovering'
                    ? '服务恢复中'
                    : startupRoleLoadingStage === 'activating'
                      ? '正在加载角色页面'
                      : '正在应用角色'
          }}
        </div>
        <div class="app-runtime-overlay-title">{{ startupRoleLoadingTitle || '正在应用启动角色' }}</div>
        <div class="app-runtime-overlay-subtitle">{{ startupRoleLoadingSubtitle || '请稍候，系统正在准备对应页面。' }}</div>
        <div class="app-runtime-overlay-progress" aria-hidden="true"></div>
      </div>
    </div>

    <section
      v-if="resumeDeleteConfirmDialog.visible"
      class="danger-confirm-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="resume-delete-confirm-title"
      tabindex="-1"
      @keydown.esc="closeResumeDeleteConfirmDialog"
    >
      <div class="danger-confirm-card">
        <div class="danger-confirm-head">
          <div class="danger-confirm-mark" aria-hidden="true">!</div>
          <div>
            <div class="danger-confirm-kicker">危险操作</div>
            <h2 id="resume-delete-confirm-title" class="danger-confirm-title">
              {{ resumeDeleteConfirmDialog.title }}
            </h2>
          </div>
        </div>

        <p class="danger-confirm-summary">{{ resumeDeleteConfirmDialog.summary }}</p>

        <div class="danger-confirm-metrics" aria-label="待删除任务概览">
          <div class="danger-confirm-metric">
            <span>任务数</span>
            <strong>{{ resumeDeleteConfirmDialog.totalCount }}</strong>
          </div>
          <div class="danger-confirm-metric">
            <span>待上传项</span>
            <strong>{{ resumeDeleteConfirmDialog.totalPendingUploadCount }}</strong>
          </div>
        </div>

        <div class="danger-confirm-warning">
          {{ resumeDeleteConfirmDialog.warning }}
        </div>

        <div class="danger-confirm-list" v-if="resumeDeleteConfirmDialog.rows.length">
          <div
            class="danger-confirm-row"
            v-for="row in resumeDeleteConfirmDialog.rows"
            :key="'resume_delete_confirm_' + row.runId"
          >
            <div>
              <div class="danger-confirm-row-title">{{ row.dateText }}</div>
              <div class="danger-confirm-row-meta">run_id: {{ row.runId }}</div>
            </div>
            <div class="danger-confirm-row-count">{{ row.pendingUploadCount }} 项</div>
          </div>
          <div class="danger-confirm-more" v-if="resumeDeleteConfirmDialog.hiddenCount > 0">
            另有 {{ resumeDeleteConfirmDialog.hiddenCount }} 个任务将在本次操作中删除
          </div>
        </div>

        <div class="danger-confirm-actions">
          <button
            type="button"
            class="btn btn-ghost"
            :disabled="isActionLocked(getResumeDeleteConfirmActionKey())"
            @click="closeResumeDeleteConfirmDialog"
          >
            取消
          </button>
          <button
            type="button"
            class="btn btn-danger"
            :disabled="isActionLocked(getResumeDeleteConfirmActionKey())"
            @click="confirmResumeDeleteDialog"
          >
            {{ isActionLocked(getResumeDeleteConfirmActionKey()) ? '删除中...' : resumeDeleteConfirmDialog.confirmLabel }}
          </button>
        </div>
      </div>
    </section>

    <div class="app-shell app-shell-ops" v-if="shouldRenderAppShell">
      <div class="top-sticky-stack">
        <section class="ops-top-nav">
          <div class="ops-top-nav-main">
            <div class="ops-top-nav-brand">
              <div class="ops-top-nav-title-row">
                <h1 class="title ops-top-nav-title">{{ appShellTitle }}</h1>
              </div>
            </div>

            <div class="ops-top-nav-actions">
              <div class="page-nav ops-page-nav">
                <button :class="['btn', isStatusView ? 'btn-primary is-active' : 'btn-ghost']" @click="openStatusPage">
                  {{ statusNavLabel }}
                </button>
                <button
                  v-if="showDashboardPageNav"
                  :class="['btn', isDashboardView ? 'btn-primary is-active' : 'btn-ghost']"
                  @click="openDashboardPage"
                >
                  {{ dashboardNavLabel }}
                </button>
                <button :class="['btn', isConfigView ? 'btn-primary is-active' : 'btn-ghost']" @click="openConfigPage">
                  {{ configNavLabel }}
                </button>
              </div>
            </div>
          </div>
        </section>
      </div>

      <section v-if="message" class="global-message ops-global-message">{{ message }}</section>
      <section
        v-if="initialLoadingPhase !== 'ready' && initialLoadingStatusText"
        class="global-message ops-global-message global-message-info"
      >
        {{ initialLoadingStatusText }}
      </section>
`;

export const APP_TEMPLATE = `${APP_TEMPLATE_PREFIX}

${STATUS_TEMPLATE}

${DASHBOARD_TEMPLATE}

${CONFIG_TEMPLATE}
</div>
</div>
`;
