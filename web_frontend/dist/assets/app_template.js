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

    <section
      v-if="startupRoleGateVisible"
      class="startup-role-gate"
      aria-live="polite"
      :aria-busy="!startupRoleGateReady || startupRoleSelectorBusy"
      role="dialog"
      aria-modal="true"
    >
      <div class="startup-role-gate-card">
          <div class="startup-role-gate-head">
            <div class="startup-role-gate-kicker">角色登录</div>
            <div class="startup-role-gate-title">选择进入角色</div>
            <div class="startup-role-gate-subtitle">
            程序启动后先选择本次使用角色。确认后将直接进入对应的内网端或外网端页面。
            </div>
          </div>

        <template v-if="startupRoleGateReady">
          <div class="startup-role-gate-meta">
            <div class="startup-role-gate-current">
              已保存角色：<strong>{{ startupRoleCurrentLabel }}</strong>
            </div>
            <div class="startup-role-gate-tip">
              只在程序刚启动时选择一次；刷新页面不会要求重复选择。共享目录需与当前角色对应。
            </div>
          </div>

          <div class="startup-role-options startup-role-options-horizontal">
            <button
              v-for="option in startupRoleOptions"
              :key="option.value"
              type="button"
              class="startup-role-option"
              :class="{ 'is-selected': startupRoleSelectorSelection === option.value }"
              :disabled="startupRoleSelectorBusy"
              @click="selectStartupRole(option.value)"
            >
              <div class="startup-role-option-head">
                <span class="startup-role-option-title">{{ option.label }}</span>
                <span
                  class="status-badge status-badge-soft"
                  :class="startupRoleSelectorSelection === option.value ? 'tone-primary' : 'tone-neutral'"
                >
                  {{ startupRoleSelectorSelection === option.value ? '当前选择' : '点击选择' }}
                </span>
              </div>
              <div class="startup-role-option-desc">{{ option.description }}</div>
            </button>
          </div>

          <section v-if="startupRoleRequiresBridgeConfig" class="startup-role-config-card">
            <div class="startup-role-config-head">
              <div class="startup-role-config-kicker">共享桥接配置</div>
              <div class="startup-role-config-title">{{ startupRoleSelectedLabel }} 运行参数</div>
              <div class="startup-role-config-subtitle">
                这里填写当前角色实际使用的共享目录。确认后会直接进入目标页面。
              </div>
            </div>

            <div
              class="startup-role-config-note"
              :class="{ 'is-warning': startupRoleBridgeValidationMessage }"
            >
              {{ startupRoleBridgeNoticeText }}
            </div>

            <div class="startup-role-config-grid startup-role-config-grid-primary">
              <div class="form-row">
                <label class="label">
                  {{ startupRoleSelectorSelection === 'internal' ? '内网共享目录' : '外网共享目录' }}
                  <span
                    class="field-help-badge"
                    :title="startupRoleSelectorSelection === 'internal'
                      ? '填写内网端本机实际使用的共享目录路径。'
                      : '填写外网端本机实际访问的共享目录路径。'"
                  >?</span>
                </label>
                <input
                  type="text"
                  v-model.trim="startupRoleBridgeDraft.root_dir"
                  placeholder="请输入当前角色可访问的共享目录路径"
                  :disabled="startupRoleSelectorBusy"
                />
              </div>
              <div class="form-row">
                <label class="label">
                  节点身份
                  <span
                    class="field-help-badge"
                    title="节点名称会自动使用角色中文名，节点 ID 会按当前机器自动生成并保持长期固定，无需手工填写。"
                  >?</span>
                </label>
                <div class="readonly-inline-card readonly-inline-card-stack">
                  <div>节点名称：{{ startupRoleSelectedLabel }}</div>
                  <div>节点 ID：{{ startupRoleNodeIdDisplayText }}</div>
                </div>
                <div class="hint">
                  {{ startupRoleNodeIdDisplayHint }}
                </div>
              </div>
            </div>

            <div class="startup-role-config-toolbar">
              <button
                class="btn btn-ghost"
                type="button"
                :disabled="startupRoleSelectorBusy"
                @click="startupRoleAdvancedVisible = !startupRoleAdvancedVisible"
              >
                {{ startupRoleAdvancedVisible ? '收起高级设置' : '展开高级设置' }}
              </button>
              <div class="startup-role-config-toolbar-hint">
                高级项通常无需修改，默认沿用当前值。
              </div>
            </div>

            <div v-if="startupRoleAdvancedVisible" class="startup-role-config-grid startup-role-config-grid-advanced">
              <div class="form-row">
                <label class="label">
                  轮询间隔（秒）
                  <span class="field-help-badge" title="当前角色多久检查一次共享任务。值越小响应越快，但访问共享目录会更频繁。">?</span>
                </label>
                <input
                  type="number"
                  min="1"
                  v-model.number="startupRoleBridgeDraft.poll_interval_sec"
                  :disabled="startupRoleSelectorBusy"
                />
              </div>
              <div class="form-row">
                <label class="label">
                  心跳间隔（秒）
                  <span class="field-help-badge" title="当前节点向共享桥接上报“我还在线”的频率，用来判断节点和 claim 是否存活。">?</span>
                </label>
                <input
                  type="number"
                  min="1"
                  v-model.number="startupRoleBridgeDraft.heartbeat_interval_sec"
                  :disabled="startupRoleSelectorBusy"
                />
              </div>
              <div class="form-row">
                <label class="label">
                  阶段租约（秒）
                  <span class="field-help-badge" title="节点 claim 某个桥接阶段后，独占处理权的有效时间。过期后才允许其他节点接管。">?</span>
                </label>
                <input
                  type="number"
                  min="5"
                  v-model.number="startupRoleBridgeDraft.claim_lease_sec"
                  :disabled="startupRoleSelectorBusy"
                />
              </div>
              <div class="form-row">
                <label class="label">
                  任务超时（秒）
                  <span class="field-help-badge" title="共享任务长时间没有推进时，会被判定为超时或陈旧任务，便于后续重试和排查。">?</span>
                </label>
                <input
                  type="number"
                  min="60"
                  v-model.number="startupRoleBridgeDraft.stale_task_timeout_sec"
                  :disabled="startupRoleSelectorBusy"
                />
              </div>
              <div class="form-row">
                <label class="label">
                  产物保留（天）
                  <span class="field-help-badge" title="共享目录里桥接产物的保留时间。过短会影响排查和续传，过长会占更多磁盘。">?</span>
                </label>
                <input
                  type="number"
                  min="1"
                  v-model.number="startupRoleBridgeDraft.artifact_retention_days"
                  :disabled="startupRoleSelectorBusy"
                />
              </div>
              <div class="form-row">
                <label class="label">
                  SQLite 忙等待（毫秒）
                  <span class="field-help-badge" title="共享 bridge.db 正在被另一台机器占用时，本机最多等待多久再报忙。">?</span>
                </label>
                <input
                  type="number"
                  min="1000"
                  step="1000"
                  v-model.number="startupRoleBridgeDraft.sqlite_busy_timeout_ms"
                  :disabled="startupRoleSelectorBusy"
                />
              </div>
            </div>
          </section>

          <div v-if="startupRoleSelectorMessage" class="global-message ops-global-message startup-role-inline-message">
            {{ startupRoleSelectorMessage }}
          </div>

          <div class="btn-line startup-role-card-actions">
            <button
              class="btn btn-primary"
              type="button"
              :disabled="startupRoleConfirmDisabled"
              @click="confirmStartupRoleSelection"
            >
              {{ startupRoleActionButtonText }}
            </button>
          </div>
        </template>

        <template v-else>
          <div class="startup-role-loading">
            <div class="startup-role-loading-title">正在准备启动信息</div>
            <div class="startup-role-loading-subtitle">
              {{ initialLoadingStatusText || '正在读取配置、运行状态与角色信息，请稍候。' }}
            </div>
            <div class="app-runtime-overlay-progress" aria-hidden="true"></div>
          </div>
        </template>
      </div>
    </section>

    <div class="app-shell app-shell-ops" v-if="shouldRenderAppShell">
      <div class="top-sticky-stack">
        <section class="ops-top-nav">
          <div class="ops-top-nav-main">
            <div class="ops-top-nav-brand">
              <div class="ops-top-nav-title-row">
                <h1 class="title ops-top-nav-title">{{ appShellTitle }}</h1>
                <span class="version-inline ops-version" v-if="updaterVersionInlineText">
                  {{ updaterVersionInlineText }}
                </span>
              </div>
            </div>

            <div class="ops-top-nav-actions">
              <div class="ops-update-cluster">
                <span
                  class="status-badge status-badge-soft"
                  :class="updaterBadgeToneClass"
                >
                  {{ updaterResultText }}
                </span>
                <button
                  class="btn"
                  :class="updaterButtonClass"
                  :disabled="isUpdaterActionLocked"
                  @click="runUpdaterMainAction"
                >
                  {{ updaterMainButtonText }}
                </button>
              </div>

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
              <button
                class="btn btn-warning"
                :disabled="startupRoleSelectorBusy || startupRoleLoadingVisible || updaterUiOverlayVisible"
                @click="exitCurrentSystemToRoleSelector"
              >
                退出系统
              </button>
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
