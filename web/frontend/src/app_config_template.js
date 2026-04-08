import { CONFIG_MENU_TEMPLATE } from "./app_config_menu_template.js";
import { CONFIG_TABS_TEMPLATE } from "./app_config_tabs_template.js";

const CONFIG_TEMPLATE_PREFIX = `<section v-if="isConfigView" class="config-shell">
      <section class="content-card">
        <div class="config-shell-top">
          <div class="config-shell-copy">
            <div class="task-block-kicker">配置中心</div>
            <h3 class="card-title" style="margin:0;">{{ configShellTitle }}</h3>
            <div class="hint">{{ configShellDescription }}</div>
          </div>
          <div class="btn-line">
            <button class="btn btn-primary" :disabled="isActionLocked(actionKeyConfigSave)" @click="saveConfig">
              {{ isActionLocked(actionKeyConfigSave) ? '保存中...' : '保存配置' }}
            </button>
            <button class="btn btn-secondary" @click="openDashboardPage">{{ configReturnButtonText }}</button>
          </div>
        </div>
        <div class="status-metric-grid status-metric-grid-compact" style="margin-bottom:12px;">
          <div class="status-metric">
            <div class="status-metric-label">当前状态</div>
            <strong class="status-metric-value">{{ configGuidanceOverview.statusText }}</strong>
          </div>
          <div class="status-metric">
            <div class="status-metric-label">重点分组</div>
            <strong class="status-metric-value">{{ configGuidanceOverview.quickTabs && configGuidanceOverview.quickTabs.length ? (configGuidanceOverview.quickTabs.length + ' 项') : '无' }}</strong>
          </div>
          <div class="status-metric">
            <div class="status-metric-label">返回入口</div>
            <strong class="status-metric-value">{{ configReturnButtonText }}</strong>
          </div>
        </div>
        <div class="task-block task-block-compact config-guidance-card" style="margin-bottom:12px;">
          <div class="task-block-head">
            <div>
              <div class="task-block-kicker">配置向导</div>
              <h4 class="card-title">先完成关键配置，再补高级参数</h4>
            </div>
            <span class="status-badge status-badge-soft" :class="'tone-' + configGuidanceOverview.tone">
              {{ configGuidanceOverview.statusText }}
            </span>
          </div>
          <div class="hint">{{ configGuidanceOverview.summaryText }}</div>
          <div class="hint">{{ configGuidanceOverview.restartImpactText }}</div>
          <div class="status-list" v-if="configGuidanceOverview.sections && configGuidanceOverview.sections.length" style="margin-top:10px;">
            <div
              class="status-list-row"
              v-for="item in configGuidanceOverview.sections"
              :key="'config-guidance-' + item.label"
            >
              <span class="status-list-label">{{ item.label }}</span>
              <span class="status-badge status-badge-soft" :class="'tone-' + item.tone">{{ item.value }}</span>
            </div>
          </div>
          <template v-for="item in configGuidanceOverview.sections" :key="'config-guidance-hint-' + item.label">
            <div class="hint" v-if="item.hint">{{ item.label }}：{{ item.hint }}</div>
          </template>
          <div class="config-quick-tab-grid" v-if="configGuidanceOverview.quickTabs && configGuidanceOverview.quickTabs.length">
            <button
              v-for="tab in configGuidanceOverview.quickTabs"
              :key="'config-guidance-tab-' + tab.id"
              class="btn btn-ghost ops-quick-action-card"
              @click="switchConfigTab(tab.id)"
            >
              <span class="ops-quick-action-title">{{ tab.label }}</span>
              <span class="ops-quick-action-desc">跳转到当前最需要优先检查的配置分组</span>
            </button>
          </div>
        </div>

        <div class="config-layout">`;

const CONFIG_TEMPLATE_SUFFIX = `
        <div class="hr"></div>
        <div class="btn-line">
          <button class="btn btn-primary" :disabled="isActionLocked(actionKeyConfigSave)" @click="saveConfig">
            {{ isActionLocked(actionKeyConfigSave) ? '保存中...' : '保存配置' }}
          </button>
          <button class="btn btn-secondary" @click="openDashboardPage">{{ configReturnButtonText }}</button>
        </div>
      </section>
    </section>
  </div>`;

export const CONFIG_TEMPLATE = `${CONFIG_TEMPLATE_PREFIX}
${CONFIG_MENU_TEMPLATE}

${CONFIG_TABS_TEMPLATE}
${CONFIG_TEMPLATE_SUFFIX}`;
