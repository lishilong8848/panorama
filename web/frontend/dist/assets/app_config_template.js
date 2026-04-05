import { CONFIG_MENU_TEMPLATE } from "./app_config_menu_template.js";
import { CONFIG_TABS_TEMPLATE } from "./app_config_tabs_template.js";

const CONFIG_TEMPLATE_PREFIX = `<section v-else class="config-shell">
      <section class="content-card">
        <div class="btn-line" style="justify-content:space-between; align-items:center; margin-bottom:8px;">
          <h3 class="card-title" style="margin:0;">{{ configShellTitle }}</h3>
          <div class="btn-line">
            <button class="btn btn-primary" :disabled="isActionLocked(actionKeyConfigSave)" @click="saveConfig">
              {{ isActionLocked(actionKeyConfigSave) ? '保存中...' : '保存配置' }}
            </button>
            <button class="btn btn-secondary" @click="openDashboardPage">{{ configReturnButtonText }}</button>
          </div>
        </div>
        <div class="hint" style="margin-bottom:8px;">{{ configShellDescription }}</div>
        <div class="task-block task-block-compact" style="margin-bottom:12px;">
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
          <div class="btn-line" style="margin-top:10px;" v-if="configGuidanceOverview.quickTabs && configGuidanceOverview.quickTabs.length">
            <button
              v-for="tab in configGuidanceOverview.quickTabs"
              :key="'config-guidance-tab-' + tab.id"
              class="btn btn-ghost"
              @click="switchConfigTab(tab.id)"
            >
              {{ tab.label }}
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
