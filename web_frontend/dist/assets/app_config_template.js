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
