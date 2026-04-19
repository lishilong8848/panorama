import { CONFIG_MENU_TEMPLATE } from "./app_config_menu_template.js";
import { CONFIG_TABS_TEMPLATE } from "./app_config_tabs_template.js";

const CONFIG_TEMPLATE_PREFIX = `<section v-if="isConfigView" class="config-shell">
      <section class="content-card">
        <div class="config-shell-top">
          <div class="config-shell-copy">
            <div class="task-block-kicker">配置中心</div>
            <h3 class="card-title" style="margin:0;">{{ configShellTitle }}</h3>
            <div class="hint">{{ configShellDescription }}</div>
            <div class="hint" v-if="activeConfigTab === 'feature_handover'">当前页公共配置与当前楼栋配置需手动点击“保存配置”后才会生效。</div>
            <div class="hint" v-if="configSaveStateText">
              {{ configSaveStateText }}<template v-if="configSaveStateDetail"> · {{ configSaveStateDetail }}</template>
            </div>
          </div>
        </div>
        <div class="config-shell-actions">
          <div class="btn-line config-shell-actions-line">
            <button
              class="btn btn-primary"
              :disabled="isConfigSaveLocked"
              @click="saveActiveConfig"
            >
              {{ configSaveButtonText }}
            </button>
            <button class="btn btn-secondary" @click="openDashboardPage">{{ configReturnButtonText }}</button>
          </div>
        </div>

        <div class="config-layout">`;

const CONFIG_TEMPLATE_SUFFIX = `
      </section>
    </section>
  </div>`;

export const CONFIG_TEMPLATE = `${CONFIG_TEMPLATE_PREFIX}
${CONFIG_MENU_TEMPLATE}

${CONFIG_TABS_TEMPLATE}
${CONFIG_TEMPLATE_SUFFIX}`;
