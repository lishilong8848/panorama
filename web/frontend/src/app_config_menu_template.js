export const CONFIG_MENU_TEMPLATE = `<aside class="config-menu">
            <div class="config-menu-title">公共配置</div>
            <button
              v-if="showCommonPathsConfigTab"
              :class="['btn', activeConfigTab==='common_paths' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('common_paths')"
            >路径与目录</button>
            <button
              :class="['btn', activeConfigTab==='common_deployment' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('common_deployment')"
            >部署与桥接</button>
            <button
              v-if="showCommonSchedulerConfigTab"
              :class="['btn', activeConfigTab==='common_scheduler' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('common_scheduler')"
            >调度</button>
            <button
              v-if="showNotifyConfigTab"
              :class="['btn', activeConfigTab==='common_notify' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('common_notify')"
            >告警通知</button>
            <button
              v-if="showFeishuAuthConfigTab"
              :class="['btn', activeConfigTab==='common_feishu_auth' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('common_feishu_auth')"
            >飞书鉴权</button>
            <button
              v-if="showCommonAlarmDbConfigTab"
              :class="['btn', activeConfigTab==='common_alarm_db' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('common_alarm_db')"
            >告警数据库</button>
            <button
              v-if="showConsoleConfigTab"
              :class="['btn', activeConfigTab==='common_console' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('common_console')"
            >控制台</button>

            <div
              v-if="showFeatureMonthlyConfigTab || showFeatureHandoverConfigTab || showFeatureWetBulbCollectionConfigTab || showFeatureAlarmExportConfigTab || showSheetImportConfigTab || showManualFeatureConfigTab"
              class="config-menu-title"
              style="margin-top:10px;"
            >功能配置</div>
            <button
              v-if="showFeatureMonthlyConfigTab"
              :class="['btn', activeConfigTab==='feature_monthly' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('feature_monthly')"
            >月报流程</button>
            <button
              v-if="showSheetImportConfigTab"
              :class="['btn', activeConfigTab==='feature_sheet' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('feature_sheet')"
            >5Sheet 导入</button>
            <button
              v-if="showFeatureHandoverConfigTab"
              :class="['btn', activeConfigTab==='feature_handover' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('feature_handover')"
            >交接班日志</button>
            <button
              v-if="showFeatureWetBulbCollectionConfigTab"
              :class="['btn', activeConfigTab==='feature_wet_bulb_collection' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('feature_wet_bulb_collection')"
            >湿球温度定时采集</button>
            <button
              v-if="showFeatureAlarmExportConfigTab"
              :class="['btn', activeConfigTab==='feature_alarm_export' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('feature_alarm_export')"
            >告警信息上传</button>
            <button
              v-if="showManualFeatureConfigTab"
              :class="['btn', activeConfigTab==='feature_manual' ? 'btn-primary is-active' : 'btn-ghost']"
              @click="switchConfigTab('feature_manual')"
            >手动补传开关</button>
          </aside>`;
