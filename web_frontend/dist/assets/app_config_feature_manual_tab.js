export const CONFIG_FEATURE_MANUAL_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_manual'">
              <div class="section-title">手动补传开关</div>
              <div class="form-row"><label><input type="checkbox" v-model="config.manual_upload_gui.enabled" /> 启用手动补传功能</label></div>
            </div>
          </div>
        
`;
