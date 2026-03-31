export const CONFIG_COMMON_NOTIFY_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_notify'">
              <div class="section-title">告警通知</div>
              <div class="form-row"><label><input type="checkbox" v-model="config.notify.enable_webhook" /> 启用飞书机器人告警</label></div>
              <div class="form-row"><label class="label">飞书机器人回调地址</label><input type="text" v-model="config.notify.feishu_webhook_url" /></div>
              <div class="form-row"><label class="label">告警关键词</label><input type="text" v-model="config.notify.keyword" /></div>
              <div class="form-row"><label class="label">请求超时（秒）</label><input type="number" v-model.number="config.notify.timeout" /></div>
            </div>

            
`;
