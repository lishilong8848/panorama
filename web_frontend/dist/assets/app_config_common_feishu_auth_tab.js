export const CONFIG_COMMON_FEISHU_AUTH_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_feishu_auth'">
              <div class="section-title">飞书鉴权</div>
              <div class="form-row"><label class="label">飞书应用编号</label><input type="text" v-model="config.feishu.app_id" /></div>
              <div class="form-row"><label class="label">飞书应用密钥</label><input type="text" v-model="config.feishu.app_secret" /></div>
              <div class="form-row"><label class="label">鉴权重试次数</label><input type="number" v-model.number="config.feishu.request_retry_count" /></div>
              <div class="form-row"><label class="label">鉴权重试间隔（秒））</label><input type="number" v-model.number="config.feishu.request_retry_interval_sec" /></div>
            </div>

            
`;
