export const CONFIG_FEATURE_MONTHLY_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_monthly'">
              <div class="section-title">月报流程</div>
              <div class="form-row">
                <label class="label">时间窗模式</label>
                <select v-model="config.download.time_range_mode">
                  <option value="yesterday_to_today_start">按天（昨天 00:00:00 到今天 00:00:00）</option>
                  <option value="last_month_to_this_month_start">按月（上月 1 号 00:00:00 到本月 1 号 00:00:00）</option>
                  <option value="custom">自定义时间</option>
                </select>
              </div>
              <div class="form-row" v-if="config.download.time_range_mode === 'custom'">
                <label class="label">自定义模式</label>
                <select v-model="config.download.custom_window_mode">
                  <option value="absolute">固定绝对时间段</option>
                  <option value="daily_relative">每日相对时间段</option>
                </select>
              </div>
              <div v-if="config.download.time_range_mode === 'custom' && config.download.custom_window_mode === 'absolute'">
                <div class="form-row"><label class="label">绝对开始时间</label><input type="datetime-local" step="1" v-model="customAbsoluteStartLocal" /></div>
                <div class="form-row"><label class="label">绝对结束时间</label><input type="datetime-local" step="1" v-model="customAbsoluteEndLocal" /></div>
              </div>
              <div v-if="config.download.time_range_mode === 'custom' && config.download.custom_window_mode === 'daily_relative'">
                <div class="form-row"><label class="label">每日开始时间</label><input type="text" v-model="config.download.daily_custom_window.start_time" /></div>
                <div class="form-row"><label class="label">每日结束时间</label><input type="text" v-model="config.download.daily_custom_window.end_time" /></div>
                <div class="form-row"><label><input type="checkbox" v-model="config.download.daily_custom_window.cross_day" /> 跨天区间</label></div>
              </div>
              <div class="form-row"><label class="label">最大重试次数</label><input type="number" v-model.number="config.download.max_retries" /></div>
              <div class="form-row"><label class="label">重试间隔（秒）</label><input type="number" v-model.number="config.download.retry_wait_sec" /></div>
              <div class="form-row"><label><input type="checkbox" v-model="config.download.only_process_downloaded_this_run" /> 只处理本次下载文件</label></div>
              <div class="form-row"><label class="label">浏览器通道</label><input type="text" v-model="config.download.browser_channel" /></div>

              <div class="section-title">站点配置</div>
              <table class="site-table">
                <thead><tr><th>楼栋</th><th>启用</th><th>主机地址</th><th>账号</th><th>密码</th><th>最终访问链接</th><th>操作</th></tr></thead>
                <tbody>
                  <tr v-for="(site, idx) in config.download.sites" :key="idx">
                    <td><input type="text" v-model="site.building" /></td>
                    <td><input type="checkbox" v-model="site.enabled" /></td>
                    <td><input type="text" v-model="site.host" placeholder="192.168.232.53" /></td>
                    <td><input type="text" v-model="site.username" /></td>
                    <td><input type="text" v-model="site.password" /></td>
                    <td><span class="hint">{{ previewSiteUrl(site) }}</span></td>
                    <td><button class="btn btn-danger" @click="removeSiteRow(idx)">删除</button></td>
                  </tr>
                </tbody>
              </table>
              <div class="btn-line" style="margin-top:8px;"><button class="btn btn-secondary" @click="addSiteRow">新增站点</button></div>

              <div class="section-title">飞书月报上传</div>
              <div class="form-row"><label><input type="checkbox" v-model="config.feishu.enable_upload" /> 启用月报上传</label></div>
              <div class="form-row"><label class="label">飞书多维访问凭证</label><input type="text" v-model="config.feishu.app_token" /></div>
              <div class="form-row"><label class="label">数据表编号</label><input type="text" v-model="config.feishu.calc_table_id" /></div>
              <div class="form-row"><label class="label">附件表 ID</label><input type="text" v-model="config.feishu.attachment_table_id" /></div>
            </div>
`;
