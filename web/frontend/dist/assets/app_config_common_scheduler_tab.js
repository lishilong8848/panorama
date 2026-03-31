export const CONFIG_COMMON_SCHEDULER_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_scheduler'">
              <div class="section-title">调度</div>
              <div class="form-row"><label><input type="checkbox" v-model="config.scheduler.enabled" /> 启用调度</label></div>
              <div class="form-row"><label><input type="checkbox" v-model="config.scheduler.auto_start_in_gui" /> 启动后自动运行调度</label></div>
              <div class="form-row"><label class="label">每日执行时间</label><input type="time" step="1" v-model="config.scheduler.run_time" /></div>
              <div class="form-row"><label class="label">检查间隔（秒）</label><input type="number" v-model.number="config.scheduler.check_interval_sec" /></div>
              <div class="form-row"><label><input type="checkbox" v-model="config.scheduler.catch_up_if_missed" /> 错过时点后补跑</label></div>
            </div>

            
`;
