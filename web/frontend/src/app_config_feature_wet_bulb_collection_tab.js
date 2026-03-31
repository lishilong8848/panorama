export const CONFIG_FEATURE_WET_BULB_COLLECTION_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_wet_bulb_collection'">
  <div class="section-title">湿球温度定时采集</div>
  <div class="hint">该模块默认提供“立即运行一次”按钮，这里不再单独配置手动开关。</div>

  <div class="section-title" style="margin-top:14px">调度</div>
  <div class="form-row"><label><input type="checkbox" v-model="config.wet_bulb_collection.scheduler.enabled" /> 启用定时调度</label></div>
  <div class="form-row"><label><input type="checkbox" v-model="config.wet_bulb_collection.scheduler.auto_start_in_gui" /> 控制台启动后自动开启</label></div>
  <div class="form-row"><label class="label">每 N 分钟运行一次</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.scheduler.interval_minutes" /></div>
  <div class="form-row"><label class="label">检查间隔（秒）</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.scheduler.check_interval_sec" /></div>
  <div class="form-row"><label><input type="checkbox" v-model="config.wet_bulb_collection.scheduler.retry_failed_on_next_tick" /> 失败后下一个周期继续重试</label></div>
  <div class="form-row"><label class="label">状态文件名</label><input type="text" v-model="config.wet_bulb_collection.scheduler.state_file" /></div>

  <div class="section-title" style="margin-top:14px">网络说明</div>
  <div class="hint">单机切网端会按固定切网流程先准备内网源数据，再按既有流程切回外网。</div>
  <div class="hint">内网端和外网端不再使用单机切网配置：内网端只负责前置源数据，外网端只负责后续提取和上传。</div>

  <div class="section-title" style="margin-top:14px">目标多维表</div>
  <div class="form-row"><label class="label">App Token</label><input type="text" v-model="config.wet_bulb_collection.target.app_token" /></div>
  <div class="form-row"><label class="label">Table ID</label><input type="text" v-model="config.wet_bulb_collection.target.table_id" /></div>
  <div class="form-row"><label class="label">分页大小</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.target.page_size" /></div>
  <div class="form-row"><label class="label">最多读取记录数</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.target.max_records" /></div>
  <div class="form-row"><label class="label">删除批次大小</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.target.delete_batch_size" /></div>
  <div class="form-row"><label class="label">写入批次大小</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.target.create_batch_size" /></div>
  <div class="hint">只需要配置 App Token 和 Table ID。系统会自动识别它对应的是 Base 还是 Wiki 多维表。</div>
  <div class="hint">每次执行会先清空目标表，再写入本次采集结果；如果本次没有可上传数据，则不会清空旧表。</div>

  <div class="section-title" style="margin-top:14px">字段映射</div>
  <div class="hint">这里填写的是目标飞书多维表中的列名，不是源 Excel 的单元格位置。</div>
  <div class="hint">只有当目标多维表的列名与默认值不一致时，才需要修改。</div>
  <div class="hint">序号字段会写入固定楼栋序号：A楼 1，B楼 2，C楼 3，D楼 4，E楼 5。</div>
  <div class="form-row"><label class="label">日期字段</label><input type="text" v-model="config.wet_bulb_collection.fields.date" /></div>
  <div class="form-row"><label class="label">楼栋字段</label><input type="text" v-model="config.wet_bulb_collection.fields.building" /></div>
  <div class="form-row"><label class="label">天气湿球温度字段</label><input type="text" v-model="config.wet_bulb_collection.fields.wet_bulb_temp" /></div>
  <div class="form-row"><label class="label">冷源运行模式字段</label><input type="text" v-model="config.wet_bulb_collection.fields.cooling_mode" /></div>
  <div class="form-row"><label class="label">序号字段</label><input type="text" v-model="config.wet_bulb_collection.fields.sequence" /></div>

  <div class="section-title" style="margin-top:14px">模式映射说明</div>
  <div class="hint">本功能不直接读取源表 D7/F7，而是复用交接班规则引擎提取“天气湿球温度”和“冷源运行模式”。</div>
  <div class="hint">上传映射：制冷 -> 机械制冷，预冷 -> 预冷模式，板换 -> 自然冷模式，停机 -> 跳过不写入。</div>
  <div class="hint">下载源、楼栋站点和共享桥接前置流程继续复用交接班模块已有配置。</div>
</div>
`;
