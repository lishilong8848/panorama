export const CONFIG_FEATURE_WET_BULB_COLLECTION_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_wet_bulb_collection'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">湿球温度定时采集</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">定时调度</div>
        <div class="status-metric-value">{{ health.wet_bulb_collection.scheduler.status || '未启动' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">目标多维表</div>
        <div class="status-metric-value">{{ config.wet_bulb_collection.target.table_id ? '已配置' : '未配置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">执行周期</div>
        <div class="status-metric-value">每 {{ config.wet_bulb_collection.scheduler.interval_minutes || 0 }} 分钟</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">本模块默认提供“立即运行一次”入口，这里不再单独配置手动开关。</div>
      <div class="hint">下载源、楼栋站点和共享桥接前置流程继续复用交接班模块已有配置。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">调度</div>
      <div class="form-row"><label class="label">每 N 分钟运行一次</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.scheduler.interval_minutes" @change="saveWetBulbCollectionSchedulerQuickConfig" /></div>
      <div class="form-row"><label class="label">检查间隔（秒）</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.scheduler.check_interval_sec" @change="saveWetBulbCollectionSchedulerQuickConfig" /></div>
      <div class="form-row"><label><input type="checkbox" v-model="config.wet_bulb_collection.scheduler.retry_failed_on_next_tick" @change="saveWetBulbCollectionSchedulerQuickConfig" /> 失败后下一个周期继续重试</label></div>
      <div class="form-row"><label class="label">状态文件名</label><input type="text" v-model="config.wet_bulb_collection.scheduler.state_file" @change="saveWetBulbCollectionSchedulerQuickConfig" /></div>
      <div class="hint">{{ wetBulbSchedulerQuickSaving ? '调度配置保存中...' : '修改后自动保存。' }}</div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">网络说明</div>
      <div class="hint-stack">
        <div class="hint">湿球温度流程现在统一按当前角色网络执行，不再切换网络。</div>
        <div class="hint">内网端只负责前置源数据，外网端只负责后续提取和上传。</div>
      </div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">目标多维表</div>
      <div class="config-form-grid three-col">
        <div class="form-row"><label class="label">应用 Token</label><input type="text" v-model="config.wet_bulb_collection.target.app_token" /></div>
        <div class="form-row"><label class="label">数据表 ID</label><input type="text" v-model="config.wet_bulb_collection.target.table_id" /></div>
        <div class="form-row"><label class="label">分页大小</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.target.page_size" /></div>
        <div class="form-row"><label class="label">最多读取记录数</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.target.max_records" /></div>
        <div class="form-row"><label class="label">删除批次大小</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.target.delete_batch_size" /></div>
        <div class="form-row"><label class="label">写入批次大小</label><input type="number" min="1" v-model.number="config.wet_bulb_collection.target.create_batch_size" /></div>
      </div>
      <div class="hint-stack">
        <div class="hint">只需要配置应用 Token 和数据表 ID。系统会自动识别它对应的是 Base 还是 Wiki 多维表。</div>
        <div class="hint">每次执行会先清空目标表，再写入本次采集结果；如果本次没有可上传数据，则不会清空旧表。</div>
      </div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">字段映射</div>
      <div class="hint-stack">
        <div class="hint">这里填写的是目标飞书多维表中的列名，不是源 Excel 的单元格位置。</div>
        <div class="hint">只有当目标多维表列名与默认值不一致时，才需要修改。</div>
        <div class="hint">序号字段会写入固定楼栋序号：A楼 1，B楼 2，C楼 3，D楼 4，E楼 5。</div>
      </div>
      <div class="config-form-grid two-col">
        <div class="form-row"><label class="label">日期字段</label><input type="text" v-model="config.wet_bulb_collection.fields.date" /></div>
        <div class="form-row"><label class="label">楼栋字段</label><input type="text" v-model="config.wet_bulb_collection.fields.building" /></div>
        <div class="form-row"><label class="label">天气湿球温度字段</label><input type="text" v-model="config.wet_bulb_collection.fields.wet_bulb_temp" /></div>
        <div class="form-row"><label class="label">冷源运行模式字段</label><input type="text" v-model="config.wet_bulb_collection.fields.cooling_mode" /></div>
        <div class="form-row"><label class="label">序号字段</label><input type="text" v-model="config.wet_bulb_collection.fields.sequence" /></div>
      </div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">模式映射说明</div>
      <div class="hint-stack">
        <div class="hint">本功能不直接读取源表 D7 / F7，而是复用交接班规则引擎提取“天气湿球温度”和“冷源运行模式”。</div>
        <div class="hint">上传映射：制冷 -> 机械制冷，预冷 -> 预冷模式，板换 -> 自然冷模式，停机 -> 跳过不写入。</div>
      </div>
    </div>
  </div>
</div>
`;
