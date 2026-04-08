export const CONFIG_COMMON_PATHS_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_paths' && showCommonPathsConfigTab" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">路径与目录</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">业务根目录</div>
        <div class="status-metric-value monospace">{{ config.download.save_dir || '未设置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">楼栋数量</div>
        <div class="status-metric-value">{{ (config.download.buildings || []).length }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">当前角色</div>
        <div class="status-metric-value">{{ config.deployment.role_mode === 'internal' ? '内网端' : (config.deployment.role_mode === 'external' ? '外网端' : '未设置') }}</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">业务根目录会派生月报下载目录、交接班日志输出目录和部分本地缓存目录。</div>
      <div class="hint">交接班共享源文件目录仅用于内网下载缓存和内部复用；外网主链直接读取共享文件夹。</div>
      <div class="hint">运行时状态目录固定为程序目录下 .runtime。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">根目录设置</div>
      <div class="form-row">
        <label class="label">业务根目录</label>
        <input type="text" v-model="config.download.save_dir" />
      </div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">交接班模板</div>
      <div class="form-row">
        <label class="label">交接班模板文件</label>
        <input type="text" v-model="config.handover_log.template.source_path" />
      </div>
      <div class="hint">用于交接班日志与后续容量报表链路的模板读取起点。</div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">楼栋范围</div>
      <div class="form-row">
        <label class="label">楼栋列表（逗号或空格分隔）</label>
        <input type="text" v-model="buildingsText" placeholder="例如 A楼 B楼 C楼 D楼 E楼" />
      </div>
      <div class="hint">这里决定默认楼栋范围；后续业务页面可以按具体任务再做单楼选择。</div>
    </div>
  </div>
</div>
`;
