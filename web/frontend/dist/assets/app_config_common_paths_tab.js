export const CONFIG_COMMON_PATHS_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_paths' && showCommonPathsConfigTab">
  <div class="section-title">路径与目录</div>

  <div class="form-row">
    <label class="label">业务根目录</label>
    <input type="text" v-model="config.download.save_dir" />
  </div>
  <div class="hint">
    该目录下会自动派生：月报下载、交接班日志输出、交接班共享源文件。运行时状态目录固定为程序目录下 .runtime。
  </div>

  <div class="section-title">交接班路径</div>
  <div class="form-row">
    <label class="label">交接班模板文件</label>
    <input type="text" v-model="config.handover_log.template.source_path" />
  </div>

  <div class="section-title">楼栋列表</div>
  <div class="form-row">
    <label class="label">楼栋列表（逗号或空格分隔）</label>
    <input type="text" v-model="buildingsText" placeholder="例如 A楼 B楼 C楼 D楼 E楼" />
  </div>
</div>
`;
