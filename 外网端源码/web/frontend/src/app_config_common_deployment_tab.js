export const CONFIG_COMMON_DEPLOYMENT_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_deployment'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">部署与桥接</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">当前角色</div>
        <div class="status-metric-value">外网端</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">共享桥接</div>
        <div class="status-metric-value">{{ config.shared_bridge.enabled ? '已启用' : '未启用' }}</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">外网端只读取共享目录中的源文件、共享缓存状态和任务信息，并执行上传、审核、发送和调度。</div>
      <div class="hint">共享目录保存后会立即重新加载运行中的桥接服务，无需重启程序。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">角色模式</div>
      <div class="form-row">
        <label class="label">固定角色</label>
        <div class="readonly-inline-card">
          外网端
        </div>
        <div class="hint">当前源码目录已固定端类型，不再允许在页面切换角色。</div>
      </div>
      <div class="form-row">
        <label class="label">节点身份</label>
        <div class="readonly-inline-card readonly-inline-card-stack">
          <div>节点名称：外网端</div>
          <div>节点 ID：{{ deploymentNodeIdDisplayText }}</div>
        </div>
        <div class="hint">{{ deploymentNodeIdDisplayHint }}</div>
      </div>
    </div>

    <div class="content-card config-panel-card">
      <div class="section-title">共享目录</div>
      <div class="form-row">
        <label class="label">启用共享桥接</label>
        <input type="checkbox" v-model="config.shared_bridge.enabled" />
      </div>
      <div class="form-row">
        <label class="label">外网共享目录</label>
        <input type="text" v-model.trim="config.shared_bridge.external_root_dir" placeholder="请输入外网端可访问的共享目录路径" />
      </div>
      <div class="hint">外网端只使用外网共享目录，不保存源站账号和采集站点配置。</div>
    </div>

    <div class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">桥接调度参数</div>
      <div class="status-metric-grid-compact">
        <div class="status-metric-card compact">
          <div class="status-metric-label">轮询间隔</div>
          <div class="status-metric-value">{{ config.shared_bridge.poll_interval_sec }} 秒</div>
        </div>
        <div class="status-metric-card compact">
          <div class="status-metric-label">心跳间隔</div>
          <div class="status-metric-value">{{ config.shared_bridge.heartbeat_interval_sec }} 秒</div>
        </div>
        <div class="status-metric-card compact">
          <div class="status-metric-label">阶段租约</div>
          <div class="status-metric-value">{{ config.shared_bridge.claim_lease_sec }} 秒</div>
        </div>
      </div>
      <div class="config-form-grid two-col">
        <div class="form-row">
          <label class="label">轮询间隔（秒）</label>
          <input type="number" min="1" v-model.number="config.shared_bridge.poll_interval_sec" />
        </div>
        <div class="form-row">
          <label class="label">心跳间隔（秒）</label>
          <input type="number" min="1" v-model.number="config.shared_bridge.heartbeat_interval_sec" />
        </div>
        <div class="form-row">
          <label class="label">阶段租约（秒）</label>
          <input type="number" min="5" v-model.number="config.shared_bridge.claim_lease_sec" />
        </div>
        <div class="form-row">
          <label class="label">任务超时（秒）</label>
          <input type="number" min="60" v-model.number="config.shared_bridge.stale_task_timeout_sec" />
        </div>
        <div class="form-row">
          <label class="label">产物保留（天）</label>
          <input type="number" min="1" v-model.number="config.shared_bridge.artifact_retention_days" />
        </div>
        <div class="form-row">
          <label class="label">SQLite 忙等待（毫秒）</label>
          <input type="number" min="1000" step="1000" v-model.number="config.shared_bridge.sqlite_busy_timeout_ms" />
        </div>
      </div>
    </div>
  </div>
</div>
`;

