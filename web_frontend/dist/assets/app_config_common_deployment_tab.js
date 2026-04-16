export const CONFIG_COMMON_DEPLOYMENT_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_deployment'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">部署与桥接</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">当前角色</div>
        <div class="status-metric-value">{{ config.deployment.role_mode === 'internal' ? '内网端' : '外网端' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">共享桥接</div>
        <div class="status-metric-value">{{ config.shared_bridge.enabled ? '已启用' : '未启用' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">启用站点</div>
        <div class="status-metric-value">{{ (config.internal_source_sites || []).filter(site => site.enabled).length }}</div>
      </div>
    </div>
    <div class="hint-stack">
      <div class="hint">系统现在只保留双角色：内网端负责源文件下载与共享登记，外网端负责业务发起、续传和共享消费。</div>
      <div class="hint">外网读取共享文件时只使用共享索引登记的相对路径，再按当前角色根目录解析成实际路径。</div>
    </div>
  </div>

  <div class="config-panel-grid two-col">
    <div class="content-card config-panel-card">
      <div class="section-title">角色模式</div>
      <div class="form-row">
        <label class="label">角色模式</label>
        <select v-model="config.deployment.role_mode">
          <option value="internal">内网端</option>
          <option value="external">外网端</option>
        </select>
      </div>
      <div class="form-row">
        <label class="label">节点身份</label>
        <div class="readonly-inline-card readonly-inline-card-stack">
          <div>节点名称：{{ config.deployment.role_mode === 'internal' ? '内网端' : '外网端' }}</div>
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
        <label class="label">内网共享目录</label>
        <input type="text" v-model.trim="config.shared_bridge.internal_root_dir" placeholder="请输入内网端可访问的共享目录路径" />
      </div>
      <div class="form-row">
        <label class="label">外网共享目录</label>
        <input type="text" v-model.trim="config.shared_bridge.external_root_dir" placeholder="请输入外网端可访问的共享目录路径" />
      </div>
      <div class="hint">内网端使用本地目录，外网端使用 UNC 路径。</div>
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

    <div v-if="isInternalDeploymentRole" class="content-card config-panel-card config-panel-card-wide">
      <div class="section-title">内网下载站点</div>
      <div class="hint-stack">
        <div class="hint">这是一套共用的 5 楼配置。交接班日志源文件、交接班容量报表源文件、月报源文件和告警源文件都复用同一组地址、账号和密码。</div>
        <div class="hint">密码按当前要求明文显示。内网端会按固定节奏刷新共享源文件。</div>
      </div>
      <div class="config-grid-table-wrap">
        <table class="config-grid-table">
          <thead>
            <tr>
              <th>楼栋</th>
              <th>启用</th>
              <th>IP / 主机地址</th>
              <th>账号</th>
              <th>密码</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="site in config.internal_source_sites" :key="'internal-source-site-' + site.building">
              <td>{{ site.building }}</td>
              <td><input type="checkbox" v-model="site.enabled" /></td>
              <td><input type="text" v-model.trim="site.host" placeholder="例如 192.168.1.10" /></td>
              <td><input type="text" v-model.trim="site.username" placeholder="账号" /></td>
              <td><input type="text" v-model="site.password" placeholder="密码" /></td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</div>
`;
