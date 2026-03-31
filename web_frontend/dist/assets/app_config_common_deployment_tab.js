export const CONFIG_COMMON_DEPLOYMENT_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='common_deployment'">
  <div class="section-title">部署角色</div>

  <div class="form-row">
    <label class="label">角色模式</label>
    <select v-model="config.deployment.role_mode">
      <option value="internal">内网端</option>
      <option value="external">外网端</option>
    </select>
  </div>
  <div class="hint">
    系统现只保留双角色：内网端只负责内网数据表下载和共享文件登记；外网端负责业务发起、外网续传和共享文件消费。
  </div>

  <div class="form-row">
    <label class="label">节点身份</label>
    <div class="readonly-inline-card readonly-inline-card-stack">
      <div>节点名称：{{ config.deployment.role_mode === 'internal' ? '内网端' : '外网端' }}</div>
      <div>节点 ID：{{ deploymentNodeIdDisplayText }}</div>
    </div>
    <div class="hint">{{ deploymentNodeIdDisplayHint }}</div>
  </div>

  <div class="section-title">共享桥接</div>

  <div class="form-row">
    <label class="label">启用共享桥接</label>
    <input type="checkbox" v-model="config.shared_bridge.enabled" />
  </div>

  <div class="form-row">
    <label class="label">内网共享目录</label>
    <input type="text" v-model.trim="config.shared_bridge.internal_root_dir" placeholder="例如 D:\\share" />
  </div>
  <div class="form-row">
    <label class="label">外网共享目录</label>
    <input type="text" v-model.trim="config.shared_bridge.external_root_dir" placeholder="例如 \\\\172.16.1.2\\share" />
  </div>
  <div class="hint">
    内网端使用本地共享目录，外网端使用 UNC 路径。外网读取共享文件时只使用共享索引登记的相对路径，再按当前角色根目录解析成实际可用路径。
  </div>

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

  <template v-if="isInternalDeploymentRole">
    <div class="section-title">内网下载站点</div>
    <div class="hint">
      这里是一套共用的 5 楼配置。交接班日志源文件和全景平台月报源文件都使用这同一套地址、账号和密码。
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
    <div class="hint">
      密码按当前要求明文显示。内网端会固定按 A 到 E 楼、每小时两类源文件共 10 个下载单元运行。
    </div>
  </template>
</div>
`;
