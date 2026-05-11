const BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"];
const SOURCE_FAMILIES = [
  ["handover_log_family", "交接班日志源文件"],
  ["handover_capacity_report_family", "交接班容量报表源文件"],
  ["monthly_report_family", "全景平台月报源文件"],
  ["branch_power_family", "支路功率源文件"],
  ["branch_current_family", "支路电流源文件"],
  ["branch_switch_family", "支路开关源文件"],
  ["alarm_event_family", "告警信息源文件"],
];

const state = {
  view: location.pathname.includes("/config") ? "config" : "status",
  health: {},
  config: null,
  summary: null,
  tasks: [],
  logs: [],
  logOffset: 0,
  message: "",
  error: "",
  saving: false,
  busy: new Set(),
};

let refreshInFlight = false;
const RUNTIME_STATUS_ERROR_PREFIX = "读取内网端状态失败";

function text(value, fallback = "-") {
  const raw = value == null ? "" : String(value).trim();
  return raw || fallback;
}

function escapeHtml(value) {
  return text(value, "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function query(path, params = {}) {
  const url = new URL(path, window.location.origin);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value) !== "") {
      url.searchParams.set(key, value);
    }
  });
  return url.pathname + url.search;
}

async function api(path, options = {}) {
  const { timeoutMs: requestedTimeoutMs, ...fetchOptions } = options;
  const timeoutMs = Number(requestedTimeoutMs || 10000);
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  let response;
  try {
    response = await fetch(path, {
      headers: { "Content-Type": "application/json", ...(fetchOptions.headers || {}) },
      ...fetchOptions,
      signal: controller.signal,
    });
  } catch (error) {
    if (error && error.name === "AbortError") {
      throw new Error(`请求超时：${path}`);
    }
    throw error;
  } finally {
    window.clearTimeout(timer);
  }
  if (!response.ok) {
    let detail = "";
    try {
      const payload = await response.json();
      detail = payload.detail || payload.error || JSON.stringify(payload);
    } catch (_) {
      detail = await response.text();
    }
    throw new Error(detail || `HTTP ${response.status}`);
  }
  const contentType = response.headers.get("content-type") || "";
  return contentType.includes("application/json") ? response.json() : response.text();
}

async function fetchText(path, { timeoutMs = 5000 } = {}) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(path, { signal: controller.signal });
    if (!response.ok) return "";
    return await response.text();
  } catch (_) {
    return "";
  } finally {
    window.clearTimeout(timer);
  }
}

function setMessage(message, isError = false) {
  state.message = isError ? "" : text(message, "");
  state.error = isError ? text(message, "") : "";
  render();
}

async function loadHealth() {
  try {
    const payload = await api(`/api/health/bootstrap?_t=${Date.now()}`);
    state.health = payload || {};
  } catch (error) {
    state.health = {};
    state.error = `读取启动状态失败：${error.message}`;
  }
}

async function loadConfig() {
  try {
    state.config = await api(`/api/config?_t=${Date.now()}`);
  } catch (error) {
    state.error = `读取配置失败：${error.message}`;
  }
}

async function loadRuntimeStatus() {
  try {
    const payload = await api(`/api/bridge/internal-runtime-status?_t=${Date.now()}`, { timeoutMs: 20000 });
    state.summary = payload.summary || null;
    if (state.error.startsWith(RUNTIME_STATUS_ERROR_PREFIX)) {
      state.error = "";
    }
  } catch (error) {
    if (!state.summary) {
      state.summary = null;
      state.error = `${RUNTIME_STATUS_ERROR_PREFIX}：${error.message}`;
    }
  }
}

async function loadTasks() {
  try {
    const payload = await api(`/api/bridge/tasks?limit=50&_t=${Date.now()}`);
    state.tasks = payload.tasks || payload.rows || payload.items || [];
  } catch (_) {
    state.tasks = [];
  }
}

async function loadLogs() {
  try {
    const textPayload = await fetchText(`/api/logs/system?offset=${state.logOffset || 0}`, { timeoutMs: 4000 });
    if (!textPayload) return;
    const lines = textPayload.split(/\r?\n/).filter(Boolean);
    if (lines.length) {
      state.logs = [...state.logs, ...lines].slice(-160);
      state.logOffset += lines.length;
    }
  } catch (_) {
    // 日志不是关键路径，失败时保持页面可用。
  }
}

async function refreshAll({ silent = false } = {}) {
  if (refreshInFlight) return;
  refreshInFlight = true;
  if (!silent) setMessage("正在刷新内网端状态...");
  try {
    await Promise.allSettled([loadHealth(), loadConfig(), loadRuntimeStatus(), loadTasks(), loadLogs()]);
    if (!silent) setMessage("状态已刷新");
    render();
  } finally {
    refreshInFlight = false;
  }
}

function statusClass(tone) {
  const normalized = text(tone, "neutral").toLowerCase();
  if (["success", "ready", "ok"].includes(normalized)) return "is-success";
  if (["danger", "failed", "error"].includes(normalized)) return "is-danger";
  if (["info", "running", "downloading"].includes(normalized)) return "is-info";
  return "is-warning";
}

function shortLine(value, maxLength = 30) {
  let raw = text(value, "");
  const match = raw.match(/^(缓存文件|文件|路径|错误|原因)：(.+)$/);
  if (match) {
    const fileName = match[2].split(/[\\/]/).filter(Boolean).pop() || match[2];
    raw = `${match[1]}：${fileName}`;
  }
  if (raw.length <= maxLength) return raw;
  return `${raw.slice(0, Math.max(8, maxLength - 1))}…`;
}

function summaryCards() {
  const summary = state.summary || {};
  const pool = summary.pool || {};
  const cache = summary.source_cache || {};
  const queue = summary.queue || {};
  return `
    <section class="card-grid">
      <div class="metric-card">
        <span>运行角色</span>
        <strong>内网端</strong>
        <small>${escapeHtml(summary.updated_at || state.health.started_at || "")}</small>
      </div>
      <div class="metric-card">
        <span>共享桥接</span>
        <strong>${summary.bridge_enabled ? "已启用" : "未启用"}</strong>
        <small>${escapeHtml(summary.last_error || summary.agent_status || "等待后端状态")}</small>
      </div>
      <div class="metric-card">
        <span>浏览器池</span>
        <strong>${pool.browser_ready ? "已就绪" : "准备中"}</strong>
        <small>${escapeHtml(pool.last_error || `活跃楼栋：${(pool.active_buildings || []).join("、") || "-"}`)}</small>
      </div>
      <div class="metric-card">
        <span>共享目录</span>
        <strong>${cache.enabled ? "已启用" : "未启用"}</strong>
        <small>${escapeHtml(cache.cache_root || "-")}</small>
      </div>
      <div class="metric-card">
        <span>任务队列</span>
        <strong>${Number(queue.task_count || 0)}</strong>
        <small>待内网：${Number(queue.pending_internal || 0)} / 异常：${Number(queue.problematic || 0)}</small>
      </div>
    </section>
  `;
}

function renderPageSlots() {
  const slots = ((state.summary || {}).pool || {}).page_slots || [];
  const byBuilding = Object.fromEntries(slots.map((slot) => [slot.building, slot]));
  return `
    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>浏览器状态</h2>
          <p>内网端只负责打开和管理 A-E 楼下载页面。</p>
        </div>
      </div>
      <div class="building-grid">
        ${BUILDINGS.map((building) => {
          const slot = byBuilding[building] || { building };
          return `
            <article class="mini-card">
              <div class="row-between">
                <strong>${building}</strong>
                <span class="pill ${statusClass(slot.tone)}">${escapeHtml(slot.status_text || "等待中")}</span>
              </div>
              <p>${escapeHtml(slot.detail_text || "等待后端状态")}</p>
              <small>登录态：${escapeHtml(slot.login_text || slot.login_state || "-")}</small>
            </article>
          `;
        }).join("")}
      </div>
    </section>
  `;
}

function renderSourceFamily(key, fallbackTitle) {
  const cache = ((state.summary || {}).source_cache || {});
  const family = cache[key] || {};
  const rows = family.buildings || [];
  const rowByBuilding = Object.fromEntries(rows.map((row) => [row.building, row]));
  return `
    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>${escapeHtml(family.title || fallbackTitle)}</h2>
          <p>${escapeHtml(family.status_text || "等待后端状态")}</p>
          <p class="muted">${(family.meta_lines || []).map(escapeHtml).join("　")}</p>
        </div>
      </div>
      <div class="source-list">
        ${BUILDINGS.map((building) => {
          const row = rowByBuilding[building] || { building, source_family: key };
          const action = ((row.actions || {}).refresh || {});
          const busyKey = `${key}:${building}`;
          const disabled = state.busy.has(busyKey) || action.pending || action.allowed === false;
          return `
            <article class="source-row">
              <div>
                <div class="row-title">
                  <strong>${building}</strong>
                  <span class="pill ${statusClass(row.tone)}">${escapeHtml(row.status_text || "等待中")}</span>
                </div>
                <p>${escapeHtml(row.detail_text || "等待共享文件就绪")}</p>
                ${(row.meta_lines || []).map((line) => `<small>${escapeHtml(line)}</small>`).join("")}
              </div>
              <button
                class="btn btn-secondary"
                data-action="refresh-building"
                data-family="${escapeHtml(key)}"
                data-building="${escapeHtml(building)}"
                ${disabled ? "disabled" : ""}
                title="${escapeHtml(action.disabled_reason || "")}"
              >${state.busy.has(busyKey) ? "提交中..." : escapeHtml(action.label || "重新拉取")}</button>
            </article>
          `;
        }).join("")}
      </div>
    </section>
  `;
}

function renderSourceMatrix() {
  const cache = ((state.summary || {}).source_cache || {});
  return `
    <section class="panel source-overview-panel">
      <div class="panel-head">
        <div>
          <h2>源文件状态总览</h2>
          <p>按源文件类型和楼栋集中展示，便于快速确认下载中、失败或已就绪状态。</p>
        </div>
      </div>
      <div class="source-matrix-wrap">
        <div class="source-matrix" style="--building-count:${BUILDINGS.length}">
          <div class="source-matrix-head source-matrix-family-head">源文件</div>
          ${BUILDINGS.map((building) => `<div class="source-matrix-head">${escapeHtml(building)}</div>`).join("")}
          ${SOURCE_FAMILIES.map(([key, fallbackTitle]) => {
            const family = cache[key] || {};
            const rows = family.buildings || [];
            const rowByBuilding = Object.fromEntries(rows.map((row) => [row.building, row]));
            const familyTitle = family.title || fallbackTitle;
            return `
              <div class="source-family-cell">
                <div class="row-title">
                  <strong>${escapeHtml(familyTitle)}</strong>
                  <span class="pill ${statusClass(family.tone)}">${escapeHtml(family.status_text || "等待中")}</span>
                </div>
                <small>${(family.meta_lines || []).map((line) => escapeHtml(shortLine(line, 42))).join("<br>") || "等待后端状态"}</small>
              </div>
              ${BUILDINGS.map((building) => {
                const row = rowByBuilding[building] || { building, source_family: key };
                const action = ((row.actions || {}).refresh || {});
                const busyKey = `${key}:${building}`;
                const disabled = state.busy.has(busyKey) || action.pending || action.allowed === false;
                const title = [
                  row.status_text || "等待中",
                  row.detail_text || "",
                  ...(row.meta_lines || []),
                  action.disabled_reason || "",
                ].filter(Boolean).join("\\n");
                const detail = shortLine(row.detail_text || "等待共享文件就绪", 34);
                const meta = (row.meta_lines || []).slice(0, 1).map((line) => shortLine(line, 34)).join("");
                return `
                  <div class="source-status-cell" title="${escapeHtml(title)}">
                    <div class="source-status-top">
                      <span class="pill ${statusClass(row.tone)}">${escapeHtml(row.status_text || "等待中")}</span>
                    </div>
                    <p>${escapeHtml(detail)}</p>
                    ${meta ? `<small>${escapeHtml(meta)}</small>` : ""}
                    <button
                      class="btn btn-secondary btn-compact"
                      data-action="refresh-building"
                      data-family="${escapeHtml(key)}"
                      data-building="${escapeHtml(building)}"
                      ${disabled ? "disabled" : ""}
                      title="${escapeHtml(action.disabled_reason || title)}"
                    >${state.busy.has(busyKey) ? "提交中" : "重新拉取"}</button>
                  </div>
                `;
              }).join("")}
            `;
          }).join("")}
        </div>
      </div>
    </section>
  `;
}

function renderTasks() {
  const tasks = state.tasks || [];
  return `
    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>共享任务</h2>
          <p>只展示内网共享文件相关任务。</p>
        </div>
      </div>
      <div class="task-list">
        ${tasks.length ? tasks.map((task) => {
          const id = task.task_id || task.job_id || task.id || "";
          const status = task.status_text || task.status || "-";
          return `
            <article class="task-row">
              <div>
                <strong>${escapeHtml(task.name || task.title || task.feature || "共享任务")}</strong>
                <p>${escapeHtml(task.summary || task.detail_text || task.error || "")}</p>
                <small>${escapeHtml(status)}　${escapeHtml(task.created_at || task.updated_at || "")}</small>
              </div>
              ${id ? `<button class="btn btn-ghost" data-action="cancel-task" data-task-id="${escapeHtml(id)}">取消任务</button>` : ""}
            </article>
          `;
        }).join("") : `<div class="empty">暂无共享任务</div>`}
      </div>
    </section>
  `;
}

function renderLogs() {
  return `
    <section class="panel">
      <div class="panel-head"><h2>系统日志</h2></div>
      <pre class="log-box">${escapeHtml((state.logs || []).slice(-120).join("\n") || "等待日志...")}</pre>
    </section>
  `;
}

function renderStatus() {
  return `
    ${summaryCards()}
    <section class="action-bar">
      <button class="btn btn-primary" data-action="refresh-all">刷新状态</button>
      <button class="btn btn-secondary" data-action="refresh-current-hour">立即下载当前小时常规源文件</button>
      <button class="btn btn-secondary" data-action="self-check">共享目录自检</button>
    </section>
    ${renderPageSlots()}
    ${renderSourceMatrix()}
    ${renderTasks()}
    ${renderLogs()}
  `;
}

function siteRows() {
  const sites = (((state.config || {}).common || {}).internal_source_sites || []);
  const byBuilding = Object.fromEntries(sites.map((site) => [site.building, site]));
  return BUILDINGS.map((building) => byBuilding[building] || { building, enabled: false, host: "", username: "", password: "" });
}

function renderConfig() {
  const config = state.config || {};
  const common = config.common || {};
  const bridge = common.shared_bridge || {};
  const cache = common.internal_source_cache || {};
  const paths = common.paths || {};
  return `
    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>内网端配置</h2>
          <p>仅保留内网采集端所需配置。保存后立即重新加载后端配置。</p>
        </div>
      </div>
      <form id="config-form" class="config-form">
        <label>
          <span>共享目录</span>
          <input name="sharedRoot" value="${escapeHtml(bridge.internal_root_dir || bridge.root_dir || "")}" />
        </label>
        <label>
          <span>业务下载目录</span>
          <input name="businessRoot" value="${escapeHtml(paths.business_root_dir || "")}" />
        </label>
        <label>
          <span>运行数据目录</span>
          <input name="runtimeRoot" value="${escapeHtml(paths.runtime_state_root || ".runtime")}" />
        </label>
        <div class="inline-fields">
          <label>
            <span>启用共享下载</span>
            <select name="cacheEnabled">
              <option value="true" ${cache.enabled !== false ? "selected" : ""}>启用</option>
              <option value="false" ${cache.enabled === false ? "selected" : ""}>停用</option>
            </select>
          </label>
          <label>
            <span>启动后自动拉取</span>
            <select name="runOnStartup">
              <option value="true" ${cache.run_on_startup !== false ? "selected" : ""}>启用</option>
              <option value="false" ${cache.run_on_startup === false ? "selected" : ""}>停用</option>
            </select>
          </label>
          <label>
            <span>状态检查间隔秒</span>
            <input name="checkInterval" type="number" min="5" value="${escapeHtml(cache.check_interval_sec || 30)}" />
          </label>
        </div>
        <h3>五楼内网页面</h3>
        <div class="site-table">
          <div class="site-head">楼栋</div>
          <div class="site-head">启用</div>
          <div class="site-head">IP / 主机地址</div>
          <div class="site-head">账号</div>
          <div class="site-head">密码</div>
          ${siteRows().map((site, index) => `
            <input type="hidden" name="siteBuilding${index}" value="${escapeHtml(site.building)}" />
            <div class="site-building">${escapeHtml(site.building)}</div>
            <label class="checkbox-cell"><input name="siteEnabled${index}" type="checkbox" ${site.enabled !== false ? "checked" : ""} /></label>
            <input name="siteHost${index}" value="${escapeHtml(site.host || site.url || "")}" />
            <input name="siteUsername${index}" value="${escapeHtml(site.username || "")}" />
            <input name="sitePassword${index}" value="${escapeHtml(site.password || "")}" />
          `).join("")}
        </div>
        <div class="action-bar">
          <button class="btn btn-primary" type="submit" ${state.saving ? "disabled" : ""}>${state.saving ? "保存中..." : "保存配置"}</button>
          <button class="btn btn-secondary" type="button" data-action="reload-config">重新读取</button>
        </div>
      </form>
    </section>
  `;
}

function render() {
  const app = document.getElementById("app");
  if (!app) return;
  app.innerHTML = `
    <div class="app-shell internal-shell">
      <header class="ops-top-nav">
        <div class="ops-top-nav-main">
          <div class="ops-top-nav-brand">
            <span class="ops-top-nav-kicker">QJPT 内网采集端</span>
            <div class="ops-top-nav-title-row">
              <h1 class="ops-top-nav-title">内网源文件采集控制台</h1>
              <span class="version-inline">固定内网端</span>
            </div>
          </div>
          <nav class="ops-page-nav">
            <button class="btn ${state.view === "status" ? "btn-primary is-active" : "btn-ghost"}" data-action="nav" data-view="status">状态</button>
            <button class="btn ${state.view === "config" ? "btn-primary is-active" : "btn-ghost"}" data-action="nav" data-view="config">配置</button>
          </nav>
        </div>
      </header>
      ${state.message ? `<div class="global-message global-message-info">${escapeHtml(state.message)}</div>` : ""}
      ${state.error ? `<div class="global-message global-message-danger">${escapeHtml(state.error)}</div>` : ""}
      <main class="internal-main">${state.view === "config" ? renderConfig() : renderStatus()}</main>
    </div>
  `;
  document.body.classList.remove("app-boot-pending");
  const overlay = document.getElementById("app-boot-overlay");
  if (overlay) overlay.classList.add("is-hidden");
}

function collectConfigPayload() {
  const form = document.getElementById("config-form");
  const field = (name) => form.elements[name];
  const current = state.config || { version: 3, common: {} };
  const common = current.common || {};
  const sites = BUILDINGS.map((_, index) => ({
    building: field(`siteBuilding${index}`).value,
    enabled: field(`siteEnabled${index}`).checked,
    host: field(`siteHost${index}`).value.trim(),
    username: field(`siteUsername${index}`).value.trim(),
    password: field(`sitePassword${index}`).value,
    url: null,
  }));
  const sharedRoot = field("sharedRoot").value.trim();
  return {
    version: current.version || 3,
    common: {
      console: common.console || {},
      deployment: {
        ...(common.deployment || {}),
        role_mode: "internal",
        last_started_role_mode: "internal",
        node_label: "内网端",
      },
      paths: {
        ...(common.paths || {}),
        business_root_dir: field("businessRoot").value.trim(),
        runtime_state_root: field("runtimeRoot").value.trim() || ".runtime",
      },
      internal_source_sites: sites,
      internal_source_cache: {
        ...(common.internal_source_cache || {}),
        enabled: field("cacheEnabled").value === "true",
        run_on_startup: field("runOnStartup").value === "true",
        check_interval_sec: Math.max(5, Number(field("checkInterval").value || 30)),
      },
      shared_bridge: {
        ...(common.shared_bridge || {}),
        enabled: true,
        root_dir: sharedRoot,
        internal_root_dir: sharedRoot,
      },
    },
  };
}

async function handleAction(event) {
  const target = event.target.closest("[data-action]");
  if (!target) return;
  const action = target.dataset.action;
  if (action === "nav") {
    state.view = target.dataset.view || "status";
    history.replaceState(null, "", state.view === "config" ? "/internal/config" : "/internal/status");
    render();
    return;
  }
  try {
    if (action === "refresh-all" || action === "reload-config") {
      await refreshAll();
    } else if (action === "self-check") {
      setMessage("正在执行共享目录自检...");
      const payload = await api("/api/bridge/shared-root/self-check", { method: "POST", body: "{}" });
      setMessage(payload.status_text || payload.message || "共享目录自检完成");
      await refreshAll({ silent: true });
    } else if (action === "refresh-current-hour") {
      setMessage("已提交当前小时常规源文件下载...");
      const payload = await api("/api/bridge/source-cache/refresh-current-hour", { method: "POST", body: "{}" });
      setMessage(payload.message || "已开始下载当前小时常规源文件");
      await refreshAll({ silent: true });
    } else if (action === "refresh-building") {
      const family = target.dataset.family || "";
      const building = target.dataset.building || "";
      const busyKey = `${family}:${building}`;
      state.busy.add(busyKey);
      render();
      const payload = await api(query("/api/bridge/source-cache/refresh-building-latest", { source_family: family, building }), {
        method: "POST",
        body: "{}",
      });
      state.busy.delete(busyKey);
      setMessage(payload.message || `已开始重新拉取 ${building}`);
      await refreshAll({ silent: true });
    } else if (action === "cancel-task") {
      const taskId = target.dataset.taskId || "";
      if (!taskId) return;
      const payload = await api(`/api/bridge/tasks/${encodeURIComponent(taskId)}/cancel`, { method: "POST", body: "{}" });
      setMessage(payload.accepted ? "任务取消请求已提交" : "任务取消完成");
      await refreshAll({ silent: true });
    }
  } catch (error) {
    state.busy.clear();
    setMessage(error.message, true);
  }
}

async function handleSubmit(event) {
  if (event.target.id !== "config-form") return;
  event.preventDefault();
  try {
    state.saving = true;
    render();
    const payload = collectConfigPayload();
    const result = await api("/api/config", {
      method: "PUT",
      body: JSON.stringify(payload),
    });
    state.config = result.config || payload;
    setMessage("内网端配置已保存");
    await refreshAll({ silent: true });
  } catch (error) {
    setMessage(`保存配置失败：${error.message}`, true);
  } finally {
    state.saving = false;
    render();
  }
}

document.addEventListener("click", handleAction);
document.addEventListener("submit", handleSubmit);

render();
refreshAll({ silent: true });
setInterval(() => refreshAll({ silent: true }), 5000);
