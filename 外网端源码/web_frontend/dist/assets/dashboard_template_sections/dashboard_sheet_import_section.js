export const DASHBOARD_SHEET_IMPORT_SECTION = `        <section class="content-card" v-if="dashboardActiveModule === 'sheet_import'">
          <div class="dashboard-module-shell">
            <div class="dashboard-module-primary-grid">
              <article class="task-block task-block-accent">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">导入入口</div>
                    <h3 class="card-title">5Sheet 导入（清空后导入）</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="sheetFile ? 'tone-info' : 'tone-neutral'">
                    {{ sheetFile ? '已选择文件' : '待选择文件' }}
                  </span>
                </div>
                <div class="form-row">
                  <label class="label">5Sheet 文件</label>
                  <input type="file" accept=".xlsx" @change="onSheetFileChange" />
                </div>
                <div class="hint">{{ externalExecutionHint }}</div>
                <div class="btn-line">
                  <button class="btn btn-primary" :disabled="!canRun || isActionLocked(actionKeySheetImport)" @click="runSheetImport">
                    {{ isActionLocked(actionKeySheetImport) ? '提交中...' : '清空并上传 5 个工作表' }}
                  </button>
                </div>
              </article>

              <article class="task-block task-block-compact dashboard-module-status-card">
                <div class="task-block-head">
                  <div>
                    <div class="task-block-kicker">当前状态</div>
                    <h3 class="card-title">导入规则概览</h3>
                  </div>
                  <span class="status-badge status-badge-soft" :class="sheetFile ? 'tone-success' : 'tone-warning'">
                    {{ sheetFile ? '可提交' : '待补文件' }}
                  </span>
                </div>
                <div class="status-metric-grid status-metric-grid-compact">
                  <div class="status-metric">
                    <div class="status-metric-label">文件状态</div>
                    <strong class="status-metric-value">{{ sheetFile ? '已选择' : '未选择' }}</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">导入策略</div>
                    <strong class="status-metric-value">清空后重写</strong>
                  </div>
                  <div class="status-metric">
                    <div class="status-metric-label">执行角色</div>
                    <strong class="status-metric-value">外网端</strong>
                  </div>
                </div>
                <div class="hint">当前文件：{{ sheetFile ? sheetFile.name : '尚未选择 5Sheet 文件' }}</div>
                <div class="hint">执行影响：目标多维表会先清空，再按 5 个工作表重写数据。</div>
              </article>
            </div>
          </div>
        </section>

`;

