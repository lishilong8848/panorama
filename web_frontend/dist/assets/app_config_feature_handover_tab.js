export const CONFIG_FEATURE_HANDOVER_TAB_TEMPLATE = `
<div v-if="activeConfigTab==='feature_handover'" class="config-tab-shell">
  <div class="config-tab-hero">
    <div class="section-title">交接班日志</div>
    <div class="status-metric-grid-compact">
      <div class="status-metric-card compact">
        <div class="status-metric-label">模板名称</div>
        <div class="status-metric-value">{{ config.handover_log.download.template_name || '未设置' }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">规则数量</div>
        <div class="status-metric-value">{{ getActiveHandoverRuleRows().length }}</div>
      </div>
      <div class="status-metric-card compact">
        <div class="status-metric-label">云文档同步</div>
        <div class="status-metric-value">{{ config.handover_log.cloud_sheet_sync.enabled ? '已启用' : '未启用' }}</div>
      </div>
    </div>
    <div class="config-quick-tab-grid">
      <a class="btn btn-secondary" href="#handover-config-basic">下载与基础班次</a>
      <a class="btn btn-secondary" href="#handover-config-sources">来源、分类与目录</a>
      <a class="btn btn-secondary" href="#handover-config-output">上报与同步</a>
      <a class="btn btn-secondary" href="#handover-config-review">审核与模板规则</a>
    </div>
    <div class="hint-stack">
      <div class="hint">交接班配置项较多，已按下载、来源、上报和审核模板拆成内部大分组。</div>
      <div class="hint">组内仍保留原有字段顺序，方便沿用现有配置经验和历史说明。</div>
    </div>
  </div>
  <details id="handover-config-basic" class="config-group-panel" open>
    <summary class="config-group-summary">下载与基础班次</summary>
    <div class="config-group-body">
      <div class="config-panel-grid two-col">
      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">下载与班次时间窗</div>
        
          <div class="form-row"><label class="label">交接班报表模板名称</label><input type="text" v-model="config.handover_log.download.template_name" /></div>
          <div class="form-row"><label class="label">查询刻度</label><input type="text" v-model="config.handover_log.download.scale_label" /></div>
          <div class="form-row"><label class="label">白班开始</label><input type="text" v-model="config.handover_log.download.shift_windows.day.start" /></div>
          <div class="form-row"><label class="label">白班结束</label><input type="text" v-model="config.handover_log.download.shift_windows.day.end" /></div>
          <div class="form-row"><label class="label">夜班开始</label><input type="text" v-model="config.handover_log.download.shift_windows.night.start" /></div>
          <div class="form-row"><label class="label">夜班结束（次日）</label><input type="text" v-model="config.handover_log.download.shift_windows.night.end_next_day" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.download.parallel_by_building" /> 并发下载楼栋</label></div>
          <div class="form-row"><label class="label">查询结果等待(ms)</label><input type="number" v-model.number="config.handover_log.download.query_result_timeout_ms" /></div>
          <div class="form-row"><label class="label">菜单等待(ms)</label><input type="number" v-model.number="config.handover_log.download.menu_visible_timeout_ms" /></div>
          <div class="form-row"><label class="label">iframe等待(ms)</label><input type="number" v-model.number="config.handover_log.download.iframe_timeout_ms" /></div>
          <div class="form-row"><label class="label">时间输入框等待(ms)</label><input type="number" v-model.number="config.handover_log.download.start_end_visible_timeout_ms" /></div>
          <div class="hint">默认串行更稳定；内网页面查询时间窗默认使用最近20分钟，只有内网页面负载较低时再开启并发。</div>
      </section>
      </div>
    </div>
  </details>
  <details id="handover-config-sources" class="config-group-panel" open>
    <summary class="config-group-summary">来源、分类与目录</summary>
    <div class="config-group-body">
      <div class="config-panel-grid two-col">
      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">排班多维来源（C3/G3/H52~H55）</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.shift_roster.enabled" /> 启用排班人员填充</label></div>
          <div class="form-row"><label class="label">多维 App Token</label><input type="text" v-model="config.handover_log.shift_roster.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.shift_roster.source.table_id" /></div>
          <div class="form-row"><label class="label">分页大小</label><input type="number" v-model.number="config.handover_log.shift_roster.source.page_size" /></div>
          <div class="form-row"><label class="label">最多读取记录数</label><input type="number" v-model.number="config.handover_log.shift_roster.source.max_records" /></div>
          <div class="form-row"><label class="label">字段：排班日期</label><input type="text" v-model="config.handover_log.shift_roster.fields.duty_date" /></div>
          <div class="form-row"><label class="label">字段：机楼</label><input type="text" v-model="config.handover_log.shift_roster.fields.building" /></div>
          <div class="form-row"><label class="label">字段：班组</label><input type="text" v-model="config.handover_log.shift_roster.fields.team" /></div>
          <div class="form-row"><label class="label">字段：班次</label><input type="text" v-model="config.handover_log.shift_roster.fields.shift" /></div>
          <div class="form-row"><label class="label">字段：人员（文本）</label><input type="text" v-model="config.handover_log.shift_roster.fields.people_text" /></div>
          <div class="form-row"><label class="label">当前班组人员单元格（C3）</label><input type="text" v-model="config.handover_log.shift_roster.cells.current_people" /></div>
          <div class="form-row"><label class="label">下一个班组人员单元格（G3）</label><input type="text" v-model="config.handover_log.shift_roster.cells.next_people" /></div>
          <div class="form-row">
            <label class="label">下个班组首人回填单元格（逗号分隔）</label>
            <input
              type="text"
              :value="(config.handover_log.shift_roster.cells.next_first_person_cells || []).join(', ')"
              @input="config.handover_log.shift_roster.cells.next_first_person_cells = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim().toUpperCase()).filter(Boolean)"
            />
          </div>
          <div class="form-row"><label class="label">机楼匹配模式</label><select v-model="config.handover_log.shift_roster.match.building_mode"><option value="exact_then_code">精确优先+楼栋编码兜底</option></select></div>
          <div class="form-row">
            <label class="label">白班别名（逗号分隔）</label>
            <input
              type="text"
              :value="(config.handover_log.shift_roster.shift_alias.day || []).join(', ')"
              @input="config.handover_log.shift_roster.shift_alias.day = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)"
            />
          </div>
          <div class="form-row">
            <label class="label">夜班别名（逗号分隔）</label>
            <input
              type="text"
              :value="(config.handover_log.shift_roster.shift_alias.night || []).join(', ')"
              @input="config.handover_log.shift_roster.shift_alias.night = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)"
            />
          </div>
          <div class="form-row"><label class="label">人员拆分正则</label><input type="text" v-model="config.handover_log.shift_roster.people_split_regex" /></div>
          <div class="hint">C3=当前班组人员，G3=下一个班组人员；G3首人会回填到 H52/H53/H54/H55。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">事件分类来源（新事件处理/历史事件跟进）</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.event_sections.enabled" /> 启用事件分类写入</label></div>
          <div class="form-row"><label class="label">多维 App Token</label><input type="text" v-model="config.handover_log.event_sections.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.event_sections.source.table_id" /></div>
          <div class="form-row"><label class="label">分页大小</label><input type="number" v-model.number="config.handover_log.event_sections.source.page_size" /></div>
          <div class="form-row"><label class="label">最多读取记录数</label><input type="number" v-model.number="config.handover_log.event_sections.source.max_records" /></div>
          <div class="form-row"><label class="label">白班开始</label><input type="time" step="1" v-model="config.handover_log.event_sections.duty_window.day_start" /></div>
          <div class="form-row"><label class="label">白班结束</label><input type="time" step="1" v-model="config.handover_log.event_sections.duty_window.day_end" /></div>
          <div class="form-row"><label class="label">夜班开始</label><input type="time" step="1" v-model="config.handover_log.event_sections.duty_window.night_start" /></div>
          <div class="form-row"><label class="label">夜班结束（次日）</label><input type="time" step="1" v-model="config.handover_log.event_sections.duty_window.night_end_next_day" /></div>
          <div class="form-row"><label class="label">边界模式</label><input type="text" v-model="config.handover_log.event_sections.duty_window.boundary_mode" /></div>
          <div class="form-row"><label class="label">字段：事件发生时间</label><input type="text" v-model="config.handover_log.event_sections.fields.event_time" /></div>
          <div class="form-row"><label class="label">字段：机楼（楼栋匹配）</label><input type="text" v-model="config.handover_log.event_sections.fields.building" /></div>
          <div class="form-row"><label class="label">字段：事件等级</label><input type="text" v-model="config.handover_log.event_sections.fields.event_level" /></div>
          <div class="form-row"><label class="label">字段：告警描述</label><input type="text" v-model="config.handover_log.event_sections.fields.description" /></div>
          <div class="form-row"><label class="label">字段：不计入事件（复选框）</label><input type="text" v-model="config.handover_log.event_sections.fields.exclude_checked" /></div>
          <div class="form-row"><label class="label">字段：最终状态（公式）</label><input type="text" v-model="config.handover_log.event_sections.fields.final_status" /></div>
          <div class="form-row"><label class="label">兼容字段：事件结束处理时长（旧）</label><input type="text" v-model="config.handover_log.event_sections.fields.exclude_duration" /></div>
          <div class="form-row"><label class="label">兼容字段：排除值（旧）</label><input type="text" v-model="config.handover_log.event_sections.fields.exclude_duration_value" /></div>
          <div class="form-row"><label class="label">字段：是否转检修</label><input type="text" v-model="config.handover_log.event_sections.fields.to_maint" /></div>
          <div class="form-row"><label class="label">字段：检修完成时间</label><input type="text" v-model="config.handover_log.event_sections.fields.maint_done_time" /></div>
          <div class="form-row"><label class="label">字段：事件结束时间</label><input type="text" v-model="config.handover_log.event_sections.fields.event_done_time" /></div>
          <div class="form-row"><label class="label">分类名：新事件处理</label><input type="text" v-model="config.handover_log.event_sections.sections.new_event" /></div>
          <div class="form-row"><label class="label">分类名：历史事件跟进</label><input type="text" v-model="config.handover_log.event_sections.sections.history_followup" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.event_sections.column_mapping.resolve_by_header" /> 按模板表头自动定位列</label></div>
          <div class="form-row"><label class="label">表头别名：事件等级（逗号分隔）</label><input type="text" :value="(config.handover_log.event_sections.column_mapping.header_alias.event_level || []).join(', ')" @input="config.handover_log.event_sections.column_mapping.header_alias.event_level = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：发生时间</label><input type="text" :value="(config.handover_log.event_sections.column_mapping.header_alias.event_time || []).join(', ')" @input="config.handover_log.event_sections.column_mapping.header_alias.event_time = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：描述</label><input type="text" :value="(config.handover_log.event_sections.column_mapping.header_alias.description || []).join(', ')" @input="config.handover_log.event_sections.column_mapping.header_alias.description = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：作业时间段</label><input type="text" :value="(config.handover_log.event_sections.column_mapping.header_alias.work_window || []).join(', ')" @input="config.handover_log.event_sections.column_mapping.header_alias.work_window = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：事件处理进展</label><input type="text" :value="(config.handover_log.event_sections.column_mapping.header_alias.progress || []).join(', ')" @input="config.handover_log.event_sections.column_mapping.header_alias.progress = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：跟进人</label><input type="text" :value="(config.handover_log.event_sections.column_mapping.header_alias.follower || []).join(', ')" @input="config.handover_log.event_sections.column_mapping.header_alias.follower = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">回退列：事件等级</label><input type="text" v-model="config.handover_log.event_sections.column_mapping.fallback_cols.event_level" /></div>
          <div class="form-row"><label class="label">回退列：发生时间</label><input type="text" v-model="config.handover_log.event_sections.column_mapping.fallback_cols.event_time" /></div>
          <div class="form-row"><label class="label">回退列：描述</label><input type="text" v-model="config.handover_log.event_sections.column_mapping.fallback_cols.description" /></div>
          <div class="form-row"><label class="label">回退列：作业时间段</label><input type="text" v-model="config.handover_log.event_sections.column_mapping.fallback_cols.work_window" /></div>
          <div class="form-row"><label class="label">回退列：事件处理进展</label><input type="text" v-model="config.handover_log.event_sections.column_mapping.fallback_cols.progress" /></div>
          <div class="form-row"><label class="label">回退列：跟进人</label><input type="text" v-model="config.handover_log.event_sections.column_mapping.fallback_cols.follower" /></div>
          <div class="form-row"><label class="label">进展文案：已完成</label><input type="text" v-model="config.handover_log.event_sections.progress_text.done" /></div>
          <div class="form-row"><label class="label">进展文案：未完成</label><input type="text" v-model="config.handover_log.event_sections.progress_text.todo" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.event_sections.cache.enabled" /> 启用未完成缓存</label></div>
          <div class="form-row"><label class="label">缓存状态文件</label><input type="text" v-model="config.handover_log.event_sections.cache.state_file" /></div>
          <div class="form-row"><label class="label">缓存上限</label><input type="number" v-model.number="config.handover_log.event_sections.cache.max_pending" /></div>
          <div class="form-row"><label class="label">最近查询ID上限</label><input type="number" v-model.number="config.handover_log.event_sections.cache.max_last_query_ids" /></div>
          <div class="hint">历史事件跟进来源 = 非当班“事件闭环转检修中” + 缓存闭环回写；同时应用于“从已有数据表生成”和“使用共享文件生成”。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">体系月度统计表（事件月度统计表）</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.monthly_event_report.enabled" /> 启用月度事件统计表处理</label></div>
          <div class="form-row"><label class="label">事件模板文件</label><input type="text" v-model="config.handover_log.monthly_event_report.template.source_path" /></div>
          <div class="form-row"><label class="label">输出目录</label><input type="text" v-model="config.handover_log.monthly_event_report.template.output_dir" /></div>
          <div class="form-row"><label class="label">文件命名规则</label><input type="text" v-model="config.handover_log.monthly_event_report.template.file_name_pattern" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.monthly_event_report.scheduler.enabled" /> 启用月度调度</label></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.monthly_event_report.scheduler.auto_start_in_gui" /> 启动后自动开启调度</label></div>
          <div class="form-row"><label class="label">每月几号</label><input type="number" min="1" max="31" v-model.number="config.handover_log.monthly_event_report.scheduler.day_of_month" /></div>
          <div class="form-row"><label class="label">调度时间</label><input type="time" step="1" v-model="config.handover_log.monthly_event_report.scheduler.run_time" /></div>
          <div class="form-row"><label class="label">检查间隔（秒）</label><input type="number" min="1" v-model.number="config.handover_log.monthly_event_report.scheduler.check_interval_sec" /></div>
          <div class="form-row"><label class="label">调度状态文件</label><input type="text" v-model="config.handover_log.monthly_event_report.scheduler.state_file" /></div>
          <div class="hint">数据源固定复用“新事件处理”同一张多维表。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">体系月度统计表（变更月度统计表）</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.monthly_change_report.enabled" /> 启用月度变更统计表处理</label></div>
          <div class="form-row"><label class="label">变更模板文件</label><input type="text" v-model="config.handover_log.monthly_change_report.template.source_path" /></div>
          <div class="form-row"><label class="label">输出目录</label><input type="text" v-model="config.handover_log.monthly_change_report.template.output_dir" /></div>
          <div class="form-row"><label class="label">文件命名规则</label><input type="text" v-model="config.handover_log.monthly_change_report.template.file_name_pattern" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.monthly_change_report.scheduler.enabled" /> 启用月度调度</label></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.monthly_change_report.scheduler.auto_start_in_gui" /> 启动后自动开启调度</label></div>
          <div class="form-row"><label class="label">每月几号</label><input type="number" min="1" max="31" v-model.number="config.handover_log.monthly_change_report.scheduler.day_of_month" /></div>
          <div class="form-row"><label class="label">调度时间</label><input type="time" step="1" v-model="config.handover_log.monthly_change_report.scheduler.run_time" /></div>
          <div class="form-row"><label class="label">检查间隔（秒）</label><input type="number" min="1" v-model.number="config.handover_log.monthly_change_report.scheduler.check_interval_sec" /></div>
          <div class="form-row"><label class="label">调度状态文件</label><input type="text" v-model="config.handover_log.monthly_change_report.scheduler.state_file" /></div>
          <div class="hint">变更月报按“变更开始时间”归属到上一个自然月，输出只写本地文件，不上传飞书。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">变更管理分类来源</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.change_management_section.enabled" /> 启用变更管理写入</label></div>
          <div class="form-row"><label class="label">多维 App Token</label><input type="text" v-model="config.handover_log.change_management_section.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.change_management_section.source.table_id" /></div>
          <div class="form-row"><label class="label">分页大小</label><input type="number" v-model.number="config.handover_log.change_management_section.source.page_size" /></div>
          <div class="form-row"><label class="label">最多读取记录数</label><input type="number" v-model.number="config.handover_log.change_management_section.source.max_records" /></div>
          <div class="form-row"><label class="label">字段：楼栋（多选）</label><input type="text" v-model="config.handover_log.change_management_section.fields.building" /></div>
          <div class="form-row"><label class="label">字段：更新最新的时间</label><input type="text" v-model="config.handover_log.change_management_section.fields.updated_time" /></div>
          <div class="form-row"><label class="label">字段：阿里-变更等级</label><input type="text" v-model="config.handover_log.change_management_section.fields.change_level" /></div>
          <div class="form-row"><label class="label">字段：过程更新时间</label><input type="text" v-model="config.handover_log.change_management_section.fields.process_updates" /></div>
          <div class="form-row"><label class="label">字段：名称</label><input type="text" v-model="config.handover_log.change_management_section.fields.description" /></div>
          <div class="form-row"><label class="label">字段：专业</label><input type="text" v-model="config.handover_log.change_management_section.fields.specialty" /></div>
          <div class="form-row"><label class="label">月报字段：楼栋</label><input type="text" v-model="config.handover_log.change_management_section.monthly_report_fields.building" /></div>
          <div class="form-row"><label class="label">月报字段：变更编码</label><input type="text" v-model="config.handover_log.change_management_section.monthly_report_fields.change_code" /></div>
          <div class="form-row"><label class="label">月报字段：名称</label><input type="text" v-model="config.handover_log.change_management_section.monthly_report_fields.name" /></div>
          <div class="form-row"><label class="label">月报字段：位置</label><input type="text" v-model="config.handover_log.change_management_section.monthly_report_fields.location" /></div>
          <div class="form-row"><label class="label">月报字段：智航-变更等级</label><input type="text" v-model="config.handover_log.change_management_section.monthly_report_fields.change_level" /></div>
          <div class="form-row"><label class="label">月报字段：变更状态</label><input type="text" v-model="config.handover_log.change_management_section.monthly_report_fields.status" /></div>
          <div class="form-row"><label class="label">月报字段：变更开始时间</label><input type="text" v-model="config.handover_log.change_management_section.monthly_report_fields.start_time" /></div>
          <div class="form-row"><label class="label">月报字段：变更结束时间</label><input type="text" v-model="config.handover_log.change_management_section.monthly_report_fields.end_time" /></div>
          <div class="form-row"><label class="label">分类名：变更管理</label><input type="text" v-model="config.handover_log.change_management_section.sections.change_management" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.change_management_section.column_mapping.resolve_by_header" /> 按模板表头自动定位列</label></div>
          <div class="form-row"><label class="label">表头别名：变更等级</label><input type="text" :value="(config.handover_log.change_management_section.column_mapping.header_alias.change_level || []).join(', ')" @input="config.handover_log.change_management_section.column_mapping.header_alias.change_level = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：作业时间段</label><input type="text" :value="(config.handover_log.change_management_section.column_mapping.header_alias.work_window || []).join(', ')" @input="config.handover_log.change_management_section.column_mapping.header_alias.work_window = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：描述</label><input type="text" :value="(config.handover_log.change_management_section.column_mapping.header_alias.description || []).join(', ')" @input="config.handover_log.change_management_section.column_mapping.header_alias.description = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：执行人</label><input type="text" :value="(config.handover_log.change_management_section.column_mapping.header_alias.executor || []).join(', ')" @input="config.handover_log.change_management_section.column_mapping.header_alias.executor = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">回退列：变更等级</label><input type="text" v-model="config.handover_log.change_management_section.column_mapping.fallback_cols.change_level" /></div>
          <div class="form-row"><label class="label">回退列：作业时间段</label><input type="text" v-model="config.handover_log.change_management_section.column_mapping.fallback_cols.work_window" /></div>
          <div class="form-row"><label class="label">回退列：描述</label><input type="text" v-model="config.handover_log.change_management_section.column_mapping.fallback_cols.description" /></div>
          <div class="form-row"><label class="label">回退列：执行人</label><input type="text" v-model="config.handover_log.change_management_section.column_mapping.fallback_cols.executor" /></div>
          <div class="form-row"><label class="label">白班作业时间锚点</label><input type="time" step="1" v-model="config.handover_log.change_management_section.work_window_text.day_anchor" /></div>
          <div class="form-row"><label class="label">白班默认结束</label><input type="time" step="1" v-model="config.handover_log.change_management_section.work_window_text.day_default_end" /></div>
          <div class="form-row"><label class="label">夜班作业时间锚点</label><input type="time" step="1" v-model="config.handover_log.change_management_section.work_window_text.night_anchor" /></div>
          <div class="form-row"><label class="label">夜班默认结束（次日）</label><input type="time" step="1" v-model="config.handover_log.change_management_section.work_window_text.night_default_end_next_day" /></div>
          <div class="hint">变更管理分类仍按“更新最新的时间”命中当前班次；月度变更统计表则复用同一张多维表，并按“变更开始时间”统计上一个自然月。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">演练管理分类来源</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.exercise_management_section.enabled" /> 启用演练管理写入</label></div>
          <div class="form-row"><label class="label">多维 App Token</label><input type="text" v-model="config.handover_log.exercise_management_section.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.exercise_management_section.source.table_id" /></div>
          <div class="form-row"><label class="label">分页大小</label><input type="number" v-model.number="config.handover_log.exercise_management_section.source.page_size" /></div>
          <div class="form-row"><label class="label">最多读取记录数</label><input type="number" v-model.number="config.handover_log.exercise_management_section.source.max_records" /></div>
          <div class="form-row"><label class="label">字段：机楼</label><input type="text" v-model="config.handover_log.exercise_management_section.fields.building" /></div>
          <div class="form-row"><label class="label">字段：演练开始时间</label><input type="text" v-model="config.handover_log.exercise_management_section.fields.start_time" /></div>
          <div class="form-row"><label class="label">字段：告警描述</label><input type="text" v-model="config.handover_log.exercise_management_section.fields.project" /></div>
          <div class="form-row"><label class="label">分类名：演练管理</label><input type="text" v-model="config.handover_log.exercise_management_section.sections.exercise_management" /></div>
          <div class="form-row"><label class="label">固定文案：演练类型</label><input type="text" v-model="config.handover_log.exercise_management_section.fixed_values.exercise_type" /></div>
          <div class="form-row"><label class="label">固定文案：演练完成情况</label><input type="text" v-model="config.handover_log.exercise_management_section.fixed_values.completion" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.exercise_management_section.column_mapping.resolve_by_header" /> 按模板表头自动定位列</label></div>
          <div class="form-row"><label class="label">表头别名：演练类型</label><input type="text" :value="(config.handover_log.exercise_management_section.column_mapping.header_alias.exercise_type || []).join(', ')" @input="config.handover_log.exercise_management_section.column_mapping.header_alias.exercise_type = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：演练项目</label><input type="text" :value="(config.handover_log.exercise_management_section.column_mapping.header_alias.exercise_item || []).join(', ')" @input="config.handover_log.exercise_management_section.column_mapping.header_alias.exercise_item = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：演练完成情况</label><input type="text" :value="(config.handover_log.exercise_management_section.column_mapping.header_alias.completion || []).join(', ')" @input="config.handover_log.exercise_management_section.column_mapping.header_alias.completion = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：执行人</label><input type="text" :value="(config.handover_log.exercise_management_section.column_mapping.header_alias.executor || []).join(', ')" @input="config.handover_log.exercise_management_section.column_mapping.header_alias.executor = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">回退列：演练类型</label><input type="text" v-model="config.handover_log.exercise_management_section.column_mapping.fallback_cols.exercise_type" /></div>
          <div class="form-row"><label class="label">回退列：演练项目</label><input type="text" v-model="config.handover_log.exercise_management_section.column_mapping.fallback_cols.exercise_item" /></div>
          <div class="form-row"><label class="label">回退列：演练完成情况</label><input type="text" v-model="config.handover_log.exercise_management_section.column_mapping.fallback_cols.completion" /></div>
          <div class="form-row"><label class="label">回退列：执行人</label><input type="text" v-model="config.handover_log.exercise_management_section.column_mapping.fallback_cols.executor" /></div>
          <div class="hint">演练管理按“机楼”包含关系归属楼栋；多楼一起执行时会一次读取当前班次记录并按楼栋复用。执行人固定取当前楼 C3。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">维护管理分类来源</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.maintenance_management_section.enabled" /> 启用维护管理写入</label></div>
          <div class="form-row"><label class="label">多维 App Token</label><input type="text" v-model="config.handover_log.maintenance_management_section.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.maintenance_management_section.source.table_id" /></div>
          <div class="form-row"><label class="label">分页大小</label><input type="number" v-model.number="config.handover_log.maintenance_management_section.source.page_size" /></div>
          <div class="form-row"><label class="label">最多读取记录数</label><input type="number" v-model.number="config.handover_log.maintenance_management_section.source.max_records" /></div>
          <div class="form-row"><label class="label">字段：楼栋</label><input type="text" v-model="config.handover_log.maintenance_management_section.fields.building" /></div>
          <div class="form-row"><label class="label">字段：开始时间（班次判定）</label><input type="text" v-model="config.handover_log.maintenance_management_section.fields.start_time" /></div>
          <div class="form-row"><label class="label">字段：名称</label><input type="text" v-model="config.handover_log.maintenance_management_section.fields.item" /></div>
          <div class="form-row"><label class="label">字段：专业</label><input type="text" v-model="config.handover_log.maintenance_management_section.fields.specialty" /></div>
          <div class="form-row"><label class="label">分类名：维护管理</label><input type="text" v-model="config.handover_log.maintenance_management_section.sections.maintenance_management" /></div>
          <div class="form-row"><label class="label">固定文案：自维</label><input type="text" v-model="config.handover_log.maintenance_management_section.fixed_values.vendor_internal" /></div>
          <div class="form-row"><label class="label">固定文案：厂维</label><input type="text" v-model="config.handover_log.maintenance_management_section.fixed_values.vendor_external" /></div>
          <div class="form-row"><label class="label">固定文案：维护完成情况</label><input type="text" v-model="config.handover_log.maintenance_management_section.fixed_values.completion" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.maintenance_management_section.column_mapping.resolve_by_header" /> 按模板表头自动定位列</label></div>
          <div class="form-row"><label class="label">表头别名：维护总项</label><input type="text" :value="(config.handover_log.maintenance_management_section.column_mapping.header_alias.maintenance_item || []).join(', ')" @input="config.handover_log.maintenance_management_section.column_mapping.header_alias.maintenance_item = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：维护执行方</label><input type="text" :value="(config.handover_log.maintenance_management_section.column_mapping.header_alias.maintenance_party || []).join(', ')" @input="config.handover_log.maintenance_management_section.column_mapping.header_alias.maintenance_party = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：维护完成情况</label><input type="text" :value="(config.handover_log.maintenance_management_section.column_mapping.header_alias.completion || []).join(', ')" @input="config.handover_log.maintenance_management_section.column_mapping.header_alias.completion = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：执行人</label><input type="text" :value="(config.handover_log.maintenance_management_section.column_mapping.header_alias.executor || []).join(', ')" @input="config.handover_log.maintenance_management_section.column_mapping.header_alias.executor = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">回退列：维护总项</label><input type="text" v-model="config.handover_log.maintenance_management_section.column_mapping.fallback_cols.maintenance_item" /></div>
          <div class="form-row"><label class="label">回退列：维护执行方</label><input type="text" v-model="config.handover_log.maintenance_management_section.column_mapping.fallback_cols.maintenance_party" /></div>
          <div class="form-row"><label class="label">回退列：维护完成情况</label><input type="text" v-model="config.handover_log.maintenance_management_section.column_mapping.fallback_cols.completion" /></div>
          <div class="form-row"><label class="label">回退列：执行人</label><input type="text" v-model="config.handover_log.maintenance_management_section.column_mapping.fallback_cols.executor" /></div>
          <div class="hint">维护管理按“楼栋”包含关系归属楼栋；多楼一起执行时会一次读取当前班次记录并按楼栋复用。执行人按“当前楼栋 + 专业”匹配工程师目录主管；名称包含“厂家/厂商”写厂维，否则写自维。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">其他重要工作记录来源</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.other_important_work_section.enabled" /> 启用其他重要工作记录写入</label></div>
          <div class="form-row"><label class="label">共享多维 App Token</label><input type="text" v-model="config.handover_log.other_important_work_section.source.app_token" /></div>
          <div class="form-row"><label class="label">分页大小</label><input type="number" v-model.number="config.handover_log.other_important_work_section.source.page_size" /></div>
          <div class="form-row"><label class="label">最多读取记录数</label><input type="number" v-model.number="config.handover_log.other_important_work_section.source.max_records" /></div>
          <div class="form-row"><label class="label">分类名：其他重要工作记录</label><input type="text" v-model="config.handover_log.other_important_work_section.sections.other_important_work" /></div>
          <div class="form-row"><label class="label">来源顺序（逗号分隔）</label><input type="text" :value="(config.handover_log.other_important_work_section.order || []).join(', ')" @input="config.handover_log.other_important_work_section.order = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.other_important_work_section.column_mapping.resolve_by_header" /> 按模板表头自动定位列</label></div>
          <div class="form-row"><label class="label">表头别名：描述</label><input type="text" :value="(config.handover_log.other_important_work_section.column_mapping.header_alias.description || []).join(', ')" @input="config.handover_log.other_important_work_section.column_mapping.header_alias.description = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：完成情况</label><input type="text" :value="(config.handover_log.other_important_work_section.column_mapping.header_alias.completion || []).join(', ')" @input="config.handover_log.other_important_work_section.column_mapping.header_alias.completion = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">表头别名：执行人</label><input type="text" :value="(config.handover_log.other_important_work_section.column_mapping.header_alias.executor || []).join(', ')" @input="config.handover_log.other_important_work_section.column_mapping.header_alias.executor = $event.target.value.split(/[，,;\\s]+/).map(v => v.trim()).filter(Boolean)" /></div>
          <div class="form-row"><label class="label">回退列：描述</label><input type="text" v-model="config.handover_log.other_important_work_section.column_mapping.fallback_cols.description" /></div>
          <div class="form-row"><label class="label">回退列：完成情况</label><input type="text" v-model="config.handover_log.other_important_work_section.column_mapping.fallback_cols.completion" /></div>
          <div class="form-row"><label class="label">回退列：执行人</label><input type="text" v-model="config.handover_log.other_important_work_section.column_mapping.fallback_cols.executor" /></div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">其他重要工作记录来源：上电通告</div>
          <div class="form-row"><label class="label">Table ID</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.power_notice.table_id" /></div>
          <div class="form-row"><label class="label">字段：楼栋</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.power_notice.fields.building" /></div>
          <div class="form-row"><label class="label">字段：实际结束时间</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.power_notice.fields.actual_end_time" /></div>
          <div class="form-row"><label class="label">字段：描述</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.power_notice.fields.description" /></div>
          <div class="form-row"><label class="label">字段：完成情况</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.power_notice.fields.completion" /></div>
          <div class="form-row"><label class="label">字段：专业</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.power_notice.fields.specialty" /></div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">其他重要工作记录来源：设备调整</div>
          <div class="form-row"><label class="label">Table ID</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_adjustment.table_id" /></div>
          <div class="form-row"><label class="label">字段：楼栋</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_adjustment.fields.building" /></div>
          <div class="form-row"><label class="label">字段：实际结束时间</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_adjustment.fields.actual_end_time" /></div>
          <div class="form-row"><label class="label">字段：描述</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_adjustment.fields.description" /></div>
          <div class="form-row"><label class="label">字段：完成情况</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_adjustment.fields.completion" /></div>
          <div class="form-row"><label class="label">字段：专业</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_adjustment.fields.specialty" /></div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">其他重要工作记录来源：设备轮巡</div>
          <div class="form-row"><label class="label">Table ID</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_patrol.table_id" /></div>
          <div class="form-row"><label class="label">字段：楼栋</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_patrol.fields.building" /></div>
          <div class="form-row"><label class="label">字段：实际结束时间</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_patrol.fields.actual_end_time" /></div>
          <div class="form-row"><label class="label">字段：描述</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_patrol.fields.description" /></div>
          <div class="form-row"><label class="label">字段：完成情况</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_patrol.fields.completion" /></div>
          <div class="form-row"><label class="label">字段：专业</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_patrol.fields.specialty" /></div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">其他重要工作记录来源：设备检修</div>
          <div class="form-row"><label class="label">Table ID</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_repair.table_id" /></div>
          <div class="form-row"><label class="label">字段：楼栋</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_repair.fields.building" /></div>
          <div class="form-row"><label class="label">字段：实际结束时间</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_repair.fields.actual_end_time" /></div>
          <div class="form-row"><label class="label">字段：描述</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_repair.fields.description" /></div>
          <div class="form-row"><label class="label">字段：完成情况</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_repair.fields.completion" /></div>
          <div class="form-row"><label class="label">字段：专业</label><input type="text" v-model="config.handover_log.other_important_work_section.sources.device_repair.fields.specialty" /></div>
          <div class="hint">4 张表按“楼栋包含命中 + 实际结束时间为空或落在本班次窗口内”汇总到其他重要工作记录。多楼一起执行时会按楼栋批量复用，最终顺序固定为：上电通告 -> 设备调整 -> 设备轮巡 -> 设备检修。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">长白岗来源（B4/F4）</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.shift_roster.long_day.enabled" /> 启用长白岗填充</label></div>
          <div class="form-row"><label class="label">多维 App Token（为空则复用上方）</label><input type="text" v-model="config.handover_log.shift_roster.long_day.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.shift_roster.long_day.source.table_id" /></div>
          <div class="form-row"><label class="label">字段：排班日期</label><input type="text" v-model="config.handover_log.shift_roster.long_day.fields.duty_date" /></div>
          <div class="form-row"><label class="label">字段：机楼</label><input type="text" v-model="config.handover_log.shift_roster.long_day.fields.building" /></div>
          <div class="form-row"><label class="label">字段：班次</label><input type="text" v-model="config.handover_log.shift_roster.long_day.fields.shift" /></div>
          <div class="form-row"><label class="label">字段：人员（文本）</label><input type="text" v-model="config.handover_log.shift_roster.long_day.fields.people_text" /></div>
          <div class="form-row"><label class="label">班次值关键字</label><input type="text" v-model="config.handover_log.shift_roster.long_day.shift_value" placeholder="例如 长白" /></div>
          <div class="form-row"><label class="label">白班填充单元格</label><input type="text" v-model="config.handover_log.shift_roster.long_day.day_cell" /></div>
          <div class="form-row"><label class="label">夜班填充单元格</label><input type="text" v-model="config.handover_log.shift_roster.long_day.night_cell" /></div>
          <div class="form-row"><label class="label">前缀</label><input type="text" v-model="config.handover_log.shift_roster.long_day.prefix" /></div>
          <div class="form-row"><label class="label">无记录文案</label><input type="text" v-model="config.handover_log.shift_roster.long_day.rest_text" /></div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card config-panel-card-wide config-editor-card">
        <div class="section-title">工程师目录来源（配置预览）</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.shift_roster.engineer_directory.enabled" /> 启用工程师目录读取</label></div>
          <div class="form-row"><label class="label">多维 App Token（为空则复用上方）</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.source.table_id" /></div>
          <div class="form-row"><label class="label">字段：机楼</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.fields.building" /></div>
          <div class="form-row"><label class="label">字段：专业</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.fields.specialty" /></div>
          <div class="form-row"><label class="label">字段：主管（文本）</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.fields.supervisor_text" /></div>
          <div class="form-row"><label class="label">字段：主管（人员）</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.fields.supervisor_person" /></div>
          <div class="form-row"><label class="label">字段：职位</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.fields.position" /></div>
          <div class="form-row"><label class="label">字段：飞书用户ID（可选）</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.fields.recipient_id" /></div>
          <div class="form-row">
            <label class="label">发送 receive_id_type</label>
            <select v-model="config.handover_log.shift_roster.engineer_directory.delivery.receive_id_type">
              <option value="open_id">open_id</option>
              <option value="user_id">user_id</option>
              <option value="email">email</option>
              <option value="mobile">mobile</option>
            </select>
          </div>
          <div class="form-row"><label class="label">发送职位关键字</label><input type="text" v-model="config.handover_log.shift_roster.engineer_directory.delivery.position_keyword" /></div>
          <div class="form-row">
            <label class="label">目标状态</label>
            <div class="hint" style="flex:1 1 auto;min-width:0;">{{ handoverEngineerDirectoryTarget.statusText }}</div>
          </div>
          <div class="form-row" v-if="handoverEngineerDirectoryTarget.displayUrl">
            <label class="label">工程师多维地址</label>
            <div class="hint" style="flex:1 1 auto;min-width:0;word-break:break-all;">
              <a :href="handoverEngineerDirectoryTarget.displayUrl" target="_blank" rel="noopener noreferrer">
                {{ handoverEngineerDirectoryTarget.displayUrl }}
              </a>
            </div>
          </div>
          <div class="btn-line" style="margin:8px 0;">
            <a
              v-if="handoverEngineerDirectoryTarget.displayUrl"
              class="btn btn-secondary"
              :href="handoverEngineerDirectoryTarget.displayUrl"
              target="_blank"
              rel="noopener noreferrer"
            >
              打开多维表
            </a>
            <button class="btn btn-secondary" :disabled="handoverEngineerLoading" @click="fetchHandoverEngineerDirectory({ forceRefresh: true })">
              {{ handoverEngineerLoading ? '读取中...' : '刷新工程师目录' }}
            </button>
            <span class="hint">{{ handoverEngineerDirectoryTarget.hintText || '用于查看各楼栋设施运维主管及其可直发飞书身份；如果主管字段是人员类型，会自动提取其中的飞书ID。' }}</span>
          </div>
          <div class="config-editor-scroll">
            <table class="site-table config-editor-table" style="margin-bottom:0;">
              <thead>
                <tr>
                  <th style="width:90px;">楼栋</th>
                  <th style="width:120px;">专业</th>
                  <th style="width:120px;">主管</th>
                  <th>职位</th>
                  <th>飞书用户ID</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in handoverEngineerDirectory" :key="'eng-row-' + idx">
                  <td>{{ row.building || '-' }}</td>
                  <td>{{ row.specialty || '-' }}</td>
                  <td>{{ row.supervisor || '-' }}</td>
                  <td>{{ row.position || '-' }}</td>
                  <td>{{ row.recipient_id || '-' }}</td>
                </tr>
                <tr v-if="!handoverEngineerDirectory.length" class="config-editor-empty-row">
                  <td colspan="5" class="hint">暂无工程师目录数据，点击“刷新工程师目录”读取。</td>
                </tr>
              </tbody>
            </table>
          </div>
      </section>
      </div>
    </div>
  </details>
  <details id="handover-config-output" class="config-group-panel" open>
    <summary class="config-group-summary">上报与同步</summary>
    <div class="config-group-body">
      <div class="config-panel-grid two-col">
      <section class="content-card config-panel-card config-subgroup-card config-panel-card-wide config-editor-card">
        <div class="section-title">白班指标上报多维</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.day_metric_export.enabled" /> 启用白班指标上报</label></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.day_metric_export.only_day_shift" /> 仅白班执行</label></div>
          <div class="form-row"><label class="label">多维 App Token</label><input type="text" v-model="config.handover_log.day_metric_export.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.day_metric_export.source.table_id" /></div>
          <div class="form-row"><label class="label">批量写入大小</label><input type="number" v-model.number="config.handover_log.day_metric_export.source.create_batch_size" /></div>
          <div class="form-row"><label class="label">字段：类型</label><input type="text" v-model="config.handover_log.day_metric_export.fields.type" /></div>
          <div class="form-row"><label class="label">字段：楼栋</label><input type="text" v-model="config.handover_log.day_metric_export.fields.building" /></div>
          <div class="form-row"><label class="label">字段：日期</label><input type="text" v-model="config.handover_log.day_metric_export.fields.date" /></div>
          <div class="form-row"><label class="label">字段：数值</label><input type="text" v-model="config.handover_log.day_metric_export.fields.value" /></div>
          <div class="form-row"><label class="label">缺失值策略</label><input type="text" v-model="config.handover_log.day_metric_export.missing_value_policy" /></div>
          <div class="btn-line" style="margin:8px 0;">
            <button class="btn btn-secondary" @click="config.handover_log.day_metric_export.types.push({ name: '', source: 'cell', cell: '', metric_id: '' })">新增指标类型</button>
          </div>
          <div class="config-editor-scroll">
            <table class="site-table config-editor-table" style="margin-bottom:0;">
              <thead>
                <tr>
                  <th style="width:220px;">类型名称</th>
                  <th style="width:140px;">来源</th>
                  <th style="width:140px;">单元格</th>
                  <th style="width:200px;">规则ID(metric_id)</th>
                  <th style="width:90px;">操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in (config.handover_log.day_metric_export.types || [])" :key="'day-metric-type-' + idx">
                  <td><input type="text" v-model="row.name" /></td>
                  <td>
                    <select v-model="row.source">
                      <option value="cell">cell</option>
                      <option value="metric">metric</option>
                      <option value="cell_percent">cell_percent</option>
                      <option value="cell_min_pair">cell_min_pair</option>
                    </select>
                  </td>
                  <td>
                    <input
                      type="text"
                      v-model="row.cell"
                      :disabled="row.source === 'metric'"
                      placeholder="如 D6"
                    />
                  </td>
                  <td>
                    <input
                      type="text"
                      v-model="row.metric_id"
                      :disabled="row.source !== 'metric'"
                      placeholder="如 cold_temp_max"
                    />
                  </td>
                  <td><button class="btn btn-danger" @click="config.handover_log.day_metric_export.types.splice(idx, 1)">删除</button></td>
                </tr>
                <tr v-if="!(config.handover_log.day_metric_export.types || []).length" class="config-editor-empty-row">
                  <td colspan="5" class="hint">暂无指标类型，请点击“新增指标类型”。</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="hint" style="margin-bottom:8px;">仅白班交接班生成成功后追加写入，不删旧记录；数值缺失按 0 写入。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">确认后源数据附件上传</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.source_data_attachment_export.enabled" /> 启用源数据附件上传</label></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.source_data_attachment_export.upload_night_shift" /> 夜班也上传附件</label></div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.source_data_attachment_export.replace_existing" /> 覆盖同楼同日同班旧记录</label></div>
          <div class="form-row"><label class="label">多维 App Token</label><input type="text" v-model="config.handover_log.source_data_attachment_export.source.app_token" /></div>
          <div class="form-row"><label class="label">多维 Table ID</label><input type="text" v-model="config.handover_log.source_data_attachment_export.source.table_id" /></div>
          <div class="form-row"><label class="label">分页大小</label><input type="number" v-model.number="config.handover_log.source_data_attachment_export.source.page_size" /></div>
          <div class="form-row"><label class="label">最多读取记录数</label><input type="number" v-model.number="config.handover_log.source_data_attachment_export.source.max_records" /></div>
          <div class="form-row"><label class="label">删除批大小</label><input type="number" v-model.number="config.handover_log.source_data_attachment_export.source.delete_batch_size" /></div>
          <div class="form-row"><label class="label">字段：类型</label><input type="text" v-model="config.handover_log.source_data_attachment_export.fields.type" /></div>
          <div class="form-row"><label class="label">字段：楼栋</label><input type="text" v-model="config.handover_log.source_data_attachment_export.fields.building" /></div>
          <div class="form-row"><label class="label">字段：日期</label><input type="text" v-model="config.handover_log.source_data_attachment_export.fields.date" /></div>
          <div class="form-row"><label class="label">字段：班次</label><input type="text" v-model="config.handover_log.source_data_attachment_export.fields.shift" /></div>
          <div class="form-row"><label class="label">字段：附件</label><input type="text" v-model="config.handover_log.source_data_attachment_export.fields.attachment" /></div>
          <div class="form-row"><label class="label">固定类型文案</label><input type="text" v-model="config.handover_log.source_data_attachment_export.fixed_values.type" /></div>
          <div class="form-row"><label class="label">白班文案</label><input type="text" v-model="config.handover_log.source_data_attachment_export.fixed_values.shift_text.day" /></div>
          <div class="form-row"><label class="label">夜班文案</label><input type="text" v-model="config.handover_log.source_data_attachment_export.fixed_values.shift_text.night" /></div>
          <div class="hint" style="margin-bottom:8px;">五楼全部确认后上传各楼用于生成交接班的源数据表附件；该上传独立于12项数值上传，支持白班和夜班。</div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">云文档同步</div>
          <div class="form-row"><label><input type="checkbox" v-model="config.handover_log.cloud_sheet_sync.enabled" /> 启用云文档同步</label></div>
          <div class="form-row"><label class="label">根 Wiki URL</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.root_wiki_url" /></div>
          <div class="form-row"><label class="label">模板 Node Token</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.template_node_token" /></div>
          <div class="form-row"><label class="label">云表名称模板</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.spreadsheet_name_pattern" /></div>
          <div class="form-row"><label class="label">源 Sheet 名</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.source_sheet_name" /></div>
          <div class="form-row"><label class="label">A楼 Sheet 名</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.sheet_names['A楼']" /></div>
          <div class="form-row"><label class="label">B楼 Sheet 名</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.sheet_names['B楼']" /></div>
          <div class="form-row"><label class="label">C楼 Sheet 名</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.sheet_names['C楼']" /></div>
          <div class="form-row"><label class="label">D楼 Sheet 名</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.sheet_names['D楼']" /></div>
          <div class="form-row"><label class="label">E楼 Sheet 名</label><input type="text" v-model="config.handover_log.cloud_sheet_sync.sheet_names['E楼']" /></div>
          <div class="form-row"><label class="label">请求超时（秒）</label><input type="number" v-model.number="config.handover_log.cloud_sheet_sync.request.timeout_sec" /></div>
          <div class="form-row"><label class="label">重试次数</label><input type="number" v-model.number="config.handover_log.cloud_sheet_sync.request.max_retries" /></div>
          <div class="form-row"><label class="label">重试退避（秒）</label><input type="number" step="0.5" v-model.number="config.handover_log.cloud_sheet_sync.request.retry_backoff_sec" /></div>
          <div class="hint" style="margin-bottom:8px;">下载完内网源表并切回外网后，系统只会为当前日期班次预创建一份云电子表格，并确保存在 A-E 楼 5 个固定 Sheet。五楼全部确认后，系统会把每个楼本地交接班成品 Excel 的“交接班日志”页覆盖写入同名 Sheet。审核保存本身不会立即同步云表；若最终上传失败，可按楼重试，也可一键重试全部失败楼栋。</div>
      </section>
      </div>
    </div>
  </details>
  <details id="handover-config-review" class="config-group-panel" open>
    <summary class="config-group-summary">审核与模板规则</summary>
    <div class="config-group-body">
      <div class="config-panel-grid two-col">
      <section class="content-card config-panel-card config-subgroup-card">
        <div class="section-title">审核页外部访问地址</div>
          <div class="hint" v-if="health.handover.review_base_url_effective">
            当前生效地址（{{ health.handover.review_base_url_effective_source === 'manual' ? '手工指定' : '已缓存自动诊断结果' }}）：{{ health.handover.review_base_url_effective }}
          </div>
          <div class="hint" v-if="health.handover.review_base_url_error">
            {{ health.handover.review_base_url_error }}
          </div>
          <div class="form-row">
            <label class="label">审核页访问基地址（手工指定后立即生效）</label>
            <input
              type="text"
              v-model="config.handover_log.review_ui.public_base_url"
              placeholder="例如 http://192.168.220.160:18765"
            />
          </div>
          <div class="btn-line" style="margin-bottom:8px;">
            <button
              class="btn btn-secondary"
              :disabled="isActionLocked(actionKeyHandoverReviewAccessReprobe)"
              @click="reprobeHandoverReviewAccess"
            >
              {{ isActionLocked(actionKeyHandoverReviewAccessReprobe) ? '探测中...' : '重新探测审核访问地址' }}
            </button>
          </div>
          <div class="hint">当前审核访问地址已持久化；外网端启动后会基于真实审核页访问结果进行校验。</div>
          <div class="hint" v-if="health.handover.review_base_url_effective_source === 'auto'">
            当前自动地址来自已缓存的历史诊断结果；只有手动点击“重新探测审核访问地址”才会再次探测。
          </div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card config-panel-card-wide config-editor-card">
        <div class="section-title">模板楼栋标题（A1）</div>
          <div class="form-row">
            <label><input type="checkbox" v-model="config.handover_log.template.apply_building_title" /> 启用按楼栋写入标题</label>
          </div>
          <div class="form-row"><label class="label">标题单元格</label><input type="text" v-model="config.handover_log.template.title_cell" placeholder="例如 A1" /></div>
          <div class="form-row"><label class="label">兜底标题模板</label><input type="text" v-model="config.handover_log.template.building_title_pattern" placeholder="例如 EA118机房{building_code}栋数据中心交接班日志" /></div>
          <div class="config-editor-scroll">
            <table class="site-table config-editor-table" style="margin-bottom:0;">
              <thead>
                <tr>
                  <th style="width:90px;">楼栋</th>
                  <th>标题文案</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>A楼</td>
                  <td><input type="text" v-model="config.handover_log.template.building_title_map['A楼']" /></td>
                </tr>
                <tr>
                  <td>B楼</td>
                  <td><input type="text" v-model="config.handover_log.template.building_title_map['B楼']" /></td>
                </tr>
                <tr>
                  <td>C楼</td>
                  <td><input type="text" v-model="config.handover_log.template.building_title_map['C楼']" /></td>
                </tr>
                <tr>
                  <td>D楼</td>
                  <td><input type="text" v-model="config.handover_log.template.building_title_map['D楼']" /></td>
                </tr>
                <tr>
                  <td>E楼</td>
                  <td><input type="text" v-model="config.handover_log.template.building_title_map['E楼']" /></td>
                </tr>
              </tbody>
            </table>
          </div>
      </section>

      <section class="content-card config-panel-card config-subgroup-card config-panel-card-wide config-editor-card">
        <div class="section-title">单元格对应关系（可新增/删除/修改）</div>
          <div class="hint">删除楼栋覆盖只影响当前楼栋，保存后生效。关键词按 D 列模糊匹配（大小写不敏感）。单元格留空可作为中间变量。</div>
        
          <div class="form-row">
            <label class="label">作用域</label>
            <select v-model="handoverRuleScope">
              <option v-for="opt in handoverRuleScopeOptions" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
            </select>
          </div>
        
          <div class="btn-line" style="margin-bottom:8px;">
            <button class="btn btn-secondary" @click="addHandoverRuleRow">新增行</button>
            <button class="btn btn-secondary" v-if="handoverRuleScope !== 'default'" @click="copyAllDefaultRulesToCurrentBuilding">复制全局规则到当前楼栋</button>
            <button class="btn btn-danger" v-if="handoverRuleScope !== 'default'" @click="clearCurrentBuildingOverrides">清空当前楼栋覆盖</button>
          </div>
        
          <div class="config-editor-scroll">
            <table class="site-table rule-table config-editor-table">
              <thead>
                <tr>
                  <th>启用</th>
                  <th>规则ID</th>
                  <th>交接班单元格</th>
                  <th>规则类型</th>
                  <th>D列关键词（逗号分隔）</th>
                  <th>聚合方式</th>
                  <th>模板</th>
                  <th>计算类型</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, idx) in getActiveHandoverRuleRows()" :key="'handover-rule-' + idx">
                  <td><input type="checkbox" v-model="row.enabled" /></td>
                  <td><input type="text" v-model="row.id" placeholder="例如 city_power" /></td>
                  <td><input type="text" v-model="row.target_cell" placeholder="如 D6" /></td>
                  <td>
                    <select v-model="row.rule_type">
                      <option value="direct">直接取值</option>
                      <option value="aggregate">聚合取值</option>
                      <option value="computed">计算规则</option>
                    </select>
                  </td>
                  <td>
                    <input
                      type="text"
                      :value="(row.d_keywords || []).join(', ')"
                      @input="updateHandoverRuleKeywords(row, $event.target.value)"
                      placeholder="例如 市电进线总功率,市电总功率"
                    />
                  </td>
                  <td>
                    <select v-model="row.agg">
                      <option value="first">首条</option>
                      <option value="max">最大值</option>
                      <option value="min">最小值</option>
                    </select>
                  </td>
                  <td><input type="text" v-model="row.template" /></td>
                  <td>
                    <select
                      v-if="row.rule_type === 'computed'"
                      :value="getHandoverComputedPreset(row)"
                      @change="onHandoverComputedPresetChange(row, $event.target.value)"
                    >
                      <option value="chiller_mode_summary">冷机模式汇总（F7）</option>
                      <option value="ring_supply_temp">供水温度汇总（H7）</option>
                      <option value="tank_backup">蓄冷时间汇总（F8）</option>
                      <option value="__expr__">表达式计算（自定义）</option>
                    </select>
                    <input
                      v-if="row.rule_type === 'computed' && getHandoverComputedPreset(row) === '__expr__'"
                      type="text"
                      v-model="row.computed_op"
                      placeholder="例如 number_js/3.14"
                    />
                    <input
                      v-if="row.rule_type !== 'computed'"
                      type="text"
                      v-model="row.computed_op"
                      disabled
                      placeholder="仅计算规则可填写"
                    />
                  </td>
                  <td>
                    <div class="btn-line">
                      <button class="btn btn-danger" @click="removeHandoverRuleRow(idx)">删除</button>
                      <button class="btn btn-ghost" v-if="handoverRuleScope !== 'default'" @click="restoreDefaultRuleForCurrentBuilding(row.id)">恢复全局</button>
                    </div>
                  </td>
                </tr>
                <tr v-if="!getActiveHandoverRuleRows().length" class="config-editor-empty-row">
                  <td colspan="9" class="hint">当前作用域暂无规则，点击“新增行”开始配置。</td>
                </tr>
              </tbody>
            </table>
          </div>
      </section>
      </div>
    </div>
  </details>
</div>
`;



