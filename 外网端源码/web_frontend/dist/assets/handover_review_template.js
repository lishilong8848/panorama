export const HANDOVER_REVIEW_TEMPLATE = `
  <div class="review-shell">
    <header class="review-header review-header-sticky">
      <div class="review-header-main">
        <div class="review-header-copy">
          <h1 class="review-title">{{ building || "交接班审核" }}</h1>
          <p class="review-subtitle">{{ sessionSummary }}</p>
        </div>
        <div class="review-header-actions">
          <button v-if="showRefreshAction" class="btn btn-secondary btn-mini" @click="refreshData" :disabled="refreshActionVm.disabled" :title="refreshActionVm.disabledReason || ''">
            {{ refreshActionVm.text }}
          </button>
          <button
            v-if="showSaveAction"
            class="btn btn-primary btn-mini"
            @click="saveCurrentReview"
            :disabled="saveActionVm.disabled"
            :title="saveActionVm.disabledReason || ''"
          >
            {{ saveActionVm.text }}
          </button>
          <button
            v-if="showDownloadAction"
            class="btn btn-secondary btn-mini"
            @click="downloadCurrentReviewFile"
            :disabled="downloadActionVm.disabled"
            :title="downloadActionVm.disabledReason || ''"
          >
            {{ downloadActionVm.text }}
          </button>
          <button
            v-if="showCapacityDownloadAction"
            class="btn btn-secondary btn-mini"
            @click="downloadCurrentCapacityReviewFile"
            :disabled="capacityDownloadDisabled"
            :title="capacityDownloadActionVm.disabledReason || ''"
          >
            {{ capacityDownloadActionVm.text }}
          </button>
          <button
            v-if="showCapacityImageSendAction"
            class="btn btn-secondary btn-mini"
            @click="sendCurrentCapacityImage"
            :disabled="capacityImageSendActionVm.disabled"
            :title="capacityImageSendActionVm.disabledReason || ''"
          >
            {{ capacityImageSendActionVm.text }}
          </button>
          <button
            v-if="showRegenerateAction"
            class="btn btn-warning btn-mini"
            @click="regenerateCurrentReview"
            :disabled="regenerateActionVm.disabled"
            :title="regenerateActionVm.disabledReason || ''"
          >
            {{ regenerateActionVm.text }}
          </button>
          <a
            v-if="reviewCloudSheetUrl"
            class="btn btn-secondary btn-mini"
            :class="{ 'is-disabled': regenerating }"
            :href="reviewCloudSheetUrl"
            target="_blank"
            rel="noopener noreferrer"
            :aria-disabled="regenerating ? 'true' : 'false'"
            @click="regenerating && $event.preventDefault()"
          >
            打开云文档
          </a>
          <button
            v-if="showConfirmAction"
            class="btn"
            :class="'btn-' + confirmActionVm.variant"
            @click="toggleConfirm"
            :disabled="confirmActionVm.disabled"
            :title="confirmActionVm.disabledReason || ''"
          >
            {{ confirmActionVm.text }}
          </button>
        </div>
      </div>

      <div class="review-meta-row review-meta-row-rich">
        <span
          v-for="(badge, index) in reviewHeaderBadges"
          :key="'review-badge-' + index"
          class="status-badge"
          :class="[
            badge.emphasis === 'solid' ? 'status-badge-solid' : badge.emphasis === 'outline' ? 'status-badge-outline' : 'status-badge-soft',
            'tone-' + badge.tone,
            'icon-' + badge.icon,
          ]"
        >
          {{ badge.text }}
        </span>
      </div>

      <div
        v-for="(banner, index) in reviewStatusBanners"
        :key="'review-banner-' + index"
        class="review-status-line"
        :class="'review-status-' + banner.tone"
      >
        {{ banner.text }}
      </div>
    </header>

    <section class="review-current-view-section">
      <article class="review-card">
        <div class="review-card-head">
          <h2>当前查看</h2>
        </div>
        <div class="review-fixed-fields review-current-view-fields">
          <label class="review-field">
            <span class="review-field-label">日期</span>
            <input class="review-input" :value="currentDutyDateText" readonly />
          </label>
          <label class="review-field">
            <span class="review-field-label">班次</span>
            <input class="review-input" :value="currentDutyShiftText" readonly />
          </label>
          <label class="review-field">
            <span class="review-field-label">模式</span>
            <input class="review-input" :value="currentModeText" readonly />
          </label>
          <label class="review-field review-field-wide">
            <span class="review-field-label">历史交接班日志</span>
            <div class="review-history-control">
              <select
                class="review-input"
                :value="selectedSessionIdInListOrEmpty"
                @focus="ensureHistoryLoaded()"
                @mousedown="ensureHistoryLoaded()"
                @change="onHistorySelectionChange($event.target.value)"
                :disabled="loading || saving || regenerating || confirming || cloudSyncBusy || historyLoading"
              >
                <option v-if="historyLoading" value="" disabled>
                  正在加载历史交接班日志...
                </option>
                <option v-else-if="!historySessions.length" value="" disabled>
                  {{ selectedSessionInHistoryList ? "暂无符合条件的历史交接班日志" : "展开后加载历史交接班日志" }}
                </option>
                <option v-else-if="!selectedSessionInHistoryList" value="" disabled>
                  当前记录未进入历史列表
                </option>
                <option v-for="item in historySessions" :key="item.session_id" :value="item.session_id">
                  {{ item.label }}
                </option>
              </select>
              <button
                v-if="showReturnToLatestAction"
                class="btn btn-secondary btn-mini review-history-return"
                @click="returnToLatestSession"
                :disabled="returnToLatestActionVm.disabled"
                :title="returnToLatestActionVm.disabledReason || ''"
              >
                {{ returnToLatestActionVm.text }}
              </button>
            </div>
            <small class="review-field-hint">{{ historySelectorHint }}</small>
          </label>
        </div>
      </article>
    </section>

    <section v-if="loading" class="review-empty-card">
      正在加载交接班审核内容...
    </section>

    <section v-else-if="!session" class="review-empty-card">
      <strong>{{ reviewPendingTitle }}</strong>
      <span>{{ reviewPendingMessage }}</span>
      <button
        v-if="showRegenerateAction"
        class="btn btn-warning btn-mini"
        @click="regenerateCurrentReview"
        :disabled="regenerateActionVm.disabled"
        :title="regenerateActionVm.disabledReason || ''"
      >
        {{ regenerateActionVm.text }}
      </button>
    </section>

    <template v-else>
      <section class="review-shared-grid">
        <article class="review-card review-substation-card">
          <div class="review-card-head">
            <div>
              <h2>110KV变电站</h2>
              <p class="review-card-subtitle">{{ substation110kvMetaText }}</p>
            </div>
            <span
              v-if="substation110kvLockText"
              class="status-badge status-badge-outline"
              :class="substation110kvLockedByOther ? 'tone-warning' : 'tone-info'"
            >
              {{ substation110kvLockText }}
            </span>
          </div>
          <div class="review-table-wrap">
            <table class="review-table review-substation-table" @paste.prevent="pasteSubstation110kvTable">
              <thead>
                <tr>
                  <th>进线/主变</th>
                  <th>线电压</th>
                  <th>电流/输出电流</th>
                  <th>当前功率KW</th>
                  <th>功率因数</th>
                  <th>负载率</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, rowIndex) in substation110kvBlock.rows" :key="row.row_id">
                  <th>{{ row.label }}</th>
                  <td v-for="column in substation110kvBlock.columns" :key="row.row_id + ':' + column.key">
                    <input
                      class="review-input review-compact-input"
                      :value="row[column.key]"
                      :disabled="substation110kvReadonly"
                      @focus="ensureSubstation110kvLock"
                      @input="updateSubstation110kvCell(rowIndex, column.key, $event.target.value)"
                      @change="updateSubstation110kvCell(rowIndex, column.key, $event.target.value)"
                    />
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </article>

        <article class="review-card review-pump-card">
          <div class="review-card-head">
            <div>
              <h2>冷却水泵压力</h2>
              <p class="review-card-subtitle">按当前运行制冷单元填写，下次同楼同区同单元自动带出。</p>
            </div>
          </div>
          <div v-if="!coolingPumpPressureRows.length" class="review-empty-inline">
            当前容量表未识别到运行制冷单元
          </div>
          <div v-else class="review-fixed-fields review-pump-fields">
            <label
              v-for="(row, rowIndex) in coolingPumpPressureRows"
              :key="row.row_id || (row.zone + ':' + row.unit)"
              class="review-field review-pump-field"
            >
              <span class="review-field-label">{{ row.zone_label }} {{ row.unit_label }} 进水压力</span>
              <input
                class="review-input"
                :value="row.inlet_pressure"
                :disabled="regenerating"
                @input="updateCoolingPumpPressure(rowIndex, 'inlet_pressure', $event.target.value)"
                @change="updateCoolingPumpPressure(rowIndex, 'inlet_pressure', $event.target.value)"
              />
              <span class="review-field-label">{{ row.zone_label }} {{ row.unit_label }} 出水压力</span>
              <input
                class="review-input"
                :value="row.outlet_pressure"
                :disabled="regenerating"
                @input="updateCoolingPumpPressure(rowIndex, 'outlet_pressure', $event.target.value)"
                @change="updateCoolingPumpPressure(rowIndex, 'outlet_pressure', $event.target.value)"
              />
            </label>
          </div>
          <div class="review-subsection-head">
            <h3>冷塔及蓄冷罐液位温度</h3>
          </div>
          <div v-if="!coolingPumpPressureRows.length" class="review-empty-inline">
            当前容量表未识别到运行制冷单元，冷却塔液位可在识别后填写
          </div>
          <div v-else class="review-fixed-fields review-pump-fields">
            <label
              v-for="(row, rowIndex) in coolingPumpPressureRows"
              :key="'tower:' + (row.row_id || (row.zone + ':' + row.unit))"
              class="review-field review-pump-field"
            >
              <span class="review-field-label">{{ row.zone_label }} {{ row.unit_label }} 冷却塔液位</span>
              <input
                class="review-input"
                :value="row.cooling_tower_level"
                :disabled="regenerating"
                @input="updateCoolingTowerLevel(rowIndex, $event.target.value)"
                @change="updateCoolingTowerLevel(rowIndex, $event.target.value)"
              />
            </label>
          </div>
          <div class="review-fixed-fields review-pump-fields">
            <label
              v-for="tank in coolingTankRows"
              :key="'tank:' + tank.zone"
              class="review-field review-pump-field"
            >
              <span class="review-field-label">{{ tank.zone_label }}蓄冷罐温度</span>
              <input
                class="review-input"
                :value="tank.temperature"
                :disabled="regenerating"
                @input="updateCoolingTankValue(tank.zone, 'temperature', $event.target.value)"
                @change="updateCoolingTankValue(tank.zone, 'temperature', $event.target.value)"
              />
              <span class="review-field-label">{{ tank.zone_label }}蓄冷罐液位</span>
              <input
                class="review-input"
                :value="tank.level"
                :disabled="regenerating"
                @input="updateCoolingTankValue(tank.zone, 'level', $event.target.value)"
                @change="updateCoolingTankValue(tank.zone, 'level', $event.target.value)"
              />
            </label>
          </div>
        </article>
      </section>

      <section v-if="document.capacity_room_inputs && document.capacity_room_inputs.rows && document.capacity_room_inputs.rows.length" class="review-sections">
        <article class="review-card review-section-card">
          <div class="review-card-head">
            <div>
              <h2>{{ document.capacity_room_inputs.title || "M1-M6包间机柜与空调启动台数" }}</h2>
              <p class="review-card-subtitle">保存后会作为本楼下次生成容量表的默认值，并同步补写当前容量表。</p>
            </div>
          </div>
          <div class="review-table-wrap capacity-room-table-wrap">
            <table class="review-table capacity-room-table">
              <thead>
                <tr>
                  <th>包间</th>
                  <th>总机柜数</th>
                  <th>上电机柜数</th>
                  <th>空调启动台数</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, rowIndex) in document.capacity_room_inputs.rows" :key="row.room + ':' + row.row">
                  <td>
                    <strong>{{ row.label || row.room }}</strong>
                    <small>{{ row.total_cell }} / {{ row.powered_cell }} / {{ row.aircon_cell }}</small>
                  </td>
                  <td>
                    <input
                      class="review-input"
                      :value="row.total_cabinets"
                      :disabled="regenerating"
                      @input="updateCapacityRoomInput(rowIndex, 'total_cabinets', $event.target.value)"
                      @change="updateCapacityRoomInput(rowIndex, 'total_cabinets', $event.target.value)"
                    />
                  </td>
                  <td>
                    <input
                      class="review-input"
                      :value="row.powered_cabinets"
                      :disabled="regenerating"
                      @input="updateCapacityRoomInput(rowIndex, 'powered_cabinets', $event.target.value)"
                      @change="updateCapacityRoomInput(rowIndex, 'powered_cabinets', $event.target.value)"
                    />
                  </td>
                  <td>
                    <input
                      class="review-input"
                      :value="row.aircon_started"
                      :disabled="regenerating"
                      @input="updateCapacityRoomInput(rowIndex, 'aircon_started', $event.target.value)"
                      @change="updateCapacityRoomInput(rowIndex, 'aircon_started', $event.target.value)"
                    />
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </article>
      </section>

      <section class="review-fixed-grid">
        <article v-for="(block, blockIndex) in document.fixed_blocks" :key="block.id || blockIndex" class="review-card">
          <div class="review-card-head">
            <h2>{{ block.title }}</h2>
          </div>
          <div class="review-fixed-fields">
            <label
              v-for="(field, fieldIndex) in block.fields"
              :key="field.cell + ':' + fieldIndex"
              class="review-field"
            >
              <span class="review-field-label">{{ field.label }} <small>{{ field.cell }}</small></span>
              <input
                class="review-input"
                :value="field.value"
                :disabled="regenerating"
                @input="updateFixedField(blockIndex, fieldIndex, $event.target.value)"
                @change="updateFixedField(blockIndex, fieldIndex, $event.target.value)"
              />
            </label>
          </div>
        </article>
      </section>

      <section class="review-sections">
        <article
          v-for="(section, sectionIndex) in document.sections"
          :key="section.name + ':' + sectionIndex"
          class="review-card review-section-card"
        >
          <div class="review-card-head">
            <div>
              <h2>{{ section.name }}</h2>
              <p class="review-card-subtitle">支持新增、删除、修改本分类内容，保存后回写 Excel。</p>
            </div>
            <div class="review-card-actions">
              <button
                v-if="isEventRefreshSection(section)"
                class="btn btn-secondary btn-mini"
                @click="refreshEventSectionFromBitable(section.name)"
                :disabled="eventSectionsRefreshing || saving || regenerating || syncingRemoteRevision"
              >
                {{ eventSectionsRefreshing ? "刷新中..." : "刷新多维" }}
              </button>
              <details v-if="findSectionPersonColumn(section)" class="person-fill-toolbar" @toggle="ensureEngineerDirectoryLoaded">
                <summary>批量填人</summary>
                <div class="person-fill-panel">
                  <select
                    class="review-person-select"
                    multiple
                    :value="selectedSectionPersonValues(sectionIndex)"
                    @focus="ensureEngineerDirectoryLoaded"
                    @mousedown="ensureEngineerDirectoryLoaded"
                    @change="updateSectionPersonSelection(sectionIndex, $event)"
                    :disabled="regenerating || sectionPersonOptions.length === 0"
                  >
                    <option
                      v-for="person in sectionPersonOptions"
                      :key="section.name + ':bulk:' + person.key"
                      :value="person.name"
                      :selected="selectedSectionPersonValues(sectionIndex).includes(person.name)"
                    >
                      {{ person.label }}
                    </option>
                  </select>
                  <button
                    class="btn btn-secondary btn-mini"
                    @click="fillSectionPeople(sectionIndex)"
                    :disabled="regenerating || !selectedSectionPersonValues(sectionIndex).length"
                  >
                    填入本分类
                  </button>
                </div>
                <small v-if="engineerDirectoryLoading" class="person-picker-hint">正在读取工程师目录...</small>
                <small v-else-if="engineerDirectoryError" class="person-picker-hint tone-danger">{{ engineerDirectoryError }}</small>
              </details>
              <button class="btn btn-secondary btn-mini" @click="addSectionRow(sectionIndex)" :disabled="regenerating">
                新增一行
              </button>
            </div>
          </div>
          <div class="review-table-wrap">
            <table class="review-table">
              <thead>
                <tr>
                  <th class="review-col-index">序号</th>
                  <th v-for="(column, columnIndex) in section.columns" :key="section.name + '-head-' + column.key + '-' + columnIndex">
                    {{ column.label || column.key }}
                  </th>
                  <th class="review-col-action">操作</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="(row, rowIndex) in section.rows"
                  :key="row.row_id || (section.name + ':' + rowIndex)"
                  :class="{ 'review-row-placeholder': row.is_placeholder_row }"
                >
                  <td class="review-col-index">{{ rowIndex + 1 }}</td>
                  <td v-for="(column, columnIndex) in section.columns" :key="section.name + ':' + rowIndex + ':' + column.key + ':' + columnIndex">
                    <textarea
                      class="review-cell-input"
                      rows="1"
                      :value="row.cells[column.key] || ''"
                      :disabled="regenerating"
                      @input="updateSectionCell(sectionIndex, rowIndex, column.key, $event.target.value)"
                      @change="updateSectionCell(sectionIndex, rowIndex, column.key, $event.target.value)"
                    ></textarea>
                    <details v-if="isSectionPersonColumn(section, column) && sectionPersonOptions.length" class="person-picker-details" @toggle="ensureEngineerDirectoryLoaded">
                      <summary>选择人名</summary>
                      <div class="person-chip-row">
                        <button
                          v-for="person in sectionPersonOptions"
                          :key="section.name + ':' + rowIndex + ':' + column.key + ':' + person.key"
                          class="person-chip"
                          :class="{ 'is-active': sectionPersonActive(row, column.key, person.name) }"
                          @click="toggleSectionPerson(sectionIndex, rowIndex, column.key, person.name)"
                          :disabled="regenerating"
                        >
                          {{ person.name }}
                        </button>
                      </div>
                      <small v-if="engineerDirectoryLoading" class="person-picker-hint">正在读取工程师目录...</small>
                      <small v-else-if="engineerDirectoryError" class="person-picker-hint tone-danger">{{ engineerDirectoryError }}</small>
                    </details>
                  </td>
                  <td class="review-col-action">
                    <div class="review-row-actions">
                      <button
                        v-if="canTransferSectionRowToOtherImportantWork(section, row)"
                        class="btn btn-secondary btn-mini"
                        @click="transferSectionRowToOtherImportantWork(sectionIndex, rowIndex)"
                        :disabled="regenerating"
                      >
                        转到其他重要工作记录
                      </button>
                      <button class="btn btn-danger btn-mini" @click="removeSectionRow(sectionIndex, rowIndex)" :disabled="regenerating">
                        删除
                      </button>
                    </div>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </article>
      </section>

      <section v-if="document.footer_blocks && document.footer_blocks.length" class="review-footer-blocks">
        <article
          v-for="(block, blockIndex) in document.footer_blocks"
          :key="block.id || blockIndex"
          class="review-card review-footer-card"
        >
          <template v-if="block.type === 'inventory_table'">
            <div class="review-card-head">
              <div>
                <h2>{{ block.title }}</h2>
                <p class="review-card-subtitle">{{ block.group_title }}</p>
              </div>
              <button class="btn btn-secondary btn-mini" @click="addFooterRow(blockIndex)" :disabled="regenerating">
                新增一行
              </button>
            </div>
            <div class="review-table-wrap">
              <table class="review-table review-footer-table review-inventory-table">
                <thead>
                  <tr>
                    <th class="review-col-index">序号</th>
                    <th v-for="(column, columnIndex) in block.columns" :key="block.id + '-head-' + column.key + '-' + columnIndex">
                      {{ column.label || column.key }}
                    </th>
                    <th class="review-col-action">操作</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="(row, rowIndex) in block.rows"
                    :key="row.row_id || (block.id + ':' + rowIndex)"
                    :class="{ 'review-row-placeholder': row.is_placeholder_row }"
                  >
                    <td class="review-col-index">{{ rowIndex + 1 }}</td>
                    <td v-for="(column, columnIndex) in block.columns" :key="block.id + ':' + rowIndex + ':' + column.key + ':' + columnIndex">
                      <textarea
                        class="review-cell-input"
                        rows="1"
                        :value="row.cells[column.key] || ''"
                        :disabled="regenerating"
                        @input="updateFooterCell(blockIndex, rowIndex, column.key, $event.target.value)"
                        @change="updateFooterCell(blockIndex, rowIndex, column.key, $event.target.value)"
                      ></textarea>
                      <details v-if="isFooterHandoverPersonColumn(column) && handoverPersonOptions.length" class="person-picker-details">
                        <summary>选择接班人</summary>
                        <div class="person-chip-row">
                          <button
                            v-for="person in handoverPersonOptions"
                            :key="block.id + ':' + rowIndex + ':' + column.key + ':' + person.key"
                            class="person-chip"
                            :class="{ 'is-active': footerPersonActive(row, column.key, person.name) }"
                            @click="toggleFooterHandoverPerson(blockIndex, rowIndex, column.key, person.name)"
                            :disabled="regenerating"
                          >
                            {{ person.name }}
                          </button>
                        </div>
                      </details>
                    </td>
                    <td class="review-col-action">
                      <button class="btn btn-danger btn-mini" @click="removeFooterRow(blockIndex, rowIndex)" :disabled="regenerating">
                        删除
                      </button>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
          <template v-else>
            <div class="review-card-head">
              <h2>{{ block.title }}</h2>
            </div>
            <div class="review-table-wrap">
              <table class="review-table review-footer-table">
                <tbody>
                  <tr v-for="(row, rowIndex) in block.rows" :key="row.row_key || (block.id + ':' + rowIndex)">
                    <td
                      v-for="(cell, cellIndex) in row.cells"
                      :key="cell.cell_key || (row.row_key + ':' + cellIndex)"
                      :colspan="cell.colspan || 1"
                    >
                      <div class="review-footer-cell-text">{{ cell.value }}</div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </article>
      </section>
    </template>
  </div>
`;
