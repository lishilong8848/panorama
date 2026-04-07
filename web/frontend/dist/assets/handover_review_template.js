export const HANDOVER_REVIEW_TEMPLATE = `
  <div class="review-shell">
    <header class="review-header review-header-sticky">
      <div class="review-header-main">
        <div class="review-header-copy">
          <h1 class="review-title">{{ building || "交接班审核" }}</h1>
          <p class="review-subtitle">{{ sessionSummary }}</p>
        </div>
        <div class="review-header-actions">
          <button class="btn btn-secondary btn-mini" @click="refreshData" :disabled="loading || saving || confirming || cloudSyncBusy">
            刷新
          </button>
          <button
            class="btn btn-secondary btn-mini"
            @click="downloadCurrentReviewFile"
            :disabled="loading || saving || downloading || cloudSyncBusy || !session || !session.session_id"
          >
            {{ downloading ? "下载中..." : "下载交接班日志" }}
          </button>
          <button
            class="btn btn-secondary btn-mini"
            @click="downloadCurrentCapacityReviewFile"
            :disabled="loading || saving || capacityDownloading || cloudSyncBusy || !session || !session.session_id || !session.capacity_output_file"
          >
            {{ capacityDownloading ? "下载中..." : "下载交接班容量报表" }}
          </button>
          <a
            v-if="reviewCloudSheetUrl"
            class="btn btn-secondary btn-mini"
            :href="reviewCloudSheetUrl"
            target="_blank"
            rel="noopener noreferrer"
          >
            打开云文档
          </a>
          <button
            v-if="canRetryCloudSync"
            class="btn btn-warning btn-mini"
            @click="retryCloudSheetSync"
            :disabled="loading || saving || confirming || cloudSyncBusy"
          >
            {{ retryingCloudSync ? "重试上传中..." : "重试云表上传" }}
          </button>
          <button
            v-if="isHistoryMode"
            class="btn btn-warning btn-mini"
            @click="updateHistoryCloudSync"
            :disabled="!canUpdateHistoryCloudSync"
          >
            {{ updatingHistoryCloudSync ? "更新中..." : "更新云文档" }}
          </button>
          <button
            v-if="!isHistoryMode"
            class="btn"
            :class="'btn-' + confirmActionVm.variant"
            @click="toggleConfirm"
            :disabled="confirmActionVm.disabled"
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
              <select class="review-input" :value="selectedSessionIdInListOrEmpty" @change="onHistorySelectionChange($event.target.value)" :disabled="loading || saving || confirming || cloudSyncBusy || !historySessions.length">
                <option v-if="!historySessions.length" value="" disabled>
                  暂无符合条件的历史交接班日志
                </option>
                <option v-else-if="!selectedSessionInHistoryList" value="" disabled>
                  当前记录未进入历史列表
                </option>
                <option v-for="item in historySessions" :key="item.session_id" :value="item.session_id">
                  {{ item.label }}
                </option>
              </select>
              <button
                v-if="canReturnToLatest"
                class="btn btn-secondary btn-mini review-history-return"
                @click="returnToLatestSession"
                :disabled="loading || saving || confirming || cloudSyncBusy"
              >
                返回最新
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
      暂无可审核交接班文件
    </section>

    <template v-else>
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
                @input="updateFixedField(blockIndex, fieldIndex, $event.target.value)"
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
              <p class="review-card-subtitle">支持新增、删除、修改本分类内容，保存后自动回写 Excel。</p>
            </div>
            <button class="btn btn-secondary btn-mini" @click="addSectionRow(sectionIndex)">
              新增一行
            </button>
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
                      @input="updateSectionCell(sectionIndex, rowIndex, column.key, $event.target.value)"
                    ></textarea>
                  </td>
                  <td class="review-col-action">
                    <button class="btn btn-danger btn-mini" @click="removeSectionRow(sectionIndex, rowIndex)">
                      删除
                    </button>
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
              <button class="btn btn-secondary btn-mini" @click="addFooterRow(blockIndex)">
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
                        @input="updateFooterCell(blockIndex, rowIndex, column.key, $event.target.value)"
                      ></textarea>
                    </td>
                    <td class="review-col-action">
                      <button class="btn btn-danger btn-mini" @click="removeFooterRow(blockIndex, rowIndex)">
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
