export function createHandoverReviewDocumentEditHelpers(options = {}) {
  const {
    documentRef,
    session,
    dirtyRegions,
    capacityTrackedCellSet,
    capacityLinkedDirty,
    documentMutationVersion,
    dirty,
    staleRevisionConflict,
    clearSaveTimers,
    beginRemoteSaveRefresh,
    isHistoryMode,
    statusText,
    touchEditingIntent,
    blankRow,
    hasSectionRowContent,
    footerRowHasContent,
    blankFooterInventoryRowWithDefaults,
    resolveFooterAutoFillCells,
    blankFooterInventoryRow,
    onFixedFieldChanged,
  } = options;

  function markDocumentDirty({ region = "", capacityCell = "" } = {}) {
    if (!session.value) return;
    touchEditingIntent();
    const targetRegion = String(region || "").trim();
    if (targetRegion && Object.prototype.hasOwnProperty.call(dirtyRegions.value, targetRegion)) {
      dirtyRegions.value[targetRegion] = true;
    }
    const cellName = String(capacityCell || "").trim().toUpperCase();
    if (cellName && capacityTrackedCellSet.value.has(cellName)) {
      capacityLinkedDirty.value = true;
    }
    documentMutationVersion.value += 1;
    dirty.value = true;
    if (staleRevisionConflict.value) {
      clearSaveTimers();
      beginRemoteSaveRefresh();
      return;
    }
    if (!isHistoryMode.value && session.value?.confirmed) {
      statusText.value = "内容已修改，保存后需重新确认";
    } else if (isHistoryMode.value) {
      statusText.value = "历史记录待保存";
    } else {
      statusText.value = "待保存";
    }
  }

  function updateFixedField(blockIndex, fieldIndex, value) {
    const block = documentRef.value.fixed_blocks?.[blockIndex];
    const field = block?.fields?.[fieldIndex];
    if (!field) return;
    const nextValue = String(value ?? "");
    if (String(field.value ?? "") === nextValue) return;
    const cellName = String(field.cell || "").trim().toUpperCase();
    field.value = nextValue;
    markDocumentDirty({ region: "fixed_blocks", capacityCell: cellName });
    if (typeof onFixedFieldChanged === "function") {
      onFixedFieldChanged({ cell: cellName, value: nextValue, blockIndex, fieldIndex });
    }
  }

  function updateSectionCell(sectionIndex, rowIndex, column, value) {
    const section = documentRef.value.sections?.[sectionIndex];
    const row = section?.rows?.[rowIndex];
    if (!section || !row || !row.cells) return;
    const nextValue = String(value ?? "");
    if (String(row.cells[column] ?? "") === nextValue) return;
    row.cells[column] = nextValue;
    row.is_placeholder_row = !hasSectionRowContent(row, section.columns);
    markDocumentDirty({ region: "sections" });
  }

  function addSectionRow(sectionIndex) {
    const section = documentRef.value.sections?.[sectionIndex];
    if (!section || !Array.isArray(section.rows)) return;
    markDocumentDirty({ region: "sections" });
    section.rows.push(blankRow(section.columns));
  }

  function removeSectionRow(sectionIndex, rowIndex) {
    const section = documentRef.value.sections?.[sectionIndex];
    if (!section || !Array.isArray(section.rows)) return;
    markDocumentDirty({ region: "sections" });
    section.rows.splice(rowIndex, 1);
    if (!section.rows.length) {
      section.rows.push(blankRow(section.columns));
    }
  }

  function normalizeHeaderText(value) {
    return String(value ?? "").replace(/\s+/g, "").trim().toLowerCase();
  }

  function normalizeCompareText(value) {
    return String(value ?? "").replace(/\s+/g, "").trim().toLowerCase();
  }

  function sectionName(section) {
    return String(section?.name || "").trim();
  }

  function findSectionIndexByName(name) {
    const target = String(name || "").trim();
    const sections = Array.isArray(documentRef.value.sections) ? documentRef.value.sections : [];
    return sections.findIndex((section) => sectionName(section) === target);
  }

  function findColumnKey(section, labels, fallbackKey) {
    const columns = Array.isArray(section?.columns) ? section.columns : [];
    const normalizedLabels = (Array.isArray(labels) ? labels : [labels])
      .map((label) => normalizeHeaderText(label))
      .filter(Boolean);
    const matched = columns.find((column) => {
      const key = normalizeHeaderText(column?.key);
      const label = normalizeHeaderText(column?.label);
      return normalizedLabels.includes(key) || normalizedLabels.includes(label);
    });
    if (matched?.key) return matched.key;
    const fallback = String(fallbackKey || "").trim().toUpperCase();
    const fallbackColumn = columns.find((column) => String(column?.key || "").trim().toUpperCase() === fallback);
    return fallbackColumn?.key || "";
  }

  function resolveTransferPayload(section, row) {
    const name = sectionName(section);
    if (name === "维护管理") {
      const descriptionKey = findColumnKey(section, ["维护总项"], "B");
      const executorKey = findColumnKey(section, ["执行人"], "H");
      return {
        description: String(row?.cells?.[descriptionKey] ?? "").trim(),
        executor: String(row?.cells?.[executorKey] ?? "").trim(),
      };
    }
    if (name === "变更管理") {
      const descriptionKey = findColumnKey(section, ["描述"], "D");
      const executorKey = findColumnKey(section, ["执行人"], "H");
      return {
        description: String(row?.cells?.[descriptionKey] ?? "").trim(),
        executor: String(row?.cells?.[executorKey] ?? "").trim(),
      };
    }
    return { description: "", executor: "" };
  }

  function canTransferSectionRowToOtherImportantWork(section, row) {
    const name = sectionName(section);
    if (name !== "维护管理" && name !== "变更管理") return false;
    if (!row || row.is_placeholder_row) return false;
    const payload = resolveTransferPayload(section, row);
    return Boolean(payload.description);
  }

  function transferSectionRowToOtherImportantWork(sectionIndex, rowIndex) {
    const sourceSection = documentRef.value.sections?.[sectionIndex];
    const sourceRow = sourceSection?.rows?.[rowIndex];
    if (!canTransferSectionRowToOtherImportantWork(sourceSection, sourceRow)) return;

    const payload = resolveTransferPayload(sourceSection, sourceRow);
    const targetIndex = findSectionIndexByName("其他重要工作记录");
    if (targetIndex < 0) {
      statusText.value = "未找到其他重要工作记录分类";
      return;
    }
    const targetSection = documentRef.value.sections[targetIndex];
    const descriptionKey = findColumnKey(targetSection, ["描述"], "B");
    const completionKey = findColumnKey(targetSection, ["完成情况"], "F");
    const executorKey = findColumnKey(targetSection, ["执行人"], "H");
    if (!descriptionKey || !completionKey || !executorKey) {
      statusText.value = "其他重要工作记录缺少描述、完成情况或执行人列";
      return;
    }

    const duplicateDescription = normalizeCompareText(payload.description);
    const duplicateExecutor = normalizeCompareText(payload.executor);
    const rows = Array.isArray(targetSection.rows) ? targetSection.rows : [];
    const exists = rows.some((row) => {
      if (!row || row.is_placeholder_row || !row.cells) return false;
      return normalizeCompareText(row.cells[descriptionKey]) === duplicateDescription
        && normalizeCompareText(row.cells[executorKey]) === duplicateExecutor;
    });

    if (!exists) {
      if (!Array.isArray(targetSection.rows)) {
        targetSection.rows = [];
      }
      let targetRow = targetSection.rows.find((row) => row?.is_placeholder_row && !hasSectionRowContent(row, targetSection.columns));
      if (!targetRow) {
        targetRow = blankRow(targetSection.columns);
        targetSection.rows.push(targetRow);
      }
      if (!targetRow.cells || typeof targetRow.cells !== "object") {
        targetRow.cells = {};
      }
      targetRow.cells[descriptionKey] = payload.description;
      targetRow.cells[completionKey] = "已完成";
      targetRow.cells[executorKey] = payload.executor;
      targetRow.is_placeholder_row = !hasSectionRowContent(targetRow, targetSection.columns);
    }

    if (Array.isArray(sourceSection.rows)) {
      sourceSection.rows.splice(rowIndex, 1);
      if (!sourceSection.rows.length) {
        sourceSection.rows.push(blankRow(sourceSection.columns));
      }
    }
    markDocumentDirty({ region: "sections" });
    statusText.value = exists
      ? "其他重要工作记录已存在该记录，已从原分类删除，待保存"
      : "已转到其他重要工作记录，待保存";
  }

  function updateFooterCell(blockIndex, rowIndex, column, value) {
    const block = documentRef.value.footer_blocks?.[blockIndex];
    if (!block || block.type !== "inventory_table") return;
    const row = block.rows?.[rowIndex];
    if (!row || !row.cells) return;
    const nextValue = String(value ?? "");
    if (String(row.cells[column] ?? "") === nextValue) return;
    row.cells[column] = nextValue;
    row.is_placeholder_row = !footerRowHasContent(row, block.columns);
    markDocumentDirty({ region: "footer_inventory" });
  }

  function addFooterRow(blockIndex) {
    const block = documentRef.value.footer_blocks?.[blockIndex];
    if (!block || block.type !== "inventory_table" || !Array.isArray(block.rows)) return;
    markDocumentDirty({ region: "footer_inventory" });
    block.rows.push(blankFooterInventoryRowWithDefaults(block.columns, resolveFooterAutoFillCells(block)));
  }

  function removeFooterRow(blockIndex, rowIndex) {
    const block = documentRef.value.footer_blocks?.[blockIndex];
    if (!block || block.type !== "inventory_table" || !Array.isArray(block.rows)) return;
    markDocumentDirty({ region: "footer_inventory" });
    if (block.rows.length <= 1) {
      const placeholder = blankFooterInventoryRow(block.columns);
      block.rows[0].cells = placeholder.cells;
      block.rows[0].is_placeholder_row = true;
      return;
    }
    block.rows.splice(rowIndex, 1);
  }

  return {
    markDocumentDirty,
    updateFixedField,
    updateSectionCell,
    addSectionRow,
    removeSectionRow,
    canTransferSectionRowToOtherImportantWork,
    transferSectionRowToOtherImportantWork,
    updateFooterCell,
    addFooterRow,
    removeFooterRow,
  };
}
