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
    updateFooterCell,
    addFooterRow,
    removeFooterRow,
  };
}
