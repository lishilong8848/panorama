export function createInternalSourceCacheUiHelpers(options = {}) {
  const {
    isActionLocked,
    getSourceCacheRefreshBuildingActionKey,
    refreshBuildingActionKeyPrefix = "",
  } = options || {};

  function getInternalSourceCacheRefreshActionKey(sourceFamily, building) {
    const sourceFamilyText = String(sourceFamily || "").trim();
    const buildingText = String(building || "").trim();
    if (typeof getSourceCacheRefreshBuildingActionKey === "function") {
      return getSourceCacheRefreshBuildingActionKey(sourceFamilyText, buildingText);
    }
    return `${String(refreshBuildingActionKeyPrefix || "").trim()}${sourceFamilyText}:${buildingText}`;
  }

  function resolveInternalSourceCacheFamilyKey(family, building) {
    return String(
      family?.key
      || building?.source_family
      || building?.sourceFamily
      || "",
    ).trim();
  }

  function resolveInternalSourceCacheBuildingName(building) {
    return String(building?.building || building?.label || "").trim();
  }

  function getInternalSourceCacheRefreshAction(family, building) {
    const actions = building?.actions && typeof building.actions === "object" ? building.actions : {};
    if (actions.refresh && typeof actions.refresh === "object") {
      return actions.refresh;
    }
    return null;
  }

  function getInternalSourceCacheRefreshDisabledReason(family, building) {
    const action = getInternalSourceCacheRefreshAction(family, building);
    if (action) {
      if (action.pending) {
        return String(action?.disabledReason || action?.disabled_reason || "").trim() || "请求处理中，请稍候";
      }
      if (action.allowed === false) {
        return String(action?.disabledReason || action?.disabled_reason || "").trim();
      }
      return String(action?.disabledReason || action?.disabled_reason || "").trim();
    }
    const actionKey = getInternalSourceCacheRefreshActionKey(
      resolveInternalSourceCacheFamilyKey(family, building),
      resolveInternalSourceCacheBuildingName(building),
    );
    if (isActionLocked?.(actionKey)) {
      return "请求处理中，请稍候";
    }
    return "";
  }

  function isInternalSourceCacheRefreshLocked(family, building) {
    const actionKey = getInternalSourceCacheRefreshActionKey(
      resolveInternalSourceCacheFamilyKey(family, building),
      resolveInternalSourceCacheBuildingName(building),
    );
    const action = getInternalSourceCacheRefreshAction(family, building);
    if (action && (action.pending || action.allowed === false)) return true;
    return Boolean(isActionLocked?.(actionKey));
  }

  function getInternalSourceCacheRefreshButtonText(family, building) {
    const actionKey = getInternalSourceCacheRefreshActionKey(
      resolveInternalSourceCacheFamilyKey(family, building),
      resolveInternalSourceCacheBuildingName(building),
    );
    const action = getInternalSourceCacheRefreshAction(family, building);
    const explicitLabel = String(action?.label || "").trim();
    if (explicitLabel) {
      return explicitLabel;
    }
    if (isActionLocked?.(actionKey)) {
      return "拉取中...";
    }
    return "重新拉取";
  }

  return {
    getInternalSourceCacheRefreshActionKey,
    getInternalSourceCacheRefreshAction,
    getInternalSourceCacheRefreshDisabledReason,
    isInternalSourceCacheRefreshLocked,
    getInternalSourceCacheRefreshButtonText,
  };
}
