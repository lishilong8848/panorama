export function createMonthlyReportUiHelpers(options = {}) {
  const {
    computed,
    config,
    message,
    normalizeReceiveIdsText,
    monthlyEventReportLastRun,
    monthlyChangeReportLastRun,
    monthlyReportTestReceiveIdDraftEvent,
    monthlyReportTestReceiveIdDraftChange,
    actionKeyMonthlyReportSendAllPrefix = "",
    actionKeyMonthlyReportSendTestPrefix = "",
    actionKeyMonthlyReportSendBuildingPrefix = "",
  } = options || {};

  function resolveMonthlyReportTargetMonth(reportType) {
    const normalizedReportType = String(reportType || "event").trim().toLowerCase() === "change" ? "change" : "event";
    const sourceLastRun = normalizedReportType === "change" ? monthlyChangeReportLastRun.value : monthlyEventReportLastRun.value;
    return String(sourceLastRun?.target_month || "").trim() || "latest";
  }

  const monthlyEventReportSendAllActionKey = computed(() =>
    `${String(actionKeyMonthlyReportSendAllPrefix || "").trim()}event:${resolveMonthlyReportTargetMonth("event")}`,
  );
  const monthlyChangeReportSendAllActionKey = computed(() =>
    `${String(actionKeyMonthlyReportSendAllPrefix || "").trim()}change:${resolveMonthlyReportTargetMonth("change")}`,
  );
  const monthlyEventReportSendTestActionKey = computed(() =>
    `${String(actionKeyMonthlyReportSendTestPrefix || "").trim()}event:${resolveMonthlyReportTargetMonth("event")}`,
  );
  const monthlyChangeReportSendTestActionKey = computed(() =>
    `${String(actionKeyMonthlyReportSendTestPrefix || "").trim()}change:${resolveMonthlyReportTargetMonth("change")}`,
  );

  function getMonthlyReportTestDeliveryConfig() {
    const handover = config.value?.handover_log;
    if (!handover || typeof handover !== "object") return null;
    const monthly = handover.monthly_event_report;
    if (!monthly || typeof monthly !== "object") return null;
    const delivery = monthly.test_delivery;
    if (!delivery || typeof delivery !== "object") return null;
    return delivery;
  }

  function ensureMonthlyReportTestDeliveryConfig() {
    if (!config.value || typeof config.value !== "object") return null;
    config.value.handover_log = config.value.handover_log && typeof config.value.handover_log === "object"
      ? config.value.handover_log
      : {};
    const handover = config.value.handover_log;
    handover.monthly_event_report = handover.monthly_event_report && typeof handover.monthly_event_report === "object"
      ? handover.monthly_event_report
      : {};
    const monthly = handover.monthly_event_report;
    monthly.test_delivery = monthly.test_delivery && typeof monthly.test_delivery === "object"
      ? monthly.test_delivery
      : {};
    const delivery = monthly.test_delivery;
    delivery.receive_id_type = String(delivery.receive_id_type || "open_id").trim() || "open_id";
    delivery.receive_ids = normalizeReceiveIdsText(delivery.receive_ids || []);
    return delivery;
  }

  const monthlyReportTestReceiveIdType = computed({
    get() {
      return String(getMonthlyReportTestDeliveryConfig()?.receive_id_type || "open_id").trim() || "open_id";
    },
    set(value) {
      const delivery = ensureMonthlyReportTestDeliveryConfig();
      if (!delivery) return;
      delivery.receive_id_type = String(value || "open_id").trim() || "open_id";
    },
  });

  const monthlyReportTestReceiveIds = computed(() =>
    normalizeReceiveIdsText(getMonthlyReportTestDeliveryConfig()?.receive_ids || []),
  );
  const monthlyReportTestReceiveCount = computed(() => monthlyReportTestReceiveIds.value.length);

  function addMonthlyReportTestReceiveId(reportType = "event") {
    const draftRef = String(reportType || "event").trim().toLowerCase() === "change"
      ? monthlyReportTestReceiveIdDraftChange
      : monthlyReportTestReceiveIdDraftEvent;
    const candidate = String(draftRef.value || "").trim();
    if (!candidate) {
      if (message) message.value = "请先输入一个测试接收人 ID。";
      return;
    }
    const delivery = ensureMonthlyReportTestDeliveryConfig();
    if (!delivery) return;
    const nextIds = normalizeReceiveIdsText([...(delivery.receive_ids || []), candidate]);
    if (nextIds.length === delivery.receive_ids.length) {
      if (message) message.value = "该测试接收人 ID 已存在。";
      draftRef.value = "";
      return;
    }
    delivery.receive_ids = nextIds;
    draftRef.value = "";
    if (message) message.value = "测试接收人 ID 已加入当前配置，请点击“保存测试配置”。";
  }

  function removeMonthlyReportTestReceiveId(targetId) {
    const delivery = ensureMonthlyReportTestDeliveryConfig();
    if (!delivery) return;
    const target = String(targetId || "").trim();
    delivery.receive_ids = (delivery.receive_ids || []).filter((item) => String(item || "").trim() !== target);
    if (message) message.value = "测试接收人 ID 已从当前配置移除，请点击“保存测试配置”。";
  }

  function getMonthlyReportSendBuildingActionKey(reportType, building) {
    const normalizedReportType = String(reportType || "event").trim().toLowerCase() === "change" ? "change" : "event";
    const monthText = resolveMonthlyReportTargetMonth(normalizedReportType);
    return `${String(actionKeyMonthlyReportSendBuildingPrefix || "").trim()}${normalizedReportType}:${String(building || "").trim()}:${monthText}`;
  }

  return {
    resolveMonthlyReportTargetMonth,
    monthlyEventReportSendAllActionKey,
    monthlyChangeReportSendAllActionKey,
    monthlyEventReportSendTestActionKey,
    monthlyChangeReportSendTestActionKey,
    getMonthlyReportTestDeliveryConfig,
    ensureMonthlyReportTestDeliveryConfig,
    monthlyReportTestReceiveIdType,
    monthlyReportTestReceiveIds,
    monthlyReportTestReceiveCount,
    addMonthlyReportTestReceiveId,
    removeMonthlyReportTestReceiveId,
    getMonthlyReportSendBuildingActionKey,
  };
}
