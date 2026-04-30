import { expandDateRange, formatDateObj, todayText } from "./config_helpers.js";

export function createDateHandoverActions(ctx) {
  const {
    config,
    message,
    selectedDate,
    rangeStartDate,
    rangeEndDate,
    selectedDates,
    handoverDutyDate,
    handoverDutyShift,
    handoverDutyAutoFollow,
    handoverDutyLastAutoAt,
  } = ctx;

  function computeAutoDutyByNow(now = new Date()) {
    const cursor = new Date(now.getTime());
    const secondOfDay = cursor.getHours() * 3600 + cursor.getMinutes() * 60 + cursor.getSeconds();
    const nineAm = 9 * 3600;
    const sixPm = 18 * 3600;
    if (secondOfDay < nineAm) {
      cursor.setDate(cursor.getDate() - 1);
      return { duty_date: formatDateObj(cursor), duty_shift: "night" };
    }
    if (secondOfDay < sixPm) {
      return { duty_date: formatDateObj(cursor), duty_shift: "day" };
    }
    return { duty_date: formatDateObj(cursor), duty_shift: "night" };
  }

  function syncHandoverDutyFromNow(force = false) {
    if (!force && !handoverDutyAutoFollow.value) return;
    const auto = computeAutoDutyByNow(new Date());
    const changed = handoverDutyDate.value !== auto.duty_date || handoverDutyShift.value !== auto.duty_shift;
    if (force || changed) {
      handoverDutyDate.value = auto.duty_date;
      handoverDutyShift.value = auto.duty_shift;
    }
    handoverDutyLastAutoAt.value = Date.now();
  }

  function onHandoverDutyDateManualChange() {
    handoverDutyAutoFollow.value = false;
  }

  function onHandoverDutyShiftManualChange() {
    handoverDutyAutoFollow.value = false;
  }

  function restoreAutoHandoverDuty() {
    handoverDutyAutoFollow.value = true;
    syncHandoverDutyFromNow(true);
    message.value = "已恢复自动班次判断";
  }

  function addDate() {
    const d = selectedDate.value;
    if (!d) return;
    if (d > todayText()) {
      message.value = "不能选择未来日期";
      return;
    }
    if (!selectedDates.value.includes(d)) {
      selectedDates.value.push(d);
      selectedDates.value.sort();
    }
  }

  function getMaxDatesPerRun() {
    const raw = config.value?.download?.multi_date?.max_dates_per_run;
    const n = Number.parseInt(raw ?? 31, 10);
    return Number.isInteger(n) && n > 0 ? n : 31;
  }

  function addDateRange() {
    const start = rangeStartDate.value;
    const end = rangeEndDate.value;
    if (!start || !end) {
      message.value = "请选择开始日期和结束日期";
      return;
    }
    if (start > end) {
      message.value = "开始日期不能晚于结束日期";
      return;
    }
    const today = todayText();
    if (end > today) {
      message.value = "结束日期不能超过今天";
      return;
    }
    const rangeDates = expandDateRange(start, end);
    if (!rangeDates.length) {
      message.value = "日期区间无效";
      return;
    }
    const merged = new Set(selectedDates.value);
    rangeDates.forEach((d) => merged.add(d));
    const maxCount = getMaxDatesPerRun();
    if (merged.size > maxCount) {
      message.value = `已选日期不能超过 ${maxCount} 天`;
      return;
    }
    selectedDates.value = Array.from(merged).sort();
    message.value = `已按区间加入 ${rangeDates.length} 天`;
  }

  function quickRangeToday() {
    const t = todayText();
    rangeStartDate.value = t;
    rangeEndDate.value = t;
  }

  function removeDate(d) {
    selectedDates.value = selectedDates.value.filter((x) => x !== d);
  }

  function clearDates() {
    selectedDates.value = [];
  }

  return {
    computeAutoDutyByNow,
    syncHandoverDutyFromNow,
    onHandoverDutyDateManualChange,
    onHandoverDutyShiftManualChange,
    restoreAutoHandoverDuty,
    addDate,
    getMaxDatesPerRun,
    addDateRange,
    quickRangeToday,
    removeDate,
    clearDates,
  };
}
