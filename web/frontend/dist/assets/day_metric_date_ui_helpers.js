import { expandDateRange, todayText } from "./config_helpers.js";

export function createDayMetricDateUiHelpers(options = {}) {
  const {
    dayMetricSelectedDates,
    dayMetricSelectedDate,
    dayMetricRangeStartDate,
    dayMetricRangeEndDate,
    message,
  } = options;

  function appendDayMetricDate(dateText) {
    const text = String(dateText || "").trim();
    if (!text) return false;
    if (dayMetricSelectedDates.value.includes(text)) return false;
    dayMetricSelectedDates.value = [...dayMetricSelectedDates.value, text].sort();
    return true;
  }

  function addDayMetricDate() {
    appendDayMetricDate(dayMetricSelectedDate.value);
  }

  function addDayMetricDateRange() {
    const startText = String(dayMetricRangeStartDate.value || "").trim();
    const endText = String(dayMetricRangeEndDate.value || "").trim();
    if (!startText || !endText) {
      message.value = "请选择有效的起止日期";
      return;
    }
    if (startText > endText) {
      message.value = "开始日期不能晚于结束日期";
      return;
    }
    const today = todayText();
    if (endText > today) {
      message.value = "结束日期不能超过今天";
      return;
    }
    const rangeDates = expandDateRange(startText, endText);
    if (!rangeDates.length) {
      message.value = "日期区间无效";
      return;
    }
    const next = new Set(dayMetricSelectedDates.value);
    rangeDates.forEach((item) => next.add(item));
    dayMetricSelectedDates.value = Array.from(next).sort();
  }

  function removeDayMetricDate(dateText) {
    const text = String(dateText || "").trim();
    dayMetricSelectedDates.value = dayMetricSelectedDates.value.filter((item) => item !== text);
  }

  function clearDayMetricDates() {
    dayMetricSelectedDates.value = [];
  }

  return {
    appendDayMetricDate,
    addDayMetricDate,
    addDayMetricDateRange,
    removeDayMetricDate,
    clearDayMetricDates,
  };
}
