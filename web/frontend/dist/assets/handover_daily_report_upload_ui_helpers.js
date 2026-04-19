export function createHandoverDailyReportUploadUiHelpers(options = {}) {
  const { message, handoverDailyReportUploadModal, uploadHandoverDailyReportAsset } = options;

  async function onHandoverDailyReportAssetFileChange(target, event) {
    const file = event?.target?.files?.[0];
    if (!file) return;
    try {
      await uploadHandoverDailyReportAsset(target, file, String(file.name || "").trim());
    } finally {
      if (event?.target) event.target.value = "";
    }
  }

  async function onHandoverDailyReportUploadPaste(event) {
    const items = Array.from(event?.clipboardData?.items || []);
    const imageItem = items.find((item) => String(item?.type || "").toLowerCase().startsWith("image/"));
    if (!imageItem) {
      event?.preventDefault?.();
      message.value = "剪贴板中没有图片";
      return;
    }
    const blob = imageItem.getAsFile();
    if (!blob) {
      event?.preventDefault?.();
      message.value = "剪贴板图片读取失败";
      return;
    }
    event?.preventDefault?.();
    const target = String(handoverDailyReportUploadModal.value?.target || "").trim().toLowerCase();
    await uploadHandoverDailyReportAsset(target, blob, "clipboard.png");
  }

  return {
    onHandoverDailyReportAssetFileChange,
    onHandoverDailyReportUploadPaste,
  };
}
