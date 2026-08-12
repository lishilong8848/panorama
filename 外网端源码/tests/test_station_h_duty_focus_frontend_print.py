from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
HANDOVER_REVIEW_APP = PROJECT_ROOT / "web" / "frontend" / "src" / "handover_review_app.js"


def _station_h_print_block() -> str:
    source = HANDOVER_REVIEW_APP.read_text(encoding="utf-8")
    start = source.index("      function printDutyFocusImage(")
    end = source.index("\n      onMounted(() => {", start)
    return source[start:end]


def test_station_h_prints_current_image_inside_page() -> None:
    block = _station_h_print_block()

    assert 'document.createElement("iframe")' in block
    assert 'printWindow.print()' in block
    assert '@page { size: A4 portrait; margin: 0; }' in block
    assert "await ensureDutyFocusImage" in block
    assert "dutyFocusImageUrl.value" in block


def test_station_h_print_does_not_open_or_download_pdf() -> None:
    block = _station_h_print_block()

    assert "window.open(" not in block
    assert "downloadHandoverReviewStationHDutyFocusPrintApi" not in block
    assert "打印文件正在新窗口生成" not in block
