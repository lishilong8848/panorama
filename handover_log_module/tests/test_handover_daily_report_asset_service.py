from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
import os

from PIL import Image

from handover_log_module.service import handover_daily_report_asset_service as asset_module


def _png_bytes() -> bytes:
    buffer = BytesIO()
    image = Image.new("RGBA", (4, 4), (30, 120, 200, 255))
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def test_daily_report_asset_service_auto_manual_effective_and_prune(tmp_path, monkeypatch):
    monkeypatch.setattr(asset_module, "resolve_runtime_state_root", lambda **_kwargs: Path(tmp_path))
    service = asset_module.HandoverDailyReportAssetService({"_global_paths": {}})
    png_bytes = _png_bytes()

    auto_summary = service.save_auto_summary_sheet_image(
        duty_date="2026-03-24",
        duty_shift="night",
        content=png_bytes,
    )
    auto_external = service.save_auto_external_page_image(
        duty_date="2026-03-24",
        duty_shift="night",
        content=png_bytes,
    )
    manual_external = service.save_manual_image(
        duty_date="2026-03-24",
        duty_shift="night",
        target="external_page",
        content=png_bytes,
        mime_type="image/png",
        original_name="clip.png",
    )

    context = service.get_capture_assets_context(duty_date="2026-03-24", duty_shift="night")
    summary = context["summary_sheet_image"]
    external = context["external_page_image"]

    assert auto_summary.exists()
    assert auto_external.exists()
    assert manual_external.exists()
    assert summary["source"] == "auto"
    assert summary["auto"]["exists"] is True
    assert summary["manual"]["exists"] is False
    assert "variant=effective" in summary["preview_url"]
    assert "view=thumb" in summary["preview_url"]
    assert "view=thumb" in summary["thumbnail_url"]
    assert "view=full" in summary["full_image_url"]
    assert summary["preview_url"] == summary["thumbnail_url"]
    assert "v=" in summary["preview_url"]
    assert "v=" in summary["auto"]["preview_url"]
    assert "view=full" in summary["auto"]["full_image_url"]
    assert external["source"] == "manual"
    assert external["manual"]["exists"] is True
    assert external["auto"]["exists"] is True
    assert "view=thumb" in external["thumbnail_url"]
    assert "view=full" in external["full_image_url"]
    assert "v=" in external["preview_url"]
    assert "v=" in external["manual"]["preview_url"]
    assert "v=" in external["auto"]["preview_url"]
    assert service.get_asset_file_path(
        duty_date="2026-03-24",
        duty_shift="night",
        target="external_page",
        variant="effective",
    ) == manual_external

    assert service.delete_manual_image(duty_date="2026-03-24", duty_shift="night", target="external_page") is True
    fallback_external = service.resolve_effective_asset(
        duty_date="2026-03-24",
        duty_shift="night",
        target="external_page",
    )
    assert fallback_external["source"] == "auto"
    assert fallback_external["stored_path"] == str(auto_external)
    assert "v=" in fallback_external["preview_url"]

    thumb_path = service.get_asset_file_path(
        duty_date="2026-03-24",
        duty_shift="night",
        target="summary_sheet",
        variant="effective",
        view="thumb",
    )
    assert thumb_path is not None
    assert thumb_path.exists()
    assert thumb_path.suffix.lower() == ".jpg"

    stale_dir = service.get_batch_dir(duty_date="2026-02-01", duty_shift="day")
    old = datetime.now() - timedelta(days=60)
    os.utime(stale_dir, (old.timestamp(), old.timestamp()))
    removed = service.prune_stale_assets()

    assert removed >= 1
    assert not stale_dir.exists()
