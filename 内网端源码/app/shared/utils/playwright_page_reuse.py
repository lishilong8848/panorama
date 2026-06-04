from __future__ import annotations

from urllib.parse import urlparse

DEFAULT_REPORT_DEVICE_SCALE_FACTOR = 2
DEFAULT_REPORT_PAGE_ZOOM = 1.00
DEFAULT_REPORT_VIEWPORT = {"width": 1600, "height": 1000}


async def apply_report_page_view(page, *, zoom: float = DEFAULT_REPORT_PAGE_ZOOM) -> None:
    """Normalize headed browser pages used by FineReport downloads.

    Some on-site machines run Windows display scaling at 200%-300%. Chromium can
    then render FineReport pages with a tiny effective viewport, causing lazy
    tables to stay blank until the user manually clicks the page. Keep every
    reused building page at a stable viewport and page zoom before detection.
    """

    try:
        await page.set_viewport_size(dict(DEFAULT_REPORT_VIEWPORT))
    except Exception:  # noqa: BLE001
        pass
    script = """(nextZoom) => {
        const zoomText = `${Math.round(Number(nextZoom || 1) * 100)}%`;
        const apply = (doc) => {
            if (!doc || !doc.documentElement) return;
            doc.documentElement.style.zoom = zoomText;
            if (doc.body) {
                doc.body.style.transformOrigin = '0 0';
            }
            try { doc.defaultView && doc.defaultView.dispatchEvent(new Event('resize')); } catch (_) {}
        };
        apply(document);
    }"""
    for frame in list(getattr(page, "frames", []) or []):
        try:
            await frame.evaluate(script, float(zoom))
        except Exception:  # noqa: BLE001
            continue


def _same_origin(left: str, right: str) -> bool:
    left_text = str(left or "").strip()
    right_text = str(right or "").strip()
    if not left_text or not right_text:
        return False
    try:
        left_parsed = urlparse(left_text)
        right_parsed = urlparse(right_text)
    except Exception:  # noqa: BLE001
        return False
    return (
        str(left_parsed.scheme or "").strip().lower(),
        str(left_parsed.netloc or "").strip().lower(),
    ) == (
        str(right_parsed.scheme or "").strip().lower(),
        str(right_parsed.netloc or "").strip().lower(),
    )


async def prepare_reusable_page(
    page,
    *,
    target_url: str,
    refresh_timeout_ms: int = 20000,
) -> None:
    current_url = str(getattr(page, "url", "") or "").strip()
    needs_goto = not current_url or current_url == "about:blank" or not _same_origin(current_url, target_url)
    if not needs_goto:
        try:
            await page.reload(wait_until="domcontentloaded", timeout=max(1000, int(refresh_timeout_ms)))
        except Exception:  # noqa: BLE001
            needs_goto = True
        else:
            current_url = str(getattr(page, "url", "") or "").strip()
            if current_url.rstrip("/") != str(target_url or "").strip().rstrip("/"):
                needs_goto = True
    if needs_goto:
        await page.goto(str(target_url or "").strip(), wait_until="domcontentloaded")
    await apply_report_page_view(page)
