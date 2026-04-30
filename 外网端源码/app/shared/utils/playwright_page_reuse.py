from __future__ import annotations

from urllib.parse import urlparse


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
