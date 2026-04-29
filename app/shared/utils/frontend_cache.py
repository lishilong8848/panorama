from __future__ import annotations

from pathlib import Path


_SOURCE_FRONTEND_NO_CACHE_HEADERS = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


def source_frontend_no_cache_headers(frontend_mode: str) -> dict[str, str]:
    if str(frontend_mode or "").strip().lower() != "source":
        return {}
    return dict(_SOURCE_FRONTEND_NO_CACHE_HEADERS)


def render_frontend_index_html(
    frontend_root: Path,
    *,
    frontend_mode: str,
    asset_base_path: str = "/assets",
) -> str:
    index_path = Path(frontend_root) / "index.html"
    html = index_path.read_text(encoding="utf-8")
    if str(frontend_mode or "").strip().lower() != "source":
        return html
    asset_prefix = str(asset_base_path or "/assets").rstrip("/")
    html = html.replace('"/assets/', f'"{asset_prefix}/')
    html = html.replace("'/assets/", f"'{asset_prefix}/")
    return html


def resolve_source_frontend_asset_path(frontend_assets_dir: Path, asset_path: str) -> Path | None:
    assets_root = Path(frontend_assets_dir).resolve()
    candidate = (assets_root / str(asset_path or "")).resolve()
    try:
        candidate.relative_to(assets_root)
    except ValueError:
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    return candidate
