from __future__ import annotations

import shutil
from pathlib import Path


def _copy_if_exists(src: Path, dst: Path) -> None:
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_tree_files(src_dir: Path, dst_dir: Path, suffixes: set[str]) -> None:
    if not src_dir.exists():
        return
    for src_file in sorted(path for path in src_dir.rglob("*") if path.is_file() and path.suffix in suffixes):
        _copy_if_exists(src_file, dst_dir / src_file.relative_to(src_dir))


def build_frontend_assets(project_dir: Path) -> None:
    src_dir = project_dir / "web" / "frontend" / "src"
    dist_dir = project_dir / "web" / "frontend" / "dist"
    legacy_dist_dir = project_dir / "web_frontend" / "dist"

    required_src = [
        src_dir / "index.html",
        src_dir / "app.js",
    ]
    missing = [str(path) for path in required_src if not path.exists()]
    if missing:
        raise FileNotFoundError(f"缺少前端源码文件: {missing}")

    asset_sources = sorted([path for path in src_dir.iterdir() if path.is_file() and path.suffix in {".js", ".css"}])

    dist_assets = dist_dir / "assets"
    dist_assets.mkdir(parents=True, exist_ok=True)

    # Vue runtime 优先使用 src 内置文件，缺失时回退到 legacy dist。
    vue_src = src_dir / "vue.global.prod.js"
    vue_dist = dist_assets / "vue.global.prod.js"
    if vue_src.exists():
        _copy_if_exists(vue_src, vue_dist)
    else:
        candidate = legacy_dist_dir / "assets" / "vue.global.prod.js"
        if candidate.exists():
            _copy_if_exists(candidate, vue_dist)
    if not vue_dist.exists():
        raise FileNotFoundError(
            f"缺少 Vue runtime 文件。请提供: {vue_src} 或 {legacy_dist_dir / 'assets' / 'vue.global.prod.js'}"
        )

    _copy_if_exists(src_dir / "index.html", dist_dir / "index.html")
    for src_file in asset_sources:
        _copy_if_exists(src_file, dist_assets / src_file.name)
    _copy_tree_files(src_dir / "dashboard_template_sections", dist_assets / "dashboard_template_sections", {".js"})

    # 兼容旧路径引用（若存在则同步）。
    if legacy_dist_dir.exists():
        _copy_if_exists(dist_dir / "index.html", legacy_dist_dir / "index.html")
        for src_file in asset_sources:
            _copy_if_exists(dist_assets / src_file.name, legacy_dist_dir / "assets" / src_file.name)
        _copy_tree_files(dist_assets / "dashboard_template_sections", legacy_dist_dir / "assets" / "dashboard_template_sections", {".js"})
        _copy_if_exists(vue_dist, legacy_dist_dir / "assets" / "vue.global.prod.js")


def main() -> None:
    project_dir = Path(__file__).resolve().parent.parent
    build_frontend_assets(project_dir)
    print("[Frontend] 已从 src 同步到 dist。")


if __name__ == "__main__":
    main()
