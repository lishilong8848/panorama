from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_build_frontend_module():
    project_root = Path(__file__).resolve().parents[2]
    module_path = project_root / "scripts" / "build_frontend.py"
    spec = importlib.util.spec_from_file_location("build_frontend_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_build_frontend_assets_copies_all_src_js_and_css(tmp_path: Path) -> None:
    module = _load_build_frontend_module()
    project_dir = tmp_path / "project"
    src_dir = project_dir / "web" / "frontend" / "src"
    dist_assets = project_dir / "web" / "frontend" / "dist" / "assets"
    legacy_assets = project_dir / "web_frontend" / "dist" / "assets"

    src_dir.mkdir(parents=True, exist_ok=True)
    legacy_assets.mkdir(parents=True, exist_ok=True)

    (src_dir / "index.html").write_text("<!doctype html>", encoding="utf-8")
    (src_dir / "app.js").write_text("import './extra.js';", encoding="utf-8")
    (src_dir / "extra.js").write_text("export const ok = true;", encoding="utf-8")
    (src_dir / "dashboard_wet_bulb_collection_actions.js").write_text("export const wet = true;", encoding="utf-8")
    (src_dir / "style.css").write_text("body { color: red; }", encoding="utf-8")
    (src_dir / "vue.global.prod.js").write_text("window.Vue = {};", encoding="utf-8")

    module.build_frontend_assets(project_dir)

    assert (dist_assets / "app.js").exists()
    assert (dist_assets / "extra.js").exists()
    assert (dist_assets / "dashboard_wet_bulb_collection_actions.js").exists()
    assert (dist_assets / "style.css").exists()
    assert (dist_assets / "vue.global.prod.js").exists()
    assert (legacy_assets / "dashboard_wet_bulb_collection_actions.js").exists()
