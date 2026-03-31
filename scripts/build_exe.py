from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path


APP_NAME = "QJPT"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.shared.runtime_dependency_spec import build_runtime_dependency_lock, normalized_runtime_dependency_specs

BUILD_DIR = PROJECT_ROOT / "build_output"
DEFAULT_MAJOR_VERSION = 3
PROJECT_VERSION_STATE_FILE = PROJECT_ROOT / "build_version_state.json"

RELEASE_ROOT_NAME = "QJPT_V3"
RELEASE_CODE_DIR_NAME = "QJPT_V3_code"
PATCH_DIR_NAME = "patch_only"
LAUNCHER_NAME = "启动程序.bat"
CODE_LAUNCHER_NAME = "启动程序.bat"
USER_CONFIG_FILE_NAME = "表格计算配置.json"

DEFAULT_GITEE_REPO = "https://gitee.com/myligitt/qjpt.git"
DEFAULT_GITEE_BRANCH = "master"
DEFAULT_GITEE_SUBDIR = "updates/patches"
DEFAULT_GITEE_MANIFEST_PATH = "updates/latest_patch.json"
DEFAULT_PATCH_ZIP_NAME = "QJPT_patch_only.zip"
DEFAULT_PIP_INDEX_URL = "https://pypi.tuna.tsinghua.edu.cn/simple"
DEFAULT_PIP_TRUSTED_HOST = "pypi.tuna.tsinghua.edu.cn"
DEFAULT_EMBED_PY_VERSION = "3.11.9"
RUNTIME_DIR_NAME = "runtime"
RUNTIME_PYTHON_DIR_NAME = "python"
RUNTIME_PIP_PACKAGES = [spec["package"] for spec in normalized_runtime_dependency_specs()]

EMBED_ZIP_MIRRORS = [
    "https://npmmirror.com/mirrors/python",
    "https://mirrors.huaweicloud.com/python",
    "https://www.python.org/ftp/python",
]

EXCLUDE_TOP_LEVEL = {
    ".git",
    ".idea",
    ".vscode",
    "__pycache__",
    ".pytest_cache",
    ".runtime",
    "runtime_state",
    "build",
    "dist",
    "build_output",
}

EXCLUDE_SUFFIX = {".pyc", ".pyo", ".tmp", ".log"}
EXCLUDE_CONTAINS = {"__pycache__", ".git", ".pytest_cache", ".mypy_cache"}
PATCH_EXCLUDE_FILE_NAMES = {
    "表格计算配置.json",
    "表格计算配置.template.json",
    "handover_default.json",
    "handover_config.schema.json",
}
PRESERVE_RELEASE_TOP_LEVEL_DIRS = {
    ".runtime",
    ".venv",
    RUNTIME_DIR_NAME,
    "runtime_state",
}

CRITICAL_RELEASE_SYNC_FILES = [
    Path("app/bootstrap/container.py"),
    Path("app/bootstrap/app_factory.py"),
    Path("app/shared/runtime_dependency_spec.py"),
    Path("app/shared/utils/frontend_cache.py"),
    Path("app/config/config_adapter.py"),
    Path("app/config/config_merge_guard.py"),
    Path("app/config/config_schema_v3.py"),
    Path("app/config/settings_loader.py"),
    Path("app/modules/updater/api/routes.py"),
    Path("app/modules/updater/service/runtime_dependency_sync_service.py"),
    Path("app/modules/updater/service/updater_service.py"),
    Path("app/modules/updater/service/update_applier.py"),
    Path("app/modules/updater/core/versioning.py"),
    Path("app/modules/updater/repository/updater_state_store.py"),
    Path("app/modules/report_pipeline/api/routes.py"),
    Path("app/modules/report_pipeline/service/job_service.py"),
    Path("app/modules/report_pipeline/service/orchestrator_service.py"),
    Path("app/modules/report_pipeline/service/runtime_config_validator.py"),
    Path("app/modules/report_pipeline/service/system_alert_log_upload_service.py"),
    Path("app/modules/network/service/network_stability.py"),
    Path("app/modules/websocket/service/log_stream_service.py"),
    Path("handover_log_module/repository/download_gateway.py"),
    Path("handover_log_module/service/day_metric_bitable_export_service.py"),
    Path("handover_log_module/service/day_metric_standalone_upload_service.py"),
    Path("handover_log_module/service/review_session_service.py"),
    Path("handover_log_module/service/review_followup_trigger_service.py"),
    Path("handover_log_module/service/handover_cloud_sheet_sync_service.py"),
    Path("handover_log_module/service/handover_daily_report_state_service.py"),
    Path("handover_log_module/service/handover_daily_report_asset_service.py"),
    Path("handover_log_module/service/handover_daily_report_screenshot_service.py"),
    Path("handover_log_module/service/handover_daily_report_bitable_export_service.py"),
    Path("handover_log_module/service/source_data_attachment_bitable_export_service.py"),
    Path("app/modules/handover_review/api/routes.py"),
    Path("web/frontend/src/api_client.js"),
    Path("web/frontend/src/app_lifecycle.js"),
    Path("web/frontend/src/app_config_feature_handover_tab.js"),
    Path("web/frontend/src/app_dashboard_template.js"),
    Path("web/frontend/src/app.js"),
    Path("web/frontend/src/app_template.js"),
    Path("web/frontend/src/index.html"),
    Path("web/frontend/src/app_state.js"),
    Path("web/frontend/src/app_status_template.js"),
    Path("web/frontend/src/config_api_utils.js"),
    Path("web/frontend/src/config_helpers.js"),
    Path("web/frontend/src/config_runtime_convert.js"),
    Path("web/frontend/src/config_runtime_defaults.js"),
    Path("web/frontend/src/dashboard_job_actions.js"),
    Path("web/frontend/src/dashboard_menu_config.js"),
    Path("web/frontend/src/log_stream.js"),
    Path("web/frontend/src/runtime_health_config_actions.js"),
    Path("web/frontend/src/runtime_resume_actions.js"),
    Path("web/frontend/src/style.css"),
    Path("web/frontend/src/updater_text.js"),
    Path("web/frontend/src/ui_local_actions.js"),
    Path("portable_launcher.py"),
    Path("main.py"),
]

SMOKE_IMPORT_MODULES = [
    "fastapi",
    "uvicorn",
    "starlette",
    "PIL",
    "playwright",
    "openpyxl",
    "pymysql",
    "app.config.config_merge_guard",
    "app.bootstrap.app_factory",
    "app.modules.report_pipeline.core.time_window_policy",
    "app.modules.report_pipeline.service.runtime_config_validator",
    "app.modules.network.service.network_stability",
    "app.modules.sheet_import.core.field_value_converter",
    "app.modules.feishu.service.bitable_client_runtime",
    "app.modules.updater.service.runtime_dependency_sync_service",
    "app.modules.updater.service.updater_service",
    "app.modules.updater.service.update_applier",
    "app.modules.updater.core.versioning",
]


def log(msg: str) -> None:
    print(f"[Build] {msg}")


def _run_cmd(args: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        text=True,
        capture_output=True,
        check=False,
    )


def _remote_branch_exists(repo_url: str, branch: str) -> bool:
    if not str(repo_url or "").strip() or not str(branch or "").strip():
        return False
    ret = _run_cmd(["git", "ls-remote", "--heads", repo_url, f"refs/heads/{branch}"])
    return ret.returncode == 0 and bool((ret.stdout or "").strip())


def _detect_remote_default_branch(repo_url: str) -> str:
    if not str(repo_url or "").strip():
        return ""
    ret = _run_cmd(["git", "ls-remote", "--symref", repo_url, "HEAD"])
    if ret.returncode != 0:
        return ""
    for line in (ret.stdout or "").splitlines():
        line = line.strip()
        if not line.startswith("ref: refs/heads/") or not line.endswith("\tHEAD"):
            continue
        head_ref = line.split("\t", 1)[0]
        return head_ref.replace("ref: refs/heads/", "", 1).strip()
    return ""


def _resolve_upload_branch(repo_url: str, preferred_branch: str) -> str:
    preferred = str(preferred_branch or "").strip() or DEFAULT_GITEE_BRANCH
    if _remote_branch_exists(repo_url, preferred):
        return preferred
    remote_default = _detect_remote_default_branch(repo_url)
    if remote_default:
        log(f"远端分支不存在: {preferred}，自动回退到默认分支: {remote_default}")
        return remote_default
    log(f"无法探测远端默认分支，仍使用: {preferred}")
    return preferred


def _ensure_smoke_imports() -> None:
    code = "import " + ", ".join(SMOKE_IMPORT_MODULES)
    result = _run_cmd([sys.executable, "-c", code], cwd=PROJECT_ROOT)
    if result.returncode != 0:
        raise RuntimeError(
            "运行时依赖导入检查失败。\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    log("运行时依赖导入检查通过")


def _ensure_release_tree_imports(code_dir: Path) -> None:
    code = "import " + ", ".join(SMOKE_IMPORT_MODULES)
    result = _run_cmd([sys.executable, "-c", code], cwd=code_dir)
    if result.returncode != 0:
        raise RuntimeError(
            "全量目录源码导入校验失败（QJPT_V3_code）。\n"
            f"code_dir={code_dir}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    log("全量目录源码导入校验通过(QJPT_V3_code)")


def _detect_current_venv_root() -> Path | None:
    exe = Path(sys.executable).resolve()
    if exe.name.lower() != "python.exe":
        return None
    if exe.parent.name.lower() != "scripts":
        return None
    root = exe.parent.parent
    if (root / "Scripts" / "python.exe").exists() and (root / "Lib" / "site-packages").exists():
        return root
    return None


def _materialize_packaged_venv(code_dir: Path) -> None:
    target_venv = code_dir / ".venv"
    if (target_venv / "Scripts" / "python.exe").exists():
        return
    source_venv = _detect_current_venv_root()
    if not source_venv:
        raise RuntimeError(
            "未检测到可复制的当前虚拟环境。请使用虚拟环境中的 python 执行 build_exe.py，"
            "或手动将 .venv 放入项目目录。"
        )
    log(f"检测到项目目录无 .venv，自动复制当前运行环境: {source_venv} -> {target_venv}")
    shutil.copytree(source_venv, target_venv, dirs_exist_ok=True)


def _ensure_packaged_runtime_imports(code_dir: Path) -> None:
    _materialize_packaged_venv(code_dir)
    venv_python = code_dir / ".venv" / "Scripts" / "python.exe"
    if not venv_python.exists():
        raise RuntimeError(f"未找到内置运行时: {venv_python}")
    code = "import " + ", ".join(SMOKE_IMPORT_MODULES)
    result = _run_cmd([str(venv_python), "-c", code], cwd=code_dir)
    if result.returncode != 0:
        raise RuntimeError(
            "内置运行时依赖校验失败（QJPT_V3_code/.venv）。\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    log("内置运行时依赖校验通过(.venv)")


def _ensure_embedded_runtime_bootstrap_imports(code_dir: Path) -> None:
    runtime_python = code_dir / RUNTIME_DIR_NAME / RUNTIME_PYTHON_DIR_NAME / "python.exe"
    if not runtime_python.exists():
        raise RuntimeError(f"未找到内置 Python 运行时: {runtime_python}")
    code = (
        "import sys; "
        "sys.path.insert(0, '.'); "
        "import main, worker_bootstrap"
    )
    result = _run_cmd([str(runtime_python), "-c", code], cwd=code_dir)
    if result.returncode != 0:
        raise RuntimeError(
            "lite 包启动链导入校验失败（QJPT_V3_code/runtime/python）。\n"
            f"code_dir={code_dir}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    worker_help = _run_cmd([str(runtime_python), str(code_dir / "worker_bootstrap.py"), "--help"], cwd=code_dir)
    if worker_help.returncode != 0:
        raise RuntimeError(
            "lite 包 worker 启动链校验失败（QJPT_V3_code/runtime/python -> worker_bootstrap.py）。\n"
            f"code_dir={code_dir}\n"
            f"stdout:\n{worker_help.stdout}\n"
            f"stderr:\n{worker_help.stderr}"
        )
    log("lite 包启动链导入校验通过(runtime/python -> import main, worker_bootstrap)")


def _embed_zip_name(version: str) -> str:
    return f"python-{version}-embed-amd64.zip"


def _download_embed_zip(version: str) -> Path:
    zip_name = _embed_zip_name(version)
    cache_dir = BUILD_DIR / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached_zip = cache_dir / zip_name
    if cached_zip.exists() and cached_zip.stat().st_size > 0:
        if zipfile.is_zipfile(cached_zip):
            return cached_zip
        log(f"检测到损坏的内置 Python 缓存 zip，已删除并重新下载: {cached_zip}")
        cached_zip.unlink(missing_ok=True)

    errors: list[str] = []
    for base_url in EMBED_ZIP_MIRRORS:
        url = f"{base_url.rstrip('/')}/{version}/{zip_name}"
        try:
            with urllib.request.urlopen(url, timeout=45) as resp, tempfile.NamedTemporaryFile(
                prefix="qjpt_embed_",
                suffix=".zip",
                delete=False,
            ) as tmp:
                shutil.copyfileobj(resp, tmp)
                tmp_path = Path(tmp.name)
            if not zipfile.is_zipfile(tmp_path):
                errors.append(f"{url} -> 下载文件不是 zip")
                tmp_path.unlink(missing_ok=True)
                continue
            shutil.move(str(tmp_path), str(cached_zip))
            log(f"已下载内置 Python 运行时: {url}")
            return cached_zip
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            errors.append(f"{url} -> {exc}")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url} -> {exc}")
    raise RuntimeError("下载内置 Python 运行时失败:\n" + "\n".join(errors))


def _patch_embed_pth(runtime_python_dir: Path) -> None:
    pth_files = sorted(runtime_python_dir.glob("python*._pth"))
    if not pth_files:
        raise RuntimeError(f"未找到内置 Python 的 _pth 文件: {runtime_python_dir}")
    pth_file = pth_files[0]
    lines = pth_file.read_text(encoding="utf-8").splitlines()
    cleaned: list[str] = []
    has_site_packages = False
    has_import_site = False
    for line in lines:
        stripped = line.strip()
        if stripped.lower() == "lib\\site-packages":
            has_site_packages = True
            cleaned.append("Lib\\site-packages")
            continue
        if stripped.lower() == "import site":
            has_import_site = True
            cleaned.append("import site")
            continue
        if stripped.lower() == "#import site":
            has_import_site = True
            cleaned.append("import site")
            continue
        cleaned.append(line)
    if not has_site_packages:
        cleaned.append("Lib\\site-packages")
    if not has_import_site:
        cleaned.append("import site")
    pth_file.write_text("\n".join(cleaned) + "\n", encoding="utf-8")


def _prepare_embedded_runtime(code_dir: Path, version: str) -> Path:
    runtime_python_dir = code_dir / RUNTIME_DIR_NAME / RUNTIME_PYTHON_DIR_NAME
    runtime_python_exe = runtime_python_dir / "python.exe"
    if runtime_python_exe.exists():
        _patch_embed_pth(runtime_python_dir)
        return runtime_python_exe

    runtime_python_dir.mkdir(parents=True, exist_ok=True)
    embed_zip = _download_embed_zip(version)
    with zipfile.ZipFile(embed_zip, "r") as zf:
        zf.extractall(runtime_python_dir)
    if not runtime_python_exe.exists():
        raise RuntimeError(f"内置 Python 解压后缺少 python.exe: {runtime_python_dir}")
    _patch_embed_pth(runtime_python_dir)
    log(f"已准备内置 Python 运行时: {runtime_python_exe}")
    return runtime_python_exe


def _normalize_repo_url(repo_url: str) -> str:
    text = str(repo_url or "").strip().rstrip("/")
    if text.endswith(".git"):
        text = text[:-4]
    return text


def _to_raw_url(repo_url: str, branch: str, repo_path: str) -> str:
    base = _normalize_repo_url(repo_url)
    rel = str(repo_path).replace("\\", "/").lstrip("/")
    return f"{base}/raw/{branch}/{rel}"


def _should_exclude(rel: Path, include_venv: bool, include_runtime: bool = True) -> bool:
    parts = {p for p in rel.parts}
    if parts & EXCLUDE_CONTAINS:
        return True
    if rel.parts and rel.parts[0] in EXCLUDE_TOP_LEVEL:
        return True
    if not include_runtime and rel.parts and rel.parts[0] == RUNTIME_DIR_NAME:
        return True
    if not include_venv and ".venv" in parts:
        return True
    if rel.suffix.lower() in EXCLUDE_SUFFIX:
        return True
    return False


def _copy_project_tree(
    dst_dir: Path,
    *,
    include_venv: bool,
    include_runtime: bool = True,
    clean_before_copy: bool = True,
) -> tuple[int, int]:
    copied = 0
    skipped = 0
    if dst_dir.exists() and clean_before_copy:
        try:
            shutil.rmtree(dst_dir)
        except PermissionError as exc:
            log(f"目录被占用，切换为覆盖同步模式: {dst_dir} ({exc})")
    dst_dir.mkdir(parents=True, exist_ok=True)
    for path in PROJECT_ROOT.rglob("*"):
        rel = path.relative_to(PROJECT_ROOT)
        if not rel.parts:
            continue
        if _should_exclude(rel, include_venv=include_venv, include_runtime=include_runtime):
            continue
        target = dst_dir / rel
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(path, target)
            copied += 1
        except PermissionError:
            skipped += 1
        except OSError:
            skipped += 1
    return copied, skipped


def _capture_existing_user_config(code_dir: Path) -> bytes | None:
    config_path = code_dir / USER_CONFIG_FILE_NAME
    if not config_path.exists():
        return None
    return config_path.read_bytes()


def _restore_existing_user_config(code_dir: Path, payload: bytes | None) -> bool:
    if payload is None:
        return False
    config_path = code_dir / USER_CONFIG_FILE_NAME
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_bytes(payload)
    return True


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_manifest(root: Path, *, include_venv: bool, include_runtime: bool = True) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        if _should_exclude(rel, include_venv=include_venv, include_runtime=include_runtime):
            continue
        out[str(rel).replace("\\", "/")] = _sha256_file(path)
    return out


def _should_exclude_from_patch(rel: Path) -> bool:
    rel_text = str(rel).replace("\\", "/")
    if rel_text.startswith(f"{RUNTIME_DIR_NAME}/"):
        return True
    if rel.name in PATCH_EXCLUDE_FILE_NAMES:
        return True
    if rel.suffix.lower() == ".json" and "config" in rel.parts:
        return True
    return False


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}


def _read_project_version_state() -> dict:
    state = _read_json(PROJECT_VERSION_STATE_FILE)
    if not isinstance(state, dict):
        return {}
    return state


def _write_project_version_state(
    *,
    major_version: int,
    patch_version: int,
    release_revision: int,
    display_version: str,
) -> None:
    payload = {
        "app_name": APP_NAME,
        "major_version": int(major_version),
        "patch_version": int(patch_version),
        "release_revision": int(release_revision),
        "display_version": str(display_version or "").strip(),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    _write_json(PROJECT_VERSION_STATE_FILE, payload)


def _pick_persisted_version() -> tuple[int, int, int]:
    state = _read_project_version_state()
    major = int(state.get("major_version", DEFAULT_MAJOR_VERSION) or DEFAULT_MAJOR_VERSION)
    patch = int(state.get("patch_version", 0) or 0)
    release_revision = int(state.get("release_revision", patch) or patch or 0)
    return major, patch, release_revision


def _zip_dir(dir_path: Path, zip_name: str) -> Path:
    zip_path = dir_path.parent / zip_name
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(dir_path.rglob("*")):
            if path.is_file():
                rel = path.relative_to(dir_path)
                zf.write(path, arcname=str(rel).replace("\\", "/"))
    return zip_path


def _build_display_version(major: int, patch: int, date_text: str) -> str:
    return f"V{major}.{patch}.{date_text}"


def _collect_runtime_dependency_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for spec in normalized_runtime_dependency_specs():
        package = spec["package"]
        try:
            versions[package] = str(importlib.metadata.version(package) or "").strip()
        except importlib.metadata.PackageNotFoundError as exc:
            raise RuntimeError(f"构建环境缺少运行时依赖，无法生成依赖锁: {package}") from exc
    return versions


def _write_runtime_dependency_lock(code_dir: Path, *, python_version: str) -> dict:
    payload = build_runtime_dependency_lock(
        package_versions=_collect_runtime_dependency_versions(),
        python_version=python_version,
    )
    _write_json(code_dir / "runtime_dependency_lock.json", payload)
    return payload


def _force_console_host_for_packaged_config(code_dir: Path) -> bool:
    config_path = code_dir / "表格计算配置.json"
    payload = _read_json(config_path)
    if not isinstance(payload, dict):
        return False
    common_cfg = payload.setdefault("common", {})
    if not isinstance(common_cfg, dict):
        payload["common"] = {}
        common_cfg = payload["common"]
    console_cfg = common_cfg.setdefault("console", {})
    if not isinstance(console_cfg, dict):
        common_cfg["console"] = {}
        console_cfg = common_cfg["console"]
    if str(console_cfg.get("host", "") or "").strip() == "0.0.0.0":
        return False
    console_cfg["host"] = "0.0.0.0"
    _write_json(config_path, payload)
    return True


def _write_build_meta(
    code_dir: Path,
    *,
    build_id: str,
    major_version: int,
    patch_version: int,
    release_revision: int,
    venv_hash: str,
    dependency_lock_hash: str = "",
) -> dict:
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    date_text = time.strftime("%Y%m%d")
    payload = {
        "app_name": APP_NAME,
        "build_id": build_id,
        "major_version": int(major_version),
        "patch_version": int(patch_version),
        "release_revision": int(release_revision),
        "display_version": _build_display_version(int(major_version), int(patch_version), date_text),
        "created_at": now,
        "venv_hash": str(venv_hash or "").strip(),
        "dependency_lock_hash": str(dependency_lock_hash or "").strip(),
    }
    _write_json(code_dir / "build_meta.json", payload)
    return payload


def _build_patch_only(
    *,
    full_dir: Path,
    baseline_dir: Path,
    patch_dir: Path,
    include_venv: bool,
) -> tuple[int, int, int, list[str]]:
    if patch_dir.exists():
        shutil.rmtree(patch_dir)
    patch_dir.mkdir(parents=True, exist_ok=True)

    full_manifest = _file_manifest(full_dir, include_venv=include_venv, include_runtime=False)
    base_manifest = _file_manifest(baseline_dir, include_venv=include_venv, include_runtime=False)
    patch_snapshot_files = sorted(
        rel for rel in full_manifest.keys() if not _should_exclude_from_patch(Path(rel))
    )
    deleted = sorted(
        rel
        for rel in base_manifest.keys()
        if rel not in full_manifest and not _should_exclude_from_patch(Path(rel))
    )

    for rel in patch_snapshot_files:
        src = full_dir / rel
        dst = patch_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)

    return len(patch_snapshot_files), 0, len(deleted), deleted


def _calc_dir_hash(dir_path: Path) -> str:
    if not dir_path.exists():
        return ""
    hasher = hashlib.sha256()
    for path in sorted(dir_path.rglob("*")):
        if not path.is_file():
            continue
        rel = str(path.relative_to(dir_path)).replace("\\", "/")
        hasher.update(rel.encode("utf-8"))
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)
    return hasher.hexdigest()


def _write_latest_manifest(
    *,
    patch_dir: Path,
    patch_zip: Path,
    repo_url: str,
    branch: str,
    subdir: str,
    build_meta: dict,
    patch_meta: dict,
) -> tuple[Path, dict]:
    patch_repo_rel = f"{str(subdir).strip().strip('/')}/{patch_zip.name}"
    manifest = {
        "app_name": APP_NAME,
        "target_version": build_meta.get("build_id", ""),
        "major_version": int(build_meta.get("major_version", 0) or 0),
        "target_patch_version": patch_meta.get("target_patch_version", 0),
        "target_release_revision": int(build_meta.get("release_revision", 0) or 0),
        "target_display_version": build_meta.get("display_version", ""),
        "zip_url": _to_raw_url(repo_url, branch, patch_repo_rel),
        "zip_sha256": _sha256_file(patch_zip),
        "zip_size": int(patch_zip.stat().st_size),
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "patch_meta_path": "patch_meta.json",
        "dependency_manifest_path": str(patch_meta.get("dependency_manifest_path", "") or "").strip(),
        "dependency_install_policy": str(patch_meta.get("dependency_install_policy", "") or "").strip(),
    }
    path = patch_dir / "latest_patch.json"
    _write_json(path, manifest)
    return path, manifest


def _build_versioned_patch_zip_name(*, target_patch_version: int, target_release_revision: int) -> str:
    patch_version = max(0, int(target_patch_version or 0))
    release_revision = max(0, int(target_release_revision or 0))
    return f"QJPT_patch_only_p{patch_version}_r{release_revision}.zip"


def _ensure_local_git_identity(repo_dir: Path) -> None:
    name_ret = _run_cmd(["git", "config", "user.name"], cwd=repo_dir)
    email_ret = _run_cmd(["git", "config", "user.email"], cwd=repo_dir)
    user_name = str(name_ret.stdout or "").strip() if name_ret.returncode == 0 else ""
    user_email = str(email_ret.stdout or "").strip() if email_ret.returncode == 0 else ""
    if not user_name:
        set_name = _run_cmd(["git", "config", "user.name", "QJPT Builder"], cwd=repo_dir)
        if set_name.returncode != 0:
            raise RuntimeError(f"git config user.name 失败: {set_name.stderr or set_name.stdout}")
    if not user_email:
        set_email = _run_cmd(["git", "config", "user.email", "qjpt-builder@localhost"], cwd=repo_dir)
        if set_email.returncode != 0:
            raise RuntimeError(f"git config user.email 失败: {set_email.stderr or set_email.stdout}")


def _upload_to_gitee(
    *,
    patch_zip: Path,
    latest_manifest_path: Path,
    repo_url: str,
    branch: str,
    subdir: str,
    manifest_repo_path: str,
    dry_run: bool,
) -> str:
    if dry_run:
        log(
            "dry-run: 跳过上传，仅输出目标路径: "
            f"{subdir}/{patch_zip.name}, {manifest_repo_path}"
        )
        return branch

    def _run_git_with_retry(
        cmd: list[str],
        *,
        cwd: Path | None = None,
        op_name: str,
        max_attempts: int = 3,
    ) -> subprocess.CompletedProcess[str]:
        wait_sec = 2
        transient_keywords = (
            "failed to connect",
            "could not connect to server",
            "operation timed out",
            "timed out",
            "connection reset",
            "connection was reset",
            "ssl",
            "eof",
            "network is unreachable",
            "temporary failure",
        )
        last_ret: subprocess.CompletedProcess[str] | None = None
        for attempt in range(1, max_attempts + 1):
            ret = _run_cmd(cmd, cwd=cwd)
            last_ret = ret
            if ret.returncode == 0:
                return ret
            combined = ((ret.stderr or "") + "\n" + (ret.stdout or "")).lower()
            is_transient = any(key in combined for key in transient_keywords)
            if attempt < max_attempts and is_transient:
                log(f"{op_name} 网络异常，第{attempt}/{max_attempts}次失败，{wait_sec}s 后重试")
                time.sleep(wait_sec)
                wait_sec *= 2
                continue
            return ret
        return last_ret if last_ret is not None else _run_cmd(cmd, cwd=cwd)

    with tempfile.TemporaryDirectory(prefix="qjpt_patch_push_") as td:
        work = Path(td) / "repo"
        used_branch = str(branch or "").strip()
        clone_ret = _run_git_with_retry(
            ["git", "clone", "-b", used_branch, repo_url, str(work)],
            op_name="git clone",
        )
        if clone_ret.returncode != 0:
            clone_msg = (clone_ret.stderr or "") + (clone_ret.stdout or "")
            branch_not_found = "remote branch" in clone_msg.lower() and "not found" in clone_msg.lower()
            if branch_not_found:
                fallback_branch = _detect_remote_default_branch(repo_url)
                if not fallback_branch:
                    for cand in ("main", "master"):
                        if cand != used_branch and _remote_branch_exists(repo_url, cand):
                            fallback_branch = cand
                            break
                if fallback_branch and fallback_branch != used_branch:
                    log(f"git clone 分支不存在，自动回退分支: {used_branch} -> {fallback_branch}")
                    used_branch = fallback_branch
                    clone_ret = _run_git_with_retry(
                        ["git", "clone", "-b", used_branch, repo_url, str(work)],
                        op_name="git clone",
                    )
            if clone_ret.returncode != 0:
                raise RuntimeError(f"git clone 失败: {clone_ret.stderr or clone_ret.stdout}")

        _ensure_local_git_identity(work)

        patch_repo_dir = work / Path(subdir)
        patch_repo_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(patch_zip, patch_repo_dir / patch_zip.name)

        manifest_repo_file = work / Path(manifest_repo_path)
        manifest_repo_file.parent.mkdir(parents=True, exist_ok=True)
        manifest_payload = _read_json(latest_manifest_path)
        zip_url = str(manifest_payload.get("zip_url", "") or "")
        marker = f"/raw/{branch}/"
        if used_branch != branch and marker in zip_url:
            manifest_payload["zip_url"] = zip_url.replace(marker, f"/raw/{used_branch}/", 1)
        _write_json(manifest_repo_file, manifest_payload)

        add_ret = _run_cmd(["git", "add", "."], cwd=work)
        if add_ret.returncode != 0:
            raise RuntimeError(f"git add 失败: {add_ret.stderr or add_ret.stdout}")

        commit_msg = f"chore: publish {APP_NAME} patch {time.strftime('%Y-%m-%d %H:%M:%S')}"
        commit_ret = _run_cmd(["git", "commit", "-m", commit_msg], cwd=work)
        if commit_ret.returncode != 0:
            combined = (commit_ret.stderr or "") + (commit_ret.stdout or "")
            if "nothing to commit" in combined.lower():
                log("Gitee 上传: 无变更可提交")
                return used_branch
            raise RuntimeError(f"git commit 失败: {combined}")

        target_push_branch = used_branch if used_branch else branch
        push_ret = _run_git_with_retry(
            ["git", "push", "origin", target_push_branch],
            cwd=work,
            op_name="git push",
        )
        if push_ret.returncode != 0:
            raise RuntimeError(f"git push 失败: {push_ret.stderr or push_ret.stdout}")
    return used_branch


def _detect_major_patch(baseline_meta: dict) -> tuple[int, int]:
    major = int(baseline_meta.get("major_version", DEFAULT_MAJOR_VERSION) or DEFAULT_MAJOR_VERSION)
    patch = int(baseline_meta.get("patch_version", 0) or 0)
    return major, patch


def _build_launcher_content(*, code_dir_expr: str, pip_index_url: str, pip_trusted_host: str) -> str:
    return (
        "@echo off\n"
        "setlocal EnableDelayedExpansion\n"
        "title QJPT Web Console Log Window\n"
        f"{code_dir_expr}\n"
        f"set \"PIP_INDEX_URL={pip_index_url}\"\n"
        f"set \"PIP_TRUSTED_HOST={pip_trusted_host}\"\n"
        "set \"PYTHON_EXE=%CD%\\runtime\\python\\python.exe\"\n"
        "echo [INFO] Web console log window is open.\n"
        "echo [INFO] Startup is checking runtime dependencies. First launch may take several minutes.\n"
        "echo [INFO] Keep this window open. Press Ctrl+C to stop.\n"
        "if exist \"%PYTHON_EXE%\" goto run_main\n"
        "set \"PYTHON_EXE=%CD%\\.venv\\Scripts\\python.exe\"\n"
        "if exist \"%PYTHON_EXE%\" goto run_main\n"
        "echo [ERROR] Python runtime not found in project folder.\n"
        "echo [ERROR] Please restore runtime\\python or .venv inside the project directory.\n"
        "pause\n"
        "exit /b 1\n"
        ":run_main\n"
        "\"%PYTHON_EXE%\" -u \"portable_launcher.py\" %*\n"
        "set \"EXIT_CODE=%ERRORLEVEL%\"\n"
        "echo.\n"
        "echo [INFO] Program exited with code %EXIT_CODE%.\n"
        "pause\n"
        "endlocal & exit /b %EXIT_CODE%\n"
    )


def _write_launcher(release_root: Path) -> Path:
    launcher = release_root / LAUNCHER_NAME
    content = _build_launcher_content(
        code_dir_expr=f"cd /d \"%~dp0{RELEASE_CODE_DIR_NAME}\"",
        pip_index_url=DEFAULT_PIP_INDEX_URL,
        pip_trusted_host=DEFAULT_PIP_TRUSTED_HOST,
    )
    launcher.write_text(content, encoding="utf-8")
    return launcher


def _write_code_launcher(code_dir: Path) -> Path:
    launcher = code_dir / CODE_LAUNCHER_NAME
    content = _build_launcher_content(
        code_dir_expr="cd /d \"%~dp0\"",
        pip_index_url=DEFAULT_PIP_INDEX_URL,
        pip_trusted_host=DEFAULT_PIP_TRUSTED_HOST,
    )
    launcher.write_text(content, encoding="utf-8")
    return launcher


def _resolve_baseline_dir(raw: str, default_code_dir: Path) -> Path:
    if not str(raw or "").strip():
        return default_code_dir
    p = Path(raw).resolve()
    if (p / "build_meta.json").exists():
        return p
    code_child = p / RELEASE_CODE_DIR_NAME
    if (code_child / "build_meta.json").exists():
        return code_child
    raise FileNotFoundError(f"baseline 不存在或不包含 build_meta.json: {p}")


def _safe_unlink(path: Path) -> bool:
    try:
        if path.is_dir():
            path.rmdir()
        else:
            path.unlink()
        return True
    except Exception:  # noqa: BLE001
        return False


def _should_preserve_release_user_path(rel: Path) -> bool:
    rel_text = str(rel).replace("\\", "/")
    if not rel.parts:
        return False
    if rel.parts[0] in PRESERVE_RELEASE_TOP_LEVEL_DIRS:
        return True
    if rel.name in PATCH_EXCLUDE_FILE_NAMES:
        return True
    if rel.suffix.lower() == ".json" and "config" in rel.parts:
        return True
    return False


def _remove_extra_paths_best_effort(
    root: Path,
    keep_rel_set: set[str],
    *,
    preserve_user_data: bool = False,
) -> tuple[int, int]:
    removed = 0
    skipped = 0
    if not root.exists():
        return removed, skipped
    items = sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True)
    for path in items:
        rel = str(path.relative_to(root)).replace("\\", "/")
        if rel in keep_rel_set:
            continue
        rel_path = Path(rel.replace("/", "\\"))
        if preserve_user_data and _should_preserve_release_user_path(rel_path):
            continue
        # keep parent dirs that still have tracked children
        if path.is_dir():
            has_tracked_child = any(k.startswith(rel + "/") for k in keep_rel_set)
            if has_tracked_child:
                continue
            if preserve_user_data:
                has_preserved_child = any(
                    _should_preserve_release_user_path(Path(k.replace("/", "\\"))) and k.startswith(rel + "/")
                    for k in keep_rel_set
                )
                if has_preserved_child:
                    continue
        if _safe_unlink(path):
            removed += 1
        else:
            skipped += 1
    return removed, skipped


def _sync_stage_to_release(
    stage_code_dir: Path,
    release_code_dir: Path,
    *,
    preserve_user_data: bool = False,
) -> dict[str, int]:
    copied = 0
    skipped = 0
    release_code_dir.mkdir(parents=True, exist_ok=True)

    stage_files = [p for p in stage_code_dir.rglob("*") if p.is_file()]
    keep_rel_set: set[str] = set()
    for src in stage_files:
        rel = src.relative_to(stage_code_dir)
        rel_text = str(rel).replace("\\", "/")
        keep_rel_set.add(rel_text)
        dst = release_code_dir / rel
        if preserve_user_data and dst.exists() and _should_preserve_release_user_path(rel):
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(src, dst)
            copied += 1
        except PermissionError:
            skipped += 1
        except OSError:
            skipped += 1

    removed, remove_skipped = _remove_extra_paths_best_effort(
        release_code_dir,
        keep_rel_set,
        preserve_user_data=preserve_user_data,
    )
    skipped += remove_skipped
    return {"copied": copied, "removed": removed, "skipped": skipped}


def _sync_critical_stage_files_to_release(stage_code_dir: Path, release_code_dir: Path) -> dict[str, int]:
    copied = 0
    skipped = 0
    release_code_dir.mkdir(parents=True, exist_ok=True)
    for rel in CRITICAL_RELEASE_SYNC_FILES:
        src = stage_code_dir / rel
        if not src.exists():
            skipped += 1
            continue
        dst = release_code_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copy2(src, dst)
            copied += 1
        except PermissionError:
            skipped += 1
        except OSError:
            skipped += 1
    return {"copied": copied, "skipped": skipped}


def main() -> None:
    parser = argparse.ArgumentParser(description="构建全景月报便携目录 + patch 补丁")
    parser.add_argument("--name", default="", help="兼容参数：当前固定输出目录 QJPT_V3")
    parser.add_argument("--baseline", default="", help="用于生成 patch 的基线目录（可选）")
    parser.add_argument("--baseline-meta", default="", help="基线 build_meta.json 路径")
    parser.add_argument("--gitee-repo", default=DEFAULT_GITEE_REPO, help="Gitee 仓库地址")
    parser.add_argument("--gitee-branch", default=DEFAULT_GITEE_BRANCH, help="Gitee 分支（默认 master，不存在时自动回退远端默认分支）")
    parser.add_argument("--gitee-subdir", default=DEFAULT_GITEE_SUBDIR, help="补丁 zip 上传子目录")
    parser.add_argument("--gitee-manifest-path", default=DEFAULT_GITEE_MANIFEST_PATH, help="latest_patch.json 在仓库中的路径")
    parser.add_argument("--strict-min-version", action="store_true", help="patch_meta 写入严格 min_version")
    parser.add_argument("--patch-include-venv", action="store_true", help="patch 包含 .venv")
    parser.add_argument("--offline", action="store_true", help="全量包包含 .venv（离线可运行）")
    parser.add_argument("--lite", action="store_true", help="轻量包（默认）：不包含 .venv，首次运行自动安装依赖")
    parser.add_argument(
        "--embed-python-version",
        default=DEFAULT_EMBED_PY_VERSION,
        help=f"内置 Python 版本（默认 {DEFAULT_EMBED_PY_VERSION}）",
    )
    parser.add_argument("--force-full", action="store_true", help="强制重建全量基线目录（QJPT_V3）")
    parser.add_argument("--skip-runtime-deps", action="store_true", help="跳过运行时依赖 smoke import 检查")
    parser.add_argument("--dry-run", action="store_true", help="仅构建本地文件，不执行远端上传")
    parser.add_argument("--legacy-onefile", action="store_true", help="兼容旧参数（已不再使用 onefile）")
    args = parser.parse_args()

    if args.legacy_onefile:
        log("legacy-onefile 已弃用：当前流程固定为便携目录 + patch。")
    if str(args.name or "").strip():
        log(f"--name 已忽略，固定输出目录: {RELEASE_ROOT_NAME}")

    package_mode = "offline" if bool(args.offline) else "lite"
    if bool(args.lite):
        package_mode = "lite"
    embed_python_version = str(args.embed_python_version or "").strip() or DEFAULT_EMBED_PY_VERSION
    include_venv_in_full = package_mode == "offline"
    log(f"打包模式: {package_mode}, embed_python={embed_python_version}")

    if not args.skip_runtime_deps:
        _ensure_smoke_imports()

    build_frontend_script = PROJECT_ROOT / "scripts" / "build_frontend.py"
    if build_frontend_script.exists():
        ret = _run_cmd([sys.executable, str(build_frontend_script)], cwd=PROJECT_ROOT)
        if ret.returncode != 0:
            raise RuntimeError(f"前端资源同步失败: {ret.stderr or ret.stdout}")
        log("前端资源已同步到 dist")

    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    release_root = BUILD_DIR / RELEASE_ROOT_NAME
    release_code_dir = release_root / RELEASE_CODE_DIR_NAME
    patch_dir = BUILD_DIR / PATCH_DIR_NAME

    persisted_major, persisted_patch, persisted_release_revision = _pick_persisted_version()
    first_full_build = bool(args.force_full) or not (release_code_dir / "build_meta.json").exists()

    if first_full_build:
        preserved_user_config = _capture_existing_user_config(release_code_dir)
        copied_count, skipped_count = _copy_project_tree(
            release_code_dir,
            include_venv=include_venv_in_full,
            clean_before_copy=True,
        )
        preserved_config_restored = _restore_existing_user_config(release_code_dir, preserved_user_config)
        if not preserved_config_restored:
            _force_console_host_for_packaged_config(release_code_dir)
        _prepare_embedded_runtime(release_code_dir, embed_python_version)
        _ensure_release_tree_imports(release_code_dir)
        _ensure_embedded_runtime_bootstrap_imports(release_code_dir)
        if include_venv_in_full:
            _ensure_packaged_runtime_imports(release_code_dir)
        dependency_lock = _write_runtime_dependency_lock(
            release_code_dir,
            python_version=embed_python_version,
        )
        build_meta = _write_build_meta(
            release_code_dir,
            build_id=RELEASE_ROOT_NAME,
            major_version=max(DEFAULT_MAJOR_VERSION, persisted_major),
            patch_version=max(1, persisted_patch),
            release_revision=max(1, persisted_release_revision or persisted_patch or 1),
            venv_hash=_calc_dir_hash(release_code_dir / ".venv"),
            dependency_lock_hash=hashlib.sha256(
                json.dumps(dependency_lock, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest(),
        )
        _write_project_version_state(
            major_version=int(build_meta.get("major_version", DEFAULT_MAJOR_VERSION) or DEFAULT_MAJOR_VERSION),
            patch_version=int(build_meta.get("patch_version", 1) or 1),
            release_revision=int(
                build_meta.get(
                    "release_revision",
                    max(1, persisted_release_revision or persisted_patch or 1),
                )
                or max(1, persisted_release_revision or persisted_patch or 1)
            ),
            display_version=str(build_meta.get("display_version", "") or ""),
        )
        launcher = _write_launcher(release_root)
        code_launcher = _write_code_launcher(release_code_dir)
        log(
            "首次构建完成（全量便携目录）: "
            f"root={release_root}, code={release_code_dir}, launcher={launcher.name}, code_launcher={code_launcher.name}, files={copied_count}, "
            f"skipped={skipped_count}, version={build_meta.get('display_version')}, mode={package_mode}, "
            f"preserved_user_config={preserved_config_restored}"
        )
        log("当前未生成补丁；后续再次执行 build_exe.py 将只生成 patch_only 并上传。")
        return

    baseline_dir = _resolve_baseline_dir(args.baseline, release_code_dir)

    baseline_meta = {}
    if str(args.baseline_meta).strip():
        baseline_meta = _read_json(Path(args.baseline_meta).resolve())
    if not baseline_meta:
        baseline_meta = _read_json(baseline_dir / "build_meta.json")

    major, base_patch = _detect_major_patch(baseline_meta)
    major = max(major, persisted_major)
    base_patch = max(base_patch, persisted_patch)
    target_patch = base_patch + 1
    base_release_revision = int(
        baseline_meta.get(
            "release_revision",
            persisted_release_revision or base_patch or 0,
        )
        or persisted_release_revision
        or base_patch
        or 0
    )
    target_release_revision = max(base_release_revision, persisted_release_revision, base_patch, 0) + 1

    with tempfile.TemporaryDirectory(prefix="qjpt_stage_") as td:
        stage_root = Path(td)
        stage_code_dir = stage_root / RELEASE_CODE_DIR_NAME
        copied_count, skipped_count = _copy_project_tree(
            stage_code_dir,
            include_venv=bool(args.patch_include_venv),
            clean_before_copy=True,
        )
        _force_console_host_for_packaged_config(stage_code_dir)
        _write_code_launcher(stage_code_dir)
        log(f"代码快照完成: {stage_code_dir} (files={copied_count}, skipped={skipped_count})")

        dependency_lock = _write_runtime_dependency_lock(
            stage_code_dir,
            python_version=embed_python_version,
        )
        build_meta = _write_build_meta(
            stage_code_dir,
            build_id=RELEASE_ROOT_NAME,
            major_version=major,
            patch_version=target_patch,
            release_revision=target_release_revision,
            venv_hash=_calc_dir_hash(stage_code_dir / ".venv"),
            dependency_lock_hash=hashlib.sha256(
                json.dumps(dependency_lock, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest(),
        )
        _write_project_version_state(
            major_version=int(build_meta.get("major_version", major) or major),
            patch_version=int(build_meta.get("patch_version", target_patch) or target_patch),
            release_revision=int(build_meta.get("release_revision", target_release_revision) or target_release_revision),
            display_version=str(build_meta.get("display_version", "") or ""),
        )

        packaged_count, changed_placeholder, deleted, deleted_files = _build_patch_only(
            full_dir=stage_code_dir,
            baseline_dir=baseline_dir,
            patch_dir=patch_dir,
            include_venv=bool(args.patch_include_venv),
        )
        patch_meta = {
            "app_name": APP_NAME,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "min_version": str(baseline_meta.get("build_id", "")) if args.strict_min_version else "",
            "target_version": RELEASE_ROOT_NAME,
            "major_version": major,
            "base_patch_version": base_patch,
            "target_patch_version": target_patch,
            "target_release_revision": target_release_revision,
            "target_display_version": build_meta.get("display_version", ""),
            "include_venv": bool(args.patch_include_venv),
            "dependency_manifest_path": "runtime_dependency_lock.json",
            "dependency_install_policy": "online_only_exact",
            "required_packages": list(dependency_lock.get("packages", [])),
            "python_version": embed_python_version,
            "deleted_files": deleted_files,
            "stats": {"packaged": packaged_count, "changed": changed_placeholder, "deleted": deleted},
            "mode": "full_code_snapshot_excluding_config",
        }
        _write_json(patch_dir / "patch_meta.json", patch_meta)
        log(f"patch_only 构建完成: {patch_dir} (packaged={packaged_count}, deleted={deleted})")

        effective_branch = _resolve_upload_branch(args.gitee_repo, args.gitee_branch)
        if effective_branch != args.gitee_branch:
            log(f"上传使用分支: {effective_branch}（请求值: {args.gitee_branch}）")
        else:
            log(f"上传使用分支: {effective_branch}")

        patch_zip_name = _build_versioned_patch_zip_name(
            target_patch_version=target_patch,
            target_release_revision=target_release_revision,
        )
        patch_zip = _zip_dir(patch_dir, patch_zip_name)
        log(f"patch zip: {patch_zip}")
        latest_manifest_path, latest_manifest = _write_latest_manifest(
            patch_dir=patch_dir,
            patch_zip=patch_zip,
            repo_url=args.gitee_repo,
            branch=effective_branch,
            subdir=args.gitee_subdir,
            build_meta=build_meta,
            patch_meta=patch_meta,
        )
        log(f"manifest: {latest_manifest_path}")
        log(
            "manifest summary: "
            f"target={latest_manifest.get('target_version')}, "
            f"patch={latest_manifest.get('target_patch_version')}, "
            f"url={latest_manifest.get('zip_url')}"
        )
        synced_release = _sync_stage_to_release(
            stage_code_dir,
            release_code_dir,
            preserve_user_data=True,
        )
        _write_code_launcher(release_code_dir)
        _write_launcher(release_root)
        _prepare_embedded_runtime(release_code_dir, embed_python_version)
        _ensure_release_tree_imports(release_code_dir)
        _ensure_embedded_runtime_bootstrap_imports(release_code_dir)
        log(
            "检测到基线大版本已存在：本次仍以 patch_only 为主，但会全量同步代码到本地目录，同时保留用户配置与运行态数据。"
            f" copied={synced_release.get('copied', 0)}, removed={synced_release.get('removed', 0)}, skipped={synced_release.get('skipped', 0)}"
        )

        try:
            uploaded_branch = _upload_to_gitee(
                patch_zip=patch_zip,
                latest_manifest_path=latest_manifest_path,
                repo_url=args.gitee_repo,
                branch=effective_branch,
                subdir=args.gitee_subdir,
                manifest_repo_path=args.gitee_manifest_path,
                dry_run=bool(args.dry_run),
            )
            if uploaded_branch and uploaded_branch != effective_branch:
                marker = f"/raw/{effective_branch}/"
                url = str(latest_manifest.get("zip_url", "") or "")
                if marker in url:
                    latest_manifest["zip_url"] = url.replace(marker, f"/raw/{uploaded_branch}/", 1)
                    _write_json(latest_manifest_path, latest_manifest)
                    log(f"本地 manifest 已同步上传分支: {uploaded_branch}")
            if args.dry_run:
                log("dry-run 完成，未执行远端提交")
            else:
                log(f"已上传 patch 与 latest manifest 到 Gitee: branch={uploaded_branch}")
        except Exception as exc:  # noqa: BLE001
            log(f"Gitee 上传失败（已保留本地产物）: {exc}")

    log("完成")


if __name__ == "__main__":
    main()

