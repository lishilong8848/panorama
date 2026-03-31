from __future__ import annotations

import importlib
import importlib.metadata
import importlib.util
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List

from pipeline_utils import get_app_dir

from app.shared.runtime_dependency_spec import normalized_runtime_dependency_specs
from app.shared.utils.runtime_temp_workspace import cleanup_runtime_temp_dir, create_runtime_temp_dir


DEFAULT_PIP_INDEX_URL = "https://pypi.tuna.tsinghua.edu.cn/simple"
DEFAULT_PIP_TRUSTED_HOST = "pypi.tuna.tsinghua.edu.cn"
DEFAULT_GET_PIP_MIRRORS = [
    "https://mirrors.aliyun.com/pypi/get-pip.py",
    "https://bootstrap.pypa.io/get-pip.py",
]


class RuntimeDependencySyncService:
    def __init__(
        self,
        *,
        app_dir: Path | None = None,
        runtime_state_root: str | None = None,
        emit_log: Callable[[str], None] | None = None,
        python_executable: str | None = None,
        pip_index_url: str = DEFAULT_PIP_INDEX_URL,
        pip_trusted_host: str = DEFAULT_PIP_TRUSTED_HOST,
        get_pip_mirrors: List[str] | None = None,
    ) -> None:
        self.app_dir = Path(app_dir) if app_dir else get_app_dir()
        self.runtime_state_root = str(runtime_state_root or "").strip()
        self.emit_log = emit_log or (lambda _text: None)
        self.python_executable = str(python_executable or sys.executable).strip() or sys.executable
        self.pip_index_url = str(pip_index_url or DEFAULT_PIP_INDEX_URL).strip() or DEFAULT_PIP_INDEX_URL
        self.pip_trusted_host = str(pip_trusted_host or DEFAULT_PIP_TRUSTED_HOST).strip() or DEFAULT_PIP_TRUSTED_HOST
        self.get_pip_mirrors = list(get_pip_mirrors or DEFAULT_GET_PIP_MIRRORS)

    def _log(self, text: str) -> None:
        self.emit_log(f"[依赖同步] {text}")

    def _runtime_config_for_temp(self) -> Dict[str, Dict[str, str]] | None:
        if not self.runtime_state_root:
            return None
        return {"paths": {"runtime_state_root": self.runtime_state_root}}

    def default_lock_path(self) -> Path:
        return self.app_dir / "runtime_dependency_lock.json"

    def load_lock_file(self, lock_path: Path | str | None = None) -> Dict[str, Any]:
        path = Path(lock_path) if lock_path else self.default_lock_path()
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _normalize_required_packages(self, packages: List[Dict[str, Any]] | None) -> List[Dict[str, str]]:
        normalized: List[Dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for item in packages or []:
            if not isinstance(item, dict):
                continue
            package = str(item.get("package", "") or "").strip()
            import_name = str(item.get("import_name", "") or "").strip()
            version = str(item.get("version", "") or "").strip()
            if not package or not import_name:
                continue
            key = (package, import_name)
            if key in seen:
                continue
            seen.add(key)
            normalized.append(
                {
                    "package": package,
                    "import_name": import_name,
                    "version": version,
                }
            )
        return normalized

    def _fallback_required_packages(self) -> List[Dict[str, str]]:
        return [
            {
                "package": spec["package"],
                "import_name": spec["import_name"],
                "version": "",
            }
            for spec in normalized_runtime_dependency_specs()
        ]

    def _find_import(self, import_name: str) -> bool:
        return importlib.util.find_spec(str(import_name or "").strip()) is not None

    def _installed_version(self, package: str) -> str:
        try:
            return str(importlib.metadata.version(str(package or "").strip()) or "").strip()
        except importlib.metadata.PackageNotFoundError:
            return ""
        except Exception:
            return ""

    def _run_pip(self, args: List[str]) -> tuple[bool, str]:
        def _build_env(*, strip_proxy: bool) -> dict[str, str]:
            env = os.environ.copy()
            if not str(env.get("PIP_INDEX_URL", "")).strip():
                env["PIP_INDEX_URL"] = self.pip_index_url
            if not str(env.get("PIP_TRUSTED_HOST", "")).strip():
                env["PIP_TRUSTED_HOST"] = self.pip_trusted_host
            if strip_proxy:
                for key in (
                    "HTTP_PROXY",
                    "HTTPS_PROXY",
                    "ALL_PROXY",
                    "http_proxy",
                    "https_proxy",
                    "all_proxy",
                ):
                    env.pop(key, None)
            return env

        command = [self.python_executable, "-m", "pip", *args]
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
            env=_build_env(strip_proxy=False),
        )
        if result.returncode == 0:
            return True, ""

        detail = (result.stderr or result.stdout or "").strip()
        lowered = detail.lower()
        proxy_failure = "proxyerror" in lowered or "cannot connect to proxy" in lowered
        if proxy_failure:
            self._log("检测到 pip 代理连接失败，改为直连镜像重试一次。")
            retry_result = subprocess.run(
                command,
                text=True,
                capture_output=True,
                check=False,
                env=_build_env(strip_proxy=True),
            )
            if retry_result.returncode == 0:
                return True, ""
            detail = (retry_result.stderr or retry_result.stdout or "").strip()
        return False, detail

    @staticmethod
    def _extract_failure_reason(detail: str) -> str:
        lines = [str(line or "").strip() for line in str(detail or "").splitlines() if str(line or "").strip()]
        if not lines:
            return "未知错误"
        for line in reversed(lines):
            lowered = line.lower()
            if "error:" in lowered or "proxyerror" in lowered or "timed out" in lowered:
                return line
        return lines[-1]

    def _format_install_failure(self, requirement: str, detail: str) -> str:
        lowered = str(detail or "").lower()
        reason = self._extract_failure_reason(detail)
        if "proxyerror" in lowered or "cannot connect to proxy" in lowered:
            advice = "检测到代理连接失败。请检查系统代理或 VPN 设置，或暂时关闭代理后重试。"
        elif "timed out" in lowered or "handshake operation timed out" in lowered or "read timed out" in lowered:
            advice = "检测到网络超时。请确认当前网络可访问 PIP 镜像，稍后重试。"
        elif "no matching distribution found" in lowered or "could not find a version that satisfies" in lowered:
            advice = "镜像未返回所需依赖版本。请检查网络、镜像可用性，稍后重试。"
        else:
            advice = "请检查当前网络、代理/VPN 设置以及 PIP 镜像连通性后重试。"
        return f"安装依赖失败 {requirement}。原因：{reason} 建议：{advice}"

    def _is_pip_available(self) -> bool:
        result = subprocess.run(
            [self.python_executable, "-m", "pip", "--version"],
            text=True,
            capture_output=True,
            check=False,
        )
        return result.returncode == 0

    def ensure_pip_available(self) -> None:
        if self._is_pip_available():
            return
        self._log("未检测到 pip，正在初始化运行时安装能力，这可能需要几十秒。")
        ensurepip_result = subprocess.run(
            [self.python_executable, "-m", "ensurepip", "--upgrade"],
            text=True,
            capture_output=True,
            check=False,
        )
        if ensurepip_result.returncode == 0 and self._is_pip_available():
            self._log("已通过 ensurepip 初始化 pip。")
            return

        last_error = (ensurepip_result.stderr or ensurepip_result.stdout or "").strip()
        for url in self.get_pip_mirrors:
            temp_dir: Path | None = None
            try:
                temp_dir = create_runtime_temp_dir(
                    kind="bootstrap_get_pip",
                    runtime_config=self._runtime_config_for_temp(),
                    app_dir=self.app_dir,
                )
                script_path = temp_dir / "get-pip.py"
                with urllib.request.urlopen(url, timeout=45) as response:
                    script_path.write_bytes(response.read())
                result = subprocess.run(
                    [self.python_executable, str(script_path), "--disable-pip-version-check"],
                    text=True,
                    capture_output=True,
                    check=False,
                )
                if result.returncode == 0 and self._is_pip_available():
                    self._log(f"已通过 get-pip 恢复 pip: {url}")
                    return
                last_error = (result.stderr or result.stdout or "").strip()
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_error = str(exc)
            finally:
                if temp_dir is not None:
                    cleanup_runtime_temp_dir(
                        temp_dir,
                        runtime_config=self._runtime_config_for_temp(),
                        app_dir=self.app_dir,
                    )
        raise RuntimeError(f"pip 自举失败: {last_error}")

    def _install_package(self, package: str, version: str = "") -> None:
        requirement = f"{package}=={version}" if version else package
        self._log(f"开始安装依赖: {requirement}")
        ok, detail = self._run_pip(["install", "--disable-pip-version-check", "--upgrade", requirement])
        if not ok:
            raise RuntimeError(self._format_install_failure(requirement, detail))
        importlib.invalidate_caches()
        self._log(f"依赖安装完成: {requirement}")

    def sync_required_packages(
        self,
        packages: List[Dict[str, Any]],
        *,
        exact_versions: bool,
    ) -> Dict[str, Any]:
        normalized = self._normalize_required_packages(packages)
        if not normalized:
            return {
                "status": "success",
                "installed": 0,
                "checked": 0,
                "packages": [],
                "exact_versions": exact_versions,
            }

        self._log(f"开始检查运行依赖: checked={len(normalized)}, exact_versions={exact_versions}")
        self.ensure_pip_available()

        installed = 0
        changed_packages: List[str] = []
        total = len(normalized)
        for index, item in enumerate(normalized, start=1):
            package = item["package"]
            import_name = item["import_name"]
            version = item["version"]
            import_ready = self._find_import(import_name)
            installed_version = self._installed_version(package)
            needs_install = not import_ready or not installed_version or (exact_versions and installed_version != version)
            if not needs_install:
                continue
            if exact_versions and not version:
                raise RuntimeError(f"依赖锁缺少准确版本: {package}")
            self._log(f"安装进度 [{index}/{total}]，准备安装: {package}")
            self._install_package(package, version if exact_versions else "")
            installed += 1
            changed_packages.append(f"{package}=={version}" if exact_versions and version else package)

        for item in normalized:
            package = item["package"]
            import_name = item["import_name"]
            version = item["version"]
            if not self._find_import(import_name):
                raise RuntimeError(f"依赖安装后仍无法导入: {import_name}")
            current_version = self._installed_version(package)
            if exact_versions and version and current_version != version:
                raise RuntimeError(
                    f"依赖版本校验失败: {package}, expected={version}, actual={current_version or '-'}"
                )

        return {
            "status": "success",
            "installed": installed,
            "checked": len(normalized),
            "packages": changed_packages,
            "exact_versions": exact_versions,
        }

    def sync_from_lock_file(self, lock_path: Path | str | None = None) -> Dict[str, Any]:
        payload = self.load_lock_file(lock_path)
        packages = payload.get("packages", []) if isinstance(payload, dict) else []
        result = self.sync_required_packages(packages if isinstance(packages, list) else [], exact_versions=True)
        result["lock_path"] = str(Path(lock_path) if lock_path else self.default_lock_path())
        return result

    def ensure_startup_dependencies(self, lock_path: Path | str | None = None) -> Dict[str, Any]:
        payload = self.load_lock_file(lock_path)
        packages = payload.get("packages", []) if isinstance(payload, dict) else []
        if isinstance(packages, list) and packages:
            return self.sync_required_packages(packages, exact_versions=True)
        return self.sync_required_packages(self._fallback_required_packages(), exact_versions=False)
