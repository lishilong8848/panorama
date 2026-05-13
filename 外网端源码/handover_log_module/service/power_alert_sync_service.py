from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

from pipeline_utils import get_app_dir


class PowerAlertSyncService:
    """Run the QJZS power-alert sync utility after daily branch source upload."""

    DEFAULT_REPO_URL = "https://github.com/frankmjy/QJZS-kw.git"
    DEFAULT_REPO_DIR = "../external_repos/QJZS-kw"
    DEFAULT_SCRIPT_PATH = "scripts/sync-power-alerts.js"
    DEFAULT_CONFIG_PATH = "config/power-alert-sync.config.json"

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config if isinstance(config, dict) else {}

    def _emit(self, emit_log: Callable[[str], None], text: str) -> None:
        try:
            emit_log(text)
        except Exception:  # noqa: BLE001
            pass

    def _cfg(self) -> Dict[str, Any]:
        features = self.config.get("features", {})
        features = features if isinstance(features, dict) else {}
        feature_branch_cfg = features.get("branch_power_upload", {})
        feature_branch_cfg = feature_branch_cfg if isinstance(feature_branch_cfg, dict) else {}
        branch_cfg = self.config.get("branch_power_upload", {})
        branch_cfg = branch_cfg if isinstance(branch_cfg, dict) else {}

        merged: Dict[str, Any] = {}
        feature_power_alert = feature_branch_cfg.get("power_alert_sync", {})
        if isinstance(feature_power_alert, dict):
            merged.update(feature_power_alert)
        branch_power_alert = branch_cfg.get("power_alert_sync", {})
        if isinstance(branch_power_alert, dict):
            merged.update(branch_power_alert)
        return merged

    @staticmethod
    def _bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on", "启用", "是"}:
            return True
        if text in {"0", "false", "no", "n", "off", "禁用", "否"}:
            return False
        return default

    @staticmethod
    def _as_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:  # noqa: BLE001
            return default

    @staticmethod
    def _tail(text: str, *, max_lines: int = 40) -> str:
        lines = [line.rstrip() for line in str(text or "").splitlines() if line.rstrip()]
        return "\n".join(lines[-max(1, max_lines):])

    @staticmethod
    def _failure_result(message: str) -> Dict[str, Any]:
        return {
            "ok": False,
            "status": "failed",
            "error": str(message or "").strip() or "动环功率统计同步失败",
        }

    def _resolve_path(self, raw_path: str, *, base_dir: Path | None = None) -> Path:
        text = str(raw_path or "").strip()
        if not text:
            return Path()
        candidate = Path(text)
        if candidate.is_absolute():
            return candidate
        candidates: List[Path] = []
        if base_dir is not None:
            candidates.append(base_dir / candidate)
        app_dir = get_app_dir()
        candidates.extend([app_dir / candidate, app_dir.parent / candidate, Path.cwd() / candidate])
        for item in candidates:
            if item.exists():
                return item
        return candidates[0] if candidates else candidate

    def _ensure_tool_repo(
        self,
        *,
        repo_dir: Path,
        cfg: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> str:
        if repo_dir.exists() and repo_dir.is_dir():
            return ""
        if not self._bool(cfg.get("auto_install"), True):
            return f"工具目录不存在: {repo_dir}"
        repo_url = str(cfg.get("repo_url", "") or self.DEFAULT_REPO_URL).strip() or self.DEFAULT_REPO_URL
        git_executable = str(cfg.get("git_executable", "") or "git").strip() or "git"
        install_timeout_sec = max(30, self._as_int(cfg.get("install_timeout_sec"), 180))
        try:
            repo_dir.parent.mkdir(parents=True, exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            return f"工具目录父目录创建失败: {repo_dir.parent}; {exc}"
        self._emit(
            emit_log,
            f"[动环功率统计同步] 工具目录不存在，开始自动拉取 repo={repo_url}, dir={repo_dir}",
        )
        try:
            completed = subprocess.run(
                [git_executable, "clone", "--depth", "1", repo_url, str(repo_dir)],
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=install_timeout_sec,
                check=False,
            )
        except FileNotFoundError:
            return f"自动拉取失败: 找不到 Git 可执行文件 {git_executable}"
        except subprocess.TimeoutExpired:
            return f"自动拉取超时: timeout_sec={install_timeout_sec}, repo={repo_url}"
        if completed.returncode != 0:
            stderr_tail = self._tail(completed.stderr, max_lines=12)
            stdout_tail = self._tail(completed.stdout, max_lines=12)
            detail = stderr_tail or stdout_tail or f"exit_code={completed.returncode}"
            return f"自动拉取失败: {detail}"
        self._emit(emit_log, f"[动环功率统计同步] 工具自动拉取完成 dir={repo_dir}")
        return ""

    def sync(
        self,
        *,
        report_date: str,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        cfg = self._cfg()
        if not self._bool(cfg.get("enabled"), True):
            return {"ok": True, "status": "skipped", "reason": "disabled"}

        required = self._bool(cfg.get("required"), False)
        dry_run = self._bool(cfg.get("dry_run"), False)
        timeout_sec = max(30, self._as_int(cfg.get("timeout_sec"), 900))
        node_executable = str(cfg.get("node_executable", "") or "node").strip() or "node"

        repo_dir = self._resolve_path(str(cfg.get("repo_dir", "") or self.DEFAULT_REPO_DIR))
        install_error = self._ensure_tool_repo(repo_dir=repo_dir, cfg=cfg, emit_log=emit_log)
        script_path = self._resolve_path(
            str(cfg.get("script_path", "") or self.DEFAULT_SCRIPT_PATH),
            base_dir=repo_dir,
        )
        config_path = self._resolve_path(
            str(cfg.get("config_path", "") or self.DEFAULT_CONFIG_PATH),
            base_dir=repo_dir,
        )

        missing: List[str] = []
        if install_error:
            missing.append(install_error)
        if (not repo_dir.exists() or not repo_dir.is_dir()) and not install_error:
            missing.append(f"工具目录不存在: {repo_dir}")
        if not script_path.exists() or not script_path.is_file():
            missing.append(f"脚本不存在: {script_path}")
        if not config_path.exists() or not config_path.is_file():
            missing.append(f"配置不存在: {config_path}")
        if missing:
            message = "；".join(missing)
            if required:
                raise RuntimeError(f"动环功率统计同步依赖缺失: {message}")
            self._emit(emit_log, f"[动环功率统计同步] 已跳过: {message}")
            return {
                "ok": True,
                "status": "skipped",
                "reason": "missing_dependency",
                "missing": missing,
            }

        date_arg = str(report_date or "").strip().replace("-", "/")
        command = [
            node_executable,
            str(script_path),
            "--date",
            date_arg,
            "--config",
            str(config_path),
        ]
        if dry_run:
            command.append("--dry-run")

        started = time.perf_counter()
        self._emit(
            emit_log,
            "[动环功率统计同步] 开始 "
            f"date={date_arg}, mode={'dry-run' if dry_run else 'write'}, cwd={repo_dir}",
        )
        try:
            completed = subprocess.run(
                command,
                cwd=str(repo_dir),
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=timeout_sec,
                check=False,
            )
        except FileNotFoundError as exc:
            message = f"动环功率统计同步失败: 找不到 Node 可执行文件 {node_executable}"
            if required:
                raise RuntimeError(message) from exc
            self._emit(emit_log, f"[动环功率统计同步] 已记录失败，不阻断主流程: {message}")
            return self._failure_result(message)
        except subprocess.TimeoutExpired as exc:
            message = f"动环功率统计同步超时: timeout_sec={timeout_sec}"
            if required:
                raise RuntimeError(message) from exc
            self._emit(emit_log, f"[动环功率统计同步] 已记录失败，不阻断主流程: {message}")
            return self._failure_result(message)

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        stdout_tail = self._tail(completed.stdout)
        stderr_tail = self._tail(completed.stderr)
        if stdout_tail:
            self._emit(emit_log, f"[动环功率统计同步][stdout]\n{stdout_tail}")
        if stderr_tail:
            self._emit(emit_log, f"[动环功率统计同步][stderr]\n{stderr_tail}")
        if completed.returncode != 0:
            message = (
                "动环功率统计同步失败: "
                f"exit_code={completed.returncode}, elapsed_ms={elapsed_ms}, "
                f"stderr={stderr_tail or '-'}"
            )
            if required:
                raise RuntimeError(message)
            self._emit(emit_log, f"[动环功率统计同步] 已记录失败，不阻断主流程: {message}")
            result = self._failure_result(message)
            result.update(
                {
                    "report_date": date_arg,
                    "dry_run": dry_run,
                    "elapsed_ms": elapsed_ms,
                    "stdout_tail": stdout_tail,
                    "stderr_tail": stderr_tail,
                }
            )
            return result

        self._emit(
            emit_log,
            f"[动环功率统计同步] 完成 date={date_arg}, elapsed_ms={elapsed_ms}",
        )
        return {
            "ok": True,
            "status": "success",
            "report_date": date_arg,
            "dry_run": dry_run,
            "elapsed_ms": elapsed_ms,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
        }
