from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

from app.shared.utils.atomic_file import (
    atomic_copy_file,
    atomic_write_text,
    validate_json_file,
    validate_non_empty_file,
)


LATEST_PATCH_NAME = "latest_patch.json"
PUBLISH_STATE_NAME = "publish_state.json"
UPDATER_ROOT_NAME = "updater"
APPROVED_DIR_NAME = "approved"
STAGING_DIR_NAME = "staging"


class SharedMirrorPendingError(RuntimeError):
    """Raised when the shared updater mirror has not been published yet."""


def _normalize_repo_url(repo_url: str) -> str:
    text = str(repo_url or "").strip().rstrip("/")
    if text.endswith(".git"):
        text = text[:-4]
    return text


def _to_raw_url(repo_url: str, branch: str, rel_path: str) -> str:
    base = _normalize_repo_url(repo_url)
    rel = str(rel_path).replace("\\", "/").lstrip("/")
    return f"{base}/raw/{branch}/{rel}"


def _with_cache_bust(url: str, token: str) -> str:
    parts = urlsplit(str(url or "").strip())
    query_items = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "_cb"]
    query_items.append(("_cb", str(token)))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query_items), parts.fragment))


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest().lower()


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_relative_path(value: str) -> Path:
    text = str(value or "").strip().replace("\\", "/").lstrip("/")
    path = Path(text)
    if not text or path.is_absolute() or ".." in path.parts:
        raise RuntimeError(f"共享镜像中的补丁路径不合法: {value}")
    return path


class ManifestClient:
    def __init__(
        self,
        *,
        repo_url: str,
        branch: str,
        manifest_path: str,
        timeout_sec: int = 20,
        retry_count: int = 3,
    ) -> None:
        self.repo_url = str(repo_url or "").strip()
        self.branch = str(branch or "master").strip() or "master"
        self.manifest_path = str(manifest_path or "").strip()
        self.timeout_sec = max(1, int(timeout_sec))
        self.retry_count = max(1, int(retry_count))

    @property
    def manifest_url(self) -> str:
        return _to_raw_url(self.repo_url, self.branch, self.manifest_path)

    def _candidate_manifest_urls(self) -> List[str]:
        branches: List[str] = []
        for item in [self.branch, "master", "main"]:
            branch = str(item or "").strip()
            if branch and branch not in branches:
                branches.append(branch)
        return [_to_raw_url(self.repo_url, branch, self.manifest_path) for branch in branches]

    def fetch_latest_manifest(self) -> Dict[str, Any]:
        last_error = ""
        for url in self._candidate_manifest_urls():
            for _ in range(self.retry_count):
                try:
                    response = requests.get(
                        url,
                        timeout=self.timeout_sec,
                        headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, dict):
                        raise RuntimeError("远端更新清单返回的不是 JSON 对象。")
                    return payload
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    time.sleep(1)
        raise RuntimeError(f"拉取 latest_patch.json 失败: {last_error}")

    def download_patch(self, zip_url: str, save_to: Path, *, expected_sha256: str = "") -> Path:
        save_to.parent.mkdir(parents=True, exist_ok=True)
        last_error = ""
        expected_sha = str(expected_sha256 or "").strip().lower()

        for attempt in range(1, self.retry_count + 1):
            request_url = zip_url if attempt == 1 else _with_cache_bust(zip_url, f"{int(time.time() * 1000)}-{attempt}")
            try:
                with requests.get(
                    request_url,
                    timeout=self.timeout_sec,
                    stream=True,
                    headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
                ) as response:
                    response.raise_for_status()
                    with save_to.open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                handle.write(chunk)

                if expected_sha:
                    actual_sha = _sha256_file(save_to)
                    if actual_sha != expected_sha:
                        try:
                            save_to.unlink(missing_ok=True)
                        except OSError:
                            pass
                        raise RuntimeError(
                            "补丁包 sha256 校验失败: "
                            f"expected={expected_sha}, actual={actual_sha}, attempt={attempt}"
                        )
                return save_to
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                try:
                    save_to.unlink(missing_ok=True)
                except OSError:
                    pass
                if attempt < self.retry_count:
                    time.sleep(min(8, 2 ** (attempt - 1)))
        raise RuntimeError(f"下载 patch 失败: {last_error}")


class SharedMirrorManifestClient:
    def __init__(self, shared_root: str | Path) -> None:
        self.shared_root = Path(shared_root).expanduser()

    @property
    def updater_root(self) -> Path:
        return self.shared_root / UPDATER_ROOT_NAME

    @property
    def approved_root(self) -> Path:
        return self.updater_root / APPROVED_DIR_NAME

    @property
    def staging_root(self) -> Path:
        return self.updater_root / STAGING_DIR_NAME

    @property
    def manifest_path(self) -> Path:
        return self.approved_root / LATEST_PATCH_NAME

    @property
    def publish_state_path(self) -> Path:
        return self.approved_root / PUBLISH_STATE_NAME

    def _default_publish_state(self) -> Dict[str, Any]:
        return {
            "mirror_ready": False,
            "mirror_version": "",
            "mirror_release_revision": 0,
            "last_publish_at": "",
            "last_publish_error": "",
            "mirror_manifest_path": str(self.manifest_path),
            "published_by_role": "",
            "published_by_node_id": "",
            "zip_relpath": "",
        }

    def load_publish_state(self) -> Dict[str, Any]:
        state = self._default_publish_state()
        if not self.publish_state_path.exists():
            return state
        try:
            payload = json.loads(self.publish_state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                state.update(payload)
        except Exception:  # noqa: BLE001
            pass
        state["mirror_manifest_path"] = str(self.manifest_path)
        return state

    def record_publish_error(
        self,
        error_text: str,
        *,
        published_by_role: str = "",
        published_by_node_id: str = "",
    ) -> Dict[str, Any]:
        payload = self.load_publish_state()
        payload.update(
            {
                "mirror_ready": False,
                "last_publish_error": str(error_text or "").strip(),
                "last_publish_at": _now_text(),
                "published_by_role": str(published_by_role or "").strip(),
                "published_by_node_id": str(published_by_node_id or "").strip(),
                "mirror_manifest_path": str(self.manifest_path),
            }
        )
        self.publish_state_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_text(
            self.publish_state_path,
            json.dumps(payload, ensure_ascii=False, indent=2),
            validator=validate_json_file,
        )
        return payload

    def _load_manifest_file(self) -> Dict[str, Any]:
        try:
            payload = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"读取共享目录更新清单失败: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("共享目录更新清单格式错误。")
        return payload

    @staticmethod
    def _normalize_zip_relpath(payload: Dict[str, Any]) -> str:
        zip_relpath = str(payload.get("zip_relpath", "") or "").strip()
        if not zip_relpath:
            zip_relpath = str(payload.get("zip_url", "") or "").strip()
        if not zip_relpath:
            raise RuntimeError("共享目录更新清单缺少 zip_relpath。")
        return str(_safe_relative_path(zip_relpath)).replace("\\", "/")

    def _build_ready_manifest(self) -> Dict[str, Any]:
        state = self.load_publish_state()
        if not self.publish_state_path.exists():
            raise SharedMirrorPendingError("共享目录中还没有已批准的更新版本。")
        if not bool(state.get("mirror_ready", False)):
            raise SharedMirrorPendingError("共享目录批准版本尚未完成发布。")
        if str(state.get("last_publish_error", "") or "").strip():
            raise SharedMirrorPendingError("共享目录批准版本发布异常，等待外网端重新发布。")
        if not self.manifest_path.exists():
            raise SharedMirrorPendingError("共享目录批准清单尚未完成发布。")

        payload = self._load_manifest_file()
        zip_relpath = self._normalize_zip_relpath(payload)
        state_zip_relpath = str(state.get("zip_relpath", "") or "").strip()
        if state_zip_relpath and state_zip_relpath != zip_relpath:
            raise SharedMirrorPendingError("共享目录批准状态与更新清单尚未一致。")

        manifest_release_revision = int(
            payload.get("target_release_revision", payload.get("approved_release_revision", 0)) or 0
        )
        state_release_revision = int(state.get("mirror_release_revision", 0) or 0)
        if state_release_revision > 0 and manifest_release_revision > 0 and state_release_revision != manifest_release_revision:
            raise SharedMirrorPendingError("共享目录批准状态与版本清单修订号不一致。")

        approved_zip = self.approved_root / _safe_relative_path(zip_relpath)
        if not approved_zip.exists():
            raise SharedMirrorPendingError("共享目录中的批准补丁还未就绪。")
        validate_non_empty_file(approved_zip)
        manifest_zip_size = int(payload.get("zip_size", 0) or 0)
        if manifest_zip_size > 0 and approved_zip.stat().st_size != manifest_zip_size:
            raise SharedMirrorPendingError("共享目录中的批准补丁大小与清单不一致。")

        payload["zip_relpath"] = zip_relpath
        payload.setdefault("zip_url", zip_relpath)
        payload.setdefault("published_at", "")
        payload.setdefault("published_by_role", "")
        payload.setdefault("published_by_node_id", "")
        payload.setdefault("approved_local_version", "")
        payload.setdefault("approved_release_revision", 0)
        return payload

    def fetch_latest_manifest(self) -> Dict[str, Any]:
        return self._build_ready_manifest()

    def download_patch(self, zip_ref: str, save_to: Path, *, expected_sha256: str = "") -> Path:
        rel_path = _safe_relative_path(str(zip_ref or "").strip())
        source_path = self.approved_root / rel_path
        if not source_path.exists():
            raise RuntimeError(f"共享目录中的补丁包不存在: {source_path}")
        save_to.parent.mkdir(parents=True, exist_ok=True)
        atomic_copy_file(source_path, save_to, validator=validate_non_empty_file)
        expected_sha = str(expected_sha256 or "").strip().lower()
        if expected_sha:
            actual_sha = _sha256_file(save_to)
            if actual_sha != expected_sha:
                save_to.unlink(missing_ok=True)
                raise RuntimeError(
                    "共享目录补丁包 sha256 校验失败: "
                    f"expected={expected_sha}, actual={actual_sha}"
                )
        return save_to

    def publish_approved_update(
        self,
        *,
        remote_manifest: Dict[str, Any],
        patch_zip: Path,
        expected_sha256: str = "",
        published_by_role: str = "",
        published_by_node_id: str = "",
        approved_local_version: str = "",
        approved_release_revision: int = 0,
    ) -> Dict[str, Any]:
        if not patch_zip.exists():
            raise FileNotFoundError(f"待发布补丁包不存在: {patch_zip}")

        self.staging_root.mkdir(parents=True, exist_ok=True)
        self.approved_root.mkdir(parents=True, exist_ok=True)

        zip_name = Path(str(remote_manifest.get("zip_url", "") or "").split("?")[0]).name or patch_zip.name
        staging_zip = self.staging_root / zip_name
        approved_zip = self.approved_root / zip_name

        atomic_copy_file(patch_zip, staging_zip, validator=validate_non_empty_file)
        actual_sha = _sha256_file(staging_zip)
        expected_sha = str(expected_sha256 or remote_manifest.get("zip_sha256", "") or "").strip().lower()
        if expected_sha and actual_sha != expected_sha:
            staging_zip.unlink(missing_ok=True)
            raise RuntimeError(
                "共享目录镜像发布失败：补丁包校验不一致。 "
                f"expected={expected_sha}, actual={actual_sha}"
            )

        atomic_copy_file(
            staging_zip,
            approved_zip,
            validator=lambda path: self._validate_zip_copy(path, expected_sha=actual_sha),
        )

        published_at = _now_text()
        approved_manifest = dict(remote_manifest if isinstance(remote_manifest, dict) else {})
        approved_manifest.update(
            {
                "zip_url": zip_name,
                "zip_relpath": zip_name,
                "zip_sha256": actual_sha,
                "zip_size": int(approved_zip.stat().st_size),
                "published_at": published_at,
                "published_by_role": str(published_by_role or "").strip(),
                "published_by_node_id": str(published_by_node_id or "").strip(),
                "approved_local_version": str(approved_local_version or "").strip(),
                "approved_release_revision": int(approved_release_revision or 0),
            }
        )
        atomic_write_text(
            self.manifest_path,
            json.dumps(approved_manifest, ensure_ascii=False, indent=2),
            validator=validate_json_file,
        )

        publish_state = {
            "mirror_ready": True,
            "mirror_version": str(
                approved_manifest.get("target_display_version")
                or approved_manifest.get("target_version")
                or approved_local_version
                or ""
            ).strip(),
            "mirror_release_revision": int(
                approved_manifest.get("target_release_revision", approved_release_revision)
                or approved_release_revision
                or 0
            ),
            "last_publish_at": published_at,
            "last_publish_error": "",
            "mirror_manifest_path": str(self.manifest_path),
            "published_by_role": str(published_by_role or "").strip(),
            "published_by_node_id": str(published_by_node_id or "").strip(),
            "zip_relpath": zip_name,
        }
        atomic_write_text(
            self.publish_state_path,
            json.dumps(publish_state, ensure_ascii=False, indent=2),
            validator=validate_json_file,
        )

        self._prune_old_zips(keep_name=zip_name)
        staging_zip.unlink(missing_ok=True)
        return {
            "manifest_path": str(self.manifest_path),
            "publish_state_path": str(self.publish_state_path),
            "zip_path": str(approved_zip),
            "zip_sha256": actual_sha,
            "published_at": published_at,
        }

    def get_runtime_snapshot(self) -> Dict[str, Any]:
        state = self.load_publish_state()
        zip_relpath = str(state.get("zip_relpath", "") or "").strip()
        mirror_ready = False
        try:
            manifest = self._build_ready_manifest()
            zip_relpath = str(manifest.get("zip_relpath", "") or "").strip() or zip_relpath
            state["mirror_version"] = str(
                manifest.get("target_display_version")
                or manifest.get("target_version")
                or state.get("mirror_version", "")
            ).strip()
            state["mirror_release_revision"] = int(
                manifest.get("target_release_revision", state.get("mirror_release_revision", 0))
                or state.get("mirror_release_revision", 0)
                or 0
            )
            state["last_publish_at"] = str(manifest.get("published_at", "") or state.get("last_publish_at", "")).strip()
            state["published_by_role"] = str(
                manifest.get("published_by_role", "") or state.get("published_by_role", "")
            ).strip()
            state["published_by_node_id"] = str(
                manifest.get("published_by_node_id", "") or state.get("published_by_node_id", "")
            ).strip()
            mirror_ready = True
        except SharedMirrorPendingError:
            mirror_ready = False
        except Exception:
            mirror_ready = False
        state["mirror_ready"] = mirror_ready
        state["mirror_manifest_path"] = str(self.manifest_path)
        state["zip_relpath"] = zip_relpath
        return state

    @staticmethod
    def _validate_zip_copy(path: Path, *, expected_sha: str) -> None:
        validate_non_empty_file(path)
        actual_sha = _sha256_file(path)
        if actual_sha != expected_sha:
            raise RuntimeError(
                "共享目录批准补丁校验失败: "
                f"expected={expected_sha}, actual={actual_sha}"
            )

    def _prune_old_zips(self, *, keep_name: str) -> None:
        for root in [self.approved_root, self.staging_root]:
            if not root.exists():
                continue
            for path in root.glob("*.zip"):
                if path.name == keep_name and root == self.approved_root:
                    continue
                path.unlink(missing_ok=True)
