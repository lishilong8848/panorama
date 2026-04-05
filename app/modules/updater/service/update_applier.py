from __future__ import annotations

import json
import shutil
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Dict, List

from app.shared.utils.atomic_file import atomic_write_file


BACKUP_MANIFEST_NAME = "backup_manifest.json"


class UpdateApplier:
    def __init__(
        self,
        *,
        app_dir: Path,
        emit_log: Callable[[str], None] | None = None,
        runtime_state_root: str | None = None,
    ) -> None:
        self.app_dir = app_dir
        self.emit_log = emit_log or (lambda _: None)
        self.runtime_state_root = str(runtime_state_root or "").strip()

    def _log(self, text: str) -> None:
        self.emit_log(f"[Updater] {text}")

    @staticmethod
    def _read_patch_meta_from_archive(archive: zipfile.ZipFile) -> Dict[str, Any]:
        try:
            raw = archive.read("patch_meta.json")
        except KeyError:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:  # noqa: BLE001
            return {}

    @staticmethod
    def _normalize_archive_member(filename: str) -> Path | None:
        text = str(filename or "").replace("\\", "/").strip()
        if not text or text.endswith("/"):
            return None
        pure = PurePosixPath(text.lstrip("/"))
        parts = [str(part or "").strip() for part in pure.parts]
        if not parts or any(part in {"", ".", ".."} for part in parts):
            return None
        return Path(*parts)

    @staticmethod
    def _write_archive_member(archive: zipfile.ZipFile, info: zipfile.ZipInfo, target: Path) -> None:
        def _writer(temp_path: Path) -> None:
            with archive.open(info, "r") as source_handle, temp_path.open("wb") as target_handle:
                shutil.copyfileobj(source_handle, target_handle, length=1024 * 1024)

        atomic_write_file(target, _writer, temp_suffix=".updating")

    @staticmethod
    def _prune_backups(backup_root: Path, max_backups: int) -> None:
        if not backup_root.exists():
            return
        keep = max(1, int(max_backups or 1))
        dirs = [path for path in backup_root.iterdir() if path.is_dir()]
        dirs.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        for stale in dirs[keep:]:
            shutil.rmtree(stale, ignore_errors=True)

    @staticmethod
    def _cleanup_empty_parents(path: Path, stop_at: Path) -> None:
        current = path
        stop_resolved = stop_at.resolve()
        while current.exists():
            try:
                current_resolved = current.resolve()
            except Exception:  # noqa: BLE001
                return
            if current_resolved == stop_resolved:
                return
            try:
                current.rmdir()
            except OSError:
                return
            current = current.parent

    def _write_backup_manifest(self, snapshot: Path, created_files: List[str]) -> None:
        payload = {
            "created_files": sorted({str(item).replace("\\", "/").strip() for item in created_files if str(item).strip()}),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        (snapshot / BACKUP_MANIFEST_NAME).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _read_backup_manifest(self, snapshot: Path) -> Dict[str, Any]:
        path = snapshot / BACKUP_MANIFEST_NAME
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:  # noqa: BLE001
            return {}

    def apply_patch_zip(
        self,
        *,
        zip_path: Path,
        backup_root: Path,
        max_backups: int,
    ) -> Dict[str, Any]:
        if not zip_path.exists():
            raise FileNotFoundError(f"patch zip 不存在: {zip_path}")

        backup_root.mkdir(parents=True, exist_ok=True)
        snapshot = backup_root / datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot.mkdir(parents=True, exist_ok=True)

        replaced = 0
        deleted = 0
        created_files: List[str] = []

        with zipfile.ZipFile(zip_path, "r") as archive:
            patch_meta = self._read_patch_meta_from_archive(archive)
            delete_list: List[str] = []
            if isinstance(patch_meta.get("deleted_files"), list):
                delete_list = [
                    str(item).replace("\\", "/").strip()
                    for item in patch_meta.get("deleted_files", [])
                    if str(item).strip()
                ]

            for info in sorted(archive.infolist(), key=lambda item: item.filename):
                if info.is_dir():
                    continue
                rel = self._normalize_archive_member(info.filename)
                if rel is None:
                    continue
                rel_text = str(rel).replace("\\", "/")
                if rel_text in {"patch_meta.json", "latest_patch.json"}:
                    continue
                dst = self.app_dir / rel
                if dst.exists():
                    backup_file = snapshot / rel
                    backup_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(dst, backup_file)
                else:
                    created_files.append(rel_text)
                dst.parent.mkdir(parents=True, exist_ok=True)
                self._write_archive_member(archive, info, dst)
                replaced += 1

            for rel_text in delete_list:
                dst = self.app_dir / Path(rel_text)
                if not dst.exists():
                    continue
                backup_target = snapshot / rel_text
                backup_target.parent.mkdir(parents=True, exist_ok=True)
                if dst.is_file():
                    shutil.copy2(dst, backup_target)
                    dst.unlink()
                    deleted += 1
                elif dst.is_dir():
                    shutil.copytree(dst, backup_target, dirs_exist_ok=True)
                    shutil.rmtree(dst)
                    deleted += 1

        self._write_backup_manifest(snapshot, created_files)
        self._prune_backups(backup_root, max_backups=max_backups)
        self._log(f"补丁应用完成: replaced={replaced}, deleted={deleted}, backup={snapshot}")
        return {
            "replaced": replaced,
            "deleted": deleted,
            "backup": str(snapshot),
            "patch_meta": patch_meta,
            "created_files": created_files,
        }

    def restore_backup_snapshot(self, backup_path: Path | str) -> Dict[str, int | str]:
        snapshot = Path(backup_path)
        if not snapshot.exists():
            raise FileNotFoundError(f"回滚快照不存在: {snapshot}")

        manifest = self._read_backup_manifest(snapshot)
        created_files = [
            str(item).replace("\\", "/").strip()
            for item in manifest.get("created_files", [])
            if str(item).strip()
        ]

        removed = 0
        restored = 0
        for rel_text in sorted(created_files, key=lambda item: len(Path(item).parts), reverse=True):
            target = self.app_dir / Path(rel_text)
            if not target.exists():
                continue
            if target.is_dir():
                shutil.rmtree(target, ignore_errors=True)
            else:
                target.unlink(missing_ok=True)
            removed += 1
            self._cleanup_empty_parents(target.parent, self.app_dir)

        for src in sorted(snapshot.rglob("*")):
            if src.name == BACKUP_MANIFEST_NAME:
                continue
            rel = src.relative_to(snapshot)
            dst = self.app_dir / rel
            if src.is_dir():
                dst.mkdir(parents=True, exist_ok=True)
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            restored += 1

        self._log(f"已从快照恢复旧版本: restored={restored}, removed_created={removed}, backup={snapshot}")
        return {
            "restored": restored,
            "removed_created": removed,
            "backup": str(snapshot),
        }
