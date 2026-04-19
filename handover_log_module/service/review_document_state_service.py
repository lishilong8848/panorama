from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

from app.shared.utils.atomic_file import atomic_save_workbook
from handover_log_module.repository.excel_reader import load_workbook_quietly
from handover_log_module.repository.footer_inventory_writer import write_footer_inventory_table
from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore
from handover_log_module.service.cabinet_power_defaults_service import CabinetPowerDefaultsService
from handover_log_module.service.footer_inventory_defaults_service import FooterInventoryDefaultsService
from handover_log_module.service.review_document_parser import ReviewDocumentParser
from handover_log_module.service.review_document_writer import ReviewDocumentWriter


class ReviewDocumentStateConflictError(RuntimeError):
    pass


class ReviewDocumentStateError(RuntimeError):
    pass


class ReviewDocumentStateService:
    """SQLite-backed source of truth for handover review documents."""

    _worker_guard = threading.Lock()
    _workers: Dict[str, threading.Thread] = {}
    _excel_lock_guard = threading.Lock()
    _excel_locks: Dict[str, threading.RLock] = {}

    def __init__(
        self,
        config: Dict[str, Any],
        *,
        parser: ReviewDocumentParser | None = None,
        writer: ReviewDocumentWriter | None = None,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config if isinstance(config, dict) else {}
        self.parser = parser or ReviewDocumentParser(self.config)
        self.writer = writer or ReviewDocumentWriter(self.config)
        self.emit_log = emit_log if callable(emit_log) else print
        self._cabinet_defaults = CabinetPowerDefaultsService()
        self._footer_defaults = FooterInventoryDefaultsService()

    def _store(self, building: str) -> ReviewBuildingDocumentStore:
        return ReviewBuildingDocumentStore(config=self.config, building=building)

    @classmethod
    def _excel_lock_for_path(cls, path: Path) -> threading.RLock:
        key = str(path.resolve(strict=False)).casefold()
        with cls._excel_lock_guard:
            lock = cls._excel_locks.get(key)
            if lock is None:
                lock = threading.RLock()
                cls._excel_locks[key] = lock
            return lock

    @staticmethod
    def _session_id(session: Dict[str, Any]) -> str:
        return str(session.get("session_id", "") or "").strip()

    @staticmethod
    def _building(session: Dict[str, Any]) -> str:
        return str(session.get("building", "") or "").strip()

    @staticmethod
    def _output_file(session: Dict[str, Any]) -> str:
        return str(session.get("output_file", "") or "").strip()

    def ensure_document_for_session(self, session: Dict[str, Any]) -> Dict[str, Any]:
        building = self._building(session)
        session_id = self._session_id(session)
        if not building or not session_id:
            raise ReviewDocumentStateError("审核会话缺少楼栋或 session_id")
        store = self._store(building)
        existing = store.get_document(session_id)
        if isinstance(existing, dict):
            current_output_file = self._output_file(session)
            existing_output_file = str(existing.get("source_excel_path", "") or "").strip()
            current_path = Path(current_output_file) if current_output_file else None
            current_mtime = ""
            current_size = 0
            if current_path is not None and current_path.exists() and current_path.is_file():
                try:
                    stat = current_path.stat()
                    current_mtime = str(getattr(stat, "st_mtime_ns", None) or int(getattr(stat, "st_mtime", 0) or 0))
                    current_size = int(getattr(stat, "st_size", 0) or 0)
                except Exception:  # noqa: BLE001
                    current_mtime = ""
                    current_size = 0
            existing_mtime = str(existing.get("source_excel_mtime", "") or "").strip()
            existing_size = int(existing.get("source_excel_size", 0) or 0)
            path_changed = bool(current_output_file and existing_output_file != current_output_file)
            fingerprint_changed = bool(
                current_mtime
                and (existing_mtime != current_mtime or existing_size != current_size)
            )
            if path_changed or fingerprint_changed:
                store.delete_document(session_id)
                self.emit_log(
                    f"[交接班][审核SQLite] 检测到会话输出文件已切换，已丢弃旧审核文档: "
                    f"building={building}, session_id={session_id}, old={existing_output_file or '-'}, "
                    f"new={current_output_file}, fingerprint_changed={'是' if fingerprint_changed else '否'}"
                )
            else:
                return existing

        output_file = self._output_file(session)
        if not output_file:
            raise ReviewDocumentStateError("交接班文件不存在，无法初始化审核文档")
        output_path = Path(output_file)
        if not output_path.exists() or not output_path.is_file():
            raise ReviewDocumentStateError(f"交接班文件不存在，无法初始化审核文档: {output_path}")
        try:
            document = self.parser.parse(output_file)
        except Exception as exc:  # noqa: BLE001
            raise ReviewDocumentStateError(f"解析交接班文件失败，无法初始化审核文档: {exc}") from exc
        imported = store.upsert_imported_document(
            session=session,
            document=document,
            imported_from_excel=True,
        )
        self.emit_log(
            f"[交接班][审核SQLite] 已从Excel导入审核文档 building={building}, "
            f"session_id={session_id}, revision={imported.get('revision', '-')}, file={output_path}"
        )
        return imported

    def attach_excel_sync(self, session: Dict[str, Any]) -> Dict[str, Any]:
        payload = dict(session if isinstance(session, dict) else {})
        building = str(payload.get("building", "") or "").strip()
        session_id = str(payload.get("session_id", "") or "").strip()
        if not building or not session_id:
            return payload
        sync_state = self._store(building).get_sync_state(session_id)
        if sync_state.get("status") == "unknown":
            revision = int(payload.get("revision", 0) or 0)
            sync_state = {
                "status": "unknown",
                "synced_revision": 0,
                "pending_revision": revision,
                "error": "",
                "updated_at": "",
            }
        payload["excel_sync"] = sync_state
        return payload

    def has_document(self, session: Dict[str, Any]) -> bool:
        building = self._building(session)
        session_id = self._session_id(session)
        if not building or not session_id:
            return False
        state = self._store(building).get_document(session_id)
        return isinstance(state, dict)

    def load_document(self, session: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        state = self.ensure_document_for_session(session)
        session_with_sync = self.attach_excel_sync(session)
        session_with_sync["revision"] = int(state.get("revision", session_with_sync.get("revision", 0)) or 0)
        return state.get("document", {}) if isinstance(state.get("document", {}), dict) else {}, session_with_sync

    def save_document(
        self,
        *,
        session: Dict[str, Any],
        document: Dict[str, Any],
        base_revision: int,
        dirty_regions: Dict[str, Any],
        ensure_ready: bool = True,
    ) -> tuple[Dict[str, Any], Dict[str, Any] | None]:
        if ensure_ready:
            self.ensure_document_for_session(session)
        try:
            return self._store(self._building(session)).save_document(
                session=session,
                document=document,
                base_revision=base_revision,
                dirty_regions=dirty_regions,
            )
        except ValueError as exc:
            if str(exc) == "revision_conflict":
                raise ReviewDocumentStateConflictError("审核内容已被其他人修改，请刷新后重试") from exc
            raise
        except KeyError as exc:
            raise ReviewDocumentStateError("审核文档尚未初始化") from exc

    def restore_document(self, *, building: str, previous: Dict[str, Any] | None) -> None:
        if not isinstance(previous, dict):
            return
        self._store(building).restore_document(previous)

    def enqueue_excel_sync(self, session: Dict[str, Any], *, target_revision: int) -> Dict[str, Any]:
        building = self._building(session)
        session_id = self._session_id(session)
        if not building or not session_id:
            return {}
        store = self._store(building)
        try:
            self._start_worker(building=building)
        except Exception as exc:  # noqa: BLE001
            return store.update_sync_state(
                session_id=session_id,
                status="failed",
                synced_revision=int(store.get_sync_state(session_id).get("synced_revision", 0) or 0),
                pending_revision=int(target_revision or 0),
                error=f"后台Excel同步器启动失败: {exc}",
            )
        return store.get_sync_state(session_id)

    def _start_worker(self, *, building: str) -> None:
        store = self._store(building)
        key = str(store.db_path.resolve(strict=False)).casefold()
        with self._worker_guard:
            worker = self._workers.get(key)
            if worker is not None and worker.is_alive():
                return
            worker = threading.Thread(
                target=self._run_worker,
                args=(building, key),
                name=f"handover-review-excel-sync-{building}",
                daemon=True,
            )
            self._workers[key] = worker
            worker.start()

    def _run_worker(self, building: str, key: str) -> None:
        try:
            self._worker_loop(building)
        finally:
            with self._worker_guard:
                current = self._workers.get(key)
                if current is threading.current_thread():
                    self._workers.pop(key, None)
            self.emit_log(f"[交接班][审核SQLite] 后台Excel同步线程已退出，等待下次任务拉起: building={building}")

    def _worker_loop(self, building: str) -> None:
        store = self._store(building)
        while True:
            try:
                job = store.claim_next_job()
                if not isinstance(job, dict):
                    return
                session_id = str(job.get("session_id", "") or "").strip()
                target_revision = int(job.get("target_revision", 0) or 0)
                try:
                    sync_state = self.force_sync_session(
                        building=building,
                        session_id=session_id,
                        target_revision=target_revision,
                        reason="background",
                        reconcile_sync_job=False,
                    )
                    store.finish_job(
                        session_id=session_id,
                        success=True,
                        claimed_target_revision=target_revision,
                        synced_revision=int(sync_state.get("synced_revision", 0) or 0),
                    )
                except Exception as exc:  # noqa: BLE001
                    sync_state = store.finish_job(
                        session_id=session_id,
                        success=False,
                        claimed_target_revision=target_revision,
                        error=str(exc),
                    )
                    self.emit_log(
                        f"[交接班][审核SQLite] 后台Excel同步失败 building={building}, "
                        f"session_id={session_id}, revision={target_revision}, "
                        f"状态={sync_state.get('status', '-')}, error={exc}"
                    )
                time.sleep(0.05)
            except Exception as exc:  # noqa: BLE001
                self.emit_log(
                    f"[交接班][审核SQLite] 后台Excel同步线程异常，已自动恢复: "
                    f"building={building}, error={exc}"
                )
                time.sleep(1.0)

    def force_sync_session(
        self,
        *,
        building: str,
        session_id: str,
        target_revision: int | None = None,
        reason: str = "manual",
        reconcile_sync_job: bool = True,
    ) -> Dict[str, Any]:
        store = self._store(building)
        state = store.get_document(session_id)
        if not isinstance(state, dict):
            raise ReviewDocumentStateError("审核文档尚未初始化，无法同步Excel")
        revision = int(state.get("revision", 0) or 0)
        expected_revision = int(target_revision or revision)
        if expected_revision and revision < expected_revision:
            raise ReviewDocumentStateError(
                f"审核文档revision落后，无法同步Excel: current={revision}, target={expected_revision}"
            )
        output_file = str(state.get("source_excel_path", "") or "").strip()
        if not output_file:
            raise ReviewDocumentStateError("交接班文件不存在，无法同步最新审核内容")
        output_path = Path(output_file)
        if not output_path.exists() or not output_path.is_file():
            raise ReviewDocumentStateError(f"交接班文件不存在，无法同步最新审核内容: {output_path}")
        dirty_regions = state.get("dirty_regions", {}) if isinstance(state.get("dirty_regions", {}), dict) else {}

        store.update_sync_state(
            session_id=session_id,
            status="syncing",
            synced_revision=store.get_sync_state(session_id).get("synced_revision", 0),
            pending_revision=revision,
            error="",
        )
        try:
            with self._excel_lock_for_path(output_path):
                self.writer.write(
                    output_file=output_file,
                    document=state.get("document", {}) if isinstance(state.get("document", {}), dict) else {},
                    dirty_regions=dirty_regions,
                )
        except Exception as exc:  # noqa: BLE001
            if reconcile_sync_job:
                store.finish_job(
                    session_id=session_id,
                    success=False,
                    claimed_target_revision=expected_revision or revision,
                    error=str(exc),
                )
            else:
                store.update_sync_state(
                    session_id=session_id,
                    status="failed",
                    synced_revision=store.get_sync_state(session_id).get("synced_revision", 0),
                    pending_revision=revision,
                    error=str(exc),
                )
            raise ReviewDocumentStateError(f"交接班Excel同步失败: {exc}") from exc

        if reconcile_sync_job:
            sync = store.finish_job(
                session_id=session_id,
                success=True,
                claimed_target_revision=expected_revision or revision,
                synced_revision=revision,
            )
        else:
            sync = store.update_sync_state(
                session_id=session_id,
                status="synced",
                synced_revision=revision,
                pending_revision=0,
                error="",
            )
        self.emit_log(
            f"[交接班][审核SQLite] Excel同步完成 building={building}, session_id={session_id}, "
            f"revision={revision}, reason={reason}, file={output_path}"
        )
        return sync

    def force_sync_session_dict(self, session: Dict[str, Any], *, reason: str = "manual") -> Dict[str, Any]:
        self.ensure_document_for_session(session)
        return self.force_sync_session(
            building=self._building(session),
            session_id=self._session_id(session),
            target_revision=int(session.get("revision", 0) or 0),
            reason=reason,
            reconcile_sync_job=True,
        )

    def persist_defaults_from_document(
        self,
        *,
        building: str,
        document: Dict[str, Any],
        dirty_regions: Dict[str, bool] | None = None,
    ) -> Dict[str, int | bool]:
        dirty = dirty_regions if isinstance(dirty_regions, dict) else {}
        footer_dirty = bool(dirty.get("footer_inventory"))
        cabinet_dirty = bool(dirty.get("fixed_blocks"))
        if not footer_dirty and not cabinet_dirty:
            return {
                "footer_inventory_rows": 0,
                "cabinet_power_fields": 0,
                "config_updated": False,
                "defaults_updated": False,
            }
        store = self._store(building)
        updated = False
        footer_rows: List[Dict[str, Any]] = []
        cabinet_cells: Dict[str, str] = {}
        if footer_dirty:
            footer_rows = self._footer_defaults.extract_rows_from_document(document)
            updated = store.set_default("footer_inventory", self._footer_defaults.normalize_rows(footer_rows)) or updated
        if cabinet_dirty:
            cabinet_cells = self._cabinet_defaults.extract_cells_from_document(document)
            updated = store.set_default("cabinet_power", self._cabinet_defaults.normalize_cells(cabinet_cells)) or updated
        return {
            "footer_inventory_rows": len(footer_rows),
            "cabinet_power_fields": len(cabinet_cells),
            "config_updated": False,
            "defaults_updated": bool(updated),
        }

    def persist_defaults_from_config(
        self,
        *,
        building: str,
        config: Dict[str, Any],
    ) -> Dict[str, int | bool]:
        store = self._store(building)
        footer_rows = self._footer_defaults.get_building_defaults(config, building)
        cabinet_cells = self._cabinet_defaults.get_building_defaults(config, building)
        updated = False
        if footer_rows is None:
            updated = store.delete_default("footer_inventory") or updated
            footer_count = 0
        else:
            normalized_rows = self._footer_defaults.normalize_rows(footer_rows)
            updated = store.set_default("footer_inventory", normalized_rows) or updated
            footer_count = len(normalized_rows)
        if cabinet_cells is None:
            updated = store.delete_default("cabinet_power") or updated
            cabinet_count = 0
        else:
            normalized_cells = self._cabinet_defaults.normalize_cells(cabinet_cells)
            updated = store.set_default("cabinet_power", normalized_cells) or updated
            cabinet_count = len(normalized_cells)
        return {
            "footer_inventory_rows": int(footer_count),
            "cabinet_power_fields": int(cabinet_count),
            "defaults_updated": bool(updated),
        }

    def _sheet_name(self) -> str:
        template_cfg = self.config.get("template", {}) if isinstance(self.config.get("template", {}), dict) else {}
        return str(template_cfg.get("sheet_name", "") or "").strip()

    def apply_cabinet_defaults_to_output(
        self,
        *,
        building: str,
        output_file: str | Path,
        emit_log: Callable[[str], None] = print,
    ) -> int | None:
        payload = self._store(building).get_default("cabinet_power")
        if not isinstance(payload, dict):
            return None
        cells = self._cabinet_defaults.normalize_cells(payload)
        output_path = Path(str(output_file).strip())
        workbook = load_workbook_quietly(output_path)
        try:
            sheet_name = self._sheet_name()
            if sheet_name and sheet_name in workbook.sheetnames:
                ws = workbook[sheet_name]
            else:
                ws = workbook.active
            for cell, value in cells.items():
                ws[cell] = value
            atomic_save_workbook(workbook, output_path, temp_suffix=".tmp")
        finally:
            workbook.close()
        emit_log(
            f"[交接班][机柜上下电默认] 已应用SQLite楼栋默认值 building={building}, fields={len(cells)}, output={output_path}"
        )
        return len(cells)

    def apply_footer_defaults_to_output(
        self,
        *,
        building: str,
        output_file: str | Path,
        emit_log: Callable[[str], None] = print,
    ) -> int | None:
        payload = self._store(building).get_default("footer_inventory")
        if not isinstance(payload, list):
            return None
        rows = self._footer_defaults.normalize_rows(payload)
        output_path = Path(str(output_file).strip())
        workbook = load_workbook_quietly(output_path)
        try:
            sheet_name = self._sheet_name()
            if sheet_name and sheet_name in workbook.sheetnames:
                ws = workbook[sheet_name]
            else:
                ws = workbook.active
            write_footer_inventory_table(
                ws=ws,
                inventory_block=self._footer_defaults.build_inventory_block(rows),
                emit_log=emit_log,
            )
            atomic_save_workbook(workbook, output_path, temp_suffix=".tmp")
        finally:
            workbook.close()
        emit_log(
            f"[交接班][工具表默认] 已应用SQLite楼栋默认工具表 building={building}, rows={len(rows)}, output={output_path}"
        )
        return len(rows)
