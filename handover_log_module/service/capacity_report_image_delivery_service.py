from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List

from app.shared.utils.atomic_file import atomic_write_bytes, validate_image_file
from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
from handover_log_module.service.handover_summary_message_service import HandoverSummaryMessageService
from handover_log_module.service.review_link_delivery_service import ReviewLinkDeliveryService
from handover_log_module.service.review_session_service import ReviewSessionNotFoundError, ReviewSessionService
from pipeline_utils import get_app_dir


_SCREENSHOT_QUEUE_TIMEOUT_SEC = 120
_CLIPBOARD_WAIT_SEC = 10


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_name(value: str) -> str:
    text = _text(value)
    text = re.sub(r"[^\w\u4e00-\u9fff.-]+", "_", text, flags=re.UNICODE)
    return text.strip("._") or "unknown"


def _runtime_root(handover_cfg: Dict[str, Any]) -> Path:
    return resolve_runtime_state_root(
        runtime_config={"paths": handover_cfg.get("_global_paths", {}) if isinstance(handover_cfg, dict) else {}},
        app_dir=get_app_dir(),
    )


class CapacityReportExcelScreenshotQueue:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._next_ticket = 0
        self._serving_ticket = 0

    @contextmanager
    def acquire(
        self,
        *,
        timeout_sec: int,
        emit_log: Callable[[str], None],
        context: str,
    ) -> Iterator[Dict[str, int]]:
        started = time.monotonic()
        with self._condition:
            ticket = self._next_ticket
            self._next_ticket += 1
            pending_before = max(0, ticket - self._serving_ticket)
            emit_log(
                "[交接班][容量表图片发送] 进入Excel截图队列 "
                f"{context}, ticket={ticket}, pending_before={pending_before}"
            )
            while ticket != self._serving_ticket:
                remaining = timeout_sec - (time.monotonic() - started)
                if remaining <= 0:
                    raise TimeoutError("容量图片截图繁忙，请稍后重试")
                self._condition.wait(timeout=remaining)
        wait_ms = int((time.monotonic() - started) * 1000)
        emit_log(
            "[交接班][容量表图片发送] 已获取Excel截图队列 "
            f"{context}, ticket={ticket}, wait_ms={wait_ms}"
        )
        try:
            yield {"ticket": ticket, "wait_ms": wait_ms}
        finally:
            with self._condition:
                self._serving_ticket += 1
                self._condition.notify_all()
            emit_log(
                "[交接班][容量表图片发送] 已释放Excel截图队列 "
                f"{context}, ticket={ticket}"
            )


_SCREENSHOT_QUEUE = CapacityReportExcelScreenshotQueue()


class CapacityReportImageDeliveryService:
    def __init__(
        self,
        handover_cfg: Dict[str, Any],
        *,
        config_path: str | Path | None = None,
        review_service: ReviewSessionService | None = None,
        link_service: ReviewLinkDeliveryService | None = None,
        capacity_service: HandoverCapacityReportService | None = None,
        summary_service: HandoverSummaryMessageService | None = None,
    ) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}
        self.config_path = Path(config_path) if config_path else None
        self.review_service = review_service or ReviewSessionService(self.handover_cfg)
        self.link_service = link_service or ReviewLinkDeliveryService(self.handover_cfg, config_path=self.config_path)
        self.capacity_service = capacity_service or HandoverCapacityReportService(self.handover_cfg)
        self.summary_service = summary_service or HandoverSummaryMessageService(
            self.handover_cfg,
            config_path=self.config_path,
        )

    def _configured_sheet_name(self) -> str:
        capacity_cfg = self.handover_cfg.get("capacity_report", {})
        template_cfg = capacity_cfg.get("template", {}) if isinstance(capacity_cfg, dict) else {}
        return _text(template_cfg.get("sheet_name") if isinstance(template_cfg, dict) else "")

    def _output_image_path(
        self,
        *,
        session: Dict[str, Any],
        signature: str,
    ) -> Path:
        building = _text(session.get("building"))
        duty_date = _text(session.get("duty_date"))
        duty_shift = _text(session.get("duty_shift")).lower()
        session_id = _text(session.get("session_id"))
        session_hash = hashlib.sha1(session_id.encode("utf-8", errors="ignore")).hexdigest()[:10]
        batch_dir = _runtime_root(self.handover_cfg) / "handover" / "capacity_report_images" / _safe_name(f"{duty_date}--{duty_shift}")
        return batch_dir / f"{_safe_name(building)}_{session_hash}_{signature[:12]}.png"

    @staticmethod
    def _image_stat(path: Path) -> Dict[str, int]:
        stat = path.stat()
        return {
            "image_file_size": int(getattr(stat, "st_size", 0) or 0),
            "image_file_mtime_ns": int(getattr(stat, "st_mtime_ns", 0) or int(getattr(stat, "st_mtime", 0) or 0)),
        }

    @staticmethod
    def _cached_image_valid(delivery: Dict[str, Any], signature: str, *, emit_log: Callable[[str], None]) -> Path | None:
        image_signature = _text(delivery.get("image_signature"))
        image_path_text = _text(delivery.get("image_path"))
        if not signature or image_signature != signature or not image_path_text:
            return None
        path = Path(image_path_text)
        if not path.exists() or not path.is_file():
            return None
        try:
            validate_image_file(path)
            stat = path.stat()
            stored_size = int(delivery.get("image_file_size", 0) or 0)
            stored_mtime = int(delivery.get("image_file_mtime_ns", 0) or 0)
            if stored_size and stored_size != int(getattr(stat, "st_size", 0) or 0):
                return None
            current_mtime = int(getattr(stat, "st_mtime_ns", 0) or int(getattr(stat, "st_mtime", 0) or 0))
            if stored_mtime and stored_mtime != current_mtime:
                return None
            return path
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][容量表图片发送] 图片缓存校验失败 image={path}, error={exc}")
            return None

    def _file_lock_path(self) -> Path:
        root = _runtime_root(self.handover_cfg) / "handover" / "capacity_report_images"
        root.mkdir(parents=True, exist_ok=True)
        return root / ".excel_copy_picture.lock"

    @contextmanager
    def _runtime_file_lock(
        self,
        *,
        timeout_sec: int,
        context: str,
        emit_log: Callable[[str], None],
    ) -> Iterator[None]:
        path = self._file_lock_path()
        started = time.monotonic()
        fd: int | None = None
        while fd is None:
            try:
                fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}|{context}|{_now_text()}".encode("utf-8", errors="ignore"))
            except FileExistsError:
                try:
                    if time.time() - path.stat().st_mtime > max(timeout_sec, 300):
                        path.unlink(missing_ok=True)
                        continue
                except Exception:
                    pass
                if time.monotonic() - started >= timeout_sec:
                    raise TimeoutError("容量图片截图繁忙，请稍后重试")
                time.sleep(0.2)
        try:
            yield
        finally:
            try:
                if fd is not None:
                    os.close(fd)
            finally:
                try:
                    path.unlink(missing_ok=True)
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[交接班][容量表图片发送] Excel截图锁文件清理失败 lock={path}, error={exc}")

    def _render_excel_copy_picture(
        self,
        *,
        source_path: Path,
        output_path: Path,
        emit_log: Callable[[str], None],
    ) -> Path:
        context = f"source={source_path}"
        with _SCREENSHOT_QUEUE.acquire(
            timeout_sec=_SCREENSHOT_QUEUE_TIMEOUT_SEC,
            emit_log=emit_log,
            context=context,
        ):
            with self._runtime_file_lock(
                timeout_sec=_SCREENSHOT_QUEUE_TIMEOUT_SEC,
                context=context,
                emit_log=emit_log,
            ):
                return self._render_excel_copy_picture_locked(
                    source_path=source_path,
                    output_path=output_path,
                    emit_log=emit_log,
                )

    def _render_excel_copy_picture_locked(
        self,
        *,
        source_path: Path,
        output_path: Path,
        emit_log: Callable[[str], None],
    ) -> Path:
        excel = None
        workbook = None
        co_initialized = False
        excel_pid = 0
        try:
            import pythoncom
            import win32com.client
            from PIL import ImageGrab

            emit_log(f"[交接班][容量表图片发送] Excel截图开始 source={source_path}, target={output_path}")
            pythoncom.CoInitialize()
            co_initialized = True
            excel = win32com.client.DispatchEx("Excel.Application")
            try:
                import win32process

                _, excel_pid = win32process.GetWindowThreadProcessId(int(excel.Hwnd))
            except Exception:
                excel_pid = 0
            emit_log(f"[交接班][容量表图片发送] Excel已启动 source={source_path}, excel_pid={excel_pid or '-'}")
            excel.Visible = False
            excel.DisplayAlerts = False
            excel.ScreenUpdating = False
            try:
                excel.EnableEvents = False
            except Exception:
                pass
            workbook = excel.Workbooks.Open(os.path.abspath(str(source_path)), UpdateLinks=0, ReadOnly=True, AddToMru=False)
            configured_sheet = self._configured_sheet_name()
            if configured_sheet:
                try:
                    sheet = workbook.Sheets(configured_sheet)
                except Exception:
                    sheet = workbook.Sheets(1)
                    emit_log(
                        "[交接班][容量表图片发送] 配置Sheet不存在，已使用首个Sheet "
                        f"configured_sheet={configured_sheet}"
                    )
            else:
                sheet = workbook.Sheets(1)
            try:
                sheet_name = str(sheet.Name)
            except Exception:
                sheet_name = configured_sheet or "1"
            workbook.Activate()
            sheet.Activate()
            used_range = sheet.UsedRange
            try:
                used_address = str(used_range.Address)
                used_rows = int(used_range.Rows.Count)
                used_cols = int(used_range.Columns.Count)
            except Exception:
                used_address = "UsedRange"
                used_rows = 0
                used_cols = 0
            emit_log(
                "[交接班][容量表图片发送] Excel截图区域 "
                f"sheet={sheet_name}, used_range={used_address}, rows={used_rows}, cols={used_cols}, excel_pid={excel_pid or '-'}"
            )
            used_range.CopyPicture(Format=2)
            deadline = time.monotonic() + _CLIPBOARD_WAIT_SEC
            image = None
            while time.monotonic() < deadline:
                grabbed = ImageGrab.grabclipboard()
                if hasattr(grabbed, "save"):
                    image = grabbed.copy() if hasattr(grabbed, "copy") else grabbed
                    break
                time.sleep(0.2)
            if image is None:
                raise RuntimeError("Excel截图失败: 剪贴板未获取到图片")
            buffer = BytesIO()
            image.save(buffer, format="PNG")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_bytes(output_path, buffer.getvalue(), validator=validate_image_file, temp_suffix=".tmp")
            stat = output_path.stat()
            emit_log(
                "[交接班][容量表图片发送] Excel截图生成成功 "
                f"image={output_path}, size={int(getattr(stat, 'st_size', 0) or 0)}, excel_pid={excel_pid or '-'}"
            )
            return output_path
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][容量表图片发送] Excel截图异常: error={exc}")
            raise
        finally:
            if workbook is not None:
                try:
                    workbook.Close(False)
                except Exception:
                    pass
            if excel is not None:
                try:
                    excel.Quit()
                except Exception:
                    pass
            if co_initialized:
                try:
                    import pythoncom

                    pythoncom.CoUninitialize()
                except Exception:
                    pass

    def _persist_delivery(
        self,
        *,
        session_id: str,
        delivery: Dict[str, Any],
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        try:
            session = self.review_service.update_capacity_image_delivery(
                session_id=session_id,
                capacity_image_delivery=delivery,
            )
            return session.get("capacity_image_delivery", delivery) if isinstance(session, dict) else delivery
        except (ReviewSessionNotFoundError, Exception) as exc:  # noqa: BLE001
            emit_log(f"[交接班][容量表图片发送] 状态更新失败 session_id={session_id}, error={exc}")
            return delivery

    def _persist_review_delivery(
        self,
        *,
        session: Dict[str, Any],
        status: str,
        attempt_at: str,
        successful_recipients: List[str],
        failed_recipients: List[Dict[str, str]],
        error: str,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        session_id = _text(session.get("session_id"))
        previous = session.get("review_link_delivery", {}) if isinstance(session.get("review_link_delivery", {}), dict) else {}
        payload = {
            **previous,
            "status": status,
            "last_attempt_at": attempt_at,
            "last_sent_at": attempt_at if status == "success" else _text(previous.get("last_sent_at")),
            "error": error,
            "successful_recipients": successful_recipients,
            "failed_recipients": failed_recipients,
            "source": "capacity_image_send",
        }
        try:
            updated = self.review_service.update_review_link_delivery(
                session_id=session_id,
                review_link_delivery=payload,
            )
            return updated.get("review_link_delivery", payload) if isinstance(updated, dict) else payload
        except Exception as exc:  # noqa: BLE001
            emit_log(f"[交接班][容量表图片发送] 审核文本发送状态更新失败 session_id={session_id}, error={exc}")
            return payload

    def _fail(
        self,
        *,
        session: Dict[str, Any],
        delivery: Dict[str, Any],
        attempt_at: str,
        error: str,
        failed_recipients: List[Dict[str, str]] | None = None,
        emit_log: Callable[[str], None],
    ) -> Dict[str, Any]:
        session_id = _text(session.get("session_id"))
        building = _text(session.get("building"))
        failed = list(failed_recipients or [])
        next_delivery = {
            **delivery,
            "status": "failed",
            "last_attempt_at": attempt_at,
            "error": error,
            "failed_recipients": failed,
            "source": "manual",
        }
        persisted = self._persist_delivery(session_id=session_id, delivery=next_delivery, emit_log=emit_log)
        review_delivery = self._persist_review_delivery(
            session=session,
            status="failed",
            attempt_at=attempt_at,
            successful_recipients=[],
            failed_recipients=failed,
            error=error,
            emit_log=emit_log,
        )
        emit_log(
            "[交接班][容量表图片发送] 失败 "
            f"building={building}, session_id={session_id}, error={error}, failed_recipients={failed}"
        )
        return {
            "ok": False,
            "status": "failed",
            "error": error,
            "building": building,
            "session_id": session_id,
            "successful_recipients": [],
            "failed_recipients": failed,
            "capacity_image_delivery": persisted,
            "review_link_delivery": review_delivery,
        }

    def send_for_session(
        self,
        session: Dict[str, Any],
        *,
        building: str = "",
        handover_cells: Dict[str, Any] | None = None,
        shared_110kv: Dict[str, Any] | None = None,
        cooling_pump_pressures: Dict[str, Any] | None = None,
        client_id: str = "",
        ensure_capacity_ready: Callable[[], Dict[str, Any]] | None = None,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        current_session = dict(session if isinstance(session, dict) else {})
        session_id = _text(current_session.get("session_id"))
        building_text = _text(building) or _text(current_session.get("building"))
        current_session["building"] = building_text
        attempt_at = _now_text()
        delivery_state = (
            dict(current_session.get("capacity_image_delivery", {}))
            if isinstance(current_session.get("capacity_image_delivery", {}), dict)
            else {}
        )
        delivery_state = {
            **delivery_state,
            "status": "sending",
            "last_attempt_at": attempt_at,
            "error": "",
            "source": "manual",
            "failed_recipients": [],
        }
        self._persist_delivery(session_id=session_id, delivery=delivery_state, emit_log=emit_log)

        capacity_file = _text(current_session.get("capacity_output_file"))
        emit_log(
            "[交接班][容量表图片发送] 开始发送 "
            f"building={building_text}, session={session_id}, source={capacity_file}, client_id={_text(client_id) or '-'}"
        )
        if not session_id or not building_text:
            return self._fail(
                session=current_session,
                delivery=delivery_state,
                attempt_at=attempt_at,
                error="容量图片发送缺少 session_id/building",
                emit_log=emit_log,
            )
        capacity_path = Path(capacity_file)
        if not capacity_file or not capacity_path.exists() or not capacity_path.is_file():
            return self._fail(
                session=current_session,
                delivery=delivery_state,
                attempt_at=attempt_at,
                error="交接班容量报表文件不存在",
                emit_log=emit_log,
            )

        recipient_snapshot = self.link_service._recipient_snapshot_for_building(building_text)
        recipients = list(recipient_snapshot.get("recipients", []))
        emit_log(
            "[交接班][容量表图片发送] 收件人读取完成 "
            f"building={building_text}, session={session_id}, recipients={len(recipients)}, "
            f"raw={int(recipient_snapshot.get('raw_count', 0) or 0)}, enabled={int(recipient_snapshot.get('enabled_count', 0) or 0)}, "
            f"disabled={int(recipient_snapshot.get('disabled_count', 0) or 0)}, invalid={int(recipient_snapshot.get('invalid_count', 0) or 0)}, "
            f"open_ids={recipient_snapshot.get('open_ids', [])}"
        )
        if not recipients:
            return self._fail(
                session=current_session,
                delivery=delivery_state,
                attempt_at=attempt_at,
                error="当前楼未配置启用且有效的审核接收人 open_id",
                emit_log=emit_log,
            )

        message_text = self.summary_service.build_for_session(current_session, emit_log=emit_log)
        if not message_text:
            return self._fail(
                session=current_session,
                delivery=delivery_state,
                attempt_at=attempt_at,
                error="交接班日志全文生成失败，未发送",
                emit_log=emit_log,
            )
        emit_log(
            "[交接班][容量表图片发送] 审核文本生成完成 "
            f"building={building_text}, session={session_id}, length={len(message_text)}"
        )

        cells = handover_cells if isinstance(handover_cells, dict) else {}
        signature_info = self.capacity_service.build_capacity_overlay_signature(
            building=building_text,
            duty_date=_text(current_session.get("duty_date")),
            duty_shift=_text(current_session.get("duty_shift")).lower(),
            handover_cells=cells,
            capacity_output_file=capacity_file,
            shared_110kv=shared_110kv,
            cooling_pump_pressures=cooling_pump_pressures,
            client_id=client_id,
            emit_log=emit_log,
        )
        target_signature = _text(signature_info.get("signature"))
        sync_state = current_session.get("capacity_sync", {}) if isinstance(current_session.get("capacity_sync", {}), dict) else {}
        sync_ready = (
            _text(sync_state.get("status")).lower() == "ready"
            and _text(sync_state.get("input_signature")) == _text(signature_info.get("input_signature"))
        )
        cached_image = self._cached_image_valid(delivery_state, target_signature, emit_log=emit_log) if sync_ready else None
        emit_log(
            "[交接班][容量表图片发送] 容量图片签名检查 "
            f"building={building_text}, session={session_id}, signature={target_signature[:12]}, "
            f"capacity_sync_ready={bool(sync_ready)}, cache_hit={bool(cached_image)}"
        )

        if cached_image is None:
            if callable(ensure_capacity_ready):
                try:
                    updated_session = ensure_capacity_ready()
                    if isinstance(updated_session, dict):
                        current_session.update(updated_session)
                        current_session["building"] = building_text
                except Exception as exc:  # noqa: BLE001
                    return self._fail(
                        session=current_session,
                        delivery=delivery_state,
                        attempt_at=attempt_at,
                        error=f"容量报表补写失败: {exc}",
                        emit_log=emit_log,
                    )
            else:
                sync_payload = self.capacity_service.sync_overlay_for_existing_report_from_cells(
                    building=building_text,
                    duty_date=_text(current_session.get("duty_date")),
                    duty_shift=_text(current_session.get("duty_shift")).lower(),
                    handover_cells=cells,
                    capacity_output_file=capacity_file,
                    shared_110kv=shared_110kv,
                    cooling_pump_pressures=cooling_pump_pressures,
                    client_id=client_id,
                    emit_log=emit_log,
                )
                sync_status = _text(sync_payload.get("status")).lower() if isinstance(sync_payload, dict) else "failed"
                try:
                    current_session.update(
                        self.review_service.update_capacity_sync(
                            session_id=session_id,
                            capacity_sync=sync_payload,
                            capacity_status="success" if sync_status == "ready" else sync_status,
                            capacity_error="" if sync_status == "ready" else _text(sync_payload.get("error")),
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    emit_log(f"[交接班][容量表图片发送] 容量补写状态更新失败 session={session_id}, error={exc}")

            sync_state = current_session.get("capacity_sync", {}) if isinstance(current_session.get("capacity_sync", {}), dict) else {}
            if _text(sync_state.get("status")).lower() != "ready":
                return self._fail(
                    session=current_session,
                    delivery=delivery_state,
                    attempt_at=attempt_at,
                    error=_text(sync_state.get("error")) or "容量报表补写未就绪，未发送",
                    emit_log=emit_log,
                )
            signature_info = self.capacity_service.build_capacity_overlay_signature(
                building=building_text,
                duty_date=_text(current_session.get("duty_date")),
                duty_shift=_text(current_session.get("duty_shift")).lower(),
                handover_cells=cells,
                capacity_output_file=capacity_file,
                shared_110kv=shared_110kv,
                cooling_pump_pressures=cooling_pump_pressures,
                client_id=client_id,
                emit_log=emit_log,
            )
            target_signature = _text(signature_info.get("signature"))
            if not bool(signature_info.get("valid", False)):
                return self._fail(
                    session=current_session,
                    delivery=delivery_state,
                    attempt_at=attempt_at,
                    error=_text(signature_info.get("error")) or "容量报表补写输入不完整，未发送",
                    emit_log=emit_log,
                )
            output_image = self._output_image_path(session=current_session, signature=target_signature)
            try:
                image_path = self._render_excel_copy_picture(
                    source_path=capacity_path,
                    output_path=output_image,
                    emit_log=emit_log,
                )
            except TimeoutError as exc:
                return self._fail(
                    session=current_session,
                    delivery=delivery_state,
                    attempt_at=attempt_at,
                    error=str(exc),
                    emit_log=emit_log,
                )
            except Exception as exc:  # noqa: BLE001
                return self._fail(
                    session=current_session,
                    delivery=delivery_state,
                    attempt_at=attempt_at,
                    error=f"Excel截图失败，未发送: {exc}",
                    emit_log=emit_log,
                )
        else:
            image_path = cached_image

        image_stat = self._image_stat(image_path)
        delivery_state = {
            **delivery_state,
            "image_path": str(image_path),
            "image_signature": target_signature,
            **image_stat,
        }
        self._persist_delivery(session_id=session_id, delivery=delivery_state, emit_log=emit_log)

        try:
            client = self.link_service._build_feishu_client()
            emit_log(f"[交接班][容量表图片发送] 飞书图片上传开始 image={image_path}")
            upload_result = client.upload_image(str(image_path))
            image_key = _text(upload_result.get("image_key"))
            if not image_key:
                raise RuntimeError("飞书图片上传未返回 image_key")
            emit_log(f"[交接班][容量表图片发送] 飞书图片上传成功 image_key={image_key}")
        except Exception as exc:  # noqa: BLE001
            return self._fail(
                session=current_session,
                delivery=delivery_state,
                attempt_at=attempt_at,
                error=f"飞书图片上传失败: {exc}",
                emit_log=emit_log,
            )

        successful_recipients: List[str] = []
        failed_recipients: List[Dict[str, str]] = []
        for recipient in recipients:
            open_id = _text(recipient.get("open_id"))
            note = _text(recipient.get("note"))
            receive_id_type = self.link_service._resolve_effective_receive_id_type(open_id, "open_id")
            try:
                emit_log(
                    "[交接班][容量表图片发送] 文本发送开始 "
                    f"building={building_text}, session_id={session_id}, open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}"
                )
                client.send_text_message(
                    receive_id=open_id,
                    receive_id_type=receive_id_type,
                    text=message_text,
                )
                emit_log(
                    "[交接班][容量表图片发送] 文本发送成功 "
                    f"building={building_text}, session_id={session_id}, open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}"
                )
            except Exception as exc:  # noqa: BLE001
                failed_recipients.append({"open_id": open_id, "note": note, "step": "text", "error": str(exc)})
                emit_log(
                    "[交接班][容量表图片发送] 文本发送失败 "
                    f"building={building_text}, session_id={session_id}, open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}, error={exc}"
                )
                continue
            try:
                emit_log(
                    "[交接班][容量表图片发送] 图片发送开始 "
                    f"building={building_text}, session_id={session_id}, open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}"
                )
                client.send_image_message(
                    receive_id=open_id,
                    receive_id_type=receive_id_type,
                    image_key=image_key,
                )
                successful_recipients.append(open_id)
                emit_log(
                    "[交接班][容量表图片发送] 图片发送成功 "
                    f"building={building_text}, session_id={session_id}, open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}"
                )
            except Exception as exc:  # noqa: BLE001
                failed_recipients.append({"open_id": open_id, "note": note, "step": "image", "error": str(exc)})
                emit_log(
                    "[交接班][容量表图片发送] 图片发送失败 "
                    f"building={building_text}, session_id={session_id}, open_id={open_id}, receive_id_type={receive_id_type}, note={note or '-'}, error={exc}"
                )

        status = "success" if not failed_recipients and len(successful_recipients) == len(recipients) else "failed"
        error = "" if status == "success" else "审核文本和容量表图片发送失败，详见收件人明细"
        final_delivery = {
            **delivery_state,
            "status": status,
            "last_attempt_at": attempt_at,
            "last_sent_at": attempt_at if status == "success" else _text(delivery_state.get("last_sent_at")),
            "error": error,
            "image_key": image_key,
            "successful_recipients": successful_recipients,
            "failed_recipients": failed_recipients,
            "source": "manual",
        }
        persisted_delivery = self._persist_delivery(session_id=session_id, delivery=final_delivery, emit_log=emit_log)
        review_delivery = self._persist_review_delivery(
            session=current_session,
            status=status,
            attempt_at=attempt_at,
            successful_recipients=successful_recipients,
            failed_recipients=failed_recipients,
            error=error,
            emit_log=emit_log,
        )
        emit_log(
            "[交接班][容量表图片发送] 完成 "
            f"building={building_text}, session_id={session_id}, status={status}, successful={len(successful_recipients)}, "
            f"failed={len(failed_recipients)}, failed_recipients={failed_recipients}"
        )
        return {
            "ok": status == "success",
            "status": status,
            "error": error,
            "building": building_text,
            "session_id": session_id,
            "successful_recipients": successful_recipients,
            "failed_recipients": failed_recipients,
            "capacity_image_delivery": persisted_delivery,
            "review_link_delivery": review_delivery,
        }
