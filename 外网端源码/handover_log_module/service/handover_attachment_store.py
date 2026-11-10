"""Serialize and pace handover attachment requests to the same Bitable table."""
from contextlib import contextmanager
from datetime import datetime, timedelta
import hashlib
import mimetypes
import os
from pathlib import Path
import tempfile
import time

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableRateLimited
from handover_log_module.service.handover_daily_report_bitable_export_service import HandoverDailyReportBitableExportService


@contextmanager
def attachment_record_lock(key, timeout=180):
    path = Path(tempfile.gettempdir()) / ("panorama_attachment_" + hashlib.sha256(key.encode()).hexdigest() + ".lock")
    fd = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o600)
    locked = False
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
                break
            except OSError as exc:
                if exc.errno not in {11, 13, 35, 36} and getattr(exc, "winerror", None) not in {32, 33, 36}:
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError("班次附件记录正在更新，请稍后续传") from exc
                time.sleep(0.1)
        yield fd
    finally:
        try:
            if locked:
                os.lseek(fd, 0, os.SEEK_SET)
                if os.name == "nt":
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


class HandoverAttachmentStore:
    FIELD = "交接班日志"

    def __init__(self, config, *, client=None):
        self.exporter = HandoverDailyReportBitableExportService(config)
        self.cfg = self.exporter._normalize_cfg()
        self.target = self.cfg["target"]
        self.table_id = self.target["table_id"]
        self.client = client or self.exporter._new_client(self.cfg)
        self.request_interval = 0.5 if client is None else 0.0

    def _call(self, method, **kwargs):
        # Flights for different shifts and the H/110 supplements use the same table queue.
        with attachment_record_lock(f"bitable-attachments:{self.target['app_token']}|{self.table_id}") as fd:
            # Byte zero is locked; the rest stores a cooldown shared by worker processes.
            os.lseek(fd, 1, os.SEEK_SET)
            try:
                remaining = float(os.read(fd, 64) or b"0") - time.time()
            except ValueError:
                remaining = 0
            if remaining > 0:
                raise FeishuBitableRateLimited(f"飞书正在限流冷却，请在约 {int(remaining) + 1} 秒后重试",
                                             retry_after=remaining)
            try:
                return method(**kwargs)
            except FeishuBitableRateLimited as exc:
                os.lseek(fd, 1, os.SEEK_SET)
                os.write(fd, f"{time.time() + exc.retry_after:.3f}".encode("ascii"))
                os.ftruncate(fd, os.lseek(fd, 0, os.SEEK_CUR))
                raise
            finally:
                if self.request_interval:
                    time.sleep(self.request_interval)

    def _lock_key(self, date, shift):
        return f"{self.target['app_token']}|{self.table_id}|{date}|{shift}"

    def record_url(self, record_id):
        return f"https://vnet.feishu.cn/base/{self.target['app_token']}?table={self.table_id}&record={record_id}"

    def _records(self, date, shift):
        start = datetime.strptime(date, "%Y-%m-%d")
        if shift not in {"day", "night"}:
            raise ValueError("班次必须为 day 或 night")
        end = (start + timedelta(days=1)).strftime("%Y-%m-%d")
        field = self.cfg["fields"]["date"]
        records = self._call(self.client.list_records,
            table_id=self.table_id, page_size=500, max_records=0,
            filter_formula=f'AND(CurrentValue.[{field}]>=TODATE("{date}"), CurrentValue.[{field}]<TODATE("{end}"))',
        )
        ids = self.exporter._match_existing_record_ids(
            existing_records=records, duty_date=date, duty_shift=shift, cfg=self.cfg,
        )
        return sorted((r for r in records if r.get("record_id") in ids), key=lambda r: r["record_id"])

    def _ensure_locked(self, date, shift):
        fields_meta = self._call(self.client.list_fields, table_id=self.table_id)
        if not any(f.get("field_name") == self.FIELD and int(f.get("type", 0)) == 17 for f in fields_meta):
            raise ValueError(f"目标表缺少附件字段：{self.FIELD}")
        records = self._records(date, shift)
        if records:
            return records[0]
        fields = self.cfg["fields"]
        responses = self._call(self.client.batch_create_records, table_id=self.table_id, fields_list=[{
            fields["year"]: self.exporter._year_text(date),
            fields["date"]: self.exporter._midnight_timestamp_ms(date),
            fields["shift"]: self.exporter._shift_text(shift),
        }], batch_size=1)
        for response in responses:
            for record in response.get("data", {}).get("records", []):
                if record.get("record_id"):
                    return record
        # Do not blindly create again if the server accepted a write with an incomplete response.
        records = self._records(date, shift)
        if not records:
            raise RuntimeError("班次记录创建结果尚未可见，请稍后重试，未重复创建")
        return records[0]

    def ensure_record(self, date, shift):
        with attachment_record_lock(self._lock_key(date, shift)):
            record = self._ensure_locked(date, shift)
            record_id = record["record_id"]
            url = self.record_url(record_id)
            fields = record.get("fields", {})
            link_field = self.cfg["fields"]["report_link"]
            if not self.exporter._is_same_report_link(fields.get(link_field), url):
                self._call(self.client.update_record,
                    table_id=self.table_id, record_id=record_id,
                    fields={link_field: {"link": url, "text": "查看本班交接班文件"}},
                )
            return {
                "record_id": record_id, "record_url": url, "status": "success",
                "attachment_names": [item.get("name") for item in fields.get(self.FIELD, []) or []
                                     if isinstance(item, dict)],
            }

    def upload_files(self, date, shift, files, *, is_current=None, expected_record_id="", required_existing_names=()):
        """files maps stable attachment names to completed local artifacts."""
        if not files:
            raise ValueError("没有可上传的成品附件")
        for name, path in files.items():
            if not Path(path).is_file() or Path(path).stat().st_size <= 0:
                raise ValueError(f"附件不存在或为空：{name}")

        def transfer(item):
            name, path = item
            content = Path(path).read_bytes()
            if not content:
                raise ValueError(f"附件为空：{name}")
            token = self._call(self.client.upload_attachment_bytes,
                file_name=name, content=content,
                mime_type=mimetypes.guess_type(name)[0] or "application/octet-stream",
            )
            if not token:
                raise RuntimeError(f"附件上传没有返回凭据：{name}")
            return {"file_token": token, "name": name}

        uploaded = [transfer(item) for item in files.items()]
        with attachment_record_lock(self._lock_key(date, shift)):
            if is_current is not None and not is_current():
                raise RuntimeError("成品版本已更新，旧版本未写入附件记录")
            record_id = expected_record_id or self._ensure_locked(date, shift)["record_id"]
            current = self._call(self.client.get_record_by_id, table_id=self.table_id, record_id=record_id)
            fields = current.get("fields")
            if current.get("record_id") != record_id or not isinstance(fields, dict):
                raise ValueError("未能读取目标班次记录，未覆盖原附件")
            if not self.exporter._match_existing_record_ids(
                existing_records=[current], duty_date=date, duty_shift=shift, cfg=self.cfg,
            ):
                raise RuntimeError("班次记录日期或班次已变化，未覆盖原附件")
            old = fields.get(self.FIELD)
            if old is None:
                old = []
            if not isinstance(old, list) or any(
                not isinstance(item, dict) or not item.get("file_token") or not item.get("name")
                for item in old
            ):
                raise ValueError("班次附件字段返回格式异常，未覆盖原附件")
            if not set(required_existing_names) <= {item["name"] for item in old}:
                raise RuntimeError("班次已有附件在上传期间变化，请续传以补齐缺失文件")
            # A change of workbook extension is still a replacement of the same deliverable.
            names = {Path(name).stem for name in files}
            preserved = [item for item in old if Path(item["name"]).stem not in names]
            tokens = [{"file_token": item["file_token"]} for item in preserved + uploaded]
            url = self.record_url(record_id)
            if is_current is not None and not is_current():
                raise RuntimeError("成品版本已更新，旧版本未写入附件记录")
            self._call(self.client.update_record, table_id=self.table_id, record_id=record_id, fields={
                self.FIELD: tokens,
                self.cfg["fields"]["report_link"]: {"link": url, "text": "查看本班交接班文件"},
            })
            saved = self._call(self.client.get_record_by_id, table_id=self.table_id, record_id=record_id)
            saved_tokens = {item.get("file_token") for item in saved.get("fields", {}).get(self.FIELD, [])}
            if saved_tokens != {item["file_token"] for item in tokens}:
                raise RuntimeError("班次附件写入结果未通过回查，请重试")
            return {"status": "success", "record_id": record_id, "record_url": url, "attachment_count": len(tokens)}
