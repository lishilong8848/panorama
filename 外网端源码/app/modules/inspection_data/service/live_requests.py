from __future__ import annotations

import contextlib
import re
import sqlite3
import time
import uuid
from pathlib import Path


MAX_REQUEST_AGE_MS = 15 * 60 * 1000
ACTIVE_JOB_STATES = {"queued", "waiting_resource", "running"}
SAFE_FAILURE = "巡检采集或完整批次上传未完成，请稍后使用原 request_id 重试"


def bridge_config(config):
    value = config.get("inspection_data_upload", {}).get("lighthouse_bridge", {})
    return value if isinstance(value, dict) else {}


def linked_mode(config):
    cfg = bridge_config(config)
    return cfg.get("enabled") is True


def validate_identity(request_id, building):
    if not isinstance(request_id, str) or not re.fullmatch(r"[0-9a-f]{32}", request_id):
        raise ValueError("request_id 必须为小写 uuidhex")
    if building not in ("D楼", "E楼"):
        raise ValueError("building 仅支持 D楼、E楼")


class LiveRequestConflict(ValueError):
    pass


class LiveRequestStore:
    """Durable per-building batches; workers acknowledge only their own request set."""

    def __init__(self, config):
        self.path = Path(config.get("paths", {}).get("runtime_state_root") or ".runtime") / "inspection_live.sqlite3"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY, building TEXT NOT NULL, not_before INTEGER NOT NULL,
                    batch_id TEXT, state TEXT NOT NULL DEFAULT 'pending', source_time INTEGER,
                    uploaded_at INTEGER, job_id TEXT, error TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id TEXT PRIMARY KEY, building TEXT NOT NULL, not_before INTEGER NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending', job_id TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                    retry_at INTEGER NOT NULL DEFAULT 0);
            """)

    @contextlib.contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def public(row):
        return {"ok": True, **{key: row[key] for key in (
            "request_id", "building", "state", "source_time", "uploaded_at", "job_id", "error")}}

    def get(self, request_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM requests WHERE request_id=?", (request_id,)).fetchone()
            return self.public(row) if row else None

    def submit(self, request_id, building, not_before, *, now_ms=None):
        validate_identity(request_id, building)
        if type(not_before) is not int:
            raise ValueError("not_before 必须为 Unix 毫秒整数")
        now = int(time.time() * 1000) if now_ms is None else now_ms
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM requests WHERE request_id=?", (request_id,)).fetchone()
            if row:
                if row["building"] != building or row["not_before"] != not_before:
                    raise LiveRequestConflict("同一 request_id 的楼栋和采样要求不能变更")
                if row["state"] == "failed":
                    db.execute("UPDATE batches SET state='pending',attempts=0,retry_at=0 WHERE batch_id=? AND state='failed'", (row["batch_id"],))
                    db.execute("UPDATE requests SET state='pending',error='' WHERE batch_id=? AND state='failed'", (row["batch_id"],))
                    row = db.execute("SELECT * FROM requests WHERE request_id=?", (request_id,)).fetchone()
                return self.public(row), False
            if not now - MAX_REQUEST_AGE_MS <= not_before <= now:
                raise ValueError("not_before 必须在当前时刻之前15分钟内，不能为未来时间")
            db.execute("INSERT INTO requests(request_id,building,not_before) VALUES(?,?,?)", (request_id, building, not_before))
        return self.get(request_id), True

    def next_batch(self, building):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            batch = db.execute("SELECT * FROM batches WHERE building=? AND state IN ('pending','running')", (building,)).fetchone()
            if batch:
                if batch["state"] == "pending":
                    db.execute("UPDATE requests SET batch_id=? WHERE building=? AND batch_id IS NULL AND state='pending'", (batch["batch_id"], building))
                    db.execute("UPDATE batches SET not_before=(SELECT MAX(not_before) FROM requests WHERE batch_id=?) WHERE batch_id=?", (batch["batch_id"], batch["batch_id"]))
                    batch = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch["batch_id"],)).fetchone()
                return dict(batch)
            rows = db.execute("SELECT * FROM requests WHERE building=? AND batch_id IS NULL AND state='pending'", (building,)).fetchall()
            if not rows:
                return None
            batch_id = uuid.uuid4().hex
            floor = max(row["not_before"] for row in rows)
            db.execute("INSERT INTO batches(batch_id,building,not_before) VALUES(?,?,?)", (batch_id, building, floor))
            db.executemany("UPDATE requests SET batch_id=? WHERE request_id=?", [(batch_id, row["request_id"]) for row in rows])
            return dict(db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone())

    def set_job(self, batch_id, job_id):
        with self.connect() as db:
            db.execute("UPDATE batches SET job_id=? WHERE batch_id=?", (job_id, batch_id))
            db.execute("UPDATE requests SET job_id=? WHERE batch_id=?", (job_id, batch_id))

    def begin(self, batch_id):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not row or row["state"] in ("uploaded", "failed"):
                return None
            db.execute("UPDATE batches SET state='running', attempts=attempts+1 WHERE batch_id=?", (batch_id,))
            db.execute("UPDATE requests SET state='running',error='' WHERE batch_id=?", (batch_id,))
            return dict(row)

    def fail(self, batch_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not row or row["state"] in ("uploaded", "failed"):
                return
            final = row["attempts"] >= 3
            state = "failed" if final else "pending"
            retry_at = int(time.time() * 1000) + 30000 * max(1, row["attempts"])
            db.execute("UPDATE batches SET state=?,retry_at=? WHERE batch_id=?", (state, retry_at, batch_id))
            db.execute("UPDATE requests SET state=?,error=? WHERE batch_id=?", (state, SAFE_FAILURE if final else "巡检任务等待恢复重试", batch_id))

    def complete(self, batch_id, result):
        with self.connect() as db:
            row = db.execute("SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if not row or row["state"] == "uploaded":
                return
            building = row["building"]
            item = result.get("results", {}).get(building, {})
            marker = item.get("batch_completion", {})
            source_time = result.get("source_time")
            uploaded_at = result.get("uploaded_at")
            if (result.get("status") != "success" or result.get("live_batch_id") != batch_id
                    or result.get("buildings") != [building] or result.get("dry_run")
                    or type(source_time) is not int or source_time < row["not_before"]
                    or type(uploaded_at) is not int or uploaded_at < source_time
                    or marker.get("status") != "completed" or marker.get("building") != building
                    or marker.get("upload_time") != item.get("upload_time")
                    or type(item.get("uploaded_count")) is not int or item["uploaded_count"] <= 0
                    or type(marker.get("device_count")) is not int or marker["device_count"] <= 0
                    or type(marker.get("point_count")) is not int or marker["point_count"] < 0
                    or marker["device_count"] + marker["point_count"] != item["uploaded_count"]):
                raise ValueError("巡检批次回执不完整或采样时间未达要求")
            db.execute("UPDATE batches SET state='uploaded' WHERE batch_id=?", (batch_id,))
            db.execute("UPDATE requests SET state='uploaded',source_time=?,uploaded_at=?,error='' WHERE batch_id=?",
                       (source_time, uploaded_at, batch_id))


def dispatch_live_requests(container):
    config = container.runtime_config
    if (not linked_mode(config)
            or not config.get("inspection_data_upload", {}).get("enabled", True)
            or container._configured_deployment_snapshot().get("role_mode") != "external"):
        return
    store = LiveRequestStore(config)
    for building in ("D楼", "E楼"):
        batch = store.next_batch(building)
        if not batch:
            continue
        key = "inspection_data_upload:live:" + building[0]
        active = container.job_service.find_active_job_by_dedupe_key(key)
        if active:
            continue
        if batch["job_id"]:
            try:
                job = container.job_service.get_job(batch["job_id"])
            except KeyError:
                job = None
            if job and job.get("status") in ACTIVE_JOB_STATES:
                continue
            # The worker normally commits the receipt itself. Recover a crash after
            # its result was persisted, before the local acknowledgement was saved.
            if job and job.get("status") == "success" and isinstance(job.get("result"), dict):
                try:
                    store.complete(batch["batch_id"], job["result"])
                    continue
                except ValueError:
                    pass
            if batch["state"] == "running":
                store.fail(batch["batch_id"])
                continue
        if batch["retry_at"] > int(time.time() * 1000):
            continue
        job = container.job_service.start_worker_job(
            name=f"{building}按需巡检采集上传", worker_handler="inspection_data_live_upload",
            worker_payload={"live_batch_id": batch["batch_id"]},
            resource_keys=[f"inspection_data_upload:{building[0]}"], priority="manual",
            feature="inspection_data_upload", dedupe_key=key, submitted_by="lighthouse",
        )
        store.set_job(batch["batch_id"], job.job_id)


def run_live_upload(config, payload, emit_log, runtime=None):
    from app.modules.inspection_data.service.inspection_data_upload_service import InspectionDataUploadService

    if not linked_mode(config) or not config.get("inspection_data_upload", {}).get("enabled", True):
        raise ValueError("巡检联动未启用或鉴权未配置")
    store = LiveRequestStore(config)
    batch_id = payload.get("live_batch_id")
    batch = store.begin(batch_id)
    if batch is None:
        return {"status": "skipped", "reason": "request_already_finished"}
    def acknowledge(receipt):
        store.complete(batch_id, {**receipt, "live_batch_id": batch_id})

    try:
        result = InspectionDataUploadService(config).run(
            building=batch["building"], not_before=batch["not_before"], force_refresh=True,
            log=emit_log, check_cancel=runtime.raise_if_cancelled if runtime else None,
            receipt_callback=acknowledge,
        )
        result["live_batch_id"] = batch_id
        store.complete(batch_id, result)
        return result
    except Exception:
        store.fail(batch_id)
        # Never expose bridge URLs, request headers, or source system exceptions.
        raise RuntimeError(SAFE_FAILURE) from None
