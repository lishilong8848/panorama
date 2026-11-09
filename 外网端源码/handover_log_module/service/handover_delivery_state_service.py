"""Persistent preparation manifests; only complete generations become downloadable."""
import json
from contextlib import closing
from pathlib import Path
import sqlite3
from datetime import datetime, timedelta

from pipeline_utils import get_app_dir


class HandoverDeliveryStateService:
    ACTIVE = {"queued", "preparing"}

    def __init__(self, config, *, initialize=True):
        paths = config.get("_global_paths", config.get("paths", {}))
        root = Path(paths.get("runtime_state_root") or Path(get_app_dir()) / ".runtime")
        if not root.is_absolute():
            root = Path(get_app_dir()) / root
        self.path = root / "handover_delivery.db"
        if not initialize:
            return
        root.mkdir(parents=True, exist_ok=True)
        with closing(self.connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            with db:
                db.execute("CREATE TABLE IF NOT EXISTS delivery (session_id TEXT PRIMARY KEY, batch_key TEXT NOT NULL, building TEXT NOT NULL, payload TEXT NOT NULL)")
                db.execute("CREATE INDEX IF NOT EXISTS delivery_batch ON delivery(batch_key)")

    def connect(self):
        db = sqlite3.connect(self.path, timeout=10)
        return db

    def get(self, session_id):
        if not self.path.is_file():
            return {}
        db = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.5)
        try:
            row = db.execute("SELECT payload FROM delivery WHERE session_id=?", (session_id,)).fetchone()
            return json.loads(row[0]) if row else {}
        finally:
            db.close()

    def begin(self, session, generation_id):
        payload = {
            "session_id": session["session_id"], "building": session["building"],
            "batch_key": session["batch_key"], "generation_id": generation_id,
            "revision": int(session.get("document_revision", session.get("revision", 0)) or 0),
            "status": "queued", "artifacts_ready": False, "error": "",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        db = self.connect()
        try:
            with db:
                db.execute("INSERT INTO delivery VALUES(?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET batch_key=excluded.batch_key,building=excluded.building,payload=excluded.payload",
                           (payload["session_id"], payload["batch_key"], payload["building"], json.dumps(payload, ensure_ascii=False)))
        finally:
            db.close()
        return payload

    def update(self, session_id, generation_id, **changes):
        db = self.connect()
        try:
            with db:
                db.execute("BEGIN IMMEDIATE")
                row = db.execute("SELECT payload FROM delivery WHERE session_id=?", (session_id,)).fetchone()
                current = json.loads(row[0]) if row else {}
                if current.get("generation_id") != generation_id:
                    raise RuntimeError("交接班成品版本已变更，本次旧任务停止写入")
                current.update(changes, updated_at=datetime.now().isoformat(timespec="seconds"))
                db.execute("UPDATE delivery SET payload=? WHERE session_id=?", (json.dumps(current, ensure_ascii=False), session_id))
                return current
        finally:
            db.close()

    def batch(self, batch_key):
        if not self.path.is_file():
            return {}
        db = sqlite3.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.5)
        try:
            return {r["building"]: r for (value,) in db.execute("SELECT payload FROM delivery WHERE batch_key=?", (batch_key,))
                    if (r := json.loads(value))}
        finally:
            db.close()

    def for_session(self, session, *, row=None):
        if row is None:
            row = self.get(session["session_id"])
        if row.get("session_id") != session["session_id"]:
            return {}
        if row.get("status") in self.ACTIVE and datetime.fromisoformat(row["updated_at"]) < datetime.now() - timedelta(minutes=45):
            return {**row, "status": "failed", "error": "成品生成任务已长时间未更新，请重新提交"}
        if row.get("artifacts_ready") and (
            row.get("revision") != int(session.get("revision", 0) or 0)
            or (row.get("document_revision") and session.get("document_revision")
                and row["document_revision"] != int(session["document_revision"]))
            or not session.get("confirmed")
        ):
            return {**row, "status": "stale", "artifacts_ready": False, "error": "审核内容已变更，请重新生成成品"}
        return row

    @staticmethod
    def sync_view(manifest, session, *, enabled=True):
        """Project new attachment state onto the existing dashboard contract."""
        status = manifest.get("status")
        url = str(manifest.get("record_url") or "")
        if not enabled:
            return {"status": "disabled"}
        if status == "uploaded" and manifest.get("artifacts_ready") and url:
            return {
                "status": "success", "success": True, "attempted": True,
                "spreadsheet_url": url, "spreadsheet_title": "班次交接班附件",
                "synced_revision": int(session.get("revision", 0) or 0),
            }
        error = manifest.get("upload_error") or (manifest.get("error") if status == "failed" else "")
        return {
            "status": "failed" if error else "pending_upload" if session.get("confirmed") else "idle",
            "error": error or "", "attempted": bool(error),
        }

    @staticmethod
    def public(manifest):
        return {key: value for key, value in manifest.items() if key not in {"paths", "hashes", "text"}}

    @staticmethod
    def artifacts_exist(manifest):
        paths = manifest.get("paths", {})
        try:
            return all(
                (path := Path(paths.get(key, ""))).is_file() and path.stat().st_size > 0
                for key in ("log", "capacity", "image", "text")
            )
        except OSError:
            return False
