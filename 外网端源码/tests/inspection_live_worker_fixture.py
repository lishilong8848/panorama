"""Test-only transport doubles; run the production worker/handler/upload pipeline."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


def main():
    import requests
    from app.modules.inspection_data.service import inspection_data_upload_service as upload
    from app.modules.inspection_data.service.live_requests import LiveRequestStore
    from app.worker.entry import main as worker_main
    from test_inspection_data_service import Cloud, inspection_datasets, set_dataset_end

    def forbidden(*args, **kwargs):
        raise AssertionError("Real HTTP requests are forbidden in isolated queue tests")
    urllib.request.urlopen = forbidden
    requests.Session.request = forbidden
    job_dir = Path(sys.argv[sys.argv.index("--job-dir") + 1]).resolve()
    config = json.loads((job_dir / "config_snapshot.json").read_text(encoding="utf8"))
    output = Path(config["_isolated_inspection_output"]).resolve()
    assert job_dir.is_relative_to(Path(config["paths"]["runtime_state_root"]).resolve())
    output.mkdir(parents=True, exist_ok=True)
    payload_file = Path(sys.argv[sys.argv.index("--payload-file") + 1])
    payload = json.loads(payload_file.read_text(encoding="utf8"))
    with LiveRequestStore(config).connect() as db:
        batch = db.execute("SELECT * FROM batches WHERE batch_id=?", (payload["live_batch_id"],)).fetchone()
        building = batch["building"]
    name = building[0]
    refreshes = []
    documents = inspection_datasets(output)[building]
    settings = {"D楼": {}, "E楼": {}}

    class Internal:
        def refresh_latest_source_cache(self, **kwargs):
            refreshes.append(kwargs)
            assert kwargs["force_refresh"] is True and kwargs["buildings"] == [building]
            started = output / ("started-" + name)
            started.write_text(str(os.getpid()), encoding="utf8")
            if config.get("_isolated_gate_building") == building:
                deadline = time.monotonic() + 30
                while not (output / ("release-" + name)).exists():
                    if time.monotonic() >= deadline:
                        raise RuntimeError("isolated queue gate timed out")
                    time.sleep(0.02)
            doc = documents[kwargs["source_family"]]
            set_dataset_end(doc, upload.datetime.now().replace(microsecond=0))
            doc["collection_started_at"] = int(time.time() * 1000)
            return {"ok": True, "accepted": True}
        def get_latest_inspection_points(self, **kwargs):
            assert kwargs["building"] == building
            return {"ok": True, "datasets": copy.deepcopy(list(documents.values()))}

    cloud_path = output / ("cloud-" + name + ".json")
    class IsolatedCloud(Cloud):
        def __init__(self):
            super().__init__()
            if cloud_path.exists():
                self.rows = json.loads(cloud_path.read_text(encoding="utf8"))
        def save(self):
            cloud_path.write_text(json.dumps(self.rows, ensure_ascii=False), encoding="utf8")
        def batch_create_records(self, table, rows, **kwargs):
            flag = output / ("failed-once-" + name)
            if config.get("_isolated_fail_once") == building and not flag.exists():
                flag.write_text("failed once before remote creation", encoding="utf8")
                raise RuntimeError("isolated creation failure")
            result = super().batch_create_records(table, rows, **kwargs)
            for response in result:
                for record in response["data"]["records"]:
                    record["record_id"] = job_dir.name + "-" + record["record_id"]
            self.save()
            return result
        def batch_delete_records(self, table, ids, **kwargs):
            result = super().batch_delete_records(table, ids, **kwargs)
            self.save()
            return result
        def update_record(self, table, record_id, fields):
            result = super().update_record(table, record_id, fields)
            self.save()
            return result

    cloud = IsolatedCloud()
    upload.InternalBridgeHttpClient.from_runtime_config = classmethod(lambda cls, cfg: Internal())
    upload.InspectionDataUploadService.make_bitable_client = lambda self: cloud
    upload.refresh_cloud_point_settings = lambda *args, **kwargs: (settings, 1, {})
    upload.load_point_settings = lambda cfg: settings
    upload.report_operating_controls = lambda *args: {}
    upload.handover_inspection_rows = lambda *args, **kwargs: {}
    upload.BACKUP_ROOT = output / "backups"
    exit_code = 1
    try:
        exit_code = worker_main()
        return exit_code
    finally:
        (output / ("job-" + job_dir.name + ".json")).write_text(json.dumps({
            "pid": os.getpid(), "exit_code": exit_code, "building": building,
            "refreshes": refreshes, "events": cloud.events,
            "records": cloud.rows[upload.DEFAULT_CONFIG["tables"][building]],
        }, ensure_ascii=False), encoding="utf8")


if __name__ == "__main__":
    raise SystemExit(main())
