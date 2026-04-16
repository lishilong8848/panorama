from __future__ import annotations

import threading
import uuid

from app.modules.shared_bridge.service.shared_bridge_runtime_mirror_store import SharedBridgeRuntimeMirrorStore


def test_runtime_mirror_store_serializes_concurrent_writes(tmp_path) -> None:
    store = SharedBridgeRuntimeMirrorStore(runtime_config={"paths": {}, "runtime_state_root": str(tmp_path)}, role_mode="internal")
    errors: list[Exception] = []
    prefix = uuid.uuid4().hex

    def _worker(index: int) -> None:
        try:
            store.upsert_task({"task_id": f"{prefix}-task-{index}", "updated_at": f"2026-04-15 10:00:{index:02d}"})
            store.set_snapshot(
                key="summary",
                payload={"updated_at": f"2026-04-15 10:00:{index:02d}", "index": index},
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_worker, args=(index,)) for index in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3.0)

    assert errors == []
    for index in range(10):
        payload = store.get_task(f"{prefix}-task-{index}")
        assert payload is not None
        assert payload["task_id"] == f"{prefix}-task-{index}"
    snapshot = store.get_snapshot(key="summary")
    assert snapshot is not None
    assert "index" in snapshot
