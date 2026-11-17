from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from handover_log_module.service import handover_delivery_prepare_service as module


@pytest.mark.parametrize("change", ["live_excel", "review_document", "artifact_copy", "reuse"])
def test_preparation_uses_frozen_excel_and_still_checks_saved_version(monkeypatch, tmp_path, change):
    service = module.HandoverDeliveryPrepareService.__new__(module.HandoverDeliveryPrepareService)
    service.config = {}
    log = tmp_path / "live-log.xlsx"
    capacity = tmp_path / "live-capacity.xlsx"
    log.write_bytes(b"log version 1")
    capacity.write_bytes(b"capacity version 1")
    session = {
        "session_id": "A|2026-11-09|night", "building": "A", "batch_key": "2026-11-09|night",
        "duty_date": "2026-11-09", "duty_shift": "night", "revision": 1,
        "output_file": str(log), "capacity_output_file": str(capacity),
        "capacity_sync": {"status": "ready", "input_signature": "input"},
    }
    state = {"revision": 1, "document": {}, "source_excel_path": str(log)}
    service.review_service = SimpleNamespace(
        get_session_by_id=lambda _id: session,
        get_saved_substation_110kv_block=lambda **_kw: {},
    )
    store = SimpleNamespace(get_document=lambda _id: state, get_sync_state=lambda _id: {
        "status": "synced", "synced_revision": 1, "pending_revision": 0,
    })
    service.document_state = SimpleNamespace(
        _store=lambda _building: store, ensure_document_for_session=Mock(),
        load_document=lambda _session: ({}, session),
    )
    if change == "reuse":
        state.update(source_excel_mtime=str(log.stat().st_mtime_ns), source_excel_size=log.stat().st_size)
        session["capacity_sync"]["download_snapshot"] = {"revision": 1}
        service.document_state.writer = SimpleNamespace(fixed_cells_need_sync_for_file=Mock(return_value=False))
    service.summary_service = SimpleNamespace(build_for_session=lambda *_a, **_kw: "summary")

    def render(*, source_path, output_path, emit_log):
        assert source_path.read_bytes() == b"capacity version 1"
        output_path.write_bytes(b"image")
        if change == "live_excel":
            capacity.write_bytes(b"another task rewrote the live workbook")
        elif change == "review_document":
            state["revision"] += 1
        elif change == "artifact_copy":
            source_path.write_bytes(b"renderer rewrote the private copy")

    service.image_service = SimpleNamespace(
        capacity_service=SimpleNamespace(download_snapshot_matches=Mock(return_value=True), build_capacity_overlay_signature=lambda **_kw: {
            "valid": True, "signature": "overlay", "input_signature": "input",
        }),
        _validate_capacity_workbook_ready_for_image=Mock(),
        _render_capacity_report_image=render, _validate_rendered_image_content=Mock(),
    )
    queue = SimpleNamespace(
        enqueue_review_excel_sync=Mock(return_value={"status": "pending", "job_id": "log"}),
        wait_for_barrier=Mock(return_value={"status": "success"}),
        enqueue_capacity_overlay_sync=Mock(return_value={"job_id": "overlay"}),
        wait_for_job=Mock(return_value={"status": "success"}),
    )
    monkeypatch.setattr(module, "HandoverXlsxWriteQueueService", lambda *_a, **_kw: queue)
    monkeypatch.setattr(module, "_runtime_root", lambda _config: tmp_path)

    if change in {"live_excel", "reuse"}:
        prepared = service.prepare(session["session_id"], expected_revision=1, expected_document_revision=1)
        assert Path(prepared["paths"]["capacity"]).read_bytes() == b"capacity version 1"
        assert Path(prepared["paths"]["log"]).read_bytes() == b"log version 1"
    else:
        with pytest.raises(module.HandoverDeliveryPrepareRetryableError):
            service.prepare(session["session_id"], expected_revision=1, expected_document_revision=1)
        assert not list((tmp_path / "handover").rglob("capacity.xlsx"))
    assert not list(tmp_path.rglob(".prepare-*"))
    if change == "reuse":
        queue.enqueue_review_excel_sync.assert_not_called()
        queue.enqueue_capacity_overlay_sync.assert_not_called()
        queue.wait_for_job.assert_not_called()
    else:
        assert [call.kwargs["job_id"] for call in queue.wait_for_job.call_args_list] == ["log", "overlay"]
    queue.wait_for_barrier.assert_not_called()


def test_delivery_write_timeout_names_exact_stage_and_job():
    service = module.HandoverDeliveryPrepareService.__new__(module.HandoverDeliveryPrepareService)
    queue = SimpleNamespace(wait_for_job=Mock(side_effect=TimeoutError))
    with pytest.raises(module.HandoverDeliveryPrepareRetryableError, match="容量Excel同步.*C楼.*job_c"):
        service._wait_for_write(queue, building="C楼", job={"job_id": "job_c"}, label="容量Excel同步")


def test_snapshot_retries_only_while_copying_same_review_version(monkeypatch, tmp_path):
    service = module.HandoverDeliveryPrepareService.__new__(module.HandoverDeliveryPrepareService)
    source, copy = tmp_path / "live.xlsx", tmp_path / "copy.xlsx"
    source.write_bytes(b"first")
    real_copy = module.shutil.copy2

    def rewrite_once(src, dst):
        real_copy(src, dst)
        if src.read_bytes() == b"first":
            src.write_bytes(b"second")

    monkeypatch.setattr(module.shutil, "copy2", rewrite_once)
    sleep = Mock()
    monkeypatch.setattr(module, "time", SimpleNamespace(sleep=sleep))
    hashes = service._capture_sources(
        {"log": source}, {"log": copy}, check_version=Mock(), check_cancelled=Mock(), emit_log=Mock(),
    )
    assert copy.read_bytes() == b"second"
    assert hashes["log"] == module._digest(copy)
    sleep.assert_called_once()
