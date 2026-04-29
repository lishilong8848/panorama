from handover_log_module.repository.review_building_document_store import ReviewBuildingDocumentStore


def test_claim_next_job_does_not_claim_when_another_job_is_running(tmp_path):
    config = {"_global_paths": {"runtime_state_root": str(tmp_path)}}
    store = ReviewBuildingDocumentStore(config=config, building="A楼")

    store.enqueue_sync_job(session_id="sess-1", target_revision=1)
    first = store.claim_next_job()
    store.enqueue_sync_job(session_id="sess-2", target_revision=2)
    second = store.claim_next_job()

    assert first == {"session_id": "sess-1", "target_revision": 1, "attempts": 1}
    assert second is None


def test_claim_next_job_continues_after_running_job_finishes(tmp_path):
    config = {"_global_paths": {"runtime_state_root": str(tmp_path)}}
    store = ReviewBuildingDocumentStore(config=config, building="A楼")

    store.enqueue_sync_job(session_id="sess-1", target_revision=1)
    first = store.claim_next_job()
    store.enqueue_sync_job(session_id="sess-2", target_revision=2)
    store.finish_job(
        session_id="sess-1",
        success=True,
        claimed_target_revision=1,
        synced_revision=1,
    )
    second = store.claim_next_job()

    assert first["session_id"] == "sess-1"
    assert second == {"session_id": "sess-2", "target_revision": 2, "attempts": 1}
