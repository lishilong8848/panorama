from __future__ import annotations

from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore


def test_retry_partial_failed_task_requeues_external_stage_only(tmp_path) -> None:
    store = SharedBridgeStore(tmp_path)
    store.ensure_ready()
    task = store.create_day_metric_from_download_task(
        selected_dates=["2026-03-28"],
        building_scope="single",
        building="A楼",
        created_by_role="external",
        created_by_node_id="ext-01",
        requested_by="manual",
    )

    claimed_internal = store.claim_next_task(role_target="internal", node_id="int-01", lease_sec=30)
    assert claimed_internal is not None
    int_token = next(
        (stage.get("claim_token", "") for stage in claimed_internal.get("stages", []) if stage.get("stage_id") == "internal_download"),
        "",
    )
    store.complete_stage(
        task_id=task["task_id"],
        stage_id="internal_download",
        claim_token=int_token,
        side="internal",
        stage_result={"status": "ok", "selected_dates": ["2026-03-28"], "selected_buildings": ["A楼"]},
        next_task_status="ready_for_external",
        task_result={
            "status": "ready_for_external",
            "internal": {"status": "ok", "selected_dates": ["2026-03-28"], "selected_buildings": ["A楼"]},
        },
    )

    claimed_external = store.claim_next_task(role_target="external", node_id="ext-01", lease_sec=30)
    assert claimed_external is not None
    ext_token = next(
        (stage.get("claim_token", "") for stage in claimed_external.get("stages", []) if stage.get("stage_id") == "external_upload"),
        "",
    )
    store.complete_stage(
        task_id=task["task_id"],
        stage_id="external_upload",
        claim_token=ext_token,
        side="external",
        stage_result={"status": "partial_failed", "error": "upload failed"},
        next_task_status="partial_failed",
        task_error="upload failed",
        task_result={
            "status": "partial_failed",
            "internal": {"status": "ok", "selected_dates": ["2026-03-28"], "selected_buildings": ["A楼"]},
            "external": {"status": "partial_failed", "error": "upload failed"},
        },
    )

    assert store.retry_task(task["task_id"]) is True

    updated = store.get_task(task["task_id"])
    assert updated is not None
    assert updated["status"] == "ready_for_external"
    assert updated["result"]["status"] == "ready_for_external"
    assert updated["result"]["internal"]["selected_buildings"] == ["A楼"]

    stages = {stage["stage_id"]: stage for stage in updated["stages"]}
    assert stages["internal_download"]["status"] == "success"
    assert stages["external_upload"]["status"] == "pending"
