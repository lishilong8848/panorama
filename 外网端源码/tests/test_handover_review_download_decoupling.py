"""下载与成品提交解耦的回归测试。

覆盖：
- 未提交成品也能下载交接班日志（走原始文件 + 强制补写通道），并从稳定副本返回。
- 审核内容已修改时，旧成品被判 stale，不再当作最新版下载，回落补写下载。
- 成品对当前版本就绪时仍优先下载成品（行为不变）。
- 容量表补写失败时返回 409，不返回旧表。
- 容量表补写结果输入签名与本次下载输入不一致时返回 409。
- 容量表补写成功且签名一致时，从稳定副本返回（不是原始路径）。
- TOCTOU 防护：补写/同步完成到返回前若有人保存新版（审核版本变化），拒绝旧版下载（409）并清理副本。
- 发送审核文本/容量表图片所依赖的 _require_review_delivery 仍要求当前版本成品就绪。
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("openpyxl")

from fastapi import HTTPException

from app.modules.handover_review.api import routes
from handover_log_module.service.handover_capacity_report_service import HandoverCapacityReportService
from handover_log_module.service.handover_delivery_state_service import HandoverDeliveryStateService

BATCH = "2026-11-09|day"
SESSION_ID = f"A楼|{BATCH}"
TRACKED_CELLS = {"J12": "100", "M12": "200"}


def _make_dummy(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"dummy-not-a-real-workbook")
    return path


def _make_xlsx(path: Path) -> Path:
    from openpyxl import Workbook

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    workbook.active["A1"] = "capacity"
    workbook.save(path)
    return path


def _container() -> SimpleNamespace:
    return SimpleNamespace(add_system_log=Mock(), job_service=None)


def _request(container: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))


def _artifacts(tmp_path: Path, *, prefix: str) -> dict:
    paths = {}
    for key in ("log", "capacity", "image", "text"):
        paths[key] = str(_make_dummy(tmp_path / f"{prefix}_{key}"))
    return paths


def _base_session(tmp_path: Path, *, revision: int = 1, confirmed: bool = False) -> dict:
    # 日志/容量原始文件都做成真实 xlsx：稳定副本用 validate_excel_workbook_file 校验。
    return {
        "session_id": SESSION_ID,
        "building": "A楼",
        "building_code": "a",
        "duty_date": "2026-11-09",
        "duty_shift": "day",
        "batch_key": BATCH,
        "revision": revision,
        "confirmed": confirmed,
        "output_file": str(_make_xlsx(tmp_path / "A_log.xlsx")),
        "capacity_output_file": str(_make_xlsx(tmp_path / "A_capacity.xlsx")),
    }


def _mock_temp(monkeypatch, tmp_path) -> Path:
    """把稳定副本临时目录指到 tmp_path 下，并屏蔽清理，便于断言副本。"""
    snapshot_root = tmp_path / "snapshot_root"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(routes, "create_runtime_temp_dir", lambda kind: snapshot_root)
    monkeypatch.setattr(routes, "cleanup_runtime_temp_dir", Mock())
    return snapshot_root


def _assert_valid_xlsx(path: Path) -> None:
    from openpyxl import load_workbook

    assert path.exists() and path.stat().st_size > 0
    load_workbook(path, read_only=True).close()


@pytest.mark.parametrize(
    ("handler", "kwargs"),
    [
        (routes.handover_review_download, {"session_id": SESSION_ID}),
        (routes.handover_review_capacity_download, {"session_id": SESSION_ID}),
        (routes.handover_review_delivery_download, {"session_id": SESSION_ID, "generation_id": "ready"}),
        (routes.handover_review_capacity_image_prepare, {"payload": {
            "session_id": SESSION_ID, "trigger_source": "capacity_image_send_button",
        }}),
        (routes.handover_review_capacity_image_send, {"payload": {
            "session_id": SESSION_ID, "trigger_source": "capacity_image_send_button",
        }}),
    ],
)
def test_active_regeneration_rejects_file_actions_before_side_effects(monkeypatch, tmp_path, handler, kwargs):
    session = _base_session(tmp_path)
    container = _container()
    find_active = Mock(return_value={"status": "waiting_resource"})
    container.job_service = SimpleNamespace(find_active_job_by_dedupe_key=find_active)
    document_state = Mock()
    require_delivery = Mock()
    verify_token = Mock()
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (object(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_session_service", lambda _c: object())
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: document_state)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)
    monkeypatch.setattr(routes, "_ensure_latest_session_actionable_or_400", lambda *a, **k: None)
    monkeypatch.setattr(routes, "_require_review_delivery", require_delivery)
    monkeypatch.setattr(routes, "_verify_capacity_image_send_token", verify_token)

    with pytest.raises(HTTPException, match="重新生成") as exc:
        handler("a", _request(container), **kwargs)

    assert exc.value.status_code == 409
    find_active.assert_called_once_with(f"handover_review_regenerate:{SESSION_ID}")
    assert not document_state.mock_calls
    require_delivery.assert_not_called()
    verify_token.assert_not_called()


@pytest.mark.parametrize("changed_during_preparation", [False, True])
def test_manual_send_can_prepare_current_files_without_submission(monkeypatch, tmp_path, changed_during_preparation):
    from handover_log_module.service import handover_delivery_prepare_service

    session = _base_session(tmp_path)
    container = _container()
    service = Mock()
    current_revision = 2
    monkeypatch.setattr(routes, "_build_review_services", lambda _: (service, None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: Mock())
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)
    monkeypatch.setattr(routes, "_ensure_latest_session_actionable_or_400", lambda *a, **k: None)
    monkeypatch.setattr(routes, "_review_delivery_state", lambda *a, **k: {})
    monkeypatch.setattr(routes, "_review_document_revision_from_store", lambda *_, **__: current_revision)
    monkeypatch.setattr(routes, "_handover_cfg", lambda _: {})
    temp_root = _mock_temp(monkeypatch, tmp_path)
    paths = {"log": "log.xlsx", "capacity": "capacity.xlsx", "image": "capacity.png", "text": "handover.txt"}

    def prepare_files(*_args, **kwargs):
        nonlocal current_revision
        assert kwargs["expected_document_revision"] == 2
        assert kwargs["temporary_root"] == temp_root
        if changed_during_preparation:
            current_revision = 3
        return {"paths": paths}

    monkeypatch.setattr(handover_delivery_prepare_service, "HandoverDeliveryPrepareService",
                        lambda _: Mock(prepare=prepare_files))
    sender = Mock()
    sender.send_for_session.return_value = {"ok": True, "status": "success"}
    monkeypatch.setattr(routes, "CapacityReportImageDeliveryService", lambda *a, **k: sender)
    payload = {"session_id": SESSION_ID, "client_id": "client-1", "trigger_source": "capacity_image_send_button"}
    response = routes.handover_review_capacity_image_prepare("a", _request(container), payload)
    assert response["generation_id"].startswith("direct:")
    payload.update(send_token=response["send_token"], generation_id=response["generation_id"])

    if changed_during_preparation:
        with pytest.raises(HTTPException, match="审核内容已更新"):
            routes.handover_review_capacity_image_send("a", _request(container), payload)
        sender.send_for_session.assert_not_called()
    else:
        assert routes.handover_review_capacity_image_send("a", _request(container), payload)["ok"]
        assert sender.send_for_session.call_args.kwargs["prepared_artifacts"] == paths
        assert sender.send_for_session.call_args.kwargs["manual_trigger"] is True
    routes.cleanup_runtime_temp_dir.assert_called_once_with(temp_root)


def test_display_actions_recover_after_regeneration_and_other_sessions_remain_available(tmp_path):
    session = _base_session(tmp_path)
    active = {f"handover_review_regenerate:{SESSION_ID}": {"status": "queued"}}
    job_service = SimpleNamespace(find_active_job_by_dedupe_key=active.get)
    file_actions = ("download", "capacity_download", "capacity_image_send")

    def actions_for(target):
        return routes._attach_review_display_state(
            {"session": target, "latest_session_id": target["session_id"]},
            service=object(),
            building=target["building"],
            include_concurrency=False,
            include_shared_blocks=False,
            delivery_job_service=job_service,
        )["display_state"]["actions"]

    for status in ("queued", "waiting_resource", "running"):
        active[f"handover_review_regenerate:{SESSION_ID}"]["status"] = status
        actions = actions_for(session)
        assert all(not actions[name]["allowed"] and actions[name]["reason_code"] == "regenerating"
                   for name in file_actions)
    other_building = {**session, "building": "B楼", "session_id": f"B楼|{BATCH}"}
    other_shift = {**session, "duty_shift": "night", "session_id": "A楼|2026-11-09|night"}
    for target in (other_building, other_shift):
        actions = actions_for(target)
        assert all(actions[name]["allowed"] for name in file_actions)
    active.clear()
    actions = actions_for(session)
    assert all(actions[name]["allowed"] for name in file_actions)


# ---------------------------------------------------------------------------
# 交接班日志下载
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("handler", [routes.handover_review_download, routes.handover_review_capacity_download])
def test_download_entry_never_deletes_document_from_new_generation(monkeypatch, tmp_path, handler):
    from handover_log_module.service.review_document_state_service import ReviewDocumentStateService

    session = _base_session(tmp_path, revision=1)
    newer = {"revision": 2, "source_excel_path": str(tmp_path / "new_log.xlsx")}
    store = SimpleNamespace(get_document=Mock(return_value=newer), delete_document=Mock())
    document_state = ReviewDocumentStateService({})
    monkeypatch.setattr(document_state, "_store", lambda _building: store)
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (object(), None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: document_state)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)

    with pytest.raises(HTTPException) as exc:
        handler("a", _request(_container()), SESSION_ID)

    assert exc.value.status_code == 409
    assert "重新生成" in exc.value.detail
    store.delete_document.assert_not_called()


@pytest.mark.parametrize("regeneration_started", [False, True])
def test_log_download_without_delivery_serves_stable_copy(monkeypatch, tmp_path, regeneration_started):
    """未提交成品（无成品清单）也能下载：回落原始文件通道，从稳定副本返回。"""
    session = _base_session(tmp_path, revision=1, confirmed=False)
    service = SimpleNamespace(config={"paths": {"runtime_state_root": tmp_path}})
    document_state = SimpleNamespace(
        ensure_document_for_session=Mock(return_value={"revision": 1, "document": {}}),
        force_sync_session_dict=Mock(return_value={}),
        peek_document_state=Mock(return_value={"revision": 1, "source_excel_path": session["output_file"]}),
    )
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (service, None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: document_state)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a, **k: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)
    monkeypatch.setattr(routes, "_review_delivery_state", lambda *a, **k: {})
    snapshot_root = _mock_temp(monkeypatch, tmp_path)

    container = _container()
    if regeneration_started:
        container.job_service = SimpleNamespace(find_active_job_by_dedupe_key=Mock(
            side_effect=[None, {"status": "running"}],
        ))
        with pytest.raises(HTTPException, match="重新生成") as exc:
            routes.handover_review_download("a", _request(container), SESSION_ID)
        assert exc.value.status_code == 409
        routes.cleanup_runtime_temp_dir.assert_called_once_with(snapshot_root)
        return

    response = routes.handover_review_download("a", _request(container), SESSION_ID)

    document_state.force_sync_session_dict.assert_called_once()
    snapshot_path = Path(response.path)
    assert snapshot_path == snapshot_root / Path(session["output_file"]).name
    assert snapshot_path != Path(session["output_file"])
    _assert_valid_xlsx(snapshot_path)


def test_log_download_skips_stale_delivery_and_serves_raw(monkeypatch, tmp_path):
    """审核内容已修改（session/document revision 升到 2），旧成品 revision=1 被判 stale，
    不再当作最新版下载，回落补写下载返回 output_file，而不是旧成品 log。"""
    config = {"paths": {"runtime_state_root": tmp_path}}
    session = _base_session(tmp_path, revision=2, confirmed=True)
    # 旧成品：revision=1、document_revision=1，已就绪。
    state = HandoverDeliveryStateService(config)
    stale_artifacts = _artifacts(tmp_path, prefix="stale")
    state.begin({**session, "revision": 1, "document_revision": 1}, "g1")
    state.update(
        SESSION_ID, "g1", status="ready", artifacts_ready=True,
        revision=1, document_revision=1, paths=stale_artifacts, hashes={},
    )
    service = SimpleNamespace(config=config)
    document_state = SimpleNamespace(
        # 当前权威文档版本为 2，与成品 revision=1 不一致。
        ensure_document_for_session=Mock(return_value={"revision": 2, "document": {}}),
        force_sync_session_dict=Mock(return_value={}),
        peek_document_state=Mock(return_value={"revision": 2, "source_excel_path": session["output_file"]}),
    )
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (service, None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: document_state)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a, **k: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)
    snapshot_root = _mock_temp(monkeypatch, tmp_path)

    response = routes.handover_review_download("a", _request(_container()), SESSION_ID)

    assert Path(response.path) != Path(stale_artifacts["log"])
    assert Path(response.path) == snapshot_root / Path(session["output_file"]).name
    _assert_valid_xlsx(Path(response.path))


def test_log_download_prefers_current_delivery_artifact(monkeypatch, tmp_path):
    """成品对当前版本仍就绪时，优先下载成品 log（行为不变）。"""
    config = {"paths": {"runtime_state_root": tmp_path}}
    session = _base_session(tmp_path, revision=1, confirmed=True)
    state = HandoverDeliveryStateService(config)
    ready_artifacts = _artifacts(tmp_path, prefix="ready")
    state.begin({**session, "revision": 1, "document_revision": 1}, "g1")
    state.update(
        SESSION_ID, "g1", status="ready", artifacts_ready=True,
        revision=1, document_revision=1, paths=ready_artifacts, hashes={},
    )
    service = SimpleNamespace(config=config)
    document_state = SimpleNamespace(
        ensure_document_for_session=Mock(return_value={"revision": 1, "document": {}}),
        force_sync_session_dict=Mock(return_value={}),
        peek_document_state=Mock(return_value={"revision": 1, "source_excel_path": session["output_file"]}),
    )
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (service, None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: document_state)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a, **k: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)

    response = routes.handover_review_download("a", _request(_container()), SESSION_ID)

    assert Path(response.path) == Path(ready_artifacts["log"])
    document_state.force_sync_session_dict.assert_not_called()


# ---------------------------------------------------------------------------
# 容量报表下载
# ---------------------------------------------------------------------------

def _patch_capacity_route(
    monkeypatch,
    tmp_path,
    *,
    session,
    document_revision=1,
    job_result=None,
    refreshed_session=None,
    delivery_state=None,
    peek_side_effect=None,
    peek_state=None,
):
    service = SimpleNamespace(
        config={"paths": {"runtime_state_root": tmp_path}},
        get_session_by_id=Mock(return_value=refreshed_session),
    )
    default_peek = {"revision": document_revision, "source_excel_path": session.get("output_file", "")}
    peek_mock = (
        Mock(side_effect=peek_side_effect)
        if peek_side_effect is not None
        else Mock(return_value=peek_state if peek_state is not None else default_peek)
    )
    document_state = SimpleNamespace(
        load_document=Mock(return_value=({}, {**session, "revision": document_revision})),
        # 返回前校验必须只读：helper 只能用 peek_document_state；ensure_document_for_session 是
        # 哨兵（路径变化时它会删除并重新导入审核文档），测试会断言它从未被调用。
        peek_document_state=peek_mock,
        ensure_document_for_session=Mock(),
    )
    queue_service = SimpleNamespace(
        enqueue_capacity_overlay_sync=Mock(return_value={"job_id": "j1"}),
        wait_for_job=Mock(return_value=job_result or {"status": "success"}),
    )
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (service, None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: document_state)
    monkeypatch.setattr(routes, "_build_xlsx_write_queue_service", lambda *a, **k: queue_service)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a, **k: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)
    monkeypatch.setattr(routes, "_extract_capacity_tracked_cells", lambda _doc: dict(TRACKED_CELLS))
    monkeypatch.setattr(
        routes, "_review_delivery_state", lambda *a, **k: (delivery_state if delivery_state is not None else {}),
    )
    return service, document_state, queue_service


def test_capacity_download_overlay_failure_returns_409(monkeypatch, tmp_path):
    """补写失败（如缺必填项）时返回 409，不返回旧容量表。"""
    session = _base_session(tmp_path)
    _patch_capacity_route(
        monkeypatch, tmp_path, session=session,
        job_result={"status": "failed", "error": "容量报表缺少必填数据：J12；请在审核页补齐并保存后再提交"},
    )

    with pytest.raises(HTTPException) as exc:
        routes.handover_review_capacity_download("a", _request(_container()), SESSION_ID)

    assert exc.value.status_code == 409
    assert "缺少必填" in exc.value.detail


def test_capacity_download_signature_mismatch_returns_409(monkeypatch, tmp_path):
    """补写成功但结果输入签名与本次下载输入不一致时返回 409，避免返回与当前审核内容不对应的旧表。"""
    session = _base_session(tmp_path)
    refreshed = {
        **session,
        "capacity_sync": {"status": "ready", "input_signature": "MISMATCHED_SIGNATURE"},
    }
    _patch_capacity_route(
        monkeypatch, tmp_path, session=session,
        job_result={"status": "success"}, refreshed_session=refreshed,
    )

    with pytest.raises(HTTPException) as exc:
        routes.handover_review_capacity_download("a", _request(_container()), SESSION_ID)

    assert exc.value.status_code == 409
    assert "不一致" in exc.value.detail


def test_capacity_download_success_returns_stable_copy(monkeypatch, tmp_path):
    """补写成功且签名一致时，从稳定副本返回（路径不同于原始容量表），且副本为有效 xlsx。"""
    session = _base_session(tmp_path)
    capacity_file = _make_xlsx(tmp_path / "A_capacity_real.xlsx")
    session["capacity_output_file"] = str(capacity_file)
    expected_signature = HandoverCapacityReportService.capacity_input_signature(TRACKED_CELLS)
    refreshed = {
        **session,
        "capacity_sync": {"status": "ready", "input_signature": expected_signature},
    }
    _patch_capacity_route(
        monkeypatch, tmp_path, session=session,
        job_result={"status": "success"}, refreshed_session=refreshed,
    )
    snapshot_root = tmp_path / "snapshot_root"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(routes, "create_runtime_temp_dir", lambda kind: snapshot_root)
    monkeypatch.setattr(routes, "cleanup_runtime_temp_dir", Mock())

    response = routes.handover_review_capacity_download("a", _request(_container()), SESSION_ID)

    snapshot_path = Path(response.path)
    assert snapshot_path == snapshot_root / capacity_file.name
    assert snapshot_path != capacity_file
    assert snapshot_path.exists() and snapshot_path.stat().st_size > 0
    # 稳定副本必须是有效工作簿。
    from openpyxl import load_workbook

    load_workbook(snapshot_path, read_only=True).close()


def test_capacity_download_passes_expected_document_revision(monkeypatch, tmp_path):
    """补写下载必须把当前权威文档版本传给补写任务，用于版本校验。"""
    session = _base_session(tmp_path)
    session["capacity_output_file"] = str(_make_xlsx(tmp_path / "A_capacity_real.xlsx"))
    expected_signature = HandoverCapacityReportService.capacity_input_signature(TRACKED_CELLS)
    refreshed = {**session, "capacity_sync": {"status": "ready", "input_signature": expected_signature}}
    _service, _document_state, queue_service = _patch_capacity_route(
        monkeypatch, tmp_path, session=session, document_revision=7,
        job_result={"status": "success"}, refreshed_session=refreshed,
    )
    snapshot_root = tmp_path / "snapshot_root"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(routes, "create_runtime_temp_dir", lambda kind: snapshot_root)
    monkeypatch.setattr(routes, "cleanup_runtime_temp_dir", Mock())

    routes.handover_review_capacity_download("a", _request(_container()), SESSION_ID)

    kwargs = queue_service.enqueue_capacity_overlay_sync.call_args.kwargs
    assert kwargs.get("expected_document_revision") == 7


# ---------------------------------------------------------------------------
# TOCTOU：补写/同步完成到返回前若有人保存新版，拒绝旧下载
# ---------------------------------------------------------------------------

def test_log_download_rejects_when_revision_changed_before_return(monkeypatch, tmp_path):
    """日志：补写同步完成到返回前有人保存新版（审核版本 1→2），只读校验发现后拒绝返回旧文件并清理副本。"""
    session = _base_session(tmp_path, revision=1, confirmed=False)
    service = SimpleNamespace(config={"paths": {"runtime_state_root": tmp_path}})
    document_state = SimpleNamespace(
        ensure_document_for_session=Mock(return_value={"revision": 1, "document": {}}),
        force_sync_session_dict=Mock(return_value={}),
        # 副本前只读核对(1，未变) → 副本后只读核对(2，已变化)
        peek_document_state=Mock(side_effect=[
            {"revision": 1, "source_excel_path": session["output_file"]},
            {"revision": 2, "source_excel_path": session["output_file"]},
        ]),
    )
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (service, None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: document_state)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a, **k: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)
    monkeypatch.setattr(routes, "_review_delivery_state", lambda *a, **k: {})
    cleanup = Mock()
    snapshot_root = tmp_path / "snapshot_root"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(routes, "create_runtime_temp_dir", lambda kind: snapshot_root)
    monkeypatch.setattr(routes, "cleanup_runtime_temp_dir", cleanup)

    with pytest.raises(HTTPException) as exc:
        routes.handover_review_download("a", _request(_container()), SESSION_ID)

    assert exc.value.status_code == 409
    assert "下载期间已更新" in exc.value.detail
    cleanup.assert_called_once()


def test_log_download_rejects_when_source_path_switched(monkeypatch, tmp_path):
    """日志：下载期间重新生成切换了源文件路径，只读校验发现 source_excel_path 不一致 → 拒绝，
    且返回前校验只走 peek（只读），不会再触发 ensure_document_for_session 的删除/重导入。"""
    session = _base_session(tmp_path, revision=1, confirmed=False)
    service = SimpleNamespace(config={"paths": {"runtime_state_root": tmp_path}})
    document_state = SimpleNamespace(
        ensure_document_for_session=Mock(return_value={"revision": 1, "document": {}}),
        force_sync_session_dict=Mock(return_value={}),
        peek_document_state=Mock(return_value={"revision": 1, "source_excel_path": "D:/新路径/A_log_v2.xlsx"}),
    )
    monkeypatch.setattr(routes, "_build_review_services", lambda _c: (service, None, None, None))
    monkeypatch.setattr(routes, "_build_review_document_state_service", lambda *a, **k: document_state)
    monkeypatch.setattr(routes, "_resolve_building_or_404", lambda *a, **k: "A楼")
    monkeypatch.setattr(routes, "_load_target_session_or_404", lambda *a, **k: session)
    monkeypatch.setattr(routes, "_review_delivery_state", lambda *a, **k: {})
    snapshot_root = tmp_path / "snapshot_root"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(routes, "create_runtime_temp_dir", lambda kind: snapshot_root)
    monkeypatch.setattr(routes, "cleanup_runtime_temp_dir", Mock())

    with pytest.raises(HTTPException) as exc:
        routes.handover_review_download("a", _request(_container()), SESSION_ID)

    assert exc.value.status_code == 409
    assert "下载期间已更新" in exc.value.detail
    # 顶部初始化调用一次 ensure 属既有行为；返回前校验只用 peek，不再额外调用 ensure（避免删除/重导入）。
    assert document_state.ensure_document_for_session.call_count == 1
    document_state.peek_document_state.assert_called()
    assert not (snapshot_root / Path(session["output_file"]).name).exists()


def test_capacity_download_rejects_when_revision_changed_before_return(monkeypatch, tmp_path):
    """容量：补写完成到返回前有人保存新版（审核版本 7→8），只读校验发现后拒绝返回旧容量表并清理副本。"""
    session = _base_session(tmp_path)
    expected_signature = HandoverCapacityReportService.capacity_input_signature(TRACKED_CELLS)
    refreshed = {**session, "capacity_sync": {"status": "ready", "input_signature": expected_signature}}
    _patch_capacity_route(
        monkeypatch, tmp_path, session=session, document_revision=7,
        job_result={"status": "success"}, refreshed_session=refreshed,
        # 副本前只读核对(7，未变) → 副本后只读核对(8，已变化)
        peek_side_effect=[
            {"revision": 7, "source_excel_path": session["output_file"]},
            {"revision": 8, "source_excel_path": session["output_file"]},
        ],
    )
    cleanup = Mock()
    snapshot_root = tmp_path / "snapshot_root"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(routes, "create_runtime_temp_dir", lambda kind: snapshot_root)
    monkeypatch.setattr(routes, "cleanup_runtime_temp_dir", cleanup)

    with pytest.raises(HTTPException) as exc:
        routes.handover_review_capacity_download("a", _request(_container()), SESSION_ID)

    assert exc.value.status_code == 409
    assert "下载期间已更新" in exc.value.detail
    cleanup.assert_called_once()


def test_capacity_download_rejects_when_revision_changed_before_copy(monkeypatch, tmp_path):
    """容量：返回前首次只读核对就发现审核版本已变（7→8），在拷贝前即拒绝，不产生副本。"""
    session = _base_session(tmp_path)
    expected_signature = HandoverCapacityReportService.capacity_input_signature(TRACKED_CELLS)
    refreshed = {**session, "capacity_sync": {"status": "ready", "input_signature": expected_signature}}
    _patch_capacity_route(
        monkeypatch, tmp_path, session=session, document_revision=7,
        job_result={"status": "success"}, refreshed_session=refreshed,
        peek_side_effect=[{"revision": 8, "source_excel_path": session["output_file"]}],
    )
    snapshot_root = tmp_path / "snapshot_root"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(routes, "create_runtime_temp_dir", lambda kind: snapshot_root)
    monkeypatch.setattr(routes, "cleanup_runtime_temp_dir", Mock())

    with pytest.raises(HTTPException) as exc:
        routes.handover_review_capacity_download("a", _request(_container()), SESSION_ID)

    assert exc.value.status_code == 409
    assert "下载期间已更新" in exc.value.detail
    assert not (snapshot_root / Path(session["capacity_output_file"]).name).exists()


def test_capacity_download_rejects_when_source_path_switched(monkeypatch, tmp_path):
    """容量：下载期间重新生成切换了源文件路径（revision 仍为 7 但 source_excel_path 变了），
    只读校验发现路径不一致 → 拒绝；且校验只走 peek，绝不调用 ensure_document_for_session（那会删除新版审核文档）。"""
    session = _base_session(tmp_path)
    expected_signature = HandoverCapacityReportService.capacity_input_signature(TRACKED_CELLS)
    refreshed = {**session, "capacity_sync": {"status": "ready", "input_signature": expected_signature}}
    _service, document_state, _queue = _patch_capacity_route(
        monkeypatch, tmp_path, session=session, document_revision=7,
        job_result={"status": "success"}, refreshed_session=refreshed,
        # revision 一致但源文件路径被重新生成切换 → 只读校验应拒绝
        peek_state={"revision": 7, "source_excel_path": "D:/新路径/A_log_v2.xlsx"},
    )
    snapshot_root = tmp_path / "snapshot_root"
    snapshot_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(routes, "create_runtime_temp_dir", lambda kind: snapshot_root)
    monkeypatch.setattr(routes, "cleanup_runtime_temp_dir", Mock())

    with pytest.raises(HTTPException) as exc:
        routes.handover_review_capacity_download("a", _request(_container()), SESSION_ID)

    assert exc.value.status_code == 409
    assert "下载期间已更新" in exc.value.detail
    # 关键：返回前校验只读，绝不触发 ensure_document_for_session（删除/重导入审核文档）。
    document_state.ensure_document_for_session.assert_not_called()
    document_state.peek_document_state.assert_called()
    assert not (snapshot_root / Path(session["capacity_output_file"]).name).exists()


# ---------------------------------------------------------------------------
# 发送仍要求成品
# ---------------------------------------------------------------------------

def test_require_review_delivery_blocks_when_not_ready(tmp_path):
    """发送审核文本/容量表图片依赖的成品门禁：未就绪时 409。"""
    config = {"paths": {"runtime_state_root": tmp_path}}
    service = SimpleNamespace(config=config)
    session = _base_session(tmp_path, revision=1, confirmed=False)

    with pytest.raises(HTTPException) as exc:
        routes._require_review_delivery(service, session)

    assert exc.value.status_code == 409
    assert "成品尚未就绪" in exc.value.detail


def test_require_review_delivery_allows_current_ready(tmp_path):
    """成品对当前版本就绪时放行（发送可用）。"""
    config = {"paths": {"runtime_state_root": tmp_path}}
    session = _base_session(tmp_path, revision=1, confirmed=True)
    state = HandoverDeliveryStateService(config)
    ready_artifacts = _artifacts(tmp_path, prefix="send_ready")
    state.begin({**session, "revision": 1, "document_revision": 1}, "g1")
    state.update(
        SESSION_ID, "g1", status="ready", artifacts_ready=True,
        revision=1, document_revision=1, paths=ready_artifacts, hashes={},
    )
    service = SimpleNamespace(config=config)

    manifest = routes._require_review_delivery(service, {**session, "document_revision": 1})

    assert manifest.get("artifacts_ready") is True
    assert manifest.get("paths", {}).get("image") == ready_artifacts["image"]


def test_require_review_delivery_blocks_stale(tmp_path):
    """审核内容已修改导致成品 stale 时，发送门禁仍拦截。"""
    config = {"paths": {"runtime_state_root": tmp_path}}
    session = _base_session(tmp_path, revision=2, confirmed=True)
    state = HandoverDeliveryStateService(config)
    stale_artifacts = _artifacts(tmp_path, prefix="send_stale")
    state.begin({**session, "revision": 1, "document_revision": 1}, "g1")
    state.update(
        SESSION_ID, "g1", status="ready", artifacts_ready=True,
        revision=1, document_revision=1, paths=stale_artifacts, hashes={},
    )
    service = SimpleNamespace(config=config)

    with pytest.raises(HTTPException) as exc:
        routes._require_review_delivery(service, {**session, "document_revision": 2})

    assert exc.value.status_code == 409
