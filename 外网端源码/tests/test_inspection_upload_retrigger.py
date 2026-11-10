"""补派闭环回归：标记持久、成功回查后 ack、回执只记实际版本、关停与节流。"""
from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from app.bootstrap.container import AppContainer, InspectionRetriggerWatchdog
from app.modules.inspection_data.service import point_settings
from app.modules.inspection_data.service.point_settings import (
    ack_inspection_upload_retrigger,
    consume_inspection_upload_retrigger,
    read_inspection_upload_retrigger,
    request_inspection_upload_retrigger,
)
from app.worker import task_handlers
from app.worker.task_handlers import HANDLER_REGISTRY


def _config(tmp_path, **extra):
    return {"paths": {"runtime_state_root": str(tmp_path)},
            "inspection_data_upload": {"enabled": True, **extra}}


def _save(config, tag):
    point_settings._save_settings(
        config, {"D楼": {"electrical_inspection_family": {"points": {tag: "备注"}, "remove": []}}})
    return point_settings.current_point_settings_revision(config)


def _fake_service(monkeypatch, results):
    import app.modules.inspection_data.service.inspection_data_upload_service as upload_module

    calls = []

    class FakeService:
        def __init__(self, cfg):
            self.cfg = cfg

        def run(self, **kwargs):
            calls.append(len(calls) + 1)
            item = results[min(len(calls) - 1, len(results) - 1)]
            if isinstance(item, Exception):
                raise item
            return item

    monkeypatch.setattr(upload_module, "InspectionDataUploadService", FakeService)
    return calls


class _Double:
    """JobService 契约双：dedupe 精确匹配、active 含排队、资源键由调用方串行驱动。"""

    def __init__(self):
        self.jobs = []
        self.submissions = []
        self.fail_submit = False

    def start_worker_job(self, *, name, worker_handler, worker_payload, resource_keys=None,
                         priority="", feature="", dedupe_key="", submitted_by=""):
        if self.fail_submit:
            raise RuntimeError("模拟任务引擎关闭")
        existing = self.find_active_job_by_dedupe_key(dedupe_key)
        if existing:
            return SimpleNamespace(job_id=existing["job_id"])
        job = SimpleNamespace(job_id=f"j{len(self.jobs)}", payload=dict(worker_payload),
                              dedupe_key=dedupe_key, feature=feature,
                              resource_keys=list(resource_keys or []), status="submitted")
        self.jobs.append(job)
        self.submissions.append(dedupe_key)
        return job

    def get_job_state(self, job_id):
        return next((job for job in self.jobs if job.job_id == job_id), None)

    def find_active_job_by_dedupe_key(self, key):
        job = next((item for item in self.jobs
                    if item.dedupe_key == key and item.status in {"submitted", "running"}), None)
        return {"job_id": job.job_id} if job else None

    def has_active_jobs_for_feature_prefixes(self, prefixes):
        return any(job.status in {"submitted", "running"}
                   and any(str(job.feature).startswith(p) for p in prefixes) for job in self.jobs)


def _fake_container(config, double, watchdog=None):
    fake = SimpleNamespace(
        runtime_config=config,
        job_service=double,
        inspection_retrigger_watchdog=watchdog,
        inspection_retrigger_shutdown=False,
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        add_system_log=lambda message: None,
    )
    fake._job_busy_for_feature_prefixes = lambda prefix: AppContainer._job_busy_for_feature_prefixes(fake, prefix)
    fake._submit_inspection_data_upload = lambda slot: AppContainer._submit_inspection_data_upload(fake, slot)
    fake._submit_inspection_retrigger = lambda: AppContainer._submit_inspection_retrigger(fake)
    return fake


def test_marker_read_ack_and_expiry(tmp_path):
    config = _config(tmp_path)
    assert read_inspection_upload_retrigger(config) is None
    rev = _save(config, "a")
    request_inspection_upload_retrigger(config, reason="测试")
    marker = read_inspection_upload_retrigger(config)
    assert marker and marker["revision"] == rev
    assert ack_inspection_upload_retrigger(config, rev - 1) is False   # 更高版本不被旧覆盖确认
    assert read_inspection_upload_retrigger(config) is not None
    assert ack_inspection_upload_retrigger(config, rev) is True
    assert read_inspection_upload_retrigger(config) is None
    request_inspection_upload_retrigger(config)
    path = tmp_path / "inspection_upload_retrigger_pending.json"
    path.write_text('{"requested_at": 1.0, "revision": 1}', encoding="utf-8")
    assert read_inspection_upload_retrigger(config) is None            # 过期不接手
    assert consume_inspection_upload_retrigger(config) is False


def test_ack_never_deletes_newer_marker(tmp_path):
    config = _config(tmp_path)
    rev1 = _save(config, "v1")
    request_inspection_upload_retrigger(config, reason="保存1")
    rev2 = _save(config, "v2")
    request_inspection_upload_retrigger(config, reason="保存2")
    assert ack_inspection_upload_retrigger(config, rev1) is False
    marker = read_inspection_upload_retrigger(config)
    assert marker and marker["revision"] == rev2                       # 新保存不被旧任务清掉
    assert ack_inspection_upload_retrigger(config, rev2) is True
    assert read_inspection_upload_retrigger(config) is None


def test_request_and_ack_serialized_by_cross_process_lock(tmp_path):
    config = _config(tmp_path)
    _save(config, "v1")
    request_inspection_upload_retrigger(config)
    stop = threading.Event()

    def acker():
        while not stop.is_set():
            ack_inspection_upload_retrigger(config, 0)                 # 永远未覆盖 → 不得删除

    def requester():
        for _ in range(50):
            request_inspection_upload_retrigger(config)

    ack_threads = [threading.Thread(target=acker) for _ in range(2)]
    req_thread = threading.Thread(target=requester)
    for thread in ack_threads:
        thread.start()
    req_thread.start()
    req_thread.join(10)
    stop.set()
    for thread in ack_threads:
        thread.join(5)
    assert read_inspection_upload_retrigger(config) is not None        # 并发下标记不丢


def test_transient_failure_preserves_marker_and_receipt(tmp_path, monkeypatch):
    config = _config(tmp_path)
    rev1 = _save(config, "v1")
    point_settings.record_inspection_upload_revision(config, rev1)
    rev2 = _save(config, "v2")
    request_inspection_upload_retrigger(config, reason="保存2")
    _fake_service(monkeypatch, [RuntimeError("模拟源数据临时失败")])
    with pytest.raises(RuntimeError):
        HANDLER_REGISTRY["inspection_data_upload"](
            config, {"source_date": "2026-11-10", "source_bucket_key": "2026-11-10 10",
                     "dry_run": False, "retrigger": True},
            lambda line: None, None)
    assert read_inspection_upload_retrigger(config)["revision"] == rev2
    assert point_settings.last_inspection_upload_revision(config) == rev1


def test_higher_save_not_cleared_by_older_covered_run(tmp_path, monkeypatch):
    config = _config(tmp_path)
    rev1 = _save(config, "v1")
    rev2 = _save(config, "v2")
    request_inspection_upload_retrigger(config, reason="保存2")
    calls = _fake_service(monkeypatch, [
        {"status": "success", "settings_revision": rev1},
        {"status": "success", "settings_revision": rev2},
    ])
    result = HANDLER_REGISTRY["inspection_data_upload"](
        config, {"source_date": "2026-11-10", "source_bucket_key": "2026-11-10 10",
                 "dry_run": False, "retrigger": True},
        lambda line: None, None)
    assert result["status"] == "success"
    assert calls == [1, 2]
    assert read_inspection_upload_retrigger(config) is None
    assert point_settings.last_inspection_upload_revision(config) == rev2


def test_disabled_skipped_writes_no_receipt(tmp_path, monkeypatch):
    config = _config(tmp_path, enabled=False)
    _save(config, "v1")
    _fake_service(monkeypatch, [{"status": "skipped", "reason": "disabled"}])
    result = HANDLER_REGISTRY["inspection_data_upload"](
        config, {"source_date": "2026-11-10", "source_bucket_key": "2026-11-10 10", "dry_run": False},
        lambda line: None, None)
    assert result["status"] == "skipped"
    assert point_settings.last_inspection_upload_revision(config) is None


def test_missing_authoritative_revision_writes_no_receipt(tmp_path, monkeypatch):
    config = _config(tmp_path)
    rev1 = _save(config, "v1")
    request_inspection_upload_retrigger(config, reason="保存1")
    calls = _fake_service(monkeypatch, [{"status": "success"}])          # 旧契约：无权威版本
    result = HANDLER_REGISTRY["inspection_data_upload"](
        config, {"source_date": "2026-11-10", "source_bucket_key": "2026-11-10 10",
                 "dry_run": False, "retrigger": True},
        lambda line: None, None)
    assert result["status"] == "success"
    assert point_settings.last_inspection_upload_revision(config) is None
    assert read_inspection_upload_retrigger(config)["revision"] == rev1  # 未确认不清标记
    assert len(calls) == 4                                               # 有界接续后留给巡检


def test_retrigger_skips_when_marker_already_covered(tmp_path, monkeypatch):
    config = _config(tmp_path)
    rev1 = _save(config, "v1")
    point_settings.record_inspection_upload_revision(config, rev1)
    request_inspection_upload_retrigger(config, reason="旧请求")
    calls = _fake_service(monkeypatch, [{"status": "success", "settings_revision": rev1}])
    result = HANDLER_REGISTRY["inspection_data_upload"](
        config, {"source_date": "2026-11-10", "source_bucket_key": "2026-11-10 10",
                 "dry_run": False, "retrigger": True},
        lambda line: None, None)
    assert result["status"] == "skipped"
    assert calls == []
    assert read_inspection_upload_retrigger(config) is None


def test_retrigger_skips_without_marker_when_receipt_current(tmp_path, monkeypatch):
    config = _config(tmp_path)
    rev1 = _save(config, "v1")
    point_settings.record_inspection_upload_revision(config, rev1)
    calls = _fake_service(monkeypatch, [{"status": "success", "settings_revision": rev1}])
    result = HANDLER_REGISTRY["inspection_data_upload"](
        config, {"source_date": "2026-11-10", "source_bucket_key": "2026-11-10 10",
                 "dry_run": False, "retrigger": True},
        lambda line: None, None)
    assert result["status"] == "skipped"
    assert calls == []


def test_load_point_settings_with_revision_returns_same_snapshot_pair(tmp_path, monkeypatch):
    config = _config(tmp_path)
    calls = {"n": 0}

    def fake_snapshot(cfg):
        calls["n"] += 1
        if calls["n"] == 1:
            return None, 0
        if calls["n"] == 2:
            # 恢复落盘后、本次快照前，第 2 版保存已落地：内容与修订号必须同源于这份快照
            return {"D楼": {"electrical_inspection_family": {"points": {"v1": "", "v2": ""}, "remove": []}}}, 22
        raise AssertionError("恢复后只应再取一次快照")

    monkeypatch.setattr(point_settings, "_settings_snapshot", fake_snapshot)
    monkeypatch.setattr(
        point_settings, "load_or_restore_point_settings",
        lambda cfg: {"D楼": {"electrical_inspection_family": {"points": {"v1": ""}, "remove": []}}})
    settings, revision = point_settings.load_point_settings_with_revision(config)
    assert revision == 22
    assert "v2" in settings["D楼"]["electrical_inspection_family"]["points"]


def test_load_point_settings_with_revision_no_restore_when_present(tmp_path, monkeypatch):
    config = _config(tmp_path)
    rev = _save(config, "v1")
    restored = {"called": False}

    def fake_restore(cfg):
        restored["called"] = True
        return {}

    monkeypatch.setattr(point_settings, "load_or_restore_point_settings", fake_restore)
    settings, revision = point_settings.load_point_settings_with_revision(config)
    assert restored["called"] is False
    assert revision == rev and settings


def test_trigger_idle_registers_marker_and_watchdog_takes_over_after_transient_failure(tmp_path, monkeypatch):
    config = _config(tmp_path, retrigger_min_interval_sec=0)
    double = _Double()
    fake = _fake_container(config, double)
    rev31 = _save(config, "v31")
    point_settings.record_inspection_upload_revision(config, rev31 - 1)
    assert AppContainer.trigger_inspection_data_upload_now(fake)["ok"] is True
    assert read_inspection_upload_retrigger(config)["revision"] == rev31   # 空闲也先持久登记
    calls = _fake_service(monkeypatch, [
        RuntimeError("模拟首次读源临时失败"),
        {"status": "success", "settings_revision": rev31},
    ])
    job = double.jobs[-1]
    with pytest.raises(RuntimeError):
        task_handlers.handle_inspection_data_upload(config, job.payload, lambda message: None, None)
    double.jobs[-1].status = "failed"
    assert read_inspection_upload_retrigger(config)["revision"] == rev31   # 失败不丢待传
    assert AppContainer.dispatch_pending_inspection_retrigger(fake)["ok"] is True
    job2 = double.jobs[-1]
    result = task_handlers.handle_inspection_data_upload(config, job2.payload, lambda message: None, None)
    assert result["status"] == "success"
    assert calls == [1, 2]
    assert read_inspection_upload_retrigger(config) is None
    assert point_settings.last_inspection_upload_revision(config) == rev31


def test_trigger_queues_retrigger_when_busy(tmp_path):
    submitted = []
    fake = SimpleNamespace(
        runtime_config=_config(tmp_path),
        inspection_retrigger_watchdog=None,
        inspection_retrigger_shutdown=False,
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        job_service=SimpleNamespace(
            find_active_job_by_dedupe_key=lambda key: {"job_id": "busy"} if key == "inspection_data_upload" else None),
        _job_busy_for_feature_prefixes=lambda prefix: (lambda: True),
        add_system_log=lambda message: None,
        _submit_inspection_data_upload=lambda slot: (_ for _ in ()).throw(AssertionError("忙时不应直接派发")),
        _submit_inspection_retrigger=lambda: submitted.append("retrigger"),
    )
    result = AppContainer.trigger_inspection_data_upload_now(fake)
    assert result["ok"] is False
    assert submitted == ["retrigger"]
    assert consume_inspection_upload_retrigger(_config(tmp_path)) is True


def test_trigger_busy_with_waiting_retrigger_does_not_stack(tmp_path):
    fake = SimpleNamespace(
        runtime_config=_config(tmp_path),
        inspection_retrigger_watchdog=None,
        inspection_retrigger_shutdown=False,
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        job_service=SimpleNamespace(find_active_job_by_dedupe_key=lambda key: {"job_id": "x"}),
        _job_busy_for_feature_prefixes=lambda prefix: (lambda: True),
        add_system_log=lambda message: None,
        _submit_inspection_data_upload=lambda slot: (_ for _ in ()).throw(AssertionError("忙时不应直接派发")),
        _submit_inspection_retrigger=lambda: (_ for _ in ()).throw(AssertionError("已有排队补派不应叠加")),
    )
    result = AppContainer.trigger_inspection_data_upload_now(fake)
    assert result["ok"] is False
    assert consume_inspection_upload_retrigger(_config(tmp_path)) is True


def test_trigger_submits_when_idle(tmp_path):
    submitted = []
    config = _config(tmp_path)
    fake = SimpleNamespace(
        runtime_config=config,
        inspection_retrigger_watchdog=None,
        inspection_retrigger_shutdown=False,
        _configured_deployment_snapshot=lambda: {"role_mode": "external"},
        job_service=SimpleNamespace(find_active_job_by_dedupe_key=lambda key: None),
        _job_busy_for_feature_prefixes=lambda prefix: (lambda: False),
        add_system_log=lambda message: None,
        _submit_inspection_data_upload=submitted.append,
    )
    result = AppContainer.trigger_inspection_data_upload_now(fake)
    assert result["ok"] is True
    assert len(submitted) == 1
    assert submitted[0][:10] == time.strftime("%Y-%m-%d")


def test_dispatch_throttles_retry_after_failed_attempt(tmp_path):
    config = _config(tmp_path)                       # 默认冷却 60 秒
    double = _Double()
    fake = _fake_container(config, double)
    _save(config, "v1")
    request_inspection_upload_retrigger(config, reason="测试")
    assert AppContainer.dispatch_pending_inspection_retrigger(fake)["ok"] is True
    double.jobs[-1].status = "failed"
    second = AppContainer.dispatch_pending_inspection_retrigger(fake)
    assert second["ok"] is False and "冷却" in second["skipped"]
    assert read_inspection_upload_retrigger(config) is not None          # 节流不清标记


def test_dispatch_survives_engine_shutdown(tmp_path):
    config = _config(tmp_path, retrigger_min_interval_sec=0)
    double = _Double()
    double.fail_submit = True
    fake = _fake_container(config, double)
    _save(config, "v1")
    request_inspection_upload_retrigger(config, reason="测试")
    result = AppContainer.dispatch_pending_inspection_retrigger(fake)
    assert result["ok"] is False and "error" in result
    assert read_inspection_upload_retrigger(config) is not None          # 派发失败不丢待传


def test_dispatch_stops_after_watchdog_shutdown(tmp_path):
    config = _config(tmp_path)
    double = _Double()
    watchdog = InspectionRetriggerWatchdog(dispatch=lambda: {"ok": True}, interval_sec=1)
    watchdog.start()
    watchdog.stop()
    watchdog.stop()                                                      # 重复停止安全
    fake = _fake_container(config, double, watchdog=watchdog)
    _save(config, "v1")
    request_inspection_upload_retrigger(config, reason="测试")
    result = AppContainer.dispatch_pending_inspection_retrigger(fake)
    assert result["ok"] is False and "停止" in result["skipped"]
    assert double.submissions == []


def test_stop_waits_for_inflight_dispatch_and_blocks_restart(tmp_path):
    config = _config(tmp_path)
    started = threading.Event()

    def slow_dispatch():
        started.set()
        threading.Event().wait(3.3)                                      # 模拟关停期间被阻塞的派发
        return {"ok": True}

    watchdog = InspectionRetriggerWatchdog(dispatch=slow_dispatch, interval_sec=1)
    watchdog.start()
    assert started.wait(5)
    fake = SimpleNamespace(runtime_config=config, inspection_retrigger_watchdog=watchdog,
                           inspection_retrigger_shutdown=False, _inspection_retrigger_lock=threading.Lock(),
                           add_system_log=lambda message: None)
    holder = {}

    def stopper():
        holder["result"] = AppContainer.stop_inspection_retrigger_watchdog(fake, source="关闭", closing=True)

    thread = threading.Thread(target=stopper)
    thread.start()
    thread.join(10)
    assert holder["result"]["running"] is False                          # 等慢派发结束才返回
    assert watchdog._thread is None                                      # 线程引用退出后才清除
    again = AppContainer.stop_inspection_retrigger_watchdog(fake, source="重复")
    assert again["ok"] is True
    restart = AppContainer.start_inspection_retrigger_watchdog(fake, source="迟到启动")
    assert restart["ok"] is False and restart["reason"] == "shutdown_started"
    assert watchdog._thread is None

def test_watchdog_can_restart_after_role_exit_but_cannot_start_after_web_close():
    watchdog = InspectionRetriggerWatchdog(dispatch=lambda: {"ok": True})
    fake = SimpleNamespace(
        inspection_retrigger_watchdog=None,
        inspection_retrigger_shutdown=False,
        _inspection_retrigger_lock=threading.Lock(),
        _build_inspection_retrigger_watchdog=lambda: watchdog,
        add_system_log=lambda message: None,
    )
    assert AppContainer.start_inspection_retrigger_watchdog(fake)["running"]
    try:
        assert AppContainer.stop_inspection_retrigger_watchdog(fake)["stopped"]
        assert AppContainer.start_inspection_retrigger_watchdog(fake)["running"]
    finally:
        AppContainer.stop_inspection_retrigger_watchdog(fake)

    fake.inspection_retrigger_watchdog = None
    result = AppContainer.stop_inspection_retrigger_watchdog(fake, closing=True)
    assert result["reason"] == "not_initialized"
    assert AppContainer.start_inspection_retrigger_watchdog(fake)["reason"] == "shutdown_started"
    assert fake.inspection_retrigger_watchdog is None
