"""补派闭环的确定性时序回归：记账对应实际上传版本 + 上限退出后必有接班者。

使用真实 task_handlers / AppContainer 原始 trigger、_submit、dispatch 方法与真实
point_settings 标记/修订号文件；队列用契约一致的双（dedupe 精确匹配、active 含
waiting、资源键串行）驱动，上传服务替换为门控隔离模拟，精确复现两种并发时序。
"""
from __future__ import annotations

import threading
import uuid
from types import SimpleNamespace

import pytest

from app.bootstrap.container import AppContainer, InspectionRetriggerWatchdog
from app.modules.inspection_data.service import point_settings
from app.worker import task_handlers

ACTIVE = {"submitted", "running"}


class QueueDouble:
    """实现 container 所依赖的 JobService 契约：dedupe 精确匹配、active 含排队、资源键串行。"""

    def __init__(self):
        self.jobs = []
        self.submissions = []

    def start_worker_job(self, *, name, worker_handler, worker_payload, resource_keys=None,
                         priority="", feature="", dedupe_key="", submitted_by=""):
        existing = self.find_active_job_by_dedupe_key(dedupe_key)
        if existing:
            return SimpleNamespace(job_id=existing["job_id"])
        job = SimpleNamespace(job_id=uuid.uuid4().hex, name=name, payload=dict(worker_payload),
                              dedupe_key=dedupe_key, feature=feature,
                              resource_keys=list(resource_keys or []), status="submitted")
        self.jobs.append(job)
        self.submissions.append((dedupe_key, job.job_id))
        return job

    def get_job_state(self, job_id):
        return next((job for job in self.jobs if job.job_id == job_id), None)

    def find_active_job_by_dedupe_key(self, key):
        job = next((item for item in self.jobs
                    if item.dedupe_key == key and item.status in ACTIVE), None)
        return {"job_id": job.job_id, "status": job.status} if job else None

    def has_active_jobs_for_feature_prefixes(self, prefixes):
        return any(job.status in ACTIVE and any(str(job.feature).startswith(p) for p in prefixes)
                   for job in self.jobs)

    def active_count(self):
        return sum(1 for job in self.jobs if job.status in ACTIVE)

    def pop_runnable(self):
        running = [job for job in self.jobs if job.status == "running"]
        for job in self.jobs:
            if job.status != "submitted":
                continue
            if any(set(job.resource_keys) & set(other.resource_keys) for other in running):
                continue
            return job
        return None

    def mark_running(self, job):
        job.status = "running"

    def mark_terminal(self, job, status="success"):
        job.status = status


class Scenario:
    def __init__(self, tmp_path, monkeypatch):
        import app.modules.inspection_data.service.inspection_data_upload_service as upload_module

        self.config = {"paths": {"runtime_state_root": str(tmp_path)},
                       "inspection_data_upload": {"enabled": True, "retrigger_min_interval_sec": 0}}
        self.double = QueueDouble()
        self.actual = []
        self.runs = []
        self.upload_module = upload_module
        monkeypatch.setattr(upload_module, "InspectionDataUploadService", self._service_factory)

    def _service_factory(self, config):
        index = len(self.runs)
        started, release = threading.Event(), threading.Event()
        self.runs.append({"started": started, "release": release})

        class GatedService:
            def __init__(self, cfg):
                self.cfg = cfg

            def run(inner, **kwargs):  # noqa: N805
                revision = point_settings.current_point_settings_revision(self.config)
                self.actual.append(revision)
                started.set()
                assert inner_release(index)
                return {"status": "success", "settings_revision": revision,
                        "uploaded_fixture_revision": revision}

        def inner_release(run_index):
            return self.runs[run_index]["release"].wait(timeout=10)

        return GatedService(config)

    def save(self, version):
        point_settings._save_settings(
            self.config, {"D楼": {"electrical_inspection_family": {
                "points": {f"v{version}": "备注"}, "remove": []}}})
        return point_settings.current_point_settings_revision(self.config)

    def fake_container(self):
        fake = SimpleNamespace(
            runtime_config=self.config,
            job_service=self.double,
            inspection_retrigger_watchdog=None,
            inspection_retrigger_shutdown=False,
            _configured_deployment_snapshot=lambda: {"role_mode": "external"},
            add_system_log=lambda message: None,
        )
        fake._job_busy_for_feature_prefixes = lambda prefix: AppContainer._job_busy_for_feature_prefixes(fake, prefix)
        fake._submit_inspection_data_upload = lambda slot: AppContainer._submit_inspection_data_upload(fake, slot)
        fake._submit_inspection_retrigger = lambda: AppContainer._submit_inspection_retrigger(fake)
        return fake

    def trigger(self, fake):
        return AppContainer.trigger_inspection_data_upload_now(fake)

    def begin_job(self, logs):
        job = self.double.pop_runnable()
        assert job is not None, "没有可运行的任务"
        self.double.mark_running(job)
        holder = {}

        def worker():
            try:
                holder["result"] = task_handlers.handle_inspection_data_upload(
                    self.config, job.payload, logs.append, None)
            except Exception as exc:  # 交给断言暴露
                holder["error"] = exc
            finally:
                self.double.mark_terminal(job)

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        return job, thread, holder

    def await_run(self, index):
        for _ in range(200):
            if len(self.runs) > index:
                break
            threading.Event().wait(0.05)
        assert len(self.runs) > index, f"第{index + 1}次上传未创建"
        assert self.runs[index]["started"].wait(timeout=10), f"第{index + 1}次上传未启动"

    def release(self, index):
        self.runs[index]["release"].set()

    def receipt(self):
        return point_settings.last_inspection_upload_revision(self.config)

    def pending(self):
        return point_settings.peek_inspection_upload_retrigger(self.config)


def _versions(scenario):
    revisions = {revision: index + 1 for index, revision in enumerate(sorted(set(scenario.actual)))}
    return [revisions[revision] for revision in scenario.actual]


def test_receipt_matches_actual_upload_and_queued_retrigger_covers_last_save(tmp_path, monkeypatch):
    scenario = Scenario(tmp_path, monkeypatch)
    fake = scenario.fake_container()
    logs = []
    revisions = {}
    revisions[1] = scenario.save(1)
    assert scenario.trigger(fake)["ok"] is True
    job0, thread0, holder0 = scenario.begin_job(logs)
    scenario.await_run(0)
    revisions[2] = scenario.save(2)
    assert scenario.trigger(fake)["ok"] is False          # 忙时：标记 + 排队补派
    assert scenario.double.submissions[-1][0] == "inspection_data_upload:retrigger"
    scenario.release(0)
    scenario.await_run(1)
    revisions[3] = scenario.save(3)
    scenario.trigger(fake)
    scenario.release(1)
    scenario.await_run(2)
    revisions[4] = scenario.save(4)
    scenario.trigger(fake)
    scenario.release(2)
    scenario.await_run(3)
    revisions[5] = scenario.save(5)                        # 第 4 次上传完成前保存 5
    scenario.trigger(fake)
    retrigger_submissions = [sid for key, sid in scenario.double.submissions
                             if key == "inspection_data_upload:retrigger"]
    assert len(retrigger_submissions) == 1                 # 多次保存不叠加补派
    scenario.release(3)
    thread0.join(10)
    assert "error" not in holder0, holder0.get("error")
    job_r, thread_r, holder_r = scenario.begin_job(logs)   # 排队补派接手版本 5
    scenario.await_run(4)
    scenario.release(4)
    thread_r.join(10)
    assert "error" not in holder_r, holder_r.get("error")
    assert _versions(scenario) == [1, 2, 3, 4, 5]
    assert scenario.receipt() == revisions[5]
    assert scenario.pending() is False
    assert scenario.double.active_count() == 0


def test_watchdog_takes_over_after_retrigger_cap_exit(tmp_path, monkeypatch):
    scenario = Scenario(tmp_path, monkeypatch)
    fake = scenario.fake_container()
    logs = []
    revisions = {version: scenario.save(version) for version in (1,)}
    scenario.trigger(fake)
    job0, thread0, holder0 = scenario.begin_job(logs)
    plan = [
        (0, 2), (1, 3), (2, 4), (3, 5),    # 主任务：run1-4，期间保存 2-5（5 在 run4 期间）
        (4, 6), (5, 7), (6, 8), (7, 9),    # 补派：run5-8，期间保存 6-9（9 在 run8 期间）
    ]
    for index in range(4):
        scenario.await_run(index)
        revisions[index + 2] = scenario.save(index + 2)
        scenario.trigger(fake)
        scenario.release(index)
    thread0.join(10)
    assert "error" not in holder0, holder0.get("error")
    job_r, thread_r, holder_r = scenario.begin_job(logs)
    for index in range(4, 8):
        scenario.await_run(index)
        revisions[index + 2] = scenario.save(index + 2)
        result = scenario.trigger(fake)
        assert result["ok"] is False
        scenario.release(index)
    thread_r.join(10)
    assert "error" not in holder_r, holder_r.get("error")
    # 补派达到接续上限退出：版本 9 的标记仍在，且队列已空闲 —— 必须由巡检接班。
    assert _versions(scenario) == [1, 2, 3, 4, 5, 6, 7, 8]
    assert scenario.receipt() == revisions[8]
    assert scenario.pending() is True
    assert scenario.double.active_count() == 0
    watchdog = InspectionRetriggerWatchdog(
        dispatch=lambda: AppContainer.dispatch_pending_inspection_retrigger(fake),
        interval_sec=1, emit_log=lambda message: None)
    watchdog.start()
    try:
        waiter = threading.Event()
        for _ in range(200):
            if len([sid for key, sid in scenario.double.submissions
                    if key == "inspection_data_upload:retrigger"]) >= 2:
                break
            waiter.wait(0.05)
        assert len([sid for key, sid in scenario.double.submissions
                    if key == "inspection_data_upload:retrigger"]) >= 2, "巡检未提交接班补派"
        job_w, thread_w, holder_w = scenario.begin_job(logs)
        scenario.await_run(8)
        scenario.release(8)
        thread_w.join(10)
        assert "error" not in holder_w, holder_w.get("error")
    finally:
        watchdog.stop()
    assert _versions(scenario) == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert scenario.receipt() == revisions[9]
    assert scenario.pending() is False
    assert scenario.double.active_count() == 0


def test_dispatch_pending_is_noop_while_queue_busy(tmp_path, monkeypatch):
    scenario = Scenario(tmp_path, monkeypatch)
    fake = scenario.fake_container()
    point_settings.request_inspection_upload_retrigger(scenario.config, reason="测试")
    job = scenario.double.start_worker_job(
        name="占用", worker_handler="inspection_data_upload", worker_payload={},
        resource_keys=["inspection_data_upload:global"], feature="inspection_data_upload",
        dedupe_key="inspection_data_upload")
    scenario.double.mark_running(job)
    result = AppContainer.dispatch_pending_inspection_retrigger(fake)
    assert result["ok"] is False
    assert scenario.pending() is True                      # 忙时不消费、不派发
    scenario.double.mark_terminal(job)
    result = AppContainer.dispatch_pending_inspection_retrigger(fake)
    assert result["ok"] is True
    assert scenario.pending() is True                      # 标记留给接班任务启动时消费
    assert scenario.double.find_active_job_by_dedupe_key("inspection_data_upload:retrigger")


def test_receipt_never_overstates_when_save_lands_before_booking(tmp_path, monkeypatch):
    import app.modules.inspection_data.service.inspection_data_upload_service as upload_module

    config = {"paths": {"runtime_state_root": str(tmp_path)},
              "inspection_data_upload": {"enabled": True, "retrigger_min_interval_sec": 0}}
    point_settings._save_settings(config, {"D楼": {"electrical_inspection_family": {"points": {"a": ""}, "remove": []}}})
    rev_start = point_settings.current_point_settings_revision(config)

    class LegacyService:  # 不上报 settings_revision 的旧契约
        def __init__(self, cfg):
            self.cfg = cfg

        def run(self, **kwargs):
            point_settings._save_settings(
                config, {"D楼": {"electrical_inspection_family": {"points": {"b": ""}, "remove": []}}})
            return {"status": "success"}

    monkeypatch.setattr(upload_module, "InspectionDataUploadService", LegacyService)
    result = task_handlers.handle_inspection_data_upload(
        config, {"source_date": "2026-11-10", "source_bucket_key": "2026-11-10 10", "dry_run": False},
        lambda line: None, None)
    assert result["status"] == "success"
    latest = point_settings.current_point_settings_revision(config)
    # 缺失权威快照版本：不写回执，运行期间的新保存绝不被冒认为已覆盖。
    assert point_settings.last_inspection_upload_revision(config) is None
    assert latest > rev_start
