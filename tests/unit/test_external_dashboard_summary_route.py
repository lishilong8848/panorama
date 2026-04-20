from types import SimpleNamespace
import threading
import time

from app.modules.report_pipeline.api import routes


class _FakeDayMetricBitableExportService:
    def __init__(self, _cfg):
        pass

    def build_target_descriptor(self, force_refresh=False):
        _ = force_refresh
        return {"display_url": "https://example.invalid/day-metric"}


class _FakeSharedSourceCacheService:
    def __init__(self, *, runtime_config, store=None, download_browser_pool=None, emit_log=None):
        _ = runtime_config, store, download_browser_pool, emit_log

    def get_alarm_event_upload_target_preview(self, force_refresh=False):
        _ = force_refresh
        return {"display_url": "https://example.invalid/alarm"}


class _FakeReviewLinkDeliveryService:
    def __init__(self, runtime_cfg, *, config_path=""):
        self.runtime_cfg = runtime_cfg
        self.config_path = config_path

    def build_recipient_status_by_building(self):
        deployment = self.runtime_cfg.get("deployment", {}) if isinstance(self.runtime_cfg, dict) else {}
        return [
            {
                "building": "A楼",
                "recipient_count": 1,
                "enabled_count": 1,
                "disabled_count": 0,
                "invalid_count": 0,
                "status": "ready",
                "status_text": "已保存，可发送",
                "reason": f"角色={deployment.get('role_mode', '')}",
            }
        ]


class _FakeCoordinator:
    def is_running(self):
        return True

    def read_scope_snapshot(self, scope):
        if scope == "runtime_health_lite":
            return {
                "payload": {
                    "ok": True,
                    "health_mode": "lite",
                    "deployment": {"role_mode": "external"},
                    "runtime_activated": True,
                    "startup_role_confirmed": True,
                }
            }
        if scope == "runtime_resources_summary":
            return {"payload": {"network": {}, "controlled_browser": {}, "batch_locks": [], "resources": []}}
        if scope in {"job_panel_dashboard_summary", "job_panel_summary"}:
            return {
                "payload": {
                    "jobs": [],
                    "count": 0,
                    "active_job_ids": [],
                    "job_counts": {},
                    "display": {
                        "overview": {
                            "tone": "neutral",
                            "status_text": "当前空闲",
                            "summary_text": "暂无长耗时任务。",
                        }
                    },
                }
            }
        if scope == "bridge_tasks_dashboard_summary":
            return {
                "payload": {
                    "tasks": [
                        {
                            "task_id": "bridge-task-1",
                            "feature": "handover_cache_fill",
                            "feature_label": "交接班历史共享文件补采",
                            "status": "ready_for_external",
                            "current_stage_name": "共享文件补采",
                            "current_stage_status": "ready_for_external",
                            "request": {
                                "continuation_kind": "handover",
                                "buildings": ["A楼"],
                                "duty_date": "2026-04-18",
                                "duty_shift": "day",
                            },
                            "events": [
                                {
                                    "event_type": "waiting_source_sync",
                                    "event_text": "等待内网补采同步",
                                }
                            ],
                        }
                    ],
                    "count": 1,
                    "display": {
                        "overview": {
                            "tone": "neutral",
                            "status_text": "当前空闲",
                            "summary_text": "暂无共享桥接任务。",
                        }
                    },
                }
            }
        if scope == "bridge_tasks_summary":
            return {
                "payload": {
                    "tasks": [
                        {
                            "task_id": "bridge-task-1",
                            "feature": "handover_cache_fill",
                            "feature_label": "交接班历史共享文件补采",
                            "status": "ready_for_external",
                            "current_stage_name": "共享文件补采",
                            "current_stage_status": "ready_for_external",
                            "request": {
                                "continuation_kind": "handover",
                                "buildings": ["A楼"],
                                "duty_date": "2026-04-18",
                                "duty_shift": "day",
                            },
                            "events": [
                                {
                                    "event_type": "waiting_source_sync",
                                    "event_text": "等待内网补采同步",
                                }
                            ],
                        }
                    ],
                    "count": 1,
                }
            }
        return None

    def request_refresh(self, reason=""):
        _ = reason


class _FakeFastSourceCacheService:
    def __init__(self, payload):
        self.payload = payload

    def get_external_source_cache_overview_fast(self):
        return self.payload


def test_get_external_dashboard_summary_returns_dashboard_display_without_runtime_cfg_error(monkeypatch):
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _FakeDayMetricBitableExportService)
    monkeypatch.setattr(routes, "SharedSourceCacheService", _FakeSharedSourceCacheService)
    monkeypatch.setattr(routes, "ReviewLinkDeliveryService", _FakeReviewLinkDeliveryService)
    monkeypatch.setattr(
        routes,
        "_build_latest_handover_review_status",
        lambda _container: {
            "has_any_session": True,
            "required_count": 5,
            "confirmed_count": 4,
            "all_confirmed": False,
            "buildings": [
                {
                    "building": "A楼",
                    "has_session": True,
                    "confirmed": False,
                    "session_id": "A楼|2026-04-18|day",
                    "revision": 3,
                    "updated_at": "2026-04-18 10:00:00",
                    "cloud_sheet_sync": {"status": "pending_upload"},
                    "review_link_delivery": {"status": "pending_access"},
                }
            ],
        },
    )
    monkeypatch.setattr(
        routes,
        "build_bridge_tasks_summary",
        lambda tasks, count=0: {"display": {"overview": {"tone": "neutral", "status_text": "当前空闲", "summary_text": "暂无共享桥接任务。"}}},
    )
    monkeypatch.setattr(routes, "present_bridge_task", lambda task: task)

    container = SimpleNamespace(
        config={"deployment": {"role_mode": "external"}},
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"root_dir": r"C:\share"},
            "feishu": {"app_id": "cli_xxx", "app_secret": "secret"},
            "handover_log": {"template": {"source_path": r"D:\tpl\handover.xlsx"}},
        },
        runtime_status_coordinator=_FakeCoordinator(),
        deployment_snapshot=lambda: {"role_mode": "external", "node_label": "外网端"},
        config_path="settings.json",
        shared_bridge_snapshot=lambda mode="external_full": {
            "enabled": True,
            "role_mode": "external",
            "internal_source_cache": {
                "handover_log_family": {
                    "display_overview": {
                        "key": "handover_log_family",
                        "title": "交接班日志源文件",
                        "tone": "warning",
                        "status_text": "等待共享文件就绪",
                        "summary_text": "等待共享文件就绪",
                        "detail_text": "等待共享文件就绪",
                        "current_bucket": "2026-04-18 10",
                        "best_bucket_key": "2026-04-18 10",
                        "can_proceed": False,
                        "buildings": [
                            {
                                "building": "A楼",
                                "status_key": "waiting",
                                "status_text": "等待中",
                                "detail_text": "等待共享文件就绪",
                                "bucket_key": "2026-04-18 10",
                            },
                            {
                                "building": "B楼",
                                "status_key": "ready",
                                "status_text": "已就绪",
                                "detail_text": "共享文件已就绪",
                                "bucket_key": "2026-04-18 10",
                            },
                        ],
                    }
                }
            },
            "internal_alert_status": {},
        },
        updater_snapshot=lambda: {"enabled": False, "disabled_reason": "source_python_run", "local_version": "web-3.0.0"},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {
            "status": "alias_match",
            "status_text": "路径写法不同但目录一致",
            "tone": "info",
            "summary_text": "映射盘与 UNC 当前都指向同一共享目录。",
            "items": [{"label": "当前角色", "value": "外网端", "tone": "info"}],
            "paths": [{"label": "外网共享目录", "path": r"Z:\share", "canonical_path": r"\\172.16.1.2\share"}],
            "notes": ["当前角色运行值和 updater 实际共享目录都来自后端运行时。"],
        },
        scheduler_status=lambda: {},
        handover_scheduler_status=lambda: {},
        wet_bulb_collection_scheduler_status=lambda: {},
        day_metric_upload_scheduler_status=lambda: {},
        alarm_event_upload_scheduler_status=lambda: {},
        monthly_event_report_scheduler_status=lambda: {},
        monthly_change_report_scheduler_status=lambda: {},
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={
                    "shared_bridge_snapshot:external_full": {
                        "ts": 0.0,
                        "value": container.shared_bridge_snapshot(),
                        "ready": True,
                        "refreshing": False,
                    },
                    "handover_review_status:latest": {
                        "ts": 0.0,
                        "value": {
                            "has_any_session": True,
                            "required_count": 5,
                            "confirmed_count": 4,
                            "all_confirmed": False,
                            "buildings": [
                                {
                                    "building": "A楼",
                                    "has_session": True,
                                    "confirmed": False,
                                    "session_id": "A楼|2026-04-18|day",
                                    "revision": 3,
                                    "updated_at": "2026-04-18 10:00:00",
                                    "cloud_sheet_sync": {"status": "pending_upload"},
                                    "review_link_delivery": {"status": "pending_access"},
                                }
                            ],
                        },
                        "ready": True,
                        "refreshing": False,
                    },
                    "handover_review_access::": {
                        "ts": 0.0,
                        "value": routes._empty_handover_review_access(),
                        "ready": True,
                        "refreshing": False,
                    },
                    "handover_review_recipient_status_by_building": {
                        "ts": 0.0,
                        "value": [
                            {
                                "building": "A楼",
                                "recipient_count": 1,
                                "enabled_count": 1,
                                "disabled_count": 0,
                                "invalid_count": 0,
                                "status": "ready",
                                "status_text": "已保存，可发送",
                                "reason": "角色=external",
                            }
                        ],
                        "ready": True,
                        "refreshing": False,
                    },
                    "shared_root_diagnostic:external": {
                        "ts": 0.0,
                        "value": container.shared_root_diagnostic_snapshot(),
                        "ready": True,
                        "refreshing": False,
                    },
                },
                _health_component_cache_lock=threading.Lock(),
            )
        )
    )

    payload = routes.get_external_dashboard_summary(request)

    assert payload["ok"] is True
    assert payload["shared_source_cache_overview"]["status_text"] == "等待共享文件就绪"
    assert payload["shared_source_cache_overview"]["families"][0]["backfill_running"] is True
    assert payload["shared_source_cache_overview"]["families"][0]["buildings"][0]["status_text"] == "补采中"
    assert payload["shared_source_cache_overview"]["families"][0]["buildings"][1]["status_text"] == "已就绪"
    assert payload["shared_source_cache_overview"]["families"][0]["buildings"][1]["bucket_key"] == "2026-04-18 10"
    assert payload["display"]["shared_source_cache_overview"]["status_text"] == "等待共享文件就绪"
    assert payload["display"]["shared_source_cache_overview"]["families"][0]["buildings"][1]["status_text"] == "已就绪"
    assert payload["display"]["task_panel_overview"]["status_text"] == "当前空闲"
    assert payload["display"]["bridge_task_panel_overview"]["status_text"] == "当前空闲"
    assert payload["display"]["system_overview"]["title"] == "当前运行环境"
    assert payload["display"]["scheduler_overview"]["title"] == "月报与交接班调度"
    assert payload["display"]["shared_root_diagnostic_overview"]["status_text"] == "路径写法不同但目录一致"
    assert payload["display"]["handover_review_overview"]["status_text"] == "还有 1 个楼待确认"
    assert payload["display"]["current_task_overview"]["status_text"] == "当前空闲"
    review_row = payload["display"]["handover_review_overview"]["review_board_rows"][0]
    assert review_row["review_link_recipient_status"]["text"] == "已保存，可发送"
    assert review_row["review_link_recipient_status"]["reason"] == "角色=external"


def test_get_external_dashboard_summary_reuses_prebuilt_shared_source_cache_display_snapshot(monkeypatch):
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _FakeDayMetricBitableExportService)
    monkeypatch.setattr(routes, "SharedSourceCacheService", _FakeSharedSourceCacheService)
    monkeypatch.setattr(routes, "ReviewLinkDeliveryService", _FakeReviewLinkDeliveryService)
    monkeypatch.setattr(
        routes,
        "_build_latest_handover_review_status",
        lambda _container: routes._empty_handover_review_status(),
    )
    monkeypatch.setattr(
        routes,
        "build_bridge_tasks_summary",
        lambda tasks, count=0: {"display": {"overview": {"tone": "neutral", "status_text": "当前空闲", "summary_text": "暂无共享桥接任务。"}}},
    )
    monkeypatch.setattr(routes, "present_bridge_task", lambda task: task)
    monkeypatch.setattr(
        routes,
        "present_external_source_cache_overview",
        lambda _payload: (_ for _ in ()).throw(AssertionError("should reuse snapshot display_overview")),
    )

    shared_source_cache_display = {
        "tone": "warning",
        "status_text": "等待共享文件就绪",
        "summary_text": "等待共享文件就绪",
        "detail_text": "等待共享文件就绪",
        "families": [
            {
                "key": "handover_log_family",
                "title": "交接班日志源文件",
                "tone": "warning",
                "status_text": "等待共享文件就绪",
                "summary_text": "等待共享文件就绪",
                "detail_text": "等待共享文件就绪",
                "buildings": [
                    {
                        "building": "A楼",
                        "status_key": "waiting",
                        "status_text": "等待中",
                        "detail_text": "等待共享文件就绪",
                        "bucket_key": "2026-04-18 10",
                    }
                ],
            },
            {
                "key": "handover_capacity_report_family",
                "title": "交接班容量报表源文件",
                "tone": "neutral",
                "status_text": "当前空闲",
                "summary_text": "",
                "detail_text": "",
                "buildings": [],
            },
            {
                "key": "monthly_report_family",
                "title": "全景平台月报源文件",
                "tone": "neutral",
                "status_text": "当前空闲",
                "summary_text": "",
                "detail_text": "",
                "buildings": [],
            },
            {
                "key": "alarm_event_family",
                "title": "告警信息源文件",
                "tone": "neutral",
                "status_text": "当前空闲",
                "summary_text": "",
                "detail_text": "",
                "buildings": [],
            },
        ],
    }

    container = SimpleNamespace(
        config={"deployment": {"role_mode": "external"}},
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"root_dir": r"C:\share"},
            "feishu": {"app_id": "cli_xxx", "app_secret": "secret"},
            "handover_log": {"template": {"source_path": r"D:\tpl\handover.xlsx"}},
        },
        runtime_status_coordinator=_FakeCoordinator(),
        deployment_snapshot=lambda: {"role_mode": "external", "node_label": "外网端"},
        config_path="settings.json",
        shared_bridge_snapshot=lambda mode="external_full": {
            "enabled": True,
            "role_mode": "external",
            "internal_source_cache": {
                "display_overview": shared_source_cache_display,
                "handover_log_family": {
                    "display_overview": {
                        "key": "handover_log_family",
                        "title": "交接班日志源文件",
                        "tone": "warning",
                        "status_text": "等待共享文件就绪",
                        "summary_text": "等待共享文件就绪",
                        "detail_text": "等待共享文件就绪",
                        "buildings": [
                            {
                                "building": "A楼",
                                "status_key": "waiting",
                                "status_text": "等待中",
                                "detail_text": "等待共享文件就绪",
                                "bucket_key": "2026-04-18 10",
                            }
                        ],
                    }
                },
            },
            "internal_alert_status": {},
        },
        updater_snapshot=lambda: {"enabled": False, "disabled_reason": "source_python_run", "local_version": "web-3.0.0"},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {},
        scheduler_status=lambda: {},
        handover_scheduler_status=lambda: {},
        wet_bulb_collection_scheduler_status=lambda: {},
        day_metric_upload_scheduler_status=lambda: {},
        alarm_event_upload_scheduler_status=lambda: {},
        monthly_event_report_scheduler_status=lambda: {},
        monthly_change_report_scheduler_status=lambda: {},
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={
                    "shared_bridge_snapshot:external_full": {
                        "ts": 0.0,
                        "value": container.shared_bridge_snapshot(),
                        "ready": True,
                        "refreshing": False,
                    },
                    "handover_review_access::": {
                        "ts": 0.0,
                        "value": routes._empty_handover_review_access(),
                        "ready": True,
                        "refreshing": False,
                    },
                    "shared_root_diagnostic:external": {
                        "ts": 0.0,
                        "value": {},
                        "ready": True,
                        "refreshing": False,
                    },
                },
                _health_component_cache_lock=threading.Lock(),
            )
        )
    )

    payload = routes.get_external_dashboard_summary(request)

    assert payload["shared_source_cache_overview"]["families"][0]["backfill_running"] is True
    assert payload["shared_source_cache_overview"]["families"][0]["buildings"][0]["status_text"] == "补采中"


def test_get_external_dashboard_summary_falls_back_to_cached_shared_bridge_source_cache(monkeypatch):
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _FakeDayMetricBitableExportService)
    monkeypatch.setattr(routes, "SharedSourceCacheService", _FakeSharedSourceCacheService)
    monkeypatch.setattr(routes, "ReviewLinkDeliveryService", _FakeReviewLinkDeliveryService)
    monkeypatch.setattr(
        routes,
        "_build_latest_handover_review_status",
        lambda _container: routes._empty_handover_review_status(),
    )
    monkeypatch.setattr(
        routes,
        "build_bridge_tasks_summary",
        lambda tasks, count=0: {
            "tasks": [],
            "count": 0,
            "display": {
                "overview": {
                    "tone": "neutral",
                    "status_text": "当前空闲",
                    "summary_text": "暂无共享桥接任务。",
                }
            },
        },
    )
    monkeypatch.setattr(routes, "present_bridge_task", lambda task: task)

    class _EmptyCoordinator(_FakeCoordinator):
        def read_scope_snapshot(self, scope):
            if scope == "bridge_tasks_dashboard_summary":
                return {
                    "payload": {
                        "tasks": [],
                        "count": 0,
                        "display": {
                            "overview": {
                                "tone": "neutral",
                                "status_text": "当前空闲",
                                "summary_text": "暂无共享桥接任务。",
                            }
                        },
                    }
                }
            if scope == "bridge_tasks_summary":
                return {"payload": {"tasks": [], "count": 0}}
            return super().read_scope_snapshot(scope)

    container = SimpleNamespace(
        config={"deployment": {"role_mode": "external"}},
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"root_dir": r"C:\share"},
            "feishu": {"app_id": "cli_xxx", "app_secret": "secret"},
            "handover_log": {"template": {"source_path": r"D:\tpl\handover.xlsx"}},
        },
        runtime_status_coordinator=_EmptyCoordinator(),
        deployment_snapshot=lambda: {"role_mode": "external", "node_label": "外网端"},
        config_path="settings.json",
        shared_bridge_snapshot=lambda mode="external_full": {
            "enabled": True,
            "role_mode": "external",
            "internal_source_cache": {
                "handover_log_family": {
                    "display_overview": {
                        "key": "handover_log_family",
                        "title": "交接班日志源文件",
                        "tone": "success",
                        "status_text": "共享文件已就绪",
                        "summary_text": "当前参考桶的共享文件已准备完成。",
                        "detail_text": "",
                        "current_bucket": "2026-04-20 13",
                        "best_bucket_key": "2026-04-20 13",
                        "can_proceed": True,
                        "buildings": [
                            {
                                "building": "A楼",
                                "status_key": "ready",
                                "status_text": "已就绪",
                                "detail_text": "2026-04-20 13:31:43",
                                "bucket_key": "2026-04-20 13",
                            }
                        ],
                    }
                }
            },
            "internal_alert_status": {},
        },
        updater_snapshot=lambda: {},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {},
        scheduler_status=lambda: {},
        handover_scheduler_status=lambda: {},
        wet_bulb_collection_scheduler_status=lambda: {},
        day_metric_upload_scheduler_status=lambda: {},
        alarm_event_upload_scheduler_status=lambda: {},
        monthly_event_report_scheduler_status=lambda: {},
        monthly_change_report_scheduler_status=lambda: {},
    )
    cached_shared_bridge = container.shared_bridge_snapshot()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={
                    "shared_bridge_snapshot:external_full": {
                        "ts": routes.time.monotonic(),
                        "value": cached_shared_bridge,
                        "ready": True,
                        "refreshing": False,
                    }
                },
                _health_component_cache_lock=threading.Lock(),
            )
        )
    )

    payload = routes.get_external_dashboard_summary(request)

    family = payload["shared_source_cache_overview"]["families"][0]
    assert family["key"] == "handover_log_family"
    assert family["status_text"] == "共享文件已就绪"
    assert family["current_bucket"] == "2026-04-20 13"
    assert family["buildings"][0]["status_text"] == "已就绪"
    assert payload["display"]["shared_source_cache_overview"]["families"][0]["buildings"][0]["status_text"] == "已就绪"


def test_get_external_dashboard_summary_keeps_last_non_empty_source_cache_when_current_snapshot_empty(monkeypatch):
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _FakeDayMetricBitableExportService)
    monkeypatch.setattr(routes, "SharedSourceCacheService", _FakeSharedSourceCacheService)
    monkeypatch.setattr(routes, "ReviewLinkDeliveryService", _FakeReviewLinkDeliveryService)
    monkeypatch.setattr(
        routes,
        "_build_latest_handover_review_status",
        lambda _container: routes._empty_handover_review_status(),
    )
    monkeypatch.setattr(
        routes,
        "build_bridge_tasks_summary",
        lambda tasks, count=0: {
            "tasks": [],
            "count": 0,
            "display": {"overview": {"tone": "neutral", "status_text": "当前空闲", "summary_text": "暂无共享桥接任务。"}},
        },
    )
    monkeypatch.setattr(routes, "present_bridge_task", lambda task: task)

    class _EmptySourceCoordinator(_FakeCoordinator):
        def read_scope_snapshot(self, scope):
            if scope == "external_shared_bridge_full":
                return {"payload": {"enabled": True, "role_mode": "external", "internal_source_cache": {}, "internal_alert_status": {}}}
            if scope == "bridge_tasks_dashboard_summary":
                return {"payload": {"tasks": [], "count": 0, "display": {"overview": {"tone": "neutral", "status_text": "当前空闲", "summary_text": "暂无共享桥接任务。"}}}}
            if scope == "bridge_tasks_summary":
                return {"payload": {"tasks": [], "count": 0}}
            return super().read_scope_snapshot(scope)

    last_non_empty = {
        "tone": "success",
        "status_text": "共享文件已就绪",
        "summary_text": "上一份有效源文件状态。",
        "detail_text": "上一份有效源文件状态。",
        "reference_bucket_key": "2026-04-20 13",
        "families": [
            {
                "key": "handover_log_family",
                "title": "交接班日志源文件",
                "tone": "success",
                "status_text": "共享文件已就绪",
                "summary_text": "当前参考桶的共享文件已准备完成。",
                "detail_text": "",
                "current_bucket": "2026-04-20 13",
                "best_bucket_key": "2026-04-20 13",
                "can_proceed": True,
                "buildings": [
                    {
                        "building": "A楼",
                        "status_key": "ready",
                        "status_text": "已就绪",
                        "detail_text": "2026-04-20 13:31:43",
                        "bucket_key": "2026-04-20 13",
                        "relative_path": "交接班日志源文件/test.xlsx",
                    }
                ],
            }
        ],
    }
    container = SimpleNamespace(
        config={"deployment": {"role_mode": "external"}},
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"root_dir": r"C:\share"},
            "feishu": {"app_id": "cli_xxx", "app_secret": "secret"},
            "handover_log": {"template": {"source_path": r"D:\tpl\handover.xlsx"}},
        },
        runtime_status_coordinator=_EmptySourceCoordinator(),
        deployment_snapshot=lambda: {"role_mode": "external", "node_label": "外网端"},
        config_path="settings.json",
        shared_bridge_snapshot=lambda mode="external_full": {
            "enabled": True,
            "role_mode": "external",
            "internal_source_cache": {},
            "internal_alert_status": {},
        },
        updater_snapshot=lambda: {},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {},
        scheduler_status=lambda: {},
        handover_scheduler_status=lambda: {},
        wet_bulb_collection_scheduler_status=lambda: {},
        day_metric_upload_scheduler_status=lambda: {},
        alarm_event_upload_scheduler_status=lambda: {},
        monthly_event_report_scheduler_status=lambda: {},
        monthly_change_report_scheduler_status=lambda: {},
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _external_source_cache_overview_last_non_empty=last_non_empty,
                _health_component_cache={},
                _health_component_cache_lock=threading.Lock(),
            )
        )
    )

    payload = routes.get_external_dashboard_summary(request)

    assert payload["shared_source_cache_overview"]["families"][0]["buildings"][0]["status_text"] == "已就绪"
    assert payload["shared_source_cache_overview"]["families"][0]["buildings"][0]["relative_path"] == "交接班日志源文件/test.xlsx"
    assert payload["display"]["shared_source_cache_overview"]["families"][0]["buildings"][0]["status_text"] == "已就绪"


def test_get_external_dashboard_summary_does_not_block_on_shared_bridge_snapshot_without_coordinator(monkeypatch):
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _FakeDayMetricBitableExportService)
    monkeypatch.setattr(routes, "SharedSourceCacheService", _FakeSharedSourceCacheService)
    monkeypatch.setattr(routes, "ReviewLinkDeliveryService", _FakeReviewLinkDeliveryService)
    monkeypatch.setattr(
        routes,
        "_build_latest_handover_review_status",
        lambda _container: routes._empty_handover_review_status(),
    )

    def _slow_shared_bridge_snapshot(*_args, **_kwargs):
        time.sleep(1.0)
        return {
            "enabled": True,
            "role_mode": "external",
            "internal_source_cache": {},
            "internal_alert_status": {},
        }

    monkeypatch.setattr(routes, "_build_shared_bridge_health_snapshot", _slow_shared_bridge_snapshot)

    container = SimpleNamespace(
        config={"deployment": {"role_mode": "external"}},
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"root_dir": r"C:\share"},
            "feishu": {"app_id": "cli_xxx", "app_secret": "secret"},
            "handover_log": {"template": {"source_path": r"D:\tpl\handover.xlsx"}},
        },
        runtime_status_coordinator=None,
        deployment_snapshot=lambda: {"role_mode": "external", "node_label": "外网端"},
        config_path="settings.json",
        updater_snapshot=lambda: {},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {},
        scheduler_status=lambda: {},
        handover_scheduler_status=lambda: {},
        wet_bulb_collection_scheduler_status=lambda: {},
        day_metric_upload_scheduler_status=lambda: {},
        alarm_event_upload_scheduler_status=lambda: {},
        monthly_event_report_scheduler_status=lambda: {},
        monthly_change_report_scheduler_status=lambda: {},
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={},
                _health_component_cache_lock=threading.Lock(),
            )
        )
    )

    started_at = time.perf_counter()
    payload = routes.get_external_dashboard_summary(request)
    elapsed = time.perf_counter() - started_at

    assert payload["ok"] is True
    assert elapsed < 0.5


def test_get_external_dashboard_summary_does_not_block_on_fast_source_cache_overview(monkeypatch):
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _FakeDayMetricBitableExportService)
    monkeypatch.setattr(routes, "SharedSourceCacheService", _FakeSharedSourceCacheService)
    monkeypatch.setattr(routes, "ReviewLinkDeliveryService", _FakeReviewLinkDeliveryService)
    monkeypatch.setattr(
        routes,
        "_build_latest_handover_review_status",
        lambda _container: routes._empty_handover_review_status(),
    )

    class _SlowBridgeService:
        def get_external_source_cache_overview_fast(self):
            time.sleep(1.0)
            return {
                "families": [
                    {
                        "key": "handover_log_family",
                        "current_bucket": "2026-04-20 14",
                        "buildings": [
                            {
                                "building": "A楼",
                                "status_key": "ready",
                                "status_text": "已就绪",
                                "detail_text": "共享文件已就绪",
                                "bucket_key": "2026-04-20 14",
                            }
                        ],
                    }
                ]
            }

    class _EmptySourceCoordinator(_FakeCoordinator):
        def read_scope_snapshot(self, scope):
            if scope == "external_shared_bridge_full":
                return {
                    "payload": {
                        "enabled": True,
                        "role_mode": "external",
                        "internal_source_cache": {},
                        "internal_alert_status": {},
                    }
                }
            return super().read_scope_snapshot(scope)

    container = SimpleNamespace(
        config={"deployment": {"role_mode": "external"}},
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"root_dir": r"C:\share"},
            "feishu": {"app_id": "cli_xxx", "app_secret": "secret"},
            "handover_log": {"template": {"source_path": r"D:\tpl\handover.xlsx"}},
        },
        runtime_status_coordinator=_EmptySourceCoordinator(),
        shared_bridge_service=_SlowBridgeService(),
        deployment_snapshot=lambda: {"role_mode": "external", "node_label": "外网端"},
        config_path="settings.json",
        shared_bridge_snapshot=lambda mode="external_full": {
            "enabled": True,
            "role_mode": "external",
            "internal_source_cache": {},
            "internal_alert_status": {},
        },
        updater_snapshot=lambda: {},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {},
        scheduler_status=lambda: {},
        handover_scheduler_status=lambda: {},
        wet_bulb_collection_scheduler_status=lambda: {},
        day_metric_upload_scheduler_status=lambda: {},
        alarm_event_upload_scheduler_status=lambda: {},
        monthly_event_report_scheduler_status=lambda: {},
        monthly_change_report_scheduler_status=lambda: {},
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={},
                _health_component_cache_lock=threading.Lock(),
            )
        )
    )

    started_at = time.perf_counter()
    payload = routes.get_external_dashboard_summary(request)
    elapsed = time.perf_counter() - started_at

    assert payload["ok"] is True
    assert elapsed < 0.5
    time.sleep(1.1)
    refreshed_payload = routes.get_external_dashboard_summary(request)
    family = refreshed_payload["shared_source_cache_overview"]["families"][0]
    assert family["buildings"][0]["status_text"] == "已就绪"
    assert family["buildings"][0]["bucket_key"] == "2026-04-20 14"


def test_external_dashboard_summary_role_mismatch_returns_empty_display():
    container = SimpleNamespace(
        config={"deployment": {"role_mode": "internal"}},
        runtime_config={"deployment": {"role_mode": "internal"}},
        deployment_snapshot=lambda: {"role_mode": "internal", "node_label": "内网端"},
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))

    payload = routes.get_external_dashboard_summary(request)

    assert payload["ok"] is True
    assert payload["reason_code"] == "role_mismatch"
    assert payload["shared_source_cache_overview"]["reason_code"] == "role_mismatch"
    assert payload["display"]["shared_source_cache_overview"]["families"] == []


def test_external_split_status_endpoints_return_role_mismatch_without_409():
    container = SimpleNamespace(
        config={"deployment": {"role_mode": "internal"}},
        runtime_config={"deployment": {"role_mode": "internal"}},
        deployment_snapshot=lambda: {"role_mode": "internal", "node_label": "内网端"},
        runtime_activated=True,
        startup_role_confirmed=True,
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))

    endpoints = [
        routes.get_external_runtime_bootstrap,
        routes.get_external_runtime_source_cache,
        routes.get_external_runtime_jobs,
        routes.get_external_runtime_bridge_tasks,
        routes.get_external_runtime_schedulers,
        routes.get_external_runtime_updater,
        routes.get_external_runtime_review_overview,
        routes.get_external_runtime_config_guidance,
        routes.get_external_runtime_system,
    ]

    for endpoint in endpoints:
        payload = endpoint(request)
        assert payload["ok"] is True
        assert payload["reason_code"] == "role_mismatch"
        assert isinstance(payload.get("display", {}), dict)


def test_external_split_status_endpoints_return_module_payloads(monkeypatch):
    monkeypatch.setattr(routes, "DayMetricBitableExportService", _FakeDayMetricBitableExportService)
    monkeypatch.setattr(routes, "SharedSourceCacheService", _FakeSharedSourceCacheService)
    monkeypatch.setattr(routes, "ReviewLinkDeliveryService", _FakeReviewLinkDeliveryService)
    monkeypatch.setattr(
        routes,
        "_build_latest_handover_review_status",
        lambda _container: routes._empty_handover_review_status(),
    )
    monkeypatch.setattr(
        routes,
        "build_bridge_tasks_summary",
        lambda tasks, count=0: {
            "tasks": tasks,
            "count": count,
            "display": {"overview": {"tone": "neutral", "status_text": "当前空闲", "summary_text": "暂无共享桥接任务。"}},
        },
    )
    monkeypatch.setattr(routes, "present_bridge_task", lambda task: task)

    container = SimpleNamespace(
        config={"deployment": {"role_mode": "external"}},
        runtime_config={
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"root_dir": r"C:\share"},
            "feishu": {"app_id": "cli_xxx", "app_secret": "secret"},
            "handover_log": {"template": {"source_path": r"D:\tpl\handover.xlsx"}},
        },
        runtime_status_coordinator=_FakeCoordinator(),
        deployment_snapshot=lambda: {"role_mode": "external", "node_label": "外网端"},
        config_path="settings.json",
        updater_snapshot=lambda: {},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {},
        scheduler_status=lambda: {},
        handover_scheduler_status=lambda: {},
        wet_bulb_collection_scheduler_status=lambda: {},
        day_metric_upload_scheduler_status=lambda: {},
        alarm_event_upload_scheduler_status=lambda: {},
        monthly_event_report_scheduler_status=lambda: {},
        monthly_change_report_scheduler_status=lambda: {},
        runtime_activated=True,
        startup_role_confirmed=True,
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={},
                _health_component_cache_lock=threading.Lock(),
            )
        )
    )

    assert routes.get_external_runtime_bootstrap(request)["health_lite"]["deployment"]["role_mode"] == "external"
    assert "shared_source_cache_overview" in routes.get_external_runtime_source_cache(request)
    assert "job_panel_summary" in routes.get_external_runtime_jobs(request)
    assert "bridge_tasks_summary" in routes.get_external_runtime_bridge_tasks(request)
    assert "scheduler_status_summary" in routes.get_external_runtime_schedulers(request)
    assert "updater_summary" in routes.get_external_runtime_updater(request)
    assert "handover_review_status" in routes.get_external_runtime_review_overview(request)
    assert "config_guidance_overview" in routes.get_external_runtime_config_guidance(request)
    assert "runtime_resources_summary" in routes.get_external_runtime_system(request)


def test_external_source_cache_endpoint_returns_slim_display_payload():
    raw_overview = {
        "tone": "success",
        "status_text": "共享文件已就绪",
        "summary_text": "已齐套",
        "reference_bucket_key": "2026-04-20 15",
        "auto_retry_signature": "raw-diagnostic-only",
        "families": [
            {
                "key": "handover_log_family",
                "title": "交接班日志源文件",
                "best_bucket_key": "2026-04-20 15",
                "status_text": "共享文件已就绪",
                "selected_entries": [
                    {
                        "entry_id": "raw-entry",
                        "file_hash": "hash",
                        "size_bytes": 123,
                        "metadata": {"raw": True},
                    }
                ],
                "buildings": [
                    {
                        "building": "A楼",
                        "bucket_key": "2026-04-20 15",
                        "status": "ready",
                        "status_key": "ready",
                        "status_text": "已就绪",
                        "detail_text": "2026-04-20 15:12:39",
                        "downloaded_at": "2026-04-20 15:12:39",
                        "relative_path": "交接班日志源文件/test.xlsx",
                        "resolved_file_path": r"\\172.16.1.2\share\交接班日志源文件\test.xlsx",
                        "file_hash": "hash",
                        "size_bytes": 123,
                        "metadata": {"raw": True},
                        "file_path": r"\\172.16.1.2\share\交接班日志源文件\test.xlsx",
                    }
                ],
            }
        ],
        "family_retry_signatures": {"handover_log_family": "raw-diagnostic-only"},
    }
    container = SimpleNamespace(
        config={"deployment": {"role_mode": "external"}},
        runtime_config={"deployment": {"role_mode": "external"}},
        deployment_snapshot=lambda: {"role_mode": "external", "node_label": "外网端"},
        shared_bridge_service=_FakeFastSourceCacheService(raw_overview),
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={},
                _health_component_cache_lock=threading.Lock(),
                _external_source_cache_overview_last_non_empty=raw_overview,
            )
        )
    )

    payload = routes.get_external_runtime_source_cache(request)

    overview = payload["shared_source_cache_overview"]
    family = overview["families"][0]
    building = family["buildings"][0]
    assert overview["status_text"] == "共享文件已就绪"
    assert family["status_text"] == "共享文件已就绪"
    assert building["status_text"] == "已就绪"
    assert building["resolved_file_path"] == r"\\172.16.1.2\share\交接班日志源文件\test.xlsx"
    assert "selected_entries" not in family
    assert "file_hash" not in building
    assert "size_bytes" not in building
    assert "metadata" not in building
    assert "file_path" not in building
    assert "auto_retry_signature" not in overview
    assert "family_retry_signatures" not in overview
    assert "shared_source_cache_overview" not in payload["display"]
