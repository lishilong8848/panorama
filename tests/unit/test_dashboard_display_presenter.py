from app.modules.shared_bridge.service.dashboard_display_presenter import (
    present_config_guidance_overview,
    present_external_dashboard_display,
    present_external_module_hero_overviews,
    present_external_scheduler_overview,
    present_external_system_overview,
    present_feature_target_displays,
    present_internal_runtime_building_display,
    present_internal_runtime_display,
    present_monthly_report_delivery_display,
    present_monthly_report_last_run_display,
    present_updater_mirror_overview,
)


def test_present_external_dashboard_display_prefers_cache_waiting_state():
    payload = present_external_dashboard_display(
        shared_source_cache_overview={
            "tone": "warning",
            "status_text": "等待共享文件就绪",
            "summary_text": "部分楼栋共享文件缺失。",
            "can_proceed_latest": False,
            "families": [
                {
                    "key": "alarm_event_family",
                    "upload_status": {
                        "tone": "neutral",
                        "status_text": "尚未上传",
                        "summary_text": "尚未执行告警上传。",
                    },
                }
            ],
        },
        review_status={
            "batch_key": "2026-04-17|day",
            "has_any_session": True,
            "required_count": 5,
            "confirmed_count": 2,
            "all_confirmed": False,
            "buildings": [
                {"building": "A楼", "has_session": True, "confirmed": True, "cloud_sheet_sync": {"status": "failed"}},
                {"building": "B楼", "has_session": True, "confirmed": False, "cloud_sheet_sync": {"status": "success"}},
            ],
            "followup_progress": {
                "can_resume_followup": False,
                "failed_count": 0,
                "pending_count": 0,
            },
        },
        task_overview={
            "tone": "info",
            "status_text": "有任务正在执行",
            "summary_text": "当前有运行中任务。",
        },
        shared_root_diagnostic={
            "status": "alias_match",
            "status_text": "路径写法不同但目录一致",
            "tone": "info",
            "summary_text": "映射盘与 UNC 当前都指向同一共享目录。",
            "items": [
                {"label": "当前角色", "value": "外网端", "tone": "info"},
            ],
            "paths": [
                {
                    "label": "外网共享目录",
                    "path": r"Z:\share",
                    "canonical_path": r"\\172.16.1.2\share",
                }
            ],
            "notes": ["当前角色运行值和 updater 实际共享目录都来自后端运行时。"],
        },
    )

    assert payload["home_overview"]["status_text"] == "等待共享文件就绪"
    assert payload["status_diagnosis_overview"]["reason_text"] == "部分楼栋共享文件缺失。"
    assert payload["handover_review_overview"]["status_text"] == "还有 3 个楼待确认"
    assert payload["handover_review_overview"]["actions"]["confirm_all"]["allowed"] is True
    assert payload["handover_review_overview"]["actions"]["retry_cloud_sync_all"]["allowed"] is False
    assert payload["handover_review_overview"]["followup_progress"]["status_text"] == "后续上传已清空"
    assert payload["handover_review_overview"]["followup_progress"]["summary_text"] == "已清空"
    assert payload["handover_review_overview"]["review_board_rows"][0]["building"] == "A楼"
    assert payload["handover_review_overview"]["review_board_rows"][0]["cloud_sheet_sync"]["text"] == "云表最终上传失败"
    assert payload["handover_review_overview"]["review_board_rows"][1]["text"] == "待确认"
    assert payload["shared_root_diagnostic_overview"]["status_text"] == "路径写法不同但目录一致"
    assert payload["shared_root_diagnostic_overview"]["paths"][0]["show_canonical_path"] is True
    assert payload["shared_root_diagnostic_overview"]["actions"] == {}


def test_present_external_system_and_scheduler_overview_return_backend_display_payload():
    system_payload = present_external_system_overview(
        health_lite={"deployment": {"role_mode": "external"}},
        runtime_resources_summary={"network": {"current_ssid": "CMCC-Office"}},
        task_overview={"tone": "info", "status_text": "运行中任务 2 个", "summary_text": "当前有任务正在执行。"},
        shared_root_diagnostic={"tone": "success", "status_text": "共享目录一致", "summary_text": "共享目录可用。"},
        updater_overview={"tone": "neutral", "status_text": "源码运行，已跳过更新"},
    )
    scheduler_payload = present_external_scheduler_overview(
        scheduler_overview_summary={
            "tone": "warning",
            "status_text": "有 1 项待关注",
            "summary_text": "部分调度尚未开启。",
            "items": [
                {"label": "已启动调度", "value": "6 项", "tone": "success"},
                {"label": "待关注项", "value": "1 项", "tone": "warning"},
            ],
        },
        scheduler_overview_items=[],
    )

    assert system_payload["title"] == "当前运行环境"
    assert system_payload["items"][0]["value"] == "外网端"
    assert system_payload["items"][1]["value"] == "CMCC-Office"
    assert scheduler_payload["title"] == "月报与交接班调度"
    assert scheduler_payload["status_text"] == "有 1 项待关注"
    assert scheduler_payload["items"][0]["value"] == "6 项"


def test_present_config_guidance_overview_returns_backend_ready_sections():
    payload = present_config_guidance_overview(
        {
            "deployment": {"role_mode": "external"},
            "shared_bridge": {"root_dir": r"C:\share"},
            "feishu": {"app_id": "cli_xxx", "app_secret": "secret"},
            "handover_log": {
                "template": {"source_path": r"D:\tpl\handover.xlsx"},
                "cloud_sheet_sync": {"root_wiki_url": "https://example.feishu.cn/wiki/abc"},
            },
            "day_metric_upload": {
                "target": {
                    "source": {"app_token": "bascn_demo", "table_id": "tbl123"},
                }
            },
            "alarm_export": {
                "shared_source_upload": {
                    "target": {"app_token": "bascn_alarm", "table_id": "tbl_alarm"},
                }
            },
        },
        configured_role_mode="external",
        running_role_mode="external",
        day_metric_target_preview={"display_url": "https://example.feishu.cn/base/day"},
        alarm_event_target_preview={"display_url": "https://example.feishu.cn/base/alarm"},
    )

    assert payload["tone"] == "success"
    assert payload["status_text"] == "关键配置已齐套"
    assert payload["ready_count"] == payload["total_count"]
    assert any(item["label"] == "12项目标" and item["ready"] is True for item in payload["sections"])
    assert any(item["id"] == "feature_alarm_export" for item in payload["quick_tabs"])


def test_present_feature_target_displays_returns_backend_ready_target_payloads():
    payload = present_feature_target_displays(
        {
            "handover_log": {
                "shift_roster": {
                    "engineer_directory": {
                        "source": {
                            "app_token": "bascn_engineer",
                            "table_id": "tbl_engineer",
                        }
                    }
                }
            },
            "wet_bulb_collection": {
                "target": {
                    "app_token": "bascn_wet",
                    "table_id": "tbl_wet",
                }
            },
            "day_metric_upload": {
                "target": {
                    "source": {
                        "app_token": "bascn_day",
                        "table_id": "tbl_day",
                    }
                }
            },
            "alarm_export": {
                "shared_source_upload": {
                    "target": {
                        "app_token": "bascn_alarm",
                        "table_id": "tbl_alarm",
                    }
                }
            },
        },
        engineer_directory_target_preview={
            "display_url": "https://example.feishu.cn/base/engineer",
            "configured_app_token": "bascn_engineer",
            "table_id": "tbl_engineer",
            "target_kind": "base_url",
        },
        wet_bulb_target_preview={
            "display_url": "https://example.feishu.cn/base/wet",
            "configured_app_token": "bascn_wet",
            "table_id": "tbl_wet",
            "target_kind": "base_url",
            "resolved_at": "2026-04-18 10:00:00",
        },
        day_metric_target_preview={
            "display_url": "https://example.feishu.cn/base/day",
            "configured_app_token": "bascn_day",
            "table_id": "tbl_day",
            "target_kind": "base_url",
        },
        alarm_event_target_preview={
            "display_url": "https://example.feishu.cn/base/alarm",
            "configured_app_token": "bascn_alarm",
            "table_id": "tbl_alarm",
            "target_kind": "base_url",
        },
    )

    assert payload["engineer_directory"]["status_text"] == "已解析"
    assert payload["wet_bulb_collection"]["configured"] is True
    assert payload["wet_bulb_collection"]["resolved_at"] == "2026-04-18 10:00:00"
    assert payload["day_metric_upload"]["display_url"] == "https://example.feishu.cn/base/day"
    assert payload["alarm_event_upload"]["configured"] is True


def test_present_monthly_report_delivery_display_returns_backend_ready_overview_and_rows():
    payload = present_monthly_report_delivery_display(
        "event",
        {"target_month": "2026-04"},
        {
            "last_run": {
                "status": "success",
                "target_month": "2026-04",
                "finished_at": "2026-04-18 10:00:00",
                "successful_buildings": ["A楼"],
                "failed_buildings": ["B楼"],
                "test_mode": True,
                "test_receive_ids": ["ou_test_1"],
                "test_receive_id_type": "open_id",
                "test_successful_receivers": ["张三"],
                "test_failed_receivers": ["李四"],
                "test_file_name": "事件月报-A楼.xlsx",
                "test_file_building": "A楼",
            },
            "recipient_status_by_building": [
                {
                    "building": "A楼",
                    "supervisor": "张三",
                    "position": "设施运维主管",
                    "recipient_id": "ou_xxx",
                    "receive_id_type": "open_id",
                    "send_ready": True,
                    "file_name": "事件月报-A楼.xlsx",
                    "file_path": r"D:\out\a.xlsx",
                    "file_exists": True,
                    "report_type": "event",
                    "target_month": "2026-04",
                },
                {
                    "building": "B楼",
                    "send_ready": False,
                    "reason": "工程师目录未匹配到该楼栋",
                    "file_exists": True,
                    "report_type": "event",
                    "target_month": "2026-04",
                },
            ],
            "error": "",
        },
    )

    assert payload["overview"]["status_text"] == "最近测试发送成功"
    assert payload["overview"]["send_ready_count"] == 1
    assert payload["last_run"]["finished_at"] == "2026-04-18 10:00:00"
    assert payload["last_run"]["successful_buildings"] == ["A楼"]
    assert payload["last_run"]["test_mode"] is True
    assert payload["last_run"]["test_receive_ids"] == ["ou_test_1"]
    assert payload["rows"][0]["status_text"] == "可发送"
    assert payload["rows"][1]["detail_text"] == "工程师目录未匹配到该楼栋"


def test_present_monthly_report_delivery_display_marks_precheck_failure_first():
    payload = present_monthly_report_delivery_display(
        "change",
        {"target_month": "2026-04"},
        {
            "last_run": {},
            "recipient_status_by_building": [],
            "error": "工程师目录读取失败",
        },
    )

    assert payload["overview"]["tone"] == "danger"
    assert payload["overview"]["status_text"] == "发送前置检查失败"
    assert payload["overview"]["summary_text"] == "工程师目录读取失败"


def test_present_monthly_report_last_run_display_keeps_recent_result_details():
    payload = present_monthly_report_last_run_display(
        "event",
        {
            "status": "success",
            "target_month": "2026-04",
            "generated_files": 5,
            "started_at": "2026-04-18 09:00:00",
            "finished_at": "2026-04-18 09:02:00",
            "successful_buildings": ["A楼", "B楼"],
            "failed_buildings": ["C楼"],
            "output_dir": r"D:\out",
        },
    )

    assert payload["status_text"] == "最近生成成功"
    assert payload["started_at"] == "2026-04-18 09:00:00"
    assert payload["finished_at"] == "2026-04-18 09:02:00"
    assert payload["successful_buildings"] == ["A楼", "B楼"]
    assert payload["failed_buildings"] == ["C楼"]
    assert payload["output_dir"] == r"D:\out"


def test_present_external_module_hero_overviews_covers_external_dashboard_modules():
    payload = present_external_module_hero_overviews(
        scheduler_overview_summary={
            "items": [
                {"label": "已启动调度", "value": "3 项"},
                {"label": "未启动调度", "value": "4 项"},
                {"label": "待关注项", "value": "1 项"},
            ]
        },
        scheduler_status_summary={
            "scheduler": {
                "display": {"status_text": "运行中"},
            },
            "day_metric_upload_scheduler": {
                "display": {"status_text": "已记住开启"},
                "next_run_time": "2026-04-18 20:00:00",
            },
            "wet_bulb_collection_scheduler": {
                "display": {"status_text": "运行中"},
                "next_run_time": "2026-04-18 19:00:00",
            },
            "monthly_event_report_scheduler": {
                "display": {"status_text": "未启动"},
                "next_run_time": "2026-05-01 01:00:00",
            },
            "monthly_change_report_scheduler": {
                "display": {"status_text": "未启动"},
                "next_run_time": "2026-05-02 02:00:00",
            },
        },
        review_status={
            "duty_text": "2026-04-18 白班",
            "confirmed_count": 2,
            "required_count": 5,
            "summary_text": "还有 3 个楼待确认",
        },
        shared_source_cache_overview={
            "families": [
                {
                    "key": "alarm_event_family",
                    "uploadLastRunAt": "2026-04-18 18:00:00",
                    "uploadRecordCount": 321,
                    "uploadFileCount": 5,
                }
            ]
        },
        runtime_resources_summary={
            "network": {"current_ssid": "QJ-External"},
        },
        job_panel_summary={
            "display": {
                "overview": {
                    "status_text": "有任务正在执行",
                    "running_count": 2,
                    "waiting_count": 1,
                    "bridge_active_count": 1,
                }
            }
        },
        feature_target_displays={
            "day_metric_upload": {"status_text": "已配置"},
            "wet_bulb_collection": {"status_text": "已解析"},
        },
    )

    assert payload["auto_flow"]["metrics"][0]["value"] == "QJ-External"
    assert payload["multi_date"]["metrics"][2]["value"] == "1 项"
    assert payload["manual_upload"]["metrics"][0]["value"] == "固定按当前角色执行"
    assert payload["sheet_import"]["metrics"][2]["value"] == "处理中"
    assert payload["day_metric_upload"]["metrics"][2]["value"] == "已配置"
    assert payload["wet_bulb_collection"]["metrics"][2]["value"] == "已解析"


def test_present_internal_runtime_display_marks_failure_first():
    payload = present_internal_runtime_display(
        {
            "source_cache": {
                "overview": {
                    "tone": "warning",
                    "status_text": "运行中",
                    "summary_text": "共享缓存仓正在维护文件。",
                    "families": [{"has_failures": True, "status_text": "存在失败楼栋"}],
                },
                "current_hour_refresh_overview": {
                    "tone": "danger",
                    "status_text": "最近一轮存在失败",
                    "summary_text": "最近一轮下载失败。",
                    "failed_buildings": ["A楼"],
                    "blocked_buildings": [],
                    "last_error": "下载失败",
                },
            },
            "pool": {
                "overview": {
                    "tone": "success",
                    "status_text": "运行中 / 5个楼已登录",
                    "summary_text": "页池正常。",
                    "slots": [],
                }
            },
        },
        task_overview={
            "tone": "neutral",
            "status_text": "当前空闲",
            "summary_text": "暂无长耗时任务。",
        },
    )

    assert payload["home_overview"]["status_text"] == "运行中"
    assert payload["status_diagnosis_overview"]["tone"] == "danger"
    assert payload["status_diagnosis_overview"]["status_text"] == "当前有需要人工处理的问题"
    assert payload["runtime_overview"]["status_text"] == "运行中"
    assert payload["runtime_overview"]["pool_status_text"] == "运行中 / 5个楼已登录"
    assert payload["history_overview"]["detail_text"] == "历史卡片只展示后端聚合后的最近时间点和最近错误，不再回退前端本地日志拼装。"
    assert payload["history_overview"]["actions"] == {}


def test_present_internal_runtime_display_marks_actions_pending_from_backend_state():
    payload = present_internal_runtime_display(
        {
            "source_cache": {
                "overview": {
                    "tone": "info",
                    "status_text": "运行中",
                    "summary_text": "共享缓存仓正在维护文件。",
                },
                "current_hour_refresh_overview": {
                    "tone": "info",
                    "status_text": "下载中",
                    "summary_text": "当前小时文件正在下载。",
                },
                "alarm_event_family": {
                    "manual_refresh": {
                        "running": True,
                    }
                },
            },
            "pool": {
                "overview": {
                    "tone": "success",
                    "status_text": "运行中 / 5个楼已登录",
                    "summary_text": "页池正常。",
                    "slots": [],
                }
            },
        },
    )

    actions = {
        item["id"]: item
        for item in payload["status_diagnosis_overview"]["actions"]
    }

    assert actions["refresh_current_hour"]["allowed"] is False
    assert actions["refresh_current_hour"]["pending"] is True
    assert actions["refresh_current_hour"]["disabled_reason"] == "当前小时共享文件正在下载"
    assert actions["refresh_manual_alarm"]["allowed"] is False
    assert actions["refresh_manual_alarm"]["pending"] is True
    assert actions["refresh_manual_alarm"]["disabled_reason"] == "当前正在拉取告警文件"


def test_present_internal_runtime_building_display_prefers_backend_rows():
    payload = present_internal_runtime_building_display(
        {
            "building": "A楼",
            "page_slot": {
                "building": "A楼",
                "status_text": "待命",
                "detail_text": "页签已就绪，等待下载任务",
                "tone": "success",
                "login_text": "已登录",
                "login_tone": "success",
                "status_key": "ready",
            },
            "source_families": {
                "handover_log_family": {"building": "A楼", "status_text": "已就绪", "tone": "success"},
                "handover_capacity_report_family": {"building": "A楼", "status_text": "下载中", "tone": "info"},
                "monthly_report_family": {"building": "A楼", "status_text": "等待中", "tone": "warning"},
                "alarm_event_family": {"building": "A楼", "status_text": "失败", "tone": "danger"},
            },
        }
    )

    assert payload["status_text"] == "待命"
    assert payload["page_slot"]["login_text"] == "已登录"
    assert payload["source_families"]["handover_capacity_report_family"]["status_text"] == "下载中"
    assert payload["families"][0]["title"] == "交接班日志源文件"
    assert payload["items"][0]["value"] == "待命"


def test_present_updater_mirror_overview_marks_python_source_run_as_debug_mode():
    payload = present_updater_mirror_overview(
        {
            "enabled": False,
            "disabled_reason": "source_python_run",
            "local_version": "web-3.0.0",
            "local_release_revision": 216,
        }
    )

    assert payload["tone"] == "info"
    assert payload["status_text"] == "请先 git pull"
    assert payload["badge_text"] == "源码直跑不走应用内更新"
    assert payload["items"][0]["value"] == "源码直跑"
    assert payload["items"][2]["value"] == "先 git pull 再重启"
    assert payload["actions"]["main"]["reason_code"] == "source_python_run"
    assert payload["actions"]["internal_peer_check"]["reason_code"] == "source_python_run"
    assert payload["actions"]["internal_peer_apply"]["reason_code"] == "source_python_run"
    assert payload["business_actions"]["allowed"] is True


def test_present_updater_mirror_overview_prefers_shared_mirror_waiting_text():
    payload = present_updater_mirror_overview(
        {
            "enabled": True,
            "source_kind": "shared_mirror",
            "mirror_ready": False,
            "last_publish_error": "",
            "internal_peer": {
                "available": True,
                "online": False,
            },
        }
    )

    assert payload["status_text"] == "等待外网端发布批准版本"
    assert payload["items"][0]["value"] == "共享目录更新源（不访问互联网）"
    assert payload["business_actions"]["allowed"] is True


def test_present_updater_mirror_overview_marks_remote_publish_ready():
    payload = present_updater_mirror_overview(
        {
            "enabled": True,
            "source_kind": "remote",
            "source_label": "远端正式更新源",
            "mirror_ready": True,
            "mirror_version": "V3.216.20260417",
            "last_publish_at": "2026-04-17 10:00:00",
            "internal_peer": {
                "available": True,
                "online": True,
                "last_check_at": "2026-04-17 10:05:00",
                "update_available": False,
            },
        }
    )

    assert payload["tone"] == "success"
    assert payload["status_text"] == "已发布批准版本到共享目录"
    assert payload["items"][2]["value"] == "V3.216.20260417"


def test_present_updater_mirror_overview_shows_git_mode_items():
    payload = present_updater_mirror_overview(
        {
            "enabled": True,
            "update_mode": "git_pull",
            "source_kind": "git_remote",
            "source_label": "Git 仓库更新源",
            "branch": "master",
            "local_commit": "1111111222222333333",
            "remote_commit": "9999999aaaaaaa",
            "worktree_dirty": True,
            "local_version": "V3.224.20260419",
        }
    )

    assert payload["items"][0]["value"] == "Git 拉取代码"
    assert payload["items"][1]["value"] == "master"
    assert payload["items"][2]["value"] == "1111111"
    assert payload["items"][4]["value"] == "存在本地修改"


def test_present_external_module_hero_overviews_prefers_scheduler_summary_items() -> None:
    payload = present_external_module_hero_overviews(
        scheduler_overview_summary={
            "items": [
                {"label": "已启动调度", "value": "3 项", "tone": "success"},
                {"label": "未启动调度", "value": "4 项", "tone": "warning"},
                {"label": "待关注项", "value": "1 项", "tone": "warning"},
                {"label": "最近即将执行", "value": "交接班日志", "tone": "info"},
            ]
        },
        scheduler_status_summary={},
        review_status={},
        shared_source_cache_overview={},
    )

    metrics = payload["scheduler_overview"]["metrics"]
    assert metrics[0]["label"] == "已启动调度"
    assert metrics[0]["value"] == "3 项"
    assert metrics[3]["label"] == "最近即将执行"


def test_present_external_module_hero_overviews_uses_backend_summaries():
    payload = present_external_module_hero_overviews(
        scheduler_overview_summary={
            "running_count": 3,
            "stopped_count": 4,
            "attention_count": 2,
        },
        scheduler_status_summary={
            "wet_bulb_collection_scheduler": {
                "status": "running",
                "next_run_time": "2026-04-18 12:00:00",
                "display": {"status_text": "运行中"},
            },
            "monthly_event_report_scheduler": {
                "status": "stopped",
                "display": {"status_text": "已停止"},
            },
            "monthly_change_report_scheduler": {
                "status": "running",
                "next_run_time": "2026-05-01 00:00:00",
                "display": {"status_text": "运行中"},
            },
        },
        review_status={
            "duty_date": "2026-04-18",
            "duty_shift": "day",
            "has_any_session": True,
            "required_count": 5,
            "confirmed_count": 3,
            "all_confirmed": False,
        },
        shared_source_cache_overview={
            "families": [
                {
                    "key": "alarm_event_family",
                    "uploadLastRunAt": "2026-04-18 11:30:00",
                    "uploadRecordCount": 123,
                    "uploadFileCount": 5,
                }
            ]
        },
    )

    assert payload["scheduler_overview"]["metrics"][0]["value"] == "3 项"
    assert payload["handover_log"]["metrics"][0]["value"] == "2026-04-18 / 白班"
    assert payload["wet_bulb_collection"]["metrics"][0]["value"] == "运行中"
    assert payload["monthly_event_report"]["metrics"][2]["value"] == "2026-05-01 00:00:00"
    assert payload["alarm_event_upload"]["metrics"][1]["value"] == "123 条"
