from app.modules.shared_bridge.service.bridge_status_presenter import (
    apply_external_source_cache_backfill_overlays,
    present_alarm_event_family,
    present_current_hour_refresh_overview,
    present_external_source_cache_family,
    present_internal_page_slot,
    present_external_source_cache_overview,
    present_source_cache_family,
    present_source_cache_building_row,
)


def test_present_source_cache_building_row_allows_manual_refresh_when_ready():
    row = present_source_cache_building_row(
        {
            "building": "A楼",
            "status": "ready",
            "ready": True,
            "downloaded_at": "2026-04-17 10:22:08",
            "resolved_file_path": r"C:\share\a.xlsx",
        },
        building="A楼",
        fallback_bucket="2026-04-17 10",
    )

    assert row["status_key"] == "ready"
    assert row["status_text"] == "已就绪"
    assert row["actions"]["refresh"]["allowed"] is True
    assert row["actions"]["refresh"]["pending"] is False
    assert row["actions"]["refresh"]["label"] == "重新拉取"
    assert row["actions"]["refresh"]["disabled_reason"] == ""
    assert row["reason_code"] == "ready"
    assert row["meta_lines"][0] == "时间桶：2026-04-17 10"
    assert any("最近成功：" in line for line in row["meta_lines"])


def test_present_source_cache_building_row_marks_refresh_pending_when_downloading():
    row = present_source_cache_building_row(
        {
            "building": "B楼",
            "status": "downloading",
            "started_at": "2026-04-17 10:22:08",
        },
        building="B楼",
        fallback_bucket="2026-04-17 10",
    )

    assert row["status_key"] == "downloading"
    assert row["status_text"] == "下载中"
    assert row["actions"]["refresh"]["allowed"] is False
    assert row["actions"]["refresh"]["pending"] is True
    assert row["actions"]["refresh"]["label"] == "拉取中..."
    assert row["actions"]["refresh"]["disabled_reason"] == "当前楼栋正在下载共享文件"


def test_present_source_cache_family_preserves_manual_refresh_payload():
    family = present_source_cache_family(
        {
            "key": "alarm_event_family",
            "title": "告警信息源文件",
            "current_bucket": "2026-04-17 10",
            "buildings": [
                {
                    "building": "A楼",
                    "bucket_key": "2026-04-17 10",
                    "status": "ready",
                    "ready": True,
                    "downloaded_at": "2026-04-17 10:12:00",
                }
            ],
            "manual_refresh": {
                "running": True,
                "bucket_key": "manual/2026-04-17 10:00:00",
                "total_row_count": "15",
                "building_row_counts": {"A楼": 3},
            },
        },
        title="告警信息源文件",
        fallback_bucket="2026-04-17 10",
    )

    assert family["manual_refresh"]["running"] is True
    assert family["key"] == "alarm_event_family"
    assert family["title"] == "告警信息源文件"
    assert family["manual_refresh"]["bucket_key"] == "manual/2026-04-17 10:00:00"
    assert family["manual_refresh"]["total_row_count"] == 15
    assert family["manual_refresh"]["building_row_counts"] == {"A楼": 3}
    assert family["reason_code"] == "ready"
    assert family["detail_text"] == "告警信息源文件当前已全部就绪。"
    assert family["meta_lines"][0] == "本小时桶：2026-04-17 10"
    assert family["items"][1]["label"] == "已就绪楼栋"


def test_present_source_cache_family_sets_source_family_on_rows():
    family = present_source_cache_family(
        {
            "buildings": [
                {
                    "building": "A楼",
                    "status": "ready",
                    "ready": True,
                }
            ],
        },
        key="handover_log_family",
        title="交接班日志源文件",
        fallback_bucket="2026-04-17 10",
    )

    assert family["key"] == "handover_log_family"
    assert family["title"] == "交接班日志源文件"
    assert family["buildings"][0]["source_family"] == "handover_log_family"


def test_present_internal_page_slot_marks_login_ready_as_ready():
    slot = present_internal_page_slot(
        {
            "building": "A楼",
            "browser_ready": True,
            "page_ready": False,
            "in_use": False,
            "login_state": "ready",
        }
    )

    assert slot["status_key"] == "ready"
    assert slot["status_text"] == "待命"
    assert slot["login_text"] == "已登录"
    assert slot["tone"] == "success"


def test_present_external_source_cache_family_emits_meta_lines():
    family = present_external_source_cache_family(
        key="handover_log_family",
        title="交接班日志源文件",
        live_payload={
            "current_bucket": "2026-04-17 10",
            "buildings": [
                {
                    "building": "A楼",
                    "bucket_key": "2026-04-17 10",
                    "status": "ready",
                    "ready": True,
                    "downloaded_at": "2026-04-17 10:22:08",
                    "resolved_file_path": r"C:\share\a.xlsx",
                }
            ],
        },
        latest_payload={
            "best_bucket_key": "2026-04-17 10",
            "can_proceed": True,
            "buildings": [
                {
                    "building": "A楼",
                    "bucket_key": "2026-04-17 10",
                    "status": "ready",
                }
            ],
        },
    )

    assert family["meta_lines"][0] == "最新时间桶：2026-04-17 10"
    assert family["items"][0]["label"] == "最新时间桶"
    assert family["items"][0]["value"] == "2026-04-17 10"
    assert family["items"][1]["label"] == "已就绪楼栋"
    assert family["items"][1]["value"] == "1/1"
    building = family["buildings"][0]
    assert building["meta_lines"][0] == "时间桶：2026-04-17 10"
    assert any("最近成功：" in line for line in building["meta_lines"])
    assert any("共享路径：" in line for line in building["meta_lines"])


def test_present_alarm_event_family_emits_meta_lines():
    family = present_alarm_event_family(
        {
            "selection_reference_date": "2026-04-17",
            "external_upload": {
                "last_run_at": "2026-04-17 12:00:00",
                "uploaded_record_count": 20,
                "uploaded_file_count": 5,
            },
            "buildings": [
                {
                    "building": "A楼",
                    "status": "ready",
                    "source_kind": "latest",
                    "selection_scope": "today",
                    "selected_downloaded_at": "2026-04-17 11:58:00",
                    "resolved_file_path": r"C:\share\a.json",
                }
            ],
        },
        key="alarm_event_family",
        title="告警信息源文件",
    )

    assert family["meta_lines"][0] == "选择策略：当天最新一份，缺失则回退昨天最新"
    assert family["items"][0]["label"] == "当天最新"
    assert family["items"][0]["value"] == "1/5 楼"
    assert family["items"][1]["label"] == "昨天回退"
    assert family["items"][1]["value"] == "0/5 楼"
    assert any("参考日期：2026-04-17" == line for line in family["meta_lines"])
    building = family["buildings"][0]
    assert any("来源：定时" == line for line in building["meta_lines"])
    assert any("选择：今天最新" == line for line in building["meta_lines"])
    assert any("共享路径：" in line for line in building["meta_lines"])


def test_present_external_source_cache_overview_emits_backend_items():
    overview = present_external_source_cache_overview(
        {
            "handover_log_family": {
                "current_bucket": "2026-04-17 10",
                "buildings": [
                    {"building": "A楼", "bucket_key": "2026-04-17 10", "status": "ready", "ready": True}
                ],
                "latest_selection": {
                    "best_bucket_key": "2026-04-17 10",
                    "can_proceed": True,
                    "buildings": [
                        {"building": "A楼", "bucket_key": "2026-04-17 10", "status": "ready"}
                    ],
                },
            },
            "monthly_report_family": {
                "current_bucket": "2026-04-17 10",
                "buildings": [
                    {"building": "A楼", "bucket_key": "2026-04-17 10", "status": "ready", "ready": True}
                ],
                "latest_selection": {
                    "best_bucket_key": "2026-04-17 10",
                    "can_proceed": True,
                    "buildings": [
                        {"building": "A楼", "bucket_key": "2026-04-17 10", "status": "ready"}
                    ],
                },
            },
            "handover_capacity_report_family": {},
            "alarm_event_family": {},
        }
    )

    assert overview["items"][0]["label"] == "主流程判断"
    assert overview["items"][1]["label"] == "显示文件类型"
    assert overview["items"][2]["label"] == "共享参考标识"


def test_apply_external_source_cache_backfill_overlays_marks_handover_families():
    overview = {
        "families": [
            {
                "key": "handover_log_family",
                "title": "交接班日志源文件",
                "buildings": [
                    {"building": "A楼", "status_key": "waiting", "meta_lines": ["时间桶：2026-04-17 10"]},
                    {"building": "B楼", "status_key": "ready", "meta_lines": ["时间桶：2026-04-17 10"]},
                ],
            },
            {
                "key": "handover_capacity_report_family",
                "title": "交接班容量报表源文件",
                "buildings": [
                    {"building": "A楼", "status_key": "waiting", "meta_lines": ["时间桶：2026-04-17 10"]},
                ],
            },
            {
                "key": "alarm_event_family",
                "title": "告警信息源文件",
                "buildings": [
                    {"building": "A楼", "status_key": "waiting", "meta_lines": ["来源：定时"]},
                ],
            },
        ],
    }
    tasks = [
        {
            "task_id": "bridge-task-1",
            "feature": "handover_cache_fill",
            "feature_label": "交接班历史共享文件补采",
            "status": "ready_for_external",
            "current_stage_name": "共享缓存补采",
            "current_stage_status": "waiting_next_side",
            "request": {
                "continuation_kind": "handover",
                "buildings": ["A楼"],
                "duty_date": "2026-04-17",
                "duty_shift": "day",
            },
            "events": [
                {"event_type": "waiting_source_sync", "event_text": "等待内网补采同步"},
            ],
        }
    ]

    presented = apply_external_source_cache_backfill_overlays(overview, tasks)

    handover_log_family = presented["families"][0]
    assert handover_log_family["backfill_running"] is True
    assert handover_log_family["status_text"] == "补采中"
    assert handover_log_family["backfill_label"] == "当前补采"
    assert any("当前补采：" in line for line in handover_log_family["meta_lines"])

    a_building = handover_log_family["buildings"][0]
    b_building = handover_log_family["buildings"][1]
    assert a_building["backfill_running"] is True
    assert a_building["status_text"] == "补采中"
    assert a_building["backfill_scope_text"] == "2026-04-17 / 白班"
    assert any("补采范围：" in line for line in a_building["meta_lines"])
    assert b_building["backfill_running"] is False

    capacity_family = presented["families"][1]
    assert capacity_family["backfill_running"] is True
    assert capacity_family["status_text"] == "补采中"

    alarm_family = presented["families"][2]
    assert alarm_family.get("backfill_running") is None


def test_apply_external_source_cache_backfill_overlays_marks_monthly_family_syncing():
    overview = {
        "families": [
            {
                "key": "monthly_report_family",
                "title": "全景平台月报源文件",
                "buildings": [
                    {"building": "A楼", "status_key": "waiting", "meta_lines": ["日期文件：2026-04-17"]},
                ],
            },
        ],
    }
    tasks = [
        {
            "task_id": "bridge-task-2",
            "feature": "monthly_cache_fill",
            "feature_label": "月报历史共享文件补采",
            "status": "external_running",
            "current_stage_name": "共享缓存同步",
            "current_stage_status": "running",
            "request": {
                "selected_dates": ["2026-04-16", "2026-04-17"],
            },
        }
    ]

    presented = apply_external_source_cache_backfill_overlays(overview, tasks)
    family = presented["families"][0]
    building = family["buildings"][0]

    assert family["backfill_running"] is True
    assert family["status_text"] == "同步中"
    assert family["backfill_label"] == "当前同步"
    assert family["backfill_scope_label"] == "同步日期"
    assert building["backfill_running"] is True
    assert building["status_text"] == "同步中"
    assert building["backfill_scope_text"] == "2026-04-16 / 2026-04-17"


def test_present_current_hour_refresh_overview_returns_backend_display_payload():
    payload = present_current_hour_refresh_overview(
        {
            "running": True,
            "last_run_at": "2026-04-18 10:00:00",
            "running_buildings": ["A楼", "B楼"],
            "failed_buildings": [],
        }
    )

    assert payload["reason_code"] == "running"
    assert payload["status_text"] == "下载中"
    assert payload["items"][2]["value"] == "A楼 / B楼"
    assert payload["actions"]["refresh_current_hour"]["allowed"] is False
    assert payload["actions"]["refresh_current_hour"]["pending"] is True
