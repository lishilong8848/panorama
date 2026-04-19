from pathlib import Path
import sys
import threading
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.modules.report_pipeline.api import routes


class _FakeWetBulbCollectionService:
    def __init__(self, _runtime_cfg):
        pass

    def build_target_descriptor(self, force_refresh=False):
        _ = force_refresh
        return {}


class _FakeDayMetricBitableExportService:
    def __init__(self, _cfg):
        pass

    def build_target_descriptor(self, force_refresh=False):
        _ = force_refresh
        return {
            "configured_app_token": "SOwsw315aiBJjgkl48ccoxIPntc",
            "operation_app_token": "SOwsw315aiBJjgkl48ccoxIPntc",
            "app_token": "SOwsw315aiBJjgkl48ccoxIPntc",
            "table_id": "tblD7hi70s6U6rlU",
            "target_kind": "wiki_token_pair",
            "resolved_from": "wiki_token_pair",
            "display_url": "https://vnet.feishu.cn/wiki/SOwsw315aiBJjgkl48ccoxIPntc?table=tblD7hi70s6U6rlU&view=vewG7OKFEg",
            "bitable_url": "https://vnet.feishu.cn/wiki/SOwsw315aiBJjgkl48ccoxIPntc?table=tblD7hi70s6U6rlU&view=vewG7OKFEg",
            "wiki_node_token": "SOwsw315aiBJjgkl48ccoxIPntc",
            "message": "",
            "resolved_at": "2026-04-03 03:00:00",
        }


class _FakeSharedSourceCacheService:
    def __init__(self, *, runtime_config, store, download_browser_pool=None, emit_log=None):
        _ = runtime_config, store, download_browser_pool, emit_log

    def get_alarm_event_upload_target_preview(self, force_refresh=False):
        _ = force_refresh
        return {
            "configured_app_token": "SOwsw315aiBJjgkl48ccoxIPntc",
            "operation_app_token": "tblx9g4wAppToken",
            "app_token": "tblx9g4wAppToken",
            "table_id": "tblD7hi70s6U6rlU",
            "target_kind": "wiki_token_pair",
            "resolved_from": "wiki_token_pair",
            "display_url": "https://vnet.feishu.cn/wiki/SOwsw315aiBJjgkl48ccoxIPntc?table=tblD7hi70s6U6rlU&view=vewG7OKFEg",
            "bitable_url": "https://vnet.feishu.cn/wiki/SOwsw315aiBJjgkl48ccoxIPntc?table=tblD7hi70s6U6rlU&view=vewG7OKFEg",
            "wiki_node_token": "SOwsw315aiBJjgkl48ccoxIPntc",
            "message": "",
            "resolved_at": "2026-04-03 03:00:00",
        }


class _FakeEventFollowupCacheStore:
    def __init__(self, *args, **kwargs):
        self.state_path = Path('handover_cache.json')

    def load_state(self):
        return {'pending_by_id': {}}


class _FakeReviewSessionService:
    def __init__(self, _cfg):
        pass

    def get_latest_batch_status(self):
        return routes._empty_handover_review_status()

    def get_batch_status_for_duty(self, _duty_date: str, _duty_shift: str):
        return routes._empty_handover_review_status()


class _FakeReviewFollowupTriggerService:
    def __init__(self, _cfg):
        pass

    def get_followup_progress(self, _batch_key: str):
        return routes._empty_followup_progress()


class _ExplodingWetBulbCollectionService:
    def __init__(self, _runtime_cfg):
        pass

    def build_target_descriptor(self, force_refresh=False):
        _ = force_refresh
        raise AssertionError("external health cold path should not block on wet bulb target preview")


class _ExplodingDayMetricBitableExportService:
    def __init__(self, _cfg):
        pass

    def build_target_descriptor(self, force_refresh=False):
        _ = force_refresh
        raise AssertionError("external health cold path should not block on day metric target preview")


class _ExplodingSharedSourceCacheService:
    def __init__(self, *, runtime_config, store, download_browser_pool=None, emit_log=None):
        _ = runtime_config, store, download_browser_pool, emit_log

    def get_alarm_event_upload_target_preview(self, force_refresh=False):
        _ = force_refresh
        raise AssertionError("external health cold path should not block on alarm target preview")


def test_external_health_hides_internal_download_pool_but_keeps_shared_file_readiness_and_alert_projection(monkeypatch, tmp_path):
    monkeypatch.setattr(routes, 'get_app_dir', lambda: tmp_path)
    monkeypatch.setattr(routes, 'WetBulbCollectionService', _FakeWetBulbCollectionService)
    monkeypatch.setattr(routes, 'DayMetricBitableExportService', _FakeDayMetricBitableExportService)
    monkeypatch.setattr(routes, 'SharedSourceCacheService', _FakeSharedSourceCacheService)
    monkeypatch.setattr(routes, 'EventFollowupCacheStore', _FakeEventFollowupCacheStore)
    monkeypatch.setattr(routes, 'load_handover_config', lambda _cfg: {})
    monkeypatch.setattr(routes, 'ReviewSessionService', _FakeReviewSessionService)
    monkeypatch.setattr(routes, 'ReviewFollowupTriggerService', _FakeReviewFollowupTriggerService)
    monkeypatch.setattr(routes, '_build_handover_review_access', lambda *_args, **_kwargs: routes._empty_handover_review_access())

    container = SimpleNamespace(
        version='web-3.0.0',
        config={'version': 3},
        config_path=tmp_path / 'config.json',
        runtime_config={
            'paths': {'runtime_state_root': str(tmp_path / '.runtime')},
            'scheduler': {},
            'download': {'resume': {}},
            'handover_log': {'template': {}, 'event_sections': {'cache': {'state_file': 'handover_cache.json'}}},
            'network': {},
            'wet_bulb_collection': {},
            'shared_bridge': {},
        },
        scheduler=None,
        frontend_mode='source',
        frontend_root=str(tmp_path / 'frontend'),
        frontend_assets_dir=str(tmp_path / 'frontend'),
        wifi_service=None,
        job_service=SimpleNamespace(
            active_job_id=lambda: '',
            active_job_ids=lambda include_waiting=True: [],
            job_counts=lambda: {'queued': 0, 'running': 0, 'finished': 0, 'failed': 0},
        ),
        updater_snapshot=lambda: {},
        handover_scheduler_status=lambda: {'enabled': False, 'running': False, 'status': '未初始化', 'slots': {}, 'state_paths': {}},
        wet_bulb_collection_scheduler_status=lambda: {
            'enabled': False,
            'running': False,
            'status': '未初始化',
            'next_run_time': '',
            'last_check_at': '',
            'last_decision': '',
            'last_trigger_at': '',
            'last_trigger_result': '',
            'state_path': '',
            'state_exists': False,
        },
        scheduler_executor_name=lambda: '-',
        is_scheduler_executor_bound=lambda: False,
        handover_scheduler_executor_name=lambda: '-',
        is_handover_scheduler_executor_bound=lambda: False,
        wet_bulb_collection_scheduler_executor_name=lambda: '-',
        is_wet_bulb_collection_scheduler_executor_bound=lambda: False,
        deployment_snapshot=lambda: {'role_mode': 'external', 'node_id': 'external-node', 'node_label': '外网端'},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {
            'status': 'alias_match',
            'status_text': '路径写法不同但目录一致',
            'tone': 'info',
            'summary_text': '映射盘与 UNC 当前都指向同一共享目录。',
            'items': [
                {'label': '当前角色', 'value': '外网端', 'tone': 'info'},
                {'label': '路径一致性', 'value': '路径写法不同但目录一致', 'tone': 'info'},
            ],
            'paths': [
                {'label': '内网共享目录', 'path': r'\\172.16.1.2\share', 'canonical_path': r'\\172.16.1.2\share'},
                {'label': '外网共享目录', 'path': r'Z:\share', 'canonical_path': r'\\172.16.1.2\share'},
            ],
            'notes': ['当前角色运行值和 updater 实际共享目录都来自后端运行时，不是前端推测值。'],
        },
        shared_bridge_snapshot=lambda mode='external_full': {
            'enabled': True,
            'role_mode': 'external',
            'root_dir': 'Z:/share',
            'internal_download_pool': {
                'enabled': True,
                'browser_ready': True,
                'page_slots': [{'building': 'A楼', 'login_state': 'ready'}],
                'active_buildings': ['A楼'],
                'last_error': 'should be hidden',
            },
            'internal_source_cache': {
                'handover_log_family': {
                    'latest_selection': {
                        'best_bucket_key': '2026-04-01 09',
                        'can_proceed': True,
                    }
                }
            },
            'internal_alert_status': {
                'buildings': [
                    {
                        'building': 'A楼',
                        'status': 'problem',
                        'status_text': '异常',
                        'summary': 'A楼 登录失败，等待内网恢复',
                        'detail': '页面无响应，请检查楼栋页面服务或网络',
                        'last_problem_at': '2026-04-01 09:10:00',
                        'last_recovered_at': '',
                        'active_count': 1,
                    }
                ],
                'active_count': 1,
                'last_notified_at': '2026-04-01 09:20:00',
            },
        },
        system_logs=[],
        get_system_log_entries=lambda **_kwargs: [],
        system_log_next_offset=lambda: 0,
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={
                    "target_preview:day_metric": {
                        "ts": 0.0,
                        "value": _FakeDayMetricBitableExportService({}).build_target_descriptor(),
                        "ready": True,
                        "refreshing": False,
                    },
                    "target_preview:alarm_event": {
                        "ts": 0.0,
                        "value": _FakeSharedSourceCacheService(runtime_config={}, store=None).get_alarm_event_upload_target_preview(),
                        "ready": True,
                        "refreshing": False,
                    },
                    "target_preview:engineer_directory": {
                        "ts": 0.0,
                        "value": {},
                        "ready": True,
                        "refreshing": False,
                    },
                    "target_preview:wet_bulb": {
                        "ts": 0.0,
                        "value": {},
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
                        "value": container.shared_root_diagnostic_snapshot(),
                        "ready": True,
                        "refreshing": False,
                    },
                },
                _health_component_cache_lock=threading.Lock(),
            )
        ),
        url=SimpleNamespace(hostname='127.0.0.1', port=18765),
    )

    payload = routes.health(request)

    assert payload['ok'] is True
    assert payload['deployment']['role_mode'] == 'external'
    assert payload['shared_bridge']['internal_download_pool'] == {
        'enabled': False,
        'browser_ready': False,
        'page_slots': [],
        'active_buildings': [],
        'last_error': '',
    }
    assert payload['shared_bridge']['internal_source_cache']['handover_log_family']['latest_selection']['best_bucket_key'] == '2026-04-01 09'
    assert payload['shared_bridge']['internal_source_cache']['handover_log_family']['latest_selection']['can_proceed'] is True
    assert payload['shared_bridge']['internal_alert_status']['active_count'] == 1
    assert payload['shared_bridge']['internal_alert_status']['buildings'][0]['building'] == 'A楼'
    assert payload['shared_bridge']['internal_alert_status']['buildings'][0]['status'] == 'problem'
    assert payload['shared_root_diagnostic']['status_text'] == '路径写法不同但目录一致'
    assert payload['shared_root_diagnostic']['paths'][1]['path'] == r'Z:\share'
    assert payload['day_metric_upload']['target_preview']['target_kind'] == 'wiki_token_pair'
    assert payload['day_metric_upload']['target_preview']['display_url'].startswith('https://vnet.feishu.cn/wiki/')
    assert payload['alarm_event_upload']['target_preview']['target_kind'] == 'wiki_token_pair'
    assert payload['alarm_event_upload']['target_preview']['display_url'].startswith('https://vnet.feishu.cn/wiki/')


def test_external_health_cold_path_returns_ok_with_empty_optional_previews(monkeypatch, tmp_path):
    monkeypatch.setattr(routes, 'get_app_dir', lambda: tmp_path)
    monkeypatch.setattr(routes, 'WetBulbCollectionService', _ExplodingWetBulbCollectionService)
    monkeypatch.setattr(routes, 'DayMetricBitableExportService', _ExplodingDayMetricBitableExportService)
    monkeypatch.setattr(routes, 'SharedSourceCacheService', _ExplodingSharedSourceCacheService)
    monkeypatch.setattr(routes, 'EventFollowupCacheStore', _FakeEventFollowupCacheStore)
    monkeypatch.setattr(routes, 'load_handover_config', lambda _cfg: {})
    monkeypatch.setattr(routes, 'ReviewSessionService', _FakeReviewSessionService)
    monkeypatch.setattr(routes, 'ReviewFollowupTriggerService', _FakeReviewFollowupTriggerService)
    monkeypatch.setattr(routes, '_build_handover_review_access', lambda *_args, **_kwargs: routes._empty_handover_review_access())

    container = SimpleNamespace(
        version='web-3.0.0',
        config={'version': 3},
        config_path=tmp_path / 'config.json',
        runtime_config={
            'paths': {'runtime_state_root': str(tmp_path / '.runtime')},
            'scheduler': {},
            'download': {'resume': {}},
            'handover_log': {'template': {}, 'event_sections': {'cache': {'state_file': 'handover_cache.json'}}},
            'network': {},
            'wet_bulb_collection': {},
            'shared_bridge': {},
        },
        scheduler=None,
        frontend_mode='source',
        frontend_root=str(tmp_path / 'frontend'),
        frontend_assets_dir=str(tmp_path / 'frontend'),
        wifi_service=None,
        job_service=SimpleNamespace(
            active_job_id=lambda: '',
            active_job_ids=lambda include_waiting=True: [],
            job_counts=lambda: {'queued': 0, 'running': 0, 'finished': 0, 'failed': 0},
        ),
        updater_snapshot=lambda: {},
        handover_scheduler_status=lambda: {'enabled': False, 'running': False, 'status': '未初始化', 'slots': {}, 'state_paths': {}},
        wet_bulb_collection_scheduler_status=lambda: {
            'enabled': False,
            'running': False,
            'status': '未初始化',
            'next_run_time': '',
            'last_check_at': '',
            'last_decision': '',
            'last_trigger_at': '',
            'last_trigger_result': '',
            'state_path': '',
            'state_exists': False,
        },
        scheduler_executor_name=lambda: '-',
        is_scheduler_executor_bound=lambda: False,
        handover_scheduler_executor_name=lambda: '-',
        is_handover_scheduler_executor_bound=lambda: False,
        wet_bulb_collection_scheduler_executor_name=lambda: '-',
        is_wet_bulb_collection_scheduler_executor_bound=lambda: False,
        deployment_snapshot=lambda: {'role_mode': 'external', 'node_id': 'external-node', 'node_label': '外网端'},
        shared_root_diagnostic_snapshot=lambda **_kwargs: {},
        shared_bridge_snapshot=lambda mode='external_full': {
            'enabled': True,
            'role_mode': 'external',
            'root_dir': 'Z:/share',
            'internal_download_pool': {
                'enabled': True,
                'browser_ready': True,
                'page_slots': [{'building': 'A楼', 'login_state': 'ready'}],
                'active_buildings': ['A楼'],
                'last_error': '',
            },
            'internal_source_cache': {},
            'internal_alert_status': {},
        },
        system_logs=[],
        get_system_log_entries=lambda **_kwargs: [],
        system_log_next_offset=lambda: 0,
    )
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                container=container,
                _health_component_cache={},
                _health_component_cache_lock=threading.Lock(),
            )
        ),
        url=SimpleNamespace(hostname='127.0.0.1', port=18765),
    )

    payload = routes.health(request)

    assert payload['ok'] is True
    assert payload['wet_bulb_collection']['target_preview'] == {}
    assert payload['day_metric_upload']['target_preview'] == {}
    assert payload['alarm_event_upload']['target_preview'] == {}
    assert payload['handover']['review_links'] == []
