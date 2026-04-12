from __future__ import annotations

import uuid
from pathlib import Path

import app.modules.shared_bridge.service.shared_bridge_runtime_service as runtime_module
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService


def _make_temp_dir(prefix: str) -> Path:
    root = Path(__file__).resolve().parents[2] / '.tmp_runtime_tests' / 'shared_bridge_internal_alert_runtime'
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{prefix}{uuid.uuid4().hex}'
    path.mkdir(parents=True, exist_ok=True)
    return path


def test_internal_runtime_creates_problem_alert_task_after_due_window() -> None:
    shared_root = _make_temp_dir('internal-alert-')
    service = SharedBridgeRuntimeService(
        runtime_config={
            'deployment': {'role_mode': 'internal'},
            'shared_bridge': {'enabled': True, 'root_dir': str(shared_root)},
        },
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )

    created: list[dict] = []
    marked: list[tuple[str, str]] = []

    class _FakeStore:
        @staticmethod
        def ensure_ready() -> None:
            return None

        @staticmethod
        def upsert_internal_issue_alert(**_kwargs):
            return None

        @staticmethod
        def list_active_internal_issue_alerts():
            return []

        @staticmethod
        def clear_internal_issue_alert(_building: str, _failure_kind: str, *, observed_at: str = '') -> None:
            return None

        @staticmethod
        def list_due_internal_issue_alerts(*, quiet_window_sec: int = 600, dedupe_window_sec: int = 3600):
            assert quiet_window_sec == 600
            assert dedupe_window_sec == 3600
            return [
                {
                    'alert_key': 'A楼|login_failed',
                    'building': 'A楼',
                    'failure_kind': 'login_failed',
                    'status_key': 'login_failed',
                    'summary': 'A楼 登录失败，等待内网恢复',
                    'latest_detail': '页面无响应，请检查楼栋页面服务或网络',
                    'first_seen_at': '2026-03-31 22:00:00',
                    'last_seen_at': '2026-03-31 22:12:00',
                    'occurrence_count': 2,
                    'active': True,
                }
            ]

        @staticmethod
        def list_due_internal_issue_recoveries():
            return []

        @staticmethod
        def find_active_task_by_dedupe_key(_dedupe_key: str):
            return None

        @staticmethod
        def create_internal_browser_alert_task(**kwargs):
            created.append(kwargs)
            return {'task_id': 'alert-task-1'}

        @staticmethod
        def mark_internal_issue_alert_pushed(alert_key: str, *, task_id: str = '', pushed_at: str = '') -> None:
            marked.append((alert_key, task_id))

        @staticmethod
        def mark_internal_issue_alert_recovery_pushed(alert_key: str, *, task_id: str = '', pushed_at: str = '') -> None:
            raise AssertionError('recovery should not be pushed in this case')

    class _FakePool:
        @staticmethod
        def get_health_snapshot():
            return {
                'page_slots': [
                    {
                        'building': 'A楼',
                        'suspended': True,
                        'failure_kind': 'login_failed',
                        'pending_issue_summary': 'A楼 登录失败，等待内网恢复',
                        'suspend_reason': 'A楼 登录失败，等待内网恢复',
                        'login_error': '页面无响应，请检查楼栋页面服务或网络',
                        'last_error': '页面无响应，请检查楼栋页面服务或网络',
                        'last_failure_at': '2026-03-31 22:00:00',
                        'next_probe_at': '2026-03-31 22:13:00',
                    }
                ]
            }

    service._store = _FakeStore()
    service._internal_download_pool = _FakePool()

    service._process_internal_browser_alerts()

    assert len(created) == 1
    assert created[0]['building'] == 'A楼'
    assert created[0]['failure_kind'] == 'login_failed'
    assert created[0]['alert_state'] == 'problem'
    assert created[0]['status_key'] == 'login_failed'
    assert marked == [('A楼|login_failed', 'alert-task-1')]


def test_internal_runtime_creates_recovery_alert_immediately_after_clear() -> None:
    shared_root = _make_temp_dir('internal-alert-recovery-')
    service = SharedBridgeRuntimeService(
        runtime_config={
            'deployment': {'role_mode': 'internal'},
            'shared_bridge': {'enabled': True, 'root_dir': str(shared_root)},
        },
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )

    created: list[dict] = []
    recovery_marked: list[tuple[str, str]] = []

    class _FakeStore:
        @staticmethod
        def ensure_ready() -> None:
            return None

        @staticmethod
        def upsert_internal_issue_alert(**_kwargs):
            return None

        @staticmethod
        def list_active_internal_issue_alerts():
            return [
                {
                    'alert_key': 'A楼|login_failed',
                    'building': 'A楼',
                    'failure_kind': 'login_failed',
                }
            ]

        @staticmethod
        def clear_internal_issue_alert(_building: str, _failure_kind: str, *, observed_at: str = '') -> None:
            return None

        @staticmethod
        def list_due_internal_issue_alerts(*, quiet_window_sec: int = 600, dedupe_window_sec: int = 3600):
            return []

        @staticmethod
        def list_due_internal_issue_recoveries():
            return [
                {
                    'alert_key': 'A楼|login_failed',
                    'building': 'A楼',
                    'failure_kind': 'login_failed',
                    'summary': 'A楼 登录失败，等待内网恢复',
                    'latest_detail': '页面无响应，请检查楼栋页面服务或网络',
                    'first_seen_at': '2026-03-31 22:00:00',
                    'last_seen_at': '2026-03-31 22:12:00',
                    'resolved_at': '2026-03-31 22:20:00',
                    'occurrence_count': 2,
                }
            ]

        @staticmethod
        def find_active_task_by_dedupe_key(_dedupe_key: str):
            return None

        @staticmethod
        def create_internal_browser_alert_task(**kwargs):
            created.append(kwargs)
            return {'task_id': 'alert-task-recovered'}

        @staticmethod
        def mark_internal_issue_alert_pushed(_alert_key: str, *, task_id: str = '', pushed_at: str = '') -> None:
            raise AssertionError('problem alert should not be pushed in this case')

        @staticmethod
        def mark_internal_issue_alert_recovery_pushed(alert_key: str, *, task_id: str = '', pushed_at: str = '') -> None:
            recovery_marked.append((alert_key, task_id))

    class _FakePool:
        @staticmethod
        def get_health_snapshot():
            return {'page_slots': []}

    service._store = _FakeStore()
    service._internal_download_pool = _FakePool()

    service._process_internal_browser_alerts()

    assert len(created) == 1
    assert created[0]['alert_state'] == 'recovered'
    assert created[0]['status_key'] == 'healthy'
    assert created[0]['building'] == 'A楼'
    assert recovery_marked == [('A楼|login_failed', 'alert-task-recovered')]


def test_external_runtime_sends_internal_browser_alert_via_webhook_and_updates_projection(monkeypatch) -> None:
    shared_root = _make_temp_dir('external-alert-')
    service = SharedBridgeRuntimeService(
        runtime_config={
            'deployment': {'role_mode': 'external'},
            'shared_bridge': {'enabled': True, 'root_dir': str(shared_root)},
            'notify': {'enable_webhook': True, 'feishu_webhook_url': 'https://example.invalid'},
        },
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )

    sent: list[dict] = []
    completed: list[dict] = []
    projections: list[dict] = []

    class _FakeNotifyService:
        def __init__(self, _config):
            pass

        def send_failure(self, stage, detail, building=None, emit_log=None, category='upload'):  # noqa: ANN001
            sent.append(
                {
                    'stage': stage,
                    'detail': detail,
                    'building': building,
                    'category': category,
                }
            )

    class _FakeStore:
        @staticmethod
        def upsert_external_alert_projection(**kwargs):
            projections.append(kwargs)
            return kwargs

        @staticmethod
        def complete_stage(**kwargs):
            completed.append(kwargs)
            return True

    monkeypatch.setattr(runtime_module, 'WebhookNotifyService', _FakeNotifyService)

    service._store = _FakeStore()
    task = {
        'task_id': 'alert-task-1',
        'request': {
            'building': 'A楼',
            'failure_kind': 'login_failed',
            'alert_state': 'problem',
            'status_key': 'login_failed',
            'summary': 'A楼 登录失败，等待内网恢复',
            'latest_detail': '页面无响应，请检查楼栋页面服务或网络',
            'first_seen_at': '2026-03-31 22:00:00',
            'last_seen_at': '2026-03-31 22:12:00',
            'resolved_at': '',
            'occurrence_count': 2,
            'still_unresolved': True,
        },
        'stages': [{'stage_id': 'external_notify', 'claim_token': 'token-1'}],
    }

    service._run_internal_browser_alert_external(task)

    assert len(sent) == 1
    assert sent[0]['stage'] == '内网环境告警'
    assert sent[0]['building'] == 'A楼'
    assert sent[0]['category'] == 'download'
    assert '楼栋：A楼' in sent[0]['detail']
    assert projections[0]['building'] == 'A楼'
    assert projections[0]['alert_state'] == 'problem'
    assert projections[0]['status_key'] == 'login_failed'
    assert completed[0]['next_task_status'] == 'success'
