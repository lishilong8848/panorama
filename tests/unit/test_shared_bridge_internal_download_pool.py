from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import pytest

import app.modules.shared_bridge.service.shared_bridge_runtime_service as runtime_module
from app.modules.shared_bridge.service.internal_download_browser_pool import InternalDownloadBrowserPool
from app.modules.shared_bridge.service.shared_bridge_runtime_service import SharedBridgeRuntimeService



def _make_temp_dir(prefix: str) -> Path:
    root = Path(__file__).resolve().parents[2] / '.tmp_runtime_tests' / 'shared_bridge_internal_download_pool'
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{prefix}{uuid.uuid4().hex}'
    path.mkdir(parents=True, exist_ok=True)
    return path



def _find_slot(snapshot: dict, building: str) -> dict:
    for slot in snapshot.get('page_slots', []):
        if slot.get('building') == building:
            return slot
    raise AssertionError(f'未找到楼栋槽位: {building}')



def test_internal_download_pool_health_snapshot_defaults_login_fields() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})

    snapshot = pool.get_health_snapshot()

    assert snapshot['enabled'] is True
    assert snapshot['browser_ready'] is False
    assert len(snapshot['page_slots']) == 5
    assert snapshot['page_slots'][0]['building'] == 'A楼'
    assert snapshot['page_slots'][0]['login_state'] == 'waiting'
    assert snapshot['page_slots'][0]['last_login_at'] == ''
    assert snapshot['page_slots'][0]['login_error'] == ''
    assert snapshot['page_slots'][0]['slot_age_sec'] == 0
    assert snapshot['page_slots'][0]['jobs_since_recycle'] == 0
    assert snapshot['page_slots'][0]['pending_recycle'] is False



def test_shared_bridge_health_snapshot_contains_internal_download_pool() -> None:
    service = SharedBridgeRuntimeService(
        runtime_config={
            'deployment': {'role_mode': 'internal'},
            'shared_bridge': {'enabled': True, 'root_dir': r'D:\QJPT_Shared'},
        },
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )
    service._internal_download_pool = type(
        '_Pool',
        (),
        {
            'get_health_snapshot': staticmethod(
                lambda: {
                    'enabled': True,
                    'browser_ready': True,
                    'page_slots': [
                        {
                            'building': 'A楼',
                            'page_ready': True,
                            'in_use': False,
                            'login_state': 'ready',
                            'last_login_at': '2026-03-30 10:00:00',
                            'login_error': '',
                        }
                    ],
                    'active_buildings': [],
                    'last_error': '',
                }
            )
        },
    )()

    snapshot = service.get_health_snapshot()

    assert snapshot['internal_download_pool']['enabled'] is True
    assert snapshot['internal_download_pool']['browser_ready'] is True
    assert snapshot['internal_download_pool']['page_slots'][0]['building'] == 'A楼'
    assert snapshot['internal_download_pool']['page_slots'][0]['login_state'] == 'ready'
    assert snapshot['cleanup_deleted_tasks'] == 0
    assert snapshot['cleanup_deleted_entries'] == 0
    assert snapshot['cleanup_deleted_files'] == 0



def test_internal_shared_bridge_start_initializes_browser_pool(monkeypatch) -> None:
    calls = {'start': 0, 'stop': 0}

    class _FakePool:
        def __init__(self, *_args, **_kwargs):
            pass

        def start(self):
            calls['start'] += 1
            return {'started': True, 'running': True, 'reason': 'started'}

        def stop(self):
            calls['stop'] += 1
            return {'stopped': True, 'running': False, 'reason': 'stopped'}

        def get_health_snapshot(self):
            return {
                'enabled': True,
                'browser_ready': True,
                'page_slots': [],
                'active_buildings': [],
                'last_error': '',
            }

    monkeypatch.setattr(runtime_module, 'InternalDownloadBrowserPool', _FakePool)
    shared_root = _make_temp_dir('internal-runtime-')

    service = SharedBridgeRuntimeService(
        runtime_config={
            'deployment': {'role_mode': 'internal'},
            'shared_bridge': {'enabled': True, 'root_dir': str(shared_root)},
        },
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )

    started = service.start()
    stopped = service.stop()

    assert started['running'] is True
    assert stopped['running'] is False
    assert calls['start'] == 1
    assert calls['stop'] == 1



def test_run_building_job_reestablishes_login_after_page_rebuild() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._locks = {'A楼': asyncio.Lock()}
    first_page = object()
    rebuilt_page = object()
    ensure_calls = []
    login_calls = []

    async def _fake_ensure_page(building: str):
        ensure_calls.append(building)
        return first_page if len(ensure_calls) == 1 else rebuilt_page

    async def _fake_ensure_logged_in(building: str, page):  # noqa: ANN001
        login_calls.append((building, page))

    async def _runner(page):  # noqa: ANN001
        assert page is first_page
        return {'status': 'ok'}

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._ensure_logged_in = _fake_ensure_logged_in  # type: ignore[method-assign]

    result = asyncio.run(pool._run_building_job('A楼', _runner))

    assert result == {'status': 'ok'}
    assert login_calls == [('A楼', first_page), ('A楼', rebuilt_page)]
    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert slot['last_result'] == 'success'
    assert slot['last_error'] == ''



def test_run_building_job_failure_marks_current_building_failed_without_relogin() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._locks = {'A楼': asyncio.Lock()}
    first_page = object()
    rebuilt_page = object()
    ensure_calls = []
    login_calls = []

    async def _fake_ensure_page(building: str):
        ensure_calls.append(building)
        return first_page if len(ensure_calls) == 1 else rebuilt_page

    async def _fake_ensure_logged_in(building: str, page):  # noqa: ANN001
        login_calls.append((building, page))

    async def _runner(_page):  # noqa: ANN001
        raise RuntimeError('下载失败')

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._ensure_logged_in = _fake_ensure_logged_in  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match='下载失败'):
        asyncio.run(pool._run_building_job('A楼', _runner))

    assert login_calls == [('A楼', first_page)]
    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert slot['last_result'] == 'failed'
    assert slot['last_error'] == '下载失败'



def test_run_building_job_failure_does_not_pollute_other_building_slots() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._locks = {'A楼': asyncio.Lock()}
    first_page = object()
    rebuilt_page = object()
    ensure_calls = []

    async def _fake_ensure_page(building: str):
        ensure_calls.append(building)
        return first_page if len(ensure_calls) == 1 else rebuilt_page

    async def _fake_ensure_logged_in(_building: str, _page):  # noqa: ANN001
        return None

    async def _runner(_page):  # noqa: ANN001
        raise RuntimeError('A楼下载失败')

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._ensure_logged_in = _fake_ensure_logged_in  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match='A楼下载失败'):
        asyncio.run(pool._run_building_job('A楼', _runner))

    failed_slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    untouched_slot = _find_slot(pool.get_health_snapshot(), 'B楼')
    assert failed_slot['last_result'] == 'failed'
    assert untouched_slot['login_state'] == 'waiting'
    assert untouched_slot['last_result'] == ''
    assert untouched_slot['last_error'] == ''



def test_run_building_alarm_job_uses_api_request_context_and_disposes_it() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._locks = {'A楼': asyncio.Lock()}
    page = object()
    login_calls = []
    api_context_calls = []

    class _FakeApiContext:
        def __init__(self) -> None:
            self.disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    api_context = _FakeApiContext()

    async def _fake_ensure_page(_building: str):
        return page

    async def _fake_ensure_logged_in(building: str, current_page):  # noqa: ANN001
        login_calls.append((building, current_page))

    async def _fake_create_alarm_api_request_context(building: str):
        api_context_calls.append(building)
        return api_context, "http://192.168.1.10"

    async def _runner(current_request_context, base_url: str):  # noqa: ANN001
        assert current_request_context is api_context
        assert base_url == "http://192.168.1.10"
        return {"status": "ok"}

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._ensure_logged_in = _fake_ensure_logged_in  # type: ignore[method-assign]
    pool._create_alarm_api_request_context = _fake_create_alarm_api_request_context  # type: ignore[method-assign]

    result = asyncio.run(pool._run_building_alarm_job('A楼', _runner))

    assert result == {"status": "ok"}
    assert login_calls == [('A楼', page)]
    assert api_context_calls == ['A楼']
    assert api_context.disposed is True
    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert slot['login_state'] == 'ready'
    assert slot['login_error'] == ''
    assert slot['last_result'] == 'success'
    assert slot['last_error'] == ''


def test_classify_failure_kind_treats_etimedout_as_page_timeout() -> None:
    kind = InternalDownloadBrowserPool._classify_failure_kind(
        "APIRequestContext.post: connect ETIMEDOUT 192.168.232.53:80"
    )
    assert kind == 'page_timeout'


def test_run_building_alarm_job_retries_again_after_successful_recovery() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._locks = {'A楼': asyncio.Lock()}
    page = object()
    api_contexts = []
    recovery_calls = []
    attempts = {'runner': 0}

    class _FakeApiContext:
        def __init__(self) -> None:
            self.disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    async def _fake_ensure_page(_building: str):
        return page

    async def _fake_ensure_logged_in(_building: str, _page):  # noqa: ANN001
        return None

    async def _fake_create_alarm_api_request_context(_building: str):
        ctx = _FakeApiContext()
        api_contexts.append(ctx)
        return ctx, "http://192.168.1.10"

    async def _fake_attempt_recovery(_building: str, *, base_reason: str, failure_kind: str, from_probe: bool = False):  # noqa: ANN001
        recovery_calls.append({
            'base_reason': base_reason,
            'failure_kind': failure_kind,
            'from_probe': from_probe,
        })
        return True

    async def _runner(_current_request_context, _base_url: str):  # noqa: ANN001
        attempts['runner'] += 1
        if attempts['runner'] < 3:
            raise RuntimeError("APIRequestContext.post: connect ETIMEDOUT 192.168.232.53:80")
        return {"status": "ok"}

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._ensure_logged_in = _fake_ensure_logged_in  # type: ignore[method-assign]
    pool._create_alarm_api_request_context = _fake_create_alarm_api_request_context  # type: ignore[method-assign]
    pool._attempt_building_recovery = _fake_attempt_recovery  # type: ignore[method-assign]

    result = asyncio.run(pool._run_building_alarm_job('A楼', _runner))

    assert result == {"status": "ok"}
    assert attempts['runner'] == 3
    assert len(recovery_calls) == 2
    assert all(call['failure_kind'] == 'page_timeout' for call in recovery_calls)
    assert len(api_contexts) == 3
    assert all(ctx.disposed is True for ctx in api_contexts)
    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert slot['login_state'] == 'ready'
    assert slot['last_result'] == 'success'


def test_recovery_probe_clears_failed_login_state_when_page_is_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._locks = {'A楼': asyncio.Lock()}
    pool._update_slot(
        'A楼',
        page_ready=True,
        login_state='failed',
        login_error='旧登录失败',
        last_error='旧登录失败',
        last_result='failed',
    )

    async def _fake_ensure_page(_building: str):
        return object()

    async def _fake_probe_existing_login_state(_page) -> str:  # noqa: ANN001
        return 'ready'

    original_sleep = asyncio.sleep
    sleep_calls = {'count': 0}

    async def _fake_sleep(_delay: float) -> None:
        sleep_calls['count'] += 1
        if sleep_calls['count'] >= 2:
            raise asyncio.CancelledError
        await original_sleep(0)

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._probe_existing_login_state = _fake_probe_existing_login_state  # type: ignore[method-assign]
    monkeypatch.setattr(asyncio, 'sleep', _fake_sleep)

    asyncio.run(pool._async_recovery_probe_loop())

    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert slot['login_state'] == 'ready'
    assert slot['login_error'] == ''
    assert slot['last_error'] == ''
    assert slot['last_result'] == 'ready'


def test_external_shared_bridge_start_stops_leftover_internal_pool() -> None:
    calls = {'stop': 0}

    class _LeftoverPool:
        def stop(self):
            calls['stop'] += 1
            return {'stopped': True, 'running': False, 'reason': 'stopped'}

    shared_root = _make_temp_dir('external-runtime-')
    service = SharedBridgeRuntimeService(
        runtime_config={
            'deployment': {'role_mode': 'external'},
            'shared_bridge': {'enabled': True, 'root_dir': str(shared_root)},
        },
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )
    service._internal_download_pool = _LeftoverPool()

    started = service.start()
    stopped = service.stop()

    assert started['running'] is True
    assert stopped['running'] is False
    assert calls['stop'] == 1
    assert service._internal_download_pool is None



def test_ensure_logged_in_preserves_expired_state_for_current_building_only() -> None:
    pool = InternalDownloadBrowserPool(
        runtime_config={
            'internal_source_sites': [
                {
                    'building': 'A楼',
                    'enabled': True,
                    'host': '192.168.1.10',
                    'username': 'admin',
                    'password': 'secret',
                }
            ]
        }
    )

    async def _fake_login_if_needed(building: str, _page, _site):  # noqa: ANN001
        pool._update_slot(building, login_state='expired', login_error='session expired', last_error='session expired')
        raise RuntimeError('A楼 登录态未就绪: session expired')

    pool._login_if_needed = _fake_login_if_needed  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match='session expired'):
        asyncio.run(pool._ensure_logged_in('A楼', object()))

    failed_slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    untouched_slot = _find_slot(pool.get_health_snapshot(), 'B楼')
    assert failed_slot['login_state'] == 'expired'
    assert failed_slot['login_error'] == 'session expired'
    assert untouched_slot['login_state'] == 'waiting'
    assert untouched_slot['last_error'] == ''



def test_ensure_logged_in_without_site_clears_stale_error_state() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._update_slot('A楼', login_state='failed', login_error='old', last_error='old')

    asyncio.run(pool._ensure_logged_in('A楼', object()))

    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert slot['login_state'] == 'waiting'
    assert slot['login_error'] == ''
    assert slot['last_error'] == ''


def test_run_building_job_fails_fast_when_login_state_failed() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._locks = {'A楼': asyncio.Lock()}
    page = object()
    pool._update_slot(
        'A楼',
        login_state='failed',
        login_error='页面无响应，请检查楼栋页面服务或网络',
        last_error='页面无响应，请检查楼栋页面服务或网络',
    )

    async def _fake_ensure_page(_building: str):
        return page

    async def _fake_probe_existing_login_state(_page):  # noqa: ANN001
        return 'login_required'

    async def _fake_ensure_logged_in(_building: str, _page):  # noqa: ANN001
        raise AssertionError('登录失败快断后不应再继续尝试登录')

    async def _runner(_page):  # noqa: ANN001
        raise AssertionError('登录失败快断后不应执行下载逻辑')

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._probe_existing_login_state = _fake_probe_existing_login_state  # type: ignore[method-assign]
    pool._ensure_logged_in = _fake_ensure_logged_in  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match='A楼 登录失败|A楼 登录态未就绪'):
        asyncio.run(pool._run_building_job('A楼', _runner))

    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert slot['last_result'] == 'failed'
    assert 'A楼' in slot['last_error']


def test_attempt_building_recovery_suspends_after_three_failed_attempts() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})

    async def _fake_ensure_page(_building: str):
        return object()

    async def _fail_refresh(_building: str, _page):  # noqa: ANN001
        return False, '页面无响应，请检查楼栋页面服务或网络'

    async def _fail_reopen(_building: str, _page):  # noqa: ANN001
        return False, '页面无响应，请检查楼栋页面服务或网络'

    async def _fail_rebuild(_building: str):
        return False, '页面无响应，请检查楼栋页面服务或网络'

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._try_recovery_refresh = _fail_refresh  # type: ignore[method-assign]
    pool._try_recovery_reopen = _fail_reopen  # type: ignore[method-assign]
    pool._try_recovery_rebuild = _fail_rebuild  # type: ignore[method-assign]

    recovered = asyncio.run(
        pool._attempt_building_recovery(
            'A楼',
            base_reason='页面无响应，请检查楼栋页面服务或网络',
            failure_kind='page_unreachable',
            from_probe=False,
        )
    )

    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert recovered is False
    assert slot['suspended'] is True
    assert slot['failure_kind'] == 'page_unreachable'
    assert slot['recovery_attempts'] == 3
    assert slot['login_state'] == 'failed'
    assert slot['next_probe_at']


def test_run_building_job_fails_fast_when_building_suspended() -> None:
    pool = InternalDownloadBrowserPool(runtime_config={})
    pool._locks = {'A楼': asyncio.Lock()}
    pool._update_slot(
        'A楼',
        suspended=True,
        suspend_reason='A楼 页面无响应: 页面无响应，请检查楼栋页面服务或网络',
        failure_kind='page_unreachable',
        next_probe_at='2026-03-31 23:00:00',
    )

    async def _fake_ensure_page(_building: str):
        return object()

    async def _probe(_page):  # noqa: ANN001
        return 'unknown'

    async def _runner(_page):  # noqa: ANN001
        raise AssertionError('挂起楼栋不应继续执行下载')

    pool._ensure_page = _fake_ensure_page  # type: ignore[method-assign]
    pool._probe_existing_login_state = _probe  # type: ignore[method-assign]

    with pytest.raises(Exception, match='A楼 页面无响应'):
        asyncio.run(pool._run_building_job('A楼', _runner))

    slot = _find_slot(pool.get_health_snapshot(), 'A楼')
    assert slot['suspended'] is True
