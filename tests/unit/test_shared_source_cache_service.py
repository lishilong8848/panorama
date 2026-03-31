from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pytest

import app.modules.shared_bridge.service.shared_source_cache_service as cache_module
from app.modules.shared_bridge.service.shared_bridge_store import SharedBridgeStore
from app.modules.shared_bridge.service.shared_source_cache_service import (
    FAMILY_HANDOVER_LOG,
    FAMILY_MONTHLY_REPORT,
    SharedSourceCacheService,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMP_ROOT = PROJECT_ROOT / '.tmp_runtime_tests' / 'shared_source_cache_service'


@pytest.fixture
def work_dir() -> Path:
    root = TEMP_ROOT / uuid.uuid4().hex
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _build_runtime_config(
    *,
    role_mode: str,
    shared_root: Path | None = None,
    legacy_root: str = '',
    internal_root: str = '',
    external_root: str = '',
) -> dict:
    shared_bridge = {
        'enabled': True,
        'root_dir': legacy_root or (str(shared_root) if shared_root is not None else ''),
    }
    if internal_root:
        shared_bridge['internal_root_dir'] = internal_root
    if external_root:
        shared_bridge['external_root_dir'] = external_root
    return {
        'deployment': {'role_mode': role_mode},
        'shared_bridge': shared_bridge,
        'internal_source_cache': {'enabled': True},
    }


def test_health_snapshot_contains_building_level_current_hour_statuses(work_dir: Path) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='internal', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )
    bucket_key = '2026-03-29 10'
    service._current_hour_bucket = bucket_key

    ready_file = shared_root / '交接班日志源文件' / '202603' / '20260329--10' / '20260329--10--交接班日志源文件--A楼.xlsx'
    ready_file.parent.mkdir(parents=True, exist_ok=True)
    ready_file.write_bytes(b'ready-a')
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building='A楼',
        bucket_kind='latest',
        bucket_key=bucket_key,
        duty_date='',
        duty_shift='',
        downloaded_at='2026-03-29 10:05:00',
        relative_path=str(ready_file.relative_to(shared_root)).replace('\\', '/'),
        status='ready',
        file_hash='hash-a',
        size_bytes=7,
    )
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building='B楼',
        bucket_kind='latest',
        bucket_key=bucket_key,
        duty_date='',
        duty_shift='',
        downloaded_at='2026-03-29 10:06:00',
        relative_path='source_cache/_failed/handover/latest/2026-03-29 10/B楼.failed',
        status='failed',
        file_hash='',
        size_bytes=0,
        metadata={'error': '下载失败'},
    )

    snapshot = service.get_health_snapshot()
    buildings = {item['building']: item for item in snapshot[FAMILY_HANDOVER_LOG]['buildings']}

    assert buildings['A楼']['status'] == 'ready'
    assert buildings['A楼']['ready'] is True
    assert buildings['A楼']['downloaded_at'] == '2026-03-29 10:05:00'
    assert buildings['A楼']['resolved_file_path'] == str(ready_file)
    assert buildings['B楼']['status'] == 'failed'
    assert buildings['B楼']['last_error'] == '下载失败'
    assert buildings['C楼']['status'] == 'waiting'
    assert snapshot[FAMILY_HANDOVER_LOG]['ready_count'] == 1
    assert snapshot[FAMILY_HANDOVER_LOG]['failed_buildings'] == ['B楼']


def test_health_snapshot_does_not_mark_missing_ready_file_as_ready(work_dir: Path) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='internal', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )
    bucket_key = '2026-03-29 11'
    service._current_hour_bucket = bucket_key

    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building='A楼',
        bucket_kind='latest',
        bucket_key=bucket_key,
        duty_date='',
        duty_shift='',
        downloaded_at='2026-03-29 11:03:00',
        relative_path='全景平台月报源文件/202603/20260329--11/缺失文件.xlsx',
        status='ready',
        file_hash='hash-missing',
        size_bytes=18,
    )

    snapshot = service.get_health_snapshot()
    buildings = {item['building']: item for item in snapshot[FAMILY_MONTHLY_REPORT]['buildings']}

    assert buildings['A楼']['status'] == 'waiting'
    assert buildings['A楼']['ready'] is False
    assert snapshot[FAMILY_MONTHLY_REPORT]['ready_count'] == 0


def test_refresh_family_bucket_calls_fill_with_keyword_arguments(work_dir: Path) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='internal', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    calls: list[tuple[str, str, bool]] = []

    def _fake_fill(*, building: str, bucket_key: str, emit_log):  # noqa: ANN001
        calls.append((building, bucket_key, callable(emit_log)))

    service.get_enabled_buildings = lambda: ['A楼']  # type: ignore[method-assign]
    service._refresh_family_bucket(
        source_family=FAMILY_HANDOVER_LOG,
        bucket_key='2026-03-29 23',
        fill_func=_fake_fill,
    )

    assert calls == [('A楼', '2026-03-29 23', True)]


def test_refresh_family_bucket_records_failed_entry_metadata(work_dir: Path) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='internal', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    def _fake_fill(*, building: str, bucket_key: str, emit_log):  # noqa: ANN001, ARG001
        raise RuntimeError(f'{building} 下载异常')

    service.get_enabled_buildings = lambda: ['A楼']  # type: ignore[method-assign]
    service._refresh_family_bucket(
        source_family=FAMILY_HANDOVER_LOG,
        bucket_key='2026-03-29 23',
        fill_func=_fake_fill,
    )

    rows = store.list_source_cache_entries(
        source_family=FAMILY_HANDOVER_LOG,
        building='A楼',
        bucket_kind='latest',
        bucket_key='2026-03-29 23',
        status='failed',
        limit=1,
    )

    assert len(rows) == 1
    assert rows[0]['relative_path'].startswith('source_cache/_failed/')
    assert rows[0]['metadata']['error'] == 'A楼 下载异常'


def test_get_monthly_by_date_entries_ignores_missing_indexed_files(work_dir: Path) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='internal', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building='A楼',
        bucket_kind='date',
        bucket_key='2026-03-29',
        duty_date='2026-03-29',
        duty_shift='',
        downloaded_at='2026-03-29 23:59:00',
        relative_path='全景平台月报源文件/202603/20260329--月报/20260329--月报--A楼.xlsx',
        status='ready',
        file_hash='missing-hash',
        size_bytes=100,
    )

    entries = service.get_monthly_by_date_entries(selected_dates=['2026-03-29'], buildings=['A楼'])

    assert entries == []


def test_get_monthly_by_date_entries_ignores_inaccessible_indexed_files(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='internal', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    actual_file = shared_root / '全景平台月报源文件' / '202603' / '20260329--月报' / '20260329--月报--A楼.xlsx'
    actual_file.parent.mkdir(parents=True, exist_ok=True)
    actual_file.write_bytes(b'monthly-a')
    relative_path = actual_file.relative_to(shared_root).as_posix()
    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building='A楼',
        bucket_kind='date',
        bucket_key='2026-03-29',
        duty_date='2026-03-29',
        duty_shift='',
        downloaded_at='2026-03-29 23:59:00',
        relative_path=relative_path,
        status='ready',
        file_hash='hash-a',
        size_bytes=9,
    )

    monkeypatch.setattr(cache_module, 'is_accessible_cached_file_path', lambda _path: False)

    entries = service.get_monthly_by_date_entries(selected_dates=['2026-03-29'], buildings=['A楼'])

    assert entries == []


def test_external_health_snapshot_resolves_file_path_from_external_root_dir(work_dir: Path) -> None:
    internal_root = work_dir / 'internal-share'
    external_root = work_dir / 'external-share'
    store = SharedBridgeStore(external_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(
            role_mode='external',
            legacy_root=str(work_dir / 'legacy-share'),
            internal_root=str(internal_root),
            external_root=str(external_root),
        ),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )
    bucket_key = '2026-03-30 08'
    service._current_hour_bucket = bucket_key

    actual_file = external_root / '全景平台月报源文件' / '202603' / '20260330--08' / '20260330--08--全景平台月报源文件-A楼.xlsx'
    actual_file.parent.mkdir(parents=True, exist_ok=True)
    actual_file.write_bytes(b'external-health')
    relative_path = actual_file.relative_to(external_root).as_posix()
    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building='A楼',
        bucket_kind='latest',
        bucket_key=bucket_key,
        duty_date='',
        duty_shift='',
        downloaded_at='2026-03-30 08:11:00',
        relative_path=relative_path,
        status='ready',
        file_hash='hash-health',
        size_bytes=len(b'external-health'),
    )

    snapshot = service.get_health_snapshot()
    buildings = {item['building']: item for item in snapshot[FAMILY_MONTHLY_REPORT]['buildings']}

    assert buildings['A楼']['status'] == 'ready'
    assert buildings['A楼']['resolved_file_path'] == str(actual_file)


def test_external_health_snapshot_resolves_failed_file_path_from_external_root_dir(work_dir: Path) -> None:
    internal_root = work_dir / 'internal-share'
    external_root = work_dir / 'external-share'
    store = SharedBridgeStore(external_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(
            role_mode='external',
            legacy_root=str(work_dir / 'legacy-share'),
            internal_root=str(internal_root),
            external_root=str(external_root),
        ),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )
    bucket_key = '2026-03-30 08'
    service._current_hour_bucket = bucket_key

    relative_path = '全景平台月报源文件/202603/20260330--08/20260330--08--全景平台月报源文件-A楼.xlsx'
    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building='A楼',
        bucket_kind='latest',
        bucket_key=bucket_key,
        duty_date='',
        duty_shift='',
        downloaded_at='2026-03-30 08:11:00',
        relative_path=relative_path,
        status='failed',
        file_hash='',
        size_bytes=0,
        metadata={'error': '共享目录不可访问'},
    )

    snapshot = service.get_health_snapshot()
    buildings = {item['building']: item for item in snapshot[FAMILY_MONTHLY_REPORT]['buildings']}

    assert buildings['A楼']['status'] == 'failed'
    assert buildings['A楼']['resolved_file_path'] == str(external_root / relative_path)
    assert buildings['A楼']['last_error'] == '共享目录不可访问'


def test_get_latest_ready_selection_allows_fallback_within_three_buckets(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='external', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    latest_a = shared_root / '全景平台月报源文件' / '202603' / '20260330--08' / '20260330--08--全景平台月报源文件--A楼.xlsx'
    latest_b = shared_root / '全景平台月报源文件' / '202603' / '20260330--07' / '20260330--07--全景平台月报源文件--B楼.xlsx'
    latest_a.parent.mkdir(parents=True, exist_ok=True)
    latest_b.parent.mkdir(parents=True, exist_ok=True)
    latest_a.write_bytes(b'a')
    latest_b.write_bytes(b'b')
    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building='A楼',
        bucket_kind='latest',
        bucket_key='2026-03-30 08',
        duty_date='2026-03-30',
        duty_shift='',
        downloaded_at='2026-03-30 08:01:00',
        relative_path=latest_a.relative_to(shared_root).as_posix(),
        status='ready',
        file_hash='hash-a',
        size_bytes=1,
    )
    store.upsert_source_cache_entry(
        source_family=FAMILY_MONTHLY_REPORT,
        building='B楼',
        bucket_kind='latest',
        bucket_key='2026-03-30 07',
        duty_date='2026-03-30',
        duty_shift='',
        downloaded_at='2026-03-30 07:30:00',
        relative_path=latest_b.relative_to(shared_root).as_posix(),
        status='ready',
        file_hash='hash-b',
        size_bytes=1,
    )
    monkeypatch.setattr(
        cache_module,
        '_now_dt',
        lambda: cache_module.datetime(2026, 3, 30, 8, 30, 0),
    )

    selection = service.get_latest_ready_selection(
        source_family=FAMILY_MONTHLY_REPORT,
        buildings=['A楼', 'B楼'],
        max_version_gap=3,
    )

    assert selection['can_proceed'] is True
    assert selection['best_bucket_key'] == '2026-03-30 08'
    assert selection['fallback_buildings'] == ['B楼']
    assert selection['missing_buildings'] == []
    assert selection['stale_buildings'] == []
    building_rows = {item['building']: item for item in selection['buildings']}
    assert building_rows['A楼']['status'] == 'ready'
    assert building_rows['A楼']['using_fallback'] is False
    assert building_rows['B楼']['status'] == 'ready'
    assert building_rows['B楼']['using_fallback'] is True
    assert building_rows['B楼']['version_gap'] == 1


def test_get_latest_ready_selection_blocks_stale_building_over_three_buckets(work_dir: Path) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='external', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    latest_a = shared_root / '交接班日志源文件' / '202603' / '20260330--08' / '20260330--08--交接班日志源文件--A楼.xlsx'
    stale_b = shared_root / '交接班日志源文件' / '202603' / '20260330--04' / '20260330--04--交接班日志源文件--B楼.xlsx'
    latest_a.parent.mkdir(parents=True, exist_ok=True)
    stale_b.parent.mkdir(parents=True, exist_ok=True)
    latest_a.write_bytes(b'a')
    stale_b.write_bytes(b'b')
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building='A楼',
        bucket_kind='latest',
        bucket_key='2026-03-30 08',
        duty_date='2026-03-30',
        duty_shift='day',
        downloaded_at='2026-03-30 08:05:00',
        relative_path=latest_a.relative_to(shared_root).as_posix(),
        status='ready',
        file_hash='hash-a',
        size_bytes=1,
    )
    store.upsert_source_cache_entry(
        source_family=FAMILY_HANDOVER_LOG,
        building='B楼',
        bucket_kind='latest',
        bucket_key='2026-03-30 04',
        duty_date='2026-03-30',
        duty_shift='night',
        downloaded_at='2026-03-30 04:05:00',
        relative_path=stale_b.relative_to(shared_root).as_posix(),
        status='ready',
        file_hash='hash-b',
        size_bytes=1,
    )

    selection = service.get_latest_ready_selection(
        source_family=FAMILY_HANDOVER_LOG,
        buildings=['A楼', 'B楼'],
        max_version_gap=3,
    )

    assert selection['can_proceed'] is False
    assert selection['best_bucket_key'] == '2026-03-30 08'
    assert selection['selected_entries'][0]['building'] == 'A楼'
    assert selection['stale_buildings'] == ['B楼']
    building_rows = {item['building']: item for item in selection['buildings']}
    assert building_rows['B楼']['status'] == 'stale'
    assert building_rows['B楼']['version_gap'] == 4


def test_get_latest_ready_selection_blocks_best_bucket_older_than_three_hours(
    monkeypatch: pytest.MonkeyPatch,
    work_dir: Path,
) -> None:
    shared_root = work_dir / 'shared'
    store = SharedBridgeStore(shared_root)
    store.ensure_ready()
    service = SharedSourceCacheService(
        runtime_config=_build_runtime_config(role_mode='external', shared_root=shared_root),
        store=store,
        emit_log=lambda *_args, **_kwargs: None,
    )

    latest_a = shared_root / '全景平台月报源文件' / '202603' / '20260330--08' / '20260330--08--全景平台月报源文件--A楼.xlsx'
    latest_b = shared_root / '全景平台月报源文件' / '202603' / '20260330--08' / '20260330--08--全景平台月报源文件--B楼.xlsx'
    latest_a.parent.mkdir(parents=True, exist_ok=True)
    latest_b.parent.mkdir(parents=True, exist_ok=True)
    latest_a.write_bytes(b'a')
    latest_b.write_bytes(b'b')
    for building, target in (('A楼', latest_a), ('B楼', latest_b)):
        store.upsert_source_cache_entry(
            source_family=FAMILY_MONTHLY_REPORT,
            building=building,
            bucket_kind='latest',
            bucket_key='2026-03-30 08',
            duty_date='2026-03-30',
            duty_shift='',
            downloaded_at='2026-03-30 08:05:00',
            relative_path=target.relative_to(shared_root).as_posix(),
            status='ready',
            file_hash=f'hash-{building}',
            size_bytes=1,
        )

    monkeypatch.setattr(cache_module, '_now_dt', lambda: cache_module.datetime(2026, 3, 30, 12, 30, 0))

    selection = service.get_latest_ready_selection(
        source_family=FAMILY_MONTHLY_REPORT,
        buildings=['A楼', 'B楼'],
        max_version_gap=3,
        max_selection_age_hours=3.0,
    )

    assert selection['best_bucket_key'] == '2026-03-30 08'
    assert selection['best_bucket_age_hours'] == 4.5
    assert selection['is_best_bucket_too_old'] is True
    assert selection['can_proceed'] is False
    assert selection['stale_buildings'] == []
    assert selection['missing_buildings'] == []
