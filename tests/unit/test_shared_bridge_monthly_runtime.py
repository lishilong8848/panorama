from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path

from app.modules.shared_bridge.service import shared_bridge_runtime_service as runtime_module



def _make_temp_dir(prefix: str) -> Path:
    root = Path(__file__).resolve().parents[2] / '.tmp_runtime_tests' / 'shared_bridge_monthly_runtime'
    root.mkdir(parents=True, exist_ok=True)
    path = root / f'{prefix}{uuid.uuid4().hex}'
    path.mkdir(parents=True, exist_ok=True)
    return path



def _runtime_config(shared_root: Path, role_mode: str) -> dict:
    return {
        'deployment': {
            'role_mode': role_mode,
            'node_id': f'{role_mode}-node',
            'node_label': role_mode,
        },
        'shared_bridge': {
            'enabled': True,
            'root_dir': str(shared_root),
            'poll_interval_sec': 1,
            'heartbeat_interval_sec': 1,
            'claim_lease_sec': 30,
            'stale_task_timeout_sec': 1800,
            'artifact_retention_days': 7,
            'sqlite_busy_timeout_ms': 5000,
        },
    }



def test_monthly_internal_stage_moves_task_to_ready_for_external(monkeypatch) -> None:
    shared_root = _make_temp_dir('monthly-internal-')

    def _fake_internal_runner(*_args, **kwargs):  # noqa: ANN002, ANN003
        task_id = str(kwargs.get('task_id', '')).strip() or 'unknown'
        source_root = runtime_module.resolve_monthly_bridge_source_root(shared_root, task_id)
        return {
            'status': 'ok',
            'run_id': f'run-{task_id}',
            'run_save_dir': str(source_root),
            'pending_upload_count': 2,
            'file_items': [{'building': 'A楼', 'file_path': str(source_root / 'A.xlsx')}],
        }

    monkeypatch.setattr(runtime_module, 'run_bridge_download_only_auto_once', _fake_internal_runner)

    service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(shared_root, 'internal'),
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = service.create_monthly_auto_once_task(requested_by='manual', source='manual')
    claimed = service._store.claim_next_task(role_target='internal', node_id='internal-node', lease_sec=30)
    assert claimed is not None

    service._run_monthly_internal_download(claimed)

    updated = service.get_task(task['task_id'])
    assert updated is not None
    assert updated['status'] == 'ready_for_external'
    assert updated['result']['internal']['run_id'] == f"run-{task['task_id']}"
    assert any(str(item.get('artifact_kind', '')).strip() == 'resume_state' for item in updated['artifacts'])



def test_monthly_external_resume_stage_completes_success(monkeypatch) -> None:
    shared_root = _make_temp_dir('monthly-external-success-')

    def _fake_external_runner(*_args, **kwargs):  # noqa: ANN002, ANN003
        return {
            'status': 'ok',
            'run_id': kwargs.get('run_id'),
            'pending_upload_count': 0,
            'upload_success_count': 3,
            'upload_failed_count': 0,
        }

    monkeypatch.setattr(runtime_module, 'run_bridge_resume_upload', _fake_external_runner)

    service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(shared_root, 'external'),
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = service.create_monthly_resume_upload_task(run_id='run-ext-1', auto_trigger=False, requested_by='manual')
    claimed = service._store.claim_next_task(role_target='external', node_id='external-node', lease_sec=30)
    assert claimed is not None

    service._run_monthly_external_resume(claimed)

    updated = service.get_task(task['task_id'])
    assert updated is not None
    assert updated['status'] == 'success'
    assert updated['result']['bridge_task_id'] == task['task_id']
    assert updated['result']['external']['status'] == 'ok'



def test_monthly_external_resume_stage_marks_partial_failed_and_sets_task_error(monkeypatch) -> None:
    shared_root = _make_temp_dir('monthly-external-partial-')

    def _fake_external_runner(*_args, **kwargs):  # noqa: ANN002, ANN003
        return {
            'status': 'partial_failed',
            'run_id': kwargs.get('run_id'),
            'pending_upload_count': 1,
            'upload_success_count': 2,
            'upload_failed_count': 1,
            'last_error': 'upload failed',
        }

    monkeypatch.setattr(runtime_module, 'run_bridge_resume_upload', _fake_external_runner)

    service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(shared_root, 'external'),
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )
    task = service.create_monthly_resume_upload_task(run_id='run-ext-2', auto_trigger=False, requested_by='manual')
    claimed = service._store.claim_next_task(role_target='external', node_id='external-node', lease_sec=30)
    assert claimed is not None

    service._run_monthly_external_resume(claimed)

    updated = service.get_task(task['task_id'])
    assert updated is not None
    assert updated['status'] == 'partial_failed'
    assert updated['error'] == 'upload failed'
    assert updated['result']['external']['status'] == 'partial_failed'



def test_process_one_task_marks_unknown_feature_failed() -> None:
    shared_root = _make_temp_dir('monthly-unknown-feature-')
    service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(shared_root, 'internal'),
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )
    assert service._store is not None
    service._store.ensure_ready()

    with service._store.connect() as conn:
        conn.execute(
            """
            INSERT INTO bridge_tasks(
                task_id, feature, mode, created_by_role, created_by_node_id, requested_by,
                status, dedupe_key, request_json, result_json, error, created_at, updated_at, revision
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                'task-unknown-feature',
                'unknown_feature',
                '',
                'internal',
                'internal-node',
                'manual',
                'queued_for_internal',
                '',
                '{}',
                '{}',
                '',
                '2026-03-30 00:00:00',
                '2026-03-30 00:00:00',
            ),
        )
        conn.execute(
            """
            INSERT INTO bridge_stages(
                task_id, stage_id, role_target, handler, status, input_json, result_json,
                claimed_by_node_id, claim_token, lease_expires_at, started_at, finished_at, error, revision
            ) VALUES(?, ?, ?, ?, ?, ?, ?, '', '', '', '', '', '', 0)
            """,
            (
                'task-unknown-feature',
                'internal_download',
                'internal',
                'unknown_internal_handler',
                'pending',
                '{}',
                '{}',
            ),
        )

    service._process_one_task_if_needed()

    updated = service.get_task('task-unknown-feature')
    assert updated is not None
    assert updated['status'] == 'failed'
    assert '共享桥接未识别或不支持的任务类型' in updated['error']
    stage = updated['stages'][0]
    assert stage['status'] == 'failed'
    assert '共享桥接未识别或不支持的任务类型' in stage['error']
    event = next(item for item in updated['events'] if item['event_type'] == 'unsupported_feature')
    payload = event['payload']
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert '共享桥接未识别或不支持的任务类型' in str(payload.get('message', '') or '')



def test_loop_recovers_after_transient_sqlite_locked() -> None:
    shared_root = _make_temp_dir('monthly-sqlite-locked-')
    service = runtime_module.SharedBridgeRuntimeService(
        runtime_config=_runtime_config(shared_root, 'external'),
        app_version='test',
        emit_log=lambda *_args, **_kwargs: None,
    )

    class _FakeStore:
        def __init__(self) -> None:
            self.ensure_calls = 0

        def ensure_ready(self) -> None:
            self.ensure_calls += 1
            if self.ensure_calls == 1:
                raise sqlite3.OperationalError('database is locked')

        def upsert_node(self, **_kwargs) -> None:  # noqa: ANN003
            return None

        def claim_next_task(self, **_kwargs):  # noqa: ANN003
            return None

        def get_task_counts(self):
            return {'pending_internal': 0, 'pending_external': 0, 'problematic': 0, 'total_count': 0, 'node_count': 1}

    class _LoopStopEvent:
        def __init__(self, iterations: int) -> None:
            self.remaining = iterations

        def is_set(self) -> bool:
            return self.remaining <= 0

        def wait(self, _timeout: float) -> bool:
            self.remaining -= 1
            return self.remaining <= 0

    service._store = _FakeStore()
    service._stop_event = _LoopStopEvent(iterations=2)
    service._loop()

    assert service._db_status == 'ok'
    assert service._last_error == ''
    assert service._last_poll_at
