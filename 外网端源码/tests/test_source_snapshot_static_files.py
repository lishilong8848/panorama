from pathlib import Path
from types import SimpleNamespace

from app.modules.updater.service.update_applier import (
    SOURCE_SNAPSHOT_SCOPE_CODE_FRONTEND_STATIC,
    _is_allowed_snapshot_member,
    _is_managed_snapshot_target,
)
from app.modules.updater.service.updater_service import UpdaterService, _is_source_snapshot_relpath


def test_station_h_focus_template_and_dependency_lock_are_packaged() -> None:
    for raw_path in ("值班关注点模板.xlsx", "runtime_dependency_lock.json"):
        path = Path(raw_path)
        assert _is_source_snapshot_relpath(path) is True
        assert _is_allowed_snapshot_member(
            path,
            scope=SOURCE_SNAPSHOT_SCOPE_CODE_FRONTEND_STATIC,
        ) is True


def test_root_static_files_are_not_pruned_when_older_snapshot_omits_them() -> None:
    for raw_path in ("值班关注点模板.xlsx", "runtime_dependency_lock.json"):
        assert _is_managed_snapshot_target(
            Path(raw_path),
            scope=SOURCE_SNAPSHOT_SCOPE_CODE_FRONTEND_STATIC,
        ) is False


def test_unrelated_root_workbooks_are_not_accepted_by_source_snapshot() -> None:
    path = Path("用户数据.xlsx")
    assert _is_source_snapshot_relpath(path) is False
    assert _is_allowed_snapshot_member(
        path,
        scope=SOURCE_SNAPSHOT_SCOPE_CODE_FRONTEND_STATIC,
    ) is False


def test_git_snapshot_collection_requests_required_root_static_files(tmp_path) -> None:
    tracked_files = (
        "module.py",
        "web/frontend/dist/index.html",
        "runtime_dependency_lock.json",
        "值班关注点模板.xlsx",
    )
    for raw_path in tracked_files:
        path = tmp_path / raw_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"snapshot-test")

    requested: list[str] = []
    service = object.__new__(UpdaterService)
    service.update_mode = "git_pull"
    service.git_available = True
    service.git_repo_detected = True
    service.app_dir = tmp_path

    def _run_git(*args: str):
        requested.extend(args)
        return SimpleNamespace(returncode=0, stdout="\n".join(tracked_files), stderr="")

    service._run_git = _run_git
    collected = service._git_tracked_source_snapshot_files()

    assert "runtime_dependency_lock.json" in requested
    assert "值班关注点模板.xlsx" in requested
    assert {path.as_posix() for path in collected} == set(tracked_files)
