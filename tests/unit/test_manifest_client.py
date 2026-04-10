from __future__ import annotations

import hashlib
from pathlib import Path

from app.modules.updater.service.manifest_client import (
    ManifestClient,
    SharedMirrorManifestClient,
    SharedMirrorPendingError,
)
from app.modules.updater.service import manifest_client as manifest_client_module


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int = 1024 * 1024):
        yield self._body


def test_download_patch_retries_with_cache_bust_after_sha_mismatch(tmp_path, monkeypatch) -> None:
    old_body = b"old-patch"
    new_body = b"new-patch"
    expected_sha = hashlib.sha256(new_body).hexdigest()
    requested_urls: list[str] = []
    responses = [old_body, new_body]

    def fake_get(url: str, timeout: int, stream: bool = False, headers=None):  # noqa: ANN001
        requested_urls.append(url)
        body = responses.pop(0)
        return _FakeResponse(body)

    monkeypatch.setattr(manifest_client_module.requests, "get", fake_get)
    monkeypatch.setattr(manifest_client_module.time, "sleep", lambda _sec: None)

    client = ManifestClient(
        repo_url="https://gitee.com/example/repo",
        branch="master",
        manifest_path="updates/latest_patch.json",
        retry_count=2,
    )
    save_to = tmp_path / "patch.zip"

    result = client.download_patch(
        "https://gitee.com/example/repo/raw/master/updates/patches/QJPT_patch_only.zip",
        save_to,
        expected_sha256=expected_sha,
    )

    assert result == save_to
    assert save_to.read_bytes() == new_body
    assert len(requested_urls) == 2
    assert requested_urls[0] == "https://gitee.com/example/repo/raw/master/updates/patches/QJPT_patch_only.zip"
    assert "_cb=" in requested_urls[1]


def test_shared_mirror_pending_before_publish(tmp_path) -> None:
    client = SharedMirrorManifestClient(tmp_path / "shared")

    try:
        client.fetch_latest_manifest()
    except SharedMirrorPendingError:
        pass
    else:
        raise AssertionError("expected SharedMirrorPendingError")


def test_shared_mirror_manifest_without_publish_state_is_still_pending(tmp_path) -> None:
    shared_root = tmp_path / "shared"
    client = SharedMirrorManifestClient(shared_root)
    approved_root = shared_root / "updater" / "approved"
    approved_root.mkdir(parents=True, exist_ok=True)
    patch_name = "QJPT_patch_only_p99_r99.zip"
    patch_path = approved_root / patch_name
    patch_path.write_bytes(b"approved-patch")
    manifest_path = approved_root / "latest_patch.json"
    manifest_path.write_text(
        (
            "{\n"
            '  "target_version": "QJPT_V3",\n'
            '  "target_display_version": "V3.99.20260328",\n'
            '  "target_release_revision": 99,\n'
            f'  "zip_relpath": "{patch_name}",\n'
            f'  "zip_size": {patch_path.stat().st_size}\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    try:
        client.fetch_latest_manifest()
    except SharedMirrorPendingError:
        pass
    else:
        raise AssertionError("expected SharedMirrorPendingError")


def test_shared_mirror_publish_state_manifest_mismatch_is_pending(tmp_path) -> None:
    shared_root = tmp_path / "shared"
    client = SharedMirrorManifestClient(shared_root)
    patch_zip = tmp_path / "QJPT_patch_only_p99_r99.zip"
    patch_zip.write_bytes(b"approved-patch")
    expected_sha = hashlib.sha256(b"approved-patch").hexdigest()

    client.publish_approved_update(
        remote_manifest={
            "target_version": "QJPT_V3",
            "target_display_version": "V3.99.20260328",
            "target_release_revision": 99,
            "zip_url": "https://example.invalid/updates/patches/QJPT_patch_only_p99_r99.zip",
            "zip_sha256": expected_sha,
        },
        patch_zip=patch_zip,
        expected_sha256=expected_sha,
        published_by_role="external",
        published_by_node_id="external-node",
        approved_local_version="V3.99.20260328",
        approved_release_revision=99,
    )

    client.manifest_path.write_text(
        client.manifest_path.read_text(encoding="utf-8").replace(
            '"zip_relpath": "QJPT_patch_only_p99_r99.zip"',
            '"zip_relpath": "QJPT_patch_only_p100_r100.zip"',
        ),
        encoding="utf-8",
    )

    try:
        client.fetch_latest_manifest()
    except SharedMirrorPendingError:
        pass
    else:
        raise AssertionError("expected SharedMirrorPendingError")


def test_shared_mirror_publish_fetch_and_download(tmp_path) -> None:
    shared_root = tmp_path / "shared"
    client = SharedMirrorManifestClient(shared_root)
    patch_zip = tmp_path / "QJPT_patch_only_p99_r99.zip"
    patch_zip.write_bytes(b"approved-patch")
    expected_sha = hashlib.sha256(b"approved-patch").hexdigest()

    publish_result = client.publish_approved_update(
        remote_manifest={
            "target_version": "QJPT_V3",
            "target_display_version": "V3.99.20260328",
            "target_release_revision": 99,
            "zip_url": "https://example.invalid/updates/patches/QJPT_patch_only_p99_r99.zip",
            "zip_sha256": expected_sha,
        },
        patch_zip=patch_zip,
        expected_sha256=expected_sha,
        published_by_role="external",
        published_by_node_id="external-node",
        approved_local_version="V3.99.20260328",
        approved_release_revision=99,
    )

    manifest = client.fetch_latest_manifest()
    runtime = client.get_runtime_snapshot()
    downloaded = client.download_patch(manifest["zip_relpath"], tmp_path / "downloaded.zip", expected_sha256=expected_sha)

    assert Path(publish_result["manifest_path"]).exists()
    assert manifest["zip_relpath"] == "QJPT_patch_only_p99_r99.zip"
    assert runtime["mirror_ready"] is True
    assert runtime["mirror_version"] == "V3.99.20260328"
    assert Path(downloaded).read_bytes() == b"approved-patch"
