from __future__ import annotations

from app.modules.updater.core.versioning import compare_versions, normalize_local_version, normalize_remote_version


def test_compare_versions_uses_release_revision_after_major_and_patch() -> None:
    local = normalize_local_version(
        {
            "major_version": 3,
            "patch_version": 48,
            "release_revision": 10,
            "display_version": "V3.48.20260326",
        }
    )
    remote = normalize_remote_version(
        {
            "major_version": 3,
            "target_patch_version": 48,
            "target_release_revision": 11,
            "target_display_version": "V3.48.20260326",
        }
    )

    assert compare_versions(local, remote) == -1


def test_compare_versions_keeps_ahead_of_remote_when_local_revision_newer() -> None:
    local = normalize_local_version({"major_version": 3, "patch_version": 48, "release_revision": 12})
    remote = normalize_remote_version({"major_version": 3, "target_patch_version": 48, "target_release_revision": 11})

    assert compare_versions(local, remote) == 1
