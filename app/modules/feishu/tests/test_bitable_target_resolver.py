from __future__ import annotations

from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver


def _build_resolver() -> BitableTargetResolver:
    return BitableTargetResolver(app_id="app_id", app_secret="app_secret")


def test_resolve_token_pair_preview_detects_base_when_wiki_invalid(monkeypatch) -> None:
    resolver = _build_resolver()
    fake_client = object()
    monkeypatch.setattr(resolver, "_new_wiki_client", lambda: fake_client)
    monkeypatch.setattr(
        resolver,
        "_probe_bitable_fields",
        lambda client, *, app_token, table_id, context_label: {"ok": True},
    )
    monkeypatch.setattr(
        resolver,
        "_probe_wiki_node",
        lambda client, *, node_token: {
            "ok": False,
            "kind": "invalid",
            "message": "Wiki 节点探测失败: not found",
        },
    )

    preview = resolver.resolve_token_pair_preview(
        configured_app_token="app_demo",
        table_id="tbl_demo",
        force_refresh=True,
    )

    assert preview["target_kind"] == "base_token_pair"
    assert preview["configured_app_token"] == "app_demo"
    assert preview["operation_app_token"] == "app_demo"
    assert preview["display_url"] == "https://vnet.feishu.cn/base/app_demo?table=tbl_demo"


def test_resolve_token_pair_preview_prefers_wiki_when_both_are_accessible(monkeypatch) -> None:
    resolver = _build_resolver()
    fake_client = object()
    calls = []
    monkeypatch.setattr(resolver, "_new_wiki_client", lambda: fake_client)

    def _fake_probe_bitable_fields(client, *, app_token, table_id, context_label):
        calls.append((app_token, table_id, context_label))
        return {"ok": True}

    monkeypatch.setattr(resolver, "_probe_bitable_fields", _fake_probe_bitable_fields)
    monkeypatch.setattr(
        resolver,
        "_probe_wiki_node",
        lambda client, *, node_token: {
            "ok": True,
            "node": {
                "node_token": node_token,
                "obj_type": "bitable",
                "obj_token": "resolved_bitable_app",
                "space_id": "space",
                "title": "demo",
            },
        },
    )

    preview = resolver.resolve_token_pair_preview(
        configured_app_token="wiki_node_token",
        table_id="tbl_demo",
        force_refresh=True,
    )

    assert preview["target_kind"] == "wiki_token_pair"
    assert preview["configured_app_token"] == "wiki_node_token"
    assert preview["operation_app_token"] == "resolved_bitable_app"
    assert preview["display_url"] == "https://vnet.feishu.cn/wiki/wiki_node_token?table=tbl_demo"
    assert calls == [
        ("wiki_node_token", "tbl_demo", "Base 多维表"),
        ("resolved_bitable_app", "tbl_demo", "Wiki 对应多维表"),
    ]


def test_resolve_token_pair_preview_returns_invalid_when_base_and_wiki_both_fail(monkeypatch) -> None:
    resolver = _build_resolver()
    fake_client = object()
    monkeypatch.setattr(resolver, "_new_wiki_client", lambda: fake_client)
    monkeypatch.setattr(
        resolver,
        "_probe_bitable_fields",
        lambda client, *, app_token, table_id, context_label: {
            "ok": False,
            "kind": "invalid",
            "message": "Base 多维表探测失败: not found",
        },
    )
    monkeypatch.setattr(
        resolver,
        "_probe_wiki_node",
        lambda client, *, node_token: {
            "ok": False,
            "kind": "invalid",
            "message": "Wiki 节点探测失败: not found",
        },
    )

    preview = resolver.resolve_token_pair_preview(
        configured_app_token="bad_token",
        table_id="tbl_demo",
        force_refresh=True,
    )

    assert preview["target_kind"] == "invalid"
    assert "Wiki 节点探测失败" in preview["message"]


def test_resolve_token_pair_preview_returns_probe_error_when_no_success_and_probe_errors_exist(monkeypatch) -> None:
    resolver = _build_resolver()
    fake_client = object()
    monkeypatch.setattr(resolver, "_new_wiki_client", lambda: fake_client)
    monkeypatch.setattr(
        resolver,
        "_probe_bitable_fields",
        lambda client, *, app_token, table_id, context_label: {
            "ok": False,
            "kind": "probe_error",
            "message": "Base 多维表探测失败: timeout",
        },
    )
    monkeypatch.setattr(
        resolver,
        "_probe_wiki_node",
        lambda client, *, node_token: {
            "ok": False,
            "kind": "invalid",
            "message": "Wiki 节点探测失败: not found",
        },
    )

    preview = resolver.resolve_token_pair_preview(
        configured_app_token="timeout_token",
        table_id="tbl_demo",
        force_refresh=True,
    )

    assert preview["target_kind"] == "probe_error"
    assert preview["message"] == "Base 多维表探测失败: timeout"
