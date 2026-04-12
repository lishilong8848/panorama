from app.modules.feishu.service.bitable_target_resolver import BitableTargetResolver


def test_resolve_token_pair_preview_prefers_wiki_when_wiki_and_base_both_probe_ok(monkeypatch):
    resolver = BitableTargetResolver(app_id="app_id", app_secret="app_secret")
    calls = []

    monkeypatch.setattr(resolver, "_new_wiki_client", lambda: object())

    def _fake_probe_wiki_node(client, *, node_token):
        _ = client
        calls.append(("wiki", node_token))
        return {
            "ok": True,
            "node": {
                "node_token": node_token,
                "obj_type": "bitable",
                "obj_token": "operation_app_token",
                "space_id": "space_demo",
                "title": "wiki-title",
            },
        }

    def _fake_probe_bitable_fields(client, *, app_token, table_id, context_label):
        _ = client, context_label
        calls.append(("bitable", app_token, table_id))
        return {"ok": True}

    monkeypatch.setattr(resolver, "_probe_wiki_node", _fake_probe_wiki_node)
    monkeypatch.setattr(resolver, "_probe_bitable_fields", _fake_probe_bitable_fields)

    preview = resolver.resolve_token_pair_preview(
        configured_app_token="wiki_node_token",
        table_id="tbl_demo",
        force_refresh=True,
    )

    assert preview["target_kind"] == "wiki_token_pair"
    assert preview["display_url"].startswith("https://vnet.feishu.cn/wiki/wiki_node_token")
    assert calls == [
        ("wiki", "wiki_node_token"),
        ("bitable", "operation_app_token", "tbl_demo"),
    ]


def test_resolve_token_pair_preview_falls_back_to_base_only_when_wiki_probe_fails(monkeypatch):
    resolver = BitableTargetResolver(app_id="app_id", app_secret="app_secret")
    calls = []

    monkeypatch.setattr(resolver, "_new_wiki_client", lambda: object())

    def _fake_probe_wiki_node(client, *, node_token):
        _ = client
        calls.append(("wiki", node_token))
        return {"ok": False, "kind": "invalid", "message": "not wiki"}

    def _fake_probe_bitable_fields(client, *, app_token, table_id, context_label):
        _ = client, context_label
        calls.append(("bitable", app_token, table_id))
        return {"ok": True}

    monkeypatch.setattr(resolver, "_probe_wiki_node", _fake_probe_wiki_node)
    monkeypatch.setattr(resolver, "_probe_bitable_fields", _fake_probe_bitable_fields)

    preview = resolver.resolve_token_pair_preview(
        configured_app_token="base_app_token",
        table_id="tbl_demo",
        force_refresh=True,
    )

    assert preview["target_kind"] == "base_token_pair"
    assert preview["display_url"].startswith("https://vnet.feishu.cn/base/base_app_token")
    assert calls == [
        ("wiki", "base_app_token"),
        ("bitable", "base_app_token", "tbl_demo"),
    ]
