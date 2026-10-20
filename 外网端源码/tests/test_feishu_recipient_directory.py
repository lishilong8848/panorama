from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.modules.feishu.service import feishu_recipient_directory as module
from app.modules.feishu.service.feishu_auth_resolver import configure_feishu_auth_source
from app.modules.feishu.service.im_file_message_client import FeishuImFileMessageClient


class Repository:
    def __init__(self):
        self.rows = {}

    def get_runtime_kv(self, namespace, key):
        return copy.deepcopy(self.rows.get((namespace, key)))

    def put_runtime_kv(self, namespace, key, value):
        self.rows[(namespace, key)] = copy.deepcopy(value)


def record(name="Person", old="ou_old", new="ou_new", **fields):
    return {"fields": {
        "姓名": name, "飞书 open_id": old, "员工姓名": [{"id": new, "name": name}], **fields,
    }}


def request_for(records):
    def request(method, url, **kwargs):
        assert method == "GET"
        assert kwargs["params"]["user_id_type"] == "open_id"
        assert "view_id" not in kwargs["params"]
        return {"data": {"items": records}}
    return request


def resolve(directory, repository, records, receive_id="ou_old", app_id="cli_test"):
    return directory.resolve(
        receive_id, app_id=app_id, app_secret="fake",
        request=request_for(records), repository=repository,
    )


def test_exact_person_maps_old_id_to_current_app_and_survives_restart():
    repo = Repository()
    assert resolve(module.FeishuRecipientDirectory(), repo, [record()]) == "ou_new"
    restarted = module.FeishuRecipientDirectory()
    assert resolve(restarted, repo, [record(new="ou_next_app")], "ou_new", "cli_next") == "ou_next_app"


@pytest.mark.parametrize("rows", [
    [record(), record(new="ou_another_person")],
    [record(**{"离职/异动情况": True})],
    [record(**{"员工姓名": []})],
    [record(**{"员工姓名": [{"id": "ou_new", "name": "Different"}]})],
    [record(), record(**{"员工姓名": []})],
])
def test_ambiguous_inactive_or_missing_person_is_not_sent(rows):
    with pytest.raises(ValueError, match="唯一"):
        resolve(module.FeishuRecipientDirectory(), Repository(), rows)


def test_inactive_namesake_does_not_hide_the_active_person():
    rows = [record(old="ou_retired", new="ou_retired_new", **{"离职/异动情况": True}), record()]
    assert resolve(module.FeishuRecipientDirectory(), Repository(), rows) == "ou_new"


def test_inactive_recipient_does_not_redirect_to_active_namesake():
    repo = Repository()
    assert resolve(module.FeishuRecipientDirectory(), repo, [record()]) == "ou_new"
    rows = [record(**{"离职/异动情况": True}), record(old="ou_other", new="ou_someone_else")]
    with pytest.raises(ValueError, match="唯一"):
        resolve(module.FeishuRecipientDirectory(), repo, rows)


def test_public_migration_preserves_flags_and_does_not_add_h_recipient():
    from app.config.feishu_recipient_defaults import migrate_public_recipient_ids

    config = {"common": {"feishu_auth": {"app_id": module.PUBLIC_ACCOUNT_APP_ID}}, "features": {
        "handover_log": {"review_ui": {"review_link_recipients_by_building": {
            "A楼": [{"open_id": module.PUBLIC_ACCOUNT_IDS["A"][0], "note": "A", "enabled": False}],
            "H楼": [],
        }}},
    }}
    migrate_public_recipient_ids(config)
    rows = config["features"]["handover_log"]["review_ui"]["review_link_recipients_by_building"]
    assert rows["A楼"] == [{"open_id": module.PUBLIC_ACCOUNT_IDS["A"][1], "note": "A", "enabled": False}]
    assert rows["H楼"] == []
    before = copy.deepcopy(config)
    migrate_public_recipient_ids(config)
    assert config == before


def test_public_migration_does_not_override_another_application():
    from app.config.feishu_recipient_defaults import migrate_public_recipient_ids

    config = {"common": {"feishu_auth": {"app_id": "cli_next"}},
              "person": {"open_id": module.PUBLIC_ACCOUNT_IDS["A"][0]}}
    before = copy.deepcopy(config)
    migrate_public_recipient_ids(config)
    assert config == before


def test_missing_and_fuzzy_names_do_not_fallback_to_old_id():
    with pytest.raises(ValueError, match="无法唯一匹配"):
        resolve(module.FeishuRecipientDirectory(), Repository(), [record(old="ou_other")])


def test_exact_configured_name_can_bind_a_recipient_without_legacy_id():
    config = {"features": {"handover_log": {"review_ui": {"review_link_recipients_by_building": {
        "A楼": [{"note": "Person", "open_id": "ou_old", "enabled": True}],
    }}}}}
    configure_feishu_auth_source(lambda: config)
    assert resolve(module.FeishuRecipientDirectory(), Repository(), [record(old="ou_other")]) == "ou_new"


def test_configured_note_cannot_override_a_different_known_person():
    config = {"features": {"handover_log": {"review_ui": {"review_link_recipients_by_building": {
        "A楼": [{"note": "Other", "open_id": "ou_old", "enabled": True}],
    }}}}}
    configure_feishu_auth_source(lambda: config)
    with pytest.raises(ValueError, match="无法唯一匹配"):
        resolve(module.FeishuRecipientDirectory(), Repository(), [
            record(), record(name="Other", old="ou_other", new="ou_other_new"),
        ])


@pytest.mark.parametrize("label", list(module.PUBLIC_ACCOUNT_IDS))
def test_confirmed_public_accounts_are_scoped_to_new_app(label):
    old, new = module.PUBLIC_ACCOUNT_IDS[label]
    directory = module.FeishuRecipientDirectory()
    assert resolve(directory, Repository(), [], old or new, module.PUBLIC_ACCOUNT_APP_ID) == new
    with pytest.raises(RuntimeError, match="另一飞书应用"):
        resolve(directory, Repository(), [], new, "cli_next")


def test_parallel_sends_share_one_directory_fetch():
    directory, repo, calls = module.FeishuRecipientDirectory(), Repository(), []

    def request(*args, **kwargs):
        calls.append(1)
        return request_for([record()])(*args, **kwargs)

    def run(_):
        return directory.resolve("ou_old", app_id="cli_test", app_secret="fake", request=request, repository=repo)

    with ThreadPoolExecutor(max_workers=5) as pool:
        assert list(pool.map(run, range(10))) == ["ou_new"] * 10
    assert len(calls) == 1


def test_directory_failure_is_cooled_down_and_does_not_use_old_id():
    directory = module.FeishuRecipientDirectory()
    calls = []

    def fail(*args, **kwargs):
        calls.append(1)
        raise RuntimeError("offline")

    for text in ("offline", "暂不可用"):
        with pytest.raises(RuntimeError, match=text):
            directory.resolve("ou_old", app_id="cli_test", app_secret="fake", request=fail, repository=Repository())
    assert len(calls) == 1


@pytest.mark.parametrize("kind", ["text", "file", "image"])
def test_all_message_types_resolve_before_post(monkeypatch, kind):
    config = {"common": {"feishu_auth": {"app_id": "cli_test", "app_secret": "fake"}}}
    configure_feishu_auth_source(lambda: config)
    client = FeishuImFileMessageClient(app_id="ignored", app_secret="ignored")
    monkeypatch.setattr(client, "refresh_token", lambda **kwargs: "test-token")
    calls = []
    monkeypatch.setattr(module.feishu_recipient_directory, "resolve", lambda rid, **kwargs: "ou_new")

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"code": 0, "data": {"message_id": "message"}}

    def post(method, url, **kwargs):
        calls.append(kwargs["json"])
        return Response()

    monkeypatch.setattr(client, "_request_with_retry", post)
    params = {"text": "content"} if kind == "text" else {f"{kind}_key": "attachment"}
    getattr(client, f"send_{kind}_message")(receive_id="ou_old", receive_id_type="open_id", **params)
    assert calls[0]["receive_id"] == "ou_new"


@pytest.mark.parametrize("kind", ["chat_id", "user_id", "email"])
def test_non_open_id_receivers_do_not_use_personnel_directory(monkeypatch, kind):
    config = {"common": {"feishu_auth": {"app_id": "cli_test", "app_secret": "fake"}}}
    configure_feishu_auth_source(lambda: config)
    client = FeishuImFileMessageClient(app_id="ignored", app_secret="ignored")
    monkeypatch.setattr(client, "refresh_token", lambda **kwargs: "test-token")
    monkeypatch.setattr(
        module.feishu_recipient_directory, "resolve",
        lambda *args, **kwargs: pytest.fail("non-open-id recipient was remapped"),
    )

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"code": 0, "data": {"message_id": "message"}}

    monkeypatch.setattr(client, "_request_with_retry", lambda *args, **kwargs: Response())
    assert client.send_text_message(receive_id="recipient", receive_id_type=kind, text="test")["message_id"] == "message"


def test_app_change_during_resolution_cannot_send_with_old_identity(monkeypatch):
    config = {"common": {"feishu_auth": {"app_id": "cli_before", "app_secret": "fake"}}}
    configure_feishu_auth_source(lambda: config)
    client = FeishuImFileMessageClient(app_id="ignored", app_secret="ignored")
    seen, sent = [], []

    def token(**kwargs):
        client.app_id = config["common"]["feishu_auth"]["app_id"]
        return client.app_id

    def recipient(rid, **kwargs):
        seen.append(kwargs["app_id"])
        config["common"]["feishu_auth"]["app_id"] = "cli_after"
        return "ou_" + kwargs["app_id"]

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"code": 0, "data": {"message_id": "message"}}

    def post(*args, **kwargs):
        sent.append((kwargs["json"]["receive_id"], kwargs["headers"]["Authorization"]))
        return Response()

    monkeypatch.setattr(client, "refresh_token", token)
    monkeypatch.setattr(module.feishu_recipient_directory, "resolve", recipient)
    monkeypatch.setattr(client, "_request_with_retry", post)
    client.send_text_message(receive_id="ou_old", receive_id_type="open_id", text="test")
    assert seen == ["cli_before", "cli_after"]
    assert sent == [("ou_cli_after", "Bearer cli_after")]

