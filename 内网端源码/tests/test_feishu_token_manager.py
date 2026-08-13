from app.modules.feishu.service.feishu_token_manager import FeishuTokenManager


def test_token_manager_reuses_cached_token(monkeypatch):
    manager = FeishuTokenManager()
    calls = []
    monkeypatch.setattr(
        manager,
        "_fetch_token",
        lambda **kwargs: calls.append(kwargs) or "token-1",
    )

    first = manager.get_token(app_id="id", app_secret="secret", timeout=30)
    second = manager.get_token(app_id="id", app_secret="secret", timeout=30)

    assert first == second == "token-1"
    assert len(calls) == 1


def test_force_refresh_replaces_token_for_all_callers(monkeypatch):
    manager = FeishuTokenManager()
    tokens = iter(("token-1", "token-2"))
    monkeypatch.setattr(manager, "_fetch_token", lambda **_kwargs: next(tokens))

    manager.get_token(app_id="id", app_secret="secret", timeout=30)
    refreshed = manager.get_token(app_id="id", app_secret="secret", timeout=30, force_refresh=True)
    reused = manager.get_token(app_id="id", app_secret="secret", timeout=30)

    assert refreshed == reused == "token-2"
