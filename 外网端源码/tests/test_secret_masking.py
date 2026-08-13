from app.config.secret_masking import mask_settings, merge_masked_values


def test_task_webhook_is_masked_and_preserved_on_save():
    original = {"common": {"notify": {"task_failure_webhook_url": "https://example.test/secret-hook"}}}
    masked = mask_settings(original)

    assert "secret-hook" not in masked["common"]["notify"]["task_failure_webhook_url"]
    assert merge_masked_values(masked, original) == original
