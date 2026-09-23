from app.modules.handover_review.api.routes import _prune_review_cache_locked


def test_review_cache_is_bounded_and_expired_entries_are_removed():
    cache = {
        "expired": {"updated_at": 1.0},
        **{f"item-{index}": {"updated_at": 100.0 + index} for index in range(5)},
    }

    _prune_review_cache_locked(cache, now=200.0, ttl_sec=150.0, max_entries=3)

    assert list(cache) == ["item-2", "item-3", "item-4"]
