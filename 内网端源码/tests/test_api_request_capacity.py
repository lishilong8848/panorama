from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event

import pytest

from app.modules.shared_bridge.service.api_request_capacity import api_request_slot


def test_five_buildings_can_collect_concurrently():
    barrier = Barrier(5, timeout=3)

    def collect():
        with api_request_slot(timeout_sec=1):
            barrier.wait()

    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(lambda _: collect(), range(5)))


def test_capacity_is_bounded_and_released_after_failure():
    barrier = Barrier(11, timeout=3)
    release = Event()

    def hold():
        with api_request_slot(timeout_sec=1):
            barrier.wait()
            release.wait(timeout=5)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(hold) for _ in range(10)]
        barrier.wait()
        try:
            with pytest.raises(RuntimeError, match="并发繁忙"):
                with api_request_slot(timeout_sec=0.01):
                    pass
        finally:
            release.set()
        for future in futures:
            future.result()
    with pytest.raises(ValueError):
        with api_request_slot(timeout_sec=1):
            raise ValueError("test")
    with api_request_slot(timeout_sec=1):
        pass
