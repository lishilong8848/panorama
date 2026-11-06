"""Isolated concurrency checks: no application imports, network or business writes."""
import ast
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
import threading
import time
from typing import Any, Callable, Dict, Iterator, List


def load_node(path, name, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    node = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name)
    if name == "HandoverOrchestrator":
        node.body = [member for member in node.body if isinstance(member, ast.FunctionDef) and member.name == "_run_building_jobs"]
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace[name]


def main():
    root = Path(__file__).resolve().parents[1] / "handover_log_module" / "service"
    namespace = dict(globals())
    queue_class = load_node(root / "capacity_report_image_delivery_service.py", "CapacityReportImageRenderQueue", namespace)
    queue = queue_class()
    timed_out = threading.Event()

    def expire():
        try:
            with queue.acquire(timeout_sec=0.03, emit_log=lambda _: None, context="expired"):
                raise AssertionError("queue entered while occupied")
        except TimeoutError:
            timed_out.set()

    with queue.acquire(timeout_sec=1, emit_log=lambda _: None, context="first"):
        thread = threading.Thread(target=expire)
        thread.start()
        thread.join(2)
        assert timed_out.is_set() and not thread.is_alive()
    with queue.acquire(timeout_sec=1, emit_log=lambda _: None, context="next"):
        pass
    assert not queue._abandoned_tickets
    try:
        with queue.acquire(timeout_sec=1, emit_log=lambda _: None, context="render-error"):
            raise ValueError("fake render error")
    except ValueError:
        pass
    with queue.acquire(timeout_sec=1, emit_log=lambda _: None, context="after-error"):
        pass

    barrier = threading.Barrier(5, timeout=3)
    seen = []
    class Worker:
        def __init__(self, config):
            self.config = config

        def run_from_existing_file(self, **kwargs):
            barrier.wait()
            kwargs["shared"].append(kwargs["building"])
            seen.append(kwargs["building"])
            if kwargs["building"] == "C楼":
                raise ValueError("isolated failure")
            return {"building": kwargs["building"]}

    class Summary:
        def __init__(self, **kwargs):
            self.results = []

        def to_dict(self):
            return {"results": self.results, "failed_count": self.failed_count}

    namespace.update(_GENERATION_SLOTS=threading.BoundedSemaphore(5),
                     _GENERATION_LOCKS={b: threading.Lock() for b in ("A楼", "B楼", "C楼", "D楼", "E楼")},
                     RunSummary=Summary, BuildingResult=lambda **kwargs: kwargs)
    orchestrator = load_node(root / "handover_orchestrator.py", "HandoverOrchestrator", namespace)
    namespace["HandoverOrchestrator"] = Worker
    instance = orchestrator()
    instance.config = {}
    shared = []
    jobs = [{"building": b, "data_file": "unused", "shared": shared} for b in ("A楼", "B楼", "C楼", "D楼", "E楼")]
    results = list(instance._run_building_jobs(jobs, lambda _: None))
    assert len(seen) == 5 and shared == []
    assert [row[0] for row in results] == [job["building"] for job in jobs]
    assert results[2][2]["failed_count"] == 1
    assert results[4][2]["building"] == "E楼"
    cache_path = root.parent / "repository" / "event_followup_cache_store.py"
    tree = ast.parse(cache_path.read_text(encoding="utf-8-sig"))
    decorator = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_locked_state_update")
    cache_class = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "EventFollowupCacheStore")
    cache_class.body = [n for n in cache_class.body if isinstance(n, ast.FunctionDef) and n.name in {"update_building_pending", "_cache_key"}]
    namespace.update(wraps=wraps, Any=Any, List=List, _now_text=lambda: "isolated")
    exec(compile(ast.Module(body=[decorator, cache_class], type_ignores=[]), str(cache_path), "exec"), namespace)
    state = {"pending_by_id": {}}
    state_lock = threading.RLock()

    def load():
        snapshot = copy.deepcopy(state)
        time.sleep(0.01)
        return snapshot

    def save(value):
        state.clear()
        state.update(value)
        return value

    def update_cache(building):
        cache = namespace["EventFollowupCacheStore"]()
        cache._state_lock = state_lock
        cache.load_state, cache.save_state = load, save
        return cache.update_building_pending(building=building, pending_rows=[{"record_id": "record"}],
                                            max_pending=100, last_query_record_ids=[], max_last_query_ids=100)

    with ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(update_cache, [job["building"] for job in jobs]))
    assert len(state["pending_by_id"]) == 5
    print("PASS: concurrent building cache updates retain all five buildings")
    print("PASS: five workers overlap; snapshots isolated; one failure isolated; queue recovers after timeout/error")


if __name__ == "__main__":
    main()
