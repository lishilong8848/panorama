"""Isolated concurrency checks: no application imports, network or business writes."""
import ast
import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import threading
import time
from typing import Callable, Dict, Iterator


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
    print("PASS: five workers overlap; snapshots isolated; one failure isolated; queue recovers after timeout/error")


if __name__ == "__main__":
    main()
