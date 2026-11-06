"""Cross-process exclusion check using temporary locks only."""
import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile


def load_guard():
    path = Path(__file__).resolve().parents[1] / "handover_log_module/service/handover_generation_guard.py"
    spec = importlib.util.spec_from_file_location("isolated_guard", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    if len(sys.argv) > 1:
        tempfile.tempdir = sys.argv[1]
        with load_guard().generation_lock("A楼", "2026-11-06", "day"):
            print("READY", flush=True)
            sys.stdin.readline()
        return
    with tempfile.TemporaryDirectory() as directory:
        previous = tempfile.tempdir
        tempfile.tempdir = directory
        child = subprocess.Popen([sys.executable, __file__, directory], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            assert child.stdout.readline().strip() == "READY"
            guard = load_guard()
            try:
                with guard.generation_lock("A楼", "2026-11-06", "day"):
                    raise AssertionError("duplicate generation allowed")
            except RuntimeError as exc:
                assert "重复请求" in str(exc)
            with guard.generation_lock("B楼", "2026-11-06", "day"):
                pass
            with guard.generation_lock("A楼", "2026-11-06", "night"):
                pass
            child.kill()
            child.communicate(timeout=5)
            with guard.generation_lock("A楼", "2026-11-06", "day"):
                pass
            guard._SLOT_WAIT_TIMEOUT_SEC = 0.01
            class FakeGenerator:
                @guard.guard_generation
                def run(self, **kwargs):
                    return kwargs["building"]
            generator = FakeGenerator()
            for _ in range(5):
                assert guard._SLOTS.acquire(blocking=False)
            try:
                try:
                    generator.run(building="A楼", duty_date="2026-11-06", duty_shift="day")
                    raise AssertionError("saturated semaphore did not time out")
                except TimeoutError:
                    pass
                with guard.generation_lock("A楼", "2026-11-06", "day"):
                    pass
            finally:
                for _ in range(5):
                    guard._SLOTS.release()
            assert generator.run(building="A楼", duty_date="2026-11-06", duty_shift="day") == "A楼"
            print("PASS: slot timeout releases session lock; subsequent generation succeeds")
            print("PASS: duplicate blocked; different building/shift allowed; crashed owner releases lock")
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)
            tempfile.tempdir = previous


if __name__ == "__main__":
    main()
