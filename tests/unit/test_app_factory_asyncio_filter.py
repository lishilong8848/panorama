from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.bootstrap import app_factory


class _FakeLoop:
    def __init__(self):
        self._handler = None
        self.default_calls = []

    def get_exception_handler(self):
        return self._handler

    def set_exception_handler(self, handler):
        self._handler = handler

    def default_exception_handler(self, context):
        self.default_calls.append(context)


class _Container:
    def __init__(self):
        self.logs = []

    def add_system_log(self, line):
        self.logs.append(line)


def test_windows_asyncio_filter_swallows_known_proactor_connection_reset(monkeypatch):
    fake_loop = _FakeLoop()
    container = _Container()
    exc = ConnectionResetError(10054, "remote closed")
    exc.winerror = 10054

    monkeypatch.setattr(app_factory.os, "name", "nt")
    monkeypatch.setattr(app_factory.asyncio, "get_running_loop", lambda: fake_loop)

    app_factory._install_windows_asyncio_exception_filter(container)

    assert callable(fake_loop._handler)
    fake_loop._handler(
        fake_loop,
        {
            "exception": exc,
            "handle": "<Handle _ProactorBasePipeTransport._call_connection_lost(None)>",
        },
    )

    assert fake_loop.default_calls == []
    assert container.logs


def test_windows_asyncio_filter_keeps_other_exceptions(monkeypatch):
    fake_loop = _FakeLoop()
    container = _Container()

    monkeypatch.setattr(app_factory.os, "name", "nt")
    monkeypatch.setattr(app_factory.asyncio, "get_running_loop", lambda: fake_loop)

    app_factory._install_windows_asyncio_exception_filter(container)

    fake_loop._handler(
        fake_loop,
        {
            "exception": RuntimeError("boom"),
            "handle": "<Handle something_else()>",
        },
    )

    assert len(fake_loop.default_calls) == 1

