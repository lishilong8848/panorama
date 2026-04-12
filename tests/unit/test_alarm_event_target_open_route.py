from pathlib import Path
import sys
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.modules.report_pipeline.api import routes


class _FakeSharedSourceCacheService:
    def __init__(self, *, runtime_config, store, download_browser_pool=None, emit_log=None):
        _ = runtime_config, store, download_browser_pool, emit_log

    def get_alarm_event_upload_target_preview(self, force_refresh=False):
        assert force_refresh is True
        return {
            "configured_app_token": "SOwsw315aiBJjgkl48ccoxIPntc",
            "operation_app_token": "tblx9g4wAppToken",
            "table_id": "tblD7hi70s6U6rlU",
            "target_kind": "wiki_token_pair",
            "display_url": "https://vnet.feishu.cn/wiki/SOwsw315aiBJjgkl48ccoxIPntc?table=tblD7hi70s6U6rlU&view=vewG7OKFEg",
            "bitable_url": "https://vnet.feishu.cn/wiki/SOwsw315aiBJjgkl48ccoxIPntc?table=tblD7hi70s6U6rlU&view=vewG7OKFEg",
            "message": "",
        }


class _FakeContainer:
    def __init__(self):
        self.runtime_config = {}
        self.logs = []

    def add_system_log(self, text) -> None:
        self.logs.append(str(text))


def test_open_alarm_event_upload_target_logs_resolved_preview(monkeypatch):
    monkeypatch.setattr(routes, "SharedSourceCacheService", _FakeSharedSourceCacheService)
    container = _FakeContainer()
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(container=container)))

    payload = routes.open_alarm_event_upload_target(request)

    assert payload["ok"] is True
    assert payload["target_preview"]["target_kind"] == "wiki_token_pair"
    assert payload["target_preview"]["display_url"].startswith("https://vnet.feishu.cn/wiki/")
    assert any("[告警上传][目标链接] 打开多维表:" in line for line in container.logs)
    assert any("kind=wiki_token_pair" in line for line in container.logs)
    assert any("display_url=https://vnet.feishu.cn/wiki/" in line for line in container.logs)
