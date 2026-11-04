from unittest.mock import Mock
import subprocess
import sys
import threading
import time

import pytest

from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from handover_log_module.service.branch_power_upload_service import (
    BranchPowerUploadService, _guard_branch_pipeline, _main_table_write_guard,
)


def _client(responses):
    client = object.__new__(FeishuBitableClient)
    client.app_token = "test"
    client._get_json = Mock(side_effect=responses)
    return client


def test_old_id_snapshot_reads_all_pages():
    client = _client([
        {"data": {"items": [{"record_id": "old-1"}], "has_more": True, "page_token": "next"}},
        {"data": {"items": [{"record_id": "old-2"}], "has_more": False}},
    ])
    assert client.list_record_ids("table", require_complete=True) == ["old-1", "old-2"]
    assert client._get_json.call_count == 2


@pytest.mark.parametrize("response", [
    {"data": {"items": [{"record_id": "old-1"}], "has_more": True}},
    {"data": {"items": [{"record_id": "old-1"}], "has_more": True, "page_token": None}},
    {"data": {"items": [{}], "has_more": False}},
    {"data": {"items": [{"record_id": None}], "has_more": False}},
    {"data": {"items": [{"record_id": "old-1"}], "page_token": "next"}},
    {"data": {"items": [{"record_id": "old-1"}, {"record_id": "old-1"}], "has_more": False}},
    {"data": {}},
])
def test_incomplete_old_id_snapshot_is_rejected(response):
    client = _client([response])
    with pytest.raises(RuntimeError, match="旧记录"):
        client.list_record_ids("table", require_complete=True)


def test_repeated_page_token_is_rejected():
    page = {"data": {"items": [{"record_id": "old-1"}], "has_more": True, "page_token": "same"}}
    client = _client([page, page])
    with pytest.raises(RuntimeError, match="分页"):
        client.list_record_ids("table", require_complete=True)


def test_main_table_guard_blocks_other_process_and_releases_after_error():
    code = (
        "from handover_log_module.service.branch_power_upload_service import _main_table_write_guard\n"
        "try:\n"
        "    with _main_table_write_guard(timeout_sec=0.1): pass\n"
        "except RuntimeError:\n"
        "    print('blocked')\n"
    )
    with pytest.raises(ValueError, match="test error"):
        with _main_table_write_guard(timeout_sec=1):
            result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=20)
            assert result.returncode == 0 and result.stdout.strip() == "blocked"
            raise ValueError("test error")
    with _main_table_write_guard(timeout_sec=1):
        pass


def test_branch_pipeline_guard_covers_followup_before_next_task():
    assert hasattr(BranchPowerUploadService.upload_day_from_source_files, "__wrapped__")
    entered = threading.Event()
    release = threading.Event()
    events = []

    @_guard_branch_pipeline
    def run(label, *, emit_log):
        events.append(label + "-main")
        if label == "first":
            entered.set()
            assert release.wait(3)
        events.append(label + "-followup")

    first = threading.Thread(target=run, args=("first",), kwargs={"emit_log": lambda _: None})
    second = threading.Thread(target=run, args=("second",), kwargs={"emit_log": lambda _: None})
    first.start()
    assert entered.wait(2)
    second.start()
    time.sleep(0.05)
    assert events == ["first-main"]
    release.set()
    first.join(2)
    second.join(2)
    assert events == ["first-main", "first-followup", "second-main", "second-followup"]
