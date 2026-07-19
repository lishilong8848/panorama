from __future__ import annotations

import pytest

from handover_log_module.service.chiller_mode_upload_service import ChillerModeUploadService


class FakeClient:
    def __init__(self, *, fail_batch: int = 0) -> None:
        self.timeout = 30
        self.request_retry_count = 1
        self.fail_batch = fail_batch
        self.batch_number = 0
        self.events: list[tuple[str, list[str] | int, int, int]] = []

    def list_record_ids(self, **_kwargs):
        self.events.append(("list", 0, self.timeout, self.request_retry_count))
        return ["old-1", "old-2"]

    def batch_create_records(self, *, fields_list, **_kwargs):
        self.batch_number += 1
        self.events.append(
            ("create", len(fields_list), self.timeout, self.request_retry_count)
        )
        if self.batch_number == self.fail_batch:
            raise TimeoutError("read timed out")
        start = (self.batch_number - 1) * 2
        return [
            {
                "data": {
                    "records": [
                        {"record_id": f"new-{start + index + 1}"}
                        for index in range(len(fields_list))
                    ]
                }
            }
        ]

    def batch_delete_records(self, *, record_ids, **_kwargs):
        normalized = list(record_ids)
        self.events.append(
            ("delete", normalized, self.timeout, self.request_retry_count)
        )
        return len(normalized)


def _target() -> dict:
    return {
        "page_size": 500,
        "delete_batch_size": 500,
        "create_batch_size": 2,
        "create_timeout_sec": 60,
        "create_retry_count": 3,
        "replace_existing": True,
    }


def test_chiller_safe_replace_creates_all_rows_before_deleting_old_records():
    service = ChillerModeUploadService({})
    client = FakeClient()

    deleted, created = service._replace_target_records_safely(
        client=client,
        table_id="table",
        rows=[{"value": index} for index in range(3)],
        target=_target(),
        list_field_names=["楼栋"],
        emit_log=lambda _text: None,
    )

    assert (deleted, created) == (2, 3)
    assert [event[0] for event in client.events] == ["list", "create", "create", "delete"]
    assert client.events[-1][1] == ["old-1", "old-2"]
    assert all(
        event[2:] == (60, 3)
        for event in client.events
        if event[0] in {"create", "delete"}
    )
    assert client.timeout == 30
    assert client.request_retry_count == 1


def test_chiller_safe_replace_keeps_old_records_when_a_create_batch_fails():
    service = ChillerModeUploadService({})
    client = FakeClient(fail_batch=2)
    logs: list[str] = []

    with pytest.raises(TimeoutError, match="read timed out"):
        service._replace_target_records_safely(
            client=client,
            table_id="table",
            rows=[{"value": index} for index in range(3)],
            target=_target(),
            list_field_names=["楼栋"],
            emit_log=logs.append,
        )

    delete_events = [event for event in client.events if event[0] == "delete"]
    assert len(delete_events) == 1
    assert delete_events[0][1] == ["new-1", "new-2"]
    assert "old-1" not in delete_events[0][1]
    assert "old-2" not in delete_events[0][1]
    assert any("旧记录保留=2" in text for text in logs)
    assert client.timeout == 30
    assert client.request_retry_count == 1
