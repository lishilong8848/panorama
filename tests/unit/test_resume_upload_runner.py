from __future__ import annotations

from pathlib import Path

from app.modules.report_pipeline.service.resume_upload_runner import upload_retryable_items


class _CalcModule:
    def __init__(self) -> None:
        self.called = 0

    def run_with_explicit_file_items(self, **kwargs) -> None:
        _ = kwargs
        self.called += 1


def test_upload_retryable_items_returns_zero_when_no_items():
    checkpoint = {"file_items": []}
    out = upload_retryable_items(
        config={},
        calc_module=_CalcModule(),
        checkpoint=checkpoint,
        gc_every_n_items=5,
        upload_chunk_threshold=20,
        upload_chunk_size=5,
        collect_retryable_file_items=lambda cp: [],
        now_text=lambda: "2026-03-08 00:00:00",
        save_checkpoint_and_index=lambda c, cp: cp,
        log_file_failure=lambda **kwargs: None,
        refresh_checkpoint_summary=lambda cp: cp,
        gc_collect=lambda: None,
    )
    assert out["processed_count"] == 0
    assert out["success_count"] == 0
    assert out["failed_count"] == 0


def test_upload_retryable_items_counts_success_and_missing_file(tmp_path):
    ok_file = tmp_path / "ok.xlsx"
    ok_file.write_text("x", encoding="utf-8")
    missing_file = tmp_path / "missing.xlsx"
    checkpoint = {"file_items": []}
    calc = _CalcModule()
    saved = {"n": 0}

    items = [
        {"building": "A楼", "file_path": str(ok_file), "upload_date": "2026-03-01", "attempts": 0},
        {"building": "B楼", "file_path": str(missing_file), "upload_date": "2026-03-01", "attempts": 0},
    ]
    failures = []

    out = upload_retryable_items(
        config={},
        calc_module=calc,
        checkpoint=checkpoint,
        gc_every_n_items=1,
        upload_chunk_threshold=20,
        upload_chunk_size=5,
        collect_retryable_file_items=lambda cp: items,
        now_text=lambda: "2026-03-08 00:00:00",
        save_checkpoint_and_index=lambda c, cp: saved.__setitem__("n", saved["n"] + 1) or cp,
        log_file_failure=lambda **kwargs: failures.append(kwargs),
        refresh_checkpoint_summary=lambda cp: cp,
        gc_collect=lambda: None,
    )

    assert calc.called == 1
    assert out["processed_count"] == 2
    assert out["success_count"] == 1
    assert out["failed_count"] == 1
    assert len(out["failure_items"]) == 1
    assert Path(out["failure_items"][0]["file_path"]).name == "missing.xlsx"
    assert saved["n"] >= 2
    assert len(failures) == 1
