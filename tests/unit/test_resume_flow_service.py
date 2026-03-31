from __future__ import annotations

from app.modules.report_pipeline.service.resume_flow_service import run_resume_upload


def _base_cfg() -> dict:
    return {
        "download": {"resume": {"enabled": True}},
        "feishu": {"enable_upload": True},
        "network": {"require_saved_profiles": True, "external_ssid": "EL-BG"},
    }


def test_run_resume_upload_returns_disabled_when_resume_off():
    cfg = _base_cfg()
    cfg["download"]["resume"]["enabled"] = False

    result = run_resume_upload(
        config=cfg,
        normalize_runtime_config=lambda c: c,
        validate_runtime_config=lambda _c: None,
        ensure_resume_config=lambda d: d["resume"],
        load_pending_checkpoint=lambda _c, run_id=None: None,
        sync_summary_from_checkpoint=lambda _s, _c: None,
        save_checkpoint_and_index=lambda _cfg, cp: cp,
        load_calc_module=lambda: object(),
        build_wifi_switcher=lambda *_a, **_k: object(),
        try_switch_wifi=lambda **_k: (True, "ok", False),
        upload_retryable_items=lambda **_k: {},
        notify_event=lambda *_a, **_k: None,
        log_file_failure=lambda **_k: None,
    )
    assert result["error"] == "download.resume.enabled=false，续传已禁用"


def test_run_resume_upload_returns_when_no_pending_checkpoint():
    cfg = _base_cfg()

    result = run_resume_upload(
        config=cfg,
        normalize_runtime_config=lambda c: c,
        validate_runtime_config=lambda _c: None,
        ensure_resume_config=lambda d: d["resume"],
        load_pending_checkpoint=lambda _c, run_id=None: None,
        sync_summary_from_checkpoint=lambda _s, _c: None,
        save_checkpoint_and_index=lambda _cfg, cp: cp,
        load_calc_module=lambda: object(),
        build_wifi_switcher=lambda *_a, **_k: object(),
        try_switch_wifi=lambda **_k: (True, "ok", False),
        upload_retryable_items=lambda **_k: {},
        notify_event=lambda *_a, **_k: None,
        log_file_failure=lambda **_k: None,
    )
    assert result["error"] == "没有待续传任务"
