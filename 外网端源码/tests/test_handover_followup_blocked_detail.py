from app.modules.report_pipeline.service.job_service import JobService, JobState
from handover_log_module.service.review_followup_trigger_service import ReviewFollowupTriggerService


class _UnconfirmedReviewService:
    def get_session_by_id(self, session_id: str):
        return {
            "session_id": session_id,
            "building": "C楼",
            "batch_key": "2026-07-22|day",
            "confirmed": False,
        }

    def list_batch_sessions(self, batch_key: str):
        return [self.get_session_by_id("C楼|2026-07-22|day")]


def test_single_building_followup_returns_explicit_blocked_detail():
    service = object.__new__(ReviewFollowupTriggerService)
    service._review_service = _UnconfirmedReviewService()
    service._existing_daily_report_record_export = lambda sessions: {"status": "idle"}
    service._existing_cabinet_shift_record_export = lambda sessions: {"status": "idle"}
    service._collect_followup_progress = lambda **kwargs: {"status": "blocked"}
    logs = []

    result = service.trigger_single_building_cloud_sync(
        batch_key="2026-07-22|day",
        building="C楼",
        session_id="C楼|2026-07-22|day",
        emit_log=logs.append,
    )

    assert result["status"] == "blocked"
    assert result["blocked_reason"] == "pending_review"
    assert result["error"] == "C楼尚未确认，未执行云文档上传"
    assert result["cloud_sheet_sync"]["error"] == result["error"]
    assert any("C楼尚未确认" in line for line in logs)


def test_job_failure_detail_reads_blocked_reason_and_nested_failures():
    blocked = JobState(
        job_id="blocked-job",
        name="blocked",
        status="success",
        result={"status": "blocked", "blocked_reason": "等待楼栋确认"},
    )
    nested = JobState(
        job_id="nested-job",
        name="nested",
        status="success",
        result={
            "status": "failed",
            "cloud_sheet_sync": {
                "failed_buildings": [{"building": "C楼", "error": "云文档上传超时"}]
            },
        },
    )

    assert JobService._job_failure_detail(blocked) == "等待楼栋确认"
    assert JobService._job_failure_detail(nested) == "C楼: 云文档上传超时"
