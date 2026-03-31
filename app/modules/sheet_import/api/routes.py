from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile

from app.modules.notify.service.webhook_notify_service import WebhookNotifyService
from app.modules.sheet_import.service.sheet_import_service import SheetImportService
from app.shared.utils.runtime_temp_workspace import cleanup_runtime_temp_dir, create_runtime_temp_dir
from pipeline_utils import get_app_dir


router = APIRouter(tags=["sheet-import"])


def _deployment_role_mode(container) -> str:
    snapshot = container.deployment_snapshot() if hasattr(container, "deployment_snapshot") else {}
    if not isinstance(snapshot, dict):
        return "switching"
    text = str(snapshot.get("role_mode", "") or "").strip().lower()
    if text == "hybrid":
        return "switching"
    if text in {"switching", "internal", "external"}:
        return text
    return "switching"


def _save_upload_file(upload: UploadFile, prefix: str, runtime_config: dict) -> Path:
    suffix = Path(upload.filename or "upload.xlsx").suffix or ".xlsx"
    tmp_dir = create_runtime_temp_dir(
        kind=prefix,
        runtime_config=runtime_config,
        app_dir=get_app_dir(),
    )
    tmp_path = tmp_dir / f"input{suffix}"
    with tmp_path.open("wb") as handle:
        handle.write(upload.file.read())
    return tmp_path


def _start_background_job(
    container,
    *,
    name: str,
    run_func,
    resource_keys: list[str] | tuple[str, ...] | None = None,
    priority: str = "manual",
    feature: str = "",
    submitted_by: str = "manual",
    worker_handler: str = "",
    worker_payload: dict | None = None,
):
    job_service = container.job_service
    if worker_handler and hasattr(job_service, "start_worker_job"):
        return job_service.start_worker_job(
            name=name,
            worker_handler=worker_handler,
            worker_payload=worker_payload or {},
            resource_keys=resource_keys,
            priority=priority,
            feature=feature,
            submitted_by=submitted_by,
        )
    return job_service.start_job(
        name=name,
        run_func=run_func,
        resource_keys=resource_keys,
        priority=priority,
        feature=feature,
        submitted_by=submitted_by,
    )


@router.post("/api/jobs/sheet-import")
async def run_sheet_import(
    request: Request,
    legacy_switch_external_before_upload: bool = Form(False, alias="switch_external_before_upload"),
    file: UploadFile = File(...),
) -> dict:
    if not file.filename:
        raise HTTPException(status_code=400, detail="请上传 xlsx 文件")

    container = request.app.state.container
    role_mode = _deployment_role_mode(container)
    if role_mode == "internal":
        raise HTTPException(status_code=409, detail="当前为内网端角色，请在外网端执行 5Sheet 导入")

    runtime_config = container.runtime_config
    temp_path = _save_upload_file(file, "sheet_import", runtime_config)

    def _run(emit_log):
        service = SheetImportService(runtime_config)
        notify = WebhookNotifyService(runtime_config)
        try:
            result = service.run(str(temp_path), role_mode == "switching", emit_log)
            if int(result.get("failed_count", 0)) > 0:
                notify.send_failure(stage="5Sheet导表", detail="存在失败 Sheet，请查看日志详情", emit_log=emit_log)
            return result
        except Exception as exc:  # noqa: BLE001
            notify.send_failure(stage="5Sheet导表", detail=str(exc), emit_log=emit_log)
            raise
        finally:
            cleanup_runtime_temp_dir(temp_path.parent, runtime_config=runtime_config, app_dir=get_app_dir())

    try:
        job = _start_background_job(
            container,
            name="5Sheet导表",
            run_func=_run,
            worker_handler="sheet_import",
            worker_payload={
                "xlsx_path": str(temp_path),
                "switch_external_before_upload": role_mode == "switching",
                "cleanup_dir": str(temp_path.parent),
                "legacy_switch_external_before_upload": bool(legacy_switch_external_before_upload),
            },
            resource_keys=["network:external"],
            priority="manual",
            feature="sheet_import",
            submitted_by="manual",
        )
        container.add_system_log(f"[任务] 已提交: 5Sheet导表 ({job.job_id})")
        return job.to_dict()
    except Exception as exc:  # noqa: BLE001
        cleanup_runtime_temp_dir(temp_path.parent, runtime_config=runtime_config, app_dir=get_app_dir())
        raise HTTPException(status_code=409, detail=str(exc)) from exc
