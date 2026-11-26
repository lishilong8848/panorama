from __future__ import annotations

from app.modules.report_pipeline.service.shared_bridge_waiting_job_helper import start_waiting_bridge_job


def submit_handover_scheduled_generation(*, job_service, bridge_service, name, payload, log_entries, capacity_entries):
    buildings = list(dict.fromkeys(payload["buildings"]))
    logs = {row["building"]: row["file_path"] for row in log_entries if row.get("file_path")}
    capacities = {row["building"]: row["file_path"] for row in capacity_entries if row.get("file_path")}
    ready = [building for building in buildings if building in logs and building in capacities]
    missing = [building for building in buildings if building not in ready]
    date, shift, slot = (payload[key] for key in ("duty_date", "duty_shift", "scheduler_slot"))
    dedupe_key = f"handover:scheduler:{slot}:{date}:{shift}"
    common = {"worker_handler": "handover_from_download", "priority": "scheduler", "submitted_by": "scheduler"}
    jobs = []
    if ready:
        job = job_service.start_worker_job(
            name=f"{name}-共享文件",
            worker_payload={
                **payload, "buildings": ready, "resume_kind": "shared_bridge_handover",
                "building_files": [{"building": building, "file_path": logs[building]} for building in ready],
                "capacity_building_files": [{"building": building, "file_path": capacities[building]} for building in ready],
            },
            resource_keys=["network:external"],
            feature="handover_cache_latest", dedupe_key=f"{dedupe_key}:ready:{','.join(ready)}",
            resume_policy="requeue",
            **common,
        )
        jobs.append(job.job_id)
    for building in missing:
        job, _ = start_waiting_bridge_job(
            job_service=job_service, bridge_service=bridge_service,
            name=f"{name}-{building}", worker_payload={**payload, "buildings": [building], "wait_for_handover_pair": True},
            resource_keys=["network:external", f"handover_building:{building}"],
            feature="handover_from_download", dedupe_key=f"{dedupe_key}:missing:{building}",
            bridge_get_or_create_name="get_or_create_handover_from_download_task",
            bridge_create_name="create_handover_from_download_task",
            bridge_kwargs={"buildings": [building], "end_time": None, "duty_date": date, "duty_shift": shift},
            summary=f"等待{building}本班次交接班日志和容量表源文件",
            **common,
        )
        jobs.append(job.job_id)
    return (
        f"duty_date={date}, duty_shift={shift}, 已提交生成={','.join(ready) or '-'}, "
        f"等待补采={','.join(missing) or '-'}, job_ids={','.join(jobs) or '-'}"
    )
