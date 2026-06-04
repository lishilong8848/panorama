from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXTERNAL_CONFIG = PROJECT_ROOT / "外网端源码" / "表格计算配置.json"

DEFAULT_BUILDINGS = ["A楼", "B楼", "C楼", "D楼", "E楼"]
DEFAULT_FAMILIES = [
    "handover_log_family",
    "handover_capacity_report_family",
    "monthly_report_family",
    "top5_monthly_report_family",
    "branch_power_family",
    "branch_current_family",
    "branch_switch_family",
    "building_full_cabinet_power_family",
    "chiller_mode_switch_family",
    "alarm_event_family",
]

FAMILY_LABELS = {
    "handover_log_family": "交接班日志源文件",
    "handover_capacity_report_family": "交接班容量报表源文件",
    "monthly_report_family": "全景平台月报源文件",
    "top5_monthly_report_family": "TOP5月报源文件",
    "branch_power_family": "支路功率源文件",
    "branch_current_family": "支路电流源文件",
    "branch_switch_family": "支路开关源文件",
    "building_full_cabinet_power_family": "楼栋全机柜功率源文件",
    "chiller_mode_switch_family": "制冷模式参数源文件",
    "alarm_event_family": "告警信息源文件",
}

TERMINAL_TASK_STATUSES = {"success", "failed", "partial_failed", "cancelled", "stale"}


def _configure_stdout() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8-sig")
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}


def _load_default_bridge_config() -> dict[str, str]:
    data = _read_json(EXTERNAL_CONFIG)
    common = data.get("common") if isinstance(data.get("common"), dict) else {}
    cfg = common.get("internal_bridge_http") if isinstance(common.get("internal_bridge_http"), dict) else {}
    shared_bridge = common.get("shared_bridge") if isinstance(common.get("shared_bridge"), dict) else {}
    base_url = str(cfg.get("base_url", "") or "").strip()
    if not base_url:
        root = str(shared_bridge.get("external_root_dir") or shared_bridge.get("root_dir") or "").strip()
        if root.startswith("\\\\"):
            host = root.strip("\\").split("\\", 1)[0]
            if host:
                base_url = f"http://{host}:{int(cfg.get('port', 18765) or 18765)}"
    return {
        "base_url": base_url or "http://127.0.0.1:18765",
        "auth_token": str(cfg.get("auth_token", "") or "").strip(),
    }


class HttpError(RuntimeError):
    pass


class BridgeClient:
    def __init__(self, *, base_url: str, token: str = "", request_timeout_sec: int = 30) -> None:
        self.base_url = str(base_url or "").strip().rstrip("/")
        self.token = str(token or "").strip()
        self.request_timeout_sec = max(1, int(request_timeout_sec or 30))

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self.base_url:
            raise HttpError("base_url 为空")
        path_text = str(path or "").strip()
        if not path_text.startswith("/"):
            path_text = "/" + path_text
        url = self.base_url + path_text
        if query:
            clean_query = {
                str(key): str(value)
                for key, value in query.items()
                if value is not None and str(value).strip() != ""
            }
            if clean_query:
                url += "?" + urllib.parse.urlencode(clean_query)

        headers = {"Accept": "application/json"}
        body = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if self.token:
            headers["X-Bridge-Token"] = self.token

        req = urllib.request.Request(url, data=body, headers=headers, method=str(method or "GET").upper())
        started = time.time()
        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout_sec) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
                if not raw:
                    return {}
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
                return {"data": parsed}
        except urllib.error.HTTPError as exc:
            body_text = ""
            try:
                body_text = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body_text = ""
            raise HttpError(f"{method} {path_text} HTTP {exc.code}: {body_text or exc.reason}") from exc
        except (TimeoutError, urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            elapsed = time.time() - started
            raise HttpError(f"{method} {path_text} 失败: {exc}; elapsed={elapsed:.1f}s") from exc

    def health(self) -> dict[str, Any]:
        return self.request("GET", "/api/internal-bridge/health")

    def refresh_latest(self, *, source_family: str, buildings: list[str]) -> dict[str, Any]:
        return self.request(
            "POST",
            "/api/internal-bridge/source-cache/refresh-latest",
            payload={"source_family": source_family, "buildings": buildings},
        )

    def source_index_batch(self, queries: list[dict[str, Any]], *, default_limit: int = 10) -> list[dict[str, Any]]:
        payload = self.request(
            "POST",
            "/api/internal-bridge/source-index/batch",
            payload={"queries": queries, "default_limit": default_limit},
        )
        results = payload.get("results", [])
        return results if isinstance(results, list) else []

    def create_alarm_window_task(
        self,
        *,
        buildings: list[str],
        query_start: str,
        query_end: str,
        duty_date: str,
        duty_shift: str,
    ) -> dict[str, Any]:
        return self.request(
            "POST",
            "/api/internal-bridge/alarm-events/window-query",
            payload={
                "buildings": buildings,
                "query_start": query_start,
                "query_end": query_end,
                "duty_date": duty_date,
                "duty_shift": duty_shift,
                "requested_by": "manual_internal_bridge_refresh_test",
            },
        )

    def task(self, task_id: str) -> dict[str, Any]:
        return self.request("GET", f"/api/internal-bridge/tasks/{urllib.parse.quote(str(task_id or '').strip())}")


@dataclass
class BuildingResult:
    building: str
    status: str = "pending"
    reason: str = ""
    bucket_key: str = ""
    bucket_kind: str = ""
    file_path: str = ""
    relative_path: str = ""
    updated_at: str = ""
    entry_status: str = ""


@dataclass
class FamilyRun:
    family: str
    label: str
    requested_at: str
    response: dict[str, Any] = field(default_factory=dict)
    buildings: dict[str, BuildingResult] = field(default_factory=dict)
    error: str = ""


def _split_csv(text: str) -> list[str]:
    return [item.strip() for item in str(text or "").replace("，", ",").split(",") if item.strip()]


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _extract_entries(batch_item: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("entries", "rows", "items", "data"):
        value = batch_item.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _best_entry(entries: list[dict[str, Any]], *, bucket_key: str, bucket_kind: str) -> dict[str, Any] | None:
    exact: list[dict[str, Any]] = []
    bucket_text = str(bucket_key or "").strip()
    kind_text = str(bucket_kind or "").strip().lower()
    for row in entries:
        row_bucket = str(row.get("bucket_key", "") or row.get("bucket_or_date", "") or "").strip()
        row_kind = str(row.get("bucket_kind", "") or "").strip().lower()
        row_date = str(row.get("duty_date", "") or "").strip()
        if bucket_text and (row_bucket == bucket_text or row_date == bucket_text):
            if not kind_text or not row_kind or row_kind == kind_text or kind_text == "latest":
                exact.append(row)
    candidates = exact or entries
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda item: (
            str(item.get("downloaded_at", "") or ""),
            str(item.get("updated_at", "") or ""),
            str(item.get("entry_id", "") or ""),
        ),
        reverse=True,
    )[0]


def _extract_refresh_units(response: dict[str, Any], buildings: list[str], family: str) -> dict[str, dict[str, str]]:
    units: dict[str, dict[str, str]] = {}

    def ingest(item: Any) -> None:
        if not isinstance(item, dict):
            return
        building = str(item.get("building", "") or "").strip()
        if not building:
            return
        units[building] = {
            "bucket_key": str(item.get("bucket_key", "") or item.get("bucket_or_date", "") or "").strip(),
            "bucket_kind": str(item.get("bucket_kind", "") or "").strip(),
            "reason": str(item.get("reason", "") or "").strip(),
            "accepted": str(bool(item.get("accepted", False))).lower(),
            "running": str(bool(item.get("running", False))).lower(),
            "file_path": str(item.get("file_path", "") or "").strip(),
            "relative_path": str(item.get("relative_path", "") or "").strip(),
        }

    for key in ("units", "results", "items", "buildings"):
        for item in _as_list(response.get(key)):
            ingest(item)

    # Older/fallback responses may only contain a top-level bucket.
    top_bucket = str(response.get("bucket_key", "") or response.get("bucket_or_date", "") or "").strip()
    top_kind = str(response.get("bucket_kind", "") or "").strip()
    for building in buildings:
        units.setdefault(
            building,
            {
                "bucket_key": top_bucket,
                "bucket_kind": top_kind,
                "reason": str(response.get("reason", "") or "").strip(),
                "accepted": str(bool(response.get("accepted", False))).lower(),
                "running": str(bool(response.get("running", False))).lower(),
                "file_path": "",
                "relative_path": "",
            },
        )
        # Daily branch/building-full families may not echo bucket_kind.
        if not units[building].get("bucket_kind"):
            units[building]["bucket_kind"] = "daily" if family in {
                "branch_power_family",
                "branch_current_family",
                "branch_switch_family",
                "building_full_cabinet_power_family",
            } else "latest"
    return units


def _make_queries(run: FamilyRun) -> list[dict[str, Any]]:
    queries: list[dict[str, Any]] = []
    for result in run.buildings.values():
        query = {
            "source_family": run.family,
            "building": result.building,
            "status": "all",
            "limit": 10,
        }
        if result.bucket_key:
            query["bucket_or_date"] = result.bucket_key
        if result.bucket_kind:
            query["bucket_kind"] = result.bucket_kind
        queries.append(query)
    return queries


def _update_from_batch(run: FamilyRun, batch_results: list[dict[str, Any]]) -> None:
    building_order = list(run.buildings.keys())
    for item in batch_results:
        if not isinstance(item, dict):
            continue
        query = item.get("query") if isinstance(item.get("query"), dict) else {}
        building = str(item.get("building", "") or query.get("building", "") or "").strip()
        if not building:
            try:
                index = int(item.get("index", -1))
            except (TypeError, ValueError):
                index = -1
            if 0 <= index < len(building_order):
                building = building_order[index]
        if not building or building not in run.buildings:
            continue
        result = run.buildings[building]
        entries = _extract_entries(item)
        entry = _best_entry(entries, bucket_key=result.bucket_key, bucket_kind=result.bucket_kind)
        if not entry:
            result.status = "pending"
            result.reason = "source-index 暂无记录"
            continue
        entry_status = str(entry.get("status", "") or "").strip().lower()
        result.entry_status = entry_status
        result.updated_at = str(entry.get("updated_at", "") or entry.get("downloaded_at", "") or "").strip()
        result.file_path = str(entry.get("file_path", "") or "").strip()
        result.relative_path = str(entry.get("relative_path", "") or "").strip()
        result.bucket_key = result.bucket_key or str(entry.get("bucket_key", "") or entry.get("duty_date", "") or "").strip()
        result.bucket_kind = result.bucket_kind or str(entry.get("bucket_kind", "") or "").strip()
        if entry_status == "ready":
            result.status = "ready"
            result.reason = "ready"
        elif entry_status in {"failed", "cancelled", "stale"}:
            result.status = entry_status
            result.reason = str(entry.get("error", "") or entry.get("last_error", "") or entry_status).strip()
        elif entry_status:
            result.status = entry_status
            result.reason = entry_status
        else:
            result.status = "pending"
            result.reason = "source-index 记录未给出状态"


def _print_family_progress(run: FamilyRun) -> None:
    ready = sum(1 for item in run.buildings.values() if item.status == "ready")
    failed = [item for item in run.buildings.values() if item.status in {"failed", "cancelled", "stale"}]
    running = [item for item in run.buildings.values() if item.status not in {"ready", "failed", "cancelled", "stale"}]
    print(
        f"[{_now_text()}] {run.label}: ready={ready}/{len(run.buildings)}, "
        f"pending={len(running)}, failed={len(failed)}"
    )


def run_family(
    client: BridgeClient,
    *,
    family: str,
    buildings: list[str],
    poll_interval_sec: int,
    timeout_sec: int,
    dry_run: bool = False,
) -> FamilyRun:
    label = FAMILY_LABELS.get(family, family)
    run = FamilyRun(family=family, label=label, requested_at=_now_text())
    print(f"\n=== {label} ({family}) ===")
    if dry_run:
        print("dry-run: 跳过触发，只演示将要请求的 family/buildings")
        return run
    try:
        response = client.refresh_latest(source_family=family, buildings=buildings)
        run.response = response
        units = _extract_refresh_units(response, buildings, family)
        for building in buildings:
            unit = units.get(building, {})
            result = BuildingResult(
                building=building,
                status="pending",
                reason=unit.get("reason", ""),
                bucket_key=unit.get("bucket_key", ""),
                bucket_kind=unit.get("bucket_kind", ""),
                file_path=unit.get("file_path", ""),
                relative_path=unit.get("relative_path", ""),
            )
            if unit.get("reason") == "already_ready" and (result.file_path or result.relative_path):
                result.status = "ready"
                result.reason = "already_ready"
            run.buildings[building] = result
        accepted = int(response.get("accepted_count", response.get("accepted", 0)) or 0) if not isinstance(response.get("accepted"), bool) else int(response.get("accepted"))
        print(f"[{_now_text()}] 已触发: accepted={accepted}, raw={json.dumps(response, ensure_ascii=False)[:600]}")
    except Exception as exc:  # noqa: BLE001
        run.error = str(exc)
        print(f"[{_now_text()}] 触发失败: {exc}")
        for building in buildings:
            run.buildings[building] = BuildingResult(building=building, status="failed", reason=str(exc))
        return run

    deadline = time.time() + max(1, timeout_sec)
    last_print = 0.0
    while time.time() < deadline:
        try:
            queries = _make_queries(run)
            if queries:
                results = client.source_index_batch(queries, default_limit=10)
                _update_from_batch(run, results)
        except Exception as exc:  # noqa: BLE001
            print(f"[{_now_text()}] source-index 查询失败: {exc}")

        if time.time() - last_print >= max(1, poll_interval_sec):
            _print_family_progress(run)
            last_print = time.time()

        statuses = {item.status for item in run.buildings.values()}
        if statuses and statuses <= {"ready"}:
            break
        if any(status in {"failed", "cancelled", "stale"} for status in statuses):
            # Keep polling until timeout so late ready entries can still be observed,
            # but no need to sleep if every building is already terminal.
            if statuses <= {"ready", "failed", "cancelled", "stale"}:
                break
        time.sleep(max(1, poll_interval_sec))

    for result in run.buildings.values():
        if result.status not in {"ready", "failed", "cancelled", "stale"}:
            result.status = "timeout"
            result.reason = result.reason or f"超过 {timeout_sec}s 未 ready"
    return run


def _default_alarm_window() -> tuple[str, str, str, str]:
    now = datetime.now().replace(microsecond=0)
    query_end = now
    query_start = now - timedelta(minutes=15)
    duty_date = now.strftime("%Y-%m-%d")
    duty_shift = "day" if 9 <= now.hour < 18 else "night"
    return (
        query_start.strftime("%Y-%m-%d %H:%M:%S"),
        query_end.strftime("%Y-%m-%d %H:%M:%S"),
        duty_date,
        duty_shift,
    )


def run_alarm_window(
    client: BridgeClient,
    *,
    buildings: list[str],
    query_start: str,
    query_end: str,
    duty_date: str,
    duty_shift: str,
    poll_interval_sec: int,
    timeout_sec: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    print("\n=== 交接班告警精确窗口补采 (handover_window) ===")
    print(f"window={query_start} ~ {query_end}, duty={duty_date}/{duty_shift}, buildings={','.join(buildings)}")
    if dry_run:
        print("dry-run: 跳过触发")
        return {"status": "dry_run"}
    try:
        task = client.create_alarm_window_task(
            buildings=buildings,
            query_start=query_start,
            query_end=query_end,
            duty_date=duty_date,
            duty_shift=duty_shift,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[{_now_text()}] 告警窗口任务触发失败: {exc}")
        return {"status": "failed", "error": str(exc)}
    task_id = str(task.get("task_id", "") or "").strip()
    print(f"[{_now_text()}] 已触发告警窗口任务: task_id={task_id or '-'}, raw={json.dumps(task, ensure_ascii=False)[:600]}")
    if not task_id:
        return {"status": "failed", "error": "任务返回缺少 task_id", "raw": task}

    deadline = time.time() + max(1, timeout_sec)
    latest: dict[str, Any] = task
    while time.time() < deadline:
        try:
            latest = client.task(task_id)
        except Exception as exc:  # noqa: BLE001
            print(f"[{_now_text()}] 告警窗口任务查询失败: {exc}")
        status = str(latest.get("status", "") or "").strip().lower()
        progress = latest.get("progress") if latest.get("progress") is not None else latest.get("result_json", "")
        print(f"[{_now_text()}] 告警窗口任务状态: {status or '-'}, progress={str(progress)[:240]}")
        if status in TERMINAL_TASK_STATUSES:
            return latest
        time.sleep(max(1, poll_interval_sec))
    latest["status"] = "timeout"
    latest["error"] = latest.get("error") or f"超过 {timeout_sec}s 未完成"
    return latest


def print_summary(runs: list[FamilyRun], alarm_result: dict[str, Any] | None) -> int:
    print("\n\n================ 测试汇总 ================")
    failed = 0
    for run in runs:
        print(f"\n{run.label} ({run.family})")
        if run.error:
            print(f"  触发失败: {run.error}")
            failed += 1
        for result in run.buildings.values():
            ok = result.status == "ready"
            if not ok:
                failed += 1
            path = result.file_path or result.relative_path or "-"
            print(
                f"  {result.building}: {result.status}"
                f" | bucket={result.bucket_kind or '-'}/{result.bucket_key or '-'}"
                f" | path={path}"
                f" | reason={result.reason or '-'}"
            )
    if alarm_result is not None:
        alarm_status = str(alarm_result.get("status", "") or "").strip().lower()
        print("\n交接班告警精确窗口")
        print(f"  status={alarm_status or '-'} error={alarm_result.get('error') or '-'}")
        if alarm_status not in {"success", "dry_run"}:
            failed += 1
    print(f"\n总结果: {'通过' if failed == 0 else '存在失败/超时'}，failed_count={failed}")
    return 0 if failed == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    defaults = _load_default_bridge_config()
    parser = argparse.ArgumentParser(
        description=(
            "模拟外网端触发内网端 HTTP 源文件补采，覆盖所有 source family。"
            "脚本只调用内网端 HTTP 桥接，不上传飞书/多维。"
        )
    )
    parser.add_argument("--base-url", default=defaults["base_url"], help="内网端 HTTP 地址，默认读取外网端配置")
    parser.add_argument("--token", default=defaults["auth_token"], help="X-Bridge-Token，默认读取外网端配置")
    parser.add_argument("--buildings", default=",".join(DEFAULT_BUILDINGS), help="楼栋列表，逗号分隔")
    parser.add_argument(
        "--families",
        default=",".join(DEFAULT_FAMILIES),
        help="要测试的 source family，逗号分隔；默认覆盖全部",
    )
    parser.add_argument("--timeout-sec", type=int, default=1800, help="每个 family 最大等待秒数")
    parser.add_argument("--poll-interval-sec", type=int, default=5, help="轮询间隔秒数")
    parser.add_argument("--request-timeout-sec", type=int, default=30, help="单次 HTTP 请求超时秒数")
    parser.add_argument("--skip-alarm-window", action="store_true", help="跳过交接班告警精确窗口补采测试")
    parser.add_argument("--alarm-window-start", default="", help="告警精确窗口开始，例如 2026-06-04 09:00:00")
    parser.add_argument("--alarm-window-end", default="", help="告警精确窗口结束，例如 2026-06-04 18:00:00")
    parser.add_argument("--duty-date", default="", help="告警窗口所属业务日期，例如 2026-06-04")
    parser.add_argument("--duty-shift", default="", choices=["", "day", "night"], help="告警窗口所属班次")
    parser.add_argument("--dry-run", action="store_true", help="只打印将要测试的接口，不实际触发补采")
    return parser


def main(argv: list[str] | None = None) -> int:
    _configure_stdout()
    parser = build_parser()
    args = parser.parse_args(argv)
    buildings = _split_csv(args.buildings) or DEFAULT_BUILDINGS
    families = _split_csv(args.families) or DEFAULT_FAMILIES
    client = BridgeClient(
        base_url=args.base_url,
        token=args.token,
        request_timeout_sec=args.request_timeout_sec,
    )

    print("内网端 HTTP 补采链路测试")
    print(f"base_url={client.base_url}")
    print(f"buildings={','.join(buildings)}")
    print(f"families={','.join(families)}")
    print("说明：本脚本会触发内网端下载/JSON查询；不会上传飞书、多维或发送消息。")

    try:
        health = client.health()
        print(f"\nhealth ok: {json.dumps(health, ensure_ascii=False)[:1000]}")
    except Exception as exc:  # noqa: BLE001
        print(f"\nhealth 失败: {exc}")
        print("内网端 health 不通时仍可继续尝试，以便看到每个接口的具体错误。")

    runs: list[FamilyRun] = []
    for family in families:
        runs.append(
            run_family(
                client,
                family=family,
                buildings=buildings,
                poll_interval_sec=args.poll_interval_sec,
                timeout_sec=args.timeout_sec,
                dry_run=args.dry_run,
            )
        )

    alarm_result = None
    if not args.skip_alarm_window:
        default_start, default_end, default_date, default_shift = _default_alarm_window()
        alarm_result = run_alarm_window(
            client,
            buildings=buildings,
            query_start=str(args.alarm_window_start or default_start).strip(),
            query_end=str(args.alarm_window_end or default_end).strip(),
            duty_date=str(args.duty_date or default_date).strip(),
            duty_shift=str(args.duty_shift or default_shift).strip(),
            poll_interval_sec=args.poll_interval_sec,
            timeout_sec=args.timeout_sec,
            dry_run=args.dry_run,
        )

    return print_summary(runs, alarm_result)


if __name__ == "__main__":
    raise SystemExit(main())
