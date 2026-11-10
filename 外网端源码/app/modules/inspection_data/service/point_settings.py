from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from os import fstat
from pathlib import Path

from app.config.inspection_data_config import INSPECTION_ROUND_FIELDS, INSPECTION_SOURCE_CONTRACTS
from app.modules.internal_bridge_http.service.client import InternalBridgeHttpClient
from app.modules.inspection_data.service.point_correction import normalize_correction_rules
from app.modules.inspection_data.service.point_status import normalize_status_rules
from app.shared.utils.atomic_file import atomic_write_text


_POINT_ID = re.compile(r"[0-9]+(?:_[0-9]+){1,15}")
_LOCK = threading.Lock()
EMPTY_POINT_GROUP = {"points": {}, "remove": []}


class PointSettingsConflict(ValueError):
    pass


def _path(config):
    return Path(config.get("paths", {}).get("runtime_state_root") or ".runtime") / "inspection_point_settings.json"


def _settings_snapshot(config):
    try:
        path = _path(config)
        with path.open(encoding="utf-8") as handle:
            saved = json.load(handle)
            legacy_revision = fstat(handle.fileno()).st_mtime_ns
    except FileNotFoundError:
        return None, 0
    if "revision" in saved and "settings" in saved:
        return saved["settings"], saved["revision"]
    return saved, legacy_revision


def load_point_settings(config):
    return _settings_snapshot(config)[0]


def _save_settings(config, settings):
    revision = max(time.time_ns(), _settings_snapshot(config)[1] + 1)
    atomic_write_text(_path(config), json.dumps({"revision": revision, "settings": settings}, ensure_ascii=False), encoding="utf-8")


@contextmanager
def _settings_write_lock(config):
    path = _path(config).with_name("inspection_point_settings.lock.db")
    path.parent.mkdir(parents=True, exist_ok=True)
    # The scheduler worker and the web process must serialize Feishu writes as well as local edits.
    with _LOCK:
        connection = sqlite3.connect(path, timeout=10)
        try:
            try:
                connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                raise OSError("巡检测点配置正在同步，请稍后再试") from exc
            yield
        finally:
            connection.close()


def load_or_restore_point_settings(config):
    settings = load_point_settings(config)
    if settings is not None:
        return settings
    from app.modules.feishu.service.feishu_auth_resolver import resolve_feishu_auth_settings
    from app.modules.inspection_data.service.point_config_bitable import read_point_config_bitable

    auth = resolve_feishu_auth_settings(config)
    if not auth.get("app_id") or not auth.get("app_secret"):
        return None
    with _settings_write_lock(config):
        settings = load_point_settings(config)
        if settings is not None:
            return settings
        settings = read_point_config_bitable(config)
        if not settings:
            return None
        for building, groups in settings.items():
            for group in groups.values():
                ids = set(group["baseline"]) | set(group["points"]) | set(group["remove"])
                if any(len(i) > 100 or not _POINT_ID.fullmatch(i) for i in ids):
                    raise ValueError("飞书巡检测点配置含无效 ID，未恢复配置")
                group["remove"].sort()
                _validate_effective_count(group["baseline"], group["points"], group["remove"])
                if "targets" in group:
                    group["targets"] = _normalize_targets(building, group["points"], group["targets"])
                if "status_rules" in group:
                    group["status_rules"] = normalize_status_rules(
                        group["status_rules"], (set(group["baseline"]) | set(group["points"])) - set(group["remove"]))
                if "correction_rules" in group:
                    group["correction_rules"] = normalize_correction_rules(
                        group["correction_rules"], (set(group["baseline"]) | set(group["points"])) - set(group["remove"]))
            _validate_target_collisions(groups)
        _save_settings(config, settings)
        return settings


def inspection_target_options(building):
    from app.modules.inspection_data.service.inspection_data_upload_service import load_catalog

    choices = {}
    for device in load_catalog():
        if device["building"] == building:
            choices.setdefault(device["workbook"], set()).add(device["sheet"].strip())
    return {name: sorted(sheets) for name, sheets in sorted(choices.items())}


def _normalize_targets(building, points, targets):
    if not isinstance(targets, dict) or set(targets) - set(points):
        raise ValueError("目标单元格必须对应已保存的测点")
    if not targets:
        return {}
    choices = inspection_target_options(building)
    result = {}
    for point_id, target in targets.items():
        if (not isinstance(target, dict) or not {"workbook", "sheet"} <= set(target)
                or set(target) - {"workbook", "sheet", "cell", "round_cells"}
                or any(not isinstance(target.get(key, ""), str) for key in ("workbook", "sheet", "cell"))):
            raise ValueError("请填写目标巡检表、工作表和单元格")
        workbook, sheet, cell = (target.get(key, "").strip() for key in ("workbook", "sheet", "cell"))
        cell = cell.upper()

        def valid_cell(value):
            match = re.fullmatch(r"([A-Z]{1,3})([1-9][0-9]{0,6})", value)
            return bool(match and int(match[2]) <= 1048576
                        and (len(match[1]) != 3 or match[1] <= "XFD"))

        if sheet not in choices.get(workbook, ()) or (cell and not valid_cell(cell)):
            raise ValueError("目标必须为本楼巡检表中的有效工作表和单元格，例如 B6")
        normalized = {"workbook": workbook, "sheet": sheet, "cell": cell}
        if "round_cells" in target:
            rounds = target["round_cells"]
            if (not isinstance(rounds, dict) or set(rounds) != set(INSPECTION_ROUND_FIELDS)
                    or any(not isinstance(value, str) for value in rounds.values())):
                raise ValueError("巡检位置格式无效")
            rounds = {key: rounds[key].strip().upper() for key in INSPECTION_ROUND_FIELDS}
            used = [value for value in rounds.values() if value]
            if any(not valid_cell(value) for value in used):
                raise ValueError("目标必须为本楼巡检表中的有效工作表和单元格，例如 B6")
            if sheet == "暖通冷机系统巡检表":
                if len(set(used)) > 1:
                    raise ValueError("暖通冷机系统巡检表只能配置一个固定位置")
                if used:
                    rounds = dict.fromkeys(INSPECTION_ROUND_FIELDS, used[0])
            elif len(used) != len(set(used)):
                raise ValueError("同一测点的巡检位置不能重复")
            normalized["round_cells"] = rounds
        if not cell and not any(normalized.get("round_cells", {}).values()):
            raise ValueError("请填写目标单元格")
        result[point_id] = normalized
    return result


def _validate_target_collisions(groups):
    occupied = {}
    for family, group in groups.items():
        for point_id, target in group.get("targets", {}).items():
            owner = (family, point_id)
            if not any(target.get("round_cells", {}).values()) and target.get("cell"):
                key = (target["workbook"], target["sheet"], target["cell"])
                if key in occupied and occupied[key] != owner:
                    raise ValueError(f"目标单元格已分配给其他测点：{target['sheet']}!{target['cell']}")
                occupied[key] = owner
            for cell in set(target.get("round_cells", {}).values()) - {""}:
                key = (target["workbook"], target["sheet"], cell)
                if key in occupied and occupied[key] != owner:
                    raise ValueError(f"目标单元格已分配给其他测点：{target['sheet']}!{cell}")
                occupied[key] = owner


def query_rules(settings):
    """产出与内网 normalize_inspection_point_overrides 完全一致（幂等）的规则。

    add/remove/baseline 均排序、去重、去空白；三者全空的族丢弃。这样：
    - sync_point_settings 的 `response.point_overrides != rules` 复核不会因内网归一化（如 remove 排序、
      空族丢弃）与外网原始 query_rules 不一致而误报“内网未确认”；
    - 上传时逐数据集 `doc.point_overrides != expected` 比较也不会因形态差异误跳过有效数据。
    """
    result = {}
    for building, groups in (settings or {}).items():
        for family, group in (groups or {}).items():
            baseline_keys = set(group.get("baseline", {}) or {})
            point_keys = set(group.get("points", {}) or {})
            normalized = {
                "add": sorted({str(i).strip() for i in (point_keys - baseline_keys)}),
                "remove": sorted({str(i).strip() for i in (group.get("remove", []) or [])}),
            }
            if "baseline" in group:
                normalized["baseline"] = sorted({str(i).strip() for i in baseline_keys})
            if any(normalized.values()):
                result.setdefault(building, {})[family] = normalized
    return result


def initialize_point_baseline(config, datasets):
    from app.modules.inspection_data.service.inspection_data_upload_service import InspectionDataUploadService

    required = {(b, f) for b in ("D楼", "E楼") for f in INSPECTION_SOURCE_CONTRACTS}
    captured = {}
    buckets = set()
    for doc in datasets:
        key = doc["building"], doc["source_family"]
        if key not in required or key in captured:
            raise ValueError("巡检基线楼栋或专业无效")
        bucket = doc["bucket_key"]
        buckets.add(bucket)
        InspectionDataUploadService._validate_dataset(doc, bucket[:10], bucket)
        ids = doc.get("point_ids", [])
        if (not 1 <= len(ids) <= 2000 or not doc.get("requested_point_ids")
                or any(not _POINT_ID.fullmatch(i) or len(i) > 100 for i in ids)):
            raise ValueError("巡检基线缺少有效测点ID")
        rows = InspectionDataUploadService._point_detail_rows(doc, INSPECTION_SOURCE_CONTRACTS[key[1]]["discipline"], {})
        captured[key] = {r["测点ID"]: {"name": r["测点名称"], "path": r["测点路径"],
                                      "value": r["测点值"], "sampleTime": doc["value_time"]} for r in rows}
        if set(captured[key]) != set(ids):
            raise ValueError("巡检基线测点明细不完整")
    if set(captured) != required or len(buckets) != 1:
        raise ValueError("巡检基线需要同一时段四组数据")
    with _settings_write_lock(config):
        settings = load_point_settings(config) or {}
        changed = False
        for (building, family), baseline in captured.items():
            groups = settings.setdefault(building, {})
            group = groups.setdefault(family, {"points": {}, "remove": []})
            if "baseline" not in group:
                _validate_effective_count(baseline, group["points"], group["remove"])
                group["baseline"] = baseline
                changed = True
        if changed:
            _save_settings(config, settings)
    return settings


def _validate_effective_count(baseline, points, remove):
    if not 1 <= len((set(baseline) | set(points)) - set(remove)) <= 2000:
        raise ValueError("每个楼栋专业必须保留 1–2000 个测点")


def sync_point_settings(config, settings, *, bridge=None):
    bridge = bridge or InternalBridgeHttpClient.from_runtime_config(config)
    if bridge is None:
        raise RuntimeError("未配置内网桥接地址")
    saved, revision = _settings_snapshot(config)
    if saved != settings or not revision:
        raise PointSettingsConflict("巡检测点配置已变更，拒绝同步旧配置")
    rules = query_rules(saved)
    response = bridge.put_inspection_point_rules(rules, revision=revision)
    if not response.get("ok") or response.get("point_overrides") != rules or response.get("revision") != revision:
        raise RuntimeError("内网未确认巡检测点配置")


def _pending_path(config):
    return _path(config).with_name("inspection_point_config_pending.json")


def _pending_buildings(config):
    try:
        return set(json.loads(_pending_path(config).read_text(encoding="utf-8")))
    except FileNotFoundError:
        return set()


def _write_pending(config, buildings):
    atomic_write_text(_pending_path(config), json.dumps(sorted(buildings), ensure_ascii=False), encoding="utf-8")


def _retrigger_path(config):
    return _path(config).with_name("inspection_upload_retrigger_pending.json")


@contextmanager
def _retrigger_lock(config):
    """补派标记的跨进程写锁：写标记与"比较+删除"必须互斥，避免旧确认删掉新请求。"""
    path = _path(config).with_name("inspection_retrigger.lock.db")
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        connection = sqlite3.connect(path, timeout=10)
        try:
            try:
                connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                raise OSError("巡检补派标记正在被其他进程修改，请稍后重试") from exc
            yield
        finally:
            connection.close()


def request_inspection_upload_retrigger(config, reason: str = "") -> None:
    """测点配置保存时登记“待传版本”；若已有上传任务在执行则由补派/巡检接手。

    标记携带保存时的配置修订号：任务只能确认覆盖到不高于该修订号的请求，
    更高的新保存不会被旧任务清掉。
    """
    with _retrigger_lock(config):
        atomic_write_text(
            _retrigger_path(config),
            json.dumps({
                "requested_at": time.time(),
                "reason": str(reason or ""),
                "revision": _settings_snapshot(config)[1],
            }, ensure_ascii=False),
            encoding="utf-8",
        )


def read_inspection_upload_retrigger(config):
    """只读取出补派标记（含修订号）；不存在或已过期返回 None，不删除。"""
    path = _retrigger_path(config)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    requested = payload.get("requested_at")
    if not isinstance(requested, (int, float)) or time.time() - requested > 6 * 3600:
        return None
    return payload


def ack_inspection_upload_retrigger(config, covered_revision) -> bool:
    """仅当标记请求的修订号已被本次实际上传覆盖（<= covered_revision）时清除标记。

    比较与删除在同一跨进程锁内完成并重新读取标记：旧确认在暂停期间被新保存
    替换的标记不会被误删。临时失败、取消、跨时段退出都不调用本函数，待传
    请求因此持久保留。
    """
    with _retrigger_lock(config):
        payload = read_inspection_upload_retrigger(config)
        if payload is None:
            return False
        requested_revision = payload.get("revision")
        if isinstance(requested_revision, int) and isinstance(covered_revision, int) \
                and requested_revision > covered_revision:
            return False
        try:
            _retrigger_path(config).unlink()
        except OSError:
            return False
        return True


def consume_inspection_upload_retrigger(config) -> bool:
    """兼容旧调用：无条件取走未过期标记。新代码应使用 read/ack 成对语义。"""
    payload = read_inspection_upload_retrigger(config)
    if payload is None:
        return False
    try:
        _retrigger_path(config).unlink()
    except OSError:
        pass
    return True


def _upload_revision_path(config):
    return _path(config).with_name("inspection_upload_last_revision.json")


def record_inspection_upload_revision(config, revision) -> None:
    """记录最近一次成功上传覆盖的配置修订号，供补派任务判断是否仍需重传。"""
    atomic_write_text(_upload_revision_path(config), json.dumps({"revision": int(revision)}), encoding="utf-8")


def last_inspection_upload_revision(config):
    try:
        payload = json.loads(_upload_revision_path(config).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return None
    revision = payload.get("revision") if isinstance(payload, dict) else None
    return revision if isinstance(revision, int) else None


def current_point_settings_revision(config):
    return _settings_snapshot(config)[1]


def load_point_settings_with_revision(config):
    """返回同一份快照的配置与修订号；首次缺失时先恢复再取一次新快照。

    恢复函数内部自行获取写锁并落盘，外层不再嵌套加锁或重写文件：
    恢复完成后直接读取新快照，配置内容与修订号必然同源，不会组成
    "旧配置 + 新版本号" 的错位对；恢复期间落地的新保存会体现在该快照中。
    """
    settings, revision = _settings_snapshot(config)
    if settings is None:
        load_or_restore_point_settings(config)
        settings, revision = _settings_snapshot(config)
    return settings, revision


def _retrigger_state_path(config):
    return _path(config).with_name("inspection_upload_retrigger_state.json")


def read_inspection_retrigger_dispatch_state(config):
    """补派巡检的派发节流状态；用于限制失败后的重试频率。"""
    try:
        payload = json.loads(_retrigger_state_path(config).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def record_inspection_retrigger_dispatch(config) -> None:
    atomic_write_text(
        _retrigger_state_path(config), json.dumps({"last_dispatch_at": time.time()}), encoding="utf-8")


def peek_inspection_upload_retrigger(config) -> bool:
    """只读检查补派标记是否存在且未过期，不消费它。"""
    path = _retrigger_path(config)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError):
        return False
    if not isinstance(payload, dict):
        return False
    requested = payload.get("requested_at")
    return isinstance(requested, (int, float)) and time.time() - requested <= 6 * 3600


def _sync_cloud_config(config, settings, buildings):
    from app.modules.inspection_data.service.point_config_bitable import sync_point_config_bitable

    sync_point_config_bitable(config, settings, buildings=tuple(sorted(buildings)), initialize=True)
    _write_pending(config, _pending_buildings(config) - set(buildings))


def retry_pending_point_config(config):
    if not _pending_buildings(config):
        return
    with _settings_write_lock(config):
        buildings = _pending_buildings(config)
        settings = load_point_settings(config)
        if settings and buildings:
            _sync_cloud_config(config, settings, buildings)


def save_point_group(config, *, building, family, points, remove, expected_group=None, targets=None, status_rules=None,
                     explicitly_enabled=None, explicitly_disabled=None, correction_rules=None):
    if not isinstance(building, str) or not isinstance(family, str) or building not in {"D楼", "E楼"} or family not in INSPECTION_SOURCE_CONTRACTS:
        raise ValueError("巡检测点只支持 D/E 楼的电气和暖通")
    if not isinstance(points, dict) or not isinstance(remove, list) or len(points) > 1000 or len(remove) > 1000:
        raise ValueError("巡检测点列表格式无效或超出限制")
    if any(not isinstance(i, str) or len(i) > 100 or not _POINT_ID.fullmatch(i) for i in [*points, *remove]):
        raise ValueError("巡检测点ID格式无效")
    if any(not isinstance(note, str) or len(note) > 500 for note in points.values()):
        raise ValueError("巡检测点备注必须为不超过500字的文本")
    if set(points) & set(remove):
        raise ValueError("同一测点不能同时纳入和排除")
    load_or_restore_point_settings(config)
    with _settings_write_lock(config):
        settings = load_point_settings(config) or {}
        groups = settings.setdefault(building, {})
        current = groups.get(family, EMPTY_POINT_GROUP)
        if expected_group is not None and current != expected_group:
            raise PointSettingsConflict("该楼栋专业的测点设置已更新，请重新加载后再修改")
        if targets is None:
            targets = {i: target for i, target in groups.get(family, {}).get("targets", {}).items() if i in points}
        targets = _normalize_targets(building, points, targets)
        active_ids = (set(current.get("baseline", {})) | set(points)) - set(remove)
        if explicitly_enabled is None:
            explicitly_enabled = [i for i in current.get("explicitly_enabled", []) if i in active_ids]
        if (not isinstance(explicitly_enabled, list) or len(explicitly_enabled) > 2000
                or any(not isinstance(i, str) or i not in active_ids for i in explicitly_enabled)):
            raise ValueError("主动启用的测点必须是当前有效测点")
        known_ids = active_ids | set(remove)
        if explicitly_disabled is None:
            explicitly_disabled = [i for i in current.get("explicitly_disabled", []) if i in known_ids]
        if (not isinstance(explicitly_disabled, list) or len(explicitly_disabled) > 2000
                or any(not isinstance(i, str) or i not in known_ids for i in explicitly_disabled)
                or set(explicitly_enabled) & set(explicitly_disabled)):
            raise ValueError("主动启用标记无效")
        if status_rules is None:
            status_rules = {i: rule for i, rule in current.get("status_rules", {}).items() if i in active_ids}
        status_rules = normalize_status_rules(status_rules, active_ids)
        if correction_rules is None:
            correction_rules = {i: rules for i, rules in current.get("correction_rules", {}).items() if i in active_ids}
        correction_rules = normalize_correction_rules(correction_rules, active_ids)
        if "baseline" in current:
            _validate_effective_count(current["baseline"], points, remove)
        if points or remove or "baseline" in current:
            _validate_target_collisions({**groups, family: {"targets": targets}})
            groups[family] = {"points": {i: note.strip() for i, note in sorted(points.items())},
                              "remove": sorted(set(remove)), **({"targets": targets} if targets else {}),
                              **({"status_rules": status_rules} if status_rules else {}),
                              **({"correction_rules": correction_rules} if correction_rules else {}),
                              **({"explicitly_enabled": sorted(set(explicitly_enabled))} if explicitly_enabled else {}),
                              **({"explicitly_disabled": sorted(set(explicitly_disabled))} if explicitly_disabled else {})}
            if "baseline" in current:
                groups[family]["baseline"] = current["baseline"]
        else:
            groups.pop(family, None)
            if not groups:
                settings.pop(building)
        has_baseline = any("baseline" in group for group in groups.values())
        if has_baseline:
            _write_pending(config, _pending_buildings(config) | {building})
        _save_settings(config, settings)
        pending = []
        cloud_synced = None
        if has_baseline:
            try:
                _sync_cloud_config(config, settings, (building,))
                cloud_synced = True
            except Exception as exc:
                cloud_synced = False
                pending.append(f"飞书配置表待同步：{exc}")
        try:
            sync_point_settings(config, settings)
        except (RuntimeError, OSError, ValueError) as exc:
            inner_synced = False
            pending.append(f"内网待同步：{exc}")
        else:
            inner_synced = True
    return {"ok": True, "settings": settings, "synced": inner_synced, "cloud_synced": cloud_synced,
            "message": "已保存，" + "；".join(pending) if pending else "测点设置已保存并同步"}
