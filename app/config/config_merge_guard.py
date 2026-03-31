from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence

from app.config.config_adapter import ensure_v3_config
from app.config.config_schema_v3 import DEFAULT_CONFIG_V3
from app.config.secret_masking import merge_masked_values


CRITICAL_SECTION_PATHS: Sequence[str] = (
    "common.network_switch",
    "common.scheduler",
    "common.notify",
    "common.feishu_auth",
    "common.alarm_db",
    "common.updater",
    "common.console",
    "features.monthly_report",
    "features.sheet_import",
    "features.handover_log",
    "features.day_metric_upload",
    "features.wet_bulb_collection",
    "features.manual_upload_gui",
)

CRITICAL_VALUE_PATHS: Sequence[str] = (
    "common.paths.business_root_dir",
)

_REJECT_LOSS_PATH_THRESHOLD = 5
_REJECT_LOSS_SECTION_THRESHOLD = 2


@dataclass(slots=True)
class ConfigMergeResult:
    merged: Dict[str, Any]
    suspicious_loss_paths: List[str]


class ConfigValueLossError(ValueError):
    def __init__(self, suspicious_paths: Sequence[str]) -> None:
        self.suspicious_paths = [str(item or "").strip() for item in suspicious_paths if str(item or "").strip()]
        preview = "、".join(self.suspicious_paths[:8])
        if len(self.suspicious_paths) > 8:
            preview = f"{preview} 等{len(self.suspicious_paths)}处"
        super().__init__(f"检测到可能会丢失用户配置，已拒绝保存: {preview}")


def _normalize_clear_paths(clear_paths: Iterable[str] | None) -> set[str]:
    return {
        str(item or "").strip()
        for item in (clear_paths or [])
        if str(item or "").strip()
    }


def _child_path(parent: str, key: str) -> str:
    return f"{parent}.{key}" if parent else key


def _is_path_cleared(path: str, clear_paths: set[str]) -> bool:
    text = str(path or "").strip()
    if not text:
        return False
    if text in clear_paths:
        return True
    prefix = f"{text}."
    return any(item.startswith(prefix) for item in clear_paths)


def _get_path(data: Any, path: str) -> Any:
    current = data
    for part in str(path or "").split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _set_path(data: Dict[str, Any], path: str, value: Any) -> None:
    parts = [part for part in str(path or "").split(".") if part]
    if not parts:
        return
    current = data
    for part in parts[:-1]:
        child = current.get(part)
        if not isinstance(child, dict):
            child = {}
            current[part] = child
        current = child
    current[parts[-1]] = copy.deepcopy(value)


def _is_empty_like(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not str(value).strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _values_equal(left: Any, right: Any) -> bool:
    return left == right


def _should_preserve_string(new_value: Any, old_value: Any, default_value: Any) -> bool:
    old_text = str(old_value or "").strip()
    if not old_text:
        return False
    default_text = str(default_value or "").strip()
    if old_text == default_text:
        return False
    new_text = str(new_value or "").strip()
    if not new_text:
        return True
    return bool(default_text) and new_text == default_text


def _should_preserve_list(new_value: Any, old_value: Any, default_value: Any) -> bool:
    if not isinstance(old_value, list) or not old_value:
        return False
    if _values_equal(old_value, default_value):
        return False
    if _is_empty_like(new_value):
        return True
    return isinstance(default_value, list) and _values_equal(new_value, default_value)


def _merge_preserving_user_values(
    new_value: Any,
    old_value: Any,
    default_value: Any,
    *,
    path: str,
    clear_paths: set[str],
) -> Any:
    if _is_path_cleared(path, clear_paths):
        return copy.deepcopy(new_value)

    if isinstance(new_value, dict) or isinstance(old_value, dict) or isinstance(default_value, dict):
        new_dict = new_value if isinstance(new_value, dict) else {}
        old_dict = old_value if isinstance(old_value, dict) else {}
        default_dict = default_value if isinstance(default_value, dict) else {}
        output: Dict[str, Any] = {}
        for key in sorted(set(new_dict.keys()) | set(old_dict.keys()) | set(default_dict.keys())):
            output[key] = _merge_preserving_user_values(
                new_dict.get(key),
                old_dict.get(key),
                default_dict.get(key),
                path=_child_path(path, key),
                clear_paths=clear_paths,
            )
        return output

    if isinstance(new_value, list) or isinstance(old_value, list) or isinstance(default_value, list):
        if _should_preserve_list(new_value, old_value, default_value):
            return copy.deepcopy(old_value)
        if isinstance(new_value, list):
            return copy.deepcopy(new_value)
        if isinstance(default_value, list):
            return copy.deepcopy(default_value)
        return []

    if isinstance(new_value, str) or isinstance(old_value, str) or isinstance(default_value, str):
        if _should_preserve_string(new_value, old_value, default_value):
            return copy.deepcopy(old_value)
        if new_value is None:
            return copy.deepcopy(default_value)
        return copy.deepcopy(new_value)

    if new_value is None:
        return copy.deepcopy(default_value)
    return copy.deepcopy(new_value)


def _collect_loss_paths(
    new_value: Any,
    old_value: Any,
    default_value: Any,
    *,
    path: str,
    clear_paths: set[str],
) -> List[str]:
    if _is_path_cleared(path, clear_paths):
        return []

    if isinstance(new_value, dict) or isinstance(old_value, dict) or isinstance(default_value, dict):
        new_dict = new_value if isinstance(new_value, dict) else {}
        old_dict = old_value if isinstance(old_value, dict) else {}
        default_dict = default_value if isinstance(default_value, dict) else {}
        losses: List[str] = []
        for key in sorted(set(new_dict.keys()) | set(old_dict.keys()) | set(default_dict.keys())):
            losses.extend(
                _collect_loss_paths(
                    new_dict.get(key),
                    old_dict.get(key),
                    default_dict.get(key),
                    path=_child_path(path, key),
                    clear_paths=clear_paths,
                )
            )
        return losses

    if _should_preserve_list(new_value, old_value, default_value):
        return [path]
    if _should_preserve_string(new_value, old_value, default_value):
        return [path]
    return []


def detect_suspicious_config_value_loss(
    new_cfg: Dict[str, Any],
    old_cfg: Dict[str, Any],
    *,
    clear_paths: Iterable[str] | None = None,
) -> List[str]:
    clear_set = _normalize_clear_paths(clear_paths)
    old_v3 = ensure_v3_config(old_cfg)
    new_v3 = ensure_v3_config(new_cfg)
    losses: List[str] = []
    for path in CRITICAL_SECTION_PATHS:
        losses.extend(
            _collect_loss_paths(
                _get_path(new_v3, path),
                _get_path(old_v3, path),
                _get_path(DEFAULT_CONFIG_V3, path),
                path=path,
                clear_paths=clear_set,
            )
        )
    for path in CRITICAL_VALUE_PATHS:
        losses.extend(
            _collect_loss_paths(
                _get_path(new_v3, path),
                _get_path(old_v3, path),
                _get_path(DEFAULT_CONFIG_V3, path),
                path=path,
                clear_paths=clear_set,
            )
        )
    deduped: List[str] = []
    seen: set[str] = set()
    for item in losses:
        text = str(item or "").strip()
        if text and text not in seen:
            seen.add(text)
            deduped.append(text)
    return deduped


def _should_reject_suspicious_loss(paths: Sequence[str]) -> bool:
    cleaned = [str(item or "").strip() for item in paths if str(item or "").strip()]
    if len(cleaned) >= _REJECT_LOSS_PATH_THRESHOLD:
        return True
    sections = {item.split(".", 2)[0:2][0] if "." not in item else ".".join(item.split(".")[:2]) for item in cleaned}
    return len(sections) >= _REJECT_LOSS_SECTION_THRESHOLD and len(cleaned) >= 3


def merge_user_config_payload(
    new_cfg: Dict[str, Any],
    old_cfg: Dict[str, Any],
    *,
    clear_paths: Iterable[str] | None = None,
    force_overwrite: bool = False,
) -> ConfigMergeResult:
    clear_set = _normalize_clear_paths(clear_paths)
    old_v3 = ensure_v3_config(old_cfg)
    new_v3 = ensure_v3_config(new_cfg)
    merged_v3 = merge_masked_values(new_v3, old_v3)

    suspicious = detect_suspicious_config_value_loss(merged_v3, old_v3, clear_paths=clear_set)
    if suspicious and not bool(force_overwrite) and _should_reject_suspicious_loss(suspicious):
        raise ConfigValueLossError(suspicious)

    protected = copy.deepcopy(merged_v3)
    for path in CRITICAL_SECTION_PATHS:
        _set_path(
            protected,
            path,
            _merge_preserving_user_values(
                _get_path(protected, path),
                _get_path(old_v3, path),
                _get_path(DEFAULT_CONFIG_V3, path),
                path=path,
                clear_paths=clear_set,
            ),
        )
    for path in CRITICAL_VALUE_PATHS:
        _set_path(
            protected,
            path,
            _merge_preserving_user_values(
                _get_path(protected, path),
                _get_path(old_v3, path),
                _get_path(DEFAULT_CONFIG_V3, path),
                path=path,
                clear_paths=clear_set,
            ),
        )

    return ConfigMergeResult(merged=ensure_v3_config(protected), suspicious_loss_paths=suspicious)


def build_repaired_user_config(
    source_old_cfg: Dict[str, Any],
    target_cfg: Dict[str, Any],
) -> ConfigMergeResult:
    old_v3 = ensure_v3_config(source_old_cfg)
    target_v3 = ensure_v3_config(target_cfg)
    suspicious = detect_suspicious_config_value_loss(target_v3, old_v3)

    def _merge_supplemental(preferred: Any, supplemental: Any, default_value: Any) -> Any:
        if isinstance(preferred, dict) or isinstance(supplemental, dict) or isinstance(default_value, dict):
            preferred_dict = preferred if isinstance(preferred, dict) else {}
            supplemental_dict = supplemental if isinstance(supplemental, dict) else {}
            default_dict = default_value if isinstance(default_value, dict) else {}
            output: Dict[str, Any] = {}
            for key in sorted(set(preferred_dict.keys()) | set(supplemental_dict.keys()) | set(default_dict.keys())):
                output[key] = _merge_supplemental(
                    preferred_dict.get(key),
                    supplemental_dict.get(key),
                    default_dict.get(key),
                )
            return output

        if isinstance(preferred, list) or isinstance(supplemental, list) or isinstance(default_value, list):
            if isinstance(preferred, list) and preferred:
                return copy.deepcopy(preferred)
            if isinstance(supplemental, list) and supplemental and not _values_equal(supplemental, default_value):
                return copy.deepcopy(supplemental)
            if isinstance(preferred, list):
                return copy.deepcopy(preferred)
            if isinstance(supplemental, list):
                return copy.deepcopy(supplemental)
            return copy.deepcopy(default_value if isinstance(default_value, list) else [])

        if isinstance(preferred, str) or isinstance(supplemental, str) or isinstance(default_value, str):
            preferred_text = str(preferred or "").strip()
            supplemental_text = str(supplemental or "").strip()
            default_text = str(default_value or "").strip()
            if preferred_text:
                return copy.deepcopy(preferred)
            if supplemental_text and supplemental_text != default_text:
                return copy.deepcopy(supplemental)
            if preferred_text:
                return copy.deepcopy(preferred)
            if supplemental_text:
                return copy.deepcopy(supplemental)
            return copy.deepcopy(default_value)

        if preferred is not None:
            return copy.deepcopy(preferred)
        if supplemental is not None and supplemental != default_value:
            return copy.deepcopy(supplemental)
        if preferred is not None:
            return copy.deepcopy(preferred)
        if supplemental is not None:
            return copy.deepcopy(supplemental)
        return copy.deepcopy(default_value)

    repaired = _merge_supplemental(old_v3, target_v3, DEFAULT_CONFIG_V3)
    return ConfigMergeResult(
        merged=ensure_v3_config(repaired),
        suspicious_loss_paths=suspicious,
    )
