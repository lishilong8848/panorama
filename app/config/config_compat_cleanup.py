from __future__ import annotations

import copy
from typing import Any, Dict


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def sanitize_day_metric_upload_config(day_metric_upload: Dict[str, Any] | None) -> Dict[str, Any]:
    sanitized = copy.deepcopy(_dict(day_metric_upload))
    sanitized.pop("enabled", None)
    sanitized.pop("manual_button_enabled", None)
    behavior = _dict(sanitized.get("behavior"))
    behavior.pop("only_day_shift", None)
    behavior.pop("rewrite_existing", None)
    behavior.pop("local_import_enabled", None)
    behavior.pop("failure_policy", None)
    behavior.pop("local_import_scope", None)
    sanitized["behavior"] = behavior
    target = _dict(sanitized.get("target"))
    source = _dict(target.get("source"))
    source.pop("base_url", None)
    source.pop("wiki_url", None)
    target["source"] = source
    target.pop("types", None)
    sanitized["target"] = target
    return sanitized


def sanitize_alarm_export_config(alarm_export: Dict[str, Any] | None) -> Dict[str, Any]:
    sanitized = copy.deepcopy(_dict(alarm_export))
    sanitized.pop("manual_button_enabled", None)
    shared_source_upload = _dict(sanitized.get("shared_source_upload"))
    shared_source_upload.pop("target", None)
    sanitized["shared_source_upload"] = shared_source_upload
    return sanitized


def sanitize_wet_bulb_collection_config(wet_bulb_collection: Dict[str, Any] | None) -> Dict[str, Any]:
    sanitized = copy.deepcopy(_dict(wet_bulb_collection))
    sanitized.pop("manual_button_enabled", None)
    source = _dict(sanitized.get("source"))
    source.pop("switch_to_internal_before_download", None)
    sanitized["source"] = source
    target = _dict(sanitized.get("target"))
    target.pop("base_url", None)
    target.pop("wiki_url", None)
    sanitized["target"] = target
    return sanitized
