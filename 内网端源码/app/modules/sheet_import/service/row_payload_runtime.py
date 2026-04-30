from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple


def prepare_row_payloads_for_table(
    *,
    raw_rows: List[Any],
    table_fields: List[Dict[str, Any]],
    tz_offset_hours: int,
    normalize_field_name: Callable[[str], str],
    convert_value_for_field: Callable[[Any, Dict[str, Any], int], Tuple[Any, bool]],
    row_payload_factory: Callable[[int, Dict[str, Any]], Any],
) -> Tuple[List[Any], Dict[str, int]]:
    exact_map: Dict[str, Dict[str, Any]] = {}
    norm_map: Dict[str, List[Dict[str, Any]]] = {}
    for item in table_fields:
        name = str(item.get("field_name", "")).strip()
        if not name:
            continue
        field_type = int(item.get("type", 0) or 0)
        prop = item.get("property") if isinstance(item.get("property"), dict) else {}
        options: List[str] = []
        raw_options = prop.get("options", [])
        if isinstance(raw_options, list):
            for opt in raw_options:
                if isinstance(opt, dict):
                    opt_name = str(opt.get("name", "")).strip()
                    if opt_name:
                        options.append(opt_name)
        meta = {
            "name": name,
            "type": field_type,
            "options": options,
        }
        exact_map[name] = meta
        norm = normalize_field_name(name)
        if norm:
            norm_map.setdefault(norm, []).append(meta)

    def resolve_meta(excel_key: str) -> Optional[Dict[str, Any]]:
        key = str(excel_key).strip()
        if not key:
            return None
        if key in exact_map:
            return exact_map[key]

        norm = normalize_field_name(key)
        if norm in norm_map and len(norm_map[norm]) == 1:
            return norm_map[norm][0]

        key_wo_dup = re.sub(r"_\d+$", "", key)
        norm2 = normalize_field_name(key_wo_dup)
        if norm2 in norm_map and len(norm_map[norm2]) == 1:
            return norm_map[norm2][0]

        fuzzy: List[Dict[str, Any]] = []
        for candidate_norm, metas in norm_map.items():
            if norm and (candidate_norm.startswith(norm) or norm.startswith(candidate_norm)):
                fuzzy.extend(metas)
        if len(fuzzy) == 1:
            return fuzzy[0]
        return None

    stats = {
        "dropped_rows": 0,
        "skipped_missing_fields": 0,
        "skipped_unsupported_values": 0,
        "skipped_invalid_values": 0,
    }
    match_cache: Dict[str, Optional[Dict[str, Any]]] = {}
    prepared_rows: List[Any] = []

    for raw in raw_rows:
        out: Dict[str, Any] = {}
        for excel_key, value in raw.fields.items():
            if excel_key not in match_cache:
                match_cache[excel_key] = resolve_meta(excel_key)
            meta = match_cache[excel_key]
            if meta is None:
                if value is not None and str(value).strip() != "":
                    stats["skipped_missing_fields"] += 1
                continue
            converted, supported = convert_value_for_field(value, meta, tz_offset_hours)
            if converted is None:
                if value is not None and str(value).strip() != "":
                    if supported:
                        stats["skipped_invalid_values"] += 1
                    else:
                        stats["skipped_unsupported_values"] += 1
                continue
            if meta["name"] not in out:
                out[meta["name"]] = converted
        if out:
            prepared_rows.append(row_payload_factory(raw.row_index, out))
        else:
            stats["dropped_rows"] += 1

    return prepared_rows, stats


def prepare_rows_for_table(
    *,
    raw_rows: List[Dict[str, Any]],
    table_fields: List[Dict[str, Any]],
    tz_offset_hours: int,
    prepare_row_payloads_for_table: Callable[..., Tuple[List[Any], Dict[str, int]]],
    row_payload_factory: Callable[[int, Dict[str, Any]], Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    payloads = [row_payload_factory(i + 1, row) for i, row in enumerate(raw_rows)]
    prepared_payloads, stats = prepare_row_payloads_for_table(
        raw_rows=payloads,
        table_fields=table_fields,
        tz_offset_hours=tz_offset_hours,
    )
    return [item.fields for item in prepared_payloads], stats
