from __future__ import annotations

from typing import Any, Dict, List


class CoolingPumpPressureDefaultsService:
    DEFAULTS_KEY = "cooling_pump_pressures"
    ZONE_LABELS = {"west": "西区", "east": "东区"}

    @staticmethod
    def _text(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @classmethod
    def _unit_key(cls, *, zone: str, unit: int) -> str:
        return f"{cls._text(zone).lower()}:{int(unit or 0)}"

    @classmethod
    def normalize_defaults(cls, raw: Any) -> Dict[str, Dict[str, str]]:
        payload = raw.get("items", raw) if isinstance(raw, dict) else raw
        if isinstance(payload, dict):
            iterable = []
            for key, value in payload.items():
                if not isinstance(value, dict):
                    continue
                zone, _, unit_text = str(key or "").partition(":")
                merged = dict(value)
                merged.setdefault("zone", zone)
                merged.setdefault("unit", unit_text)
                iterable.append(merged)
        elif isinstance(payload, list):
            iterable = payload
        else:
            iterable = []
        output: Dict[str, Dict[str, str]] = {}
        for item in iterable:
            if not isinstance(item, dict):
                continue
            zone = cls._text(item.get("zone")).lower()
            if zone not in cls.ZONE_LABELS:
                continue
            try:
                unit = int(item.get("unit", 0) or 0)
            except Exception:  # noqa: BLE001
                unit = 0
            if unit <= 0:
                continue
            inlet = cls._text(item.get("inlet_pressure"))
            outlet = cls._text(item.get("outlet_pressure"))
            if not inlet and not outlet:
                continue
            output[cls._unit_key(zone=zone, unit=unit)] = {
                "zone": zone,
                "unit": str(unit),
                "inlet_pressure": inlet,
                "outlet_pressure": outlet,
            }
        return output

    @classmethod
    def normalize_rows(cls, raw: Any) -> List[Dict[str, Any]]:
        payload = raw.get("rows", raw) if isinstance(raw, dict) else raw
        if not isinstance(payload, list):
            return []
        rows: List[Dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            zone = cls._text(item.get("zone")).lower()
            if zone not in cls.ZONE_LABELS:
                continue
            try:
                unit = int(item.get("unit", 0) or 0)
            except Exception:  # noqa: BLE001
                unit = 0
            if unit <= 0:
                continue
            position = int(item.get("position", 0) or 0)
            rows.append(
                {
                    "row_id": f"{zone}:{unit}",
                    "zone": zone,
                    "zone_label": cls.ZONE_LABELS[zone],
                    "unit": unit,
                    "unit_label": cls._text(item.get("unit_label")) or f"{unit}#制冷单元",
                    "position": position,
                    "mode_text": cls._text(item.get("mode_text")),
                    "inlet_pressure": cls._text(item.get("inlet_pressure")),
                    "outlet_pressure": cls._text(item.get("outlet_pressure")),
                }
            )
        rows.sort(key=lambda row: (0 if row["zone"] == "west" else 1, int(row.get("position", 0) or 0), int(row.get("unit", 0) or 0)))
        return rows

    @classmethod
    def build_rows(
        cls,
        *,
        running_units: Dict[str, List[Dict[str, Any]]],
        defaults: Any,
        existing_rows: Any = None,
    ) -> List[Dict[str, Any]]:
        default_map = cls.normalize_defaults(defaults)
        existing_map = {
            cls._unit_key(zone=str(row.get("zone", "")).lower(), unit=int(row.get("unit", 0) or 0)): row
            for row in cls.normalize_rows(existing_rows)
        }
        rows: List[Dict[str, Any]] = []
        running = running_units if isinstance(running_units, dict) else {}
        for zone in ("west", "east"):
            active_units = list(running.get(zone, []) if isinstance(running.get(zone, []), list) else [])[:2]
            for position, unit_info in enumerate(active_units):
                try:
                    unit = int(unit_info.get("unit", 0) or 0)
                except Exception:  # noqa: BLE001
                    unit = 0
                if unit <= 0:
                    continue
                key = cls._unit_key(zone=zone, unit=unit)
                source = existing_map.get(key) or default_map.get(key) or {}
                rows.append(
                    {
                        "row_id": key,
                        "zone": zone,
                        "zone_label": cls.ZONE_LABELS[zone],
                        "unit": unit,
                        "unit_label": f"{unit}#制冷单元",
                        "position": position,
                        "mode_text": cls._text(unit_info.get("mode_text")),
                        "inlet_pressure": cls._text(source.get("inlet_pressure")),
                        "outlet_pressure": cls._text(source.get("outlet_pressure")),
                    }
                )
        return rows

    @classmethod
    def document_payload(
        cls,
        *,
        running_units: Dict[str, List[Dict[str, Any]]],
        defaults: Any,
        existing_rows: Any = None,
    ) -> Dict[str, Any]:
        return {
            "rows": cls.build_rows(
                running_units=running_units,
                defaults=defaults,
                existing_rows=existing_rows,
            )
        }

    @classmethod
    def merge_document_rows_into_defaults(cls, *, existing_defaults: Any, document_payload: Any) -> Dict[str, Dict[str, str]]:
        merged = cls.normalize_defaults(existing_defaults)
        for row in cls.normalize_rows(document_payload):
            key = cls._unit_key(zone=str(row.get("zone", "")).lower(), unit=int(row.get("unit", 0) or 0))
            inlet = cls._text(row.get("inlet_pressure"))
            outlet = cls._text(row.get("outlet_pressure"))
            if inlet or outlet:
                merged[key] = {
                    "zone": str(row.get("zone", "")).lower(),
                    "unit": str(int(row.get("unit", 0) or 0)),
                    "inlet_pressure": inlet,
                    "outlet_pressure": outlet,
                }
            else:
                merged.pop(key, None)
        return merged

    @classmethod
    def signature(cls, payload: Any) -> str:
        parts = []
        for row in cls.normalize_rows(payload):
            parts.append(
                f"{row.get('zone')}:{row.get('unit')}={cls._text(row.get('inlet_pressure'))}/{cls._text(row.get('outlet_pressure'))}"
            )
        return "|".join(parts)
