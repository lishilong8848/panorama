from __future__ import annotations

import json
import re
import time
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List

import requests

from app.modules.feishu.service.feishu_auth_resolver import resolve_feishu_auth_settings
from app.modules.feishu.service.feishu_token_manager import feishu_token_manager
from app.shared.utils.file_utils import fallback_missing_windows_drive_path
from pipeline_utils import get_app_dir
from vendor.ali_monthly_reports.utils import cell_text, generated_file_payload, make_unique_path


_DEFAULT_APP_TOKEN = "MliKbC3fXa8PXrsndKscmxjdn1g"
_DEFAULT_TABLE_ID = "tblkh6YCMYtS8nHa"
_DEFAULT_VIEW_ID = "vewrHJHl3v"
_DEFAULT_OUTPUT_DIR = r"D:\QLDownload\月度超功率附件"
_DEFAULT_ZIP_PATTERN = "月度超功率附件_{year}{month}_{timestamp}.zip"
_RECORDS_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
_TOKEN_INVALID_CODES = {"99991661", "99991663", "99991668"}
_RETRYABLE_CODES = {"90217", "1254290", "1254607", "1255001", "1255002"}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _clean_text(value: Any) -> str:
    return cell_text(value).strip()


def _month_number(value: Any) -> int | None:
    text = _clean_text(value)
    if not text:
        return None
    match = re.search(r"(\d{1,2})", text)
    if not match:
        return None
    number = int(match.group(1))
    if 1 <= number <= 12:
        return number
    return None


def _year_text(value: Any) -> str:
    text = _clean_text(value)
    match = re.search(r"(20\d{2})", text)
    return match.group(1) if match else text


class AliMonthlyOverPowerAttachmentService:
    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        self.runtime_config = runtime_config if isinstance(runtime_config, dict) else {}

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "enabled": True,
            "app_token": _DEFAULT_APP_TOKEN,
            "table_id": _DEFAULT_TABLE_ID,
            "view_id": _DEFAULT_VIEW_ID,
            "output_dir": _DEFAULT_OUTPUT_DIR,
            "zip_file_name_pattern": _DEFAULT_ZIP_PATTERN,
            "page_size": 500,
            "max_records": 0,
        }

    def _normalize_cfg(self) -> Dict[str, Any]:
        handover_cfg = self.runtime_config.get("handover_log", {})
        if not isinstance(handover_cfg, dict):
            handover_cfg = {}
        top5_cfg = handover_cfg.get("top5_power_report", {})
        if not isinstance(top5_cfg, dict):
            top5_cfg = {}
        raw_cfg = top5_cfg.get("over_power_attachment", {})
        cfg = _deep_merge(self._defaults(), raw_cfg if isinstance(raw_cfg, dict) else {})
        cfg["enabled"] = bool(cfg.get("enabled", True))
        for key in ("app_token", "table_id", "view_id", "output_dir", "zip_file_name_pattern"):
            cfg[key] = str(cfg.get(key, "") or "").strip()
        cfg["page_size"] = max(1, min(500, int(cfg.get("page_size", 500) or 500)))
        cfg["max_records"] = max(0, int(cfg.get("max_records", 0) or 0))
        return cfg

    def _auth(self) -> Dict[str, Any]:
        common_cfg = self.runtime_config.get("common", {})
        if not isinstance(common_cfg, dict):
            common_cfg = {}
        auth_cfg = common_cfg.get("feishu_auth", {})
        auth = resolve_feishu_auth_settings(auth_cfg if isinstance(auth_cfg, dict) else {})
        if not str(auth.get("app_id", "") or "").strip() or not str(auth.get("app_secret", "") or "").strip():
            raise ValueError("飞书配置缺失: common.feishu_auth.app_id/app_secret")
        return auth

    def _token(self, *, force_refresh: bool = False) -> str:
        auth = self._auth()
        return feishu_token_manager.get_token(
            app_id=str(auth.get("app_id", "") or "").strip(),
            app_secret=str(auth.get("app_secret", "") or "").strip(),
            timeout=int(auth.get("timeout", 30) or 30),
            force_refresh=force_refresh,
        )

    def _invalidate_token(self) -> None:
        auth = self._auth()
        feishu_token_manager.invalidate(
            app_id=str(auth.get("app_id", "") or "").strip(),
            app_secret=str(auth.get("app_secret", "") or "").strip(),
        )

    def _request_json(
        self,
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        emit_log: Callable[[str], None] | None = None,
    ) -> Dict[str, Any]:
        auth = self._auth()
        retry_count = max(0, int(auth.get("request_retry_count", 3) or 3))
        retry_interval = max(0.0, float(auth.get("request_retry_interval_sec", 2) or 2))
        timeout = max(1, int(auth.get("timeout", 30) or 30))
        last_error = ""
        for api_attempt in range(1, max(2, retry_count + 2) + 1):
            for auth_attempt in range(2):
                token = self._token(force_refresh=auth_attempt > 0)
                try:
                    response = requests.get(
                        url,
                        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                        params=params or {},
                        timeout=timeout,
                    )
                    response.raise_for_status()
                    body = response.json()
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    if api_attempt <= retry_count + 1:
                        time.sleep(retry_interval * api_attempt)
                        continue
                    raise RuntimeError(f"飞书HTTP请求失败: {exc}") from exc
                code_text = str(body.get("code", "")).strip()
                if code_text == "0":
                    return body
                if auth_attempt == 0 and code_text in _TOKEN_INVALID_CODES:
                    self._invalidate_token()
                    continue
                if code_text in _RETRYABLE_CODES and api_attempt <= retry_count + 1:
                    last_error = json.dumps(body, ensure_ascii=False)
                    if callable(emit_log):
                        emit_log(f"[月度超功率附件] 飞书暂不可用，稍后重试: code={code_text}, attempt={api_attempt}")
                    time.sleep(max(1.0, retry_interval) * api_attempt)
                    break
                raise RuntimeError(f"飞书接口调用失败: {body}")
        raise RuntimeError(f"飞书接口调用失败: 重试后仍失败 {last_error}")

    def _list_records(
        self,
        cfg: Dict[str, Any],
        *,
        emit_log: Callable[[str], None] | None = None,
    ) -> List[Dict[str, Any]]:
        url = _RECORDS_URL.format(app_token=cfg["app_token"], table_id=cfg["table_id"])
        records: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            params: Dict[str, Any] = {
                "view_id": cfg["view_id"],
                "page_size": cfg["page_size"],
            }
            if page_token:
                params["page_token"] = page_token
            body = self._request_json(url, params=params, emit_log=emit_log)
            data = body.get("data") if isinstance(body, dict) else {}
            items = data.get("items") if isinstance(data, dict) else []
            for item in items if isinstance(items, list) else []:
                if isinstance(item, dict):
                    records.append(item)
                    if cfg["max_records"] and len(records) >= cfg["max_records"]:
                        return records[: cfg["max_records"]]
            if not bool(data.get("has_more")):
                break
            page_token = str(data.get("page_token", "") or "").strip()
            if not page_token:
                break
        return records

    @staticmethod
    def _record_matches_year_month(record: Dict[str, Any], *, year: str, month: int) -> bool:
        fields = record.get("fields", {}) if isinstance(record.get("fields", {}), dict) else {}
        record_year = _year_text(fields.get("年度", ""))
        record_month = _month_number(fields.get("月份", ""))
        if record_year and record_year != str(year):
            return False
        return record_month == int(month)

    @staticmethod
    def _attachment_matches(file_name: str) -> bool:
        name = str(file_name or "").strip()
        upper_name = name.upper()
        return ("超功耗" in name or "超功率" in name) and "TOP5" not in upper_name

    def _resolve_output_dir(self, cfg: Dict[str, Any], *, year: str, month: int) -> Path:
        raw_dir = str(cfg.get("output_dir", "") or "").strip() or _DEFAULT_OUTPUT_DIR
        path = Path(raw_dir)
        if path.is_absolute():
            root = fallback_missing_windows_drive_path(path, app_dir=get_app_dir())
        else:
            root = get_app_dir() / path
        period_dir = root / f"{year}{month:02d}"
        period_dir.mkdir(parents=True, exist_ok=True)
        return period_dir

    @staticmethod
    def _build_zip_path(cfg: Dict[str, Any], output_dir: Path, *, year: str, month: int) -> Path:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        pattern = str(cfg.get("zip_file_name_pattern", "") or _DEFAULT_ZIP_PATTERN).strip() or _DEFAULT_ZIP_PATTERN
        file_name = pattern.format(year=year, month=f"{month:02d}", timestamp=timestamp)
        if not file_name.lower().endswith(".zip"):
            file_name += ".zip"
        return make_unique_path(output_dir, file_name)

    def _download_attachment(self, url: str, save_path: Path, token: str) -> None:
        response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=60)
        if response.status_code in {401, 403}:
            self._invalidate_token()
            token = self._token(force_refresh=True)
            response = requests.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=60)
        if response.status_code != 200:
            raise RuntimeError(f"附件下载失败: HTTP {response.status_code}")
        with save_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)

    def run(
        self,
        *,
        year: str,
        month: int,
        emit_log: Callable[[str], None] = print,
    ) -> Dict[str, Any]:
        cfg = self._normalize_cfg()
        if not cfg["enabled"]:
            raise RuntimeError("月度超功率/超功耗附件获取已禁用")
        target_year = str(year or "").strip()
        if not re.fullmatch(r"20\d{2}", target_year):
            raise ValueError("year 必须为四位年份")
        target_month = int(month)
        if target_month < 1 or target_month > 12:
            raise ValueError("month 必须在 1-12 之间")
        missing = [key for key in ("app_token", "table_id", "view_id") if not str(cfg.get(key, "") or "").strip()]
        if missing:
            raise ValueError(f"月度超功率附件配置缺失: {', '.join(missing)}")

        started_at = time.strftime("%Y-%m-%d %H:%M:%S")
        emit_log(f"[月度超功率附件] 开始读取高功率月报表: year={target_year}, month={target_month:02d}")
        records = self._list_records(cfg, emit_log=emit_log)
        matched_records = [
            record
            for record in records
            if self._record_matches_year_month(record, year=target_year, month=target_month)
        ]
        if not matched_records:
            raise RuntimeError(f"没有找到 {target_year}年{target_month}月 的超功率记录")

        output_dir = self._resolve_output_dir(cfg, year=target_year, month=target_month)
        token = self._token()
        downloaded: List[Dict[str, Any]] = []
        errors: List[str] = []
        paths: List[Path] = []
        for record_index, record in enumerate(matched_records, start=1):
            fields = record.get("fields", {}) if isinstance(record.get("fields", {}), dict) else {}
            for field_name, field_value in fields.items():
                if not isinstance(field_value, list) or not field_value:
                    continue
                if not isinstance(field_value[0], dict):
                    continue
                if not any(isinstance(item, dict) and (item.get("url") or item.get("tmp_url")) for item in field_value):
                    continue
                for attachment in field_value:
                    file_name = str(attachment.get("name", "") or f"attachment_{record_index}").strip()
                    if not self._attachment_matches(file_name):
                        continue
                    safe_name = "".join("_" if ord(ch) < 32 or ch in '<>:"/\\|?*' else ch for ch in Path(file_name).name).strip(" .")
                    safe_name = safe_name or f"attachment_{record_index}.xlsx"
                    save_path = make_unique_path(output_dir, safe_name)
                    url = str(attachment.get("url", "") or attachment.get("tmp_url", "") or "").strip()
                    if not url:
                        errors.append(f"{file_name}: 无下载 URL")
                        continue
                    try:
                        emit_log(f"[月度超功率附件] 下载附件: field={field_name}, file={save_path.name}")
                        self._download_attachment(url, save_path, token)
                        paths.append(save_path)
                        downloaded.append(generated_file_payload(save_path))
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"{file_name}: {exc}")
        if not paths:
            detail = f"，失败明细: {'; '.join(errors[:5])}" if errors else ""
            raise RuntimeError(f"未匹配到月度超功率/超功耗附件{detail}")

        zip_path = self._build_zip_path(cfg, output_dir, year=target_year, month=target_month)
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in paths:
                archive.write(path, arcname=path.name)
        finished_at = time.strftime("%Y-%m-%d %H:%M:%S")
        emit_log(
            "[月度超功率附件] 获取完成: "
            f"records={len(matched_records)}, files={len(paths)}, errors={len(errors)}, zip={zip_path.name}"
        )
        return {
            "status": "ok",
            "report_type": "top5_over_power_attachment",
            "year": target_year,
            "month": f"{target_month:02d}",
            "started_at": started_at,
            "finished_at": finished_at,
            "record_count": len(records),
            "matched_record_count": len(matched_records),
            "downloaded_count": len(paths),
            "error_count": len(errors),
            "errors": errors,
            "files": downloaded,
            "zip_file": str(zip_path),
            "zip_file_name": zip_path.name,
            "output_dir": str(output_dir),
        }
