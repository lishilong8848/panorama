from __future__ import annotations

import base64
import hashlib
import json
import re
import threading
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List
from urllib.parse import urlparse

import requests
from PIL import Image

from app.core.app_state import AppStateRepository
from app.modules.feishu.service.bitable_client_runtime import FeishuBitableClient
from app.modules.feishu.service.feishu_auth_resolver import require_feishu_auth_settings
from app.modules.report_pipeline.core.metrics_math import date_text_to_timestamp_ms
from pipeline_utils import get_app_dir


STATION_H_SIGNATURE_APP_TOKEN = "HU38bc1vnamMK9sCeOgclUvXnFc"
STATION_H_SIGNATURE_TABLES = (
    ("tbluozblhRAjbljX", "人员签名"),
    ("tblC77nllNrprHBY", "公司外人员签名"),
)
STATION_H_SIGNATURE_CACHE_NAMESPACE = "station_h_signature_directory"
STATION_H_SIGNATURE_CACHE_KEY = "latest"
_SIGNATURE_MAGIC_V2 = b"CLIPFLOW_SIGENC_V2\n"
_MAX_SIGNATURE_ATTACHMENT_BYTES = 20 * 1024 * 1024
_MAX_SIGNATURE_IMAGE_PIXELS = 25_000_000
_SIGNATURE_DIRECTORY_REFRESH_LOCK = threading.Lock()


class StationHSignatureError(RuntimeError):
    pass


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value).strip()
    if isinstance(value, list):
        return "、".join(item for item in (_text(item) for item in value) if item)
    if isinstance(value, dict):
        for key in ("text", "name", "value", "en_name", "display_name"):
            result = _text(value.get(key))
            if result:
                return result
        return "、".join(item for item in (_text(item) for item in value.values()) if item)
    return str(value).strip()


def _name_key(value: Any) -> str:
    return re.sub(r"\s+", "", _text(value)).casefold()


def _metadata(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        if "version" in value:
            return dict(value)
        for key in ("text", "value"):
            nested = _metadata(value.get(key))
            if nested:
                return nested
        return {}
    if isinstance(value, list):
        return _metadata("".join(_text(item) for item in value))
    text = _text(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _b64decode(value: Any) -> bytes:
    text = _text(value)
    if not text:
        return b""
    return base64.urlsafe_b64decode((text + "=" * (-len(text) % 4)).encode("ascii"))


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _version(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _inactive(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, list):
        return any(_inactive(item) for item in value)
    if isinstance(value, dict):
        known = False
        for key in ("checked", "value", "text", "name"):
            if key not in value:
                continue
            known = True
            if _inactive(value.get(key)):
                return True
        return False if known else any(_inactive(item) for item in value.values())
    text = str(value).strip().casefold()
    if not text:
        return False
    return text not in {"false", "0", "否", "未勾选", "未选", "no", "none", "null"}


def _runtime_config(config: Dict[str, Any]) -> Dict[str, Any]:
    payload = config if isinstance(config, dict) else {}
    paths = payload.get("paths", {}) if isinstance(payload.get("paths", {}), dict) else {}
    global_paths = payload.get("_global_paths", {}) if isinstance(payload.get("_global_paths", {}), dict) else {}
    runtime_state_root = _text(paths.get("runtime_state_root")) or _text(global_paths.get("runtime_state_root"))
    return {"paths": {"runtime_state_root": runtime_state_root}} if runtime_state_root else {}


class StationHSignatureService:
    """Read-only signature directory and portable V2 signature decryption."""

    def __init__(
        self,
        config: Dict[str, Any] | None = None,
        *,
        app_state_repository: AppStateRepository | None = None,
        app_dir: Path | None = None,
        client_factory: Callable[[str], FeishuBitableClient] | None = None,
        emit_log: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config if isinstance(config, dict) else {}
        self._repository = app_state_repository or self._build_repository(app_dir=app_dir)
        self._client_factory = client_factory
        self._emit_log = emit_log

    def _build_repository(self, *, app_dir: Path | None) -> AppStateRepository | None:
        try:
            repository = AppStateRepository(
                runtime_config=_runtime_config(self.config),
                app_dir=app_dir or get_app_dir(),
            )
            repository.ensure_ready()
            return repository
        except Exception:
            return None

    def _new_client(self, table_id: str) -> FeishuBitableClient:
        if callable(self._client_factory):
            return self._client_factory(table_id)
        auth = require_feishu_auth_settings(self.config)
        return FeishuBitableClient(
            app_id=_text(auth.get("app_id")),
            app_secret=_text(auth.get("app_secret")),
            app_token=STATION_H_SIGNATURE_APP_TOKEN,
            calc_table_id=table_id,
            attachment_table_id=table_id,
            timeout=int(auth.get("timeout", 30) or 30),
            request_retry_count=int(auth.get("request_retry_count", 3) or 3),
            request_retry_interval_sec=float(auth.get("request_retry_interval_sec", 2) or 2),
            date_text_to_timestamp_ms_fn=date_text_to_timestamp_ms,
            canonical_metric_name_fn=lambda value: _text(value),
            dimension_mapping={},
            emit_log=self._emit_log,
        )

    @staticmethod
    def _attachment(fields: Dict[str, Any]) -> Dict[str, Any]:
        raw = fields.get("手写签名", []) if isinstance(fields, dict) else []
        if not isinstance(raw, list):
            return {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            name = _text(item.get("name")) or _text(item.get("file_name")) or _text(item.get("filename"))
            if not name.lower().endswith(".sigenc"):
                continue
            file_token = _text(item.get("file_token")) or _text(item.get("token"))
            url = (
                _text(item.get("download_url"))
                or _text(item.get("url"))
                or _text(item.get("tmp_url"))
            )
            try:
                size = int(item.get("size", item.get("file_size", -1)) or 0)
            except (TypeError, ValueError):
                size = -1
            if (file_token or url) and size != 0:
                return dict(item)
        return {}

    @staticmethod
    def _signature_revision(
        *,
        attachment: Dict[str, Any],
        metadata: Dict[str, Any],
        record: Dict[str, Any],
    ) -> str:
        if not attachment:
            return ""
        payload = {
            "file_token": (
                _text(attachment.get("file_token"))
                or _text(attachment.get("fileToken"))
                or _text(attachment.get("token"))
            ),
            "file_name": (
                _text(attachment.get("name"))
                or _text(attachment.get("file_name"))
                or _text(attachment.get("filename"))
            ),
            "file_size": attachment.get("size", attachment.get("file_size", "")),
            "encrypted_sha256": _text(metadata.get("encrypted_sha256")),
            "signature_sha256": _text(metadata.get("signature_sha256")),
            "record_modified_at": (
                _text(record.get("last_modified_time"))
                or _text(record.get("modified_time"))
                or _text(record.get("updated_at"))
            ),
        }
        return hashlib.sha256(_canonical_json(payload)).hexdigest()

    @classmethod
    def _person_from_record(cls, record: Dict[str, Any], *, table_id: str, source_label: str) -> Dict[str, Any]:
        fields = record.get("fields", {}) if isinstance(record.get("fields", {}), dict) else {}
        name = _text(fields.get("姓名")) or _text(fields.get("员工姓名")) or _text(fields.get("人员姓名"))
        record_id = _text(record.get("record_id"))
        attachment = cls._attachment(fields)
        metadata = _metadata(fields.get("密钥"))
        is_v2 = (
            _version(metadata.get("version")) == 2
            and bool(_text(metadata.get("portable_dek")))
            and bool(_text(metadata.get("file_nonce")))
        )
        inactive = _inactive(fields.get("离职/异动情况"))
        return {
            "selection_id": f"{table_id}:{record_id}" if table_id and record_id else "",
            "record_id": record_id,
            "table_id": table_id,
            "source_label": source_label,
            "name": name,
            "employee_no": (
                _text(fields.get("员工工号"))
                or _text(fields.get("工号"))
                or _text(fields.get("员工ID"))
                or _text(fields.get("人员ID"))
                or _text(fields.get("SourceID"))
            ),
            "building": (
                _text(fields.get("楼栋（用）"))
                or _text(fields.get("楼栋"))
                or _text(fields.get("机楼/专业"))
                or _text(fields.get("专业"))
            ),
            "team": _text(fields.get("班组")) or _text(fields.get("班组名称")) or _text(fields.get("团队")),
            "role": _text(fields.get("岗位")) or _text(fields.get("岗位性质")) or _text(fields.get("职务")),
            "signature_revision": cls._signature_revision(
                attachment=attachment,
                metadata=metadata,
                record=record,
            ),
            "available": bool(name and record_id and attachment and is_v2 and not inactive),
            "availability": "available" if attachment and is_v2 and not inactive else (
                "inactive" if inactive else ("legacy" if attachment else "missing")
            ),
        }

    @staticmethod
    def _sort_key(person: Dict[str, Any]) -> tuple[int, int, str, str]:
        building = _text(person.get("building"))
        return (
            0 if "H楼" in building or re.search(r"(^|[^A-Z])H([^A-Z]|$)", building, flags=re.IGNORECASE) else 1,
            0 if bool(person.get("available")) else 1,
            _text(person.get("name")),
            _text(person.get("employee_no")),
        )

    def cached_directory(self) -> Dict[str, Any]:
        repository = self._repository
        if repository is None:
            return {"people": [], "refreshed_at": "", "error": "签名目录状态库不可用"}
        try:
            payload = repository.get_runtime_kv(STATION_H_SIGNATURE_CACHE_NAMESPACE, STATION_H_SIGNATURE_CACHE_KEY)
        except Exception as exc:
            return {"people": [], "refreshed_at": "", "error": str(exc)}
        raw = payload if isinstance(payload, dict) else {}
        people = raw.get("people", []) if isinstance(raw.get("people", []), list) else []
        normalized = [dict(item) for item in people if isinstance(item, dict) and _text(item.get("selection_id"))]
        normalized.sort(key=self._sort_key)
        return {
            "people": normalized,
            "refreshed_at": _text(raw.get("refreshed_at")),
            "error": _text(raw.get("error")),
            "source_counts": dict(raw.get("source_counts", {})) if isinstance(raw.get("source_counts", {}), dict) else {},
        }

    def refresh_directory(self) -> Dict[str, Any]:
        from datetime import datetime

        with _SIGNATURE_DIRECTORY_REFRESH_LOCK:
            people: List[Dict[str, Any]] = []
            source_counts: Dict[str, int] = {}
            for table_id, source_label in STATION_H_SIGNATURE_TABLES:
                client = self._new_client(table_id)
                records = client.list_records(table_id=table_id, page_size=500)
                source_counts[table_id] = len(records)
                for record in records:
                    if not isinstance(record, dict):
                        continue
                    person = self._person_from_record(record, table_id=table_id, source_label=source_label)
                    if person.get("selection_id") and person.get("name"):
                        people.append(person)

            deduped: Dict[str, Dict[str, Any]] = {}
            for person in sorted(people, key=self._sort_key):
                deduped.setdefault(_text(person.get("selection_id")), person)
            result_people = list(deduped.values())
            result_people.sort(key=self._sort_key)
            payload = {
                "people": result_people,
                "refreshed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source_counts": source_counts,
                "error": "",
            }
            if self._repository is not None:
                self._repository.put_runtime_kv(
                    STATION_H_SIGNATURE_CACHE_NAMESPACE,
                    STATION_H_SIGNATURE_CACHE_KEY,
                    payload,
                )
            return payload

    @staticmethod
    def match_person(people: Iterable[Dict[str, Any]], name: Any) -> Dict[str, Any] | None:
        target = _name_key(name)
        if not target:
            return None
        matches = [
            item for item in people
            if isinstance(item, dict) and _name_key(item.get("name")) == target and bool(item.get("available"))
        ]
        matches.sort(key=StationHSignatureService._sort_key)
        return dict(matches[0]) if matches else None

    def _fetch_signature_record(self, *, table_id: str, record_id: str) -> tuple[FeishuBitableClient, Dict[str, Any]]:
        allowed = {item[0] for item in STATION_H_SIGNATURE_TABLES}
        if table_id not in allowed or not record_id:
            raise StationHSignatureError("签名人员选择无效，请刷新签名后重新选择")
        client = self._new_client(table_id)
        record = client.get_record_by_id(table_id=table_id, record_id=record_id)
        if not isinstance(record, dict) or not record:
            raise StationHSignatureError("签名记录不存在，请刷新签名后重新选择")
        return client, record

    @staticmethod
    def _download_encrypted_attachment(client: FeishuBitableClient, attachment: Dict[str, Any]) -> bytes:
        url = (
            _text(attachment.get("download_url"))
            or _text(attachment.get("url"))
            or _text(attachment.get("tmp_url"))
        )
        file_token = (
            _text(attachment.get("file_token"))
            or _text(attachment.get("fileToken"))
            or _text(attachment.get("token"))
        )
        if not url and file_token:
            url = f"https://open.feishu.cn/open-apis/drive/v1/medias/{file_token}/download"
        if not url:
            raise StationHSignatureError("签名附件缺少下载地址")
        if urlparse(url).scheme.lower() not in {"http", "https"}:
            raise StationHSignatureError("签名附件下载地址无效")

        last_error = ""
        for attempt in range(1, 4):
            token = client.refresh_token(force=attempt > 1)
            try:
                response = requests.get(
                    url,
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=(10, max(30, int(getattr(client, "timeout", 30) or 30))),
                    stream=True,
                )
                with response:
                    if response.status_code in {401, 403}:
                        client.invalidate_token()
                        last_error = f"HTTP {response.status_code}"
                        continue
                    if response.status_code >= 400:
                        raise StationHSignatureError(f"签名附件下载失败: HTTP {response.status_code}")
                    try:
                        content_length = int(response.headers.get("Content-Length", "0") or 0)
                    except (TypeError, ValueError):
                        content_length = 0
                    if content_length > _MAX_SIGNATURE_ATTACHMENT_BYTES:
                        raise StationHSignatureError("签名附件超过20MB限制")
                    chunks: List[bytes] = []
                    total = 0
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        if not chunk:
                            continue
                        total += len(chunk)
                        if total > _MAX_SIGNATURE_ATTACHMENT_BYTES:
                            raise StationHSignatureError("签名附件超过20MB限制")
                        chunks.append(bytes(chunk))
                    if total <= 0:
                        raise StationHSignatureError("签名附件为空")
                    return b"".join(chunks)
            except requests.RequestException as exc:
                last_error = str(exc)
                continue
        raise StationHSignatureError(f"签名附件下载失败: {last_error or '未知错误'}")

    @staticmethod
    def _decrypt_portable_v2(encrypted_file: bytes, metadata: Dict[str, Any]) -> bytes:
        if not encrypted_file.startswith(_SIGNATURE_MAGIC_V2):
            raise StationHSignatureError("签名附件不是 CLIPFLOW_SIGENC_V2 格式")
        if _version(metadata.get("version")) != 2:
            raise StationHSignatureError("签名密钥不是便携式 V2 格式")
        expected_encrypted = _text(metadata.get("encrypted_sha256"))
        if expected_encrypted and hashlib.sha256(encrypted_file).hexdigest() != expected_encrypted:
            raise StationHSignatureError("签名附件与密钥记录不匹配")
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ModuleNotFoundError as exc:
            raise StationHSignatureError("缺少 cryptography 依赖，无法解密手写签名") from exc
        try:
            key = _b64decode(metadata.get("portable_dek"))
            nonce = _b64decode(metadata.get("file_nonce"))
        except Exception as exc:
            raise StationHSignatureError("签名密钥编码无效") from exc
        if len(key) != 32 or len(nonce) != 12:
            raise StationHSignatureError("签名密钥或随机数长度错误")
        try:
            plain = AESGCM(key).decrypt(
                nonce,
                encrypted_file[len(_SIGNATURE_MAGIC_V2):],
                _canonical_json(metadata.get("aad") or {}),
            )
        except Exception as exc:
            raise StationHSignatureError("签名解密失败，附件或密钥可能已更新") from exc
        expected_plain = _text(metadata.get("signature_sha256"))
        if not expected_plain:
            raise StationHSignatureError("签名密钥缺少明文校验值")
        if hashlib.sha256(plain).hexdigest() != expected_plain:
            raise StationHSignatureError("签名解密校验失败")
        try:
            from io import BytesIO

            with Image.open(BytesIO(plain)) as image:
                width, height = image.size
                if width <= 0 or height <= 0 or width * height > _MAX_SIGNATURE_IMAGE_PIXELS:
                    raise StationHSignatureError("签名图片尺寸无效或过大")
                image.verify()
        except StationHSignatureError:
            raise
        except Exception as exc:
            raise StationHSignatureError("签名解密结果不是有效图片") from exc
        return plain

    def resolve_signature_png(self, *, table_id: str, record_id: str) -> Dict[str, Any]:
        client, record = self._fetch_signature_record(table_id=table_id, record_id=record_id)
        fields = record.get("fields", {}) if isinstance(record.get("fields", {}), dict) else {}
        attachment = self._attachment(fields)
        metadata = _metadata(fields.get("密钥"))
        if not attachment:
            raise StationHSignatureError("所选人员没有手写签名附件")
        encrypted = self._download_encrypted_attachment(client, attachment)
        png = self._decrypt_portable_v2(encrypted, metadata)
        return {
            "name": _text(fields.get("姓名")) or _text(fields.get("员工姓名")) or _text(fields.get("人员姓名")),
            "record_id": record_id,
            "table_id": table_id,
            "png": png,
            "sha256": hashlib.sha256(png).hexdigest(),
        }
