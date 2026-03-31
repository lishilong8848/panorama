from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
import shutil
from typing import Any, Dict
from urllib.parse import urlencode

from PIL import Image

from app.shared.utils.runtime_temp_workspace import resolve_runtime_state_root
from app.shared.utils.atomic_file import atomic_write_bytes, atomic_write_file, validate_image_file


class HandoverDailyReportAssetService:
    ASSET_ROOT = Path("handover") / "daily_report_assets"
    RETENTION_DAYS = 30
    VALID_TARGETS = {"summary_sheet", "external_page"}
    VALID_VARIANTS = {"auto", "manual", "effective"}
    THUMBNAIL_MAX_WIDTH = 480

    def __init__(self, handover_cfg: Dict[str, Any]) -> None:
        self.handover_cfg = handover_cfg if isinstance(handover_cfg, dict) else {}

    def _runtime_root(self) -> Path:
        return resolve_runtime_state_root(
            runtime_config={"paths": self.handover_cfg.get("_global_paths", {})},
            app_dir=Path(__file__).resolve().parents[2],
        )

    @staticmethod
    def _safe_batch_dir_name(duty_date: str, duty_shift: str) -> str:
        return f"{str(duty_date or '').strip()}_{str(duty_shift or '').strip().lower()}"

    def get_batch_dir(self, *, duty_date: str, duty_shift: str) -> Path:
        path = self._runtime_root() / self.ASSET_ROOT / self._safe_batch_dir_name(duty_date, duty_shift)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _normalize_target(target: str) -> str:
        return str(target or "").strip().lower()

    @staticmethod
    def _normalize_variant(variant: str) -> str:
        return str(variant or "").strip().lower()

    def get_asset_path(self, *, duty_date: str, duty_shift: str, target: str, variant: str) -> Path:
        target_text = self._normalize_target(target)
        variant_text = self._normalize_variant(variant)
        if target_text not in self.VALID_TARGETS:
            raise ValueError(f"invalid target: {target}")
        if variant_text not in {"auto", "manual"}:
            raise ValueError(f"invalid variant: {variant}")
        return self.get_batch_dir(duty_date=duty_date, duty_shift=duty_shift) / f"{target_text}_{variant_text}.png"

    def get_summary_sheet_path(self, *, duty_date: str, duty_shift: str) -> Path:
        return self.get_asset_path(duty_date=duty_date, duty_shift=duty_shift, target="summary_sheet", variant="auto")

    def get_external_page_path(self, *, duty_date: str, duty_shift: str) -> Path:
        return self.get_asset_path(duty_date=duty_date, duty_shift=duty_shift, target="external_page", variant="auto")

    @staticmethod
    def _now_text() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def save_auto_summary_sheet_image(self, *, duty_date: str, duty_shift: str, content: bytes) -> Path:
        path = self.get_asset_path(duty_date=duty_date, duty_shift=duty_shift, target="summary_sheet", variant="auto")
        atomic_write_bytes(path, content, validator=validate_image_file, temp_suffix=".tmp")
        return path

    def save_auto_external_page_image(self, *, duty_date: str, duty_shift: str, content: bytes) -> Path:
        path = self.get_asset_path(duty_date=duty_date, duty_shift=duty_shift, target="external_page", variant="auto")
        atomic_write_bytes(path, content, validator=validate_image_file, temp_suffix=".tmp")
        return path

    # backward-compatible aliases
    def save_summary_sheet_image(self, *, duty_date: str, duty_shift: str, content: bytes) -> Path:
        return self.save_auto_summary_sheet_image(duty_date=duty_date, duty_shift=duty_shift, content=content)

    def save_external_page_image(self, *, duty_date: str, duty_shift: str, content: bytes) -> Path:
        return self.save_auto_external_page_image(duty_date=duty_date, duty_shift=duty_shift, content=content)

    def save_manual_image(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        target: str,
        content: bytes,
        mime_type: str = "",  # noqa: ARG002
        original_name: str = "",  # noqa: ARG002
    ) -> Path:
        target_text = self._normalize_target(target)
        if target_text not in self.VALID_TARGETS:
            raise ValueError("invalid target")
        path = self.get_asset_path(duty_date=duty_date, duty_shift=duty_shift, target=target_text, variant="manual")
        path.parent.mkdir(parents=True, exist_ok=True)
        with Image.open(BytesIO(content)) as image:
            rendered = image.convert("RGBA")
        atomic_write_file(
            path,
            lambda temp_path: rendered.save(temp_path, format="PNG"),
            validator=validate_image_file,
            temp_suffix=".tmp",
        )
        return path

    def delete_manual_image(self, *, duty_date: str, duty_shift: str, target: str) -> bool:
        path = self.get_asset_path(duty_date=duty_date, duty_shift=duty_shift, target=target, variant="manual")
        if not path.exists():
            return False
        path.unlink(missing_ok=True)
        return True

    @staticmethod
    def _captured_at(path: Path) -> str:
        if not path.exists():
            return ""
        try:
            return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        except OSError:
            return ""

    @staticmethod
    def _preview_version(path: Path) -> str:
        if not path.exists():
            return ""
        try:
            return str(int(path.stat().st_mtime_ns // 1_000_000))
        except OSError:
            return ""

    @classmethod
    def _build_preview_url(
        cls,
        *,
        duty_date: str,
        duty_shift: str,
        target: str,
        variant: str,
        view: str,
        version: str = "",
    ) -> str:
        params = {
            "duty_date": str(duty_date or "").strip(),
            "duty_shift": str(duty_shift or "").strip().lower(),
            "target": str(target or "").strip().lower(),
            "variant": str(variant or "").strip().lower(),
            "view": str(view or "").strip().lower() or "full",
        }
        version_text = str(version or "").strip()
        if version_text:
            params["v"] = version_text
        params = urlencode(params)
        return f"/api/handover/daily-report/capture-assets/file?{params}"

    def _build_asset_urls(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        target: str,
        variant: str,
        version: str = "",
    ) -> Dict[str, str]:
        thumbnail_url = self._build_preview_url(
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target,
            variant=variant,
            view="thumb",
            version=version,
        )
        full_image_url = self._build_preview_url(
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target,
            variant=variant,
            view="full",
            version=version,
        )
        return {
            "preview_url": thumbnail_url,
            "thumbnail_url": thumbnail_url,
            "full_image_url": full_image_url,
        }

    def _thumbnail_cache_path(self, *, duty_date: str, duty_shift: str, target: str, variant: str) -> Path:
        target_text = self._normalize_target(target)
        variant_text = self._normalize_variant(variant)
        return self.get_batch_dir(duty_date=duty_date, duty_shift=duty_shift) / f"{target_text}_{variant_text}_thumb.jpg"

    def _ensure_thumbnail(
        self,
        *,
        duty_date: str,
        duty_shift: str,
        target: str,
        variant: str,
        source_path: Path,
    ) -> Path:
        thumb_path = self._thumbnail_cache_path(
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target,
            variant=variant,
        )
        try:
            source_mtime = source_path.stat().st_mtime_ns
        except OSError:
            return source_path
        try:
            thumb_mtime = thumb_path.stat().st_mtime_ns if thumb_path.exists() else 0
        except OSError:
            thumb_mtime = 0
        if thumb_path.exists() and thumb_mtime >= source_mtime:
            return thumb_path
        thumb_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with Image.open(source_path) as image:
                rendered = image.convert("RGB")
                width, height = rendered.size
                if width > self.THUMBNAIL_MAX_WIDTH:
                    target_height = max(1, int(height * (self.THUMBNAIL_MAX_WIDTH / float(width))))
                    resampling_module = getattr(Image, "Resampling", Image)
                    rendered = rendered.resize((self.THUMBNAIL_MAX_WIDTH, target_height), resampling_module.LANCZOS)
                atomic_write_file(
                    thumb_path,
                    lambda temp_path: rendered.save(temp_path, format="JPEG", quality=72, optimize=True),
                    validator=validate_image_file,
                    temp_suffix=".tmp",
                )
            return thumb_path
        except Exception:
            return source_path

    def _variant_payload(self, *, duty_date: str, duty_shift: str, target: str, variant: str) -> Dict[str, Any]:
        if variant not in {"auto", "manual"}:
            return {
                "exists": False,
                "stored_path": "",
                "captured_at": "",
                "preview_url": "",
                "thumbnail_url": "",
                "full_image_url": "",
            }
        path = self.get_asset_path(duty_date=duty_date, duty_shift=duty_shift, target=target, variant=variant)
        exists = path.exists()
        version = self._preview_version(path)
        urls = self._build_asset_urls(
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target,
            variant=variant,
            version=version,
        ) if exists else {"preview_url": "", "thumbnail_url": "", "full_image_url": ""}
        return {
            "exists": exists,
            "stored_path": str(path) if exists else "",
            "captured_at": self._captured_at(path),
            **urls,
        }

    def resolve_effective_asset(self, *, duty_date: str, duty_shift: str, target: str) -> Dict[str, Any]:
        target_text = self._normalize_target(target)
        if target_text not in self.VALID_TARGETS:
            raise ValueError("invalid target")
        manual_payload = self._variant_payload(
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target_text,
            variant="manual",
        )
        auto_payload = self._variant_payload(
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target_text,
            variant="auto",
        )
        if manual_payload["exists"]:
            manual_path = Path(str(manual_payload["stored_path"]))
            urls = self._build_asset_urls(
                duty_date=duty_date,
                duty_shift=duty_shift,
                target=target_text,
                variant="effective",
                version=self._preview_version(manual_path),
            )
            return {
                "exists": True,
                "source": "manual",
                "stored_path": manual_payload["stored_path"],
                "captured_at": manual_payload["captured_at"],
                **urls,
                "auto": auto_payload,
                "manual": manual_payload,
            }
        if auto_payload["exists"]:
            auto_path = Path(str(auto_payload["stored_path"]))
            urls = self._build_asset_urls(
                duty_date=duty_date,
                duty_shift=duty_shift,
                target=target_text,
                variant="effective",
                version=self._preview_version(auto_path),
            )
            return {
                "exists": True,
                "source": "auto",
                "stored_path": auto_payload["stored_path"],
                "captured_at": auto_payload["captured_at"],
                **urls,
                "auto": auto_payload,
                "manual": manual_payload,
            }
        return {
            "exists": False,
            "source": "none",
            "stored_path": "",
            "captured_at": "",
            "preview_url": "",
            "thumbnail_url": "",
            "full_image_url": "",
            "auto": auto_payload,
            "manual": manual_payload,
        }

    def get_capture_assets_context(self, *, duty_date: str, duty_shift: str) -> Dict[str, Any]:
        return {
            "summary_sheet_image": self.resolve_effective_asset(
                duty_date=duty_date,
                duty_shift=duty_shift,
                target="summary_sheet",
            ),
            "external_page_image": self.resolve_effective_asset(
                duty_date=duty_date,
                duty_shift=duty_shift,
                target="external_page",
            ),
        }

    def get_asset_file_path(self, *, duty_date: str, duty_shift: str, target: str, variant: str, view: str = "full") -> Path | None:
        target_text = self._normalize_target(target)
        variant_text = self._normalize_variant(variant)
        view_text = str(view or "").strip().lower() or "full"
        if target_text not in self.VALID_TARGETS or variant_text not in self.VALID_VARIANTS or view_text not in {"full", "thumb"}:
            return None
        if variant_text == "effective":
            payload = self.resolve_effective_asset(duty_date=duty_date, duty_shift=duty_shift, target=target_text)
            path_text = str(payload.get("stored_path", "") or "").strip()
            source_path = Path(path_text) if path_text else None
        else:
            path = self.get_asset_path(duty_date=duty_date, duty_shift=duty_shift, target=target_text, variant=variant_text)
            source_path = path if path.exists() else None
        if source_path is None:
            return None
        if view_text == "full":
            return source_path
        return self._ensure_thumbnail(
            duty_date=duty_date,
            duty_shift=duty_shift,
            target=target_text,
            variant=variant_text,
            source_path=source_path,
        )

    def prune_stale_assets(self, *, retention_days: int | None = None) -> int:
        root = self._runtime_root() / self.ASSET_ROOT
        if not root.exists():
            return 0
        keep_days = retention_days if retention_days is not None else self.RETENTION_DAYS
        cutoff = datetime.now() - timedelta(days=max(1, int(keep_days or self.RETENTION_DAYS)))
        removed = 0
        for batch_dir in root.iterdir():
            if not batch_dir.is_dir():
                continue
            try:
                modified_at = datetime.fromtimestamp(batch_dir.stat().st_mtime)
            except OSError:
                continue
            if modified_at >= cutoff:
                continue
            shutil.rmtree(batch_dir, ignore_errors=True)
            removed += 1
        return removed
