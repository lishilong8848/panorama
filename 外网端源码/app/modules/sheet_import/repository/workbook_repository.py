from __future__ import annotations

import mimetypes
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple


class WorkbookRepository:
    def exists(self, file_path: str) -> bool:
        return Path(file_path).exists()


def extract_header_pairs(ws: Any, header_row: int) -> List[Tuple[int, str]]:
    max_col = ws.max_column or 0
    headers: List[Tuple[int, str]] = []
    for col in range(1, max_col + 1):
        value = ws.cell(row=header_row, column=col).value
        name = str(value).strip() if value is not None else ""
        if name:
            headers.append((col, name))
    return headers


def build_raw_header_name_by_column(ws: Any, header_row: int) -> Dict[int, str]:
    return {col: name for col, name in extract_header_pairs(ws, header_row)}


def extract_rows_with_row_index(
    *,
    ws: Any,
    header_row: int,
    row_payload_factory: Callable[[int, Dict[str, Any]], Any],
) -> List[Any]:
    headers = extract_header_pairs(ws, header_row)
    if not headers:
        return []

    row_payloads: List[Any] = []
    max_row = ws.max_row or header_row
    for row_idx in range(header_row + 1, max_row + 1):
        fields: Dict[str, Any] = {}
        has_any = False
        for col, header_name in headers:
            value = ws.cell(row=row_idx, column=col).value
            if value is not None and str(value).strip() != "":
                has_any = True
            fields[header_name] = value
        if not has_any:
            continue
        row_payloads.append(row_payload_factory(row_idx, fields))
    return row_payloads


def safe_file_token(text: str) -> str:
    return re.sub(r"[^0-9a-zA-Z_-]+", "_", text)


def image_extension_and_mime(image_obj: Any) -> Tuple[str, str]:
    ext = ""
    path = getattr(image_obj, "path", None)
    if path:
        ext = str(Path(str(path)).suffix).lower().lstrip(".")
    if not ext:
        fmt = getattr(image_obj, "format", None)
        if fmt:
            ext = str(fmt).lower()
    if not ext:
        ext = "png"
    if ext == "jpg":
        ext = "jpeg"
    mime = mimetypes.types_map.get(f".{ext}", f"image/{ext}")
    return ext, mime


def extract_sheet_images_by_anchor(
    *,
    ws: Any,
    header_row: int,
    image_placement_factory: Callable[..., Any],
) -> List[Any]:
    placements: List[Any] = []
    images = list(getattr(ws, "_images", []) or [])
    sheet_token = safe_file_token(getattr(ws, "title", "sheet"))
    for idx, image_obj in enumerate(images, 1):
        anchor = getattr(image_obj, "anchor", None)
        if anchor is None or not hasattr(anchor, "_from"):
            continue
        row_index = int(anchor._from.row) + 1
        col_index = int(anchor._from.col) + 1
        if row_index <= header_row:
            continue
        try:
            content = image_obj._data()
        except Exception:
            continue
        if not content:
            continue
        ext, mime = image_extension_and_mime(image_obj)
        file_name = f"{sheet_token}_r{row_index}_c{col_index}_{idx}.{ext}"
        placements.append(
            image_placement_factory(
                row_index=row_index,
                column_index=col_index,
                image_index=idx,
                file_name=file_name,
                mime_type=mime,
                content=content,
            )
        )
    return placements
