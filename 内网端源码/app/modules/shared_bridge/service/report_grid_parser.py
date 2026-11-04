"""Offline, incremental extraction of one report table, without business inference.

``feed`` accepts either text chunks or byte chunks (not a mixture). Byte chunks
are decoded incrementally using the configured encoding. ``max_body_bytes``
counts original bytes for byte input and UTF-8 bytes for text input.

The result has a rectangular ``grid`` and ``row_ids``, a mapping from zero-based
HTML row indexes to hidden point values. A hidden point value may be the cell's
trimmed text or its ``value`` attribute. Only the cell's own ``hidden`` attribute
and inline ``display`` declarations are considered; this is not a CSS engine.
"""

from __future__ import annotations

import codecs
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import TypedDict


class ReportGridParseError(ValueError):
    """The input, table structure, or configured resource limit is invalid."""


class ReportGrid(TypedDict):
    grid: list[list[str]]
    row_ids: dict[int, str]


_VOID_TAGS = frozenset(
    {"area", "base", "br", "col", "embed", "hr", "img", "input", "link",
     "meta", "param", "source", "track", "wbr"}
)
_ROW_PARENTS = frozenset({"table", "thead", "tbody", "tfoot"})
_ROW_GROUPS = frozenset({"thead", "tbody", "tfoot"})
_POINT_CELL_ID = re.compile(r"E[0-9]+-.+", re.DOTALL)
_POINT_VALUE = re.compile(r"[0-9]+(?:_[0-9]+)+")
_IMPORTANT = re.compile(r"\s*!\s*important\s*$", re.IGNORECASE)
_DISPLAY_VALUES = frozenset({
    "none", "contents", "block", "inline", "run-in", "flow", "flow-root", "table",
    "flex", "grid", "ruby", "list-item", "inline-block", "inline-table", "inline-flex",
    "inline-grid", "table-row-group", "table-header-group", "table-footer-group",
    "table-row", "table-cell", "table-column-group", "table-column", "table-caption",
    "ruby-base", "ruby-text", "ruby-base-container", "ruby-text-container",
    "block flow", "block flow-root", "block flex", "block grid", "block table",
    "inline flow", "inline flow-root", "inline flex", "inline grid", "inline table",
    "initial", "unset",
})


def _selected_attrs(attrs: list[tuple[str, str | None]], names: set[str]) -> dict[str, str | None]:
    result: dict[str, str | None] = {}
    for name, value in attrs:
        if name in names:
            if name in result:
                raise ReportGridParseError(f"duplicate {name!r} attribute")
            result[name] = value
    return result


def _span(attrs: dict[str, str | None], name: str, limit: int) -> int:
    if name not in attrs:
        return 1
    raw = (attrs[name] or "").strip()
    if not re.fullmatch(r"[0-9]+", raw):
        raise ReportGridParseError(f"invalid {name}: {raw[:40]!r}")
    normalized = raw.lstrip("0") or "0"
    if normalized == "0":
        raise ReportGridParseError(f"{name} must be positive")
    limit_text = str(limit)
    if len(normalized) > len(limit_text) or (
        len(normalized) == len(limit_text) and normalized > limit_text
    ):
        limit_name = "max_rows" if name == "rowspan" else "max_columns"
        raise ReportGridParseError(f"{name} exceeds {limit_name} ({limit})")
    return int(normalized)


def _css_declarations(style: str) -> list[str]:
    declarations: list[str] = []
    current: list[str] = []
    quote = ""
    depth = 0
    index = 0
    while index < len(style):
        char = style[index]
        if char == "\\":
            if index + 1 >= len(style):
                raise ReportGridParseError("incomplete inline style escape")
            current.extend(style[index:index + 2])
            index += 2
            continue
        if quote:
            current.append(char)
            if char == quote:
                quote = ""
        elif style.startswith("/*", index):
            end = style.find("*/", index + 2)
            if end < 0:
                raise ReportGridParseError("unclosed inline style comment")
            current.append(" ")
            index = end + 2
            continue
        elif char in "\"'":
            quote = char
            current.append(char)
        elif char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            if depth == 0:
                raise ReportGridParseError("unbalanced inline style parentheses")
            depth -= 1
            current.append(char)
        elif char == ";" and depth == 0:
            declarations.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    if quote or depth:
        raise ReportGridParseError("incomplete inline style declaration")
    declarations.append("".join(current))
    return declarations


def _hidden(attrs: dict[str, str | None]) -> bool:
    if "hidden" in attrs:
        return True
    display = ""
    priority = -1
    for declaration in _css_declarations(attrs.get("style") or ""):
        name, separator, value = declaration.partition(":")
        name = name.strip().lower()
        if "\\" in name:
            raise ReportGridParseError("escaped inline style property is unsupported")
        if not separator or name != "display":
            continue
        important = _IMPORTANT.search(value)
        value = " ".join((value[:important.start()] if important else value).lower().split())
        if value and value not in _DISPLAY_VALUES:
            raise ReportGridParseError(f"unsupported inline display value: {value[:40]!r}")
        if value and int(bool(important)) >= priority:
            display = value
            priority = int(bool(important))
    return display == "none"


@dataclass
class _Cell:
    tag: str
    hidden: bool
    cell_id: str
    value: str
    rowspan: int
    colspan: int
    text: list[str] = field(default_factory=list)


class ReportGridParser(HTMLParser):
    """Incrementally parse one strict ``table.x-table`` from an HTML body."""

    def __init__(
        self,
        *,
        max_rows: int = 10_000,
        max_columns: int = 4_096,
        max_body_bytes: int = 32 * 1024 * 1024,
        max_cells: int = 2_000_000,
        encoding: str = "utf-8",
    ) -> None:
        for name, value in (
            ("max_rows", max_rows), ("max_columns", max_columns),
            ("max_body_bytes", max_body_bytes), ("max_cells", max_cells),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ReportGridParseError(f"{name} must be a positive integer")
        try:
            decoder = codecs.getincrementaldecoder(encoding)
        except LookupError as exc:
            raise ReportGridParseError(f"unknown input encoding: {encoding!r}") from exc
        super().__init__(convert_charrefs=True)
        self.max_rows = max_rows
        self.max_columns = max_columns
        self.max_body_bytes = max_body_bytes
        self.max_cells = max_cells
        self._decoder = decoder(errors="strict")
        self._input_type: type | None = None
        self._body_bytes = 0
        self._finished = False
        self._failed = False
        self._found = False
        self._active = False
        self._stack: list[str] = []
        self._grid: list[list[str | None]] = []
        self._row_ids: dict[int, str] = {}
        self._row_index = -1
        self._column = 0
        self._width = 0
        self._cell: _Cell | None = None

    def _ensure_open(self) -> None:
        if self._finished or self._failed:
            raise ReportGridParseError("parser is already closed or failed")

    def feed(self, data: str | bytes) -> None:
        self._ensure_open()
        try:
            if not isinstance(data, (str, bytes)):
                raise ReportGridParseError("chunks must be str or bytes")
            chunk_type = bytes if isinstance(data, bytes) else str
            if self._input_type is not None and self._input_type is not chunk_type:
                raise ReportGridParseError("cannot mix text and byte chunks")
            self._input_type = chunk_type
            remaining = self.max_body_bytes - self._body_bytes
            if len(data) > remaining:
                raise ReportGridParseError("HTML body exceeds max_body_bytes")
            size = len(data) if chunk_type is bytes else len(data.encode("utf-8"))
            if size > remaining:
                raise ReportGridParseError("HTML body exceeds max_body_bytes")
            self._body_bytes += size
            text = self._decoder.decode(data, final=False) if chunk_type is bytes else data
            super().feed(text)
        except UnicodeError as exc:
            self._failed = True
            raise ReportGridParseError(f"invalid input encoding: {exc}") from exc
        except Exception:
            self._failed = True
            raise

    def close(self) -> ReportGrid:
        self._ensure_open()
        try:
            if self._input_type is bytes:
                super().feed(self._decoder.decode(b"", final=True))
            super().close()
            if self._active:
                raise ReportGridParseError(f"unclosed table element: {self._stack[-1]}")
            if not self._found:
                raise ReportGridParseError("no table with class x-table")
            grid = [
                [value if value is not None else "" for value in row]
                + [""] * (self._width - len(row))
                for row in self._grid
            ]
            self._finished = True
            return {"grid": grid, "row_ids": dict(self._row_ids)}
        except UnicodeError as exc:
            self._failed = True
            raise ReportGridParseError(f"invalid input encoding: {exc}") from exc
        except Exception:
            self._failed = True
            raise

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            classes = [value or "" for name, value in attrs if name == "class"]
            is_target = any("x-table" in value.split() for value in classes)
            if is_target:
                if len(classes) != 1:
                    raise ReportGridParseError("duplicate class attribute on x-table")
                if self._found:
                    raise ReportGridParseError("multiple tables with class x-table")
                self._found = True
            if self._active:
                raise ReportGridParseError("nested table inside x-table")
            if is_target:
                self._active = True
                self._stack = ["table"]
            return

        if not self._active:
            return
        parent = self._stack[-1]
        if tag in _ROW_GROUPS:
            if parent != "table":
                raise ReportGridParseError(f"{tag} must be directly inside table")
            if len(self._grid) > self._row_index + 1:
                raise ReportGridParseError("rowspan crosses a table row group")
        if tag == "tr":
            if parent not in _ROW_PARENTS:
                raise ReportGridParseError("tr is not inside a table row group")
            self._row_index += 1
            if self._row_index >= self.max_rows:
                raise ReportGridParseError("table exceeds max_rows")
            if self._row_index == len(self._grid):
                if (len(self._grid) + 1) * self._width > self.max_cells:
                    raise ReportGridParseError("table exceeds max_cells")
                self._grid.append([])
            self._column = 0
        elif tag in {"td", "th"}:
            if parent != "tr":
                raise ReportGridParseError(f"{tag} is not directly inside tr")
            selected = _selected_attrs(attrs, {"hidden", "style", "id", "value", "rowspan", "colspan"})
            self._cell = _Cell(
                tag=tag,
                hidden=_hidden(selected),
                cell_id=selected.get("id") or "",
                value=selected.get("value") or "",
                rowspan=_span(selected, "rowspan", self.max_rows),
                colspan=_span(selected, "colspan", self.max_columns),
            )
        if tag not in _VOID_TAGS:
            self._stack.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _VOID_TAGS:
            self.handle_starttag(tag, attrs)
            return
        target_table = tag == "table" and any(
            name == "class" and "x-table" in (value or "").split() for name, value in attrs
        )
        if self._active or target_table:
            raise ReportGridParseError(f"non-void element {tag!r} cannot be self-closing")

    def handle_data(self, data: str) -> None:
        if self._active and self._cell is not None:
            self._cell.text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if not self._active:
            return
        # FineReport sometimes emits closing tags for void elements such as img.
        if tag in _VOID_TAGS:
            return
        if not self._stack or self._stack[-1] != tag:
            expected = self._stack[-1] if self._stack else "none"
            raise ReportGridParseError(f"unexpected closing {tag!r}; expected {expected!r}")
        if tag in {"td", "th"}:
            self._finish_cell()
        elif tag in _ROW_GROUPS and len(self._grid) > self._row_index + 1:
            raise ReportGridParseError("rowspan crosses a table row group")
        elif tag == "table":
            if len(self._grid) > self._row_index + 1:
                raise ReportGridParseError("rowspan extends beyond the last row")
            self._active = False
        self._stack.pop()

    def _finish_cell(self) -> None:
        cell = self._cell
        if cell is None:
            raise ReportGridParseError("closing cell without an open cell")
        text = "".join(cell.text).strip()
        self._cell = None
        if cell.hidden:
            if _POINT_CELL_ID.fullmatch(cell.cell_id):
                value = cell.value.strip()
                if text and value and text != value and (
                    _POINT_VALUE.fullmatch(text) or _POINT_VALUE.fullmatch(value)
                ):
                    raise ReportGridParseError("conflicting hidden point values")
                point = text or value
                if _POINT_VALUE.fullmatch(point):
                    if self._row_index in self._row_ids:
                        raise ReportGridParseError("multiple hidden point IDs in one row")
                    self._row_ids[self._row_index] = point
            return

        row = self._grid[self._row_index]
        while self._column < len(row) and row[self._column] is not None:
            self._column += 1
        end_row = self._row_index + cell.rowspan
        end_column = self._column + cell.colspan
        if end_row > self.max_rows:
            raise ReportGridParseError("rowspan exceeds max_rows")
        if end_column > self.max_columns:
            raise ReportGridParseError("table exceeds max_columns")
        height = max(len(self._grid), end_row)
        width = max(self._width, end_column)
        if height * width > self.max_cells:
            raise ReportGridParseError("table exceeds max_cells")
        for target in self._grid[self._row_index:end_row]:
            if any(value is not None for value in target[self._column:end_column]):
                raise ReportGridParseError("overlapping cells")
        while len(self._grid) < end_row:
            self._grid.append([])
        for target in self._grid[self._row_index:end_row]:
            target.extend([None] * max(0, end_column - len(target)))
            target[self._column:end_column] = [text] * cell.colspan
        self._column = end_column
        self._width = width


def parse_report_grid(
    chunks: str | bytes | Iterable[str] | Iterable[bytes],
    *,
    max_rows: int = 10_000,
    max_columns: int = 4_096,
    max_body_bytes: int = 32 * 1024 * 1024,
    max_cells: int = 2_000_000,
    encoding: str = "utf-8",
) -> ReportGrid:
    """Parse a complete body or a lazy iterable of consistently typed chunks."""
    parser = ReportGridParser(
        max_rows=max_rows,
        max_columns=max_columns,
        max_body_bytes=max_body_bytes,
        max_cells=max_cells,
        encoding=encoding,
    )
    for chunk in (chunks,) if isinstance(chunks, (str, bytes)) else chunks:
        parser.feed(chunk)
    return parser.close()
