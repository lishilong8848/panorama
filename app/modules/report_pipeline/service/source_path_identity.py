from __future__ import annotations

import ntpath
from typing import Any


def source_file_identity_key(value: Any) -> str:
    """Return a stable source-file key without touching the filesystem."""
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = ntpath.normpath(text.replace("/", "\\"))
    return normalized.rstrip("\\").casefold()
