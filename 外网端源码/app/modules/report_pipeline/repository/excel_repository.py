from __future__ import annotations

from pathlib import Path
from typing import Iterable, List


class ExcelRepository:
    def list_xlsx(self, directory: str | Path, pattern: str = "*.xlsx") -> List[Path]:
        root = Path(directory)
        if not root.exists():
            return []
        return sorted([p for p in root.glob(pattern) if p.is_file()])

    def exists(self, path: str | Path) -> bool:
        return Path(path).exists()

    def ensure_parent(self, path: str | Path) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
