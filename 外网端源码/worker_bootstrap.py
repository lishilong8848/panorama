from __future__ import annotations

import sys
from pathlib import Path


def _ensure_project_root_on_path() -> Path:
    project_root = Path(__file__).resolve().parent
    project_root_text = str(project_root)
    if not any(str(item or "").strip() == project_root_text for item in sys.path):
        sys.path.insert(0, project_root_text)
    return project_root


_ensure_project_root_on_path()

from app.worker.entry import main


if __name__ == "__main__":
    raise SystemExit(main())
