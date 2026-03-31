from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 常见 UTF-8/GBK 混乱后出现的可疑片段
SUSPECT_TOKENS = (
    "\u951f",
    "\u9286",
    "\u951b",
    "\u93c8\u20ac",
    "\u9a9e\u51b2",
    "\u9359\u6220",
    "\u7481\uff04",
    "\u93c3\u5815",
)

SCAN_SUFFIXES = {".py", ".md", ".json", ".txt"}
SKIP_DIR_NAMES = {"__pycache__", ".git", ".idea", ".vscode", "build", "dist", ".agent"}


def _iter_scan_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.name == "check_text_encoding.py":
            continue
        if path.suffix.lower() not in SCAN_SUFFIXES:
            continue
        if any(part in SKIP_DIR_NAMES for part in path.parts):
            continue
        yield path


def _find_suspects(path: Path) -> List[Tuple[int, str, str]]:
    hits: List[Tuple[int, str, str]] = []
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # 非 UTF-8 文件直接报可疑，避免隐式乱码
        hits.append((0, "<decode>", "文件不是 UTF-8"))
        return hits
    except Exception:
        return hits

    for lineno, line in enumerate(text.splitlines(), start=1):
        for token in SUSPECT_TOKENS:
            if token in line:
                snippet = line.strip()
                if len(snippet) > 140:
                    snippet = snippet[:137] + "..."
                hits.append((lineno, token, snippet))
                break
    return hits


def main() -> int:
    all_hits: List[Tuple[Path, int, str, str]] = []
    for file_path in _iter_scan_files(PROJECT_ROOT):
        for lineno, token, snippet in _find_suspects(file_path):
            all_hits.append((file_path, lineno, token, snippet))

    if all_hits:
        print("[ENCODING-CHECK] 发现可疑乱码片段:")
        for file_path, lineno, token, snippet in all_hits:
            rel = file_path.relative_to(PROJECT_ROOT)
            print(f"- {rel}:{lineno} token={token} line={snippet}")
        return 2

    print("[ENCODING-CHECK] 通过: 未发现可疑乱码片段。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
