from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TARGET_ROOT = PROJECT_ROOT / "runtime" / "python"
RUNTIME_META_FILENAME = ".qjpt_runtime.json"
IGNORED_DIR_NAMES = {
    "__pycache__",
    ".git",
    ".pytest_cache",
}
IGNORED_FILE_SUFFIXES = {
    ".pyc",
    ".pyo",
}
IGNORED_FILE_NAMES = {
    "pyvenv.cfg",
}


@dataclass(frozen=True)
class CopyPlanItem:
    source: Path
    target: Path


def _normalize_runtime_root(raw: Path) -> Path:
    path = raw.expanduser().resolve()
    if path.is_file():
        path = path.parent
    if path.name.lower() in {"scripts", "bin"} and (path.parent / "Lib").exists():
        return path.parent
    return path


def _looks_like_runtime_root(path: Path) -> bool:
    return path.exists() and path.is_dir() and (path / "Lib").is_dir() and any(
        (path / candidate).exists() for candidate in ("python.exe", "python")
    )


def resolve_source_runtime_root(explicit: str | None = None) -> Path:
    candidates: list[Path] = []
    if explicit:
        candidates.append(_normalize_runtime_root(Path(explicit)))
    candidates.extend(
        [
            _normalize_runtime_root(Path(sys.base_prefix)),
            _normalize_runtime_root(Path(sys.executable)),
            _normalize_runtime_root(Path(sys.prefix)),
        ]
    )
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if _looks_like_runtime_root(candidate):
            return candidate
    joined = ", ".join(str(path) for path in candidates if str(path))
    raise RuntimeError(f"未找到可复制的 Python 运行时目录。已尝试: {joined}")


def _iter_runtime_copy_plan(source_root: Path, target_root: Path) -> Iterable[CopyPlanItem]:
    for path in sorted(source_root.rglob("*")):
        if not path.exists():
            continue
        relative = path.relative_to(source_root)
        if not relative.parts:
            continue
        if any(part in IGNORED_DIR_NAMES for part in relative.parts):
            continue
        if path.is_file():
            if path.name in IGNORED_FILE_NAMES:
                continue
            if path.suffix.lower() in IGNORED_FILE_SUFFIXES:
                continue
        yield CopyPlanItem(source=path, target=target_root / relative)


def _write_runtime_meta(source_root: Path, target_root: Path, *, dry_run: bool) -> dict[str, str]:
    payload = {
        "prepared_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "source_root": str(source_root),
        "target_root": str(target_root),
        "python_version": sys.version.split()[0],
        "python_executable": str(Path(sys.executable).resolve()),
        "base_prefix": str(Path(sys.base_prefix).resolve()),
    }
    if not dry_run:
        (target_root / RUNTIME_META_FILENAME).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return payload


def prepare_runtime_python(
    *,
    source_root: Path,
    target_root: Path,
    clear_target: bool = False,
    dry_run: bool = False,
    emit_log: Callable[[str], None] | None = None,
) -> dict[str, object]:
    log = emit_log or (lambda text: print(text, flush=True))
    source_root = _normalize_runtime_root(source_root)
    target_root = target_root.expanduser().resolve()
    if source_root == target_root:
        raise RuntimeError("运行时源目录与目标目录不能相同。")
    if source_root in target_root.parents:
        raise RuntimeError("目标目录不能位于源运行时目录内部。")
    if not _looks_like_runtime_root(source_root):
        raise RuntimeError(f"无效的 Python 运行时目录: {source_root}")

    if target_root.exists() and clear_target:
        log(f"[运行时准备] 清理旧目录: {target_root}")
        if not dry_run:
            shutil.rmtree(target_root, ignore_errors=False)
    if not dry_run:
        target_root.mkdir(parents=True, exist_ok=True)

    copied_files = 0
    copied_dirs = 0
    for item in _iter_runtime_copy_plan(source_root, target_root):
        if item.source.is_dir():
            copied_dirs += 1
            if not dry_run:
                item.target.mkdir(parents=True, exist_ok=True)
            continue
        copied_files += 1
        if not dry_run:
            item.target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item.source, item.target)

    meta = _write_runtime_meta(source_root, target_root, dry_run=dry_run)
    return {
        "source_root": str(source_root),
        "target_root": str(target_root),
        "copied_files": copied_files,
        "copied_dirs": copied_dirs,
        "dry_run": dry_run,
        "metadata": meta,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="为源码直跑项目准备可随项目目录分发的 runtime/python 运行时")
    parser.add_argument(
        "--source-root",
        default="",
        help="Python 运行时源目录；默认自动从当前 Python/base_prefix 推断",
    )
    parser.add_argument(
        "--target-root",
        default=str(DEFAULT_TARGET_ROOT),
        help="输出目录，默认写入项目根目录下的 runtime/python",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="复制前先清空目标目录",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印计划，不实际复制文件",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_root = resolve_source_runtime_root(args.source_root or None)
    target_root = Path(str(args.target_root or "").strip() or str(DEFAULT_TARGET_ROOT))
    print(f"[运行时准备] 源运行时目录: {source_root}", flush=True)
    print(f"[运行时准备] 目标目录: {target_root}", flush=True)
    result = prepare_runtime_python(
        source_root=source_root,
        target_root=target_root,
        clear_target=bool(args.clear),
        dry_run=bool(args.dry_run),
    )
    print(
        "[运行时准备] 完成: "
        f"copied_dirs={result['copied_dirs']}, copied_files={result['copied_files']}, dry_run={result['dry_run']}",
        flush=True,
    )
    if bool(args.dry_run):
        print("[运行时准备] 当前为 dry-run，未实际写入文件。", flush=True)
    else:
        print("[运行时准备] 现在可以把整个项目目录连同 runtime/python 一起发给用户。", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
