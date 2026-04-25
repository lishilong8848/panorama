from __future__ import annotations

import argparse
import importlib.util
import shutil
import subprocess
from pathlib import Path
from typing import Callable, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_ROOT = PROJECT_ROOT / "runtime" / "python"
SITE_PACKAGES_RELATIVE = Path("Lib") / "site-packages"
PYWIN32_ITEMS = (
    "win32",
    "win32com",
    "win32comext",
    "pythonwin",
    "pywin32_system32",
    "pywin32-310.dist-info",
    "pywin32_ctypes-0.2.3.dist-info",
    "win32ctypes",
    "pythoncom.py",
    "pywin32.pth",
    "pywin32.version.txt",
    "PyWin32.chm",
)


def _runtime_site_packages(runtime_root: Path) -> Path:
    return runtime_root / SITE_PACKAGES_RELATIVE


def _site_packages_has_pywin32(site_packages: Path) -> bool:
    return (site_packages / "pythoncom.py").exists() and (site_packages / "win32com").is_dir()


def _resolve_source_site_packages(raw: str = "") -> Path:
    if raw:
        candidate = Path(raw).expanduser().resolve()
        if candidate.name.lower() == "site-packages":
            return candidate
        return _runtime_site_packages(candidate).resolve()

    project_site_packages = _runtime_site_packages(DEFAULT_RUNTIME_ROOT).resolve()
    if _site_packages_has_pywin32(project_site_packages):
        return project_site_packages

    spec = importlib.util.find_spec("pythoncom")
    if spec is not None and spec.origin:
        return Path(spec.origin).resolve().parent
    raise RuntimeError("未找到本机 pywin32；请在已有 pywin32 的机器上运行此脚本")


def _resolve_target_runtime(*, target_project: str = "", target_runtime: str = "") -> Path:
    if target_runtime:
        return Path(target_runtime).expanduser().resolve()
    if target_project:
        return (Path(target_project).expanduser().resolve() / "runtime" / "python").resolve()
    return DEFAULT_RUNTIME_ROOT.resolve()


def _infer_runtime_from_site_packages(site_packages: Path) -> Path | None:
    if site_packages.name.lower() == "site-packages" and site_packages.parent.name.lower() == "lib":
        return site_packages.parent.parent.resolve()
    return None


def _resolve_target(
    *,
    target_project: str = "",
    target_runtime: str = "",
    target_site_packages: str = "",
) -> tuple[Path, Path | None]:
    if target_site_packages:
        site_packages = Path(target_site_packages).expanduser().resolve()
        return site_packages, _infer_runtime_from_site_packages(site_packages)
    runtime_root = _resolve_target_runtime(target_project=target_project, target_runtime=target_runtime)
    return _runtime_site_packages(runtime_root), runtime_root


def _is_relative_to(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _copy_item(source: Path, target: Path, *, target_site_packages: Path, clean: bool, dry_run: bool) -> None:
    if not _is_relative_to(target, target_site_packages):
        raise RuntimeError(f"拒绝写入 site-packages 外部路径: {target}")
    if clean and target.exists():
        if not _is_relative_to(target, target_site_packages):
            raise RuntimeError(f"拒绝清理异常目标路径: {target}")
        if not dry_run:
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
    if dry_run:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True)
    else:
        shutil.copy2(source, target)


def _missing_items(source_site_packages: Path, items: Iterable[str] = PYWIN32_ITEMS) -> list[str]:
    return [name for name in items if not (source_site_packages / name).exists()]


def _verify_target(target_runtime: Path) -> None:
    python_exe = target_runtime / "python.exe"
    if not python_exe.exists():
        raise RuntimeError(f"目标运行时缺少 python.exe，无法验证: {python_exe}")
    result = subprocess.run(
        [
            str(python_exe),
            "-c",
            "import pythoncom; import win32com.client; print('pywin32 ok')",
        ],
        text=True,
        capture_output=True,
        check=False,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"目标运行时 pywin32 验证失败: {detail}")


def sync_pywin32_runtime(
    *,
    source: str = "",
    target_project: str = "",
    target_runtime: str = "",
    target_site_packages: str = "",
    clean: bool = True,
    verify: bool = True,
    dry_run: bool = False,
    emit_log: Callable[[str], None] | None = None,
) -> dict[str, object]:
    log = emit_log or (lambda text: print(text, flush=True))
    source_site_packages = _resolve_source_site_packages(source)
    resolved_target_site_packages, target_runtime_root = _resolve_target(
        target_project=target_project,
        target_runtime=target_runtime,
        target_site_packages=target_site_packages,
    )

    missing = _missing_items(source_site_packages)
    if missing:
        raise RuntimeError(f"源 pywin32 文件不完整，缺少: {', '.join(missing)}")

    log(f"[pywin32同步] 源目录: {source_site_packages}")
    log(f"[pywin32同步] 目标目录: {resolved_target_site_packages}")
    if dry_run:
        log("[pywin32同步] 当前为预演模式，不会写入目标目录")
    copied: list[str] = []
    for name in PYWIN32_ITEMS:
        _copy_item(
            source_site_packages / name,
            resolved_target_site_packages / name,
            target_site_packages=resolved_target_site_packages,
            clean=clean,
            dry_run=dry_run,
        )
        copied.append(name)
        action = "将复制" if dry_run else "已复制"
        log(f"[pywin32同步] {action}: {name}")

    verified = False
    if verify and not dry_run:
        if target_runtime_root is None:
            raise RuntimeError("无法从目标 site-packages 推断 runtime/python，不能执行导入验证；请改用 --target-runtime 或加 --no-verify")
        _verify_target(target_runtime_root)
        verified = True
        log("[pywin32同步] 验证通过: import pythoncom; import win32com.client")
    elif verify and dry_run:
        log("[pywin32同步] 预演模式已跳过目标 Python 导入验证")
    return {
        "source_site_packages": str(source_site_packages),
        "target_site_packages": str(resolved_target_site_packages),
        "target_runtime": str(target_runtime_root or ""),
        "copied": copied,
        "verified": verified,
        "dry_run": bool(dry_run),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步 pywin32 到项目自带 runtime/python")
    parser.add_argument("--source", default="", help="源 runtime/python 或 site-packages；默认自动定位当前项目")
    parser.add_argument("--target-project", default="", help="内网端项目根目录；会写入 runtime/python/Lib/site-packages")
    parser.add_argument("--target-runtime", default="", help="内网端 runtime/python 目录；优先级高于 --target-project")
    parser.add_argument("--target-site-packages", default="", help="直接指定内网端 Lib/site-packages 目录；优先级最高")
    parser.add_argument("--no-clean", action="store_true", help="不先清理目标 pywin32 旧文件")
    parser.add_argument("--no-verify", action="store_true", help="复制后不执行目标 Python 导入验证")
    parser.add_argument("--dry-run", action="store_true", help="只打印将复制的内容，不写入目标目录")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sync_pywin32_runtime(
        source=str(args.source or "").strip(),
        target_project=str(args.target_project or "").strip(),
        target_runtime=str(args.target_runtime or "").strip(),
        target_site_packages=str(args.target_site_packages or "").strip(),
        clean=not bool(args.no_clean),
        verify=not bool(args.no_verify),
        dry_run=bool(args.dry_run),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
