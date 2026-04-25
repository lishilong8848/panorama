from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


RUNTIME_DEPENDENCY_SPECS: List[Dict[str, str]] = [
    {"package": "fastapi", "import_name": "fastapi"},
    {"package": "uvicorn", "import_name": "uvicorn"},
    {"package": "starlette", "import_name": "starlette"},
    {"package": "openpyxl", "import_name": "openpyxl"},
    {"package": "Pillow", "import_name": "PIL"},
    {"package": "pywin32", "import_name": "pythoncom"},
    {"package": "pywin32", "import_name": "win32com.client"},
    {"package": "requests", "import_name": "requests"},
    {"package": "pymysql", "import_name": "pymysql"},
    {"package": "playwright", "import_name": "playwright"},
    {"package": "python-multipart", "import_name": "multipart"},
]


def normalized_runtime_dependency_specs() -> List[Dict[str, str]]:
    specs: List[Dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in RUNTIME_DEPENDENCY_SPECS:
        if not isinstance(item, dict):
            continue
        package = str(item.get("package", "") or "").strip()
        import_name = str(item.get("import_name", "") or "").strip()
        if not package or not import_name:
            continue
        key = (package, import_name)
        if key in seen:
            continue
        seen.add(key)
        specs.append({"package": package, "import_name": import_name})
    return specs


def build_runtime_dependency_lock(
    *,
    package_versions: Dict[str, str],
    python_version: str,
    generated_at: str | None = None,
) -> Dict[str, Any]:
    packages: List[Dict[str, str]] = []
    for spec in normalized_runtime_dependency_specs():
        package = spec["package"]
        version = str(package_versions.get(package, "") or "").strip()
        if not version:
            raise RuntimeError(f"缺少运行时依赖版本: {package}")
        packages.append(
            {
                "package": package,
                "version": version,
                "import_name": spec["import_name"],
            }
        )
    return {
        "python_version": str(python_version or "").strip(),
        "generated_at": str(generated_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")).strip(),
        "packages": packages,
    }
