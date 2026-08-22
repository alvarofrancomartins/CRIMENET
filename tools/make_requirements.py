"""
make_requirements.py

Generate a MINIMAL requirements.txt by scanning the project's own source for
third-party imports — not by dumping the whole environment (pip freeze would
include pandas, jupyter, and 100 transitive deps you never import).

It walks every .py file, collects top-level imported modules, drops the Python
standard library and the project's own local modules, maps import names to
their PyPI package names (e.g. `bs4` -> beautifulsoup4), and writes the result
sorted. By default it pins the version currently installed for each package;
use --no-versions for an unpinned list.

Run from the repo root:
    python tools/make_requirements.py
    python tools/make_requirements.py --no-versions
    python tools/make_requirements.py --root . --output requirements.txt
"""

import argparse
import ast
import sys
from pathlib import Path

# import-name -> PyPI package name, for the cases where they differ.
IMPORT_TO_PYPI = {
    "bs4": "beautifulsoup4",
    "dotenv": "python-dotenv",
    "yaml": "pyyaml",
    "PIL": "pillow",
    "cv2": "opencv-python",
    "sklearn": "scikit-learn",
}

# Folders to skip entirely while scanning.
SKIP_DIRS = {".git", "__pycache__", ".venv", "venv", "env", "node_modules",
             ".ipynb_checkpoints", "build", "dist"}


def stdlib_names():
    """Names that ship with Python (so they never go in requirements)."""
    names = set(getattr(sys, "stdlib_module_names", set()))
    # Safety net for names some interpreters omit from stdlib_module_names.
    names.update({
        "argparse", "ast", "json", "re", "time", "os", "sys", "pathlib",
        "collections", "concurrent", "threading", "urllib", "unicodedata",
        "csv", "logging", "functools", "itertools", "math", "random",
        "datetime", "subprocess", "importlib", "typing", "dataclasses",
        "io", "html", "string", "shutil", "glob", "hashlib", "runpy",
        "abc", "copy", "enum", "warnings", "traceback", "tempfile",
        "textwrap", "operator", "contextlib", "inspect", "pprint",
        "decimal", "fractions", "statistics", "secrets", "uuid", "base64",
        "pickle", "sqlite3", "socket", "ssl", "http", "email", "xml",
        "configparser", "platform", "signal", "multiprocessing", "queue",
        "asyncio", "struct", "array", "bisect", "heapq", "weakref",
    })
    return names


def local_module_names(root: Path):
    """Top-level names that are this project's OWN modules (so they're not
    treated as third-party): every .py file's stem and every package dir."""
    names = set()
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        names.add(p.stem)
    for p in root.iterdir():
        if p.is_dir() and (p / "__init__.py").exists():
            names.add(p.name)
    return names


def top_level_imports(py_file: Path):
    """Top-level module name of every import in one file. Returns a set."""
    try:
        tree = ast.parse(py_file.read_text("utf-8"), filename=str(py_file))
    except (SyntaxError, UnicodeDecodeError):
        return set()
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                out.add(a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            # ignore relative imports (from . import x) — those are local
            if node.level == 0 and node.module:
                out.add(node.module.split(".")[0])
    return out


def installed_version(pypi_name: str):
    """Installed version of a package, or None if not installed."""
    try:
        from importlib.metadata import version, PackageNotFoundError
    except ImportError:
        return None
    try:
        return version(pypi_name)
    except PackageNotFoundError:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="project root to scan")
    ap.add_argument("--output", "-o", default="requirements.txt")
    ap.add_argument("--no-versions", action="store_true",
                    help="write unpinned names only (no ==version)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    std = stdlib_names()
    local = local_module_names(root)

    # Collect all third-party top-level import names across the project.
    found = set()
    scanned = 0
    for py in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in py.parts):
            continue
        scanned += 1
        found |= top_level_imports(py)

    third_party = {
        name for name in found
        if name not in std and name not in local and not name.startswith("_")
    }

    # Map to PyPI names and (optionally) pin versions.
    rows, missing = [], []
    for name in sorted(third_party, key=str.lower):
        pypi = IMPORT_TO_PYPI.get(name, name)
        ver = installed_version(pypi)
        if ver and not args.no_versions:
            rows.append(f"{pypi}=={ver}")
        else:
            rows.append(pypi)
            if not ver:
                missing.append(pypi)

    out = Path(args.output)
    header = ("# Auto-generated by tools/make_requirements.py — scans source "
              "imports.\n# Review before committing.\n")
    out.write_text(header + "\n".join(rows) + "\n", encoding="utf-8")

    print(f"Scanned {scanned} .py file(s) under {root}")
    print(f"Wrote {len(rows)} dependency line(s) -> {out}")
    for r in rows:
        print(f"  {r}")
    if missing:
        print("\n  note: these are imported but NOT installed in this env, so "
              "they were written unpinned:")
        for mname in missing:
            print(f"    - {mname}")
    print("\nReview the file — remove anything that's actually an optional or "
          "dev-only dependency.")


if __name__ == "__main__":
    main()