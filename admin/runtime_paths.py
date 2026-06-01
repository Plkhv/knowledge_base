from __future__ import annotations

import sys
from pathlib import Path


def get_runtime_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def find_repo_root(start: Path | None = None) -> Path:
    current = start or get_runtime_dir()

    for candidate in (current, *current.parents):
        if (candidate / "admin").is_dir() and (candidate / "lakehouse_infra").is_dir():
            return candidate

    if current.name == "admin" and current.parent.exists():
        return current.parent

    return current
