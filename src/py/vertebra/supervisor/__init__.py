"""Vertebra process supervisor — manages Rust server lifecycle and Python workers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.supervisor._supervisor import (
        build_server_command,
        find_repo_root,
        find_server_binary,
        run_supervisor,
    )
    from vertebra.supervisor._worker import main as worker_main

__all__ = (
    "build_server_command",
    "find_repo_root",
    "find_server_binary",
    "run_supervisor",
    "worker_main",
)

__lazy_modules__ = [
    "vertebra.supervisor._supervisor",
    "vertebra.supervisor._worker",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "build_server_command": ("vertebra.supervisor._supervisor", "build_server_command"),
    "find_repo_root": ("vertebra.supervisor._supervisor", "find_repo_root"),
    "find_server_binary": ("vertebra.supervisor._supervisor", "find_server_binary"),
    "run_supervisor": ("vertebra.supervisor._supervisor", "run_supervisor"),
    "worker_main": ("vertebra.supervisor._worker", "main"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
