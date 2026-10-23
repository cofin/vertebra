"""Banner renderer for Vertebra startup and shutdown."""

import importlib.metadata
import shutil
import sys
from pathlib import Path

from vertebra import __version__
from vertebra.build import BuildConfig
from vertebra.cli.banner.logos import LOGO_COMPACT, get_logo
from vertebra.cli.theme import (
    ACCENT,
    SECONDARY,
    SUCCESS,
    SYM_ELLIPSIS,
    SYM_SUCCESS,
    Console,
    console,
    render_shutdown_blocks,
    render_startup_blocks,
)
from vertebra.utils.module_loader import import_optional

__all__ = ("render_banner", "render_shutdown")


def render_banner(
    *,
    mode: str = "serve",
    host: str = "127.0.0.1",
    port: int = 8000,
    workers: int = 1,
    vc: Console | None = None,
) -> None:
    """Render the startup banner to the terminal."""
    target = vc or console
    if not target.is_tty:
        return

    version = _get_version()
    width = shutil.get_terminal_size().columns
    logo = get_logo(width)
    py_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    has_rust = _detect_rust_server()
    has_frontend = _detect_frontend()

    target.blank()

    if logo == LOGO_COMPACT:
        target.raw(
            f"  [{ACCENT} bold]{logo}[/{ACCENT} bold] [bold]vertebra[/bold] [{SECONDARY}]v{version}[/{SECONDARY}]"
        )
    else:
        lines = logo.splitlines()
        for i, line in enumerate(lines):
            if i == len(lines) - 1:
                target.raw(f"  [{ACCENT} bold]{line}[/{ACCENT} bold]  [{SECONDARY}]v{version}[/{SECONDARY}]")
            else:
                target.raw(f"  [{ACCENT} bold]{line}[/{ACCENT} bold]")

    target.blank()

    worker_label = f"{workers} worker{'s' if workers != 1 else ''}"
    target.info(f"{mode} → {host}:{port} ({worker_label})")

    rust_status = f"[{SUCCESS}]{SYM_SUCCESS}[/{SUCCESS}]" if has_rust else f"[{SECONDARY}]—[/{SECONDARY}]"
    frontend_status = f"[{SUCCESS}]{SYM_SUCCESS}[/{SUCCESS}]" if has_frontend else f"[{SECONDARY}]—[/{SECONDARY}]"
    target.raw(
        f"  [{SECONDARY}]  · Python {py_version} · Rust server {rust_status} · Frontend {frontend_status}[/{SECONDARY}]"
    )

    target.blank()

    render_startup_blocks(target)


def render_shutdown(*, vc: Console | None = None) -> None:
    """Render the shutdown display."""
    target = vc or console
    if not target.is_tty:
        return

    target.blank()
    target.raw(f"  [{SECONDARY}]{SYM_ELLIPSIS} shutting down…[/{SECONDARY}]")

    render_shutdown_blocks(target)

    target.success("shutdown complete")
    target.blank()


def _get_version() -> str:
    """Get the vertebra package version."""
    try:
        return importlib.metadata.version("vertebra")
    except importlib.metadata.PackageNotFoundError:
        return __version__ or "dev"


def _detect_rust_server() -> bool:
    """Check if the Rust server binary is available."""
    try:
        sup_mod = import_optional("vertebra.supervisor._supervisor")
        if sup_mod is None:
            return False
        sup_mod.find_server_binary()
    except (SystemExit, Exception):
        return False
    return True


def _detect_frontend() -> bool:
    """Check if a frontend build config exists."""
    try:
        config = BuildConfig.load(Path.cwd())
        return bool(config.entry)
    except Exception:
        return False
