"""Banner renderer for Vertebra startup."""

from __future__ import annotations

import sys

from rich.console import Console


def render_banner(
    *,
    mode: str = "serve",
    host: str = "127.0.0.1",
    port: int = 8000,
    workers: int = 1,
    log_level: str = "info",
) -> None:
    """Render the startup banner to the terminal."""
    console = Console(stderr=True)
    is_tty = sys.stderr.isatty()

    if not is_tty:
        return

    console.print()
    console.print("  [bold cyan]Vertebra[/bold cyan] v0.1.0", highlight=False)
    console.print(f"  [dim]{mode}[/dim] → {host}:{port} ({workers} worker{'s' if workers != 1 else ''})")
    console.print()


__all__ = ("render_banner",)
