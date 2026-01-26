"""Banner rendering engine with TTY and non-TTY support.

The renderer adapts output based on terminal capabilities:
- TTY: Unicode art, colors, responsive layout
- Non-TTY: JSON structured log entry
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import click

from vertebra.cli.banner.hooks import get_registry
from vertebra.cli.banner.logos import get_logo_lines, get_logo_variant
from vertebra.cli.components import console, key_value, section_header, status_indicator
from vertebra.cli.theme import TerminalCaps, apply_color, detect_terminal_capabilities, get_chars

if TYPE_CHECKING:
    from vertebra.cli.banner.collector import ServerStatus

# Version - should match pyproject.toml
VERSION = "0.1.0"


@dataclass(frozen=True, slots=True)
class BannerConfig:
    """Configuration passed to banner renderer."""

    mode: str  # "serve" or "dev"
    host: str
    port: int
    workers: int
    log_level: str
    watched_paths: list[str] | None = None  # For dev mode


class BaseRenderer(ABC):
    """Abstract base class for banner renderers."""

    def __init__(self, caps: TerminalCaps) -> None:
        self.caps = caps
        self.chars = get_chars(caps)

    @abstractmethod
    def render(self, config: BannerConfig, status: ServerStatus | None = None) -> None:
        """Render the banner to stdout.

        Args:
            config: Banner configuration
            status: Optional server status from Bun
        """


class TTYRenderer(BaseRenderer):
    """Rich terminal renderer with colors and Unicode art."""

    def render(self, config: BannerConfig, status: ServerStatus | None = None) -> None:
        """Render full banner with logo, sections, and status."""
        width = self.caps.width
        variant = get_logo_variant(width)

        # Render based on width breakpoint
        if variant == "minimal":
            self._render_minimal(config, status)
        elif variant == "compact":
            self._render_compact(config, status)
        else:
            self._render_full(config, status)

    def _render_full(self, config: BannerConfig, status: ServerStatus | None) -> None:
        """Render full-width layout (>= 100 columns)."""
        # Logo with version
        logo_lines = get_logo_lines("full")
        version_text = f"v{VERSION}"
        if config.mode == "dev":
            version_text += " " + apply_color("[DEV]", "gold", self.caps)

        # Print logo (preserve existing styling)
        for i, line in enumerate(logo_lines):
            if i == len(logo_lines) - 1:
                # Add version on last line
                click.echo(f"{line}   {version_text}")
            else:
                click.echo(line)
        click.echo()

        # Server section
        section_header("Server")
        console.print(key_value("Listening", f"http://{config.host}:{config.port}"))
        worker_text = f"{config.workers} process{'es' if config.workers > 1 else ''}"
        if config.mode == "dev":
            worker_text += " (auto-reload)"
        console.print(key_value("Workers", worker_text))
        console.print(key_value("Log Level", config.log_level))
        click.echo()

        # Development section (dev mode only)
        if config.mode == "dev":
            section_header("Development")
            watched = ", ".join(config.watched_paths or ["src/", "app/"])
            console.print(key_value("Hot Reload", f"enabled (watching: {watched})"))
            console.print(key_value("Debug UI", f"http://{config.host}:{config.port}/__debug__"))
            console.print(key_value("Reload", f"http://{config.host}:{config.port}/__reload__"))
            click.echo()

        # Components section
        section_header("Components")
        self._render_component_status("Bun Engine", status, "bun")
        self._render_component_status("Rust IPC", status, "ipc")
        self._render_component_status("Broker", status, "broker")
        click.echo()

        # User extension sections
        self._render_extensions(config.mode)

    def _render_compact(self, config: BannerConfig, status: ServerStatus | None) -> None:
        """Render compact layout (60-99 columns)."""
        logo_lines = get_logo_lines("compact")
        version_text = f"v{VERSION}"
        if config.mode == "dev":
            version_text += " " + apply_color("[DEV]", "gold", self.caps)

        # Logo on first line with version
        click.echo(f"{logo_lines[0]}   {version_text}")
        if len(logo_lines) > 1:
            click.echo(logo_lines[1])
        click.echo()

        # Server info on one line
        click.echo("Server")
        info_line = f"  http://{config.host}:{config.port} | {config.workers} workers | {config.log_level}"
        click.echo(info_line)
        click.echo()

        # Compact component status
        click.echo("Components")
        bun_status = self._get_status_text(status, "bun")
        ipc_status = self._get_status_text(status, "ipc")
        broker_status = self._get_status_text(status, "broker")

        # Use rich console for markup
        console.print(
            f"  {status_indicator(bun_status)} Bun {self.chars['dots'] * 5} {bun_status}   "
            f"{status_indicator(ipc_status)} IPC {self.chars['dots'] * 5} {ipc_status}"
        )
        console.print(f"  {status_indicator(broker_status)} Broker {self.chars['dots'] * 2} {broker_status}")
        click.echo()

    def _render_minimal(self, config: BannerConfig, status: ServerStatus | None) -> None:
        """Render minimal layout (< 60 columns)."""
        logo = get_logo_lines("minimal")[0]
        version_text = f"VERTEBRA v{VERSION}"
        if config.mode == "dev":
            version_text += " [DEV]"

        click.echo(f"{logo} {version_text}")
        click.echo(f"http://{config.host}:{config.port}")

        # Overall status indicator
        overall = self._get_overall_status(status)
        # Use console.print for colored indicator
        console.print(f"{config.workers} workers | {status_indicator(overall)} {overall}")
        click.echo()

    def _render_component_status(self, name: str, status: ServerStatus | None, component: str) -> None:
        """Render a component status line with leader dots."""
        status_text = self._get_status_text(status, component)
        details = self._get_status_details(status, component)

        # Calculate leader dots
        # status_indicator returns rich markup, visually 1 char
        indicator = status_indicator(status_text)

        # Visual length calculation (approximate for dots)
        base_len = 2 + 1 + 1 + len(name) + 1  # "  * Name "
        suffix_len = 1 + len(status_text) + (len(details) + 3 if details else 0)

        base_text = f"  {indicator} {name} "
        suffix = f" {status_text} ({details})" if details else f" {status_text}"

        # Fill with dots
        available = self.caps.width - base_len - suffix_len - 2
        dots = self.chars["dots"] * max(available, 3)

        console.print(f"{base_text}{dots}{suffix}")

    def _get_status_text(self, status: ServerStatus | None, component: str) -> str:
        """Extract status text for a component."""
        if status is None:
            return "starting"

        component_data = getattr(status, component, None)
        if component_data is None:
            return "unknown"

        return str(component_data.status)

    def _get_status_details(self, status: ServerStatus | None, component: str) -> str:
        """Extract additional status details for a component."""
        if status is None:
            return ""

        component_data = getattr(status, component, None)
        if component_data is None:
            return ""

        if component == "bun" and component_data.version:
            return str(component_data.version)
        if component == "ipc":
            channels = getattr(component_data, "channels", None)
            if channels is not None:
                return f"{channels} channels"
        if component == "broker":
            pools = getattr(component_data, "pools", None)
            if pools:
                parts = [f"{k}: {v}" for k, v in pools.items()]
                return ", ".join(parts)

        return ""

    def _get_overall_status(self, status: ServerStatus | None) -> str:
        """Get overall server status summary."""
        if status is None:
            return "starting"

        # Check all components
        for component in ["bun", "ipc", "broker"]:
            component_data = getattr(status, component, None)
            if component_data and component_data.status != "ready":
                return str(component_data.status)

        return "ready"

    def _render_extensions(self, mode: str) -> None:
        """Render user-defined extension sections."""
        registry = get_registry()
        sections = registry.collect(mode=mode)

        for name, data in sections:
            section_header(name)
            for key, value in data.items():
                console.print(key_value(key, value))
            click.echo()


class BasicRenderer(BaseRenderer):
    """Plain text / JSON renderer for non-TTY environments."""

    def render(self, config: BannerConfig, status: ServerStatus | None = None) -> None:
        """Render banner as JSON log entry."""
        # Build structured log entry
        log_entry: dict[str, Any] = {
            "event": "server.startup",
            "version": VERSION,
            "host": config.host,
            "port": config.port,
            "workers": config.workers,
            "log_level": config.log_level,
            "mode": config.mode,
        }

        # Add component status if available
        if status:
            log_entry["components"] = {
                "bun": status.bun.status if status.bun else "unknown",
                "ipc": status.ipc.status if status.ipc else "unknown",
                "broker": status.broker.status if status.broker else "unknown",
            }
        else:
            log_entry["components"] = {"bun": "starting", "ipc": "starting", "broker": "starting"}

        # Collect user extensions
        registry = get_registry()
        sections = registry.collect(mode=config.mode)
        if sections:
            log_entry["extensions"] = dict(sections)

        # Output as JSON
        click.echo(json.dumps(log_entry))


def render_banner(
    mode: str = "serve",
    host: str = "127.0.0.1",
    port: int = 8000,
    workers: int = 1,
    log_level: str = "info",
    watched_paths: list[str] | None = None,
    status: ServerStatus | None = None,
) -> None:
    """Render the startup banner.

    This is the main entry point for banner rendering. It automatically
    detects terminal capabilities and chooses the appropriate renderer.

    Args:
        mode: Run mode ("serve" or "dev")
        host: Bound host address
        port: Bound port number
        workers: Number of worker processes
        log_level: Log level string
        watched_paths: Paths being watched for hot reload (dev mode)
        status: Optional server status from Bun
    """
    caps = detect_terminal_capabilities()
    config = BannerConfig(
        mode=mode, host=host, port=port, workers=workers, log_level=log_level, watched_paths=watched_paths
    )

    # Choose renderer based on TTY detection
    if caps.is_tty:
        renderer: BaseRenderer = TTYRenderer(caps)
    else:
        renderer = BasicRenderer(caps)

    renderer.render(config, status)
