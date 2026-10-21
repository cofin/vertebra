"""Pluggable CLI extension registry and CLIPluginProtocol auto-discovery."""

from __future__ import annotations

import contextlib
import importlib.metadata
from typing import Any, Protocol, runtime_checkable

import click

__all__ = (
    "CLIPluginProtocol",
    "discover_entry_point_commands",
    "register_app_cli_plugins",
)


@runtime_checkable
class CLIPluginProtocol(Protocol):
    """Protocol implemented by plugins that contribute subcommands to the vtb CLI."""

    def on_cli_init(self, cli: click.Group) -> None:
        """Register plugin subcommands or options on the root click.Group."""
        ...


def discover_entry_point_commands(
    cli_group: click.Group,
    *,
    group: str = "vertebra.cli",
) -> list[str]:
    """Load and attach click commands or hooks exposed via Python package entry points."""
    registered: list[str] = []
    try:
        eps = importlib.metadata.entry_points(group=group)
    except Exception:
        return registered

    for ep in eps:
        with contextlib.suppress(Exception):
            loaded = ep.load()
            if isinstance(loaded, click.Command):
                cmd_name = loaded.name or ep.name
                if cmd_name not in cli_group.commands:
                    cli_group.add_command(loaded, name=cmd_name)
                    registered.append(cmd_name)
            elif isinstance(loaded, CLIPluginProtocol) or hasattr(loaded, "on_cli_init"):
                loaded.on_cli_init(cli_group)
                registered.append(ep.name)
            elif callable(loaded):
                result = loaded(cli_group)
                if isinstance(result, click.Command):
                    cmd_name = result.name or ep.name
                    if cmd_name not in cli_group.commands:
                        cli_group.add_command(result, name=cmd_name)
                registered.append(ep.name)
    return registered


def register_app_cli_plugins(cli_group: click.Group, app: Any) -> list[str]:
    """Invoke on_cli_init(cli_group) on any plugin attached to a Vertebra or Litestar application."""
    activated: list[str] = []
    if app is None:
        return activated

    plugins_attr = getattr(app, "plugins", None)
    if plugins_attr is None:
        return activated

    cli_plugins = getattr(plugins_attr, "cli", None)
    if cli_plugins is not None:
        candidates = list(cli_plugins)
    else:
        raw = getattr(plugins_attr, "plugins", plugins_attr)
        candidates = list(raw) if isinstance(raw, (list, tuple, set)) else []

    for plugin in candidates:
        hook = getattr(plugin, "on_cli_init", None)
        if callable(hook):
            hook(cli_group)
            activated.append(type(plugin).__name__)
    return activated
