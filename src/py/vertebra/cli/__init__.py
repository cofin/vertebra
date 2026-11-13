"""Command line interface for Vertebra."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.cli.plugin_registry import (
        CLIPlugin,
        CLIPluginProtocol,
        VertebraEnv,
        VertebraExtensionGroup,
        VertebraGroup,
    )

    cli: VertebraExtensionGroup

__all__ = (
    "CLIPlugin",
    "CLIPluginProtocol",
    "VertebraEnv",
    "VertebraExtensionGroup",
    "VertebraGroup",
    "cli",
)

_PLUGIN_EXPORTS: dict[str, tuple[str, str]] = {
    "CLIPlugin": ("vertebra.cli.plugin_registry", "CLIPlugin"),
    "CLIPluginProtocol": ("vertebra.cli.plugin_registry", "CLIPluginProtocol"),
    "VertebraEnv": ("vertebra.cli.plugin_registry", "VertebraEnv"),
    "VertebraExtensionGroup": ("vertebra.cli.plugin_registry", "VertebraExtensionGroup"),
    "VertebraGroup": ("vertebra.cli.plugin_registry", "VertebraGroup"),
}


def _build_cli() -> Any:
    """Build and populate the root Click extension group on demand."""
    vertebra_mod = ensure_module("vertebra")
    config_mod = ensure_module("vertebra.cli.config")
    plugin_registry = ensure_module("vertebra.cli.plugin_registry")
    ai_mod = ensure_module("vertebra.cli.ai")
    build_mod = ensure_module("vertebra.cli.build")
    dev_mod = ensure_module("vertebra.cli.dev")
    init_mod = ensure_module("vertebra.cli.init")
    js_mod = ensure_module("vertebra.cli.js")
    run_mod = ensure_module("vertebra.cli.run")
    workers_mod = ensure_module("vertebra.cli.workers")

    config_mod.configure_rich_click()

    @click.group(cls=plugin_registry.VertebraExtensionGroup, name="cli")
    @click.option(
        "--app",
        "app_path",
        default=None,
        help="Application import path in MODULE:ATTR format",
    )
    @click.option(
        "--app-dir",
        "app_dir",
        type=click.Path(exists=True, file_okay=False, path_type=Path),
        default=None,
        help="Directory added to sys.path for application discovery",
    )
    @click.version_option(
        version=vertebra_mod.__version__,
        package_name="vertebra-core",
        prog_name="vertebra",
    )
    def root_cli(app_path: str | None = None, app_dir: Path | None = None) -> None:
        """Vertebra: High-performance polyglot application backbone.

        Python-first architecture with Rust IPC primitives.
        """
        _ = (app_path, app_dir)

    root_cli.add_command(run_mod.run)
    root_cli.add_command(dev_mod.dev)
    root_cli.add_command(js_mod.js)
    root_cli.add_command(init_mod.init)
    root_cli.add_command(ai_mod.ai)
    root_cli.add_command(workers_mod.workers)
    root_cli.add_command(build_mod.build, name="build")
    plugin_registry.discover_entry_point_commands(root_cli)
    return root_cli


def __getattr__(name: str) -> Any:
    """Lazy-load CLI group and plugin registry exports to prevent circular imports."""
    if name == "cli":
        value = _build_cli()
        globals()["cli"] = value
        return value
    target = _PLUGIN_EXPORTS.get(name)
    if target is not None:
        module_path, attr_name = target
        value = getattr(ensure_module(module_path), attr_name)
        globals()[name] = value
        return value
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    """Return public export symbols and module globals for dir()."""
    return sorted(set(__all__) | set(globals().keys()))


if __name__ == "__main__":
    __getattr__("cli")()
