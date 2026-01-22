"""Vertebra plugin for Litestar framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from vertebra.exceptions import MissingDependencyError

try:
    from litestar.plugins import CLIPluginProtocol, InitPluginProtocol

    _LITESTAR_AVAILABLE = True
    _LITESTAR_IMPORT_ERROR: ImportError | None = None
except ImportError as exc:
    _LITESTAR_AVAILABLE = False
    _LITESTAR_IMPORT_ERROR = exc

    # Provide stub protocols for type checking when Litestar is not installed
    class CLIPluginProtocol:  # type: ignore[no-redef]
        """Stub protocol for type checking."""

    class InitPluginProtocol:  # type: ignore[no-redef]
        """Stub protocol for type checking."""


if TYPE_CHECKING:
    from litestar.config.app import AppConfig

from vertebra.extensions.litestar.config import VertebraPluginConfig

__all__ = ("VertebraPlugin",)


@dataclass(slots=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):  # type: ignore[misc]
    """Litestar plugin providing Vertebra integration.

    This plugin integrates Vertebra's Bun-backed server with Litestar,
    providing:

    - Native CLI commands for ``litestar run``
    - Automatic logging configuration for Vertebra loggers
    - Broker lifecycle management
    - Session stores via SQLSpec extensions

    The plugin implements both ``InitPluginProtocol`` for app initialization
    and ``CLIPluginProtocol`` for CLI command registration, following the
    same pattern as litestar-granian.

    Attributes:
        config: Plugin configuration options.
        log_level: Deprecated. Use ``config.log_level`` instead.

    Example:
        Basic usage with default settings::

            from litestar import Litestar
            from vertebra.extensions.litestar import VertebraPlugin

            app = Litestar(
                plugins=[VertebraPlugin()],
                route_handlers=[...],
            )

        With custom configuration::

            from vertebra.extensions.litestar import VertebraPlugin, VertebraPluginConfig

            config = VertebraPluginConfig(
                host="0.0.0.0",
                port=8080,
                workers=4,
                log_level="debug",
            )
            app = Litestar(
                plugins=[VertebraPlugin(config=config)],
                route_handlers=[...],
            )

        For session stores, use the SQLSpec-specific modules::

            from vertebra.extensions.sqlspec.postgres.litestar import PostgresLitestarStore
    """

    config: VertebraPluginConfig = field(default_factory=VertebraPluginConfig)
    log_level: str | None = None  # Deprecated, for backward compatibility

    def __post_init__(self) -> None:
        """Validate dependencies and merge configuration."""
        if not _LITESTAR_AVAILABLE:
            raise MissingDependencyError(package="litestar") from _LITESTAR_IMPORT_ERROR

        # Backward compatibility: if log_level is set directly, use it
        if self.log_level is not None and self.config.log_level == "info":
            # Only override if config has default value
            object.__setattr__(
                self,
                "config",
                VertebraPluginConfig(
                    host=self.config.host,
                    port=self.config.port,
                    workers=self.config.workers,
                    log_level=self.log_level,
                    broker_config_path=self.config.broker_config_path,
                    drain_timeout_ms=self.config.drain_timeout_ms,
                    shutdown_timeout_ms=self.config.shutdown_timeout_ms,
                    enable_reload=self.config.enable_reload,
                    reload_dirs=self.config.reload_dirs,
                ),
            )

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        """Initialize Vertebra with the Litestar application.

        This method is called during Litestar app initialization and:
        - Stores the plugin reference in app state for access by handlers
        - Configures logging for Vertebra loggers

        Args:
            app_config: The Litestar application configuration.

        Returns:
            The modified application configuration.
        """
        from vertebra.extensions.litestar._logging import configure_logging

        # Store plugin in app state for access by handlers
        app_config.state.setdefault("vertebra_plugin", self)

        # Also store under "vertebra" for backward compatibility
        app_config.state.setdefault("vertebra", self)

        # Configure logging for Vertebra loggers
        configure_logging(app_config, self.config.log_level)

        return app_config

    def on_cli_init(self, cli: Any) -> Any:
        """Register Vertebra CLI commands.

        This method is called during Litestar CLI initialization and
        registers the ``run`` command that starts the Vertebra server.

        Args:
            cli: The Litestar CLI Click group.

        Returns:
            The CLI group (unchanged).
        """
        try:
            from vertebra.extensions.litestar.cli import run_command
        except ImportError:
            # Click not available
            return cli

        # Add the run command to the CLI group
        add_command = getattr(cli, "add_command", None)
        if callable(add_command):
            cli.add_command(run_command, name="run")

        return cli
