"""Configuration for Vertebra Litestar plugin."""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ("VertebraPluginConfig",)


@dataclass(slots=True)
class VertebraPluginConfig:
    """Configuration for the Vertebra Litestar plugin.

    This dataclass holds all configuration options for the Vertebra server
    when running through Litestar's CLI. Options mirror those available in
    the ``vertebra serve`` command.

    Attributes:
        host: Host address to bind to.
        port: Port number to bind to.
        workers: Number of worker processes.
        log_level: Logging level (debug, info, warning, error, critical).
        broker_config_path: Path to broker configuration file.
        drain_timeout_ms: Drain timeout in milliseconds before shutdown.
        shutdown_timeout_ms: Shutdown timeout in milliseconds.
        enable_reload: Enable auto-reload for development.
        reload_dirs: Directories to watch for reload.

    Example:
        >>> config = VertebraPluginConfig(
        ...     host="0.0.0.0",
        ...     port=8080,
        ...     workers=4,
        ...     log_level="debug",
        ... )
    """

    host: str = "127.0.0.1"
    port: int = 8000
    workers: int = 1
    log_level: str = "info"
    broker_config_path: str | None = None
    drain_timeout_ms: int = 5000
    shutdown_timeout_ms: int = 30000
    enable_reload: bool = False
    reload_dirs: list[str] = field(default_factory=list)
