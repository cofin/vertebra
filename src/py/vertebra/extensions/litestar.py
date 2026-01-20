"""Litestar plugin integration for Vertebra."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from vertebra._internal.dependencies import LITESTAR_INSTALLED
from vertebra._internal.errors import MissingDependencyError

_LITESTAR_AVAILABLE = LITESTAR_INSTALLED
_LITESTAR_IMPORT_ERROR: Exception | None = None

if TYPE_CHECKING:
    try:
        from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
    except ImportError:
        InitPluginProtocol = object  # type: ignore[assignment,misc]
        CLIPluginProtocol = object  # type: ignore[assignment,misc]
else:
    if _LITESTAR_AVAILABLE:
        try:  # pragma: no cover - optional dependency
            from litestar.plugins import CLIPluginProtocol, InitPluginProtocol
        except ImportError as exc:  # pragma: no cover - optional dependency
            _LITESTAR_AVAILABLE = False
            _LITESTAR_IMPORT_ERROR = exc
            InitPluginProtocol = object
            CLIPluginProtocol = object
    else:
        InitPluginProtocol = object
        CLIPluginProtocol = object


@dataclass(slots=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):
    """Litestar plugin providing Vertebra integration."""

    log_level: str | None = None

    def __post_init__(self) -> None:
        if not _LITESTAR_AVAILABLE:
            raise MissingDependencyError(package="litestar") from _LITESTAR_IMPORT_ERROR

    def on_app_init(self, app_config: Any) -> Any:
        app_config.state.setdefault("vertebra", self)
        return app_config

    def on_cli_init(self, cli: Any) -> Any:
        try:
            import click
        except ImportError:
            return cli

        command = getattr(cli, "command", None)
        if not callable(command):
            return cli

        @command("run")
        @click.argument("app")
        @click.option("--host", default="127.0.0.1")
        @click.option("--port", default=8000, type=int)
        @click.option("--workers", default=1, type=int)
        @click.option("--config", "config_path", default=None)
        @click.option("--log-level", default=self.log_level or "info")
        def _run(app: str, host: str, port: int, workers: int, config_path: str | None, log_level: str) -> None:
            from vertebra.cli import serve

            # broker_config_path is required by serve but not exposed in CLI yet
            # defaulting to None as it's optional in serve signature but mypy complains
            raise SystemExit(
                serve(
                    app=app,
                    host=host,
                    port=port,
                    workers=workers,
                    config_path=config_path,
                    log_level=log_level,
                    broker_config_path=None,
                )
            )

        return cli


__all__ = ("VertebraPlugin",)