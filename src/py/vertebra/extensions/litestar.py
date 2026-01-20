"""Litestar plugin integration for Vertebra."""

from dataclasses import dataclass
from typing import Any

from vertebra._internal.errors import MissingDependencyError

try:
    from litestar.plugins import CLIPluginProtocol, InitPluginProtocol

    _LITESTAR_AVAILABLE = True
    _LITESTAR_IMPORT_ERROR: ImportError | None = None
except ImportError as exc:
    _LITESTAR_AVAILABLE = False
    _LITESTAR_IMPORT_ERROR = exc

    # Provide stub protocols for type checking
    class CLIPluginProtocol:  # type: ignore[no-redef]
        pass

    class InitPluginProtocol:  # type: ignore[no-redef]
        pass


@dataclass(slots=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):  # type: ignore[misc]
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

        @command("run")  # type: ignore[untyped-decorator]
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
