"""Vertebra Litestar Plugin."""

from dataclasses import dataclass
from typing import Any

from litestar.plugins import CLIPluginProtocol, InitPluginProtocol

from vertebra.serializers.litestar import VERTEBRA_TYPE_DECODERS, VERTEBRA_TYPE_ENCODERS


@dataclass(slots=True, frozen=True)
class VertebraPlugin(InitPluginProtocol, CLIPluginProtocol):
    """Litestar plugin providing Vertebra integration.

    Automatically registers type encoders/decoders for:
    - datetime, date, time, timedelta
    - UUID, Decimal, Path
    - IPv4Address, IPv6Address
    - Pydantic BaseModel (if installed)
    - attrs classes (if installed)
    - Arrow tables (if installed)
    """

    log_level: str | None = None
    register_type_encoders: bool = True

    def on_app_init(self, app_config: Any) -> Any:
        """Register Vertebra with Litestar app."""
        app_config.state.setdefault("vertebra", self)

        if self.register_type_encoders:
            self._register_type_encoders(app_config)

        return app_config

    def _register_type_encoders(self, app_config: Any) -> None:
        """Register Vertebra type encoders/decoders."""
        # Merge with existing
        existing_encoders = getattr(app_config, "type_encoders", None) or {}
        app_config.type_encoders = {**existing_encoders, **VERTEBRA_TYPE_ENCODERS}

        existing_decoders = getattr(app_config, "type_decoders", None) or []
        app_config.type_decoders = list(existing_decoders) + VERTEBRA_TYPE_DECODERS

    def on_cli_init(self, cli: Any) -> Any:
        """Register CLI commands."""
        return cli
