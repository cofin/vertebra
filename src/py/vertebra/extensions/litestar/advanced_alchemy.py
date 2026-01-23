"""Vertebra Advanced Alchemy Litestar Plugin."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

try:
    from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin
except ImportError:
    SQLAlchemyPlugin = object

if TYPE_CHECKING:
    from litestar.config.app import AppConfig


class VertebraAdvancedAlchemyPlugin(SQLAlchemyPlugin):
    """Vertebra Advanced Alchemy Plugin."""

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        """Register type encoders/decoders and call super."""
        from ._plugin import VertebraPlugin

        # Auto-register Vertebra types
        vertebra = VertebraPlugin()
        app_config = vertebra.on_app_init(app_config)

        if hasattr(super(), "on_app_init"):
            return super().on_app_init(app_config)
        return app_config
