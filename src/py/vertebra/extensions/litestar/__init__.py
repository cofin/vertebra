"""Vertebra Litestar plugin integration.

This module provides seamless integration between Vertebra's high-performance
Bun-backed server and the Litestar framework. The plugin hooks into Litestar's
initialization and CLI systems to provide a native experience.

Example:
    Basic usage::

        from litestar import Litestar
        from vertebra.extensions.litestar import VertebraPlugin

        app = Litestar(
            plugins=[VertebraPlugin()],
            route_handlers=[...],
        )

    Then run the server with::

        $ litestar run --workers 4

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

For session stores, use the SQLSpec-specific modules:

- ``vertebra.extensions.sqlspec.sqlite.litestar.SqliteLitestarStore``
- ``vertebra.extensions.sqlspec.postgres.litestar.PostgresLitestarStore``
- ``vertebra.extensions.sqlspec.mysql.litestar.MysqlLitestarStore``
"""

from __future__ import annotations

from vertebra.__metadata__ import __project__, __version__
from vertebra.extensions.litestar.config import VertebraPluginConfig
from vertebra.extensions.litestar.plugin import VertebraPlugin

__all__ = ("VertebraPlugin", "VertebraPluginConfig", "__project__", "__version__")
