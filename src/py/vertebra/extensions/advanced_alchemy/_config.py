"""Vertebra Advanced Alchemy config."""

from __future__ import annotations

from typing import Any

from vertebra.utils.module_loader import ensure_dependency

ensure_dependency("advanced_alchemy", install_package="advanced-alchemy")
ensure_dependency("sqlalchemy")

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402

# Try Litestar-compatible configs first, fall back to base configs
try:
    from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig, SQLAlchemySyncConfig
except ImportError:
    from advanced_alchemy.config import SQLAlchemyAsyncConfig, SQLAlchemySyncConfig


class VertebraAsyncAlchemyConfig(SQLAlchemyAsyncConfig):
    """Base config for broker-backed Advanced Alchemy (Async)."""

    def __init__(self, pool: str, driver: str = "postgresql+vertebra", **kwargs: Any) -> None:
        self.pool = pool
        engine = create_async_engine(f"{driver}://{pool}", poolclass=NullPool)

        # Call the base class __init__ correctly
        SQLAlchemyAsyncConfig.__init__(self, engine_instance=engine, **kwargs)


class VertebraSyncAlchemyConfig(SQLAlchemySyncConfig):
    """Base config for broker-backed Advanced Alchemy (Sync)."""

    def __init__(self, pool: str, driver: str = "postgresql+vertebra_sync", **kwargs: Any) -> None:
        self.pool = pool
        engine = create_engine(f"{driver}://{pool}", poolclass=NullPool)

        SQLAlchemySyncConfig.__init__(self, engine_instance=engine, **kwargs)
