"""Vertebra Advanced Alchemy config."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from vertebra.utils.module_loader import dependency_flag

if TYPE_CHECKING:
    from advanced_alchemy.config import SQLAlchemyAsyncConfig, SQLAlchemySyncConfig
else:
    try:
        from advanced_alchemy.config import SQLAlchemyAsyncConfig, SQLAlchemySyncConfig
    except ImportError:
        SQLAlchemyAsyncConfig = object
        SQLAlchemySyncConfig = object

ADVANCED_ALCHEMY_INSTALLED = dependency_flag("advanced_alchemy")


class VertebraAsyncAlchemyConfig(SQLAlchemyAsyncConfig):
    """Base config for broker-backed Advanced Alchemy (Async)."""

    def __init__(self, pool: str, driver: str = "postgresql+vertebra", **kwargs: Any) -> None:
        if not ADVANCED_ALCHEMY_INSTALLED:
            msg = "advanced-alchemy is not installed."
            raise ImportError(msg)

        self.pool = pool
        engine = create_async_engine(f"{driver}://{pool}", poolclass=NullPool)
        
        # Use SQLAlchemyAsyncConfig's __init__ directly to avoid any issues with super() 
        # and ensure we pass the correct field names.
        SQLAlchemyAsyncConfig.__init__(self, engine_instance=engine, **kwargs)


class VertebraSyncAlchemyConfig(SQLAlchemySyncConfig):
    """Base config for broker-backed Advanced Alchemy (Sync)."""

    def __init__(self, pool: str, driver: str = "postgresql+vertebra_sync", **kwargs: Any) -> None:
        if not ADVANCED_ALCHEMY_INSTALLED:
            msg = "advanced-alchemy is not installed."
            raise ImportError(msg)

        self.pool = pool
        engine = create_engine(f"{driver}://{pool}", poolclass=NullPool)
        
        SQLAlchemySyncConfig.__init__(self, engine_instance=engine, **kwargs)
