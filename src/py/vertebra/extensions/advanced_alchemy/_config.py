"""Vertebra Advanced Alchemy config."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from vertebra.utils.module_loader import dependency_flag

if TYPE_CHECKING:
    try:
        from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig, SQLAlchemySyncConfig
    except ImportError:
        from advanced_alchemy.config import SQLAlchemyAsyncConfig, SQLAlchemySyncConfig
else:
    try:
        # Prefer Litestar-compatible configs if available
        from advanced_alchemy.extensions.litestar import SQLAlchemyAsyncConfig, SQLAlchemySyncConfig
    except ImportError:
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
        
        # Call the base class __init__ correctly
        # Most AA configs are dataclasses, but some might have custom __init__
        if hasattr(SQLAlchemyAsyncConfig, "__init__"):
            SQLAlchemyAsyncConfig.__init__(self, engine_instance=engine, **kwargs)
        else:
            # Fallback for object or pure dataclass without init
            self.engine_instance = engine
            for key, value in kwargs.items():
                setattr(self, key, value)


class VertebraSyncAlchemyConfig(SQLAlchemySyncConfig):
    """Base config for broker-backed Advanced Alchemy (Sync)."""

    def __init__(self, pool: str, driver: str = "postgresql+vertebra_sync", **kwargs: Any) -> None:
        if not ADVANCED_ALCHEMY_INSTALLED:
            msg = "advanced-alchemy is not installed."
            raise ImportError(msg)

        self.pool = pool
        engine = create_engine(f"{driver}://{pool}", poolclass=NullPool)
        
        if hasattr(SQLAlchemySyncConfig, "__init__"):
            SQLAlchemySyncConfig.__init__(self, engine_instance=engine, **kwargs)
        else:
            self.engine_instance = engine
            for key, value in kwargs.items():
                setattr(self, key, value)