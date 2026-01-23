"""Vertebra Postgres Advanced Alchemy config."""

from typing import Any

from vertebra.extensions.advanced_alchemy._config import VertebraAsyncAlchemyConfig, VertebraSyncAlchemyConfig


class VertebraPostgresAlchemyAsyncConfig(VertebraAsyncAlchemyConfig):
    """Postgres Async Config."""

    def __init__(self, pool: str, **kwargs: Any) -> None:
        super().__init__(pool=pool, driver="postgresql+vertebra", **kwargs)


class VertebraPostgresAlchemySyncConfig(VertebraSyncAlchemyConfig):
    """Postgres Sync Config."""

    def __init__(self, pool: str, **kwargs: Any) -> None:
        super().__init__(pool=pool, driver="postgresql+vertebra_sync", **kwargs)
