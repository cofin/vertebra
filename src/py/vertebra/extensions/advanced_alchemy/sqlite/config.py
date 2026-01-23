"""Vertebra SQLite Advanced Alchemy config."""

from typing import Any

from vertebra.extensions.advanced_alchemy._config import VertebraAsyncAlchemyConfig, VertebraSyncAlchemyConfig


class VertebraSqliteAsyncConfig(VertebraAsyncAlchemyConfig):
    """SQLite Async Config."""

    def __init__(self, pool: str, **kwargs: Any) -> None:
        super().__init__(pool=pool, driver="sqlite+vertebra", **kwargs)


class VertebraSqliteSyncConfig(VertebraSyncAlchemyConfig):
    """SQLite Sync Config."""

    def __init__(self, pool: str, **kwargs: Any) -> None:
        super().__init__(pool=pool, driver="sqlite+vertebra_sync", **kwargs)
