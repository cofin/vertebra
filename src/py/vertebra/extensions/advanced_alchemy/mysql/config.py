"""Vertebra MySQL Advanced Alchemy config."""

from typing import Any

from vertebra.extensions.advanced_alchemy._config import VertebraAsyncAlchemyConfig, VertebraSyncAlchemyConfig


class VertebraMysqlAlchemyAsyncConfig(VertebraAsyncAlchemyConfig):
    """MySQL Async Config."""

    def __init__(self, pool: str, **kwargs: Any) -> None:
        super().__init__(pool=pool, driver="mysql+vertebra_async", **kwargs)


class VertebraMysqlAlchemySyncConfig(VertebraSyncAlchemyConfig):
    """MySQL Sync Config."""

    def __init__(self, pool: str, **kwargs: Any) -> None:
        super().__init__(pool=pool, driver="mysql+vertebra", **kwargs)
