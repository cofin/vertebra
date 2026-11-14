"""Discovery and registration of installed native database engine capsules."""

from __future__ import annotations

from typing import Final

from vertebra._vertebra import BrokerEngineRegistry
from vertebra.utils.module_loader import import_optional

__all__ = ("BrokerEngineDiscovery",)


class BrokerEngineDiscovery:
    """Discover optional native database extensions and register their C-ABI capsules."""

    ENGINE_MODULES: Final[tuple[str, ...]] = (
        "vertebra._postgres",
        "vertebra._mysql",
        "vertebra._oracle",
        "vertebra._mssql",
        "vertebra._duckdb",
    )

    @classmethod
    def register_installed(cls) -> tuple[int, ...]:
        """Import each installed engine extension once and register its capsule."""
        registered: list[int] = []
        for module_name in cls.ENGINE_MODULES:
            module = import_optional(module_name)
            if module is None:
                continue
            capsule_factory = module.broker_engine_capsule
            registered.append(BrokerEngineRegistry.register(capsule_factory()))
        return tuple(registered)
