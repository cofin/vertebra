"""PostgreSQL SQLSpec adapter package with PEP 562 lazy exports."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.postgres.adk import (
        PostgresAsyncADKArtifactStore,
        PostgresAsyncADKMemoryStore,
        PostgresAsyncADKStore,
        PostgresSyncADKArtifactStore,
        PostgresSyncADKMemoryStore,
        PostgresSyncADKStore,
    )
    from vertebra.extensions.sqlspec.adapters.postgres.config import PostgresAsyncConfig, PostgresSyncConfig
    from vertebra.extensions.sqlspec.adapters.postgres.data_dictionary import (
        PostgresAsyncDataDictionary,
        PostgresSyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.postgres.driver import PostgresAsyncDriver, PostgresSyncDriver
    from vertebra.extensions.sqlspec.adapters.postgres.events import (
        PostgresAsyncEventsBackend,
        PostgresEventQueueStore,
        PostgresSyncEventsBackend,
    )
    from vertebra.extensions.sqlspec.adapters.postgres.litestar import PostgresLitestarStore

__all__ = (
    "PostgresAsyncADKArtifactStore",
    "PostgresAsyncADKMemoryStore",
    "PostgresAsyncADKStore",
    "PostgresAsyncConfig",
    "PostgresAsyncDataDictionary",
    "PostgresAsyncDriver",
    "PostgresAsyncEventsBackend",
    "PostgresEventQueueStore",
    "PostgresLitestarStore",
    "PostgresSyncADKArtifactStore",
    "PostgresSyncADKMemoryStore",
    "PostgresSyncADKStore",
    "PostgresSyncConfig",
    "PostgresSyncDataDictionary",
    "PostgresSyncDriver",
    "PostgresSyncEventsBackend",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "PostgresAsyncADKArtifactStore": (
        "vertebra.extensions.sqlspec.adapters.postgres.adk",
        "PostgresAsyncADKArtifactStore",
    ),
    "PostgresAsyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.postgres.adk", "PostgresAsyncADKMemoryStore"),
    "PostgresAsyncADKStore": ("vertebra.extensions.sqlspec.adapters.postgres.adk", "PostgresAsyncADKStore"),
    "PostgresAsyncConfig": ("vertebra.extensions.sqlspec.adapters.postgres.config", "PostgresAsyncConfig"),
    "PostgresAsyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.postgres.data_dictionary",
        "PostgresAsyncDataDictionary",
    ),
    "PostgresAsyncDriver": ("vertebra.extensions.sqlspec.adapters.postgres.driver", "PostgresAsyncDriver"),
    "PostgresAsyncEventsBackend": (
        "vertebra.extensions.sqlspec.adapters.postgres.events",
        "PostgresAsyncEventsBackend",
    ),
    "PostgresEventQueueStore": ("vertebra.extensions.sqlspec.adapters.postgres.events", "PostgresEventQueueStore"),
    "PostgresLitestarStore": ("vertebra.extensions.sqlspec.adapters.postgres.litestar", "PostgresLitestarStore"),
    "PostgresSyncADKArtifactStore": (
        "vertebra.extensions.sqlspec.adapters.postgres.adk",
        "PostgresSyncADKArtifactStore",
    ),
    "PostgresSyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.postgres.adk", "PostgresSyncADKMemoryStore"),
    "PostgresSyncADKStore": ("vertebra.extensions.sqlspec.adapters.postgres.adk", "PostgresSyncADKStore"),
    "PostgresSyncConfig": ("vertebra.extensions.sqlspec.adapters.postgres.config", "PostgresSyncConfig"),
    "PostgresSyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.postgres.data_dictionary",
        "PostgresSyncDataDictionary",
    ),
    "PostgresSyncDriver": ("vertebra.extensions.sqlspec.adapters.postgres.driver", "PostgresSyncDriver"),
    "PostgresSyncEventsBackend": ("vertebra.extensions.sqlspec.adapters.postgres.events", "PostgresSyncEventsBackend"),
}


def __getattr__(name: str) -> Any:
    """Resolve PostgreSQL adapter symbols lazily via `ensure_module`."""
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    mod_name, attr_name = target
    mod = ensure_module(mod_name)
    val = getattr(mod, attr_name)
    globals()[name] = val
    return val
