"""Oracle SQLSpec adapter package with PEP 562 lazy exports."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from vertebra.extensions.sqlspec.adapters.oracle.adk import (
        OracleAsyncADKArtifactStore,
        OracleAsyncADKMemoryStore,
        OracleAsyncADKStore,
        OracleSyncADKArtifactStore,
        OracleSyncADKMemoryStore,
        OracleSyncADKStore,
    )
    from vertebra.extensions.sqlspec.adapters.oracle.config import OracleAsyncConfig, OracleSyncConfig
    from vertebra.extensions.sqlspec.adapters.oracle.data_dictionary import (
        OracleAsyncDataDictionary,
        OracleSyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.oracle.driver import OracleAsyncDriver, OracleSyncDriver
    from vertebra.extensions.sqlspec.adapters.oracle.events import (
        OracleAsyncEventsBackend,
        OracleEventQueueStore,
        OracleSyncEventsBackend,
    )
    from vertebra.extensions.sqlspec.adapters.oracle.litestar import OracleLitestarStore

__all__ = (
    "OracleAsyncADKArtifactStore",
    "OracleAsyncADKMemoryStore",
    "OracleAsyncADKStore",
    "OracleAsyncConfig",
    "OracleAsyncDataDictionary",
    "OracleAsyncDriver",
    "OracleAsyncEventsBackend",
    "OracleEventQueueStore",
    "OracleLitestarStore",
    "OracleSyncADKArtifactStore",
    "OracleSyncADKMemoryStore",
    "OracleSyncADKStore",
    "OracleSyncConfig",
    "OracleSyncDataDictionary",
    "OracleSyncDriver",
    "OracleSyncEventsBackend",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "OracleAsyncADKArtifactStore": ("vertebra.extensions.sqlspec.adapters.oracle.adk", "OracleAsyncADKArtifactStore"),
    "OracleAsyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.oracle.adk", "OracleAsyncADKMemoryStore"),
    "OracleAsyncADKStore": ("vertebra.extensions.sqlspec.adapters.oracle.adk", "OracleAsyncADKStore"),
    "OracleAsyncConfig": ("vertebra.extensions.sqlspec.adapters.oracle.config", "OracleAsyncConfig"),
    "OracleAsyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.oracle.data_dictionary",
        "OracleAsyncDataDictionary",
    ),
    "OracleAsyncDriver": ("vertebra.extensions.sqlspec.adapters.oracle.driver", "OracleAsyncDriver"),
    "OracleAsyncEventsBackend": ("vertebra.extensions.sqlspec.adapters.oracle.events", "OracleAsyncEventsBackend"),
    "OracleEventQueueStore": ("vertebra.extensions.sqlspec.adapters.oracle.events", "OracleEventQueueStore"),
    "OracleLitestarStore": ("vertebra.extensions.sqlspec.adapters.oracle.litestar", "OracleLitestarStore"),
    "OracleSyncADKArtifactStore": ("vertebra.extensions.sqlspec.adapters.oracle.adk", "OracleSyncADKArtifactStore"),
    "OracleSyncADKMemoryStore": ("vertebra.extensions.sqlspec.adapters.oracle.adk", "OracleSyncADKMemoryStore"),
    "OracleSyncADKStore": ("vertebra.extensions.sqlspec.adapters.oracle.adk", "OracleSyncADKStore"),
    "OracleSyncConfig": ("vertebra.extensions.sqlspec.adapters.oracle.config", "OracleSyncConfig"),
    "OracleSyncDataDictionary": (
        "vertebra.extensions.sqlspec.adapters.oracle.data_dictionary",
        "OracleSyncDataDictionary",
    ),
    "OracleSyncDriver": ("vertebra.extensions.sqlspec.adapters.oracle.driver", "OracleSyncDriver"),
    "OracleSyncEventsBackend": ("vertebra.extensions.sqlspec.adapters.oracle.events", "OracleSyncEventsBackend"),
}


def __getattr__(name: str) -> Any:
    """Resolve Oracle adapter symbols lazily via `ensure_module`."""
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    mod_name, attr_name = target
    mod = ensure_module(mod_name)
    val = getattr(mod, attr_name)
    globals()[name] = val
    return val
