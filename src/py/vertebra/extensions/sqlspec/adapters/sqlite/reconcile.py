"""SQLite extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

from vertebra.extensions.sqlspec._reconcile import AdapterReconcilerSpec, make_adapter_reconciler

__all__ = ("down", "downgrade", "reconcile")

_SPEC = AdapterReconcilerSpec(
    label="SQLite",
    supported_adapters=frozenset({"aiosqlite", "sqlite", "vertebra_sqlite"}),
    litestar_module="vertebra.extensions.sqlspec.adapters.sqlite.litestar",
    litestar_class="SQLiteLitestarStore",
    events_module="vertebra.extensions.sqlspec.adapters.sqlite.events",
    events_class="SQLiteEventQueueStore",
    fallback_extension_name="sqlite",
)

reconcile, downgrade = make_adapter_reconciler(_SPEC, module_name=__name__)
down = downgrade
