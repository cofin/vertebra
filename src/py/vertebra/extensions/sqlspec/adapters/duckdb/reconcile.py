"""DuckDB extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

from vertebra.extensions.sqlspec._reconcile import AdapterReconcilerSpec, make_adapter_reconciler

__all__ = ("down", "downgrade", "reconcile")

_SPEC = AdapterReconcilerSpec(
    label="DuckDB",
    supported_adapters=frozenset({"duckdb", "vertebra_duckdb"}),
    litestar_module="vertebra.extensions.sqlspec.adapters.duckdb.litestar",
    litestar_class="DuckDBLitestarStore",
    events_module="vertebra.extensions.sqlspec.adapters.duckdb.events",
    events_class="DuckDBEventQueueStore",
    fallback_extension_name="duckdb",
)

reconcile, downgrade = make_adapter_reconciler(_SPEC, module_name=__name__)
down = downgrade
