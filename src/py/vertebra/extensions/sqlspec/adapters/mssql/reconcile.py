"""MSSQL extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

from vertebra.extensions.sqlspec._reconcile import AdapterReconcilerSpec, make_adapter_reconciler

__all__ = ("down", "downgrade", "reconcile")

_SPEC = AdapterReconcilerSpec(
    label="MSSQL",
    supported_adapters=frozenset({"mssql", "mssql_python", "pymssql", "vertebra_mssql"}),
    litestar_module="vertebra.extensions.sqlspec.adapters.mssql.litestar",
    litestar_class="MSSQLLitestarStore",
    events_module="vertebra.extensions.sqlspec.adapters.mssql.events",
    events_class="MSSQLEventQueueStore",
    fallback_extension_name="mssql",
)

reconcile, downgrade = make_adapter_reconciler(_SPEC, module_name=__name__)
down = downgrade
