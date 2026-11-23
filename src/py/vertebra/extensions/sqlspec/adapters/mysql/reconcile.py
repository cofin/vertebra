"""MySQL extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

from vertebra.extensions.sqlspec._reconcile import AdapterReconcilerSpec, make_adapter_reconciler

__all__ = ("down", "downgrade", "reconcile")

_SPEC = AdapterReconcilerSpec(
    label="MySQL",
    supported_adapters=frozenset({"aiomysql", "asyncmy", "mysqlconnector", "pymysql", "vertebra_mysql"}),
    litestar_module="vertebra.extensions.sqlspec.adapters.mysql.litestar",
    litestar_class="MySQLLitestarStore",
    events_module="vertebra.extensions.sqlspec.adapters.mysql.events",
    events_class="MySQLEventQueueStore",
    fallback_extension_name="mysql",
)

reconcile, downgrade = make_adapter_reconciler(_SPEC, module_name=__name__)
down = downgrade
