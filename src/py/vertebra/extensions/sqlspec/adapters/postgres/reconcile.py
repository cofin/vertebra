"""PostgreSQL extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

from vertebra.extensions.sqlspec._reconcile import AdapterReconcilerSpec, make_adapter_reconciler

__all__ = ("down", "downgrade", "reconcile")

_SPEC = AdapterReconcilerSpec(
    label="PostgreSQL",
    supported_adapters=frozenset({"vertebra_postgres", "postgres", "postgresql", "asyncpg", "psycopg", "psqlpy"}),
    litestar_module="vertebra.extensions.sqlspec.adapters.postgres.litestar",
    litestar_class="PostgresLitestarStore",
    events_module="vertebra.extensions.sqlspec.adapters.postgres.events",
    events_class="PostgresEventQueueStore",
    fallback_extension_name="postgres",
)

reconcile, downgrade = make_adapter_reconciler(_SPEC, module_name=__name__)
down = downgrade
