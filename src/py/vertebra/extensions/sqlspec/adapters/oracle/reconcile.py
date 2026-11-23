"""Oracle extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

from vertebra.extensions.sqlspec._reconcile import AdapterReconcilerSpec, make_adapter_reconciler

__all__ = ("down", "downgrade", "reconcile")

_SPEC = AdapterReconcilerSpec(
    label="Oracle",
    supported_adapters=frozenset({"vertebra_oracle", "oracle", "oracledb"}),
    litestar_module="vertebra.extensions.sqlspec.adapters.oracle.litestar",
    litestar_class="OracleLitestarStore",
    events_module="vertebra.extensions.sqlspec.adapters.oracle.events",
    events_class="OracleEventQueueStore",
    fallback_extension_name="oracle",
)

reconcile, downgrade = make_adapter_reconciler(_SPEC, module_name=__name__)
down = downgrade
