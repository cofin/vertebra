"""BigQuery extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

from typing import Any, cast

from sqlspec.migrations.schema import DropStatement, SchemaTarget

from vertebra.extensions.sqlspec._reconcile import AdapterReconcilerSpec, make_adapter_reconciler
from vertebra.extensions.sqlspec.adapters.bigquery.core import qualify_store_table
from vertebra.utils.module_loader import import_optional

__all__ = ("down", "downgrade", "reconcile")


def _qualify_schema_target(config: Any, driver: Any, target: SchemaTarget) -> SchemaTarget:
    if target.schema is not None or not isinstance(target.create_statement, str):
        return target
    qualified = qualify_store_table(config, target.table_name)
    if "." not in qualified:
        qualified = qualify_store_table(driver, target.table_name)
    if "." not in qualified:
        return target
    return SchemaTarget.from_ddl(qualified, target.create_statement, dialect="bigquery")


def _adk_targets_sync(driver: Any, config: Any, store: Any) -> list[SchemaTarget]:
    return [_qualify_schema_target(config, driver, t) for t in store.get_schema_targets_sync(driver)]


async def _adk_targets_async(driver: Any, config: Any, store: Any) -> list[SchemaTarget]:
    raw_targets = await store.get_schema_targets_async(driver)
    return [_qualify_schema_target(config, driver, t) for t in raw_targets]


def _adk_store_drops(config: Any, store: Any) -> list[DropStatement]:
    art_mod = import_optional("sqlspec.extensions.adk.artifact.store")
    if art_mod is not None and isinstance(store, (art_mod.BaseSyncADKArtifactStore, art_mod.BaseAsyncADKArtifactStore)):
        build_drop = getattr(store, "_build_drop_artifact_table_sql", None)
        if callable(build_drop):
            return [DropStatement(statement) for statement in cast("list[str]", build_drop())]
        downgrade_fn = getattr(store, "downgrade_drops", None)
        if callable(downgrade_fn):
            return list(cast("list[DropStatement]", downgrade_fn()))
        return [DropStatement(f"DROP TABLE IF EXISTS {qualify_store_table(config, store.artifact_table)}")]
    return list(cast("list[DropStatement]", store.downgrade_drops()))


_SPEC = AdapterReconcilerSpec(
    label="BigQuery",
    supported_adapters=frozenset({"bigquery", "vertebra_bigquery"}),
    litestar_module="vertebra.extensions.sqlspec.adapters.bigquery.litestar",
    litestar_class="BigQueryLitestarStore",
    events_module="vertebra.extensions.sqlspec.adapters.bigquery.events",
    events_class="BigQueryEventQueueStore",
    fallback_extension_name="bigquery",
    adk_targets_sync=_adk_targets_sync,
    adk_targets_async=_adk_targets_async,
    adk_store_drops=_adk_store_drops,
)

reconcile, downgrade = make_adapter_reconciler(_SPEC, module_name=__name__)
down = downgrade
