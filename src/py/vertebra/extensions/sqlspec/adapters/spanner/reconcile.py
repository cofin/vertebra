"""Cloud Spanner extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

import inspect
import re
from typing import Any, Final, cast

from sqlspec.migrations.schema import DropStatement, SchemaTarget

from vertebra.extensions.sqlspec._reconcile import AdapterReconcilerSpec, make_adapter_reconciler
from vertebra.extensions.sqlspec.adapters.spanner.core import (
    normalize_spanner_dialect,
    validate_spanner_identifier,
)
from vertebra.utils.module_loader import import_optional

__all__ = ("down", "downgrade", "reconcile")

_VECTOR_LENGTH_PARAM_RE: Final[re.Pattern[str]] = re.compile(r"\(\s*vector_length\s*=>\s*\d+\s*\)")


def _build_memory_schema_targets(config: Any, table_name: str, raw_ddl: str | list[str]) -> list[SchemaTarget]:
    ddl_str = raw_ddl if isinstance(raw_ddl, str) else ";\n".join(raw_ddl)
    parse_ddl = _VECTOR_LENGTH_PARAM_RE.sub("", ddl_str)
    dialect = normalize_spanner_dialect(
        getattr(config, "spanner_dialect", None) or getattr(config, "dialect", "googlesql")
    )
    parse_dialect = "postgres" if dialect == "spangres" else "spanner"
    target = SchemaTarget.from_ddl(table_name, parse_ddl, dialect=parse_dialect)
    target.create_statement = ddl_str
    return [target]


def _adk_targets_sync(driver: Any, config: Any, store: Any) -> list[SchemaTarget]:
    try:
        return list(cast("list[SchemaTarget]", store.get_schema_targets_sync(driver)))
    except ValueError:
        mem_mod = import_optional("sqlspec.extensions.adk.memory.store")
        if mem_mod is not None and isinstance(store, (mem_mod.BaseSyncADKMemoryStore, mem_mod.BaseAsyncADKMemoryStore)):
            build_ddl = getattr(store, "_memory_table_ddl", None)
            table_name = getattr(store, "_memory_table", None)
            if callable(build_ddl) and isinstance(table_name, str):
                raw_ddl = build_ddl()
                if not inspect.isawaitable(raw_ddl):
                    return _build_memory_schema_targets(config, table_name, cast("str | list[str]", raw_ddl))
        raise


async def _adk_targets_async(driver: Any, config: Any, store: Any) -> list[SchemaTarget]:
    try:
        return list(cast("list[SchemaTarget]", await store.get_schema_targets_async(driver)))
    except ValueError:
        mem_mod = import_optional("sqlspec.extensions.adk.memory.store")
        if mem_mod is not None and isinstance(store, (mem_mod.BaseSyncADKMemoryStore, mem_mod.BaseAsyncADKMemoryStore)):
            build_ddl = getattr(store, "_memory_table_ddl", None)
            table_name = getattr(store, "_memory_table", None)
            if callable(build_ddl) and isinstance(table_name, str):
                raw_ddl = build_ddl()
                resolved_ddl = await raw_ddl if inspect.isawaitable(raw_ddl) else raw_ddl
                return _build_memory_schema_targets(config, table_name, cast("str | list[str]", resolved_ddl))
        raise


def _adk_store_drops(config: Any, store: Any) -> list[DropStatement]:
    art_mod = import_optional("sqlspec.extensions.adk.artifact.store")
    if art_mod is not None and isinstance(store, (art_mod.BaseSyncADKArtifactStore, art_mod.BaseAsyncADKArtifactStore)):
        build_drop = getattr(store, "_build_drop_artifact_table_sql", None)
        if callable(build_drop):
            return [DropStatement(statement) for statement in cast("list[str]", build_drop())]
        downgrade_fn = getattr(store, "downgrade_drops", None)
        if callable(downgrade_fn):
            return list(cast("list[DropStatement]", downgrade_fn()))
        table = validate_spanner_identifier(store.artifact_table, "artifact_table")
        return [DropStatement(f"DROP TABLE IF EXISTS {table}")]
    mem_mod = import_optional("sqlspec.extensions.adk.memory.store")
    if mem_mod is not None and isinstance(store, (mem_mod.BaseSyncADKMemoryStore, mem_mod.BaseAsyncADKMemoryStore)):
        dialect = normalize_spanner_dialect(
            getattr(config, "spanner_dialect", None) or getattr(config, "dialect", "googlesql")
        )
        drops: list[DropStatement] = []
        if dialect == "googlesql":
            table = validate_spanner_identifier(store._memory_table, "memory_table")
            base_name = table.replace(".", "_")
            graph_fn = getattr(store, "_resolved_graph_name", None)
            graph_name = graph_fn() if callable(graph_fn) else None
            if isinstance(graph_name, str) and graph_name:
                safe_graph = validate_spanner_identifier(graph_name, "graph_name")
                drops.append(DropStatement(f"DROP PROPERTY GRAPH IF EXISTS {safe_graph}"))
            if getattr(store, "_use_fts", False):
                drops.append(DropStatement(f"DROP SEARCH INDEX IF EXISTS {base_name}_fts_idx"))
            drops.append(DropStatement(f"DROP VECTOR INDEX IF EXISTS {base_name}_vec_idx"))
        drops.extend(store.downgrade_drops())
        return drops
    return list(cast("list[DropStatement]", store.downgrade_drops()))


_SPEC = AdapterReconcilerSpec(
    label="Spanner",
    supported_adapters=frozenset({"spanner", "spangres", "vertebra_spanner"}),
    litestar_module="vertebra.extensions.sqlspec.adapters.spanner.litestar",
    litestar_class="SpannerLitestarStore",
    events_module="vertebra.extensions.sqlspec.adapters.spanner.events",
    events_class="SpannerEventQueueStore",
    fallback_extension_name="spanner",
    adk_targets_sync=_adk_targets_sync,
    adk_targets_async=_adk_targets_async,
    adk_store_drops=_adk_store_drops,
)

reconcile, downgrade = make_adapter_reconciler(_SPEC, module_name=__name__)
down = downgrade
