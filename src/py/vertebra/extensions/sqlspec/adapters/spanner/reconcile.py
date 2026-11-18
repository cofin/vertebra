"""Cloud Spanner extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

import inspect
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.exceptions import ImproperConfigurationError, MigrationError, SQLSpecError
from sqlspec.migrations.schema import (
    DropStatement,
    SchemaTarget,
    ensure_schema_async,
    ensure_schema_sync,
    run_drop_statements_async,
    run_drop_statements_sync,
)
from sqlspec.migrations.utils import resolve_extension_migration_mode
from sqlspec.utils.adapters import resolve_adapter_name, resolve_extension_store_class

from vertebra.extensions.sqlspec.adapters.spanner.core import (
    normalize_spanner_dialect,
    validate_spanner_identifier,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.extensions.events import BaseEventQueueStore
    from sqlspec.extensions.litestar.store import BaseSQLSpecStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "downgrade", "reconcile")

_SUPPORTED_ADAPTERS: Final[frozenset[str]] = frozenset({"spanner", "spangres", "vertebra_spanner"})
_KNOWN_EXTENSIONS: Final[tuple[str, ...]] = ("litestar", "events", "adk")
_RECONCILE_MODULE_PREFIX: Final[str] = "_sqlspec_reconcile_"
_UUID_HEX_SUFFIX_LEN: Final[int] = 33
_VECTOR_LENGTH_PARAM_RE: Final[re.Pattern[str]] = re.compile(r"\(\s*vector_length\s*=>\s*\d+\s*\)")


async def _noop_async() -> None:
    """Return an empty coroutine when async reconciliation has no active extensions."""


def _runner_extension_name() -> str | None:
    """Extract the extension name when invoked by SQLSpec's fresh-module reconciler loader."""
    if (
        __name__.startswith(_RECONCILE_MODULE_PREFIX)
        and len(__name__) > len(_RECONCILE_MODULE_PREFIX) + _UUID_HEX_SUFFIX_LEN
        and __name__[-_UUID_HEX_SUFFIX_LEN] == "_"
    ):
        return __name__[len(_RECONCILE_MODULE_PREFIX) : -_UUID_HEX_SUFFIX_LEN]
    return None


def _validate_context(context: MigrationContext) -> tuple[Any, Any, bool]:
    """Validate ``MigrationContext`` config, driver, adapter family, and sync/async parity."""
    if context.config is None:
        msg = "Spanner schema reconciliation requires context.config on MigrationContext."
        raise MigrationError(msg)
    if context.driver is None:
        msg = "Spanner schema reconciliation requires context.driver on MigrationContext."
        raise MigrationError(msg)

    is_async = context.is_async_driver
    config_is_async = getattr(context.config, "is_async", None)
    if isinstance(config_is_async, bool) and config_is_async != is_async:
        driver_mode = "async" if is_async else "sync"
        config_mode = "async" if config_is_async else "sync"
        msg = (
            f"Spanner schema reconciliation driver/config mode mismatch: "
            f"config is {config_mode}, but MigrationContext supplied a {driver_mode} driver."
        )
        raise MigrationError(msg)

    adapter_name = resolve_adapter_name(context.config)
    if adapter_name is not None and adapter_name not in _SUPPORTED_ADAPTERS:
        msg = f"Spanner extension reconciler does not support adapter '{adapter_name}'."
        raise MigrationError(msg)

    return context.config, context.driver, is_async


def _resolve_extensions(context: MigrationContext, config: Any) -> list[str]:
    """Resolve ordered Spanner extension names configured for reconcile mode."""
    runner_ext = _runner_extension_name()
    ctx_ext = context.extension_config if isinstance(context.extension_config, Mapping) else {}
    ctx_meta = context.metadata if isinstance(context.metadata, Mapping) else {}
    if "migration_mode" in ctx_ext:
        mode = resolve_extension_migration_mode(None, ctx_ext, extension_name=runner_ext or "spanner")
        if mode != "reconcile":
            return []

    cfg_ext_raw = getattr(config, "extension_config", None)
    cfg_ext: Mapping[str, Any] = cfg_ext_raw if isinstance(cfg_ext_raw, Mapping) else {}

    explicit_ext = ctx_meta.get("extension") or ctx_ext.get("extension")
    if explicit_ext is not None:
        if explicit_ext not in _KNOWN_EXTENSIONS:
            msg = f"Unsupported extension {explicit_ext!r} for Spanner reconciliation."
            raise MigrationError(msg)
        ext_opts = cfg_ext.get(explicit_ext)
        if isinstance(ext_opts, Mapping):
            mode = resolve_extension_migration_mode(None, ext_opts, extension_name=explicit_ext)
            if mode != "reconcile":
                return []
        return [explicit_ext]

    if runner_ext in _KNOWN_EXTENSIONS:
        ext_opts = cfg_ext.get(runner_ext)
        if isinstance(ext_opts, Mapping):
            mode = resolve_extension_migration_mode(None, ext_opts, extension_name=runner_ext)
            if mode != "reconcile":
                return []
        return [runner_ext]

    source_map: Mapping[str, Any] = ctx_ext if any(key in ctx_ext for key in _KNOWN_EXTENSIONS) else cfg_ext
    migration_cfg = getattr(config, "migration_config", None)
    include_extensions = migration_cfg.get("include_extensions", ()) if isinstance(migration_cfg, Mapping) else ()

    ordered: list[str] = [
        name
        for name in include_extensions
        if isinstance(name, str) and name in _KNOWN_EXTENSIONS and name in source_map
    ]
    for name in source_map:
        if name in _KNOWN_EXTENSIONS and name not in ordered:
            ordered.append(name)

    selected: list[str] = []
    for ext_name in ordered:
        ext_opts = source_map.get(ext_name)
        if isinstance(ext_opts, Mapping):
            mode = resolve_extension_migration_mode(None, ext_opts, extension_name=ext_name)
            if mode == "reconcile":
                selected.append(ext_name)
    return selected


def _resolve_litestar_store(config: Any) -> BaseSQLSpecStore[Any]:
    """Resolve and instantiate the Spanner Litestar session store."""
    try:
        store_cls = resolve_extension_store_class(
            config,
            extension="litestar",
            store_attr="litestar_store_class",
            store_suffix="Store",
        )
    except ImportError as exc:
        msg = f"Failed to import Spanner Litestar store: {exc}"
        raise MigrationError(msg) from exc
    if store_cls is None:
        from vertebra.extensions.sqlspec.adapters.spanner.litestar import SpannerLitestarStore

        store_cls = SpannerLitestarStore
    try:
        return cast("BaseSQLSpecStore[Any]", store_cls(config))
    except (ImproperConfigurationError, ValueError) as exc:
        msg = f"Invalid Spanner Litestar configuration for reconciliation: {exc}"
        raise MigrationError(msg) from exc


def _resolve_events_store(config: Any) -> tuple[BaseEventQueueStore[Any], str, bool]:
    """Resolve and instantiate the Spanner event queue store and backend metadata."""
    from sqlspec.extensions.events._channel import _resolve_backend_name

    try:
        store_cls = resolve_extension_store_class(
            config,
            extension="events",
            store_attr="events_store_class",
            store_suffix="EventQueueStore",
        )
    except ImportError as exc:
        msg = f"Failed to import Spanner events store: {exc}"
        raise MigrationError(msg) from exc
    if store_cls is None:
        from vertebra.extensions.sqlspec.adapters.spanner.events import SpannerEventQueueStore

        store_cls = SpannerEventQueueStore
    try:
        store = cast("BaseEventQueueStore[Any]", store_cls(config))
        backend_name = _resolve_backend_name(config, store.settings, resolve_adapter_name(config))
    except (ImproperConfigurationError, ValueError) as exc:
        raise MigrationError(str(exc)) from exc
    return store, backend_name, backend_name != "notify"


def _resolve_adk_stores(config: Any, *, is_async: bool) -> list[Any]:
    """Lazily import and resolve enabled Spanner ADK stores in canonical dependency order."""
    from sqlspec.extensions.adk._config_utils import (
        _adk_adapter_store_class,
        _adk_config_from_extension,
        _adk_memory_migration_enabled,
        _adk_memory_migration_store_class,
        _adk_sessions_migration_enabled,
    )
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore
    from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore

    try:
        adk_config = _adk_config_from_extension(config)
    except MigrationError:
        raise
    except SQLSpecError as exc:
        raise MigrationError(str(exc)) from exc

    stores: list[Any] = []
    try:
        if _adk_sessions_migration_enabled(config):
            session_store_cls = _adk_adapter_store_class(config, "ADKStore")
            stores.append(session_store_cls(config=config))
        if _adk_memory_migration_enabled(config):
            memory_store_cls = _adk_memory_migration_store_class(config)
            if memory_store_cls is None:
                msg = "ADK memory reconciliation is enabled, but no ADKMemoryStore class could be resolved."
                raise MigrationError(msg)
            stores.append(memory_store_cls(config=config))
        if adk_config.get("artifact_table"):
            artifact_store_cls = resolve_extension_store_class(
                config,
                extension="adk",
                store_attr="adk_artifact_store_class",
                store_suffix="ADKArtifactStore",
            )
            if artifact_store_cls is not None:
                stores.append(artifact_store_cls(config=config))
    except MigrationError:
        raise
    except (SQLSpecError, ImportError, ValueError) as exc:
        msg = f"Failed to resolve Spanner ADK store for schema reconciliation: {exc}"
        raise MigrationError(msg) from exc

    for store in stores:
        if is_async and not isinstance(store, (BaseAsyncADKStore, BaseAsyncADKMemoryStore, BaseAsyncADKArtifactStore)):
            msg = f"ADK store '{type(store).__name__}' is synchronous, but MigrationContext supplied an async driver."
            raise MigrationError(msg)
        if not is_async and not isinstance(store, (BaseSyncADKStore, BaseSyncADKMemoryStore, BaseSyncADKArtifactStore)):
            msg = (
                f"ADK store '{type(store).__name__}' is asynchronous, "
                "but MigrationContext supplied a synchronous driver."
            )
            raise MigrationError(msg)

    return stores


def _build_memory_schema_targets(config: Any, table_name: str, raw_ddl: str | list[str]) -> list[SchemaTarget]:
    """Parse Spanner ADK memory DDL when GoogleSQL ``vector_length=>N`` syntax exceeds SQLGlot."""
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
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore

    try:
        return list(cast("list[SchemaTarget]", store.get_schema_targets_sync(driver)))
    except ValueError:
        if isinstance(store, (BaseSyncADKMemoryStore, BaseAsyncADKMemoryStore)):
            build_ddl = getattr(store, "_memory_table_ddl", None)
            table_name = getattr(store, "_memory_table", None)
            if callable(build_ddl) and isinstance(table_name, str):
                raw_ddl = build_ddl()
                if not inspect.isawaitable(raw_ddl):
                    return _build_memory_schema_targets(config, table_name, cast("str | list[str]", raw_ddl))
        raise


async def _adk_targets_async(driver: Any, config: Any, store: Any) -> list[SchemaTarget]:
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore

    try:
        return list(cast("list[SchemaTarget]", await store.get_schema_targets_async(driver)))
    except ValueError:
        if isinstance(store, (BaseSyncADKMemoryStore, BaseAsyncADKMemoryStore)):
            build_ddl = getattr(store, "_memory_table_ddl", None)
            table_name = getattr(store, "_memory_table", None)
            if callable(build_ddl) and isinstance(table_name, str):
                raw_ddl = build_ddl()
                resolved_ddl = await raw_ddl if inspect.isawaitable(raw_ddl) else raw_ddl
                return _build_memory_schema_targets(config, table_name, cast("str | list[str]", resolved_ddl))
        raise


def _adk_store_drops(config: Any, store: Any) -> list[DropStatement]:
    """Return index-before-table downgrade drop statements for an ADK store."""
    from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore
    from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore

    if isinstance(store, (BaseSyncADKArtifactStore, BaseAsyncADKArtifactStore)):
        build_drop = getattr(store, "_build_drop_artifact_table_sql", None)
        if callable(build_drop):
            return [DropStatement(statement) for statement in cast("list[str]", build_drop())]
        downgrade_fn = getattr(store, "downgrade_drops", None)
        if callable(downgrade_fn):
            return list(cast("list[DropStatement]", downgrade_fn()))
        table = validate_spanner_identifier(store.artifact_table, "artifact_table")
        return [DropStatement(f"DROP TABLE IF EXISTS {table}")]

    if isinstance(store, (BaseSyncADKMemoryStore, BaseAsyncADKMemoryStore)):
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


def _reconcile_adk_sync(driver: Any, config: Any) -> None:
    """Execute the four-phase ADK schema reconciliation pipeline synchronously without DDL commits."""
    stores = _resolve_adk_stores(config, is_async=False)
    if not stores:
        return
    manage_schema, create_schema = stores[0]._schema_management_flags()
    if not manage_schema:
        return
    for store in stores:
        store.prepare_schema_sync(driver)
    for store in stores:
        store.prepare_reconcile_sync(driver)

    targets: list[SchemaTarget] = []
    for store in stores:
        targets.extend(_adk_targets_sync(driver, config, store))
    ensure_schema_sync(driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True)

    for store in stores:
        store.reconcile_companion_objects_sync(driver)
    for store in stores:
        store.finalize_reconcile_sync(driver)


async def _reconcile_adk_async(driver: Any, config: Any) -> None:
    """Execute the four-phase ADK schema reconciliation pipeline asynchronously without DDL commits."""
    stores = _resolve_adk_stores(config, is_async=True)
    if not stores:
        return
    manage_schema, create_schema = stores[0]._schema_management_flags()
    if not manage_schema:
        return
    for store in stores:
        await store.prepare_schema_async(driver)
    for store in stores:
        await store.prepare_reconcile_async(driver)

    targets: list[SchemaTarget] = []
    for store in stores:
        targets.extend(await _adk_targets_async(driver, config, store))
    await ensure_schema_async(
        driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True
    )

    for store in stores:
        await store.reconcile_companion_objects_async(driver)
    for store in stores:
        await store.finalize_reconcile_async(driver)


def _reconcile_sync(driver: Any, config: Any, extensions: list[str]) -> None:
    """Reconcile enabled Spanner extension schemas synchronously."""
    for ext_name in extensions:
        if ext_name == "litestar":
            store = _resolve_litestar_store(config)
            manage_schema, create_schema = store._schema_management_flags()
            if not manage_schema:
                continue
            store.prepare_schema_sync(driver)
            ensure_schema_sync(
                driver,
                store.get_schema_targets(),
                manage_schema=True,
                create_schema=create_schema,
                commit=False,
                strict=True,
            )
            store.reconcile_companion_objects_sync(driver)
        elif ext_name == "events":
            event_store, backend_name, needs_table = _resolve_events_store(config)
            if not needs_table:
                continue
            manage_schema, create_schema = event_store._schema_management_flags()
            if not manage_schema:
                continue
            event_store.prepare_schema_sync(driver)
            targets = event_store.native_backend_targets(backend_name)
            ensure_schema_sync(
                driver,
                event_store.get_schema_targets(),
                manage_schema=True,
                create_schema=create_schema,
                commit=False,
                strict=True,
            )
            event_store.reconcile_companion_objects_sync(driver)
            event_store.reconcile_native_backend_sync(driver, backend_name, targets, create_schema=create_schema)
        elif ext_name == "adk":
            _reconcile_adk_sync(driver, config)


async def _reconcile_async(driver: Any, config: Any, extensions: list[str]) -> None:
    """Reconcile enabled Spanner extension schemas asynchronously."""
    for ext_name in extensions:
        if ext_name == "litestar":
            store = _resolve_litestar_store(config)
            manage_schema, create_schema = store._schema_management_flags()
            if not manage_schema:
                continue
            await store.prepare_schema_async(driver)
            await ensure_schema_async(
                driver,
                store.get_schema_targets(),
                manage_schema=True,
                create_schema=create_schema,
                commit=False,
                strict=True,
            )
            await store.reconcile_companion_objects_async(driver)
        elif ext_name == "events":
            event_store, backend_name, needs_table = _resolve_events_store(config)
            if not needs_table:
                continue
            manage_schema, create_schema = event_store._schema_management_flags()
            if not manage_schema:
                continue
            await event_store.prepare_schema_async(driver)
            targets = event_store.native_backend_targets(backend_name)
            await ensure_schema_async(
                driver,
                event_store.get_schema_targets(),
                manage_schema=True,
                create_schema=create_schema,
                commit=False,
                strict=True,
            )
            await event_store.reconcile_companion_objects_async(driver)
            await event_store.reconcile_native_backend_async(driver, backend_name, targets, create_schema=create_schema)
        elif ext_name == "adk":
            await _reconcile_adk_async(driver, config)


def _downgrade_sync(driver: Any, config: Any, extensions: list[str]) -> None:
    """Drop enabled Spanner extension schemas in reverse order synchronously."""
    for ext_name in reversed(extensions):
        if ext_name == "litestar":
            store = _resolve_litestar_store(config)
            manage_schema, _ = store._schema_management_flags()
            if manage_schema:
                run_drop_statements_sync(driver, store.downgrade_drops())
        elif ext_name == "events":
            event_store, _backend_name, needs_table = _resolve_events_store(config)
            if not needs_table:
                continue
            manage_schema, _ = event_store._schema_management_flags()
            if manage_schema:
                run_drop_statements_sync(driver, event_store.downgrade_drops())
        elif ext_name == "adk":
            stores = _resolve_adk_stores(config, is_async=False)
            if not stores:
                continue
            manage_schema, _ = stores[0]._schema_management_flags()
            if not manage_schema:
                continue
            for store in reversed(stores):
                run_drop_statements_sync(driver, _adk_store_drops(config, store))


async def _downgrade_async(driver: Any, config: Any, extensions: list[str]) -> None:
    """Drop enabled Spanner extension schemas in reverse order asynchronously."""
    for ext_name in reversed(extensions):
        if ext_name == "litestar":
            store = _resolve_litestar_store(config)
            manage_schema, _ = store._schema_management_flags()
            if manage_schema:
                await run_drop_statements_async(driver, store.downgrade_drops())
        elif ext_name == "events":
            event_store, _backend_name, needs_table = _resolve_events_store(config)
            if not needs_table:
                continue
            manage_schema, _ = event_store._schema_management_flags()
            if manage_schema:
                await run_drop_statements_async(driver, event_store.downgrade_drops())
        elif ext_name == "adk":
            stores = _resolve_adk_stores(config, is_async=True)
            if not stores:
                continue
            manage_schema, _ = stores[0]._schema_management_flags()
            if not manage_schema:
                continue
            for store in reversed(stores):
                await run_drop_statements_async(driver, _adk_store_drops(config, store))


def reconcile(context: MigrationContext) -> Awaitable[None] | None:
    """Reconcile configured Spanner extension schemas on the active migration driver."""
    config, driver, is_async = _validate_context(context)
    extensions = _resolve_extensions(context, config)
    if not extensions:
        return _noop_async() if is_async else None
    if is_async:
        return _reconcile_async(driver, config, extensions)
    _reconcile_sync(driver, config, extensions)
    return None


def downgrade(context: MigrationContext) -> Awaitable[None] | None:
    """Drop configured Spanner extension schemas in reverse order on the active migration driver."""
    config, driver, is_async = _validate_context(context)
    extensions = _resolve_extensions(context, config)
    if not extensions:
        return _noop_async() if is_async else None
    if is_async:
        return _downgrade_async(driver, config, extensions)
    _downgrade_sync(driver, config, extensions)
    return None


down = downgrade
