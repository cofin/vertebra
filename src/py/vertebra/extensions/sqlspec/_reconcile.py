"""Shared SQLSpec extension schema reconciler factory for Vertebra adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Final, cast

import msgspec
from sqlspec.exceptions import ImproperConfigurationError, MigrationError, SQLSpecError
from sqlspec.extensions.events import resolve_backend_name
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

from vertebra.utils.module_loader import ensure_module

if TYPE_CHECKING:
    from sqlspec.extensions.events import BaseEventQueueStore
    from sqlspec.extensions.litestar.store import BaseSQLSpecStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("AdapterReconcilerSpec", "make_adapter_reconciler")

_KNOWN_EXTENSIONS: Final[tuple[str, ...]] = ("litestar", "events", "adk")
_RECONCILE_MODULE_PREFIX: Final[str] = "_sqlspec_reconcile_"
_UUID_HEX_SUFFIX_LEN: Final[int] = 33
_LITESTAR_DISCRIMINATORS: Final[frozenset[str]] = frozenset(
    {
        "auto_trace_headers",
        "commit_mode",
        "connection_key",
        "correlation_header",
        "correlation_headers",
        "enable_correlation_middleware",
        "pool_key",
        "session_key",
        "tenant_extractor",
        "tenant_header",
        "tenant_jwt_claim",
        "tenant_memberships_attr",
        "tenant_path_parameter",
        "tenant_session_key",
        "tenant_user_attr",
        "tenant_variables_attr",
    }
)
_EVENTS_DISCRIMINATORS: Final[frozenset[str]] = frozenset(
    {
        "aq_channels",
        "aq_queue",
        "aq_visibility",
        "aq_wait_seconds",
        "backend",
        "cleanup_on_ack",
        "event_poll_interval",
        "lease_seconds",
        "listener_queue_capacity",
        "max_attempts",
        "poll_interval",
        "queue_table",
        "retention_seconds",
        "retry_backoff_seconds",
        "select_for_update",
        "shard_count",
        "skip_locked",
    }
)
_ADK_DISCRIMINATORS: Final[frozenset[str]] = frozenset(
    {
        "app_state_table",
        "artifact_table",
        "enable_bm25",
        "enable_memory",
        "enable_sessions",
        "events_table",
        "memory_max_results",
        "memory_table",
        "memory_use_fts",
        "metadata_table",
        "owner_id_column",
        "scann_num_leaves",
        "scann_quantizer",
        "use_fts",
        "user_state_table",
        "vector_dimensions",
        "vector_index_type",
    }
)


class AdapterReconcilerSpec(msgspec.Struct, frozen=True):
    """Dialect parameters configuring a Vertebra SQLSpec extension schema reconciler."""

    label: str
    supported_adapters: frozenset[str]
    litestar_module: str
    litestar_class: str
    events_module: str
    events_class: str
    fallback_extension_name: str
    adk_targets_sync: Callable[[Any, Any, Any], list[SchemaTarget]] | None = None
    adk_targets_async: Callable[[Any, Any, Any], Awaitable[list[SchemaTarget]]] | None = None
    adk_store_drops: Callable[[Any, Any], list[DropStatement]] | None = None


def make_adapter_reconciler(
    spec: AdapterReconcilerSpec,
    *,
    module_name: str,
) -> tuple[
    Callable[[MigrationContext], Awaitable[None] | None],
    Callable[[MigrationContext], Awaitable[None] | None],
]:
    """Build ``(reconcile, downgrade)`` entrypoints for a SQLSpec adapter module."""

    def reconcile(context: MigrationContext) -> Awaitable[None] | None:
        config, driver, is_async = _validate_context(spec, context)
        targets = _resolve_extension_targets(spec, context, config, module_name=module_name)
        if not targets:
            return _noop_async() if is_async else None
        if is_async:
            return _reconcile_async(spec, driver, config, targets)
        _reconcile_sync(spec, driver, config, targets)
        return None

    def downgrade(context: MigrationContext) -> Awaitable[None] | None:
        config, driver, is_async = _validate_context(spec, context)
        targets = _resolve_extension_targets(spec, context, config, module_name=module_name)
        if not targets:
            return _noop_async() if is_async else None
        if is_async:
            return _downgrade_async(spec, driver, config, targets)
        _downgrade_sync(spec, driver, config, targets)
        return None

    return reconcile, downgrade


async def _noop_async() -> None:
    """Return an empty coroutine when async reconciliation has no active extensions."""


def _runner_extension_name(module_name: str) -> str | None:
    if (
        module_name.startswith(_RECONCILE_MODULE_PREFIX)
        and len(module_name) > len(_RECONCILE_MODULE_PREFIX) + _UUID_HEX_SUFFIX_LEN
        and module_name[-_UUID_HEX_SUFFIX_LEN] == "_"
    ):
        return module_name[len(_RECONCILE_MODULE_PREFIX) : -_UUID_HEX_SUFFIX_LEN]
    return None


def _validate_context(spec: AdapterReconcilerSpec, context: MigrationContext) -> tuple[Any, Any, bool]:
    if context.config is None:
        msg = f"{spec.label} extension reconciliation requires context.config on MigrationContext."
        raise MigrationError(msg)
    if context.driver is None:
        msg = f"{spec.label} extension reconciliation requires context.driver on MigrationContext."
        raise MigrationError(msg)
    is_async = context.is_async_driver
    config_is_async = getattr(context.config, "is_async", None)
    if isinstance(config_is_async, bool) and config_is_async != is_async:
        mode_cfg = "async" if config_is_async else "sync"
        mode_drv = "async" if is_async else "sync"
        msg = f"Store/driver sync-async mismatch: config is {mode_cfg}, driver is {mode_drv}."
        raise MigrationError(msg)
    adapter_name = resolve_adapter_name(context.config)
    if adapter_name is not None and adapter_name not in spec.supported_adapters:
        msg = f"{spec.label} extension reconciler does not support adapter {adapter_name!r}."
        raise MigrationError(msg)
    return context.config, context.driver, is_async


def _infer_flat_extension(
    ctx_ext: dict[str, Any],
    cfg_ext: dict[str, dict[str, Any]],
) -> str | None:
    matching = [name for name in _KNOWN_EXTENSIONS if name in cfg_ext and cfg_ext[name] == ctx_ext]
    if len(matching) == 1:
        return matching[0]
    keys = set(ctx_ext)
    candidates: set[str] = set()
    if keys & _LITESTAR_DISCRIMINATORS:
        candidates.add("litestar")
    if keys & _EVENTS_DISCRIMINATORS:
        candidates.add("events")
    if keys & _ADK_DISCRIMINATORS:
        candidates.add("adk")
    if "session_table" in keys and "adk" not in candidates:
        if "adk" in cfg_ext and "litestar" not in cfg_ext:
            candidates.add("adk")
        else:
            candidates.add("litestar")
    if len(candidates) == 1:
        return next(iter(candidates))
    return None


def _ordered_extensions(config: Any, source_keys: Mapping[str, Any]) -> list[str]:
    migration_cfg = getattr(config, "migration_config", None)
    include_extensions = migration_cfg.get("include_extensions", ()) if isinstance(migration_cfg, Mapping) else ()
    ordered: list[str] = [
        name
        for name in include_extensions
        if isinstance(name, str) and name in _KNOWN_EXTENSIONS and name in source_keys
    ]
    for name in source_keys:
        if name in _KNOWN_EXTENSIONS and name not in ordered:
            ordered.append(name)
    return ordered


def _resolve_extension_targets(
    spec: AdapterReconcilerSpec,
    context: MigrationContext,
    config: Any,
    *,
    module_name: str,
) -> list[tuple[str, dict[str, Any]]]:
    raw_cfg_ext = getattr(config, "extension_config", None)
    cfg_ext: dict[str, dict[str, Any]] = (
        {name: dict(val) for name, val in raw_cfg_ext.items() if name in _KNOWN_EXTENSIONS and isinstance(val, Mapping)}
        if isinstance(raw_cfg_ext, Mapping)
        else {}
    )
    ctx_ext = dict(context.extension_config) if isinstance(context.extension_config, Mapping) else {}
    ctx_meta = context.metadata if isinstance(context.metadata, Mapping) else {}
    runner_ext = _runner_extension_name(module_name)

    if "migration_mode" in ctx_ext and not any(name in ctx_ext for name in _KNOWN_EXTENSIONS):
        ext_hint = runner_ext or _infer_flat_extension(ctx_ext, cfg_ext) or spec.fallback_extension_name
        mode = resolve_extension_migration_mode(None, ctx_ext, extension_name=ext_hint)
        if mode != "reconcile":
            return []

    if runner_ext in _KNOWN_EXTENSIONS:
        merged = dict(cfg_ext.get(runner_ext, {}))
        if not any(name in ctx_ext for name in _KNOWN_EXTENSIONS):
            merged.update({key: val for key, val in ctx_ext.items() if key != "extension"})
        if merged:
            mode = resolve_extension_migration_mode(None, merged, extension_name=runner_ext)
            if mode != "reconcile":
                return []
        return [(runner_ext, merged)]

    explicit_ext = ctx_meta.get("extension") or ctx_ext.get("extension")
    if explicit_ext is not None:
        if explicit_ext not in _KNOWN_EXTENSIONS:
            msg = f"Unsupported extension {explicit_ext!r} for {spec.label} reconciliation."
            raise MigrationError(msg)
        merged = dict(cfg_ext.get(explicit_ext, {}))
        merged.update({key: val for key, val in ctx_ext.items() if key != "extension"})
        mode = resolve_extension_migration_mode(None, merged, extension_name=explicit_ext)
        return [(explicit_ext, merged)] if mode == "reconcile" else []

    if any(name in ctx_ext for name in _KNOWN_EXTENSIONS):
        selected: list[tuple[str, dict[str, Any]]] = []
        for name in _ordered_extensions(config, ctx_ext):
            nested = ctx_ext[name]
            if not isinstance(nested, Mapping):
                msg = f"Extension '{name}' configuration must be a mapping."
                raise MigrationError(msg)
            merged = dict(cfg_ext.get(name, {}))
            merged.update(nested)
            if resolve_extension_migration_mode(None, merged, extension_name=name) == "reconcile":
                selected.append((name, merged))
        return selected

    if ctx_ext:
        inferred = _infer_flat_extension(ctx_ext, cfg_ext)
        if inferred is not None:
            merged = dict(cfg_ext.get(inferred, {}))
            merged.update(ctx_ext)
            mode = resolve_extension_migration_mode(None, merged, extension_name=inferred)
            return [(inferred, merged)] if mode == "reconcile" else []

    selected = []
    for name in _ordered_extensions(config, cfg_ext):
        merged = dict(cfg_ext[name])
        if len(cfg_ext) == 1 and ctx_ext:
            merged.update(ctx_ext)
        if resolve_extension_migration_mode(None, merged, extension_name=name) == "reconcile":
            selected.append((name, merged))
    return selected


def _resolve_flags(store: Any, settings: dict[str, Any]) -> tuple[bool, bool]:
    manage_schema, create_schema = store._schema_management_flags()
    if "manage_schema" in settings:
        manage_schema = bool(settings["manage_schema"])
    if "create_schema" in settings:
        create_schema = bool(settings["create_schema"])
    return manage_schema, create_schema


def _resolve_litestar_store(spec: AdapterReconcilerSpec, config: Any) -> BaseSQLSpecStore[Any]:
    try:
        store_cls = resolve_extension_store_class(
            config,
            extension="litestar",
            store_attr="litestar_store_class",
            store_suffix="Store",
        )
    except ImportError as exc:
        msg = f"Failed to import {spec.label} Litestar store: {exc}"
        raise MigrationError(msg) from exc
    if store_cls is None:
        mod = ensure_module(spec.litestar_module, package="vertebra-core[sqlspec]")
        store_cls = getattr(mod, spec.litestar_class)
    try:
        return cast("BaseSQLSpecStore[Any]", store_cls(config))
    except (ImproperConfigurationError, ValueError) as exc:
        msg = f"Invalid {spec.label} Litestar configuration for reconciliation: {exc}"
        raise MigrationError(msg) from exc


def _resolve_events_store(
    spec: AdapterReconcilerSpec,
    config: Any,
    settings: dict[str, Any],
) -> tuple[BaseEventQueueStore[Any], str, bool]:
    try:
        store_cls = resolve_extension_store_class(
            config,
            extension="events",
            store_attr="events_store_class",
            store_suffix="EventQueueStore",
        )
    except ImportError as exc:
        msg = f"Failed to import {spec.label} events store: {exc}"
        raise MigrationError(msg) from exc
    if store_cls is None:
        mod = ensure_module(spec.events_module, package="vertebra-core[sqlspec]")
        store_cls = getattr(mod, spec.events_class)
    try:
        store = cast("BaseEventQueueStore[Any]", store_cls(config))
        effective_settings = dict(store.settings)
        effective_settings.update(settings)
        backend_name = resolve_backend_name(config, effective_settings, resolve_adapter_name(config))
    except (ImproperConfigurationError, ValueError) as exc:
        raise MigrationError(str(exc)) from exc
    return store, backend_name, backend_name != "notify"


def _resolve_adk_stores(
    spec: AdapterReconcilerSpec,
    config: Any,
    settings: dict[str, Any],
    *,
    is_async: bool,
) -> tuple[list[Any], bool, bool]:
    adk_mod = ensure_module("sqlspec.extensions.adk", package="sqlspec")
    art_mod = ensure_module("sqlspec.extensions.adk.artifact.store", package="sqlspec")
    mem_mod = ensure_module("sqlspec.extensions.adk.memory.store", package="sqlspec")
    sess_mod = ensure_module("sqlspec.extensions.adk.store", package="sqlspec")

    try:
        validated = adk_mod.resolve_adk_config(config)
    except MigrationError:
        raise
    except SQLSpecError as exc:
        raise MigrationError(str(exc)) from exc

    merged_adk: dict[str, Any] = {**validated, **settings}
    enable_sessions = (
        bool(settings["enable_sessions"])
        if "enable_sessions" in settings
        else bool(adk_mod.adk_sessions_migration_enabled(config))
    )
    enable_memory = (
        bool(settings["enable_memory"])
        if "enable_memory" in settings
        else bool(adk_mod.adk_memory_migration_enabled(config))
    )
    has_artifacts = bool(merged_adk.get("artifact_table"))
    if not (enable_sessions or enable_memory or has_artifacts):
        return [], True, True

    stores: list[Any] = []
    try:
        if enable_sessions:
            session_store_cls = adk_mod.resolve_adk_store_class(config, "ADKStore")
            stores.append(session_store_cls(config=config))
        if enable_memory:
            memory_store_cls = adk_mod.resolve_adk_memory_store_class(config)
            if memory_store_cls is None:
                msg = "ADK memory reconciliation is enabled, but no ADKMemoryStore class could be resolved."
                raise MigrationError(msg)
            stores.append(memory_store_cls(config=config))
        if has_artifacts:
            artifact_store_cls = adk_mod.resolve_adk_artifact_store_class(config)
            if artifact_store_cls is not None:
                stores.append(artifact_store_cls(config=config))
    except MigrationError:
        raise
    except (SQLSpecError, ImportError, ValueError) as exc:
        msg = f"Failed to resolve {spec.label} ADK store for schema reconciliation: {exc}"
        raise MigrationError(msg) from exc

    async_bases = (sess_mod.BaseAsyncADKStore, mem_mod.BaseAsyncADKMemoryStore, art_mod.BaseAsyncADKArtifactStore)
    sync_bases = (sess_mod.BaseSyncADKStore, mem_mod.BaseSyncADKMemoryStore, art_mod.BaseSyncADKArtifactStore)
    for store in stores:
        if is_async and not isinstance(store, async_bases):
            msg = f"ADK store '{type(store).__name__}' is synchronous, but MigrationContext supplied an async driver."
            raise MigrationError(msg)
        if not is_async and not isinstance(store, sync_bases):
            msg = (
                f"ADK store '{type(store).__name__}' is asynchronous, "
                "but MigrationContext supplied a synchronous driver."
            )
            raise MigrationError(msg)

    if not stores:
        return [], True, True
    manage_schema, create_schema = _resolve_flags(stores[0], settings)
    return stores, manage_schema, create_schema


def _default_adk_store_drops(store: Any) -> list[DropStatement]:
    art_mod = ensure_module("sqlspec.extensions.adk.artifact.store", package="sqlspec")
    if isinstance(store, (art_mod.BaseSyncADKArtifactStore, art_mod.BaseAsyncADKArtifactStore)):
        build_drop = getattr(store, "_build_drop_artifact_table_sql", None)
        if callable(build_drop):
            return [DropStatement(statement) for statement in cast("list[str]", build_drop())]
        downgrade_fn = getattr(store, "downgrade_drops", None)
        if callable(downgrade_fn):
            return list(cast("list[DropStatement]", downgrade_fn()))
        return [DropStatement(f"DROP TABLE IF EXISTS {store.artifact_table}")]
    return list(cast("list[DropStatement]", store.downgrade_drops()))


def _adk_drops(spec: AdapterReconcilerSpec, config: Any, store: Any) -> list[DropStatement]:
    if spec.adk_store_drops is not None:
        return spec.adk_store_drops(config, store)
    return _default_adk_store_drops(store)


def _reconcile_adk_sync(spec: AdapterReconcilerSpec, driver: Any, config: Any, settings: dict[str, Any]) -> None:
    stores, manage_schema, create_schema = _resolve_adk_stores(spec, config, settings, is_async=False)
    if not stores or not manage_schema:
        return
    for store in stores:
        store.prepare_schema_sync(driver)
    for store in stores:
        store.prepare_reconcile_sync(driver)
    targets: list[SchemaTarget] = []
    for store in stores:
        if spec.adk_targets_sync is not None:
            targets.extend(spec.adk_targets_sync(driver, config, store))
        else:
            targets.extend(store.get_schema_targets_sync(driver))
    ensure_schema_sync(driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True)
    for store in stores:
        store.reconcile_companion_objects_sync(driver)
    for store in stores:
        store.finalize_reconcile_sync(driver)


async def _reconcile_adk_async(
    spec: AdapterReconcilerSpec,
    driver: Any,
    config: Any,
    settings: dict[str, Any],
) -> None:
    stores, manage_schema, create_schema = _resolve_adk_stores(spec, config, settings, is_async=True)
    if not stores or not manage_schema:
        return
    for store in stores:
        await store.prepare_schema_async(driver)
    for store in stores:
        await store.prepare_reconcile_async(driver)
    targets: list[SchemaTarget] = []
    for store in stores:
        if spec.adk_targets_async is not None:
            targets.extend(await spec.adk_targets_async(driver, config, store))
        else:
            targets.extend(await store.get_schema_targets_async(driver))
    await ensure_schema_async(
        driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True
    )
    for store in stores:
        await store.reconcile_companion_objects_async(driver)
    for store in stores:
        await store.finalize_reconcile_async(driver)


def _reconcile_sync(
    spec: AdapterReconcilerSpec,
    driver: Any,
    config: Any,
    targets: list[tuple[str, dict[str, Any]]],
) -> None:
    for ext_name, ext_settings in targets:
        if ext_name == "litestar":
            store = _resolve_litestar_store(spec, config)
            manage_schema, create_schema = _resolve_flags(store, ext_settings)
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
            event_store, backend_name, needs_table = _resolve_events_store(spec, config, ext_settings)
            if not needs_table:
                continue
            manage_schema, create_schema = _resolve_flags(event_store, ext_settings)
            if not manage_schema:
                continue
            event_store.prepare_schema_sync(driver)
            native_targets = event_store.native_backend_targets(backend_name)
            ensure_schema_sync(
                driver,
                event_store.get_schema_targets(),
                manage_schema=True,
                create_schema=create_schema,
                commit=False,
                strict=True,
            )
            event_store.reconcile_companion_objects_sync(driver)
            event_store.reconcile_native_backend_sync(driver, backend_name, native_targets, create_schema=create_schema)
        elif ext_name == "adk":
            _reconcile_adk_sync(spec, driver, config, ext_settings)


async def _reconcile_async(
    spec: AdapterReconcilerSpec,
    driver: Any,
    config: Any,
    targets: list[tuple[str, dict[str, Any]]],
) -> None:
    for ext_name, ext_settings in targets:
        if ext_name == "litestar":
            store = _resolve_litestar_store(spec, config)
            manage_schema, create_schema = _resolve_flags(store, ext_settings)
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
            event_store, backend_name, needs_table = _resolve_events_store(spec, config, ext_settings)
            if not needs_table:
                continue
            manage_schema, create_schema = _resolve_flags(event_store, ext_settings)
            if not manage_schema:
                continue
            await event_store.prepare_schema_async(driver)
            native_targets = event_store.native_backend_targets(backend_name)
            await ensure_schema_async(
                driver,
                event_store.get_schema_targets(),
                manage_schema=True,
                create_schema=create_schema,
                commit=False,
                strict=True,
            )
            await event_store.reconcile_companion_objects_async(driver)
            await event_store.reconcile_native_backend_async(
                driver, backend_name, native_targets, create_schema=create_schema
            )
        elif ext_name == "adk":
            await _reconcile_adk_async(spec, driver, config, ext_settings)


def _downgrade_sync(
    spec: AdapterReconcilerSpec,
    driver: Any,
    config: Any,
    targets: list[tuple[str, dict[str, Any]]],
) -> None:
    for ext_name, ext_settings in reversed(targets):
        if ext_name == "litestar":
            store = _resolve_litestar_store(spec, config)
            manage_schema, _ = _resolve_flags(store, ext_settings)
            if manage_schema:
                run_drop_statements_sync(driver, store.downgrade_drops())
        elif ext_name == "events":
            event_store, _backend_name, needs_table = _resolve_events_store(spec, config, ext_settings)
            if not needs_table:
                continue
            manage_schema, _ = _resolve_flags(event_store, ext_settings)
            if manage_schema:
                run_drop_statements_sync(driver, event_store.downgrade_drops())
        elif ext_name == "adk":
            stores, manage_schema, _ = _resolve_adk_stores(spec, config, ext_settings, is_async=False)
            if not stores or not manage_schema:
                continue
            for adk_store in reversed(stores):
                run_drop_statements_sync(driver, _adk_drops(spec, config, adk_store))


async def _downgrade_async(
    spec: AdapterReconcilerSpec,
    driver: Any,
    config: Any,
    targets: list[tuple[str, dict[str, Any]]],
) -> None:
    for ext_name, ext_settings in reversed(targets):
        if ext_name == "litestar":
            store = _resolve_litestar_store(spec, config)
            manage_schema, _ = _resolve_flags(store, ext_settings)
            if manage_schema:
                await run_drop_statements_async(driver, store.downgrade_drops())
        elif ext_name == "events":
            event_store, _backend_name, needs_table = _resolve_events_store(spec, config, ext_settings)
            if not needs_table:
                continue
            manage_schema, _ = _resolve_flags(event_store, ext_settings)
            if manage_schema:
                await run_drop_statements_async(driver, event_store.downgrade_drops())
        elif ext_name == "adk":
            stores, manage_schema, _ = _resolve_adk_stores(spec, config, ext_settings, is_async=True)
            if not stores or not manage_schema:
                continue
            for adk_store in reversed(stores):
                await run_drop_statements_async(driver, _adk_drops(spec, config, adk_store))
