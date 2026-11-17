"""Oracle extension schema reconciler for Litestar, Events, and ADK stores."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from sqlspec.exceptions import ImproperConfigurationError, MigrationError, SQLSpecError
from sqlspec.extensions.events._channel import _resolve_backend_name
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

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from sqlspec.extensions.events import BaseEventQueueStore
    from sqlspec.extensions.litestar.store import BaseSQLSpecStore
    from sqlspec.migrations.context import MigrationContext

__all__ = ("down", "downgrade", "reconcile")

_ALLOWED_ADAPTERS: frozenset[str] = frozenset({"vertebra_oracle", "oracle", "oracledb"})
_KNOWN_EXTENSIONS: tuple[str, ...] = ("litestar", "events", "adk")
_LITESTAR_DISCRIMINATORS: frozenset[str] = frozenset(
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
_EVENTS_DISCRIMINATORS: frozenset[str] = frozenset(
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
_ADK_DISCRIMINATORS: frozenset[str] = frozenset(
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


async def _noop_async() -> None:
    """Return an empty coroutine when async reconciliation is skipped."""


def _validate_context(context: MigrationContext) -> tuple[Any, Any, bool]:
    """Validate config, driver, adapter compatibility, and sync/async parity."""
    if context.config is None:
        msg = "Oracle extension reconciliation requires context.config on MigrationContext."
        raise MigrationError(msg)
    if context.driver is None:
        msg = "Oracle extension reconciliation requires context.driver on MigrationContext."
        raise MigrationError(msg)
    adapter_name = resolve_adapter_name(context.config)
    if adapter_name is not None and adapter_name not in _ALLOWED_ADAPTERS:
        msg = f"Oracle reconciler cannot handle adapter {adapter_name!r}."
        raise MigrationError(msg)
    is_async = context.is_async_driver
    if hasattr(context.config, "is_async") and bool(context.config.is_async) != is_async:
        mode_cfg = "async" if context.config.is_async else "sync"
        mode_drv = "async" if is_async else "sync"
        msg = f"Store/driver sync-async mismatch: config is {mode_cfg}, driver is {mode_drv}."
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


def _resolve_extension_targets(context: MigrationContext) -> list[tuple[str, dict[str, Any]]]:
    """Resolve active extension names and validated settings for reconciliation."""
    raw_cfg_ext = getattr(context.config, "extension_config", None)
    cfg_ext: dict[str, dict[str, Any]] = (
        {name: dict(val) for name, val in raw_cfg_ext.items() if name in _KNOWN_EXTENSIONS and isinstance(val, Mapping)}
        if isinstance(raw_cfg_ext, Mapping)
        else {}
    )
    ctx_ext = dict(context.extension_config) if isinstance(context.extension_config, Mapping) else {}
    ctx_meta = context.metadata if isinstance(context.metadata, Mapping) else {}

    explicit_ext = ctx_meta.get("extension") or ctx_ext.get("extension")
    if explicit_ext is not None:
        if explicit_ext not in _KNOWN_EXTENSIONS:
            msg = f"Unsupported extension {explicit_ext!r} for Oracle reconciliation."
            raise MigrationError(msg)
        merged = dict(cfg_ext.get(explicit_ext, {}))
        merged.update({key: val for key, val in ctx_ext.items() if key != "extension"})
        mode = resolve_extension_migration_mode(None, merged, extension_name=explicit_ext)
        return [(explicit_ext, merged)] if mode == "reconcile" else []

    if any(name in ctx_ext for name in _KNOWN_EXTENSIONS):
        selected: list[tuple[str, dict[str, Any]]] = []
        for name in _KNOWN_EXTENSIONS:
            if name not in ctx_ext:
                continue
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
        if "migration_mode" in ctx_ext and not cfg_ext:
            resolve_extension_migration_mode(None, ctx_ext, extension_name="extension")

    selected = []
    for name in _KNOWN_EXTENSIONS:
        if name not in cfg_ext:
            continue
        merged = dict(cfg_ext[name])
        if len(cfg_ext) == 1 and ctx_ext:
            merged.update(ctx_ext)
        if resolve_extension_migration_mode(None, merged, extension_name=name) == "reconcile":
            selected.append((name, merged))
    return selected


def _resolve_litestar_store(config: Any) -> BaseSQLSpecStore[Any]:
    from vertebra.extensions.sqlspec.adapters.oracle.litestar import OracleLitestarStore

    try:
        store_cls = resolve_extension_store_class(
            config, extension="litestar", store_attr="litestar_store_class", store_suffix="Store"
        )
    except ImportError as exc:
        msg = f"Failed to import Oracle Litestar store: {exc}"
        raise MigrationError(msg) from exc
    resolved_cls = store_cls if store_cls is not None else OracleLitestarStore
    try:
        return cast("BaseSQLSpecStore[Any]", resolved_cls(config))
    except (ImproperConfigurationError, ValueError) as exc:
        msg = f"Invalid Oracle Litestar configuration for reconciliation: {exc}"
        raise MigrationError(msg) from exc


def _resolve_events_store(config: Any, settings: dict[str, Any]) -> tuple[BaseEventQueueStore[Any], str, bool]:
    from vertebra.extensions.sqlspec.adapters.oracle.events import OracleEventQueueStore

    try:
        store_cls = resolve_extension_store_class(
            config, extension="events", store_attr="events_store_class", store_suffix="EventQueueStore"
        )
    except ImportError as exc:
        msg = f"Failed to import Oracle events store: {exc}"
        raise MigrationError(msg) from exc
    resolved_cls = store_cls if store_cls is not None else OracleEventQueueStore
    try:
        store = cast("BaseEventQueueStore[Any]", resolved_cls(config))
    except (ImproperConfigurationError, ValueError) as exc:
        raise MigrationError(str(exc)) from exc
    effective_settings = dict(store.settings)
    effective_settings.update(settings)
    try:
        backend_name = _resolve_backend_name(config, effective_settings, resolve_adapter_name(config))
    except ImproperConfigurationError as exc:
        raise MigrationError(str(exc)) from exc
    return store, backend_name, backend_name != "notify"


def _resolve_adk_stores(
    config: Any,
    settings: dict[str, Any],
    *,
    is_async_driver: bool,
) -> tuple[list[Any], bool, bool]:
    raw_ext = getattr(config, "extension_config", None)
    raw_adk = raw_ext.get("adk", {}) if isinstance(raw_ext, Mapping) else {}
    adk_config: dict[str, Any] = dict(raw_adk) if isinstance(raw_adk, Mapping) else {}
    adk_config.update(settings)

    enable_sessions = bool(adk_config.get("enable_sessions", True)) if "enable_sessions" in adk_config else True
    enable_memory = bool(adk_config.get("enable_memory", True)) if "enable_memory" in adk_config else True
    has_artifacts = bool(adk_config.get("artifact_table"))
    if not (enable_sessions or enable_memory or has_artifacts):
        return [], True, True

    from sqlspec.extensions.adk._config_utils import _adk_config_from_extension

    try:
        validated = _adk_config_from_extension(config)
        adk_config = {**validated, **settings}
    except MigrationError:
        raise
    except SQLSpecError as exc:
        raise MigrationError(str(exc)) from exc

    stores: list[Any] = []
    try:
        if enable_sessions:
            from sqlspec.extensions.adk.store import BaseAsyncADKStore, BaseSyncADKStore

            session_cls = getattr(config, "adk_store_class", None)
            if session_cls is None:
                msg = "Oracle config defines no adk_store_class for ADK session reconciliation."
                raise MigrationError(msg)
            session_store = session_cls(config=config)
            expected = BaseAsyncADKStore if is_async_driver else BaseSyncADKStore
            if not isinstance(session_store, expected):
                msg = f"ADK store '{type(session_store).__name__}' does not match driver sync/async mode."
                raise MigrationError(msg)
            stores.append(session_store)
        if enable_memory:
            from sqlspec.extensions.adk.memory.store import BaseAsyncADKMemoryStore, BaseSyncADKMemoryStore

            memory_cls = getattr(config, "adk_memory_store_class", None)
            if memory_cls is None:
                msg = "ADK memory reconciliation is enabled, but no adk_memory_store_class could be resolved."
                raise MigrationError(msg)
            memory_store = memory_cls(config=config)
            expected_mem = BaseAsyncADKMemoryStore if is_async_driver else BaseSyncADKMemoryStore
            if not isinstance(memory_store, expected_mem):
                msg = f"ADK store '{type(memory_store).__name__}' does not match driver sync/async mode."
                raise MigrationError(msg)
            stores.append(memory_store)
        if has_artifacts:
            from sqlspec.extensions.adk.artifact.store import BaseAsyncADKArtifactStore, BaseSyncADKArtifactStore

            artifact_cls = getattr(config, "adk_artifact_store_class", None)
            if artifact_cls is not None:
                artifact_store = artifact_cls(config=config)
                expected_art = BaseAsyncADKArtifactStore if is_async_driver else BaseSyncADKArtifactStore
                if not isinstance(artifact_store, expected_art):
                    msg = f"ADK store '{type(artifact_store).__name__}' does not match driver sync/async mode."
                    raise MigrationError(msg)
                stores.append(artifact_store)
    except MigrationError:
        raise
    except (SQLSpecError, ImportError, ValueError) as exc:
        msg = f"Failed to resolve Oracle ADK store for reconciliation: {exc}"
        raise MigrationError(msg) from exc

    if not stores:
        return [], True, True
    manage_schema, create_schema = stores[0]._schema_management_flags()
    if "manage_schema" in settings:
        manage_schema = bool(settings["manage_schema"])
    if "create_schema" in settings:
        create_schema = bool(settings["create_schema"])
    return stores, manage_schema, create_schema


def _adk_store_downgrade_drops(store: Any) -> list[DropStatement]:
    if hasattr(store, "downgrade_drops"):
        return list(store.downgrade_drops())
    build_drop = getattr(store, "_build_drop_artifact_table_sql", None)
    if callable(build_drop):
        return [DropStatement(statement) for statement in cast("list[str]", build_drop())]
    artifact_table = getattr(store, "artifact_table", None)
    if isinstance(artifact_table, str) and artifact_table:
        from vertebra.extensions.sqlspec.adapters.oracle.core import escape_oracle_literal, oracle_table_ref

        escaped = escape_oracle_literal(f"DROP TABLE {oracle_table_ref(artifact_table)}")
        plsql = (
            "BEGIN\n"
            f"    EXECUTE IMMEDIATE '{escaped}';\n"
            "EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF;\n"
            "END;"
        )
        return [DropStatement(plsql)]
    return []


def _reconcile_litestar_sync(driver: Any, config: Any, settings: dict[str, Any]) -> None:
    store = _resolve_litestar_store(config)
    manage_schema, create_schema = store._schema_management_flags()
    if "manage_schema" in settings:
        manage_schema = bool(settings["manage_schema"])
    if "create_schema" in settings:
        create_schema = bool(settings["create_schema"])
    if not manage_schema:
        return
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


async def _reconcile_litestar_async(driver: Any, config: Any, settings: dict[str, Any]) -> None:
    store = _resolve_litestar_store(config)
    manage_schema, create_schema = store._schema_management_flags()
    if "manage_schema" in settings:
        manage_schema = bool(settings["manage_schema"])
    if "create_schema" in settings:
        create_schema = bool(settings["create_schema"])
    if not manage_schema:
        return
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


def _reconcile_events_sync(driver: Any, config: Any, settings: dict[str, Any]) -> None:
    store, backend_name, needs_table = _resolve_events_store(config, settings)
    if not needs_table:
        return
    manage_schema, create_schema = store._schema_management_flags()
    if "manage_schema" in settings:
        manage_schema = bool(settings["manage_schema"])
    if "create_schema" in settings:
        create_schema = bool(settings["create_schema"])
    if not manage_schema:
        return
    store.prepare_schema_sync(driver)
    targets = store.native_backend_targets(backend_name)
    ensure_schema_sync(
        driver,
        store.get_schema_targets(),
        manage_schema=True,
        create_schema=create_schema,
        commit=False,
        strict=True,
    )
    store.reconcile_companion_objects_sync(driver)
    store.reconcile_native_backend_sync(driver, backend_name, targets, create_schema=create_schema)


async def _reconcile_events_async(driver: Any, config: Any, settings: dict[str, Any]) -> None:
    store, backend_name, needs_table = _resolve_events_store(config, settings)
    if not needs_table:
        return
    manage_schema, create_schema = store._schema_management_flags()
    if "manage_schema" in settings:
        manage_schema = bool(settings["manage_schema"])
    if "create_schema" in settings:
        create_schema = bool(settings["create_schema"])
    if not manage_schema:
        return
    await store.prepare_schema_async(driver)
    targets = store.native_backend_targets(backend_name)
    await ensure_schema_async(
        driver,
        store.get_schema_targets(),
        manage_schema=True,
        create_schema=create_schema,
        commit=False,
        strict=True,
    )
    await store.reconcile_companion_objects_async(driver)
    await store.reconcile_native_backend_async(driver, backend_name, targets, create_schema=create_schema)


def _reconcile_adk_sync(driver: Any, config: Any, settings: dict[str, Any]) -> None:
    stores, manage_schema, create_schema = _resolve_adk_stores(config, settings, is_async_driver=False)
    if not stores or not manage_schema:
        return
    for store in stores:
        store.prepare_schema_sync(driver)
    for store in stores:
        store.prepare_reconcile_sync(driver)
    targets: list[SchemaTarget] = []
    for store in stores:
        targets.extend(store.get_schema_targets_sync(driver))
    ensure_schema_sync(driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True)
    for store in stores:
        store.reconcile_companion_objects_sync(driver)
    for store in stores:
        store.finalize_reconcile_sync(driver)


async def _reconcile_adk_async(driver: Any, config: Any, settings: dict[str, Any]) -> None:
    stores, manage_schema, create_schema = _resolve_adk_stores(config, settings, is_async_driver=True)
    if not stores or not manage_schema:
        return
    for store in stores:
        await store.prepare_schema_async(driver)
    for store in stores:
        await store.prepare_reconcile_async(driver)
    targets: list[SchemaTarget] = []
    for store in stores:
        targets.extend(await store.get_schema_targets_async(driver))
    await ensure_schema_async(
        driver, targets, manage_schema=True, create_schema=create_schema, commit=False, strict=True
    )
    for store in stores:
        await store.reconcile_companion_objects_async(driver)
    for store in stores:
        await store.finalize_reconcile_async(driver)


def _reconcile_sync(driver: Any, config: Any, targets: list[tuple[str, dict[str, Any]]]) -> None:
    for ext_name, ext_settings in targets:
        if ext_name == "litestar":
            _reconcile_litestar_sync(driver, config, ext_settings)
        elif ext_name == "events":
            _reconcile_events_sync(driver, config, ext_settings)
        elif ext_name == "adk":
            _reconcile_adk_sync(driver, config, ext_settings)


async def _reconcile_async(driver: Any, config: Any, targets: list[tuple[str, dict[str, Any]]]) -> None:
    for ext_name, ext_settings in targets:
        if ext_name == "litestar":
            await _reconcile_litestar_async(driver, config, ext_settings)
        elif ext_name == "events":
            await _reconcile_events_async(driver, config, ext_settings)
        elif ext_name == "adk":
            await _reconcile_adk_async(driver, config, ext_settings)


def _downgrade_sync(driver: Any, config: Any, targets: list[tuple[str, dict[str, Any]]]) -> None:
    for ext_name, ext_settings in reversed(targets):
        if ext_name == "litestar":
            litestar_store = _resolve_litestar_store(config)
            manage_schema, _ = litestar_store._schema_management_flags()
            if "manage_schema" in ext_settings:
                manage_schema = bool(ext_settings["manage_schema"])
            if manage_schema:
                run_drop_statements_sync(driver, litestar_store.downgrade_drops())
        elif ext_name == "events":
            events_store, _backend_name, needs_table = _resolve_events_store(config, ext_settings)
            if not needs_table:
                continue
            manage_schema, _ = events_store._schema_management_flags()
            if "manage_schema" in ext_settings:
                manage_schema = bool(ext_settings["manage_schema"])
            if manage_schema:
                run_drop_statements_sync(driver, events_store.downgrade_drops())
        elif ext_name == "adk":
            stores, manage_schema, _ = _resolve_adk_stores(config, ext_settings, is_async_driver=False)
            if not stores or not manage_schema:
                continue
            for adk_store in reversed(stores):
                run_drop_statements_sync(driver, _adk_store_downgrade_drops(adk_store))


async def _downgrade_async(driver: Any, config: Any, targets: list[tuple[str, dict[str, Any]]]) -> None:
    for ext_name, ext_settings in reversed(targets):
        if ext_name == "litestar":
            litestar_store = _resolve_litestar_store(config)
            manage_schema, _ = litestar_store._schema_management_flags()
            if "manage_schema" in ext_settings:
                manage_schema = bool(ext_settings["manage_schema"])
            if manage_schema:
                await run_drop_statements_async(driver, litestar_store.downgrade_drops())
        elif ext_name == "events":
            events_store, _backend_name, needs_table = _resolve_events_store(config, ext_settings)
            if not needs_table:
                continue
            manage_schema, _ = events_store._schema_management_flags()
            if "manage_schema" in ext_settings:
                manage_schema = bool(ext_settings["manage_schema"])
            if manage_schema:
                await run_drop_statements_async(driver, events_store.downgrade_drops())
        elif ext_name == "adk":
            stores, manage_schema, _ = _resolve_adk_stores(config, ext_settings, is_async_driver=True)
            if not stores or not manage_schema:
                continue
            for adk_store in reversed(stores):
                await run_drop_statements_async(driver, _adk_store_downgrade_drops(adk_store))


def reconcile(context: MigrationContext) -> Awaitable[None] | None:
    """Converge Oracle Litestar, Events, and ADK extension schemas on the active driver."""
    config, driver, is_async = _validate_context(context)
    targets = _resolve_extension_targets(context)
    if not targets:
        return _noop_async() if is_async else None
    if is_async:
        return _reconcile_async(driver, config, targets)
    _reconcile_sync(driver, config, targets)
    return None


def downgrade(context: MigrationContext) -> Awaitable[None] | None:
    """Drop Oracle Litestar, Events, and ADK extension schemas on the active driver."""
    config, driver, is_async = _validate_context(context)
    targets = _resolve_extension_targets(context)
    if not targets:
        return _noop_async() if is_async else None
    if is_async:
        return _downgrade_async(driver, config, targets)
    _downgrade_sync(driver, config, targets)
    return None


down = downgrade
