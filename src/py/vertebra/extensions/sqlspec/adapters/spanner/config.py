"""Cloud Spanner and Spangres SQLSpec synchronous and asynchronous adapter configurations."""

import asyncio
import contextlib
import inspect
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Literal

from sqlspec.core import StatementConfig

from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    SpannerConfig,
    StalenessBound,
    SyncSpannerDriver,
)
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.driver import NotSupportedError
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.spanner.adk import (
    SpannerAsyncADKMemoryStore,
    SpannerAsyncADKStore,
    SpannerSyncADKMemoryStore,
    SpannerSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.spanner.core import (
    ADAPTER_NAME,
    SpannerConnectionShim,
    default_statement_config,
    normalize_spanner_dialect,
    resolve_spanner_config,
    resolve_storage_backend,
    sqlspec_dialect_name,
)
from vertebra.extensions.sqlspec.adapters.spanner.driver import (
    SpannerAsyncDriver,
    SpannerSyncDriver,
)
from vertebra.extensions.sqlspec.adapters.spanner.events import SpannerEventQueueStore
from vertebra.extensions.sqlspec.adapters.spanner.litestar import SpannerLitestarStore
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend

__all__ = (
    "SpangresAsyncConfig",
    "SpangresSyncConfig",
    "SpannerAsyncConfig",
    "SpannerSyncConfig",
)


def _reject_tenant_settings(
    tenant_predicate: Any,
    tenant_id: Any,
    driver_features: Mapping[str, Any] | None,
) -> None:
    if tenant_predicate is not None or tenant_id is not None:
        msg = "Tenant session enforcement is unsupported for Cloud Spanner"
        raise NotSupportedError(msg)
    if driver_features and (
        driver_features.get("tenant_predicate") is not None or driver_features.get("tenant_id") is not None
    ):
        msg = "Tenant session enforcement is unsupported for Cloud Spanner"
        raise NotSupportedError(msg)


def _filter_driver_features(
    driver_features: dict[str, Any] | None,
    *,
    storage_backend: ObjectStoreBackend | str | None,
    statement_cache_size: int,
    enable_sqlcommenter: bool,
) -> dict[str, Any]:
    features = dict(driver_features or {})
    if storage_backend is not None and "storage_backend" not in features:
        features["storage_backend"] = storage_backend
    if "statement_cache_size" not in features:
        features["statement_cache_size"] = statement_cache_size
    if enable_sqlcommenter and "enable_sqlcommenter" not in features:
        features["enable_sqlcommenter"] = True
    return features


class _SyncSnapshotAsyncProxy:
    """Async adapter delegating ``SyncSpannerSnapshot`` operations via ``asyncio.to_thread``."""

    __slots__ = ("_snapshot",)

    def __init__(self, snapshot: Any) -> None:
        self._snapshot = snapshot

    @property
    def staleness(self) -> Any:
        """Return the underlying snapshot staleness bound."""
        return getattr(self._snapshot, "staleness", None)

    async def __aenter__(self) -> "_SyncSnapshotAsyncProxy":
        enter = getattr(self._snapshot, "__enter__", None)
        if callable(enter):
            await asyncio.to_thread(enter)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        exit_fn = getattr(self._snapshot, "__exit__", None)
        if callable(exit_fn):
            await asyncio.to_thread(exit_fn, exc_type, exc, tb)

    async def query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a read-only query on a worker thread."""
        return list(await asyncio.to_thread(self._snapshot.query, sql, parameters, **kwargs))

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a read-only select on a worker thread."""
        return list(await asyncio.to_thread(self._snapshot.select, sql, parameters, **kwargs))

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute an Arrow query on a worker thread."""
        return await asyncio.to_thread(self._snapshot.select_to_arrow, sql, parameters, **kwargs)


class _SyncSpannerAsyncProxy:
    """Async core-driver proxy offloading ``SyncSpannerDriver`` calls via ``asyncio.to_thread``."""

    __slots__ = ("_sync",)

    def __init__(self, sync_driver: SyncSpannerDriver) -> None:
        self._sync = sync_driver

    @property
    def config(self) -> SpannerConfig:
        """Return the underlying ``SpannerConfig``."""
        return self._sync.config

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the Spanner dialect literal from the synchronous driver."""
        return self._sync.dialect

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the normalized Spanner dialect literal."""
        return normalize_spanner_dialect(self._sync.config.dialect)

    @property
    def project_id(self) -> str:
        """Return the Google Cloud project ID."""
        return self._sync.project_id

    @property
    def instance_id(self) -> str:
        """Return the Cloud Spanner instance ID."""
        return self._sync.instance_id

    @property
    def database_id(self) -> str:
        """Return the Cloud Spanner database ID."""
        return self._sync.database_id

    @property
    def last_commit_stats(self) -> dict[str, Any]:
        """Return commit statistics recorded on the synchronous driver."""
        return dict(getattr(self._sync, "last_commit_stats", {}) or {})

    @property
    def active_transaction(self) -> Any:
        """Return the active interactive transaction on the synchronous driver, if any."""
        return getattr(self._sync, "active_transaction", None)

    @property
    def transaction_id(self) -> bytes | None:
        """Return the active transaction identifier bytes on the synchronous driver, if any."""
        return getattr(self._sync, "transaction_id", None)

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> _SyncSnapshotAsyncProxy:
        """Open a thread-offloaded async snapshot wrapper over ``SyncSpannerSnapshot``."""
        return _SyncSnapshotAsyncProxy(self._sync.snapshot(staleness, multi_use=multi_use))

    async def connect(self) -> "_SyncSpannerAsyncProxy":
        """Connect the underlying synchronous driver on a worker thread."""
        await asyncio.to_thread(self._sync.connect)
        return self

    async def close(self) -> None:
        """Close the underlying synchronous driver on a worker thread."""
        await asyncio.to_thread(self._sync.close)

    async def begin(self, **kwargs: Any) -> None:
        """Begin a transaction on the synchronous driver on a worker thread."""
        await asyncio.to_thread(self._sync.begin, **kwargs)

    async def commit(self) -> None:
        """Commit the active transaction on a worker thread."""
        await asyncio.to_thread(self._sync.commit)

    async def rollback(self) -> None:
        """Roll back the active transaction on a worker thread."""
        await asyncio.to_thread(self._sync.rollback)

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute DML or DDL on the synchronous driver via ``asyncio.to_thread``."""
        return int(await asyncio.to_thread(self._sync.execute, sql, parameters, **kwargs))

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute parameterized batch DML on a worker thread."""
        return int(await asyncio.to_thread(self._sync.execute_many, sql, parameter_sets, **kwargs))

    async def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> list[int]:
        """Execute batch DML on a worker thread."""
        return list(await asyncio.to_thread(self._sync.execute_batch_dml, statements, **kwargs))

    async def execute_partitioned_dml(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute partitioned DML on a worker thread."""
        return int(await asyncio.to_thread(self._sync.execute_partitioned_dml, sql, parameters, **kwargs))

    async def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> int:
        """Execute a SQL script on a worker thread."""
        return int(await asyncio.to_thread(self._sync.execute_script, sql, parameters))

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query returning rows and column names on a worker thread."""
        if staleness is not None:
            return await asyncio.to_thread(
                self._sync.execute_with_description, sql, parameters, staleness=staleness, **kwargs
            )
        return await asyncio.to_thread(self._sync.execute_with_description, sql, parameters, **kwargs)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a select query on a worker thread."""
        if staleness is not None:
            return list(
                await asyncio.to_thread(
                    self._sync.select,
                    sql,
                    parameters,
                    schema_type=schema_type,
                    staleness=staleness,
                    **kwargs,
                )
            )
        return list(await asyncio.to_thread(self._sync.select, sql, parameters, schema_type=schema_type, **kwargs))

    async def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Execute Spanner DDL statements on a worker thread."""
        return str(await asyncio.to_thread(self._sync.update_ddl, statements))

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute an Arrow read on a worker thread."""
        return await asyncio.to_thread(self._sync.select_to_arrow, sql, parameters, **kwargs)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        **kwargs: Any,
    ) -> int:
        """Load Arrow records on a worker thread."""
        return int(await asyncio.to_thread(self._sync.load_from_arrow, table_name, data, **kwargs))

    async def apply_mutations(
        self,
        mutations: Sequence[Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Apply Spanner mutations on a worker thread."""
        return dict(await asyncio.to_thread(self._sync.apply_mutations, mutations, **kwargs))

    async def batch_write(
        self,
        mutation_groups: Sequence[Any],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Execute BatchWrite on a worker thread."""
        return list(await asyncio.to_thread(self._sync.batch_write, mutation_groups, **kwargs))

    async def run_in_transaction(self, fn: Any, **kwargs: Any) -> Any:
        """Run a synchronous transaction callback on a worker thread."""
        return await asyncio.to_thread(self._sync.run_in_transaction, fn, **kwargs)


class SpannerSyncConfig(SyncAdapterConfig[SpannerConfig, SpannerSyncDriver]):
    """Synchronous SQLSpec adapter configuration for Cloud Spanner GoogleSQL and Spangres."""

    adapter_name: ClassVar[str] = ADAPTER_NAME
    dialect: ClassVar[str] = "spanner"
    is_async: ClassVar[bool] = False
    connection_type: ClassVar[type[Any]] = SyncSpannerDriver
    _config_cls: ClassVar[type[SpannerConfig]] = SpannerConfig
    _driver_cls: ClassVar[type[SyncSpannerDriver]] = SyncSpannerDriver
    driver_type: ClassVar[type[SpannerSyncDriver]] = SpannerSyncDriver
    adk_store_class: ClassVar[type[Any]] = SpannerSyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = SpannerSyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = SpannerEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = SpannerLitestarStore

    def __init__(
        self,
        config: SpannerConfig | Mapping[str, Any] | str | None = None,
        *,
        dsn: str | None = None,
        project_id: str | None = None,
        instance_id: str | None = None,
        database_id: str | None = None,
        database_role: str | None = None,
        dialect: str = "googlesql",
        endpoint: str | None = None,
        admin_endpoint: str | None = None,
        pool_size: int = 4,
        route_to_leader_enabled: bool = True,
        max_commit_delay_ms: int | None = None,
        data_boost_enabled: bool = True,
        max_partitions: int = 4,
        credentials_path: str | None = None,
        service_account_key: str | dict[str, Any] | None = None,
        quota_project_id: str | None = None,
        impersonate_service_account: str | None = None,
        use_gcloud_cli: bool = False,
        scopes: Sequence[str] | None = None,
        access_token: str | None = None,
        use_shm_cache: bool = True,
        statement_cache_capacity: int = 512,
        ca_pem: bytes | None = None,
        storage_backend: ObjectStoreBackend | str | None = None,
        connection_instance: Any = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = False,
        tenant_predicate: Any = None,
        tenant_id: Any = None,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
    ) -> None:
        _reject_tenant_settings(tenant_predicate, tenant_id, driver_features)
        config_broker = getattr(config, "broker", None) if isinstance(config, SpannerConfig) else None
        resolved_broker = (
            broker
            if broker is not None
            else config_broker
            if config_broker is not None
            else DatabaseRegistry.default_broker()
            if share_pool
            else None
        )
        resolved_cfg = resolve_spanner_config(
            config,
            dsn=dsn,
            project_id=project_id,
            instance_id=instance_id,
            database_id=database_id,
            database_role=database_role,
            dialect=dialect,
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
            pool_size=pool_size,
            route_to_leader_enabled=route_to_leader_enabled,
            max_commit_delay_ms=max_commit_delay_ms,
            data_boost_enabled=data_boost_enabled,
            max_partitions=max_partitions,
            credentials_path=credentials_path,
            service_account_key=service_account_key,
            quota_project_id=quota_project_id,
            impersonate_service_account=impersonate_service_account,
            use_gcloud_cli=use_gcloud_cli,
            scopes=scopes,
            access_token=access_token,
            use_shm_cache=use_shm_cache,
            statement_cache_capacity=statement_cache_capacity,
            ca_pem=ca_pem,
            broker=resolved_broker,
        )
        self._spanner_dialect: Literal["googlesql", "spangres"] = normalize_spanner_dialect(resolved_cfg.dialect)
        effective_stmt_cfg = statement_config or default_statement_config(self._spanner_dialect)
        merged_features = _filter_driver_features(
            driver_features,
            storage_backend=storage_backend,
            statement_cache_size=statement_cache_size,
            enable_sqlcommenter=enable_sqlcommenter,
        )
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=effective_stmt_cfg,
            connection_config=connection_config,
            migration_config=migration_config,
            driver_features=merged_features,
            extension_config=extension_config,
            observability_config=observability_config,
        )
        object.__setattr__(self, "dialect", sqlspec_dialect_name(self._spanner_dialect))
        self._resolved_config: SpannerConfig = resolved_cfg
        self.connection_instance = connection_instance
        self.storage_backend = storage_backend
        self.statement_cache_size = statement_cache_size
        self.enable_sqlcommenter = enable_sqlcommenter
        self.tenant_predicate = tenant_predicate
        self.tenant_id = tenant_id
        self._cached_core: SyncSpannerDriver | None = None

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the underlying Vertebra Spanner dialect literal (``googlesql`` or ``spangres``)."""
        return self._spanner_dialect

    def resolve_spanner_config(self) -> SpannerConfig:
        """Return the canonical ``SpannerConfig`` for this SQLSpec configuration."""
        return self._resolved_config

    def resolve_storage_backend(self) -> ObjectStoreBackend | None:
        """Resolve the configured ``ObjectStoreBackend`` instance, if any."""
        return resolve_storage_backend(self.storage_backend)

    def _resolve_core_driver(self) -> Any:
        if self.connection_instance is not None:
            if isinstance(self.connection_instance, SpannerConnectionShim):
                return self.connection_instance.driver
            return self.connection_instance
        if self._cached_core is None:
            self._cached_core = SyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_core

    def create_driver(self) -> SpannerSyncDriver:
        """Create a standalone ``SpannerSyncDriver`` over the resolved core driver."""
        return SpannerSyncDriver(
            self._resolve_core_driver(),
            statement_config=self.statement_config,
            driver_features=dict(self.driver_features),
            observability=self.observability_config,
            shared_pool=bool(self.share_pool),
            tenant_predicate=self.tenant_predicate,
            tenant_id=self.tenant_id,
            storage_backend=self.resolve_storage_backend(),
        )

    def provide_driver(
        self,
        broker: Any = None,
        statement_cache: Any = None,
    ) -> SpannerSyncDriver:
        """Return a configured ``SpannerSyncDriver`` instance."""
        _ = broker, statement_cache
        return self.create_driver()

    def create_connection(self) -> Any:
        """Return a ``SpannerConnectionShim`` over the synchronous core driver."""
        core = self._resolve_core_driver()
        return SpannerConnectionShim(core) if isinstance(core, SyncSpannerDriver) else core

    def close(self) -> None:
        """Close the cached synchronous Spanner core driver and adapter pool."""
        if self._cached_core is not None:
            close_fn = getattr(self._cached_core, "close", None)
            if callable(close_fn):
                close_fn()
            self._cached_core = None
        super().close()

    @contextmanager
    def provide_session(
        self,
        *args: Any,
        transaction: bool = False,
        staleness: StalenessBound | None = None,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> Iterator[SpannerSyncDriver]:
        """Yield an isolated ``SpannerSyncDriver`` for autocommit, ReadOnly snapshot, or ReadWrite transaction."""
        _ = args, kwargs
        core = self._resolve_core_driver()
        storage = self.resolve_storage_backend()
        features = dict(self.driver_features)
        if not transaction:
            use_snapshot = bool(read_only) if read_only is not None else (staleness is not None)
            if not use_snapshot:
                yield SpannerSyncDriver(
                    core,
                    statement_config=self.statement_config,
                    driver_features=features,
                    observability=self.observability_config,
                    shared_pool=bool(self.share_pool),
                    tenant_predicate=self.tenant_predicate,
                    tenant_id=self.tenant_id,
                    read_only=False,
                    in_transaction=False,
                    staleness=None,
                    active_snapshot=None,
                    storage_backend=storage,
                )
                return
            snapshot_fn = getattr(core, "snapshot", None)
            if callable(snapshot_fn):
                snap_ctx = snapshot_fn(staleness, multi_use=True)
                if hasattr(snap_ctx, "__enter__"):
                    with snap_ctx as snap:
                        yield SpannerSyncDriver(
                            core,
                            statement_config=self.statement_config,
                            driver_features=features,
                            observability=self.observability_config,
                            shared_pool=bool(self.share_pool),
                            tenant_predicate=self.tenant_predicate,
                            tenant_id=self.tenant_id,
                            read_only=True,
                            in_transaction=False,
                            staleness=staleness,
                            active_snapshot=snap,
                            storage_backend=storage,
                        )
                    return
                yield SpannerSyncDriver(
                    core,
                    statement_config=self.statement_config,
                    driver_features=features,
                    observability=self.observability_config,
                    shared_pool=bool(self.share_pool),
                    tenant_predicate=self.tenant_predicate,
                    tenant_id=self.tenant_id,
                    read_only=True,
                    in_transaction=False,
                    staleness=staleness,
                    active_snapshot=snap_ctx,
                    storage_backend=storage,
                )
                return
            yield SpannerSyncDriver(
                core,
                statement_config=self.statement_config,
                driver_features=features,
                observability=self.observability_config,
                shared_pool=bool(self.share_pool),
                tenant_predicate=self.tenant_predicate,
                tenant_id=self.tenant_id,
                read_only=True,
                in_transaction=False,
                staleness=staleness,
                active_snapshot=None,
                storage_backend=storage,
            )
            return
        begin_fn = getattr(core, "begin", None)
        if callable(begin_fn):
            begin_fn()
        session_driver = SpannerSyncDriver(
            core,
            statement_config=self.statement_config,
            driver_features=features,
            observability=self.observability_config,
            shared_pool=bool(self.share_pool),
            tenant_predicate=self.tenant_predicate,
            tenant_id=self.tenant_id,
            read_only=False,
            in_transaction=True,
            storage_backend=storage,
        )
        try:
            yield session_driver
        except BaseException:
            if session_driver.in_transaction:
                rollback_fn = getattr(core, "rollback", None)
                if callable(rollback_fn):
                    with contextlib.suppress(Exception):
                        rollback_fn()
            raise
        else:
            if session_driver.in_transaction:
                commit_fn = getattr(core, "commit", None)
                if callable(commit_fn):
                    commit_fn()


class SpannerAsyncConfig(AsyncAdapterConfig[SpannerConfig, SpannerAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for Cloud Spanner GoogleSQL and Spangres."""

    adapter_name: ClassVar[str] = ADAPTER_NAME
    dialect: ClassVar[str] = "spanner"
    is_async: ClassVar[bool] = True
    connection_type: ClassVar[type[Any]] = AsyncSpannerDriver
    _config_cls: ClassVar[type[SpannerConfig]] = SpannerConfig
    _driver_cls: ClassVar[type[AsyncSpannerDriver]] = AsyncSpannerDriver
    driver_type: ClassVar[type[SpannerAsyncDriver]] = SpannerAsyncDriver
    adk_store_class: ClassVar[type[Any]] = SpannerAsyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = SpannerAsyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = SpannerEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = SpannerLitestarStore

    def __init__(
        self,
        config: SpannerConfig | Mapping[str, Any] | str | None = None,
        *,
        dsn: str | None = None,
        project_id: str | None = None,
        instance_id: str | None = None,
        database_id: str | None = None,
        database_role: str | None = None,
        dialect: str = "googlesql",
        endpoint: str | None = None,
        admin_endpoint: str | None = None,
        pool_size: int = 4,
        route_to_leader_enabled: bool = True,
        max_commit_delay_ms: int | None = None,
        data_boost_enabled: bool = True,
        max_partitions: int = 4,
        credentials_path: str | None = None,
        service_account_key: str | dict[str, Any] | None = None,
        quota_project_id: str | None = None,
        impersonate_service_account: str | None = None,
        use_gcloud_cli: bool = False,
        scopes: Sequence[str] | None = None,
        access_token: str | None = None,
        use_shm_cache: bool = True,
        statement_cache_capacity: int = 512,
        ca_pem: bytes | None = None,
        storage_backend: ObjectStoreBackend | str | None = None,
        connection_instance: Any = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = False,
        tenant_predicate: Any = None,
        tenant_id: Any = None,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
    ) -> None:
        _reject_tenant_settings(tenant_predicate, tenant_id, driver_features)
        config_broker = getattr(config, "broker", None) if isinstance(config, SpannerConfig) else None
        resolved_broker = (
            broker
            if broker is not None
            else config_broker
            if config_broker is not None
            else DatabaseRegistry.default_broker()
            if share_pool
            else None
        )
        resolved_cfg = resolve_spanner_config(
            config,
            dsn=dsn,
            project_id=project_id,
            instance_id=instance_id,
            database_id=database_id,
            database_role=database_role,
            dialect=dialect,
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
            pool_size=pool_size,
            route_to_leader_enabled=route_to_leader_enabled,
            max_commit_delay_ms=max_commit_delay_ms,
            data_boost_enabled=data_boost_enabled,
            max_partitions=max_partitions,
            credentials_path=credentials_path,
            service_account_key=service_account_key,
            quota_project_id=quota_project_id,
            impersonate_service_account=impersonate_service_account,
            use_gcloud_cli=use_gcloud_cli,
            scopes=scopes,
            access_token=access_token,
            use_shm_cache=use_shm_cache,
            statement_cache_capacity=statement_cache_capacity,
            ca_pem=ca_pem,
            broker=resolved_broker,
        )
        self._spanner_dialect: Literal["googlesql", "spangres"] = normalize_spanner_dialect(resolved_cfg.dialect)
        effective_stmt_cfg = statement_config or default_statement_config(self._spanner_dialect)
        merged_features = _filter_driver_features(
            driver_features,
            storage_backend=storage_backend,
            statement_cache_size=statement_cache_size,
            enable_sqlcommenter=enable_sqlcommenter,
        )
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=effective_stmt_cfg,
            connection_config=connection_config,
            migration_config=migration_config,
            driver_features=merged_features,
            extension_config=extension_config,
            observability_config=observability_config,
        )
        object.__setattr__(self, "dialect", sqlspec_dialect_name(self._spanner_dialect))
        self._resolved_config: SpannerConfig = resolved_cfg
        self.connection_instance = connection_instance
        self.storage_backend = storage_backend
        self.statement_cache_size = statement_cache_size
        self.enable_sqlcommenter = enable_sqlcommenter
        self.tenant_predicate = tenant_predicate
        self.tenant_id = tenant_id
        self._cached_core: AsyncSpannerDriver | None = None
        self._cached_sync_core: SyncSpannerDriver | None = None

    @property
    def spanner_dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the underlying Vertebra Spanner dialect literal (``googlesql`` or ``spangres``)."""
        return self._spanner_dialect

    def resolve_spanner_config(self) -> SpannerConfig:
        """Return the canonical ``SpannerConfig`` for this SQLSpec configuration."""
        return self._resolved_config

    def resolve_storage_backend(self) -> ObjectStoreBackend | None:
        """Resolve the configured ``ObjectStoreBackend`` instance, if any."""
        return resolve_storage_backend(self.storage_backend)

    def _resolve_core_driver(self) -> Any:
        if self.connection_instance is not None:
            if isinstance(self.connection_instance, SpannerConnectionShim):
                return _SyncSpannerAsyncProxy(self.connection_instance.driver)
            if isinstance(self.connection_instance, SyncSpannerDriver):
                return _SyncSpannerAsyncProxy(self.connection_instance)
            return self.connection_instance
        if self._cached_core is None:
            self._cached_core = AsyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_core

    def _resolve_sync_core_driver(self) -> Any:
        if isinstance(self.connection_instance, SyncSpannerDriver):
            return self.connection_instance
        if isinstance(self.connection_instance, SpannerConnectionShim):
            return self.connection_instance.driver
        if self._cached_sync_core is None:
            self._cached_sync_core = SyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_sync_core

    def create_driver(self) -> SpannerAsyncDriver:
        """Create a standalone ``SpannerAsyncDriver`` over the resolved core driver."""
        return SpannerAsyncDriver(
            self._resolve_core_driver(),
            statement_config=self.statement_config,
            driver_features=dict(self.driver_features),
            observability=self.observability_config,
            shared_pool=bool(self.share_pool),
            tenant_predicate=self.tenant_predicate,
            tenant_id=self.tenant_id,
            storage_backend=self.resolve_storage_backend(),
        )

    def provide_driver(
        self,
        broker: Any = None,
        statement_cache: Any = None,
    ) -> SpannerAsyncDriver:
        """Return a configured ``SpannerAsyncDriver`` instance."""
        _ = broker, statement_cache
        return self.create_driver()

    def create_connection(self) -> Any:
        """Return a ``SpannerConnectionShim`` for synchronous SQLSpec tooling."""
        core = self._resolve_sync_core_driver()
        return SpannerConnectionShim(core) if isinstance(core, SyncSpannerDriver) else core

    async def close(self) -> None:
        """Close cached asynchronous and synchronous Spanner core drivers and adapter pool."""
        if self._cached_core is not None:
            close_fn = getattr(self._cached_core, "close", None)
            if callable(close_fn):
                res = close_fn()
                if inspect.isawaitable(res):
                    await res
            self._cached_core = None
        if self._cached_sync_core is not None:
            sync_close_fn = getattr(self._cached_sync_core, "close", None)
            if callable(sync_close_fn):
                await asyncio.to_thread(sync_close_fn)
            self._cached_sync_core = None
        await super().close()

    @asynccontextmanager
    async def provide_session(
        self,
        *args: Any,
        transaction: bool = False,
        staleness: StalenessBound | None = None,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[SpannerAsyncDriver]:
        """Yield an isolated ``SpannerAsyncDriver`` for autocommit, ReadOnly snapshot, or ReadWrite transaction."""
        _ = args, kwargs
        core = self._resolve_core_driver()
        storage = self.resolve_storage_backend()
        features = dict(self.driver_features)
        if not transaction:
            use_snapshot = bool(read_only) if read_only is not None else (staleness is not None)
            if not use_snapshot:
                yield SpannerAsyncDriver(
                    core,
                    statement_config=self.statement_config,
                    driver_features=features,
                    observability=self.observability_config,
                    shared_pool=bool(self.share_pool),
                    tenant_predicate=self.tenant_predicate,
                    tenant_id=self.tenant_id,
                    read_only=False,
                    in_transaction=False,
                    staleness=None,
                    active_snapshot=None,
                    storage_backend=storage,
                )
                return
            snapshot_fn = getattr(core, "snapshot", None)
            if callable(snapshot_fn):
                snap_ctx = snapshot_fn(staleness, multi_use=True)
                if hasattr(snap_ctx, "__aenter__"):
                    async with snap_ctx as snap:
                        yield SpannerAsyncDriver(
                            core,
                            statement_config=self.statement_config,
                            driver_features=features,
                            observability=self.observability_config,
                            shared_pool=bool(self.share_pool),
                            tenant_predicate=self.tenant_predicate,
                            tenant_id=self.tenant_id,
                            read_only=True,
                            in_transaction=False,
                            staleness=staleness,
                            active_snapshot=snap,
                            storage_backend=storage,
                        )
                    return
                if hasattr(snap_ctx, "__enter__"):
                    with snap_ctx as snap:
                        yield SpannerAsyncDriver(
                            core,
                            statement_config=self.statement_config,
                            driver_features=features,
                            observability=self.observability_config,
                            shared_pool=bool(self.share_pool),
                            tenant_predicate=self.tenant_predicate,
                            tenant_id=self.tenant_id,
                            read_only=True,
                            in_transaction=False,
                            staleness=staleness,
                            active_snapshot=snap,
                            storage_backend=storage,
                        )
                    return
                yield SpannerAsyncDriver(
                    core,
                    statement_config=self.statement_config,
                    driver_features=features,
                    observability=self.observability_config,
                    shared_pool=bool(self.share_pool),
                    tenant_predicate=self.tenant_predicate,
                    tenant_id=self.tenant_id,
                    read_only=True,
                    in_transaction=False,
                    staleness=staleness,
                    active_snapshot=snap_ctx,
                    storage_backend=storage,
                )
                return
            yield SpannerAsyncDriver(
                core,
                statement_config=self.statement_config,
                driver_features=features,
                observability=self.observability_config,
                shared_pool=bool(self.share_pool),
                tenant_predicate=self.tenant_predicate,
                tenant_id=self.tenant_id,
                read_only=True,
                in_transaction=False,
                staleness=staleness,
                active_snapshot=None,
                storage_backend=storage,
            )
            return
        begin_fn = getattr(core, "begin", None)
        if callable(begin_fn):
            res = begin_fn()
            if inspect.isawaitable(res):
                await res
        session_driver = SpannerAsyncDriver(
            core,
            statement_config=self.statement_config,
            driver_features=features,
            observability=self.observability_config,
            shared_pool=bool(self.share_pool),
            tenant_predicate=self.tenant_predicate,
            tenant_id=self.tenant_id,
            read_only=False,
            in_transaction=True,
            storage_backend=storage,
        )
        try:
            yield session_driver
        except BaseException:
            if session_driver.in_transaction:
                rollback_fn = getattr(core, "rollback", None)
                if callable(rollback_fn):
                    with contextlib.suppress(Exception):
                        rb_res = rollback_fn()
                        if inspect.isawaitable(rb_res):
                            await asyncio.shield(rb_res)
            raise
        else:
            if session_driver.in_transaction:
                commit_fn = getattr(core, "commit", None)
                if callable(commit_fn):
                    cm_res = commit_fn()
                    if inspect.isawaitable(cm_res):
                        await cm_res


class SpangresSyncConfig(SpannerSyncConfig):
    """Synchronous SQLSpec adapter configuration defaulting to Spanner PostgreSQL (``spangres``)."""

    adapter_name: ClassVar[str] = ADAPTER_NAME
    dialect: ClassVar[str] = "spangres"
    is_async: ClassVar[bool] = False
    adk_store_class: ClassVar[type[Any]] = SpannerSyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = SpannerSyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = SpannerEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = SpannerLitestarStore

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("dialect", "spangres")
        super().__init__(*args, **kwargs)
        object.__setattr__(self, "dialect", "spangres")


class SpangresAsyncConfig(SpannerAsyncConfig):
    """Asynchronous SQLSpec adapter configuration defaulting to Spanner PostgreSQL (``spangres``)."""

    adapter_name: ClassVar[str] = ADAPTER_NAME
    dialect: ClassVar[str] = "spangres"
    is_async: ClassVar[bool] = True
    adk_store_class: ClassVar[type[Any]] = SpannerAsyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = SpannerAsyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = SpannerEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = SpannerLitestarStore

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("dialect", "spangres")
        super().__init__(*args, **kwargs)
        object.__setattr__(self, "dialect", "spangres")
