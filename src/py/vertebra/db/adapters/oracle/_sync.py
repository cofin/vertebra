"""Synchronous native Oracle 26ai Thin driver and connection pool."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, ClassVar, Self

from vertebra.db.adapters.oracle.config import OracleConfig, OracleSessionAttrs
from vertebra.db.adapters.oracle.core import ORACLE_DIALECT_TAG, OracleCore
from vertebra.db.adapters.oracle.notify import (
    OracleNotifyConfig,
    SyncOracleNotificationSource,
    validate_oracle_channel_name,
)
from vertebra.db.adapters.oracle.pipeline import SyncOraclePipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import InterfaceError, NotificationEvent, SyncDatabaseDriver
from vertebra.db.rls import TenantScopeConfig
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.cdc import ChangeFeed

__all__ = (
    "SyncOracleDriver",
    "SyncOraclePool",
)

_DEFAULT_TENANT_SCOPE = TenantScopeConfig()
_SYNC_PINNED_TX: ContextVar[dict[int, tuple[int, int | None, Any, bool]] | None] = ContextVar(
    "_SYNC_PINNED_TX",
    default=None,
)


class SyncOraclePool:
    """Synchronous physical Oracle connection pool backed by `vertebra._oracle.SyncOraclePool`."""

    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        native_pool: Any | None = None,
        **kwargs: Any,
    ) -> None:
        resolved_cfg, pool_kwargs = OracleConfig.resolve_pool_kwargs(config, kwargs)
        self._config: OracleConfig = resolved_cfg
        if native_pool is not None:
            self._pool = native_pool
        else:
            mod = OracleCore.require_native_module()
            self._pool = mod.SyncOraclePool(**pool_kwargs)

    @property
    def config(self) -> OracleConfig:
        """Return the `OracleConfig` governing this connection pool."""
        return self._config

    @property
    def native_pool(self) -> Any:
        """Return the underlying `vertebra._oracle.SyncOraclePool` instance."""
        return self._pool

    @property
    def min_size(self) -> int:
        """Minimum number of pooled physical connections."""
        return int(self._pool.min_size)

    @property
    def max_size(self) -> int:
        """Maximum number of pooled physical connections."""
        return int(self._pool.max_size)

    @property
    def size(self) -> int:
        """Current total number of open + opening connections in the pool."""
        return int(self._pool.size)

    @property
    def idle_size(self) -> int:
        """Current number of idle connections available for checkout."""
        return int(self._pool.idle_size)

    @property
    def borrowed_size(self) -> int:
        """Current number of borrowed connections checked out by callers."""
        return int(self._pool.borrowed_size)

    @property
    def closed(self) -> bool:
        """Return `True` when the pool has been closed."""
        return bool(self._pool.closed)

    @property
    def is_closed(self) -> bool:
        """Return `True` when the pool has been closed."""
        return bool(self._pool.closed)

    def acquire(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> Any:
        """Acquire a `SyncOracleClient` lease from the pool."""
        return self._pool.acquire(
            timeout=timeout,
            connection_tag=connection_tag,
            purity=purity,
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

    def acquire_from_pool(self, **kwargs: Any) -> Any:
        """Acquire a `SyncOracleClient` lease from the pool with DRCP tag and TTC piggyback options."""
        if hasattr(self._pool, "acquire_from_pool"):
            return self._pool.acquire_from_pool(**kwargs)
        return self._pool.acquire(**kwargs)

    def release(
        self,
        client: Any,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None:
        """Release a borrowed `SyncOracleClient` lease back to the pool."""
        self._pool.release(client, connection_tag=connection_tag, discard=discard)

    def release_to_pool(self, client: Any, **kwargs: Any) -> None:
        """Release a borrowed `SyncOracleClient` lease back to the pool with tag or discard flags."""
        if hasattr(self._pool, "release_to_pool"):
            self._pool.release_to_pool(client, **kwargs)
        else:
            self._pool.release(client, **kwargs)

    def evict(self, client: Any) -> None:
        """Permanently evict and close a broken or poisoned `SyncOracleClient`."""
        self._pool.evict(client)

    def stats(self) -> dict[str, int]:
        """Return a snapshot dictionary of pool metrics."""
        return dict(self._pool.stats())

    @contextmanager
    def lease(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> Iterator[SyncOracleDriver]:
        """Check out a pooled connection wrapped in a `SyncOracleDriver` and release on exit."""
        client = self.acquire_from_pool(
            timeout=timeout,
            connection_tag=connection_tag,
            purity=purity,
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )
        drv = SyncOracleDriver(self._config, client=client)
        if any((client_identifier, module, action, ecid)):
            drv._pending_session_attrs = None
        discard = False
        try:
            yield drv
        except BaseException as exc:
            discard = bool(getattr(client, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
            raise
        finally:
            effective_tag = getattr(client, "connection_tag", None) or connection_tag
            self.release_to_pool(
                client,
                connection_tag=effective_tag,
                discard=discard or bool(getattr(client, "closed", False)),
            )

    def close(self) -> None:
        """Close the pool and all idle physical sessions."""
        self._pool.close()

    def __enter__(self) -> Self:
        """Enter the synchronous pool context."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the synchronous pool on context exit."""
        self.close()


class SyncOracleDriver(SyncDatabaseDriver[OracleConfig]):
    """Synchronous Oracle 26ai Thin driver backed by `vertebra._oracle.SyncOracleClient`."""

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        broker: Any = None,
        client: Any = None,
        pool: SyncOraclePool | Any | None = None,
    ) -> None:
        resolved = config or OracleConfig()
        super().__init__(resolved, broker=broker)
        self._pool: SyncOraclePool | Any | None = pool
        if client is not None:
            self._client = client
        else:
            mod = OracleCore.require_native_module()
            self._client = mod.SyncOracleClient(**resolved.to_client_kwargs())
        self._in_transaction: bool = False
        self._session_context: dict[tuple[str, str], str] = {}
        initial_attrs = self.config.session_attrs()
        self._pending_session_attrs: OracleSessionAttrs | None = (
            initial_attrs
            if any((initial_attrs.client_identifier, initial_attrs.module, initial_attrs.action, initial_attrs.ecid))
            else None
        )
        self._last_piggyback_frame: bytes | None = None
        self._pipe_savepoint_seq: int = 0
        self._pipeline_active: bool = False
        self._last_pipeline_outcomes: tuple[Any, ...] = ()
        self._last_out_binds: Any = None
        self._tx_events: list[str] = []

    @property
    def out_binds(self) -> Any:
        """Return the coerced `out_binds` payload from the most recent execution."""
        return self._last_out_binds

    @property
    def last_pipeline_outcomes(self) -> tuple[Any, ...]:
        """Return the `OraclePipelineOutcome` items from the most recent pipeline `results()` flush."""
        return self._last_pipeline_outcomes

    @property
    def transaction_events(self) -> tuple[str, ...]:
        """Return the ordered history of transaction lifecycle commands issued on this driver."""
        return tuple(self._tx_events)

    @property
    def pool(self) -> SyncOraclePool | Any | None:
        """Return the optional `SyncOraclePool` attached to this driver."""
        return self._pool

    @property
    def client(self) -> Any:
        """Return the pinned transaction `SyncOracleClient` or the default underlying client."""
        pinned = self._get_pinned_lease()
        return pinned if pinned is not None else self._client

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit transaction is pinned in the current thread/task."""
        pinned = self._get_pinned_lease()
        if pinned is not None:
            return True
        if self._pool is not None:
            return False
        return bool(getattr(self._client, "in_transaction", self._in_transaction))

    @property
    def last_piggyback_frame(self) -> bytes | None:
        """Return the most recently piggybacked TTC `0x11` session attribute frame."""
        active = self._get_pinned_lease() or (self._client if self._pool is None else None)
        if active is not None:
            native_frame = getattr(active, "last_piggyback_frame", None)
            if native_frame is not None:
                return bytes(native_frame)
        return self._last_piggyback_frame

    def pipeline(self) -> SyncOraclePipeline:
        """Create a delayed-commit synchronous query pipeline bound to this driver."""
        if self._pipeline_active:
            raise InterfaceError("An Oracle pipeline is already active on this driver")
        return SyncOraclePipeline(self)

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Queue updated session tracing attributes to be piggybacked onto the next SQL round-trip."""
        prev = self._pending_session_attrs
        merged = OracleSessionAttrs(
            client_identifier=client_identifier
            if client_identifier is not None
            else (prev.client_identifier if prev else None),
            module=module if module is not None else (prev.module if prev else None),
            action=action if action is not None else (prev.action if prev else None),
            ecid=ecid if ecid is not None else (prev.ecid if prev else None),
        )
        self._pending_session_attrs = merged
        active = self._get_pinned_lease() or (self._client if self._pool is None else None)
        if active is not None and hasattr(active, "set_session_attrs"):
            active.set_session_attrs(
                client_identifier=client_identifier,
                module=module,
                action=action,
                ecid=ecid,
            )

    def connect(self) -> Self:
        """Establish or validate the underlying Oracle session."""
        if self._pool is None:
            self._client.connect()
        self._closed = False
        return self

    def ping(self) -> None:
        """Verify liveness of the underlying Oracle session."""
        with self._checkout_client() as active_client:
            active_client.ping()

    def begin(self) -> None:
        """Begin an explicit Oracle transaction and pin the connection lease in a guarded `ContextVar`."""
        pinned = self._get_pinned_lease()
        if pinned is not None:
            pinned.begin()
            self._in_transaction = True
            self._tx_events.append("BEGIN")
            return

        if self._pool is not None:
            lease = self._acquire_pool_client()
            try:
                self._apply_pending_session_attrs(lease)
                lease.begin()
            except BaseException as exc:
                self._release_pool_client(
                    lease,
                    discard=bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc),
                )
                raise
            self._set_pinned_lease(lease, from_pool=True)
            self._in_transaction = True
            self._tx_events.append("BEGIN")
            return

        self._apply_pending_session_attrs(self._client)
        self._client.begin()
        self._set_pinned_lease(self._client, from_pool=False)
        self._in_transaction = True
        self._tx_events.append("BEGIN")

    def commit(self) -> None:
        """Commit the active Oracle transaction and release any pinned pool lease."""
        entry = self._pop_pinned_entry()
        self._in_transaction = False
        self._tx_events.append("COMMIT")
        if entry is not None:
            _, _, active_client, from_pool = entry
            discard = False
            try:
                active_client.commit()
            except BaseException as exc:
                discard = bool(getattr(active_client, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
                raise
            finally:
                self._capture_piggyback(active_client)
                if from_pool and self._pool is not None:
                    self._release_pool_client(active_client, discard=discard)
            return

        if self._pool is None:
            try:
                self._client.commit()
            finally:
                self._capture_piggyback(self._client)

    def rollback(self) -> None:
        """Roll back the active Oracle transaction and release any pinned pool lease."""
        entry = self._pop_pinned_entry()
        self._in_transaction = False
        self._tx_events.append("ROLLBACK")
        if entry is not None:
            _, _, active_client, from_pool = entry
            discard = False
            try:
                active_client.rollback()
            except BaseException:
                discard = True
                raise
            finally:
                self._capture_piggyback(active_client)
                if from_pool and self._pool is not None:
                    self._release_pool_client(active_client, discard=discard)
            return

        if self._pool is None:
            try:
                self._client.rollback()
            finally:
                self._capture_piggyback(self._client)

    def savepoint(self, name: str = "sp") -> None:
        """Create a transaction savepoint with identifier validation."""
        validate_identifier(name, "savepoint")
        active_client = self._get_pinned_lease() or self._client
        active_client.savepoint(name)
        self._in_transaction = True
        self._tx_events.append(f"SAVEPOINT {name}")

    def release_savepoint(self, name: str = "sp") -> None:
        """Validate savepoint identifier (`RELEASE SAVEPOINT` is a validated no-op in Oracle SQL)."""
        validate_identifier(name, "savepoint")

    def rollback_to_savepoint(self, name: str = "sp") -> None:
        """Roll back to a previously established savepoint with identifier validation."""
        validate_identifier(name, "savepoint")
        active_client = self._get_pinned_lease() or self._client
        active_client.rollback_to_savepoint(name)
        self._in_transaction = True
        self._tx_events.append(f"ROLLBACK TO SAVEPOINT {name}")

    def cancel(self) -> None:
        """Trigger an out-of-band `break_execution()` on any in-flight command."""
        if not self._closed:
            active_client = self._get_pinned_lease() or self._client
            active_client.cancel()

    def close(self) -> None:
        """Close the underlying native Oracle client and release any pinned lease."""
        self._closed = True
        self._in_transaction = False
        entry = self._pop_pinned_entry()
        if entry is not None:
            _, _, active_client, from_pool = entry
            if from_pool and self._pool is not None:
                with suppress(Exception):
                    self._release_pool_client(active_client, discard=False)
        self._client.close()

    def enter_tenant_scope(
        self,
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> str:
        """Enter a session-level VPD tenant scope using trusted package or `SET_IDENTIFIER`."""
        namespace = self.config.context_namespace
        sql, params = OracleCore.build_session_context_sql(
            str(tenant_value),
            config,
            namespace=namespace,
            context_package_proc=self.config.context_package_proc,
        )
        self.execute(sql, params)
        attribute = config.setting_name.rsplit(".", 1)[-1]
        self._session_context[(namespace, attribute)] = str(tenant_value)
        return str(tenant_value)

    def exit_tenant_scope(
        self,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> None:
        """Reset session-level VPD tenant context using trusted clear procedure or `CLEAR_IDENTIFIER`."""
        namespace = self.config.context_namespace
        sql, params = OracleCore.build_clear_context_sql(
            config,
            namespace=namespace,
            context_clear_proc=self.config.context_clear_proc,
        )
        try:
            if params:
                self.execute(sql, params)
            else:
                self.execute(sql)
        except BaseException:
            self.evict_connection()
            raise
        attribute = config.setting_name.rsplit(".", 1)[-1]
        self._session_context.pop((namespace, attribute), None)

    def evict_connection(self) -> None:
        """Permanently evict or close the active physical session so tainted tenant state is never reused."""
        self._session_context.clear()
        self._in_transaction = False
        entry = self._pop_pinned_entry()
        if entry is not None:
            _, _, active_client, from_pool = entry
            if from_pool and self._pool is not None:
                with suppress(Exception):
                    self._release_pool_client(active_client, discard=True)
                return
        self._closed = True
        with suppress(Exception):
            self._client.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML, DDL, or PL/SQL statement and return the affected row count."""
        in_params, out_binds, out_vars = OracleCore.split_out_bind_parameters(parameters, kwargs.get("out_binds"))
        coerced = OracleCore.coerce_parameters(in_params, features=self.config.driver_features)
        param_count = len(coerced) if coerced is not None else 0
        self._lookup_or_cache_statement(sql, param_count=param_count, param_plan=b"\x00\x01")
        grant = self._acquire_broker_lease()
        try:
            with self._checkout_client() as active_client:
                OracleCore.track_session_context_sql(self._session_context, sql, in_params)
                result = active_client.execute(
                    sql,
                    coerced,
                    out_binds=out_binds,
                    fetch_decimals=self.config.driver_features.fetch_decimals,
                    enable_lowercase_column_names=self.config.driver_features.enable_lowercase_column_names,
                )
                self._last_out_binds = OracleCore.populate_out_bind_vars(
                    out_vars,
                    result,
                    features=self.config.driver_features,
                )
                if isinstance(result, int):
                    return max(result, 0)
                return max(int(result.rowcount), 0)
        finally:
            self._release_broker_lease(grant)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a DML statement across a batch of parameter sets and return total affected rows."""
        _ = kwargs
        if not parameter_sets:
            return 0
        coerced_seq = [
            OracleCore.coerce_parameters(p, features=self.config.driver_features) or () for p in parameter_sets
        ]
        first_len = len(coerced_seq[0]) if coerced_seq else 0
        self._lookup_or_cache_statement(sql, param_count=first_len, param_plan=b"\x00\x01")
        grant = self._acquire_broker_lease()
        try:
            with self._checkout_client() as active_client:
                return int(active_client.execute_many(sql, coerced_seq))
        finally:
            self._release_broker_lease(grant)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query or `DML ... RETURNING ... INTO` statement and return positional tuples."""
        rows, _ = self.execute_with_description(sql, parameters, **kwargs)
        return rows

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(rows, column_names)` with configured column-casing and cell coercion."""
        in_params, out_binds, out_vars = OracleCore.split_out_bind_parameters(parameters, kwargs.get("out_binds"))
        coerced = OracleCore.coerce_parameters(in_params, features=self.config.driver_features)
        param_count = len(coerced) if coerced is not None else 0
        self._lookup_or_cache_statement(sql, param_count=param_count, param_plan=b"\x00\x01")
        grant = self._acquire_broker_lease()
        try:
            with self._checkout_client() as active_client:
                OracleCore.track_session_context_sql(self._session_context, sql, in_params)
                lowercase = self.config.driver_features.enable_lowercase_column_names
                fetch_decimals = self.config.driver_features.fetch_decimals
                if OracleCore.is_dml_or_plsql(sql):
                    result = active_client.execute(
                        sql,
                        coerced,
                        out_binds=out_binds,
                        fetch_decimals=fetch_decimals,
                        enable_lowercase_column_names=lowercase,
                    )
                else:
                    result = active_client.query(
                        sql,
                        coerced,
                        out_binds=out_binds,
                        fetch_decimals=fetch_decimals,
                        enable_lowercase_column_names=lowercase,
                    )
                self._last_out_binds = OracleCore.populate_out_bind_vars(
                    out_vars,
                    result,
                    features=self.config.driver_features,
                )
                return OracleCore.coerce_result_rows_and_cols(
                    result,
                    features=self.config.driver_features,
                    out_hints=out_binds,
                )
        finally:
            self._release_broker_lease(grant)

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` or `DML ... RETURNING` query and hydrate dictionaries or `schema_type` instances."""
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        if not rows:
            return []
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int | None = None,
        force_shm: bool | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query and materialize the result set as a zero-copy `ArrowCursor`."""
        if hasattr(self._client, "query_arrow") and (
            force_shm is not None or threshold_rows == ARROW_SHM_THRESHOLD_ROWS
        ):
            coerced = OracleCore.coerce_parameters(parameters, features=self.config.driver_features)
            param_count = len(coerced) if coerced is not None else 0
            self._lookup_or_cache_statement(sql, param_count=param_count, param_plan=b"\x00\x01")
            grant = self._acquire_broker_lease()
            try:
                with self._checkout_client() as active_client:
                    OracleCore.track_session_context_sql(self._session_context, sql, parameters)
                    effective_batch = int(batch_size) if batch_size is not None and batch_size > 0 else 8192
                    cursor: ArrowCursor = active_client.query_arrow(
                        sql,
                        coerced,
                        batch_size=effective_batch,
                        force_shm=force_shm,
                        enable_lowercase_column_names=self.config.driver_features.enable_lowercase_column_names,
                    )
                    return cursor
            finally:
                self._release_broker_lease(grant)
        rows, raw_cols = self.execute_with_description(sql, parameters, **kwargs)
        cols = [col.lower() if self.config.driver_features.enable_lowercase_column_names else col for col in raw_cols]
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        **kwargs: Any,
    ) -> Iterator[ArrowCursor]:
        """Stream query results as a sequence of `ArrowCursor` batches of at most `batch_size` rows."""
        chunk = max(1, int(batch_size))
        rows, cols = self.execute_with_description(sql, parameters, **kwargs)
        for idx in range(0, len(rows), chunk):
            yield ArrowCursor.from_rows(rows[idx : idx + chunk], cols)

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an Arrow Table, `ArrowCursor`, or row sequence into `table_name`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column_name", allow_dots=False)
        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column_name", allow_dots=False)
        if not rows or not cols:
            return 0
        use_direct_path = (
            self.config.driver_features.enable_direct_path_load if direct_path is None else bool(direct_path)
        )
        hint = "/*+ APPEND_VALUES */ " if use_direct_path else ""
        placeholders = ", ".join(f":{i + 1}" for i in range(len(cols)))
        col_list = ", ".join(str(col) for col in cols)
        sql = f"INSERT {hint}INTO {table_name} ({col_list}) VALUES ({placeholders})"
        return self.execute_many(sql, rows)

    def publish_notification(
        self,
        channel: str,
        payload: bytes | str,
        *,
        queue_prefix: str = "VTB_AQ_",
        consumer_name: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Enqueue an AQ / TxEventQ notification synchronously and return its message ID."""
        queue_name = validate_oracle_channel_name(channel, queue_prefix=queue_prefix)
        raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        with self._checkout_client() as active_client:
            msg_id_bytes: bytes = active_client.enqueue_notification(
                queue_name=queue_name,
                payload=raw_bytes,
                consumer_name=consumer_name,
                correlation_id=correlation_id,
            )
            return bytes(msg_id_bytes).decode("ascii", errors="replace")

    def notification_source(
        self,
        config: OracleNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
    ) -> SyncOracleNotificationSource:
        """Construct a SyncOracleNotificationSource bound to this synchronous driver."""
        return SyncOracleNotificationSource(self, config, change_feed=change_feed)

    def listen_notifications(
        self,
        channels: Sequence[str],
        *,
        config: OracleNotifyConfig | None = None,
    ) -> Iterator[NotificationEvent]:
        """Subscribe to channels and yield NotificationEvent frames with automatic cleanup."""
        source = SyncOracleNotificationSource(self, config)
        source.subscribe(channels)
        yield from source.listen()

    def _get_pinned_lease(self) -> Any | None:
        mapping = _SYNC_PINNED_TX.get()
        if not mapping:
            return None
        entry = mapping.get(id(self))
        if entry is None:
            return None
        stored_thread, stored_task, lease, _ = entry
        curr_thread, curr_task = OracleCore.current_execution_identity()
        if stored_thread != curr_thread or stored_task != curr_task:
            return None
        return lease

    def _set_pinned_lease(self, lease: Any, *, from_pool: bool) -> None:
        curr_thread, curr_task = OracleCore.current_execution_identity()
        existing = _SYNC_PINNED_TX.get()
        updated = dict(existing) if existing is not None else {}
        updated[id(self)] = (curr_thread, curr_task, lease, from_pool)
        _SYNC_PINNED_TX.set(updated)

    def _pop_pinned_entry(self) -> tuple[int, int | None, Any, bool] | None:
        mapping = _SYNC_PINNED_TX.get()
        if not mapping:
            return None
        entry = mapping.get(id(self))
        if entry is None:
            return None
        stored_thread, stored_task, _, _ = entry
        curr_thread, curr_task = OracleCore.current_execution_identity()
        if stored_thread != curr_thread or stored_task != curr_task:
            return None
        updated = dict(mapping)
        updated.pop(id(self), None)
        _SYNC_PINNED_TX.set(updated or None)
        return entry

    def _acquire_pool_client(self) -> Any:
        if self._pool is None:
            raise InterfaceError("Oracle connection pool is not configured")
        attrs = self._pending_session_attrs
        self._pending_session_attrs = None
        acquire_kwargs = OracleCore.build_session_attr_acquire_kwargs(attrs)
        if hasattr(self._pool, "acquire_from_pool"):
            return self._pool.acquire_from_pool(**acquire_kwargs)
        return self._pool.acquire(**acquire_kwargs)

    def _release_pool_client(self, lease: Any, *, discard: bool = False) -> None:
        if self._pool is None:
            return
        if hasattr(self._pool, "release_to_pool"):
            self._pool.release_to_pool(lease, discard=discard)
        else:
            self._pool.release(lease, discard=discard)

    @contextmanager
    def _checkout_client(self) -> Iterator[Any]:
        pinned = self._get_pinned_lease()
        if pinned is not None:
            self._apply_pending_session_attrs(pinned)
            try:
                yield pinned
            finally:
                self._capture_piggyback(pinned)
            return

        if self._pool is None:
            self._apply_pending_session_attrs(self._client)
            try:
                yield self._client
            finally:
                self._capture_piggyback(self._client)
            return

        lease = self._acquire_pool_client()
        discard = False
        try:
            yield lease
        except BaseException as exc:
            discard = bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
            raise
        finally:
            self._capture_piggyback(lease)
            self._release_pool_client(lease, discard=discard)

    def _apply_pending_session_attrs(self, client: Any) -> None:
        if self._pending_session_attrs is None:
            return
        attrs = self._pending_session_attrs
        self._pending_session_attrs = None
        if any((attrs.client_identifier, attrs.module, attrs.action, attrs.ecid)):
            if hasattr(client, "set_session_attrs"):
                client.set_session_attrs(
                    client_identifier=attrs.client_identifier,
                    module=attrs.module,
                    action=attrs.action,
                    ecid=attrs.ecid,
                )
            else:
                self._last_piggyback_frame = attrs.encode_ttc_piggyback()

    def _capture_piggyback(self, client: Any) -> None:
        frame = getattr(client, "last_piggyback_frame", None)
        if frame is not None:
            self._last_piggyback_frame = bytes(frame)
