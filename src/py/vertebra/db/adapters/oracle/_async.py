"""Asynchronous native Oracle 26ai Thin driver and connection pool."""

from __future__ import annotations

import asyncio
import os
import threading
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, ClassVar, Self

from vertebra.db.adapters.oracle.config import OracleConfig, OracleSessionAttrs
from vertebra.db.adapters.oracle.core import ORACLE_DIALECT_TAG, OracleCore
from vertebra.db.adapters.oracle.notify import (
    OracleNotificationSource,
    OracleNotifyConfig,
    validate_oracle_channel_name,
)
from vertebra.db.adapters.oracle.pipeline import AsyncOraclePipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _hydrate_rows_with_schema,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import AsyncDatabaseDriver, InterfaceError, NotificationEvent
from vertebra.db.rls import TenantScopeConfig
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra.db.cdc import ChangeFeed

_DEFAULT_TENANT_SCOPE = TenantScopeConfig()

__all__ = (
    "AsyncOracleDriver",
    "AsyncOraclePool",
)

_ASYNC_PINNED_TX: ContextVar[dict[int, tuple[int, int | None, Any, bool]] | None] = ContextVar(
    "_ASYNC_PINNED_TX",
    default=None,
)


def _current_execution_identity() -> tuple[int, int | None]:
    """Return `(thread_ident, current_asyncio_task_id)` for ContextVar transaction isolation."""
    thread_id = threading.get_ident()
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return (thread_id, id(task) if task is not None else None)


class AsyncOraclePool:
    """Asynchronous physical Oracle connection pool backed by `vertebra._oracle.AsyncOraclePool`."""

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
            self._pool = mod.AsyncOraclePool(**pool_kwargs)

    @property
    def config(self) -> OracleConfig:
        """Return the `OracleConfig` governing this connection pool."""
        return self._config

    @property
    def native_pool(self) -> Any:
        """Return the underlying `vertebra._oracle.AsyncOraclePool` instance."""
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

    async def acquire(
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
        """Acquire an `AsyncOracleClient` lease from the pool asynchronously."""
        return await self._pool.acquire(
            timeout=timeout,
            connection_tag=connection_tag,
            purity=purity,
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )

    async def acquire_from_pool(self, **kwargs: Any) -> Any:
        """Acquire an `AsyncOracleClient` lease from the pool with DRCP tag and TTC piggyback options."""
        if hasattr(self._pool, "acquire_from_pool"):
            return await self._pool.acquire_from_pool(**kwargs)
        return await self._pool.acquire(**kwargs)

    async def release(
        self,
        client: Any,
        *,
        connection_tag: str | None = None,
        discard: bool = False,
    ) -> None:
        """Release a borrowed `AsyncOracleClient` lease back to the pool asynchronously."""
        await self._pool.release(client, connection_tag=connection_tag, discard=discard)

    async def release_to_pool(self, client: Any, **kwargs: Any) -> None:
        """Release a borrowed `AsyncOracleClient` lease back to the pool with tag or discard flags."""
        if hasattr(self._pool, "release_to_pool"):
            await self._pool.release_to_pool(client, **kwargs)
        else:
            await self._pool.release(client, **kwargs)

    async def evict(self, client: Any) -> None:
        """Permanently evict and close a broken or poisoned `AsyncOracleClient`."""
        await self._pool.evict(client)

    def stats(self) -> dict[str, int]:
        """Return a snapshot dictionary of pool metrics."""
        return dict(self._pool.stats())

    @asynccontextmanager
    async def lease(
        self,
        *,
        timeout: float | None = None,
        connection_tag: str | None = None,
        purity: str | None = None,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> AsyncIterator[AsyncOracleDriver]:
        """Check out a pooled connection wrapped in an `AsyncOracleDriver` and release on exit."""
        client = await self.acquire_from_pool(
            timeout=timeout,
            connection_tag=connection_tag,
            purity=purity,
            client_identifier=client_identifier,
            module=module,
            action=action,
            ecid=ecid,
        )
        drv = AsyncOracleDriver(self._config, client=client)
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
            await self.release_to_pool(
                client,
                connection_tag=effective_tag,
                discard=discard or bool(getattr(client, "closed", False)),
            )

    async def close(self) -> None:
        """Close the pool and all idle physical sessions asynchronously."""
        await self._pool.close()

    async def __aenter__(self) -> Self:
        """Enter the asynchronous pool context."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the asynchronous pool on context exit."""
        await self.close()


class AsyncOracleDriver(AsyncDatabaseDriver[OracleConfig]):
    """Asynchronous Oracle 26ai Thin driver backed by `vertebra._oracle.AsyncOracleClient`."""

    dialect: ClassVar[str] = "oracle"
    dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG

    def __init__(
        self,
        config: OracleConfig | None = None,
        *,
        broker: Any = None,
        client: Any = None,
        pool: AsyncOraclePool | Any | None = None,
    ) -> None:
        resolved = config or OracleConfig()
        super().__init__(resolved, broker=broker)
        self._pool: AsyncOraclePool | Any | None = pool
        if client is not None:
            self._client = client
        else:
            mod = OracleCore.require_native_module()
            self._client = mod.AsyncOracleClient(**resolved.to_client_kwargs())
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
        self._tx_events: list[str] = []

    @property
    def last_pipeline_outcomes(self) -> tuple[Any, ...]:
        """Return the `OraclePipelineOutcome` items from the most recent pipeline `results()` flush."""
        return self._last_pipeline_outcomes

    @property
    def transaction_events(self) -> tuple[str, ...]:
        """Return the ordered history of transaction lifecycle commands issued on this driver."""
        return tuple(self._tx_events)

    def pipeline(self) -> AsyncOraclePipeline:
        """Create a delayed-commit asynchronous query pipeline bound to this driver."""
        if self._pipeline_active:
            raise InterfaceError("An Oracle pipeline is already active on this driver")
        return AsyncOraclePipeline(self)

    @property
    def pool(self) -> AsyncOraclePool | Any | None:
        """Return the optional `AsyncOraclePool` attached to this driver."""
        return self._pool

    @property
    def client(self) -> Any:
        """Return the pinned transaction `AsyncOracleClient` or the default underlying client."""
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

    def set_session_attrs(
        self,
        *,
        client_identifier: str | None = None,
        module: str | None = None,
        action: str | None = None,
        ecid: str | None = None,
    ) -> None:
        """Queue updated session tracing attributes for the next SQL round-trip."""
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

    async def connect(self) -> Self:
        """Establish or validate the underlying asynchronous Oracle session."""
        if self._pool is None:
            await self._client.connect()
        self._closed = False
        return self

    async def ping(self) -> None:
        """Verify liveness of the underlying asynchronous Oracle session."""
        async with self._checkout_client() as active_client:
            await active_client.ping()

    async def begin(self) -> None:
        """Begin an explicit asynchronous Oracle transaction and pin the connection lease in a guarded `ContextVar`."""
        pinned = self._get_pinned_lease()
        if pinned is not None:
            await pinned.begin()
            self._in_transaction = True
            self._tx_events.append("BEGIN")
            return

        if self._pool is not None:
            lease = await self._acquire_pool_client()
            try:
                self._apply_pending_session_attrs(lease)
                await lease.begin()
            except BaseException as exc:
                await self._release_pool_client(
                    lease,
                    discard=bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc),
                )
                raise
            self._set_pinned_lease(lease, from_pool=True)
            self._in_transaction = True
            self._tx_events.append("BEGIN")
            return

        self._apply_pending_session_attrs(self._client)
        await self._client.begin()
        self._set_pinned_lease(self._client, from_pool=False)
        self._in_transaction = True
        self._tx_events.append("BEGIN")

    async def commit(self) -> None:
        """Commit the active asynchronous Oracle transaction and release any pinned pool lease."""
        entry = self._pop_pinned_entry()
        self._in_transaction = False
        self._tx_events.append("COMMIT")
        if entry is not None:
            _, _, active_client, from_pool = entry
            discard = False
            try:
                await active_client.commit()
            except BaseException as exc:
                discard = bool(getattr(active_client, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
                raise
            finally:
                self._capture_piggyback(active_client)
                if from_pool and self._pool is not None:
                    await self._release_pool_client(active_client, discard=discard)
            return

        if self._pool is None:
            try:
                await self._client.commit()
            finally:
                self._capture_piggyback(self._client)

    async def rollback(self) -> None:
        """Roll back the active asynchronous Oracle transaction and release any pinned pool lease."""
        entry = self._pop_pinned_entry()
        self._in_transaction = False
        self._tx_events.append("ROLLBACK")
        if entry is not None:
            _, _, active_client, from_pool = entry
            discard = False
            try:
                await active_client.rollback()
            except BaseException:
                discard = True
                raise
            finally:
                self._capture_piggyback(active_client)
                if from_pool and self._pool is not None:
                    await self._release_pool_client(active_client, discard=discard)
            return

        if self._pool is None:
            try:
                await self._client.rollback()
            finally:
                self._capture_piggyback(self._client)

    async def savepoint(self, name: str = "sp") -> None:
        """Create a transaction savepoint asynchronously with identifier validation."""
        validate_identifier(name, "savepoint")
        active_client = self._get_pinned_lease() or self._client
        await active_client.savepoint(name)
        self._in_transaction = True
        self._tx_events.append(f"SAVEPOINT {name}")

    async def release_savepoint(self, name: str = "sp") -> None:
        """Validate savepoint identifier asynchronously (`RELEASE SAVEPOINT` is a validated no-op in Oracle SQL)."""
        validate_identifier(name, "savepoint")

    async def rollback_to_savepoint(self, name: str = "sp") -> None:
        """Roll back to a previously established savepoint asynchronously with identifier validation."""
        validate_identifier(name, "savepoint")
        active_client = self._get_pinned_lease() or self._client
        await active_client.rollback_to_savepoint(name)
        self._in_transaction = True
        self._tx_events.append(f"ROLLBACK TO SAVEPOINT {name}")

    async def cancel(self) -> None:
        """Trigger an out-of-band `break_execution()` on any in-flight command."""
        if not self._closed:
            active_client = self._get_pinned_lease() or self._client
            active_client.cancel()

    async def close(self) -> None:
        """Close the underlying native asynchronous Oracle client and release any pinned lease."""
        self._closed = True
        self._in_transaction = False
        entry = self._pop_pinned_entry()
        if entry is not None:
            _, _, active_client, from_pool = entry
            if from_pool and self._pool is not None:
                with suppress(Exception):
                    await self._release_pool_client(active_client, discard=False)
        await self._client.close()

    async def enter_tenant_scope(
        self,
        tenant_value: str,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> str:
        """Enter a session-level VPD tenant scope asynchronously using trusted package or `SET_IDENTIFIER`."""
        namespace = self.config.context_namespace
        sql, params = OracleCore.build_session_context_sql(
            str(tenant_value),
            config,
            namespace=namespace,
            context_package_proc=self.config.context_package_proc,
        )
        await self.execute(sql, params)
        attribute = config.setting_name.rsplit(".", 1)[-1]
        self._session_context[(namespace, attribute)] = str(tenant_value)
        return str(tenant_value)

    async def exit_tenant_scope(
        self,
        config: TenantScopeConfig = _DEFAULT_TENANT_SCOPE,
    ) -> None:
        """Reset session-level VPD tenant context asynchronously using trusted clear procedure or `CLEAR_IDENTIFIER`."""
        namespace = self.config.context_namespace
        sql, params = OracleCore.build_clear_context_sql(
            config,
            namespace=namespace,
            context_clear_proc=self.config.context_clear_proc,
        )
        try:
            if params:
                await self.execute(sql, params)
            else:
                await self.execute(sql)
        except BaseException:
            await self.evict_connection()
            raise
        attribute = config.setting_name.rsplit(".", 1)[-1]
        self._session_context.pop((namespace, attribute), None)

    async def evict_connection(self) -> None:
        """Permanently evict or close the active asynchronous physical session so tainted tenant state is never reused."""
        self._session_context.clear()
        self._in_transaction = False
        entry = self._pop_pinned_entry()
        if entry is not None:
            _, _, active_client, from_pool = entry
            if from_pool and self._pool is not None:
                with suppress(Exception):
                    await self._release_pool_client(active_client, discard=True)
                return
        self._closed = True
        with suppress(Exception):
            await self._client.close()

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML, DDL, or PL/SQL statement asynchronously and return affected row count."""
        out_binds = kwargs.get("out_binds")
        coerced = self._coerce_parameters(parameters)
        param_count = len(coerced) if coerced is not None else 0
        grant = self._acquire_oracle_broker_lease(sql, param_count)
        try:
            async with self._checkout_client() as active_client:
                self._track_session_context_sql(sql, parameters)
                result = await active_client.execute(
                    sql,
                    coerced,
                    out_binds=out_binds,
                    fetch_decimals=self.config.driver_features.fetch_decimals,
                    enable_lowercase_column_names=self.config.driver_features.enable_lowercase_column_names,
                )
                if isinstance(result, int):
                    return max(result, 0)
                return max(int(result.rowcount), 0)
        finally:
            self._release_oracle_broker_lease(grant)

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a DML statement across a batch of parameter sets asynchronously."""
        _ = kwargs
        if not parameter_sets:
            return 0
        coerced_seq = [self._coerce_parameters(p) or () for p in parameter_sets]
        first_len = len(coerced_seq[0]) if coerced_seq else 0
        grant = self._acquire_oracle_broker_lease(sql, first_len)
        try:
            async with self._checkout_client() as active_client:
                return int(await active_client.execute_many(sql, coerced_seq))
        finally:
            self._release_oracle_broker_lease(grant)

    async def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query or `DML ... RETURNING ... INTO` statement and return positional tuples."""
        rows, _ = await self.execute_with_description(sql, parameters, **kwargs)
        return rows

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL asynchronously and return `(rows, column_names)`."""
        out_binds = kwargs.get("out_binds")
        coerced = self._coerce_parameters(parameters)
        param_count = len(coerced) if coerced is not None else 0
        grant = self._acquire_oracle_broker_lease(sql, param_count)
        try:
            async with self._checkout_client() as active_client:
                self._track_session_context_sql(sql, parameters)
                lowercase = self.config.driver_features.enable_lowercase_column_names
                fetch_decimals = self.config.driver_features.fetch_decimals
                if OracleCore.is_dml_or_plsql(sql):
                    result = await active_client.execute(
                        sql,
                        coerced,
                        out_binds=out_binds,
                        fetch_decimals=fetch_decimals,
                        enable_lowercase_column_names=lowercase,
                    )
                else:
                    result = await active_client.query(
                        sql,
                        coerced,
                        out_binds=out_binds,
                        fetch_decimals=fetch_decimals,
                        enable_lowercase_column_names=lowercase,
                    )
                return self._coerce_result_rows_and_cols(result)
        finally:
            self._release_oracle_broker_lease(grant)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` or `DML ... RETURNING` query asynchronously and hydrate rows."""
        rows, cols = await self.execute_with_description(sql, parameters, **kwargs)
        if not rows:
            return []
        return _hydrate_rows_with_schema(rows, cols, schema_type)

    async def exec_returning_async(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `DML ... RETURNING ... INTO` statement in one round-trip and return hydrated rows."""
        return await self.select(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int | None = None,
        force_shm: bool | None = None,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a query asynchronously and return a zero-copy `ArrowCursor`."""
        if hasattr(self._client, "query_arrow") and (
            force_shm is not None or threshold_rows == ARROW_SHM_THRESHOLD_ROWS
        ):
            coerced = self._coerce_parameters(parameters)
            param_count = len(coerced) if coerced is not None else 0
            grant = self._acquire_oracle_broker_lease(sql, param_count)
            try:
                async with self._checkout_client() as active_client:
                    self._track_session_context_sql(sql, parameters)
                    effective_batch = int(batch_size) if batch_size is not None and batch_size > 0 else 8192
                    cursor: ArrowCursor = await active_client.query_arrow(
                        sql,
                        coerced,
                        batch_size=effective_batch,
                        force_shm=force_shm,
                        enable_lowercase_column_names=self.config.driver_features.enable_lowercase_column_names,
                    )
                    return cursor
            finally:
                self._release_oracle_broker_lease(grant)
        rows, raw_cols = await self.execute_with_description(sql, parameters, **kwargs)
        cols = [col.lower() if self.config.driver_features.enable_lowercase_column_names else col for col in raw_cols]
        return ArrowCursor.from_rows(rows, cols, threshold_rows=threshold_rows)

    async def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 8192,
        **kwargs: Any,
    ) -> AsyncIterator[ArrowCursor]:
        """Asynchronously stream query results as a sequence of `ArrowCursor` batches."""
        chunk = max(1, int(batch_size))
        rows, cols = await self.execute_with_description(sql, parameters, **kwargs)
        for idx in range(0, len(rows), chunk):
            yield ArrowCursor.from_rows(rows[idx : idx + chunk], cols)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        direct_path: bool | None = None,
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-ingest an Arrow Table or `ArrowCursor` into `table_name`."""
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
        return await self.execute_many(sql, rows)

    def notification_source(
        self,
        config: OracleNotifyConfig | None = None,
        *,
        change_feed: ChangeFeed | None = None,
    ) -> OracleNotificationSource:
        """Construct an `OracleNotificationSource` bound to this asynchronous driver."""
        return OracleNotificationSource(self, config, change_feed=change_feed)

    async def publish_notification(
        self,
        channel: str,
        payload: bytes | str,
        *,
        queue_prefix: str = "VTB_AQ_",
        consumer_name: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Enqueue an AQ / TxEventQ notification asynchronously and return its message ID."""
        queue_name = validate_oracle_channel_name(channel, queue_prefix=queue_prefix)
        raw_bytes = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        async with self._checkout_client() as active_client:
            msg_id_bytes: bytes = await active_client.enqueue_notification(
                queue_name=queue_name,
                payload=raw_bytes,
                consumer_name=consumer_name,
                correlation_id=correlation_id,
            )
            return bytes(msg_id_bytes).decode("ascii", errors="replace")

    async def listen_notifications(
        self,
        channels: Sequence[str],
        *,
        config: OracleNotifyConfig | None = None,
    ) -> AsyncIterator[NotificationEvent]:
        """Subscribe to `channels` and yield `NotificationEvent` frames with automatic cleanup."""
        source = OracleNotificationSource(self, config)
        await source.subscribe(channels)
        try:
            async for event in source.listen():
                yield event
        finally:
            await source.close()

    def _get_pinned_lease(self) -> Any | None:
        mapping = _ASYNC_PINNED_TX.get()
        if not mapping:
            return None
        entry = mapping.get(id(self))
        if entry is None:
            return None
        stored_thread, stored_task, lease, _ = entry
        curr_thread, curr_task = _current_execution_identity()
        if stored_thread != curr_thread or stored_task != curr_task:
            return None
        return lease

    def _set_pinned_lease(self, lease: Any, *, from_pool: bool) -> None:
        curr_thread, curr_task = _current_execution_identity()
        existing = _ASYNC_PINNED_TX.get()
        updated = dict(existing) if existing is not None else {}
        updated[id(self)] = (curr_thread, curr_task, lease, from_pool)
        _ASYNC_PINNED_TX.set(updated)

    def _pop_pinned_entry(self) -> tuple[int, int | None, Any, bool] | None:
        mapping = _ASYNC_PINNED_TX.get()
        if not mapping:
            return None
        entry = mapping.get(id(self))
        if entry is None:
            return None
        stored_thread, stored_task, _, _ = entry
        curr_thread, curr_task = _current_execution_identity()
        if stored_thread != curr_thread or stored_task != curr_task:
            return None
        updated = dict(mapping)
        updated.pop(id(self), None)
        _ASYNC_PINNED_TX.set(updated or None)
        return entry

    async def _acquire_pool_client(self) -> Any:
        assert self._pool is not None
        attrs = self._pending_session_attrs
        self._pending_session_attrs = None
        acquire_kwargs: dict[str, Any] = {}
        if attrs is not None:
            if attrs.client_identifier is not None:
                acquire_kwargs["client_identifier"] = attrs.client_identifier
            if attrs.module is not None:
                acquire_kwargs["module"] = attrs.module
            if attrs.action is not None:
                acquire_kwargs["action"] = attrs.action
            if attrs.ecid is not None:
                acquire_kwargs["ecid"] = attrs.ecid
        if hasattr(self._pool, "acquire_from_pool"):
            return await self._pool.acquire_from_pool(**acquire_kwargs)
        return await self._pool.acquire(**acquire_kwargs)

    async def _release_pool_client(self, lease: Any, *, discard: bool = False) -> None:
        if self._pool is None:
            return
        if hasattr(self._pool, "release_to_pool"):
            await self._pool.release_to_pool(lease, discard=discard)
        else:
            await self._pool.release(lease, discard=discard)

    @asynccontextmanager
    async def _checkout_client(self) -> AsyncIterator[Any]:
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

        lease = await self._acquire_pool_client()
        discard = False
        try:
            yield lease
        except BaseException as exc:
            discard = bool(getattr(lease, "closed", False)) or OracleCore.is_fatal_transport_error(exc)
            raise
        finally:
            self._capture_piggyback(lease)
            await self._release_pool_client(lease, discard=discard)

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

    def _coerce_parameters(
        self,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> list[Any] | dict[str, Any] | None:
        if parameters is None:
            return None
        features = self.config.driver_features
        if isinstance(parameters, Mapping):
            return {
                str(k).lstrip(":"): OracleCore.coerce_bind_parameter(v, features=features)
                for k, v in parameters.items()
            }
        if isinstance(parameters, Sequence) and not isinstance(parameters, (str, bytes, bytearray)):
            return [OracleCore.coerce_bind_parameter(v, features=features) for v in parameters]
        return [OracleCore.coerce_bind_parameter(v, features=features) for v in parameters]

    def _coerce_result_rows_and_cols(
        self,
        result: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        features = self.config.driver_features
        lowercase = features.enable_lowercase_column_names
        raw_columns = getattr(result, "columns", ()) or ()
        cols = tuple(str(col.name).lower() if lowercase else str(col.name) for col in raw_columns)
        col_types = tuple(
            str(getattr(col, "oracle_type", getattr(col, "type_name", "VARCHAR2"))) for col in raw_columns
        )
        raw_rows = getattr(result, "rows", ()) or ()
        if raw_rows:
            coerced_rows = [
                tuple(
                    OracleCore.coerce_row_cell(
                        cell,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, cell in enumerate(row)
                )
                for row in raw_rows
            ]
            return coerced_rows, cols

        out_binds = getattr(result, "out_binds", None)
        if out_binds is not None:
            if isinstance(out_binds, Mapping):
                out_cols = cols or tuple(
                    str(k).lstrip(":").lower() if lowercase else str(k).lstrip(":") for k in out_binds
                )
                row_tuple = tuple(
                    OracleCore.coerce_row_cell(
                        val,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, val in enumerate(out_binds.values())
                )
                return [row_tuple], out_cols
            if isinstance(out_binds, Sequence) and not isinstance(out_binds, (str, bytes, bytearray)):
                out_cols = cols or tuple(f"out_{i + 1}" for i in range(len(out_binds)))
                row_tuple = tuple(
                    OracleCore.coerce_row_cell(
                        val,
                        col_types[idx] if idx < len(col_types) else None,
                        features=features,
                    )
                    for idx, val in enumerate(out_binds)
                )
                return [row_tuple], out_cols

        return [], cols

    def _track_session_context_sql(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> None:
        upper = sql.strip().upper()
        if "SET_IDENTIFIER" in upper and isinstance(parameters, Mapping):
            norm = {str(k).lstrip(":").lower(): v for k, v in parameters.items()}
            val = norm.get("value", norm.get("identifier"))
            if val is not None:
                self._session_context[("USERENV", "CLIENT_IDENTIFIER")] = str(val)
            return
        if "CLEAR_IDENTIFIER" in upper:
            self._session_context.pop(("USERENV", "CLIENT_IDENTIFIER"), None)
            return
        if isinstance(parameters, Mapping):
            norm = {str(k).lstrip(":").lower(): v for k, v in parameters.items()}
            if "namespace" in norm and "attribute" in norm:
                key = (str(norm["namespace"]), str(norm["attribute"]))
                if "value" in norm:
                    self._session_context[key] = str(norm["value"])
                else:
                    self._session_context.pop(key, None)

    def _acquire_oracle_broker_lease(self, sql: str, param_count: int) -> Any:
        if self._broker is None:
            return None
        if hasattr(self._broker, "statement_cache"):
            sc_attr = self._broker.statement_cache
            sc = sc_attr() if callable(sc_attr) else sc_attr
            if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
                sql_bytes = sql.encode("utf-8")
                try:
                    cached = sc.lookup(ORACLE_DIALECT_TAG, sql)
                    if cached is None:
                        sc.insert(
                            ORACLE_DIALECT_TAG,
                            sql,
                            param_count,
                            sql_bytes,
                            b"\x00\x01",
                        )
                except TypeError:
                    cached = sc.lookup(ORACLE_DIALECT_TAG, sql_bytes)
                    if cached is None:
                        sc.insert(
                            ORACLE_DIALECT_TAG,
                            sql_bytes,
                            param_count,
                            sql_bytes,
                            b"\x00\x01",
                        )
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(os.getpid())
            except TypeError:
                return self._broker.acquire(bind_key="oracle")
        return None

    def _release_oracle_broker_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None:
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                self._broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(self._broker, "release") and hasattr(grant, "slot_index"):
                self._broker.release(grant.slot_index, grant.generation)
