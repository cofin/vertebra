"""Synchronous pure-Rust Google Cloud Spanner v1 driver."""

from __future__ import annotations

import contextlib
import time as _time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any, ClassVar, Literal, Protocol, Self

import msgspec

from vertebra._vertebra import (
    LeaseBroker,
    NotFoundError,
    NotSupportedError,
    SpannerAbortedError,
)
from vertebra._vertebra import (
    StatementCache as _RustStatementCache,
)
from vertebra.db._staging import ObjectStoreStaging
from vertebra.db.adapters.spanner.config import (
    SPANGRES_DIALECT_TAG,
    SPANNER_GOOGLESQL_DIALECT_TAG,
    SpannerConfig,
)
from vertebra.db.adapters.spanner.core import SpannerCore
from vertebra.db.adapters.spanner.mutations import (
    Mutation,
    MutationGroup,
    StalenessBound,
)
from vertebra.db.adapters.spanner.transactions import (
    SyncSpannerSnapshot,
    SyncSpannerTransaction,
    compute_abort_backoff_seconds,
)
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver._sync import SyncDatabaseDriver
from vertebra.telemetry.db_propagation import SqlTracePropagator
from vertebra.utils.schema import to_value_type
from vertebra.utils.serializers import encode_json
from vertebra.utils.text import validate_identifier

__all__ = (
    "SyncSpannerClientProtocol",
    "SyncSpannerDriver",
    "SyncSpannerReadWriteTxProtocol",
)


class SyncSpannerReadWriteTxProtocol(Protocol):
    """Structural protocol for synchronous interactive Spanner ``ReadWrite`` handles."""

    @property
    def transaction_id(self) -> bytes:
        """Return the transaction identifier bytes."""
        ...

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> dict[str, Any]:
        """Execute a SQL query inside this transaction."""
        ...

    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> int:
        """Execute a DML statement inside this transaction."""
        ...

    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements inside this transaction."""
        ...

    def commit(self, mutations_json: str | None = None) -> dict[str, Any]:
        """Commit this transaction and optional buffered mutations."""
        ...

    def rollback(self) -> None:
        """Roll back this transaction."""
        ...


class SyncSpannerClientProtocol(Protocol):
    """Structural protocol for synchronous Cloud Spanner wire clients and test doubles."""

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        """Execute a streaming SQL read."""
        ...

    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> int:
        """Execute a single-statement transactional DML statement."""
        ...

    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements."""
        ...

    def execute_partitioned_dml(
        self,
        sql: str,
        params_json: str | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute a Partitioned DML statement."""
        ...

    def apply_mutations(
        self,
        mutations_json: str,
        transaction_tag: str | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        """Commit a JSON-encoded list of Spanner mutations."""
        ...

    def batch_write(
        self,
        mutation_groups_json: str,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Commit independent mutation groups via ``BatchWrite``."""
        ...

    def update_ddl(self, statements: Sequence[str]) -> str:
        """Execute schema DDL statements."""
        ...

    def partitioned_query(
        self,
        sql: str,
        params_json: str | None = None,
        max_partitions: int = 4,
        data_boost_enabled: bool = False,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        """Execute a parallel ``PartitionQuery`` read."""
        ...

    def begin_read_write_transaction(
        self,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> SyncSpannerReadWriteTxProtocol:
        """Open a stateful synchronous interactive ``ReadWrite`` transaction handle."""
        ...

    def close(self) -> None:
        """Close the client handle."""
        ...


class SyncSpannerDriver(SyncDatabaseDriver[SpannerConfig]):
    """Synchronous pure-Rust Google Cloud Spanner v1 driver."""

    dialect_tag: ClassVar[int] = SPANNER_GOOGLESQL_DIALECT_TAG

    __slots__ = (
        "_active_transaction",
        "_client",
        "_dialect_tag",
        "_last_commit_stats",
        "_propagator",
        "_statement_cache",
        "dialect",
    )

    def __init__(
        self,
        config: SpannerConfig | str | None = None,
        *,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
        propagator: SqlTracePropagator | None = None,
        client: SyncSpannerClientProtocol | None = None,
        **overrides: Any,
    ) -> None:
        if isinstance(config, str):
            resolved = SpannerConfig.from_dsn(config, **overrides)
        elif isinstance(config, SpannerConfig):
            resolved = msgspec.structs.replace(config, **overrides) if overrides else config
        else:
            dsn = overrides.pop("dsn", None)
            if isinstance(dsn, str) and dsn.startswith(("spanner", "spangres")):
                resolved = SpannerConfig.from_dsn(dsn, **overrides)
            else:
                resolved = SpannerConfig(**overrides)

        resolved_broker = broker or resolved.broker or LeaseBroker(max(1, resolved.pool_size * 4))
        super().__init__(resolved, broker=resolved_broker)
        self.dialect: Literal["googlesql", "spangres"] = resolved.dialect
        self._dialect_tag: int = (
            SPANGRES_DIALECT_TAG if resolved.dialect == "spangres" else SPANNER_GOOGLESQL_DIALECT_TAG
        )
        self._statement_cache: _RustStatementCache | None = (
            statement_cache
            if statement_cache is not None
            else (_RustStatementCache(max(16, resolved.statement_cache_capacity)) if resolved.use_shm_cache else None)
        )
        self._propagator: SqlTracePropagator = propagator or SqlTracePropagator()
        self._client: SyncSpannerClientProtocol | None = client
        self._last_commit_stats: dict[str, Any] = {}
        self._active_transaction: SyncSpannerTransaction | None = None

    @property
    def active_transaction(self) -> SyncSpannerTransaction | None:
        """Return the currently active interactive `ReadWrite` transaction, if any."""
        return self._active_transaction

    @property
    def transaction_id(self) -> bytes | None:
        """Return the active interactive transaction identifier bytes, if inside a transaction."""
        return self._active_transaction.transaction_id if self._active_transaction is not None else None

    @property
    def project_id(self) -> str:
        """Return the effective Google Cloud project ID."""
        return self.config.effective_project_id()

    @property
    def instance_id(self) -> str:
        """Return the effective Cloud Spanner instance ID."""
        return self.config.effective_instance_id()

    @property
    def database_id(self) -> str:
        """Return the effective Cloud Spanner database ID."""
        return self.config.effective_database_id()

    @property
    def database_uri(self) -> str:
        """Return the canonical `projects/{p}/instances/{i}/databases/{d}` resource URI."""
        return self.config.database_uri

    @property
    def last_commit_stats(self) -> dict[str, Any]:
        """Return commit timestamp and mutation count from the last write operation."""
        return dict(self._last_commit_stats)

    def connect(self) -> Self:
        """Initialize the underlying Rust `SyncSpannerClient` channel pool."""
        if self._client is None:
            self._client = SpannerCore.create_sync_rust_client(self.config)
        self._closed = False
        return self

    def close(self) -> None:
        """Release the underlying Rust client reference and roll back any active transaction."""
        if self._active_transaction is not None:
            with contextlib.suppress(Exception):
                self.rollback()
        self._client = None
        self._closed = True

    def _open_rw_transaction(
        self,
        *,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> SyncSpannerTransaction:
        client = self._ensure_client()
        effective_delay = max_commit_delay_ms if max_commit_delay_ms is not None else self.config.max_commit_delay_ms
        handle = client.begin_read_write_transaction(
            transaction_tag,
            previous_transaction_id,
            exclude_txn_from_change_streams,
            effective_delay,
        )
        return SyncSpannerTransaction(self, handle, transaction_tag=transaction_tag)

    def begin(
        self,
        *,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> None:
        """Begin an explicit interactive Spanner `ReadWrite` transaction scope."""
        if self._active_transaction is not None:
            msg = "Nested Spanner ReadWrite transactions are not supported"
            raise NotSupportedError(msg)
        self._active_transaction = self._open_rw_transaction(
            transaction_tag=transaction_tag,
            max_commit_delay_ms=max_commit_delay_ms,
            exclude_txn_from_change_streams=exclude_txn_from_change_streams,
        )

    def commit(self) -> None:
        """Commit the active interactive Spanner `ReadWrite` transaction and buffered mutations."""
        tx = self._active_transaction
        if tx is None:
            return
        self._active_transaction = None
        tx.commit()

    def rollback(self) -> None:
        """Roll back the active interactive Spanner `ReadWrite` transaction and discard buffered mutations."""
        tx = self._active_transaction
        if tx is None:
            return
        self._active_transaction = None
        tx.rollback()

    @contextmanager
    def transaction(
        self,
        *,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> Iterator[Self]:
        """Execute a single-attempt block inside an interactive Spanner `ReadWrite` transaction."""
        if self._active_transaction is not None:
            msg = "Nested Spanner ReadWrite transactions are not supported"
            raise NotSupportedError(msg)
        tx = self._open_rw_transaction(
            transaction_tag=transaction_tag,
            max_commit_delay_ms=max_commit_delay_ms,
            exclude_txn_from_change_streams=exclude_txn_from_change_streams,
        )
        self._active_transaction = tx
        try:
            yield self
        except BaseException:
            if tx.is_active:
                with contextlib.suppress(Exception):
                    tx.rollback()
            raise
        else:
            if tx.is_active:
                tx.commit()
        finally:
            if self._active_transaction is tx:
                self._active_transaction = None

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> SyncSpannerSnapshot:
        """Create a lock-free `ReadOnly` `SyncSpannerSnapshot` bound to `staleness`."""
        _ = multi_use
        return SyncSpannerSnapshot(self, staleness or StalenessBound.strong())

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML or DDL statement and return the affected row count."""
        _ = kwargs
        stripped = sql.strip()
        if SpannerCore.is_ddl_statement(stripped):
            if self._active_transaction is not None:
                msg = "DDL statements cannot be executed inside a Spanner ReadWrite transaction"
                raise NotSupportedError(msg)
            self.update_ddl([stripped])
            return 0
        if self._active_transaction is not None:
            return self._active_transaction.execute(stripped, parameters, request_tag=request_tag)

        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(stripped, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        lease = self._acquire_pid_lease()
        try:
            return int(
                client.execute_dml(
                    annotated_sql,
                    params_json,
                    request_tag,
                    transaction_tag,
                )
            )
        finally:
            self._release_pid_lease(lease)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute `sql` across multiple parameter sets via `ExecuteBatchDml`."""
        _ = kwargs
        if not parameter_sets:
            return 0
        statements = [(sql, params) for params in parameter_sets]
        counts = self.execute_batch_dml(
            statements,
            request_tag=request_tag,
            transaction_tag=transaction_tag,
        )
        return sum(counts)

    def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None] | Mapping[str, Any]],
        *,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        tag: str | None = None,
    ) -> list[int]:
        """Execute multiple DML statements in a single `ExecuteBatchDml` RPC."""
        effective_tx_tag = transaction_tag or (tag if tag else None)
        if not statements:
            return []
        if self._active_transaction is not None:
            return self._active_transaction.execute_batch_dml(statements, request_tag=request_tag)

        client = self._ensure_client()
        batch_json = SpannerCore.encode_batch_dml_json(statements, dialect=self.dialect)
        lease = self._acquire_pid_lease()
        try:
            return [
                int(c)
                for c in client.execute_batch_dml(
                    batch_json,
                    request_tag,
                    effective_tx_tag,
                )
            ]
        finally:
            self._release_pid_lease(lease)

    def execute_partitioned_dml(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute a large-scale Partitioned DML statement and return lower-bound affected rows."""
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        return int(
            client.execute_partitioned_dml(
                annotated_sql,
                params_json,
                exclude_txn_from_change_streams,
            )
        )

    def execute_script(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> int:
        """Execute a semicolon-delimited script of DDL or DML statements."""
        parts = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        if not parts:
            return 0
        ddl_batch = [stmt for stmt in parts if SpannerCore.is_ddl_statement(stmt)]
        dml_batch = [stmt for stmt in parts if not SpannerCore.is_ddl_statement(stmt)]
        if ddl_batch:
            self.update_ddl(ddl_batch)
        total = 0
        for stmt in dml_batch:
            total += self.execute(stmt, parameters)
        return total

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters, staleness=staleness)
        cols = tuple(str(c) for c in raw.get("columns", ()))
        rows = [tuple(r) for r in raw.get("rows", ())]
        return rows, cols

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and hydrate rows as dictionaries or `schema_type` instances."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters, staleness=staleness)
        return SpannerCore.hydrate_query_rows(
            raw.get("columns", ()),
            raw.get("rows", ()),
            schema_type=schema_type,
        )

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `NotFoundError` when 0 rows match."""
        rows = self.select(sql, parameters, schema_type=schema_type, staleness=staleness, **kwargs)
        if not rows:
            msg = "Spanner query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None` when empty."""
        rows = self.select(sql, parameters, schema_type=schema_type, staleness=staleness, **kwargs)
        return rows[0] if rows else None

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        raw = self._run_query_sync(sql, parameters, staleness=staleness)
        rows = raw.get("rows", ())
        if not rows:
            msg = "Spanner query returned no rows"
            raise NotFoundError(msg)
        if not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    def run_in_transaction(
        self,
        fn: Callable[[SyncSpannerTransaction], Any],
        *,
        max_attempts: int = 5,
        transaction_tag: str | None = None,
        tag: str | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> Any:
        """Execute `fn(tx)` inside an interactive `ReadWrite` transaction with wound-wait retry on `Aborted`."""
        effective_tag = transaction_tag or (tag if tag else None)
        attempts = max(1, int(max_attempts))
        previous_transaction_id: bytes | None = None
        for attempt in range(1, attempts + 1):
            tx = self._open_rw_transaction(
                transaction_tag=effective_tag,
                previous_transaction_id=previous_transaction_id,
                max_commit_delay_ms=max_commit_delay_ms,
                exclude_txn_from_change_streams=exclude_txn_from_change_streams,
            )
            try:
                result = fn(tx)
                if tx.is_active:
                    tx.commit()
                return result
            except SpannerAbortedError as exc:
                previous_transaction_id = tx.transaction_id or previous_transaction_id
                with contextlib.suppress(Exception):
                    tx.rollback()
                if attempt >= attempts:
                    raise
                _time.sleep(compute_abort_backoff_seconds(exc, attempt))
            except BaseException:
                with contextlib.suppress(Exception):
                    tx.rollback()
                raise
        return None

    def apply_mutations(
        self,
        mutations: Sequence[Mutation | Mapping[str, Any]],
        *,
        transaction_tag: str | None = None,
        max_commit_delay_ms: int | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> dict[str, Any]:
        """Commit a sequence of `Mutation` structs atomically."""
        if not mutations:
            return {"commit_timestamp_micros": 0, "mutation_count": 0, "retry_count": 0}
        normalized = [Mutation.coerce(m) for m in mutations]
        if self._active_transaction is not None:
            self._active_transaction.buffer_mutations(normalized)
            return {
                "commit_timestamp_micros": None,
                "mutation_count": len(normalized),
                "retry_count": 0,
            }

        client = self._ensure_client()
        effective_delay = max_commit_delay_ms if max_commit_delay_ms is not None else self.config.max_commit_delay_ms
        total_stats: dict[str, Any] = {
            "commit_timestamp_micros": None,
            "mutation_count": 0,
            "retry_count": 0,
        }
        for chunk in Mutation.chunk_by_cell_limit(normalized):
            mutations_json = encode_json([m.to_dict() for m in chunk])
            res = client.apply_mutations(
                mutations_json,
                transaction_tag,
                exclude_txn_from_change_streams,
                effective_delay,
            )
            total_stats["commit_timestamp_micros"] = res.get("commit_timestamp_micros")
            total_stats["mutation_count"] += int(res.get("mutation_count", len(chunk)))
            total_stats["retry_count"] += int(res.get("retry_count", 0))
        self._last_commit_stats = total_stats
        return total_stats

    def write_insert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `insert` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.insert(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    def write_update(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `update` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.update(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    def write_upsert(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply an `insert_or_update` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.insert_or_update(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    def write_replace(
        self,
        table: str,
        columns: Sequence[str],
        values: Sequence[Sequence[Any]],
        *,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply a `replace` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.replace(table, columns, values)],
            transaction_tag=transaction_tag,
        )

    def write_delete(
        self,
        table: str,
        keys: Sequence[Sequence[Any] | Any] = (),
        *,
        all_rows: bool = False,
        transaction_tag: str | None = None,
    ) -> dict[str, Any]:
        """Apply a `delete` mutation on `table`."""
        return self.apply_mutations(
            [Mutation.delete(table, keys, all_rows=all_rows)],
            transaction_tag=transaction_tag,
        )

    def batch_write(
        self,
        mutation_groups: Sequence[MutationGroup | Sequence[Mutation | Mapping[str, Any]]],
        *,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Commit independent `MutationGroup` batches via streaming `BatchWrite`."""
        if not mutation_groups:
            return []
        groups = [MutationGroup.coerce(g) for g in mutation_groups]
        client = self._ensure_client()
        groups_json = encode_json([g.to_dict() for g in groups])
        return list(client.batch_write(groups_json, exclude_txn_from_change_streams))

    def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Execute one or more DDL statements via `DatabaseAdmin/UpdateDatabaseDdl`."""
        stmt_list = [statements] if isinstance(statements, str) else [str(s) for s in statements if str(s).strip()]
        if not stmt_list:
            return ""
        client = self._ensure_client()
        return str(client.update_ddl(stmt_list))

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        max_partitions: int | None = None,
        data_boost_enabled: bool | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Execute a parallel `PartitionQuery` + Data Boost read into a `VTBARROW` `ArrowCursor`."""
        _ = kwargs
        query_sql = SpannerCore.build_select_sql_if_table(sql)
        bound = staleness or StalenessBound.strong()
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(query_sql, parameters)
        partitions = max_partitions if max_partitions is not None else self.config.max_partitions
        boost = data_boost_enabled if data_boost_enabled is not None else self.config.data_boost_enabled
        if hasattr(client, "partitioned_query"):
            try:
                raw = client.partitioned_query(
                    rewritten_sql,
                    params_json,
                    max(1, int(partitions)),
                    bool(boost),
                    bound.mode,
                    bound.effective_value(),
                )
            except Exception as exc:
                if not SpannerCore.is_partitioned_query_fallback_error(exc):
                    raise
                raw = self._run_query_sync(query_sql, parameters, staleness=bound)
        else:
            raw = self._run_query_sync(query_sql, parameters, staleness=bound)
        return ArrowCursor.from_rows(
            raw.get("rows", ()),
            columns=[str(c) for c in raw.get("columns", ())],
            threshold_rows=threshold_rows,
        )

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        upsert: bool = True,
        **kwargs: Any,
    ) -> int:
        """Bulk-load an `ArrowCursor`, Arrow IPC buffer, or `pyarrow` table into `table_name` via chunked mutations."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        rows = SpannerCore.coerce_arrow_to_dicts(data)
        if not rows:
            return 0
        cols = list(columns) if columns is not None else list(rows[0].keys())
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        values = [[r.get(c) for c in cols] for r in rows]
        mutation = (
            Mutation.insert_or_update(table_name, cols, values) if upsert else Mutation.insert(table_name, cols, values)
        )
        self.apply_mutations([mutation])
        return len(rows)

    def export_to_object_store(
        self,
        sql_or_table: str,
        store_or_uri: Any,
        destination_uri: str | None = None,
        *,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        store: Any | None = None,
        format: Literal["parquet", "arrow", "csv", "json", "jsonl"] = "parquet",
        compression: str = "ZSTD",
        data_boost_enabled: bool = True,
        staleness: StalenessBound | None = None,
    ) -> dict[str, Any]:
        """Export a Spanner table or query to `ObjectStoreBackend` via zero-copy `ArrowCursor`."""
        if destination_uri is None and isinstance(store_or_uri, str):
            target_uri = store_or_uri
            target_store = store
        else:
            target_store = store_or_uri if store is None else store
            target_uri = destination_uri or ""
        ObjectStoreStaging.validate_staging_target(target_store, target_uri)
        cursor = self.select_to_arrow(
            sql_or_table,
            params,
            data_boost_enabled=data_boost_enabled,
            staleness=staleness,
        )
        ObjectStoreStaging.write_cursor_sync(
            cursor,
            destination_uri=target_uri,
            store=target_store,
            format=format,
            compression=compression,
        )
        return {
            "rows_exported": cursor.row_count,
            "row_count": cursor.row_count,
            "destination_uri": target_uri,
            "format": format,
            "data_boost_enabled": bool(data_boost_enabled),
        }

    def load_from_object_store(
        self,
        table: str,
        store_or_uris: Any,
        source_uris: str | Sequence[str] | None = None,
        *,
        store: Any | None = None,
        format: Literal["parquet", "arrow", "csv", "json", "jsonl"] = "parquet",
        upsert: bool = True,
    ) -> int:
        """Load Parquet, Arrow IPC, CSV, or JSONL files from `ObjectStoreBackend` into `table`."""
        if source_uris is None and isinstance(store_or_uris, (str, Sequence)):
            uris_arg = store_or_uris
            target_store = store
        else:
            target_store = store_or_uris if store is None else store
            uris_arg = () if source_uris is None else source_uris
        uris = [uris_arg] if isinstance(uris_arg, str) else list(uris_arg)
        total = 0
        for uri in uris:
            cursor = ObjectStoreStaging.read_cursor_sync(uri, store=target_store, format=format)
            total += self.load_from_arrow(table, cursor, upsert=upsert)
        return total

    def _ensure_client(self) -> SyncSpannerClientProtocol:
        if self._client is None:
            self._client = SpannerCore.create_sync_rust_client(self.config)
        return self._client

    def _prepare_sql_and_params(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None,
    ) -> tuple[str, str | None]:
        rewritten = sql
        if self._statement_cache is not None:
            raw_bytes = sql.encode("utf-8")
            plan = self._statement_cache.lookup(self._dialect_tag, raw_bytes)
            if plan is not None and plan.compiled_sql:
                rewritten = plan.compiled_sql.decode("utf-8", errors="replace")
            else:
                param_count = len(parameters) if parameters is not None else 0
                self._statement_cache.insert(
                    self._dialect_tag,
                    raw_bytes,
                    param_count,
                    raw_bytes,
                    b"",
                )
        params_json = SpannerCore.encode_parameters_json(parameters, dialect=self.dialect)
        return rewritten, params_json

    def _run_query_sync(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        staleness: StalenessBound | None = None,
    ) -> dict[str, Any]:
        if self._active_transaction is not None and staleness is None:
            return self._active_transaction._query_raw(sql, parameters)
        client = self._ensure_client()
        bound = staleness or StalenessBound.strong()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = SpannerCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        raw = client.query(
            annotated_sql,
            params_json,
            bound.mode,
            bound.effective_value(),
        )
        return dict(raw)
