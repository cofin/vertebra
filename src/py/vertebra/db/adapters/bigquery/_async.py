"""Asynchronous pure-Rust Google Cloud BigQuery driver."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, Self

import msgspec

from vertebra._vertebra import (
    ArrowCursor as _RustArrowCursor,
)
from vertebra._vertebra import (
    LeaseBroker,
    NotFoundError,
)
from vertebra._vertebra import (
    StatementCache as _RustStatementCache,
)
from vertebra.db._staging import ObjectStoreStaging
from vertebra.db.adapters.bigquery.config import (
    BIGQUERY_DIALECT_TAG,
    BigQueryConfig,
    BigQueryDryRunResult,
)
from vertebra.db.adapters.bigquery.core import BigQueryCore
from vertebra.db.adapters.bigquery.object_tables import (
    ObjectRef,
    ObjectTableConfig,
)
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver import DatabaseError
from vertebra.db.driver._async import AsyncDatabaseDriver
from vertebra.telemetry.db_propagation import SqlTracePropagator
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import quote_dialect_identifier, validate_identifier

if TYPE_CHECKING:
    from vertebra.storage import AsyncObjectStore, SyncObjectStore

__all__ = (
    "AsyncBigQueryClientProtocol",
    "AsyncBigQueryCursorProtocol",
    "AsyncBigQueryDriver",
)


class AsyncBigQueryCursorProtocol(Protocol):
    """Structural protocol for asynchronous BigQuery PEP 249 cursors."""

    arraysize: int

    @property
    def description(self) -> tuple[tuple[Any, ...], ...] | None:
        """Return column descriptions for the current result set, or ``None``."""
        ...

    @property
    def rowcount(self) -> int:
        """Return the row or DML count of the last statement, or ``-1``."""
        ...

    @property
    def lastrowid(self) -> int | None:
        """Return ``None`` (BigQuery does not report last insert rowids)."""
        ...

    @property
    def query_metadata(self) -> Mapping[str, Any] | None:
        """Return job metadata for the last executed query, or ``None``."""
        ...

    def execute(
        self,
        sql: str,
        params: str | Sequence[Any] | Mapping[str, Any] | None = (),
    ) -> Awaitable[Any]:
        """Execute one query job asynchronously and buffer its rows, description, and metadata."""
        ...

    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[str | Sequence[Any] | Mapping[str, Any]],
    ) -> Awaitable[Any]:
        """Execute one query job per parameter set asynchronously and sum DML row counts."""
        ...

    def fetchone(self) -> Awaitable[tuple[Any, ...] | None]:
        """Return the next buffered row, or ``None`` when exhausted."""
        ...

    def fetchmany(self, size: int | None = None) -> Awaitable[list[tuple[Any, ...]]]:
        """Return up to ``size`` buffered rows."""
        ...

    def fetchall(self) -> Awaitable[list[tuple[Any, ...]]]:
        """Return every remaining buffered row."""
        ...

    def close(self) -> Awaitable[None]:
        """Close the cursor and drop its buffered rows."""
        ...


class AsyncBigQueryClientProtocol(Protocol):
    """Structural protocol for asynchronous BigQuery wire clients and test doubles."""

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        session_id: str | None = None,
        create_session: bool = False,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> Awaitable[Mapping[str, Any]]:
        """Execute a GoogleSQL query asynchronously."""
        ...

    def dry_run(
        self,
        sql: str,
        params_json: str | None = None,
    ) -> Awaitable[Mapping[str, Any]]:
        """Validate ``sql`` asynchronously via ``jobs.insert(dryRun=True)``."""
        ...

    def read_table_arrow(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int = 4,
        compression: str = "LZ4_FRAME",
    ) -> Awaitable[_RustArrowCursor]:
        """Stream a BigQuery table asynchronously into a zero-copy ``ArrowCursor``."""
        ...

    def append_arrow(
        self,
        parent_table: str,
        arrow_ipc_bytes: bytes,
        mode: str = "_default",
    ) -> Awaitable[Mapping[str, Any]]:
        """Append an Arrow IPC stream asynchronously via the Storage Write API."""
        ...

    def cursor(
        self,
        *,
        session_id: str | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: str | None = None,
        labels: Mapping[str, str] | None = None,
        param_encoder: Callable[[str, Any], tuple[str, str | None]] | None = None,
        lazy_begin: bool = False,
    ) -> AsyncBigQueryCursorProtocol:
        """Create an asynchronous PEP 249 cursor backed by this client."""
        ...

    def close(self) -> Any:
        """Close the client handle."""
        ...


class AsyncBigQueryDriver(AsyncDatabaseDriver[BigQueryConfig]):
    """Asynchronous pure-Rust Google Cloud BigQuery driver."""

    dialect: ClassVar[str] = "bigquery"
    dialect_tag: ClassVar[int] = BIGQUERY_DIALECT_TAG

    __slots__ = (
        "_client",
        "_in_transaction",
        "_last_query_metadata",
        "_propagator",
        "_session_id",
        "_statement_cache",
    )

    def __init__(
        self,
        config: BigQueryConfig | str | None = None,
        *,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
        propagator: SqlTracePropagator | None = None,
        client: AsyncBigQueryClientProtocol | None = None,
        **overrides: Any,
    ) -> None:
        if isinstance(config, str):
            resolved = BigQueryConfig.from_dsn(config, **overrides)
        elif isinstance(config, BigQueryConfig):
            resolved = msgspec.structs.replace(config, **overrides) if overrides else config
        else:
            dsn = overrides.pop("dsn", None)
            if isinstance(dsn, str) and dsn.startswith(("bigquery://", "bq://")):
                resolved = BigQueryConfig.from_dsn(dsn, **overrides)
            else:
                resolved = BigQueryConfig(**overrides)

        resolved_broker = broker or resolved.broker or LeaseBroker(max(1, resolved.pool_size * 4))
        super().__init__(resolved, broker=resolved_broker)
        self._statement_cache: _RustStatementCache | None = (
            statement_cache
            if statement_cache is not None
            else (_RustStatementCache(max(16, resolved.statement_cache_capacity)) if resolved.use_shm_cache else None)
        )
        self._propagator: SqlTracePropagator = propagator or SqlTracePropagator()
        self._client: AsyncBigQueryClientProtocol | None = client
        self._session_id: str | None = None
        self._in_transaction: bool = False
        self._last_query_metadata: dict[str, Any] = {}

    @property
    def project_id(self) -> str:
        """Return the effective Google Cloud project ID."""
        return self.config.effective_project_id()

    @property
    def dataset_id(self) -> str | None:
        """Return the effective default BigQuery dataset ID."""
        return self.config.effective_dataset_id()

    @property
    def session_id(self) -> str | None:
        """Return the active BigQuery multi-statement session ID, if any."""
        return self._session_id

    @property
    def in_transaction(self) -> bool:
        """Return whether a session-backed transaction is currently active."""
        return self._in_transaction

    @property
    def last_query_metadata(self) -> dict[str, Any]:
        """Return execution statistics from the most recent query job."""
        return dict(self._last_query_metadata)

    async def connect(self) -> Self:
        """Initialize the underlying Rust `AsyncBigQueryClient` channel pool."""
        if self._client is None:
            self._client = BigQueryCore.create_async_rust_client(self.config)
        self._closed = False
        return self

    async def close(self) -> None:
        """Roll back any active transaction, close the Rust client, and release references."""
        client = self._client
        try:
            if self._in_transaction:
                with suppress(Exception):
                    await self.rollback()
            if client is not None:
                res = client.close()
                if inspect.isawaitable(res):
                    await res
        finally:
            self._session_id = None
            self._in_transaction = False
            self._client = None
            self._closed = True

    def cursor(
        self,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        lazy_begin: bool = False,
    ) -> AsyncBigQueryCursorProtocol:
        """Create an asynchronous PEP 249 cursor bound to this driver's client and session."""
        merged_labels = BigQueryCore.sanitize_labels(self.config.labels, job_labels)
        effective_max_bytes = (
            maximum_bytes_billed if maximum_bytes_billed is not None else self.config.maximum_bytes_billed
        )
        return self._ensure_client().cursor(
            session_id=self._session_id,
            destination_table=destination_table,
            write_disposition=write_disposition,
            maximum_bytes_billed=effective_max_bytes,
            job_timeout_ms=self.config.job_timeout_ms,
            priority=self.config.priority,
            labels=merged_labels or None,
            param_encoder=self._encode_for_cursor,
            lazy_begin=lazy_begin,
        )

    async def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DML, DDL, or query statement and return affected or produced row count."""
        _ = kwargs
        cur = await self._execute_cursor(
            sql,
            parameters,
            job_labels=job_labels,
            destination_table=destination_table,
            write_disposition=write_disposition,
            maximum_bytes_billed=maximum_bytes_billed,
        )
        try:
            if cur.description is None:
                return max(cur.rowcount, 0)
            return len(await cur.fetchall())
        finally:
            await cur.close()

    async def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute a query and return `(tuple_rows, column_names)`."""
        _ = kwargs
        cur = await self._execute_cursor(
            sql,
            parameters,
            job_labels=job_labels,
            destination_table=destination_table,
            write_disposition=write_disposition,
            maximum_bytes_billed=maximum_bytes_billed,
        )
        try:
            description = cur.description
            if not description:
                return [], ()
            names = tuple(str(col[0]) for col in description)
            return await cur.fetchall(), names
        finally:
            await cur.close()

    async def execute_script(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a GoogleSQL procedural script or multi-statement batch."""
        rows, cols = await self.execute_with_description(sql, parameters)
        return BigQueryCore.rows_to_dicts(cols, rows)

    async def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        maximum_bytes_billed: int | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return rows as dictionaries or `schema_type` instances."""
        rows, cols = await self.execute_with_description(
            sql,
            parameters,
            maximum_bytes_billed=maximum_bytes_billed,
            **kwargs,
        )
        return BigQueryCore.hydrate_query_rows(
            cols,
            rows,
            schema_type=schema_type,
        )

    async def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `NotFoundError` when 0 rows match."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    async def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None` when 0 rows match."""
        rows = await self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    async def query(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        maximum_bytes_billed: int | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and return rows as dictionaries or `schema_type` instances."""
        return await self.select(
            sql, parameters, schema_type=schema_type, maximum_bytes_billed=maximum_bytes_billed, **kwargs
        )

    async def query_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `NotFoundError` when 0 rows match."""
        return await self.select_one(sql, parameters, schema_type=schema_type, **kwargs)

    async def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        rows, _ = await self.execute_with_description(sql, parameters, **kwargs)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        if not rows[0]:
            return None
        val = rows[0][0]
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    async def dry_run(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> BigQueryDryRunResult:
        """Validate `sql` via `jobs.insert(dryRun=True)` and return byte and cost estimates."""
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        raw = await client.dry_run(rewritten_sql, params_json)
        return BigQueryCore.build_dry_run_result(raw)

    async def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int | None = None,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Asynchronously read a table or query into a zero-copy `VTBARROW` `ArrowCursor`."""
        _ = kwargs
        client = self._ensure_client()
        streams = max_streams if max_streams is not None else self.config.max_read_streams
        table_parts = BigQueryCore.try_parse_table_reference(
            sql,
            default_project=self.project_id,
            default_dataset=self.dataset_id,
        )
        if table_parts is not None and not parameters and hasattr(client, "read_table_arrow"):
            project_id, dataset_id, table_id = table_parts
            lease = self._acquire_pid_lease()
            try:
                raw_cursor = await client.read_table_arrow(
                    project_id,
                    dataset_id,
                    table_id,
                    list(selected_fields) if selected_fields else None,
                    row_restriction,
                    streams,
                    self.config.arrow_compression,
                )
                return ArrowCursor(raw_cursor)
            except Exception as exc:
                if not BigQueryCore.is_storage_api_fallback_error(exc):
                    raise
            finally:
                self._release_pid_lease(lease)

        select_sql = BigQueryCore.build_table_or_query_sql(
            sql,
            selected_fields=selected_fields,
            row_restriction=row_restriction,
        )
        raw = await self._run_query_async(select_sql, parameters)
        return BigQueryCore.query_result_to_arrow_cursor(raw, threshold_rows=threshold_rows)

    async def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        mode: Literal["_default", "COMMITTED", "PENDING", "BUFFERED"] = "_default",
        **kwargs: Any,
    ) -> int:
        """Asynchronously bulk-load an Arrow IPC stream, `ArrowCursor`, or `pyarrow` table."""
        _ = kwargs
        BigQueryCore.validate_and_quote_table(table_name)
        if columns is not None:
            for col in columns:
                validate_identifier(str(col), "column", allow_dots=False)
        client = self._ensure_client()
        ipc_bytes = BigQueryCore.coerce_arrow_ipc_bytes(data)
        if not ipc_bytes:
            return 0
        if not hasattr(client, "append_arrow"):
            return await self._fallback_insert_arrow_via_rest(table_name, ipc_bytes)
        parent_table = BigQueryCore.qualify_table_resource(
            table_name,
            default_project=self.project_id,
            default_dataset=self.dataset_id,
        )
        lease = self._acquire_pid_lease()
        try:
            res = await client.append_arrow(parent_table, ipc_bytes, mode)
            return int(res.get("appended_rows", 0))
        except Exception as exc:
            if BigQueryCore.is_storage_api_fallback_error(exc):
                return await self._fallback_insert_arrow_via_rest(table_name, ipc_bytes)
            raise
        finally:
            self._release_pid_lease(lease)

    async def begin(self) -> None:
        """Start a BigQuery multi-statement transaction session."""
        if self._session_id is None:
            client = self._ensure_client()
            res = await client.query(
                BigQueryCore.annotate_sql_with_trace("BEGIN TRANSACTION", self._propagator),
                None,
                None,
                True,
            )
            self._after_query(res)
        else:
            cur = await self._execute_cursor("BEGIN TRANSACTION", None)
            await cur.close()
        self._in_transaction = True

    async def commit(self) -> None:
        """Commit the active BigQuery session transaction."""
        if not self._in_transaction:
            return
        try:
            cur = await self._execute_cursor("COMMIT TRANSACTION", None)
            await cur.close()
        finally:
            self._in_transaction = False

    async def rollback(self) -> None:
        """Roll back the active BigQuery session transaction."""
        if not self._in_transaction:
            return
        try:
            cur = await self._execute_cursor("ROLLBACK TRANSACTION", None)
            await cur.close()
        finally:
            self._in_transaction = False

    async def export_to_object_store(
        self,
        sql_or_table: str,
        destination_uri: str,
        *,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro"] = "parquet",
        compression: str = "ZSTD",
        overwrite: bool = True,
    ) -> int:
        """Asynchronously export a table or query to `gs://` or multicloud `ObjectStore`."""
        if destination_uri.startswith("gs://") and store is None:
            ObjectStoreStaging.validate_gcs_uri(destination_uri)
            export_sql = BigQueryCore.build_export_data_sql(
                sql_or_table,
                destination_uri=destination_uri,
                format=format,
                compression=compression,
                overwrite=overwrite,
            )
            return await self.execute(export_sql)

        ObjectStoreStaging.validate_staging_target(store, destination_uri)
        cursor = await self.select_to_arrow(sql_or_table)
        await ObjectStoreStaging.write_cursor_async(
            cursor,
            destination_uri=destination_uri,
            store=store,
            format=format,
            compression=compression,
        )
        return cursor.row_count

    async def load_from_object_store(
        self,
        table: str,
        source_uris: str | Sequence[str],
        *,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro", "orc"] = "parquet",
        write_disposition: Literal["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"] = "WRITE_APPEND",
    ) -> int:
        """Asynchronously load files from `gs://` or stream from multicloud `ObjectStore`."""
        uris = [source_uris] if isinstance(source_uris, str) else list(source_uris)
        if not uris:
            return 0
        if all(u.startswith("gs://") for u in uris) and store is None:
            for u in uris:
                ObjectStoreStaging.validate_gcs_uri(u)
            load_sql = BigQueryCore.build_load_data_sql(
                table,
                uris=uris,
                format=format,
                write_disposition=write_disposition,
            )
            return await self.execute(load_sql)

        total_rows = 0
        for uri in uris:
            cursor = await ObjectStoreStaging.read_cursor_async(
                uri,
                store=store,
                format=format,
            )
            total_rows += await self.load_from_arrow(table, cursor)
        return total_rows

    async def create_object_table(
        self,
        config: ObjectTableConfig,
        *,
        if_not_exists: bool = True,
    ) -> None:
        """Asynchronously create a BigQuery External Object Table from `config`."""
        ddl = config.create_object_table_sql(if_not_exists=if_not_exists)
        await self.execute(ddl)

    async def list_object_table_refs(
        self,
        config: ObjectTableConfig,
        *,
        store: SyncObjectStore | AsyncObjectStore | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ObjectRef]:
        """Asynchronously query a BigQuery Object Table and hydrate presigned `ObjectRef` entries."""
        sql = config.build_select_sql(where=where, limit=limit)
        try:
            rows = await self.select(sql)
        except (DatabaseError, OSError):
            if store is not None:
                rows = await config.list_from_store_async(store, limit=limit)
            else:
                raise
        return await config.hydrate_object_refs_async(
            rows,
            store=store if presign else None,
            expires_in_secs=expires_in_secs,
        )

    def _ensure_client(self) -> AsyncBigQueryClientProtocol:
        if self._client is None:
            created: AsyncBigQueryClientProtocol = BigQueryCore.create_async_rust_client(self.config)
            self._client = created
            return created
        return self._client

    def _prepare_sql_and_params(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> tuple[str, str | None]:
        rewritten = sql
        if self._statement_cache is not None:
            raw_bytes = sql.encode("utf-8")
            plan = self._statement_cache.lookup(BIGQUERY_DIALECT_TAG, raw_bytes)
            if plan is not None and plan.compiled_sql:
                rewritten = plan.compiled_sql.decode("utf-8", errors="replace")
            else:
                param_count = len(parameters) if parameters is not None else 0
                self._statement_cache.insert(
                    BIGQUERY_DIALECT_TAG,
                    raw_bytes,
                    param_count,
                    raw_bytes,
                    b"",
                )
        params_json = BigQueryCore.encode_parameters_json(parameters)
        return rewritten, params_json

    def _encode_for_cursor(
        self,
        sql: str,
        parameters: Any,
    ) -> tuple[str, str | None]:
        return self._prepare_sql_and_params(sql, parameters)

    def _after_query(self, metadata: Mapping[str, Any]) -> None:
        session_id = metadata.get("session_id")
        if session_id:
            self._session_id = str(session_id)
        self._last_query_metadata = {
            "job_id": metadata.get("job_id"),
            "session_id": self._session_id,
            "total_rows": metadata.get("total_rows", 0),
            "total_bytes_processed": metadata.get("total_bytes_processed", 0),
            "total_bytes_billed": metadata.get("total_bytes_billed", 0),
            "slot_ms": metadata.get("slot_ms", 0),
            "cache_hit": metadata.get("cache_hit", False),
            "num_dml_affected_rows": metadata.get("num_dml_affected_rows"),
        }
        BigQueryCore.record_finops_metrics(self.project_id, self.config.location, metadata)

    async def _execute_cursor(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
    ) -> AsyncBigQueryCursorProtocol:
        annotated_sql = BigQueryCore.annotate_sql_with_trace(sql, self._propagator)
        cur = self.cursor(
            job_labels=job_labels,
            destination_table=destination_table,
            write_disposition=write_disposition,
            maximum_bytes_billed=maximum_bytes_billed,
        )
        try:
            await cur.execute(annotated_sql, parameters or ())
        except Exception:
            await cur.close()
            raise
        metadata = cur.query_metadata
        if metadata is not None:
            self._after_query(metadata)
        return cur

    async def _run_query_async(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
    ) -> dict[str, Any]:
        client = self._ensure_client()
        rewritten_sql, params_json = self._prepare_sql_and_params(sql, parameters)
        annotated_sql = BigQueryCore.annotate_sql_with_trace(rewritten_sql, self._propagator)
        merged_labels = BigQueryCore.sanitize_labels(self.config.labels, job_labels)
        raw = await client.query(
            annotated_sql,
            params_json,
            self._session_id,
            False,
            destination_table,
            write_disposition,
            maximum_bytes_billed if maximum_bytes_billed is not None else self.config.maximum_bytes_billed,
            self.config.job_timeout_ms,
            self.config.priority,
            merged_labels or None,
        )
        self._after_query(raw)
        return dict(raw)

    async def _fallback_insert_arrow_via_rest(
        self,
        table: str,
        ipc_bytes: bytes,
    ) -> int:
        quoted_table = BigQueryCore.validate_and_quote_table(table)
        rows = BigQueryCore.arrow_ipc_bytes_to_dicts(ipc_bytes)
        if not rows:
            return 0
        cols = list(rows[0].keys())
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        col_list = ", ".join(quote_dialect_identifier(str(c), "bigquery") for c in cols)
        placeholders = ", ".join(f"@{c}" for c in cols)
        sql = f"INSERT INTO {quoted_table} ({col_list}) VALUES ({placeholders})"
        return await self.execute_many(sql, rows)
