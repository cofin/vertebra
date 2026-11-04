"""SQLSpec adapter, data dictionary, and zero-SDK sub-extension stores for Google Cloud BigQuery."""

from __future__ import annotations

import math
import time
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Final, Literal, Self

from vertebra._vertebra import LeaseBroker
from vertebra._vertebra import StatementCache as _RustStatementCache
from vertebra.db._arrow import ArrowCursor
from vertebra.db.bigquery import (
    AsyncBigQueryDriver as _CoreAsyncBigQueryDriver,
)
from vertebra.db.bigquery import (
    BigQueryConfig,
    BigQueryDryRunResult,
    ObjectRef,
    ObjectTableConfig,
)
from vertebra.db.bigquery import (
    SyncBigQueryDriver as _CoreSyncBigQueryDriver,
)
from vertebra.extensions.sqlspec.adapters import (
    AsyncAdapterConfig,
    BaseAsyncDriver,
    BaseSyncDriver,
    SyncAdapterConfig,
    TenantPredicate,
)
from vertebra.extensions.sqlspec.storage import ObjectStoreBackend
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.serializers import decode_json, encode_json
from vertebra.utils.text import quote_backtick_identifier, split_qualified_identifier
from vertebra.utils.uuids import uuid4

__all__ = (
    "BigQueryAsyncADKArtifactStore",
    "BigQueryAsyncADKMemoryStore",
    "BigQueryAsyncADKStore",
    "BigQueryAsyncConfig",
    "BigQueryAsyncDriver",
    "BigQueryAsyncEventQueueStore",
    "BigQueryAsyncStore",
    "BigQueryConnectionQueryJob",
    "BigQueryConnectionShim",
    "BigQueryAsyncDataDictionary",
    "BigQuerySyncDataDictionary",
    "BigQuerySyncADKArtifactStore",
    "BigQuerySyncADKMemoryStore",
    "BigQuerySyncADKStore",
    "BigQuerySyncConfig",
    "BigQuerySyncDriver",
    "BigQuerySyncEventQueueStore",
    "BigQuerySyncStore",
)

_DEFAULT_SESSIONS_TABLE: Final[str] = "vertebra_adk_sessions"
_DEFAULT_EVENTS_TABLE: Final[str] = "vertebra_adk_events"
_DEFAULT_MEMORIES_TABLE: Final[str] = "vertebra_adk_memories"
_DEFAULT_ARTIFACTS_TABLE: Final[str] = "vertebra_adk_artifacts"
_DEFAULT_KV_STORE_TABLE: Final[str] = "vertebra_kv_store"
_DEFAULT_EVENT_QUEUE_TABLE: Final[str] = "vertebra_event_queue"


class BigQueryConnectionQueryJob:
    """Lightweight query job handle returned by ``BigQueryConnectionShim.query()``."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        metadata: Mapping[str, Any],
        arrow_cursor: ArrowCursor | None = None,
    ) -> None:
        self._rows = rows
        self._metadata = dict(metadata)
        self._arrow_cursor = arrow_cursor

    @property
    def job_id(self) -> str | None:
        """Return the BigQuery job identifier."""
        val = self._metadata.get("job_id")
        return str(val) if val is not None else None

    @property
    def session_id(self) -> str | None:
        """Return the active BigQuery session identifier, if any."""
        val = self._metadata.get("session_id")
        return str(val) if val is not None else None

    @property
    def total_bytes_processed(self) -> int:
        """Return total bytes processed by the query job."""
        return int(self._metadata.get("total_bytes_processed", 0) or 0)

    @property
    def total_bytes_billed(self) -> int:
        """Return total bytes billed by the query job."""
        return int(self._metadata.get("total_bytes_billed", 0) or 0)

    @property
    def cache_hit(self) -> bool:
        """Return whether the query result was served from BigQuery cache."""
        return bool(self._metadata.get("cache_hit", False))

    @property
    def num_dml_affected_rows(self) -> int | None:
        """Return affected row count for DML statements."""
        val = self._metadata.get("num_dml_affected_rows")
        return int(val) if val is not None else None

    def result(self) -> list[dict[str, Any]]:
        """Return materialized query rows as dictionaries."""
        return list(self._rows)

    def to_arrow(self) -> Any:
        """Return the query result as an Arrow table or ``ArrowCursor``."""
        if self._arrow_cursor is not None:
            return self._arrow_cursor.to_arrow_table()
        cursor = ArrowCursor.from_rows(self._rows, threshold_rows=1)
        return cursor.to_arrow_table()

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self._rows)

    def __len__(self) -> int:
        return len(self._rows)


class BigQueryConnectionShim:
    """Zero-SDK connection shim exposing ``.query()`` and ``.close()`` over ``SyncBigQueryDriver``."""

    def __init__(self, driver: Any) -> None:
        self.driver: Any = driver

    @property
    def project(self) -> str:
        """Return the effective Google Cloud project ID."""
        return str(self.driver.project_id)

    @property
    def dataset_id(self) -> str | None:
        """Return the default BigQuery dataset ID."""
        val = self.driver.dataset_id
        return str(val) if val is not None else None

    @property
    def location(self) -> str:
        """Return the configured BigQuery processing location."""
        return str(self.driver.config.location)

    def query(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        job_config: Any = None,
    ) -> BigQueryConnectionQueryJob:
        """Execute ``sql`` through the Rust BigQuery driver and return a ``BigQueryConnectionQueryJob``."""
        _ = job_config
        rows = self.driver.select(sql, parameters)
        return BigQueryConnectionQueryJob(rows, self.driver.last_query_metadata)

    def close(self) -> None:
        """Close the underlying ``SyncBigQueryDriver``."""
        self.driver.close()

    def __enter__(self) -> Self:
        self.driver.connect()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class _BaseBigQueryDataDictionary:
    """Base helper for BigQuery ``INFORMATION_SCHEMA`` metadata queries."""

    def __init__(
        self,
        driver: Any,
        *,
        default_dataset: str | None = None,
    ) -> None:
        self._driver = driver
        self._default_dataset = default_dataset or getattr(driver, "dataset_id", None)

    def _resolve_dataset(self, dataset_id: str | None) -> str:
        resolved = dataset_id or self._default_dataset or getattr(self._driver, "dataset_id", None)
        if not resolved:
            raise ImproperConfigurationError(
                "BigQuerySyncDataDictionary / BigQueryAsyncDataDictionary requires a dataset_id argument or default BigQueryConfig.dataset_id"
            )
        return str(resolved).strip().strip("`")


class BigQuerySyncDataDictionary(_BaseBigQueryDataDictionary):
    """Synchronous BigQuery ``INFORMATION_SCHEMA`` metadata introspection helper."""

    def get_tables(self, dataset_id: str | None = None) -> list[dict[str, Any]]:
        """Query ``INFORMATION_SCHEMA.TABLES`` synchronously for ``dataset_id``."""
        sync_driver = self._require_sync_driver()
        ds = self._resolve_dataset(dataset_id)
        sql = (
            f"SELECT table_catalog, table_schema, table_name, table_type, creation_time, ddl "
            f"FROM `{ds}.INFORMATION_SCHEMA.TABLES` "
            f"ORDER BY table_name"
        )
        res: list[dict[str, Any]] = sync_driver.select(sql)
        return res

    def get_columns(self, table_name: str, dataset_id: str | None = None) -> list[dict[str, Any]]:
        """Query ``INFORMATION_SCHEMA.COLUMNS`` synchronously for ``table_name``."""
        sync_driver = self._require_sync_driver()
        ds = self._resolve_dataset(dataset_id)
        sql = (
            f"SELECT table_name, column_name, ordinal_position, is_nullable, data_type, "
            f"is_partitioning_column, clustering_ordinal_position "
            f"FROM `{ds}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = @table_name "
            f"ORDER BY ordinal_position"
        )
        res: list[dict[str, Any]] = sync_driver.select(sql, {"table_name": table_name})
        return res

    def get_partitions(self, table_name: str, dataset_id: str | None = None) -> list[dict[str, Any]]:
        """Query ``INFORMATION_SCHEMA.PARTITIONS`` synchronously for ``table_name``."""
        sync_driver = self._require_sync_driver()
        ds = self._resolve_dataset(dataset_id)
        sql = (
            f"SELECT table_name, partition_id, total_rows, total_logical_bytes, "
            f"total_billable_bytes, last_modified_time, storage_tier "
            f"FROM `{ds}.INFORMATION_SCHEMA.PARTITIONS` "
            f"WHERE table_name = @table_name "
            f"ORDER BY partition_id"
        )
        res: list[dict[str, Any]] = sync_driver.select(sql, {"table_name": table_name})
        return res

    def _require_sync_driver(self) -> Any:
        if isinstance(self._driver, _CoreSyncBigQueryDriver) or hasattr(self._driver, "select"):
            return self._driver
        raise ImproperConfigurationError("Synchronous BigQuerySyncDataDictionary methods require SyncBigQueryDriver")


class BigQueryAsyncDataDictionary(_BaseBigQueryDataDictionary):
    """Asynchronous BigQuery ``INFORMATION_SCHEMA`` metadata introspection helper."""

    async def get_tables(self, dataset_id: str | None = None) -> list[dict[str, Any]]:
        """Query ``INFORMATION_SCHEMA.TABLES`` asynchronously for ``dataset_id``."""
        async_driver = self._require_async_driver()
        ds = self._resolve_dataset(dataset_id)
        sql = (
            f"SELECT table_catalog, table_schema, table_name, table_type, creation_time, ddl "
            f"FROM `{ds}.INFORMATION_SCHEMA.TABLES` "
            f"ORDER BY table_name"
        )
        res: list[dict[str, Any]] = await async_driver.select(sql)
        return res

    async def get_columns(self, table_name: str, dataset_id: str | None = None) -> list[dict[str, Any]]:
        """Query ``INFORMATION_SCHEMA.COLUMNS`` asynchronously for ``table_name``."""
        async_driver = self._require_async_driver()
        ds = self._resolve_dataset(dataset_id)
        sql = (
            f"SELECT table_name, column_name, ordinal_position, is_nullable, data_type, "
            f"is_partitioning_column, clustering_ordinal_position "
            f"FROM `{ds}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = @table_name "
            f"ORDER BY ordinal_position"
        )
        res: list[dict[str, Any]] = await async_driver.select(sql, {"table_name": table_name})
        return res

    async def get_partitions(self, table_name: str, dataset_id: str | None = None) -> list[dict[str, Any]]:
        """Query ``INFORMATION_SCHEMA.PARTITIONS`` asynchronously for ``table_name``."""
        async_driver = self._require_async_driver()
        ds = self._resolve_dataset(dataset_id)
        sql = (
            f"SELECT table_name, partition_id, total_rows, total_logical_bytes, "
            f"total_billable_bytes, last_modified_time, storage_tier "
            f"FROM `{ds}.INFORMATION_SCHEMA.PARTITIONS` "
            f"WHERE table_name = @table_name "
            f"ORDER BY partition_id"
        )
        res: list[dict[str, Any]] = await async_driver.select(sql, {"table_name": table_name})
        return res

    def _require_async_driver(self) -> Any:
        if isinstance(self._driver, _CoreAsyncBigQueryDriver) or hasattr(self._driver, "select"):
            return self._driver
        raise ImproperConfigurationError("Asynchronous BigQueryAsyncDataDictionary methods require AsyncBigQueryDriver")


class BigQuerySyncDriver(BaseSyncDriver):
    """Synchronous SQLSpec adapter driver for Google Cloud BigQuery."""

    dialect: str = "bigquery"

    def __init__(
        self,
        core_driver: Any,
        statement_config: Any | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        *,
        storage_backend: ObjectStoreBackend | None = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = True,
        tenant_predicate: TenantPredicate | None = None,
    ) -> None:
        super().__init__(
            core_driver,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
        )
        self._core: Any = core_driver
        self._storage_backend: ObjectStoreBackend | None = storage_backend
        self._stmt_cache_capacity: int = statement_cache_size
        self._enable_sqlcommenter: bool = enable_sqlcommenter
        self._tenant_predicate: TenantPredicate | None = tenant_predicate
        self._data_dictionary = BigQuerySyncDataDictionary(core_driver)

    @property
    def core_driver(self) -> Any:
        """Return the underlying ``SyncBigQueryDriver`` instance."""
        return self._core

    @property
    def data_dictionary(self) -> Any:
        """Return the ``BigQuerySyncDataDictionary`` metadata helper."""
        return self._data_dictionary

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured ``ObjectStoreBackend``, if any."""
        return self._storage_backend

    def execute(
        self,
        sql: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        tenant_id: str | None = None,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a GoogleSQL statement and return affected or produced row count."""
        _ = (statement_config, kwargs)
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        params = self._normalize_params(parameters)
        return int(
            self._core.execute(
                prepared_sql,
                params,
                job_labels=job_labels,
                destination_table=destination_table,
                write_disposition=write_disposition,
                maximum_bytes_billed=maximum_bytes_billed,
            )
        )

    def execute_many(
        self,
        sql: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: Any | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute ``sql`` across multiple parameter sets and return total affected rows."""
        _ = (filters, statement_config, kwargs)
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        return int(self._core.execute_many(prepared_sql, parameters))

    def execute_script(
        self,
        sql: Any,
        /,
        *parameters: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute a multi-statement GoogleSQL script."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = self._normalize_params(parameters)
        return self._core.execute_script(sql_str, params)

    def execute_stack(
        self,
        statements: Any,
        *,
        continue_on_error: bool = False,
        in_transaction: bool = False,
        tenant_id: str | None = None,
    ) -> Any:
        """Compile and execute a batch of statements sequentially or inside a BigQuery session transaction."""
        if hasattr(statements, "operations"):
            return super().execute_stack(statements, continue_on_error=continue_on_error)
        results: list[Any] = []
        if in_transaction:
            with self._core.transaction():
                for item in statements:
                    results.append(self._execute_stack_item(item, tenant_id=tenant_id))
            return results
        for item in statements:
            results.append(self._execute_stack_item(item, tenant_id=tenant_id))
        return results

    def select(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a ``SELECT`` query and hydrate rows into dictionaries or ``schema_type`` structs."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        params = self._normalize_params(parameters)
        res: list[Any] = self._core.select(prepared_sql, params, schema_type=schema_type)
        return res

    def select_one(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a ``SELECT`` query and return the first row, raising ``KeyError`` if empty."""
        _ = kwargs
        row = self.select_one_or_none(sql, *parameters, schema_type=schema_type, tenant_id=tenant_id)
        if row is None:
            raise KeyError("BigQuery query returned no rows")
        return row

    def select_one_or_none(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a ``SELECT`` query and return the first row or ``None``."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        params = self._normalize_params(parameters)
        return self._core.select_one(prepared_sql, params, schema_type=schema_type)

    def select_value(
        self,
        sql: Any,
        /,
        *parameters: Any,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a ``SELECT`` query and return the first scalar column of the first row."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        params = self._normalize_params(parameters)
        return self._core.select_value(prepared_sql, params)

    def dry_run(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        tenant_id: str | None = None,
    ) -> BigQueryDryRunResult:
        """Validate ``sql`` via ``jobs.insert(dryRun=True)`` and return byte and USD cost estimates."""
        prepared_sql = self._prepare_sql(sql, tenant_id=tenant_id)
        res: BigQueryDryRunResult = self._core.dry_run(prepared_sql, parameters)
        return res

    def select_to_arrow(
        self,
        sql_or_table: Any,
        /,
        *parameters: Any,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Read a table or query into a zero-copy ``VTBARROW`` shared-memory ``ArrowCursor``."""
        _ = kwargs
        sql_str = sql_or_table.sql if hasattr(sql_or_table, "sql") else str(sql_or_table)
        params = self._normalize_params(parameters)
        return self._core.fetch_arrow(
            sql_str,
            params,
            selected_fields=selected_fields,
            row_restriction=row_restriction,
            max_streams=max_streams,
        )

    def load_from_arrow(
        self,
        table: str,
        source: ArrowCursor | bytes | Any,
        *,
        mode: Literal["_default", "COMMITTED", "PENDING", "BUFFERED"] = "_default",
        **kwargs: Any,
    ) -> Any:
        """Append an ``ArrowCursor``, Arrow IPC buffer, or ``pyarrow`` table via Storage Write API v1."""
        _ = kwargs
        return int(self._core.insert_arrow(table, source, mode=mode))

    def select_to_storage(
        self,
        sql_or_table: Any,
        destination_uri: Any,
        /,
        *parameters: Any,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro"] = "parquet",
        compression: str = "ZSTD",
        overwrite: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Export a BigQuery table or query to ``gs://`` or configured multicloud ``ObjectStoreBackend``."""
        _ = (parameters, kwargs)
        effective_store = store if store is not None else self._storage_backend
        return int(
            self._core.export_to_object_store(
                str(sql_or_table),
                str(destination_uri),
                store=effective_store,
                format=format,
                compression=compression,
                overwrite=overwrite,
            )
        )

    def load_from_storage(
        self,
        table: str,
        source: Any,
        *,
        file_format: Any = None,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro", "orc"] = "parquet",
        write_disposition: Literal["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"] = "WRITE_APPEND",
        **kwargs: Any,
    ) -> Any:
        """Load files from ``gs://`` or stream from configured multicloud ``ObjectStoreBackend``."""
        _ = kwargs
        effective_store = store if store is not None else self._storage_backend
        effective_fmt: Any = file_format if file_format is not None else format
        return int(
            self._core.load_from_object_store(
                table,
                source,
                store=effective_store,
                format=effective_fmt,
                write_disposition=write_disposition,
            )
        )

    def create_object_table(
        self,
        config: ObjectTableConfig,
        *,
        if_not_exists: bool = True,
    ) -> None:
        """Create a BigQuery External Object Table from ``config``."""
        self._core.create_object_table(config, if_not_exists=if_not_exists)

    def list_object_table_refs(
        self,
        config: ObjectTableConfig,
        *,
        store: Any | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ObjectRef]:
        """Query a BigQuery Object Table and hydrate presigned ``ObjectRef`` entries."""
        effective_store = store if store is not None else self._storage_backend
        res: list[ObjectRef] = self._core.list_object_table_refs(
            config,
            store=effective_store,
            where=where,
            limit=limit,
            presign=presign,
            expires_in_secs=expires_in_secs,
        )
        return res

    def begin(self) -> None:
        """Begin a BigQuery multi-statement transaction session."""
        self._core.begin()

    def commit(self) -> None:
        """Commit the active BigQuery transaction session."""
        self._core.commit()

    def rollback(self) -> None:
        """Roll back the active BigQuery transaction session."""
        self._core.rollback()

    def close(self) -> None:
        """Close the underlying ``SyncBigQueryDriver``."""
        self._core.close()

    def _prepare_sql(self, sql: str, *, tenant_id: str | None = None) -> str:
        if self._tenant_predicate is not None and tenant_id is not None:
            apply_fn = getattr(self._tenant_predicate, "apply", None)
            if callable(apply_fn):
                return str(apply_fn(sql, tenant_id))
        return sql

    def _execute_stack_item(
        self,
        item: str | tuple[str, Mapping[str, Any] | Sequence[Any] | None],
        *,
        tenant_id: str | None = None,
    ) -> Any:
        if isinstance(item, tuple):
            sql, params = item
        else:
            sql, params = item, None
        stripped = sql.strip().upper()
        if stripped.startswith(("SELECT ", "WITH ")):
            return (
                self.select(sql, params, tenant_id=tenant_id)
                if params is not None
                else self.select(sql, tenant_id=tenant_id)
            )
        return (
            self.execute(sql, params, tenant_id=tenant_id)
            if params is not None
            else self.execute(sql, tenant_id=tenant_id)
        )


class BigQueryAsyncDriver(BaseAsyncDriver):
    """Asynchronous SQLSpec adapter driver for Google Cloud BigQuery."""

    dialect: str = "bigquery"

    def __init__(
        self,
        core_driver: Any,
        statement_config: Any | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        *,
        storage_backend: ObjectStoreBackend | None = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = True,
        tenant_predicate: TenantPredicate | None = None,
    ) -> None:
        super().__init__(
            core_driver,
            statement_config=statement_config,
            driver_features=driver_features,
            observability=observability,
        )
        self._core: Any = core_driver
        self._storage_backend: ObjectStoreBackend | None = storage_backend
        self._stmt_cache_capacity: int = statement_cache_size
        self._enable_sqlcommenter: bool = enable_sqlcommenter
        self._tenant_predicate: TenantPredicate | None = tenant_predicate
        self._data_dictionary = BigQueryAsyncDataDictionary(core_driver)

    @property
    def core_driver(self) -> Any:
        """Return the underlying ``AsyncBigQueryDriver`` instance."""
        return self._core

    @property
    def data_dictionary(self) -> Any:
        """Return the ``BigQueryAsyncDataDictionary`` metadata helper."""
        return self._data_dictionary

    @property
    def storage_backend(self) -> ObjectStoreBackend | None:
        """Return the configured ``ObjectStoreBackend``, if any."""
        return self._storage_backend

    async def execute(
        self,
        sql: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        tenant_id: str | None = None,
        job_labels: Mapping[str, str] | None = None,
        destination_table: str | None = None,
        write_disposition: str | None = None,
        maximum_bytes_billed: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a GoogleSQL statement and return affected or produced row count."""
        _ = (statement_config, kwargs)
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        params = BaseSyncDriver._normalize_params(parameters)
        return int(
            await self._core.execute(
                prepared_sql,
                params,
                job_labels=job_labels,
                destination_table=destination_table,
                write_disposition=write_disposition,
                maximum_bytes_billed=maximum_bytes_billed,
            )
        )

    async def execute_many(
        self,
        sql: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: Any | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute ``sql`` across multiple parameter sets and return total affected rows."""
        _ = (filters, statement_config, kwargs)
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        return int(await self._core.execute_many(prepared_sql, parameters))

    async def execute_script(
        self,
        sql: Any,
        /,
        *parameters: Any,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a multi-statement GoogleSQL script."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = BaseSyncDriver._normalize_params(parameters)
        return await self._core.execute_script(sql_str, params)

    async def execute_stack(
        self,
        statements: Any,
        *,
        continue_on_error: bool = False,
        in_transaction: bool = False,
        tenant_id: str | None = None,
    ) -> Any:
        """Asynchronously compile and execute a batch of statements."""
        if hasattr(statements, "operations"):
            return await super().execute_stack(statements, continue_on_error=continue_on_error)
        results: list[Any] = []
        if in_transaction:
            async with self._core.transaction():
                for item in statements:
                    results.append(await self._execute_stack_item_async(item, tenant_id=tenant_id))
            return results
        for item in statements:
            results.append(await self._execute_stack_item_async(item, tenant_id=tenant_id))
        return results

    async def select(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Asynchronously execute a ``SELECT`` query and hydrate rows."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        params = BaseSyncDriver._normalize_params(parameters)
        res: list[Any] = await self._core.select(prepared_sql, params, schema_type=schema_type)
        return res

    async def select_one(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a ``SELECT`` query and return the first row, raising ``KeyError`` if empty."""
        _ = kwargs
        row = await self.select_one_or_none(sql, *parameters, schema_type=schema_type, tenant_id=tenant_id)
        if row is None:
            raise KeyError("BigQuery query returned no rows")
        return row

    async def select_one_or_none(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Asynchronously execute a ``SELECT`` query and return the first row or ``None``."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        params = BaseSyncDriver._normalize_params(parameters)
        return await self._core.select_one(prepared_sql, params, schema_type=schema_type)

    async def select_value(
        self,
        sql: Any,
        /,
        *parameters: Any,
        tenant_id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a ``SELECT`` query and return the first scalar value."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        prepared_sql = self._prepare_sql(sql_str, tenant_id=tenant_id)
        params = BaseSyncDriver._normalize_params(parameters)
        return await self._core.select_value(prepared_sql, params)

    async def dry_run(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        tenant_id: str | None = None,
    ) -> BigQueryDryRunResult:
        """Asynchronously validate ``sql`` via ``jobs.insert(dryRun=True)``."""
        prepared_sql = self._prepare_sql(sql, tenant_id=tenant_id)
        res: BigQueryDryRunResult = await self._core.dry_run(prepared_sql, parameters)
        return res

    async def select_to_arrow(
        self,
        sql_or_table: Any,
        /,
        *parameters: Any,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
        max_streams: int | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously read a table or query into a zero-copy ``VTBARROW`` ``ArrowCursor``."""
        _ = kwargs
        sql_str = sql_or_table.sql if hasattr(sql_or_table, "sql") else str(sql_or_table)
        params = BaseSyncDriver._normalize_params(parameters)
        return await self._core.fetch_arrow(
            sql_str,
            params,
            selected_fields=selected_fields,
            row_restriction=row_restriction,
            max_streams=max_streams,
        )

    async def load_from_arrow(
        self,
        table: str,
        source: ArrowCursor | bytes | Any,
        *,
        mode: Literal["_default", "COMMITTED", "PENDING", "BUFFERED"] = "_default",
        **kwargs: Any,
    ) -> Any:
        """Asynchronously append an ``ArrowCursor``, Arrow IPC buffer, or ``pyarrow`` table."""
        _ = kwargs
        return int(await self._core.insert_arrow(table, source, mode=mode))

    async def select_to_storage(
        self,
        sql_or_table: Any,
        destination_uri: Any,
        /,
        *parameters: Any,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro"] = "parquet",
        compression: str = "ZSTD",
        overwrite: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously export a BigQuery table or query to ``gs://`` or ``ObjectStoreBackend``."""
        _ = (parameters, kwargs)
        effective_store = store if store is not None else self._storage_backend
        return int(
            await self._core.export_to_object_store(
                str(sql_or_table),
                str(destination_uri),
                store=effective_store,
                format=format,
                compression=compression,
                overwrite=overwrite,
            )
        )

    async def load_from_storage(
        self,
        table: str,
        source: Any,
        *,
        file_format: Any = None,
        store: Any | None = None,
        format: Literal["parquet", "csv", "json", "avro", "orc"] = "parquet",
        write_disposition: Literal["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"] = "WRITE_APPEND",
        **kwargs: Any,
    ) -> Any:
        """Asynchronously load files from ``gs://`` or stream from ``ObjectStoreBackend``."""
        _ = kwargs
        effective_store = store if store is not None else self._storage_backend
        effective_fmt: Any = file_format if file_format is not None else format
        return int(
            await self._core.load_from_object_store(
                table,
                source,
                store=effective_store,
                format=effective_fmt,
                write_disposition=write_disposition,
            )
        )

    async def create_object_table(
        self,
        config: ObjectTableConfig,
        *,
        if_not_exists: bool = True,
    ) -> None:
        """Asynchronously create a BigQuery External Object Table from ``config``."""
        await self._core.create_object_table(config, if_not_exists=if_not_exists)

    async def list_object_table_refs(
        self,
        config: ObjectTableConfig,
        *,
        store: Any | None = None,
        where: str | None = None,
        limit: int | None = None,
        presign: bool = True,
        expires_in_secs: int = 3600,
    ) -> list[ObjectRef]:
        """Asynchronously query a BigQuery Object Table and hydrate presigned ``ObjectRef`` entries."""
        effective_store = store if store is not None else self._storage_backend
        res: list[ObjectRef] = await self._core.list_object_table_refs(
            config,
            store=effective_store,
            where=where,
            limit=limit,
            presign=presign,
            expires_in_secs=expires_in_secs,
        )
        return res

    async def begin(self) -> None:
        """Begin a BigQuery multi-statement transaction session."""
        await self._core.begin()

    async def commit(self) -> None:
        """Commit the active BigQuery transaction session."""
        await self._core.commit()

    async def rollback(self) -> None:
        """Roll back the active BigQuery transaction session."""
        await self._core.rollback()

    async def close(self) -> None:
        """Close the underlying ``AsyncBigQueryDriver``."""
        await self._core.close()

    def _prepare_sql(self, sql: str, *, tenant_id: str | None = None) -> str:
        if self._tenant_predicate is not None and tenant_id is not None:
            apply_fn = getattr(self._tenant_predicate, "apply", None)
            if callable(apply_fn):
                return str(apply_fn(sql, tenant_id))
        return sql

    async def _execute_stack_item_async(
        self,
        item: str | tuple[str, Mapping[str, Any] | Sequence[Any] | None],
        *,
        tenant_id: str | None = None,
    ) -> Any:
        if isinstance(item, tuple):
            sql, params = item
        else:
            sql, params = item, None
        stripped = sql.strip().upper()
        if stripped.startswith(("SELECT ", "WITH ")):
            return (
                await self.select(sql, params, tenant_id=tenant_id)
                if params is not None
                else await self.select(sql, tenant_id=tenant_id)
            )
        return (
            await self.execute(sql, params, tenant_id=tenant_id)
            if params is not None
            else await self.execute(sql, tenant_id=tenant_id)
        )


class BigQuerySyncConfig(SyncAdapterConfig[BigQueryConfig, BigQuerySyncDriver]):
    """Synchronous SQLSpec configuration for Google Cloud BigQuery."""

    dialect: ClassVar[str] = "bigquery"
    _config_cls: ClassVar[type[BigQueryConfig]] = BigQueryConfig
    _driver_cls: ClassVar[type[_CoreSyncBigQueryDriver]] = _CoreSyncBigQueryDriver
    driver_type: ClassVar[type[BigQuerySyncDriver]] = BigQuerySyncDriver

    def __init__(
        self,
        config: BigQueryConfig | None = None,
        *,
        dsn: str | None = None,
        project_id: str = "",
        dataset_id: str | None = None,
        location: str = "US",
        rest_endpoint: str | None = None,
        storage_endpoint: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: Literal["INTERACTIVE", "BATCH"] = "INTERACTIVE",
        labels: Mapping[str, str] | None = None,
        credentials_path: str | None = None,
        service_account_key: str | dict[str, Any] | None = None,
        quota_project_id: str | None = None,
        scopes: Sequence[str] = (
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform",
        ),
        storage_backend: ObjectStoreBackend | str | None = None,
        connection_instance: Any = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = True,
        tenant_predicate: TenantPredicate | None = None,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = False,
        statement_config: Any | None = None,
    ) -> None:
        self.dsn = dsn
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location
        self.rest_endpoint = rest_endpoint
        self.storage_endpoint = storage_endpoint
        self.maximum_bytes_billed = maximum_bytes_billed
        self.job_timeout_ms = job_timeout_ms
        self.priority: Literal["INTERACTIVE", "BATCH"] = priority
        self.labels: dict[str, str] = dict(labels) if labels else {}
        self.credentials_path = credentials_path
        self.service_account_key = service_account_key
        self.quota_project_id = quota_project_id
        self.scopes: tuple[str, ...] = tuple(scopes)
        self.storage_backend = storage_backend
        self.connection_instance = connection_instance
        self.statement_cache_size = statement_cache_size
        self.enable_sqlcommenter = enable_sqlcommenter
        self.tenant_predicate = tenant_predicate
        self.config = config
        resolved_cfg = self.resolve_bigquery_config()
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=share_pool,
            statement_config=statement_config,
        )
        self.connection_instance = connection_instance

    def resolve_bigquery_config(self) -> BigQueryConfig:
        """Resolve the canonical ``BigQueryConfig`` from constructor fields and ``config``."""
        if isinstance(self.config, BigQueryConfig):
            return self.config
        if self.dsn:
            overrides: dict[str, Any] = {}
            if self.project_id:
                overrides["project_id"] = self.project_id
            if self.dataset_id:
                overrides["dataset_id"] = self.dataset_id
            if self.location != "US":
                overrides["location"] = self.location
            if self.rest_endpoint:
                overrides["rest_endpoint"] = self.rest_endpoint
            if self.storage_endpoint:
                overrides["storage_endpoint"] = self.storage_endpoint
            if self.maximum_bytes_billed is not None:
                overrides["maximum_bytes_billed"] = self.maximum_bytes_billed
            if self.job_timeout_ms is not None:
                overrides["job_timeout_ms"] = self.job_timeout_ms
            if self.priority != "INTERACTIVE":
                overrides["priority"] = self.priority
            if self.labels:
                overrides["labels"] = dict(self.labels)
            if self.credentials_path:
                overrides["credentials_path"] = self.credentials_path
            if self.service_account_key is not None:
                overrides["service_account_key"] = self.service_account_key
            if self.quota_project_id:
                overrides["quota_project_id"] = self.quota_project_id
            if self.scopes:
                overrides["scopes"] = self.scopes
            return BigQueryConfig.from_dsn(self.dsn, **overrides)
        return BigQueryConfig(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            location=self.location,
            rest_endpoint=self.rest_endpoint or "https://bigquery.googleapis.com",
            storage_endpoint=self.storage_endpoint or "https://bigquerystorage.googleapis.com",
            maximum_bytes_billed=self.maximum_bytes_billed,
            job_timeout_ms=self.job_timeout_ms,
            priority=self.priority,
            labels=dict(self.labels),
            credentials_path=self.credentials_path,
            service_account_key=self.service_account_key,
            quota_project_id=self.quota_project_id,
            scopes=self.scopes,
            statement_cache_capacity=self.statement_cache_size,
        )

    def resolve_storage_backend(self) -> ObjectStoreBackend | None:
        """Resolve ``storage_backend`` into an ``ObjectStoreBackend`` instance if configured."""
        if isinstance(self.storage_backend, ObjectStoreBackend):
            return self.storage_backend
        if isinstance(self.storage_backend, str) and self.storage_backend.strip():
            return ObjectStoreBackend(self.storage_backend.strip())
        return None

    def provide_driver(
        self,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
    ) -> BigQuerySyncDriver:
        """Instantiate a ``BigQuerySyncDriver`` bound to this configuration."""
        if isinstance(self.connection_instance, BigQuerySyncDriver):
            return self.connection_instance
        if self.connection_instance is not None:
            core = self.connection_instance
        else:
            bq_cfg = self.resolve_bigquery_config()
            core = _CoreSyncBigQueryDriver(
                bq_cfg,
                broker=broker,
                statement_cache=statement_cache,
            )
        return BigQuerySyncDriver(
            core,
            statement_config=self.statement_config,
            storage_backend=self.resolve_storage_backend(),
            statement_cache_size=self.statement_cache_size,
            enable_sqlcommenter=self.enable_sqlcommenter,
            tenant_predicate=self.tenant_predicate,
        )

    def create_driver(self) -> BigQuerySyncDriver:
        """Create or return the cached ``BigQuerySyncDriver`` instance."""
        if self._driver_instance is None:
            self._driver_instance = self.provide_driver(broker=self.broker)
        return self._driver_instance

    @contextmanager
    def provide_session(
        self,
        *args: Any,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
        **kwargs: Any,
    ) -> Iterator[BigQuerySyncDriver]:
        """Yield a connected ``BigQuerySyncDriver`` session and close it on exit."""
        _ = (args, kwargs)
        driver = self.provide_driver(broker=broker, statement_cache=statement_cache)
        driver.core_driver.connect()
        try:
            yield driver
        finally:
            if self.connection_instance is None:
                driver.close()

    def create_connection(self) -> BigQueryConnectionShim:
        """Create a zero-SDK ``BigQueryConnectionShim`` for upstream SQLSpec stores."""
        driver = self.provide_driver()
        driver.core_driver.connect()
        return BigQueryConnectionShim(driver.core_driver)


class BigQueryAsyncConfig(AsyncAdapterConfig[BigQueryConfig, BigQueryAsyncDriver]):
    """Asynchronous SQLSpec configuration for Google Cloud BigQuery."""

    dialect: ClassVar[str] = "bigquery"
    _config_cls: ClassVar[type[BigQueryConfig]] = BigQueryConfig
    _driver_cls: ClassVar[type[_CoreAsyncBigQueryDriver]] = _CoreAsyncBigQueryDriver
    driver_type: ClassVar[type[BigQueryAsyncDriver]] = BigQueryAsyncDriver

    def __init__(
        self,
        config: BigQueryConfig | None = None,
        *,
        dsn: str | None = None,
        project_id: str = "",
        dataset_id: str | None = None,
        location: str = "US",
        rest_endpoint: str | None = None,
        storage_endpoint: str | None = None,
        maximum_bytes_billed: int | None = None,
        job_timeout_ms: int | None = None,
        priority: Literal["INTERACTIVE", "BATCH"] = "INTERACTIVE",
        labels: Mapping[str, str] | None = None,
        credentials_path: str | None = None,
        service_account_key: str | dict[str, Any] | None = None,
        quota_project_id: str | None = None,
        scopes: Sequence[str] = (
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/cloud-platform",
        ),
        storage_backend: ObjectStoreBackend | str | None = None,
        connection_instance: Any = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = True,
        tenant_predicate: TenantPredicate | None = None,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = False,
        statement_config: Any | None = None,
    ) -> None:
        self.dsn = dsn
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.location = location
        self.rest_endpoint = rest_endpoint
        self.storage_endpoint = storage_endpoint
        self.maximum_bytes_billed = maximum_bytes_billed
        self.job_timeout_ms = job_timeout_ms
        self.priority: Literal["INTERACTIVE", "BATCH"] = priority
        self.labels: dict[str, str] = dict(labels) if labels else {}
        self.credentials_path = credentials_path
        self.service_account_key = service_account_key
        self.quota_project_id = quota_project_id
        self.scopes: tuple[str, ...] = tuple(scopes)
        self.storage_backend = storage_backend
        self.connection_instance = connection_instance
        self.statement_cache_size = statement_cache_size
        self.enable_sqlcommenter = enable_sqlcommenter
        self.tenant_predicate = tenant_predicate
        self.config = config
        resolved_cfg = self.resolve_bigquery_config()
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=share_pool,
            statement_config=statement_config,
        )
        self.connection_instance = connection_instance

    def resolve_bigquery_config(self) -> BigQueryConfig:
        """Resolve the canonical ``BigQueryConfig`` from constructor fields and ``config``."""
        if isinstance(self.config, BigQueryConfig):
            return self.config
        if self.dsn:
            overrides: dict[str, Any] = {}
            if self.project_id:
                overrides["project_id"] = self.project_id
            if self.dataset_id:
                overrides["dataset_id"] = self.dataset_id
            if self.location != "US":
                overrides["location"] = self.location
            if self.rest_endpoint:
                overrides["rest_endpoint"] = self.rest_endpoint
            if self.storage_endpoint:
                overrides["storage_endpoint"] = self.storage_endpoint
            if self.maximum_bytes_billed is not None:
                overrides["maximum_bytes_billed"] = self.maximum_bytes_billed
            if self.job_timeout_ms is not None:
                overrides["job_timeout_ms"] = self.job_timeout_ms
            if self.priority != "INTERACTIVE":
                overrides["priority"] = self.priority
            if self.labels:
                overrides["labels"] = dict(self.labels)
            if self.credentials_path:
                overrides["credentials_path"] = self.credentials_path
            if self.service_account_key is not None:
                overrides["service_account_key"] = self.service_account_key
            if self.quota_project_id:
                overrides["quota_project_id"] = self.quota_project_id
            if self.scopes:
                overrides["scopes"] = self.scopes
            return BigQueryConfig.from_dsn(self.dsn, **overrides)
        return BigQueryConfig(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            location=self.location,
            rest_endpoint=self.rest_endpoint or "https://bigquery.googleapis.com",
            storage_endpoint=self.storage_endpoint or "https://bigquerystorage.googleapis.com",
            maximum_bytes_billed=self.maximum_bytes_billed,
            job_timeout_ms=self.job_timeout_ms,
            priority=self.priority,
            labels=dict(self.labels),
            credentials_path=self.credentials_path,
            service_account_key=self.service_account_key,
            quota_project_id=self.quota_project_id,
            scopes=self.scopes,
            statement_cache_capacity=self.statement_cache_size,
        )

    def resolve_storage_backend(self) -> ObjectStoreBackend | None:
        """Resolve ``storage_backend`` into an ``ObjectStoreBackend`` instance if configured."""
        if isinstance(self.storage_backend, ObjectStoreBackend):
            return self.storage_backend
        if isinstance(self.storage_backend, str) and self.storage_backend.strip():
            return ObjectStoreBackend(self.storage_backend.strip())
        return None

    def provide_driver(
        self,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
    ) -> BigQueryAsyncDriver:
        """Instantiate a ``BigQueryAsyncDriver`` bound to this configuration."""
        if isinstance(self.connection_instance, BigQueryAsyncDriver):
            return self.connection_instance
        if self.connection_instance is not None:
            core = self.connection_instance
        else:
            bq_cfg = self.resolve_bigquery_config()
            core = _CoreAsyncBigQueryDriver(
                bq_cfg,
                broker=broker,
                statement_cache=statement_cache,
            )
        return BigQueryAsyncDriver(
            core,
            statement_config=self.statement_config,
            storage_backend=self.resolve_storage_backend(),
            statement_cache_size=self.statement_cache_size,
            enable_sqlcommenter=self.enable_sqlcommenter,
            tenant_predicate=self.tenant_predicate,
        )

    def create_driver(self) -> BigQueryAsyncDriver:
        """Create or return the cached ``BigQueryAsyncDriver`` instance."""
        if self._driver_instance is None:
            self._driver_instance = self.provide_driver(broker=self.broker)
        return self._driver_instance

    @asynccontextmanager
    async def provide_session(
        self,
        *args: Any,
        broker: LeaseBroker | None = None,
        statement_cache: _RustStatementCache | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[BigQueryAsyncDriver]:
        """Asynchronously yield a connected ``BigQueryAsyncDriver`` session and close it on exit."""
        _ = (args, kwargs)
        driver = self.provide_driver(broker=broker, statement_cache=statement_cache)
        await driver.core_driver.connect()
        try:
            yield driver
        finally:
            if self.connection_instance is None:
                await driver.close()

    def create_connection(self) -> Any:
        """Create a zero-SDK ``BigQueryConnectionShim`` for upstream SQLSpec stores."""
        bq_cfg = self.resolve_bigquery_config()
        sync_core = _CoreSyncBigQueryDriver(bq_cfg)
        sync_core.connect()
        return BigQueryConnectionShim(sync_core)


class BigQuerySyncADKStore:
    """Synchronous Google ADK session and event store backed by clustered BigQuery tables."""

    def __init__(
        self,
        config_or_driver: BigQuerySyncConfig | BigQuerySyncDriver | _CoreSyncBigQueryDriver,
        *,
        sessions_table: str = _DEFAULT_SESSIONS_TABLE,
        events_table: str = _DEFAULT_EVENTS_TABLE,
    ) -> None:
        self._driver = _coerce_sync_driver(config_or_driver)
        self.sessions_table = _qualify_store_table(self._driver, sessions_table)
        self.events_table = _qualify_store_table(self._driver, events_table)

    def create_tables(self) -> None:
        """Create clustered BigQuery ``sessions`` and ``events`` tables if they do not exist."""
        sessions_ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.sessions_table} ("
            f"session_id STRING NOT NULL, "
            f"user_id STRING NOT NULL, "
            f"app_name STRING NOT NULL, "
            f"state_json STRING, "
            f"created_at TIMESTAMP, "
            f"updated_at TIMESTAMP"
            f") CLUSTER BY user_id, session_id"
        )
        events_ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.events_table} ("
            f"event_id STRING NOT NULL, "
            f"session_id STRING NOT NULL, "
            f"author STRING NOT NULL, "
            f"content_json STRING, "
            f"actions_json STRING, "
            f"timestamp TIMESTAMP"
            f") CLUSTER BY session_id, timestamp"
        )
        self._driver.execute(sessions_ddl)
        self._driver.execute(events_ddl)

    def create_session(
        self,
        session_id: str,
        user_id: str,
        app_name: str = "default",
        state: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Insert a new ADK session row and return the session dictionary."""
        now_iso = _utc_now_iso()
        state_dict = dict(state) if state is not None else {}
        state_json = encode_json(state_dict)
        sql = (
            f"INSERT INTO {self.sessions_table} "
            f"(session_id, user_id, app_name, state_json, created_at, updated_at) "
            f"VALUES (@session_id, @user_id, @app_name, @state_json, @created_at, @updated_at)"
        )
        self._driver.execute(
            sql,
            {
                "session_id": session_id,
                "user_id": user_id,
                "app_name": app_name,
                "state_json": state_json,
                "created_at": now_iso,
                "updated_at": now_iso,
            },
        )
        return {
            "session_id": session_id,
            "user_id": user_id,
            "app_name": app_name,
            "state": state_dict,
            "created_at": now_iso,
            "updated_at": now_iso,
        }

    def get_session(
        self,
        session_id: str,
        user_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Fetch an ADK session by ``session_id`` (and optional ``user_id``)."""
        if user_id is not None:
            sql = (
                f"SELECT session_id, user_id, app_name, state_json, created_at, updated_at "
                f"FROM {self.sessions_table} "
                f"WHERE session_id = @session_id AND user_id = @user_id LIMIT 1"
            )
            row = self._driver.select_one_or_none(sql, {"session_id": session_id, "user_id": user_id})
        else:
            sql = (
                f"SELECT session_id, user_id, app_name, state_json, created_at, updated_at "
                f"FROM {self.sessions_table} "
                f"WHERE session_id = @session_id LIMIT 1"
            )
            row = self._driver.select_one_or_none(sql, {"session_id": session_id})
        return _hydrate_adk_session_row(row) if row is not None else None

    def update_session_state(
        self,
        session_id: str,
        state: Mapping[str, Any],
    ) -> None:
        """Update the serialized JSON state of ``session_id``."""
        now_iso = _utc_now_iso()
        sql = (
            f"UPDATE {self.sessions_table} "
            f"SET state_json = @state_json, updated_at = @updated_at "
            f"WHERE session_id = @session_id"
        )
        self._driver.execute(
            sql,
            {
                "session_id": session_id,
                "state_json": encode_json(dict(state)),
                "updated_at": now_iso,
            },
        )

    def list_sessions(
        self,
        user_id: str,
        app_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """List all ADK sessions for ``user_id`` (and optional ``app_name``)."""
        if app_name is not None:
            sql = (
                f"SELECT session_id, user_id, app_name, state_json, created_at, updated_at "
                f"FROM {self.sessions_table} "
                f"WHERE user_id = @user_id AND app_name = @app_name "
                f"ORDER BY updated_at DESC"
            )
            rows = self._driver.select(sql, {"user_id": user_id, "app_name": app_name})
        else:
            sql = (
                f"SELECT session_id, user_id, app_name, state_json, created_at, updated_at "
                f"FROM {self.sessions_table} "
                f"WHERE user_id = @user_id "
                f"ORDER BY updated_at DESC"
            )
            rows = self._driver.select(sql, {"user_id": user_id})
        return [_hydrate_adk_session_row(r) for r in rows]

    def delete_session(self, session_id: str) -> None:
        """Delete a session and all associated events by ``session_id``."""
        self._driver.execute(
            f"DELETE FROM {self.events_table} WHERE session_id = @session_id",
            {"session_id": session_id},
        )
        self._driver.execute(
            f"DELETE FROM {self.sessions_table} WHERE session_id = @session_id",
            {"session_id": session_id},
        )

    def append_event(
        self,
        session_id: str,
        event_id: str,
        author: str,
        content: Any,
        *,
        timestamp: str | None = None,
        actions: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append a turn event to ``events_table``."""
        ts = timestamp or _utc_now_iso()
        content_json = encode_json(content) if not isinstance(content, str) else content
        actions_json = encode_json(dict(actions)) if actions is not None else "{}"
        sql = (
            f"INSERT INTO {self.events_table} "
            f"(event_id, session_id, author, content_json, actions_json, timestamp) "
            f"VALUES (@event_id, @session_id, @author, @content_json, @actions_json, @timestamp)"
        )
        self._driver.execute(
            sql,
            {
                "event_id": event_id,
                "session_id": session_id,
                "author": author,
                "content_json": content_json,
                "actions_json": actions_json,
                "timestamp": ts,
            },
        )
        return {
            "event_id": event_id,
            "session_id": session_id,
            "author": author,
            "content": content,
            "actions": dict(actions) if actions is not None else {},
            "timestamp": ts,
        }

    def get_events(
        self,
        session_id: str,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return ordered turn events for ``session_id``."""
        sql = (
            f"SELECT event_id, session_id, author, content_json, actions_json, timestamp "
            f"FROM {self.events_table} "
            f"WHERE session_id = @session_id "
            f"ORDER BY timestamp ASC"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = self._driver.select(sql, {"session_id": session_id})
        return [_hydrate_adk_event_row(r) for r in rows]


class BigQueryAsyncADKStore:
    """Asynchronous Google ADK session and event store backed by clustered BigQuery tables."""

    def __init__(
        self,
        config_or_driver: BigQueryAsyncConfig | BigQueryAsyncDriver | _CoreAsyncBigQueryDriver,
        *,
        sessions_table: str = _DEFAULT_SESSIONS_TABLE,
        events_table: str = _DEFAULT_EVENTS_TABLE,
    ) -> None:
        self._driver = _coerce_async_driver(config_or_driver)
        self.sessions_table = _qualify_store_table(self._driver, sessions_table)
        self.events_table = _qualify_store_table(self._driver, events_table)

    async def create_tables(self) -> None:
        """Asynchronously create clustered BigQuery ``sessions`` and ``events`` tables."""
        sessions_ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.sessions_table} ("
            f"session_id STRING NOT NULL, "
            f"user_id STRING NOT NULL, "
            f"app_name STRING NOT NULL, "
            f"state_json STRING, "
            f"created_at TIMESTAMP, "
            f"updated_at TIMESTAMP"
            f") CLUSTER BY user_id, session_id"
        )
        events_ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.events_table} ("
            f"event_id STRING NOT NULL, "
            f"session_id STRING NOT NULL, "
            f"author STRING NOT NULL, "
            f"content_json STRING, "
            f"actions_json STRING, "
            f"timestamp TIMESTAMP"
            f") CLUSTER BY session_id, timestamp"
        )
        await self._driver.execute(sessions_ddl)
        await self._driver.execute(events_ddl)

    async def create_session(
        self,
        session_id: str,
        user_id: str,
        app_name: str = "default",
        state: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Asynchronously insert a new ADK session row."""
        now_iso = _utc_now_iso()
        state_dict = dict(state) if state is not None else {}
        state_json = encode_json(state_dict)
        sql = (
            f"INSERT INTO {self.sessions_table} "
            f"(session_id, user_id, app_name, state_json, created_at, updated_at) "
            f"VALUES (@session_id, @user_id, @app_name, @state_json, @created_at, @updated_at)"
        )
        await self._driver.execute(
            sql,
            {
                "session_id": session_id,
                "user_id": user_id,
                "app_name": app_name,
                "state_json": state_json,
                "created_at": now_iso,
                "updated_at": now_iso,
            },
        )
        return {
            "session_id": session_id,
            "user_id": user_id,
            "app_name": app_name,
            "state": state_dict,
            "created_at": now_iso,
            "updated_at": now_iso,
        }

    async def get_session(
        self,
        session_id: str,
        user_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Asynchronously fetch an ADK session by ``session_id``."""
        if user_id is not None:
            sql = (
                f"SELECT session_id, user_id, app_name, state_json, created_at, updated_at "
                f"FROM {self.sessions_table} "
                f"WHERE session_id = @session_id AND user_id = @user_id LIMIT 1"
            )
            row = await self._driver.select_one_or_none(sql, {"session_id": session_id, "user_id": user_id})
        else:
            sql = (
                f"SELECT session_id, user_id, app_name, state_json, created_at, updated_at "
                f"FROM {self.sessions_table} "
                f"WHERE session_id = @session_id LIMIT 1"
            )
            row = await self._driver.select_one_or_none(sql, {"session_id": session_id})
        return _hydrate_adk_session_row(row) if row is not None else None

    async def update_session_state(
        self,
        session_id: str,
        state: Mapping[str, Any],
    ) -> None:
        """Asynchronously update the serialized JSON state of ``session_id``."""
        now_iso = _utc_now_iso()
        sql = (
            f"UPDATE {self.sessions_table} "
            f"SET state_json = @state_json, updated_at = @updated_at "
            f"WHERE session_id = @session_id"
        )
        await self._driver.execute(
            sql,
            {
                "session_id": session_id,
                "state_json": encode_json(dict(state)),
                "updated_at": now_iso,
            },
        )

    async def list_sessions(
        self,
        user_id: str,
        app_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Asynchronously list all ADK sessions for ``user_id``."""
        if app_name is not None:
            sql = (
                f"SELECT session_id, user_id, app_name, state_json, created_at, updated_at "
                f"FROM {self.sessions_table} "
                f"WHERE user_id = @user_id AND app_name = @app_name "
                f"ORDER BY updated_at DESC"
            )
            rows = await self._driver.select(sql, {"user_id": user_id, "app_name": app_name})
        else:
            sql = (
                f"SELECT session_id, user_id, app_name, state_json, created_at, updated_at "
                f"FROM {self.sessions_table} "
                f"WHERE user_id = @user_id "
                f"ORDER BY updated_at DESC"
            )
            rows = await self._driver.select(sql, {"user_id": user_id})
        return [_hydrate_adk_session_row(r) for r in rows]

    async def delete_session(self, session_id: str) -> None:
        """Asynchronously delete a session and all associated events by ``session_id``."""
        await self._driver.execute(
            f"DELETE FROM {self.events_table} WHERE session_id = @session_id",
            {"session_id": session_id},
        )
        await self._driver.execute(
            f"DELETE FROM {self.sessions_table} WHERE session_id = @session_id",
            {"session_id": session_id},
        )

    async def append_event(
        self,
        session_id: str,
        event_id: str,
        author: str,
        content: Any,
        *,
        timestamp: str | None = None,
        actions: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Asynchronously append a turn event to ``events_table``."""
        ts = timestamp or _utc_now_iso()
        content_json = encode_json(content) if not isinstance(content, str) else content
        actions_json = encode_json(dict(actions)) if actions is not None else "{}"
        sql = (
            f"INSERT INTO {self.events_table} "
            f"(event_id, session_id, author, content_json, actions_json, timestamp) "
            f"VALUES (@event_id, @session_id, @author, @content_json, @actions_json, @timestamp)"
        )
        await self._driver.execute(
            sql,
            {
                "event_id": event_id,
                "session_id": session_id,
                "author": author,
                "content_json": content_json,
                "actions_json": actions_json,
                "timestamp": ts,
            },
        )
        return {
            "event_id": event_id,
            "session_id": session_id,
            "author": author,
            "content": content,
            "actions": dict(actions) if actions is not None else {},
            "timestamp": ts,
        }

    async def get_events(
        self,
        session_id: str,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Asynchronously return ordered turn events for ``session_id``."""
        sql = (
            f"SELECT event_id, session_id, author, content_json, actions_json, timestamp "
            f"FROM {self.events_table} "
            f"WHERE session_id = @session_id "
            f"ORDER BY timestamp ASC"
        )
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = await self._driver.select(sql, {"session_id": session_id})
        return [_hydrate_adk_event_row(r) for r in rows]


class BigQuerySyncADKMemoryStore:
    """Synchronous Google ADK long-term memory store using BigQuery ``SEARCH`` and ``VECTOR_SEARCH``."""

    def __init__(
        self,
        config_or_driver: BigQuerySyncConfig | BigQuerySyncDriver | _CoreSyncBigQueryDriver,
        *,
        memories_table: str = _DEFAULT_MEMORIES_TABLE,
    ) -> None:
        self._driver = _coerce_sync_driver(config_or_driver)
        self.memories_table = _qualify_store_table(self._driver, memories_table)

    def create_tables(self) -> None:
        """Create the clustered BigQuery ADK memory table."""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.memories_table} ("
            f"memory_id STRING NOT NULL, "
            f"user_id STRING NOT NULL, "
            f"app_name STRING NOT NULL, "
            f"content STRING, "
            f"embedding ARRAY<FLOAT64>, "
            f"metadata_json STRING, "
            f"created_at TIMESTAMP"
            f") CLUSTER BY user_id, app_name"
        )
        self._driver.execute(ddl)

    def store_memory(
        self,
        memory_id: str,
        user_id: str,
        content: str,
        *,
        app_name: str = "default",
        embedding: Sequence[float] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Store a long-term memory entry with optional vector embedding."""
        now_iso = _utc_now_iso()
        emb_list = [float(x) for x in embedding] if embedding is not None else []
        meta_dict = dict(metadata) if metadata is not None else {}
        sql = (
            f"INSERT INTO {self.memories_table} "
            f"(memory_id, user_id, app_name, content, embedding, metadata_json, created_at) "
            f"VALUES (@memory_id, @user_id, @app_name, @content, @embedding, @metadata_json, @created_at)"
        )
        self._driver.execute(
            sql,
            {
                "memory_id": memory_id,
                "user_id": user_id,
                "app_name": app_name,
                "content": content,
                "embedding": emb_list,
                "metadata_json": encode_json(meta_dict),
                "created_at": now_iso,
            },
        )
        return {
            "memory_id": memory_id,
            "user_id": user_id,
            "app_name": app_name,
            "content": content,
            "embedding": emb_list,
            "metadata": meta_dict,
            "created_at": now_iso,
        }

    def search_memory(
        self,
        user_id: str,
        *,
        query: str | None = None,
        embedding: Sequence[float] | None = None,
        app_name: str | None = None,
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        """Search memories via ``VECTOR_SEARCH`` / ``SEARCH`` with client-side fallback."""
        sql = (
            f"SELECT memory_id, user_id, app_name, content, embedding, metadata_json, created_at "
            f"FROM {self.memories_table} WHERE user_id = @user_id"
        )
        params: dict[str, Any] = {"user_id": user_id}
        if app_name is not None:
            sql += " AND app_name = @app_name"
            params["app_name"] = app_name
        rows = self._driver.select(sql, params)
        return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)


class BigQueryAsyncADKMemoryStore:
    """Asynchronous Google ADK long-term memory store using BigQuery ``SEARCH`` and ``VECTOR_SEARCH``."""

    def __init__(
        self,
        config_or_driver: BigQueryAsyncConfig | BigQueryAsyncDriver | _CoreAsyncBigQueryDriver,
        *,
        memories_table: str = _DEFAULT_MEMORIES_TABLE,
    ) -> None:
        self._driver = _coerce_async_driver(config_or_driver)
        self.memories_table = _qualify_store_table(self._driver, memories_table)

    async def create_tables(self) -> None:
        """Asynchronously create the clustered BigQuery ADK memory table."""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.memories_table} ("
            f"memory_id STRING NOT NULL, "
            f"user_id STRING NOT NULL, "
            f"app_name STRING NOT NULL, "
            f"content STRING, "
            f"embedding ARRAY<FLOAT64>, "
            f"metadata_json STRING, "
            f"created_at TIMESTAMP"
            f") CLUSTER BY user_id, app_name"
        )
        await self._driver.execute(ddl)

    async def store_memory(
        self,
        memory_id: str,
        user_id: str,
        content: str,
        *,
        app_name: str = "default",
        embedding: Sequence[float] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Asynchronously store a long-term memory entry with optional vector embedding."""
        now_iso = _utc_now_iso()
        emb_list = [float(x) for x in embedding] if embedding is not None else []
        meta_dict = dict(metadata) if metadata is not None else {}
        sql = (
            f"INSERT INTO {self.memories_table} "
            f"(memory_id, user_id, app_name, content, embedding, metadata_json, created_at) "
            f"VALUES (@memory_id, @user_id, @app_name, @content, @embedding, @metadata_json, @created_at)"
        )
        await self._driver.execute(
            sql,
            {
                "memory_id": memory_id,
                "user_id": user_id,
                "app_name": app_name,
                "content": content,
                "embedding": emb_list,
                "metadata_json": encode_json(meta_dict),
                "created_at": now_iso,
            },
        )
        return {
            "memory_id": memory_id,
            "user_id": user_id,
            "app_name": app_name,
            "content": content,
            "embedding": emb_list,
            "metadata": meta_dict,
            "created_at": now_iso,
        }

    async def search_memory(
        self,
        user_id: str,
        *,
        query: str | None = None,
        embedding: Sequence[float] | None = None,
        app_name: str | None = None,
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        """Asynchronously search memories via ``VECTOR_SEARCH`` / ``SEARCH`` with fallback."""
        sql = (
            f"SELECT memory_id, user_id, app_name, content, embedding, metadata_json, created_at "
            f"FROM {self.memories_table} WHERE user_id = @user_id"
        )
        params: dict[str, Any] = {"user_id": user_id}
        if app_name is not None:
            sql += " AND app_name = @app_name"
            params["app_name"] = app_name
        rows = await self._driver.select(sql, params)
        return _rank_memory_rows(rows, query=query, embedding=embedding, top_k=top_k)


class BigQuerySyncADKArtifactStore:
    """Synchronous Google ADK artifact store persisting metadata in BigQuery and bytes in ``ObjectStoreBackend``."""

    def __init__(
        self,
        config_or_driver: BigQuerySyncConfig | BigQuerySyncDriver | _CoreSyncBigQueryDriver,
        *,
        storage_backend: ObjectStoreBackend | str | None = None,
        artifacts_table: str = _DEFAULT_ARTIFACTS_TABLE,
    ) -> None:
        self._driver = _coerce_sync_driver(config_or_driver)
        self._storage = _resolve_artifact_storage(config_or_driver, storage_backend)
        self.artifacts_table = _qualify_store_table(self._driver, artifacts_table)

    def create_tables(self) -> None:
        """Create the clustered BigQuery ADK artifact metadata table."""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.artifacts_table} ("
            f"session_id STRING NOT NULL, "
            f"artifact_key STRING NOT NULL, "
            f"version INT64 NOT NULL, "
            f"uri STRING NOT NULL, "
            f"content_type STRING, "
            f"size INT64, "
            f"metadata_json STRING, "
            f"updated_at TIMESTAMP"
            f") CLUSTER BY session_id, artifact_key"
        )
        self._driver.execute(ddl)

    def save_artifact(
        self,
        session_id: str,
        artifact_key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        version: int = 1,
        metadata: Mapping[str, Any] | None = None,
        expires_in_secs: int = 3600,
    ) -> ObjectRef:
        """Write artifact bytes to ``ObjectStoreBackend`` and record metadata in BigQuery."""
        object_path = f"adk_artifacts/{session_id}/{artifact_key}/v{int(version)}"
        self._storage.write_bytes_sync(object_path, data)
        uri = f"{self._storage.store_uri.rstrip('/')}/{object_path}"
        now_iso = _utc_now_iso()
        sql = (
            f"INSERT INTO {self.artifacts_table} "
            f"(session_id, artifact_key, version, uri, content_type, size, metadata_json, updated_at) "
            f"VALUES (@session_id, @artifact_key, @version, @uri, @content_type, @size, @metadata_json, @updated_at)"
        )
        self._driver.execute(
            sql,
            {
                "session_id": session_id,
                "artifact_key": artifact_key,
                "version": int(version),
                "uri": uri,
                "content_type": content_type,
                "size": len(data),
                "metadata_json": encode_json(dict(metadata) if metadata else {}),
                "updated_at": now_iso,
            },
        )
        signed_url = _try_sign_sync(self._storage, object_path, expires_in_secs)
        return ObjectRef(
            uri=uri,
            version=str(version),
            content_type=content_type,
            size=len(data),
            updated=now_iso,
            signed_url=signed_url,
        )

    def get_artifact(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
        expires_in_secs: int = 3600,
    ) -> tuple[bytes, ObjectRef] | None:
        """Fetch artifact payload bytes and ``ObjectRef`` metadata."""
        ref = self.get_artifact_ref(
            session_id,
            artifact_key,
            version=version,
            expires_in_secs=expires_in_secs,
        )
        if ref is None:
            return None
        ver = ref.version or "1"
        object_path = f"adk_artifacts/{session_id}/{artifact_key}/v{ver}"
        payload = self._storage.read_bytes_sync(object_path)
        return payload, ref

    def get_artifact_ref(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
        expires_in_secs: int = 3600,
    ) -> ObjectRef | None:
        """Fetch ``ObjectRef`` metadata with a zero-RTT presigned URL without reading payload bytes."""
        if version is not None:
            sql = (
                f"SELECT uri, version, content_type, size, updated_at "
                f"FROM {self.artifacts_table} "
                f"WHERE session_id = @session_id AND artifact_key = @artifact_key AND version = @version "
                f"LIMIT 1"
            )
            row = self._driver.select_one_or_none(
                sql,
                {"session_id": session_id, "artifact_key": artifact_key, "version": int(version)},
            )
        else:
            sql = (
                f"SELECT uri, version, content_type, size, updated_at "
                f"FROM {self.artifacts_table} "
                f"WHERE session_id = @session_id AND artifact_key = @artifact_key "
                f"ORDER BY version DESC LIMIT 1"
            )
            row = self._driver.select_one_or_none(
                sql,
                {"session_id": session_id, "artifact_key": artifact_key},
            )
        if row is None:
            return None
        ver_str = str(row.get("version", 1))
        object_path = f"adk_artifacts/{session_id}/{artifact_key}/v{ver_str}"
        signed_url = _try_sign_sync(self._storage, object_path, expires_in_secs)
        return ObjectRef(
            uri=str(row.get("uri", "")),
            version=ver_str,
            content_type=row.get("content_type"),
            size=int(row.get("size", 0) or 0),
            updated=str(row.get("updated_at", "")) if row.get("updated_at") is not None else None,
            signed_url=signed_url,
        )

    def list_artifacts(
        self,
        session_id: str,
        *,
        expires_in_secs: int = 3600,
    ) -> list[ObjectRef]:
        """List all artifacts for ``session_id`` with presigned URLs."""
        sql = (
            f"SELECT artifact_key, uri, version, content_type, size, updated_at "
            f"FROM {self.artifacts_table} "
            f"WHERE session_id = @session_id "
            f"ORDER BY artifact_key ASC, version DESC"
        )
        rows = self._driver.select(sql, {"session_id": session_id})
        refs: list[ObjectRef] = []
        for row in rows:
            key = str(row.get("artifact_key", ""))
            ver_str = str(row.get("version", 1))
            object_path = f"adk_artifacts/{session_id}/{key}/v{ver_str}"
            signed_url = _try_sign_sync(self._storage, object_path, expires_in_secs)
            refs.append(
                ObjectRef(
                    uri=str(row.get("uri", "")),
                    version=ver_str,
                    content_type=row.get("content_type"),
                    size=int(row.get("size", 0) or 0),
                    updated=str(row.get("updated_at", "")) if row.get("updated_at") is not None else None,
                    signed_url=signed_url,
                )
            )
        return refs

    def delete_artifact(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
    ) -> None:
        """Delete artifact metadata from BigQuery."""
        if version is not None:
            self._driver.execute(
                f"DELETE FROM {self.artifacts_table} "
                f"WHERE session_id = @session_id AND artifact_key = @artifact_key AND version = @version",
                {"session_id": session_id, "artifact_key": artifact_key, "version": int(version)},
            )
        else:
            self._driver.execute(
                f"DELETE FROM {self.artifacts_table} WHERE session_id = @session_id AND artifact_key = @artifact_key",
                {"session_id": session_id, "artifact_key": artifact_key},
            )


class BigQueryAsyncADKArtifactStore:
    """Asynchronous Google ADK artifact store persisting metadata in BigQuery and bytes in ``ObjectStoreBackend``."""

    def __init__(
        self,
        config_or_driver: BigQueryAsyncConfig | BigQueryAsyncDriver | _CoreAsyncBigQueryDriver,
        *,
        storage_backend: ObjectStoreBackend | str | None = None,
        artifacts_table: str = _DEFAULT_ARTIFACTS_TABLE,
    ) -> None:
        self._driver = _coerce_async_driver(config_or_driver)
        self._storage = _resolve_artifact_storage(config_or_driver, storage_backend)
        self.artifacts_table = _qualify_store_table(self._driver, artifacts_table)

    async def create_tables(self) -> None:
        """Asynchronously create the clustered BigQuery ADK artifact metadata table."""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.artifacts_table} ("
            f"session_id STRING NOT NULL, "
            f"artifact_key STRING NOT NULL, "
            f"version INT64 NOT NULL, "
            f"uri STRING NOT NULL, "
            f"content_type STRING, "
            f"size INT64, "
            f"metadata_json STRING, "
            f"updated_at TIMESTAMP"
            f") CLUSTER BY session_id, artifact_key"
        )
        await self._driver.execute(ddl)

    async def save_artifact(
        self,
        session_id: str,
        artifact_key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        version: int = 1,
        metadata: Mapping[str, Any] | None = None,
        expires_in_secs: int = 3600,
    ) -> ObjectRef:
        """Asynchronously write artifact bytes to ``ObjectStoreBackend`` and record metadata in BigQuery."""
        object_path = f"adk_artifacts/{session_id}/{artifact_key}/v{int(version)}"
        await self._storage.write_bytes_async(object_path, data)
        uri = f"{self._storage.store_uri.rstrip('/')}/{object_path}"
        now_iso = _utc_now_iso()
        sql = (
            f"INSERT INTO {self.artifacts_table} "
            f"(session_id, artifact_key, version, uri, content_type, size, metadata_json, updated_at) "
            f"VALUES (@session_id, @artifact_key, @version, @uri, @content_type, @size, @metadata_json, @updated_at)"
        )
        await self._driver.execute(
            sql,
            {
                "session_id": session_id,
                "artifact_key": artifact_key,
                "version": int(version),
                "uri": uri,
                "content_type": content_type,
                "size": len(data),
                "metadata_json": encode_json(dict(metadata) if metadata else {}),
                "updated_at": now_iso,
            },
        )
        signed_url = await _try_sign_async(self._storage, object_path, expires_in_secs)
        return ObjectRef(
            uri=uri,
            version=str(version),
            content_type=content_type,
            size=len(data),
            updated=now_iso,
            signed_url=signed_url,
        )

    async def get_artifact(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
        expires_in_secs: int = 3600,
    ) -> tuple[bytes, ObjectRef] | None:
        """Asynchronously fetch artifact payload bytes and ``ObjectRef`` metadata."""
        ref = await self.get_artifact_ref(
            session_id,
            artifact_key,
            version=version,
            expires_in_secs=expires_in_secs,
        )
        if ref is None:
            return None
        ver = ref.version or "1"
        object_path = f"adk_artifacts/{session_id}/{artifact_key}/v{ver}"
        payload = await self._storage.read_bytes_async(object_path)
        return payload, ref

    async def get_artifact_ref(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
        expires_in_secs: int = 3600,
    ) -> ObjectRef | None:
        """Asynchronously fetch ``ObjectRef`` metadata with a zero-RTT presigned URL."""
        if version is not None:
            sql = (
                f"SELECT uri, version, content_type, size, updated_at "
                f"FROM {self.artifacts_table} "
                f"WHERE session_id = @session_id AND artifact_key = @artifact_key AND version = @version "
                f"LIMIT 1"
            )
            row = await self._driver.select_one_or_none(
                sql,
                {"session_id": session_id, "artifact_key": artifact_key, "version": int(version)},
            )
        else:
            sql = (
                f"SELECT uri, version, content_type, size, updated_at "
                f"FROM {self.artifacts_table} "
                f"WHERE session_id = @session_id AND artifact_key = @artifact_key "
                f"ORDER BY version DESC LIMIT 1"
            )
            row = await self._driver.select_one_or_none(
                sql,
                {"session_id": session_id, "artifact_key": artifact_key},
            )
        if row is None:
            return None
        ver_str = str(row.get("version", 1))
        object_path = f"adk_artifacts/{session_id}/{artifact_key}/v{ver_str}"
        signed_url = await _try_sign_async(self._storage, object_path, expires_in_secs)
        return ObjectRef(
            uri=str(row.get("uri", "")),
            version=ver_str,
            content_type=row.get("content_type"),
            size=int(row.get("size", 0) or 0),
            updated=str(row.get("updated_at", "")) if row.get("updated_at") is not None else None,
            signed_url=signed_url,
        )

    async def list_artifacts(
        self,
        session_id: str,
        *,
        expires_in_secs: int = 3600,
    ) -> list[ObjectRef]:
        """Asynchronously list all artifacts for ``session_id`` with presigned URLs."""
        sql = (
            f"SELECT artifact_key, uri, version, content_type, size, updated_at "
            f"FROM {self.artifacts_table} "
            f"WHERE session_id = @session_id "
            f"ORDER BY artifact_key ASC, version DESC"
        )
        rows = await self._driver.select(sql, {"session_id": session_id})
        refs: list[ObjectRef] = []
        for row in rows:
            key = str(row.get("artifact_key", ""))
            ver_str = str(row.get("version", 1))
            object_path = f"adk_artifacts/{session_id}/{key}/v{ver_str}"
            signed_url = await _try_sign_async(self._storage, object_path, expires_in_secs)
            refs.append(
                ObjectRef(
                    uri=str(row.get("uri", "")),
                    version=ver_str,
                    content_type=row.get("content_type"),
                    size=int(row.get("size", 0) or 0),
                    updated=str(row.get("updated_at", "")) if row.get("updated_at") is not None else None,
                    signed_url=signed_url,
                )
            )
        return refs

    async def delete_artifact(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
    ) -> None:
        """Asynchronously delete artifact metadata from BigQuery."""
        if version is not None:
            await self._driver.execute(
                f"DELETE FROM {self.artifacts_table} "
                f"WHERE session_id = @session_id AND artifact_key = @artifact_key AND version = @version",
                {"session_id": session_id, "artifact_key": artifact_key, "version": int(version)},
            )
        else:
            await self._driver.execute(
                f"DELETE FROM {self.artifacts_table} WHERE session_id = @session_id AND artifact_key = @artifact_key",
                {"session_id": session_id, "artifact_key": artifact_key},
            )


class BigQuerySyncStore:
    """Synchronous Litestar key-value / session store backed by BigQuery ``MERGE`` upserts."""

    def __init__(
        self,
        config_or_driver: BigQuerySyncConfig | BigQuerySyncDriver | _CoreSyncBigQueryDriver,
        *,
        table_name: str = _DEFAULT_KV_STORE_TABLE,
    ) -> None:
        self._driver = _coerce_sync_driver(config_or_driver)
        self.table_name = _qualify_store_table(self._driver, table_name)

    def create_tables(self) -> None:
        """Create the clustered BigQuery key-value store table."""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
            f"store_key STRING NOT NULL, "
            f"value_bytes BYTES, "
            f"expires_at TIMESTAMP"
            f") CLUSTER BY store_key"
        )
        self._driver.execute(ddl)

    def set(
        self,
        key: str,
        value: bytes | str,
        *,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Upsert ``key`` and ``value`` into BigQuery using a single ``MERGE`` statement."""
        raw_bytes = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        expires_iso = _compute_expires_iso(expires_in)
        sql = (
            f"MERGE {self.table_name} AS target "
            f"USING (SELECT @store_key AS store_key, @value_bytes AS value_bytes, @expires_at AS expires_at) AS source "
            f"ON target.store_key = source.store_key "
            f"WHEN MATCHED THEN UPDATE SET value_bytes = source.value_bytes, expires_at = source.expires_at "
            f"WHEN NOT MATCHED THEN INSERT (store_key, value_bytes, expires_at) "
            f"VALUES (source.store_key, source.value_bytes, source.expires_at)"
        )
        self._driver.execute(
            sql,
            {
                "store_key": key,
                "value_bytes": raw_bytes,
                "expires_at": expires_iso,
            },
        )

    def get(
        self,
        key: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> bytes | None:
        """Retrieve non-expired bytes for ``key``, optionally renewing its expiration."""
        sql = f"SELECT value_bytes, expires_at FROM {self.table_name} WHERE store_key = @store_key LIMIT 1"
        row = self._driver.select_one_or_none(sql, {"store_key": key})
        if row is None:
            return None
        if _is_expired(row.get("expires_at")):
            self.delete(key)
            return None
        raw = _coerce_stored_bytes(row.get("value_bytes"))
        if renew_for is not None and raw is not None:
            self.set(key, raw, expires_in=renew_for)
        return raw

    def delete(self, key: str) -> None:
        """Delete ``key`` from the store."""
        self._driver.execute(
            f"DELETE FROM {self.table_name} WHERE store_key = @store_key",
            {"store_key": key},
        )

    def delete_all(self) -> None:
        """Delete all entries from the store table."""
        self._driver.execute(f"DELETE FROM {self.table_name} WHERE TRUE")

    def delete_expired(self) -> None:
        """Delete all expired entries from the store table."""
        now_iso = _utc_now_iso()
        self._driver.execute(
            f"DELETE FROM {self.table_name} WHERE expires_at IS NOT NULL AND expires_at <= @now",
            {"now": now_iso},
        )

    def exists(self, key: str) -> bool:
        """Return whether a non-expired entry exists for ``key``."""
        return self.get(key) is not None

    def expires_in(self, key: str) -> int | None:
        """Return remaining TTL seconds for ``key``, or ``None`` if unset or missing."""
        sql = f"SELECT expires_at FROM {self.table_name} WHERE store_key = @store_key LIMIT 1"
        row = self._driver.select_one_or_none(sql, {"store_key": key})
        if row is None:
            return None
        return _remaining_seconds(row.get("expires_at"))


class BigQueryAsyncStore:
    """Asynchronous Litestar key-value / session store backed by BigQuery ``MERGE`` upserts."""

    def __init__(
        self,
        config_or_driver: BigQueryAsyncConfig | BigQueryAsyncDriver | _CoreAsyncBigQueryDriver,
        *,
        table_name: str = _DEFAULT_KV_STORE_TABLE,
    ) -> None:
        self._driver = _coerce_async_driver(config_or_driver)
        self.table_name = _qualify_store_table(self._driver, table_name)

    async def create_tables(self) -> None:
        """Asynchronously create the clustered BigQuery key-value store table."""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
            f"store_key STRING NOT NULL, "
            f"value_bytes BYTES, "
            f"expires_at TIMESTAMP"
            f") CLUSTER BY store_key"
        )
        await self._driver.execute(ddl)

    async def set(
        self,
        key: str,
        value: bytes | str,
        *,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Asynchronously upsert ``key`` and ``value`` into BigQuery using ``MERGE``."""
        raw_bytes = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        expires_iso = _compute_expires_iso(expires_in)
        sql = (
            f"MERGE {self.table_name} AS target "
            f"USING (SELECT @store_key AS store_key, @value_bytes AS value_bytes, @expires_at AS expires_at) AS source "
            f"ON target.store_key = source.store_key "
            f"WHEN MATCHED THEN UPDATE SET value_bytes = source.value_bytes, expires_at = source.expires_at "
            f"WHEN NOT MATCHED THEN INSERT (store_key, value_bytes, expires_at) "
            f"VALUES (source.store_key, source.value_bytes, source.expires_at)"
        )
        await self._driver.execute(
            sql,
            {
                "store_key": key,
                "value_bytes": raw_bytes,
                "expires_at": expires_iso,
            },
        )

    async def get(
        self,
        key: str,
        *,
        renew_for: int | timedelta | None = None,
    ) -> bytes | None:
        """Asynchronously retrieve non-expired bytes for ``key``."""
        sql = f"SELECT value_bytes, expires_at FROM {self.table_name} WHERE store_key = @store_key LIMIT 1"
        row = await self._driver.select_one_or_none(sql, {"store_key": key})
        if row is None:
            return None
        if _is_expired(row.get("expires_at")):
            await self.delete(key)
            return None
        raw = _coerce_stored_bytes(row.get("value_bytes"))
        if renew_for is not None and raw is not None:
            await self.set(key, raw, expires_in=renew_for)
        return raw

    async def delete(self, key: str) -> None:
        """Asynchronously delete ``key`` from the store."""
        await self._driver.execute(
            f"DELETE FROM {self.table_name} WHERE store_key = @store_key",
            {"store_key": key},
        )

    async def delete_all(self) -> None:
        """Asynchronously delete all entries from the store table."""
        await self._driver.execute(f"DELETE FROM {self.table_name} WHERE TRUE")

    async def delete_expired(self) -> None:
        """Asynchronously delete all expired entries from the store table."""
        now_iso = _utc_now_iso()
        await self._driver.execute(
            f"DELETE FROM {self.table_name} WHERE expires_at IS NOT NULL AND expires_at <= @now",
            {"now": now_iso},
        )

    async def exists(self, key: str) -> bool:
        """Asynchronously return whether a non-expired entry exists for ``key``."""
        return (await self.get(key)) is not None

    async def expires_in(self, key: str) -> int | None:
        """Asynchronously return remaining TTL seconds for ``key``."""
        sql = f"SELECT expires_at FROM {self.table_name} WHERE store_key = @store_key LIMIT 1"
        row = await self._driver.select_one_or_none(sql, {"store_key": key})
        if row is None:
            return None
        return _remaining_seconds(row.get("expires_at"))


class BigQuerySyncEventQueueStore:
    """Synchronous SQLSpec event queue store backed by BigQuery."""

    def __init__(
        self,
        config_or_driver: BigQuerySyncConfig | BigQuerySyncDriver | _CoreSyncBigQueryDriver,
        *,
        queue_table: str = _DEFAULT_EVENT_QUEUE_TABLE,
    ) -> None:
        self._driver = _coerce_sync_driver(config_or_driver)
        self.queue_table = _qualify_store_table(self._driver, queue_table)

    def create_tables(self) -> None:
        """Create the clustered BigQuery event queue table."""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.queue_table} ("
            f"event_id STRING NOT NULL, "
            f"channel STRING NOT NULL, "
            f"payload_json STRING NOT NULL, "
            f"status STRING NOT NULL, "
            f"published_at TIMESTAMP"
            f") CLUSTER BY channel, status, published_at"
        )
        self._driver.execute(ddl)

    def publish(
        self,
        channel: str,
        payload: Mapping[str, Any] | str,
        *,
        event_id: str | None = None,
    ) -> str:
        """Publish an event to ``channel`` and return its ``event_id``."""
        eid = event_id or str(uuid4())
        payload_json = payload if isinstance(payload, str) else encode_json(dict(payload))
        now_iso = _utc_now_iso()
        sql = (
            f"INSERT INTO {self.queue_table} "
            f"(event_id, channel, payload_json, status, published_at) "
            f"VALUES (@event_id, @channel, @payload_json, 'PENDING', @published_at)"
        )
        self._driver.execute(
            sql,
            {
                "event_id": eid,
                "channel": channel,
                "payload_json": payload_json,
                "published_at": now_iso,
            },
        )
        return eid

    def consume(
        self,
        channel: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Fetch up to ``limit`` pending events from ``channel``."""
        sql = (
            f"SELECT event_id, channel, payload_json, status, published_at "
            f"FROM {self.queue_table} "
            f"WHERE channel = @channel AND status = 'PENDING' "
            f"ORDER BY published_at ASC LIMIT {int(limit)}"
        )
        rows = self._driver.select(sql, {"channel": channel})
        return [_hydrate_queue_event_row(r) for r in rows]

    def ack(self, event_id: str) -> None:
        """Mark ``event_id`` as acknowledged."""
        self._driver.execute(
            f"UPDATE {self.queue_table} SET status = 'ACKED' WHERE event_id = @event_id",
            {"event_id": event_id},
        )


class BigQueryAsyncEventQueueStore:
    """Asynchronous SQLSpec event queue store backed by BigQuery."""

    def __init__(
        self,
        config_or_driver: BigQueryAsyncConfig | BigQueryAsyncDriver | _CoreAsyncBigQueryDriver,
        *,
        queue_table: str = _DEFAULT_EVENT_QUEUE_TABLE,
    ) -> None:
        self._driver = _coerce_async_driver(config_or_driver)
        self.queue_table = _qualify_store_table(self._driver, queue_table)

    async def create_tables(self) -> None:
        """Asynchronously create the clustered BigQuery event queue table."""
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {self.queue_table} ("
            f"event_id STRING NOT NULL, "
            f"channel STRING NOT NULL, "
            f"payload_json STRING NOT NULL, "
            f"status STRING NOT NULL, "
            f"published_at TIMESTAMP"
            f") CLUSTER BY channel, status, published_at"
        )
        await self._driver.execute(ddl)

    async def publish(
        self,
        channel: str,
        payload: Mapping[str, Any] | str,
        *,
        event_id: str | None = None,
    ) -> str:
        """Asynchronously publish an event to ``channel`` and return its ``event_id``."""
        eid = event_id or str(uuid4())
        payload_json = payload if isinstance(payload, str) else encode_json(dict(payload))
        now_iso = _utc_now_iso()
        sql = (
            f"INSERT INTO {self.queue_table} "
            f"(event_id, channel, payload_json, status, published_at) "
            f"VALUES (@event_id, @channel, @payload_json, 'PENDING', @published_at)"
        )
        await self._driver.execute(
            sql,
            {
                "event_id": eid,
                "channel": channel,
                "payload_json": payload_json,
                "published_at": now_iso,
            },
        )
        return eid

    async def consume(
        self,
        channel: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Asynchronously fetch up to ``limit`` pending events from ``channel``."""
        sql = (
            f"SELECT event_id, channel, payload_json, status, published_at "
            f"FROM {self.queue_table} "
            f"WHERE channel = @channel AND status = 'PENDING' "
            f"ORDER BY published_at ASC LIMIT {int(limit)}"
        )
        rows = await self._driver.select(sql, {"channel": channel})
        return [_hydrate_queue_event_row(r) for r in rows]

    async def ack(self, event_id: str) -> None:
        """Asynchronously mark ``event_id`` as acknowledged."""
        await self._driver.execute(
            f"UPDATE {self.queue_table} SET status = 'ACKED' WHERE event_id = @event_id",
            {"event_id": event_id},
        )


def _coerce_sync_driver(
    config_or_driver: Any,
) -> BigQuerySyncDriver:
    if isinstance(config_or_driver, BigQuerySyncDriver):
        return config_or_driver
    if isinstance(config_or_driver, BigQuerySyncConfig):
        return config_or_driver.provide_driver()
    if isinstance(config_or_driver, _CoreSyncBigQueryDriver) or hasattr(config_or_driver, "select"):
        return BigQuerySyncDriver(config_or_driver)
    raise ImproperConfigurationError(
        f"Expected BigQuerySyncConfig or BigQuerySyncDriver, got {type(config_or_driver)!r}"
    )


def _coerce_async_driver(
    config_or_driver: Any,
) -> BigQueryAsyncDriver:
    if isinstance(config_or_driver, BigQueryAsyncDriver):
        return config_or_driver
    if isinstance(config_or_driver, BigQueryAsyncConfig):
        return config_or_driver.provide_driver()
    if isinstance(config_or_driver, _CoreAsyncBigQueryDriver) or hasattr(config_or_driver, "select"):
        return BigQueryAsyncDriver(config_or_driver)
    raise ImproperConfigurationError(
        f"Expected BigQueryAsyncConfig or BigQueryAsyncDriver, got {type(config_or_driver)!r}"
    )


def _resolve_artifact_storage(
    config_or_driver: Any,
    storage_backend: ObjectStoreBackend | str | None,
) -> ObjectStoreBackend:
    if isinstance(storage_backend, ObjectStoreBackend):
        return storage_backend
    if isinstance(storage_backend, str) and storage_backend.strip():
        return ObjectStoreBackend(storage_backend.strip())
    if (
        isinstance(config_or_driver, (BigQuerySyncDriver, BigQueryAsyncDriver))
        and config_or_driver.storage_backend is not None
    ):
        return config_or_driver.storage_backend
    if isinstance(config_or_driver, (BigQuerySyncConfig, BigQueryAsyncConfig)):
        resolved = config_or_driver.resolve_storage_backend()
        if resolved is not None:
            return resolved
    return ObjectStoreBackend("memory://adk-artifacts")


def _qualify_store_table(
    driver: BigQuerySyncDriver | BigQueryAsyncDriver,
    table_name: str,
) -> str:
    parts = split_qualified_identifier(table_name)
    if len(parts) > 1:
        return quote_backtick_identifier(".".join(parts))
    stripped = parts[0] if parts else table_name.strip().strip("`")
    ds = driver.core_driver.dataset_id
    if ds:
        return quote_backtick_identifier(f"{ds}.{stripped}")
    return quote_backtick_identifier(stripped)


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _parse_json_safe(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    if isinstance(raw, (bytes, bytearray, memoryview)):
        try:
            return decode_json(bytes(raw))
        except Exception:
            return default
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return default
        try:
            return decode_json(stripped)
        except Exception:
            return raw
    return default


def _hydrate_adk_session_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "session_id": str(row.get("session_id", "")),
        "user_id": str(row.get("user_id", "")),
        "app_name": str(row.get("app_name", "default")),
        "state": _parse_json_safe(row.get("state_json"), {}),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _hydrate_adk_event_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "event_id": str(row.get("event_id", "")),
        "session_id": str(row.get("session_id", "")),
        "author": str(row.get("author", "")),
        "content": _parse_json_safe(row.get("content_json"), row.get("content_json")),
        "actions": _parse_json_safe(row.get("actions_json"), {}),
        "timestamp": row.get("timestamp"),
    }


def _hydrate_queue_event_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "event_id": str(row.get("event_id", "")),
        "channel": str(row.get("channel", "")),
        "payload": _parse_json_safe(row.get("payload_json"), {}),
        "status": str(row.get("status", "PENDING")),
        "published_at": row.get("published_at"),
    }


def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
    if not vec_a or not vec_b or len(vec_a) != len(vec_b):
        return 0.0
    dot = sum(float(a) * float(b) for a, b in zip(vec_a, vec_b, strict=False))
    norm_a = math.sqrt(sum(float(a) * float(a) for a in vec_a))
    norm_b = math.sqrt(sum(float(b) * float(b) for b in vec_b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


def _rank_memory_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    query: str | None,
    embedding: Sequence[float] | None,
    top_k: int,
) -> list[dict[str, Any]]:
    scored: list[tuple[float, dict[str, Any]]] = []
    q_lower = query.strip().lower() if query else ""
    for row in rows:
        content = str(row.get("content") or "")
        raw_emb = row.get("embedding")
        row_emb = [float(x) for x in raw_emb] if isinstance(raw_emb, (list, tuple)) else []
        score = 0.0
        if embedding is not None and row_emb:
            score += _cosine_similarity(embedding, row_emb)
        if q_lower:
            if q_lower in content.lower():
                score += 1.0
            else:
                tokens = [t for t in q_lower.split() if t]
                if tokens:
                    matched = sum(1 for t in tokens if t in content.lower())
                    score += matched / len(tokens)
        if not q_lower and embedding is None:
            score = 1.0
        if q_lower and embedding is None and score == 0.0:
            continue
        scored.append(
            (
                score,
                {
                    "memory_id": str(row.get("memory_id", "")),
                    "user_id": str(row.get("user_id", "")),
                    "app_name": str(row.get("app_name", "default")),
                    "content": content,
                    "embedding": row_emb,
                    "metadata": _parse_json_safe(row.get("metadata_json"), {}),
                    "score": round(score, 6),
                    "created_at": row.get("created_at"),
                },
            )
        )
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return [item for _, item in scored[: max(1, int(top_k))]]


def _try_sign_sync(storage: ObjectStoreBackend, path: str, expires_in_secs: int) -> str | None:
    try:
        return storage.sign_sync(path, expires_in=expires_in_secs)
    except Exception:
        return f"{storage.store_uri.rstrip('/')}/{path.lstrip('/')}?expires={int(expires_in_secs)}"


async def _try_sign_async(storage: ObjectStoreBackend, path: str, expires_in_secs: int) -> str | None:
    try:
        return await storage.sign_async(path, expires_in=expires_in_secs)
    except Exception:
        return f"{storage.store_uri.rstrip('/')}/{path.lstrip('/')}?expires={int(expires_in_secs)}"


def _compute_expires_iso(expires_in: int | timedelta | None) -> str | None:
    if expires_in is None:
        return None
    seconds = expires_in.total_seconds() if isinstance(expires_in, timedelta) else float(expires_in)
    return (datetime.now(tz=UTC) + timedelta(seconds=seconds)).isoformat()


def _parse_timestamp_epoch(raw: Any) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        val = float(raw)
        return val / 1_000_000.0 if val > 1e12 else val
    if isinstance(raw, datetime):
        return raw.timestamp()
    if isinstance(raw, str):
        stripped = raw.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            pass
        try:
            normalized = stripped[:-1] + "+00:00" if stripped.endswith("Z") else stripped
            dt = datetime.fromisoformat(normalized)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.timestamp()
        except ValueError:
            return None
    return None


def _is_expired(expires_at: Any) -> bool:
    epoch = _parse_timestamp_epoch(expires_at)
    if epoch is None:
        return False
    return epoch <= time.time()


def _remaining_seconds(expires_at: Any) -> int | None:
    epoch = _parse_timestamp_epoch(expires_at)
    if epoch is None:
        return None
    rem = int(round(epoch - time.time()))
    return max(0, rem)


def _coerce_stored_bytes(raw: Any) -> bytes | None:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray, memoryview)):
        return bytes(raw)
    if isinstance(raw, str):
        return raw.encode("utf-8")
    return bytes(raw)
