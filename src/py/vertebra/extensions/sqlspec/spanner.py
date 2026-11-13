"""SQLSpec adapter, data dictionary, and zero-SDK sub-extension stores for Google Cloud Spanner."""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager, suppress
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, Final, Literal, Self, cast

from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig

from vertebra._vertebra import LeaseBroker, StatementCache
from vertebra.db.adapters.bigquery import ObjectRef
from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    AsyncSpannerSnapshot,
    Mutation,
    MutationGroup,
    SpannerConfig,
    StalenessBound,
    SyncSpannerDriver,
    SyncSpannerSnapshot,
)
from vertebra.db.driver import NotSupportedError
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
from vertebra.utils.uuids import uuid4

__all__ = (
    "SpannerAsyncADKArtifactStore",
    "SpannerAsyncADKMemoryStore",
    "SpannerAsyncADKStore",
    "SpannerAsyncConfig",
    "SpannerAsyncDriver",
    "SpannerAsyncEventQueueStore",
    "SpannerAsyncStore",
    "SpannerConnectionBatch",
    "SpannerConnectionShim",
    "SpannerAsyncDataDictionary",
    "SpannerSyncDataDictionary",
    "SpannerSyncADKArtifactStore",
    "SpannerSyncADKMemoryStore",
    "SpannerSyncADKStore",
    "SpannerSyncConfig",
    "SpannerSyncDriver",
    "SpannerSyncEventQueueStore",
    "SpannerSyncStore",
)

_DEFAULT_SESSIONS_TABLE: Final[str] = "vertebra_adk_sessions"
_DEFAULT_EVENTS_TABLE: Final[str] = "vertebra_adk_events"
_DEFAULT_MEMORIES_TABLE: Final[str] = "vertebra_adk_memories"
_DEFAULT_ARTIFACTS_TABLE: Final[str] = "vertebra_adk_artifacts"
_DEFAULT_KV_STORE_TABLE: Final[str] = "vertebra_kv_store"
_DEFAULT_EVENT_QUEUE_TABLE: Final[str] = "vertebra_event_queue"


def _farm_fingerprint_shard(value: str, shard_count: int = 16) -> int:
    """Compute a deterministic non-negative shard identifier emulating ``MOD(ABS(FARM_FINGERPRINT(key)), N)``."""
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    raw = int.from_bytes(digest, "little", signed=False)
    return raw % max(1, shard_count)


class SpannerConnectionBatch:
    """Context manager emulating ``google.cloud.spanner_v1.database.Batch`` over ``Mutation``."""

    def __init__(self, driver: SyncSpannerDriver) -> None:
        self._driver = driver
        self._mutations: list[Mutation] = []

    def insert(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer an ``insert`` mutation."""
        self._mutations.append(Mutation.insert(table, columns, values))

    def update(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer an ``update`` mutation."""
        self._mutations.append(Mutation.update(table, columns, values))

    def insert_or_update(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer an ``insert_or_update`` mutation."""
        self._mutations.append(Mutation.insert_or_update(table, columns, values))

    def upsert(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer an ``insert_or_update`` mutation (alias for ``insert_or_update``)."""
        self._mutations.append(Mutation.insert_or_update(table, columns, values))

    def replace(self, table: str, columns: Sequence[str], values: Sequence[Sequence[Any]]) -> None:
        """Buffer a ``replace`` mutation."""
        self._mutations.append(Mutation.replace(table, columns, values))

    def delete(self, table: str, keys: Sequence[Any]) -> None:
        """Buffer a ``delete`` mutation."""
        self._mutations.append(Mutation.delete(table, keys))

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None and self._mutations:
            self._driver.apply_mutations(self._mutations)


class SpannerConnectionShim:
    """Drop-in protocol shim for ``google.cloud.spanner_v1.database.Database`` backed by ``SyncSpannerDriver``.

    Allows upstream ``sqlspec.adapters.spanner.*`` stores and user code expecting
    a database/client connection to execute ``run_in_transaction``, ``snapshot``,
    ``batch``, ``update_ddl``, and ``execute_sql`` without importing
    ``google-cloud-spanner``.
    """

    def __init__(self, driver: SyncSpannerDriver) -> None:
        self._driver = driver

    @property
    def driver(self) -> SyncSpannerDriver:
        """Return the underlying synchronous Spanner driver."""
        return self._driver

    @property
    def database_path(self) -> str:
        """Return the fully qualified Spanner database resource path."""
        return self._driver.config.database_path

    @property
    def project_id(self) -> str:
        """Return the GCP project identifier."""
        return self._driver.config.project_id

    @property
    def instance_id(self) -> str:
        """Return the Spanner instance identifier."""
        return self._driver.config.instance_id

    @property
    def database_id(self) -> str:
        """Return the Spanner database identifier."""
        return self._driver.config.database_id

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the active Spanner SQL dialect."""
        return self._driver.config.dialect

    def run_in_transaction(
        self,
        fn: Any,
        *,
        tag: str = "",
        max_commit_delay_ms: int | None = None,
    ) -> Any:
        """Execute ``fn(transaction)`` inside an atomic ReadWrite Spanner transaction."""
        return self._driver.run_in_transaction(fn, tag=tag, max_commit_delay_ms=max_commit_delay_ms)

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> SyncSpannerSnapshot:
        """Create a lock-free ReadOnly ``SyncSpannerSnapshot``."""
        return self._driver.snapshot(staleness, multi_use=multi_use)

    def batch(self) -> SpannerConnectionBatch:
        """Return a ``SpannerConnectionBatch`` context manager for buffered mutations."""
        return SpannerConnectionBatch(self._driver)

    def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Execute one or more schema DDL statements."""
        return self._driver.update_ddl(statements)

    def execute_sql(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Execute a SQL statement and return materialized row dictionaries."""
        del kwargs
        stripped = sql.strip().upper()
        if stripped.startswith(("SELECT", "WITH", "GRAPH")):
            return self._driver.select(sql, params)
        self._driver.execute(sql, params)
        return []

    def query(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Alias of ``execute_sql`` for upstream SQLSpec compatibility."""
        return self.execute_sql(sql, params, **kwargs)

    def close(self) -> None:
        """Close the underlying Spanner driver."""
        self._driver.close()


class _BaseSpannerDataDictionary:
    """Introspects Spanner ``INFORMATION_SCHEMA`` across GoogleSQL and Spangres dialects.

    Includes interleaved parent-child table hierarchy metadata
    (``PARENT_TABLE_NAME`` and ``ON_DELETE_ACTION``).
    """

    def __init__(
        self,
        driver: Any,
        *,
        dialect: Literal["googlesql", "spangres"] | None = None,
    ) -> None:
        self._driver = driver
        inferred = dialect or getattr(driver, "dialect", None) or "googlesql"
        self._dialect: Literal["googlesql", "spangres"] = (
            "spangres" if str(inferred).lower() in {"spangres", "postgres", "postgresql"} else "googlesql"
        )

    @property
    def dialect(self) -> Literal["googlesql", "spangres"]:
        """Return the Spanner SQL dialect used for ``INFORMATION_SCHEMA`` queries."""
        return self._dialect

    def build_tables_sql(self, schema: str | None = None) -> tuple[str, dict[str, Any]]:
        """Build the ``INFORMATION_SCHEMA.TABLES`` query including ``PARENT_TABLE_NAME`` and ``ON_DELETE_ACTION``."""
        if self._dialect == "spangres":
            target_schema = schema or "public"
            sql = (
                "SELECT table_catalog, table_schema, table_name, table_type, "
                "parent_table_name, on_delete_action, spanner_state "
                "FROM information_schema.tables "
                "WHERE table_schema = $1 "
                "ORDER BY table_name"
            )
            return sql, {"p1": target_schema}
        target_schema = schema if schema is not None else ""
        sql = (
            "SELECT TABLE_CATALOG AS table_catalog, TABLE_SCHEMA AS table_schema, "
            "TABLE_NAME AS table_name, TABLE_TYPE AS table_type, "
            "PARENT_TABLE_NAME AS parent_table_name, ON_DELETE_ACTION AS on_delete_action, "
            "SPANNER_STATE AS spanner_state "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = @schema "
            "ORDER BY TABLE_NAME"
        )
        return sql, {"schema": target_schema}

    def build_interleaved_tables_sql(self, parent_table: str | None = None) -> tuple[str, dict[str, Any]]:
        """Build a query returning interleaved child tables and their ``ON_DELETE_ACTION``."""
        if self._dialect == "spangres":
            if parent_table:
                sql = (
                    "SELECT table_name, parent_table_name, on_delete_action "
                    "FROM information_schema.tables "
                    "WHERE parent_table_name = $1 "
                    "ORDER BY table_name"
                )
                return sql, {"p1": parent_table}
            sql = (
                "SELECT table_name, parent_table_name, on_delete_action "
                "FROM information_schema.tables "
                "WHERE parent_table_name IS NOT NULL "
                "ORDER BY table_name"
            )
            return sql, {}
        if parent_table:
            sql = (
                "SELECT TABLE_NAME AS table_name, PARENT_TABLE_NAME AS parent_table_name, "
                "ON_DELETE_ACTION AS on_delete_action "
                "FROM INFORMATION_SCHEMA.TABLES "
                "WHERE PARENT_TABLE_NAME = @parent_table "
                "ORDER BY TABLE_NAME"
            )
            return sql, {"parent_table": parent_table}
        sql = (
            "SELECT TABLE_NAME AS table_name, PARENT_TABLE_NAME AS parent_table_name, "
            "ON_DELETE_ACTION AS on_delete_action "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE PARENT_TABLE_NAME IS NOT NULL "
            "ORDER BY TABLE_NAME"
        )
        return sql, {}

    def build_columns_sql(self, table_name: str, schema: str | None = None) -> tuple[str, dict[str, Any]]:
        """Build the ``INFORMATION_SCHEMA.COLUMNS`` query for ``table_name``."""
        if self._dialect == "spangres":
            target_schema = schema or "public"
            sql = (
                "SELECT table_name, column_name, ordinal_position, is_nullable, "
                "data_type, spanner_type, is_generated, generation_expression "
                "FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = $2 "
                "ORDER BY ordinal_position"
            )
            return sql, {"p1": target_schema, "p2": table_name}
        target_schema = schema if schema is not None else ""
        sql = (
            "SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name, "
            "ORDINAL_POSITION AS ordinal_position, IS_NULLABLE AS is_nullable, "
            "SPANNER_TYPE AS spanner_type, IS_GENERATED AS is_generated, "
            "GENERATION_EXPRESSION AS generation_expression "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table_name "
            "ORDER BY ORDINAL_POSITION"
        )
        return sql, {"schema": target_schema, "table_name": table_name}

    def build_indexes_sql(self, table_name: str, schema: str | None = None) -> tuple[str, dict[str, Any]]:
        """Build the ``INFORMATION_SCHEMA.INDEXES`` query for ``table_name``."""
        if self._dialect == "spangres":
            target_schema = schema or "public"
            sql = (
                "SELECT index_name, table_name, is_unique, is_null_filtered, "
                "index_state, spanner_is_managed "
                "FROM information_schema.indexes "
                "WHERE table_schema = $1 AND table_name = $2 "
                "ORDER BY index_name"
            )
            return sql, {"p1": target_schema, "p2": table_name}
        target_schema = schema if schema is not None else ""
        sql = (
            "SELECT INDEX_NAME AS index_name, TABLE_NAME AS table_name, "
            "IS_UNIQUE AS is_unique, IS_NULL_FILTERED AS is_null_filtered, "
            "INDEX_STATE AS index_state, SPANNER_IS_MANAGED AS spanner_is_managed "
            "FROM INFORMATION_SCHEMA.INDEXES "
            "WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table_name "
            "ORDER BY INDEX_NAME"
        )
        return sql, {"schema": target_schema, "table_name": table_name}

    def build_table_constraints_sql(self, table_name: str, schema: str | None = None) -> tuple[str, dict[str, Any]]:
        """Build the ``INFORMATION_SCHEMA.TABLE_CONSTRAINTS`` query for ``table_name``."""
        if self._dialect == "spangres":
            target_schema = schema or "public"
            sql = (
                "SELECT constraint_name, table_name, constraint_type, enforced "
                "FROM information_schema.table_constraints "
                "WHERE table_schema = $1 AND table_name = $2 "
                "ORDER BY constraint_name"
            )
            return sql, {"p1": target_schema, "p2": table_name}
        target_schema = schema if schema is not None else ""
        sql = (
            "SELECT CONSTRAINT_NAME AS constraint_name, TABLE_NAME AS table_name, "
            "CONSTRAINT_TYPE AS constraint_type, ENFORCED AS enforced "
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
            "WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table_name "
            "ORDER BY CONSTRAINT_NAME"
        )
        return sql, {"schema": target_schema, "table_name": table_name}


class SpannerSyncDataDictionary(_BaseSpannerDataDictionary):
    """Synchronous Spanner ``INFORMATION_SCHEMA`` metadata introspection helper."""

    def get_tables(self, schema: str | None = None) -> list[dict[str, Any]]:
        """Return table metadata rows including ``parent_table_name`` and ``on_delete_action``."""
        sql, params = self.build_tables_sql(schema)
        res = self._driver.select(sql, params)
        return list(res) if isinstance(res, list) else []

    def get_interleaved_tables(self, parent_table: str | None = None) -> list[dict[str, Any]]:
        """Return interleaved child tables and their ``on_delete_action``."""
        sql, params = self.build_interleaved_tables_sql(parent_table)
        res = self._driver.select(sql, params)
        return list(res) if isinstance(res, list) else []

    def get_columns(self, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Return column metadata rows for ``table_name``."""
        sql, params = self.build_columns_sql(table_name, schema)
        res = self._driver.select(sql, params)
        return list(res) if isinstance(res, list) else []

    def get_indexes(self, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Return index metadata rows for ``table_name``."""
        sql, params = self.build_indexes_sql(table_name, schema)
        res = self._driver.select(sql, params)
        return list(res) if isinstance(res, list) else []

    def get_table_constraints(self, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Return table constraint metadata rows for ``table_name``."""
        sql, params = self.build_table_constraints_sql(table_name, schema)
        res = self._driver.select(sql, params)
        return list(res) if isinstance(res, list) else []


class SpannerAsyncDataDictionary(_BaseSpannerDataDictionary):
    """Asynchronous Spanner ``INFORMATION_SCHEMA`` metadata introspection helper."""

    async def get_tables(self, schema: str | None = None) -> list[dict[str, Any]]:
        """Asynchronously return table metadata rows."""
        sql, params = self.build_tables_sql(schema)
        res = self._driver.select(sql, params)
        if isinstance(res, list):
            return res
        return list(await res)

    async def get_interleaved_tables(self, parent_table: str | None = None) -> list[dict[str, Any]]:
        """Asynchronously return interleaved child tables and their ``on_delete_action``."""
        sql, params = self.build_interleaved_tables_sql(parent_table)
        res = self._driver.select(sql, params)
        if isinstance(res, list):
            return res
        return list(await res)

    async def get_columns(self, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Asynchronously return column metadata rows for ``table_name``."""
        sql, params = self.build_columns_sql(table_name, schema)
        res = self._driver.select(sql, params)
        if isinstance(res, list):
            return res
        return list(await res)

    async def get_indexes(self, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Asynchronously return index metadata rows for ``table_name``."""
        sql, params = self.build_indexes_sql(table_name, schema)
        res = self._driver.select(sql, params)
        if isinstance(res, list):
            return res
        return list(await res)

    async def get_table_constraints(self, table_name: str, schema: str | None = None) -> list[dict[str, Any]]:
        """Asynchronously return table constraint metadata rows for ``table_name``."""
        sql, params = self.build_table_constraints_sql(table_name, schema)
        res = self._driver.select(sql, params)
        if isinstance(res, list):
            return res
        return list(await res)


class SpannerSyncDriver(BaseSyncDriver):
    """Synchronous SQLSpec driver for Google Cloud Spanner (GoogleSQL and Spangres dialects)."""

    dialect: str = "googlesql"

    _arrow_execution_options = (
        *BaseSyncDriver._arrow_execution_options,
        "data_boost",
        "max_partitions",
        "staleness",
    )

    def __init__(
        self,
        core: SyncSpannerDriver,
        statement_cache: StatementCache | None = None,
        enable_sqlcommenter: bool = False,
        tenant_predicate: TenantPredicate | None = None,
        storage_backend: ObjectStoreBackend | None = None,
        *,
        statement_config: Any | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        read_only: bool = False,
        in_transaction: bool = False,
        staleness: StalenessBound | None = None,
        active_snapshot: SyncSpannerSnapshot | None = None,
    ) -> None:
        if tenant_predicate is not None:
            msg = "Tenant session enforcement is unsupported for Spanner"
            raise NotSupportedError(msg)
        super().__init__(
            core,
            statement_config=statement_config or _spanner_statement_config(core.config.dialect),
            driver_features=driver_features,
            observability=observability,
        )
        self._core: SyncSpannerDriver = core
        self._rust_statement_cache = statement_cache
        self._enable_sqlcommenter = enable_sqlcommenter
        self._storage_backend = storage_backend
        self.dialect = "spangres" if core.config.dialect == "spangres" else "googlesql"
        self._read_only = read_only
        self._in_transaction = in_transaction
        self._staleness = staleness
        self._active_snapshot = active_snapshot
        self._data_dictionary: SpannerSyncDataDictionary | None = None

    @property
    def core_driver(self) -> SyncSpannerDriver:
        """Return the underlying ``SyncSpannerDriver``."""
        return self._core

    @property
    def is_read_only(self) -> bool:
        """Return whether this session is bound to a lock-free ``ReadOnly`` snapshot."""
        return self._read_only

    @property
    def in_transaction(self) -> bool:
        """Return whether this session is bound to an active ``ReadWrite`` transaction."""
        return self._in_transaction

    @property
    def staleness(self) -> StalenessBound | None:
        """Return the configured ``StalenessBound`` when bound to a snapshot session."""
        return self._staleness

    @property
    def data_dictionary(self) -> Any:
        """Return the ``SpannerSyncDataDictionary`` for ``INFORMATION_SCHEMA`` introspection."""
        if self._data_dictionary is None:
            self._data_dictionary = SpannerSyncDataDictionary(self._core, dialect=self._core.config.dialect)
        return self._data_dictionary

    def execute(
        self,
        sql: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a DML or DDL statement and return affected row count."""
        _ = (statement_config, kwargs)
        if self._read_only:
            raise ImproperConfigurationError("Cannot execute DML inside a read-only Spanner snapshot session")
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = self._normalize_params(parameters)
        return self._core.execute(sql_str, params)

    def execute_many(
        self,
        sql: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a parameterized DML statement across a batch of parameter sets via ``ExecuteBatchDml``."""
        _ = (filters, statement_config, kwargs)
        if self._read_only:
            raise ImproperConfigurationError("Cannot execute batch DML inside a read-only Spanner snapshot session")
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        statements = [(sql_str, p) for p in parameters]
        counts = self._core.execute_batch_dml(statements)
        return sum(counts)

    def execute_script(
        self,
        sql_script: Any,
        /,
        *parameters: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute a semicolon-delimited Spanner SQL or DDL script."""
        _ = (parameters, kwargs)
        script_str = sql_script.sql if hasattr(sql_script, "sql") else str(sql_script)
        statements = [stmt.strip() for stmt in script_str.split(";") if stmt.strip()]
        if not statements:
            return 0
        ddl_prefixes = ("CREATE ", "ALTER ", "DROP ")
        if all(s.upper().startswith(ddl_prefixes) for s in statements):
            self._core.update_ddl(statements)
            return len(statements)
        total = 0
        for stmt in statements:
            if stmt.upper().startswith(ddl_prefixes):
                self._core.update_ddl([stmt])
            else:
                total += int(self.execute(stmt))
        return total

    def execute_stack(
        self,
        statements: Any,
        *,
        continue_on_error: bool = False,
        in_transaction: bool = False,
    ) -> Any:
        """Execute a multi-statement stack with independent reads or native ``execute_batch_dml`` for DML."""
        if hasattr(statements, "operations"):
            return super().execute_stack(statements, continue_on_error=continue_on_error)
        normalized: list[tuple[str, Mapping[str, Any] | Sequence[Any] | None]] = [
            (item, None) if isinstance(item, str) else (item[0], item[1]) for item in statements
        ]
        if not normalized:
            return []

        all_dml = all(sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for sql, _ in normalized)
        if all_dml and not self._read_only:
            return list(self._core.execute_batch_dml(normalized))

        if in_transaction and not self._read_only:

            def _run_stack(tx: Any) -> list[Any]:
                results: list[Any] = []
                for sql, params in normalized:
                    if sql.strip().upper().startswith(("SELECT", "WITH", "GRAPH")):
                        results.append(tx.query(sql, params))
                    else:
                        results.append(tx.execute(sql, params))
                return results

            return cast("list[Any]", self._core.run_in_transaction(_run_stack))

        results: list[Any] = []
        for sql, params in normalized:
            if sql.strip().upper().startswith(("SELECT", "WITH", "GRAPH")):
                results.append(self.select(sql, params))
            else:
                results.append(self.execute(sql, params))
        return results

    def select(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a ``SELECT`` query and hydrate rows as dictionaries or ``schema_type``."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = self._normalize_params(parameters)
        if self._active_snapshot is not None:
            return self._active_snapshot.query(sql_str, params, schema_type=schema_type)
        return self._core.select(sql_str, params, schema_type=schema_type)

    def select_one(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = self._normalize_params(parameters)
        if self._active_snapshot is not None:
            return self._active_snapshot.query_one(sql_str, params, schema_type=schema_type)
        return self._core.select_one(sql_str, params, schema_type=schema_type)

    def select_one_or_none(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or ``None``."""
        rows = self.select(sql, *parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    def select_value(
        self,
        sql: Any,
        /,
        *parameters: Any,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column of the first row."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = self._normalize_params(parameters)
        if self._active_snapshot is not None:
            return self._active_snapshot.select_value(sql_str, params, value_type=value_type)
        return self._core.select_value(sql_str, params, value_type=value_type)

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> SyncSpannerSnapshot:
        """Open a lock-free ReadOnly ``SyncSpannerSnapshot``."""
        return self._core.snapshot(staleness, multi_use=multi_use)

    def run_in_transaction(
        self,
        fn: Any,
        *,
        tag: str = "",
        max_commit_delay_ms: int | None = None,
    ) -> Any:
        """Execute ``fn(tx)`` inside a ReadWrite transaction with automatic abort retry."""
        return self._core.run_in_transaction(fn, tag=tag, max_commit_delay_ms=max_commit_delay_ms)

    def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None]],
        *,
        tag: str = "",
    ) -> list[int]:
        """Execute multiple DML statements in a single ``ExecuteBatchDml`` RPC."""
        return self._core.execute_batch_dml(statements, tag=tag)

    def execute_partitioned_dml(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> int:
        """Execute bulk Partitioned DML across database splits."""
        return self._core.execute_partitioned_dml(sql, params)

    def apply_mutations(
        self,
        mutations: Sequence[Mutation],
        *,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        """Commit a sequence of ``Mutation`` operations."""
        return self._core.apply_mutations(mutations, max_commit_delay_ms=max_commit_delay_ms)

    def batch_write(
        self,
        mutation_groups: Sequence[MutationGroup | Sequence[Mutation]],
    ) -> list[dict[str, Any]]:
        """Execute ``BatchWrite`` across independent ``MutationGroup`` batches."""
        return self._core.batch_write(mutation_groups)

    def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Apply Spanner schema DDL statements."""
        return self._core.update_ddl(statements)

    def select_to_arrow(
        self,
        sql_or_table: Any,
        /,
        *parameters: Any,
        data_boost: bool = True,
        max_partitions: int | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a parallel ``PartitionQuery`` scan (with ``data_boost``) into a zero-copy ``ArrowCursor``."""
        _ = kwargs
        raw_str = sql_or_table.sql if hasattr(sql_or_table, "sql") else str(sql_or_table)
        sql = raw_str if " " in raw_str.strip() else f"SELECT * FROM {raw_str}"
        params = self._normalize_params(parameters)
        return self._core.select_to_arrow(
            sql,
            params,
            data_boost_enabled=data_boost,
            max_partitions=max_partitions,
            staleness=staleness or self._staleness,
        )

    def load_from_arrow(
        self,
        table: str,
        source: Any,
        *,
        upsert: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Ingest an ``ArrowCursor``, PyArrow ``Table``, or row list into ``table`` via Spanner mutations."""
        _ = kwargs
        return self._core.load_from_arrow(table, source, upsert=upsert)

    def select_to_storage(
        self,
        sql_or_table: Any,
        destination_path: Any,
        /,
        *parameters: Any,
        format: Literal["parquet", "jsonl", "csv", "arrow"] = "parquet",
        storage_backend: ObjectStoreBackend | None = None,
        data_boost: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Export a Spanner query or table into ``ObjectStoreBackend`` via parallel Arrow partitions."""
        _ = kwargs
        backend = self._require_storage(storage_backend)
        raw_str = sql_or_table.sql if hasattr(sql_or_table, "sql") else str(sql_or_table)
        sql = raw_str if " " in raw_str.strip() else f"SELECT * FROM {raw_str}"
        params = self._normalize_params(parameters)
        normalized_fmt: Literal["parquet", "arrow", "jsonl"] = "jsonl" if format == "csv" else format
        return self._core.export_to_object_store(
            sql,
            backend,
            str(destination_path),
            params=params,
            format=normalized_fmt,
            data_boost_enabled=data_boost,
        )

    def load_from_storage(
        self,
        table: str,
        source: Any,
        *,
        file_format: Any = None,
        format: Literal["parquet", "jsonl", "csv", "arrow"] = "parquet",
        storage_backend: ObjectStoreBackend | None = None,
        upsert: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Load an object from ``ObjectStoreBackend`` into a Spanner table via batched mutations."""
        _ = kwargs
        backend = self._require_storage(storage_backend)
        effective_fmt = str(file_format if file_format is not None else format)
        normalized_fmt: Literal["parquet", "arrow", "jsonl"] = (
            "parquet" if effective_fmt == "parquet" else "arrow" if effective_fmt in {"arrow", "arrow-ipc"} else "jsonl"
        )
        return self._core.load_from_object_store(
            table,
            backend,
            str(source),
            format=normalized_fmt,
            upsert=upsert,
        )

    def _require_storage(self, override: ObjectStoreBackend | None = None) -> ObjectStoreBackend:
        backend = override or self._storage_backend
        if backend is None:
            raise ImproperConfigurationError("Spanner storage operation requires a configured ObjectStoreBackend")
        return backend


class SpannerAsyncDriver(BaseAsyncDriver):
    """Asynchronous SQLSpec driver for Google Cloud Spanner (GoogleSQL and Spangres dialects)."""

    dialect: str = "googlesql"

    _arrow_execution_options = (
        *BaseAsyncDriver._arrow_execution_options,
        "data_boost",
        "max_partitions",
        "staleness",
    )

    def __init__(
        self,
        core: AsyncSpannerDriver,
        statement_cache: StatementCache | None = None,
        enable_sqlcommenter: bool = False,
        tenant_predicate: TenantPredicate | None = None,
        storage_backend: ObjectStoreBackend | None = None,
        *,
        statement_config: Any | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
        read_only: bool = False,
        in_transaction: bool = False,
        staleness: StalenessBound | None = None,
        active_snapshot: AsyncSpannerSnapshot | None = None,
    ) -> None:
        if tenant_predicate is not None:
            msg = "Tenant session enforcement is unsupported for Spanner"
            raise NotSupportedError(msg)
        super().__init__(
            core,
            statement_config=statement_config or _spanner_statement_config(core.config.dialect),
            driver_features=driver_features,
            observability=observability,
        )
        self._core: AsyncSpannerDriver = core
        self._rust_statement_cache = statement_cache
        self._enable_sqlcommenter = enable_sqlcommenter
        self._storage_backend = storage_backend
        self.dialect = "spangres" if core.config.dialect == "spangres" else "googlesql"
        self._read_only = read_only
        self._in_transaction = in_transaction
        self._staleness = staleness
        self._active_snapshot = active_snapshot
        self._data_dictionary: SpannerAsyncDataDictionary | None = None

    @property
    def core_driver(self) -> AsyncSpannerDriver:
        """Return the underlying ``AsyncSpannerDriver``."""
        return self._core

    @property
    def is_read_only(self) -> bool:
        """Return whether this session is bound to a lock-free ``ReadOnly`` snapshot."""
        return self._read_only

    @property
    def in_transaction(self) -> bool:
        """Return whether this session is bound to an active ``ReadWrite`` transaction."""
        return self._in_transaction

    @property
    def staleness(self) -> StalenessBound | None:
        """Return the configured ``StalenessBound`` when bound to a snapshot session."""
        return self._staleness

    @property
    def data_dictionary(self) -> Any:
        """Return the ``SpannerAsyncDataDictionary`` for ``INFORMATION_SCHEMA`` introspection."""
        if self._data_dictionary is None:
            self._data_dictionary = SpannerAsyncDataDictionary(self._core, dialect=self._core.config.dialect)
        return self._data_dictionary

    async def execute(
        self,
        sql: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a DML or DDL statement."""
        _ = (statement_config, kwargs)
        if self._read_only:
            raise ImproperConfigurationError("Cannot execute DML inside a read-only Spanner snapshot session")
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = BaseSyncDriver._normalize_params(parameters)
        return await self._core.execute(sql_str, params)

    async def execute_many(
        self,
        sql: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a batch of DML statements via ``ExecuteBatchDml``."""
        _ = (filters, statement_config, kwargs)
        if self._read_only:
            raise ImproperConfigurationError("Cannot execute batch DML inside a read-only Spanner snapshot session")
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        statements = [(sql_str, p) for p in parameters]
        counts = await self._core.execute_batch_dml(statements)
        return sum(counts)

    async def execute_script(
        self,
        sql_script: Any,
        /,
        *parameters: Any,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a semicolon-delimited Spanner SQL or DDL script."""
        _ = (parameters, kwargs)
        script_str = sql_script.sql if hasattr(sql_script, "sql") else str(sql_script)
        statements = [stmt.strip() for stmt in script_str.split(";") if stmt.strip()]
        if not statements:
            return 0
        ddl_prefixes = ("CREATE ", "ALTER ", "DROP ")
        if all(s.upper().startswith(ddl_prefixes) for s in statements):
            await self._core.update_ddl(statements)
            return len(statements)
        total = 0
        for stmt in statements:
            if stmt.upper().startswith(ddl_prefixes):
                await self._core.update_ddl([stmt])
            else:
                total += int(await self.execute(stmt))
        return total

    async def execute_stack(
        self,
        statements: Any,
        *,
        continue_on_error: bool = False,
        in_transaction: bool = False,
    ) -> Any:
        """Asynchronously execute a multi-statement stack with independent reads or native ``execute_batch_dml``."""
        if hasattr(statements, "operations"):
            return await super().execute_stack(statements, continue_on_error=continue_on_error)
        normalized: list[tuple[str, Mapping[str, Any] | Sequence[Any] | None]] = [
            (item, None) if isinstance(item, str) else (item[0], item[1]) for item in statements
        ]
        if not normalized:
            return []

        all_dml = all(sql.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for sql, _ in normalized)
        if all_dml and not self._read_only:
            return list(await self._core.execute_batch_dml(normalized))

        if in_transaction and not self._read_only:

            async def _run_stack(tx: Any) -> list[Any]:
                results: list[Any] = []
                for sql, params in normalized:
                    if sql.strip().upper().startswith(("SELECT", "WITH", "GRAPH")):
                        results.append(await tx.query(sql, params))
                    else:
                        results.append(await tx.execute(sql, params))
                return results

            return cast("list[Any]", await self._core.run_in_transaction(_run_stack))

        results: list[Any] = []
        for sql, params in normalized:
            if sql.strip().upper().startswith(("SELECT", "WITH", "GRAPH")):
                results.append(await self.select(sql, params))
            else:
                results.append(await self.execute(sql, params))
        return results

    async def select(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Asynchronously execute a ``SELECT`` query."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = BaseSyncDriver._normalize_params(parameters)
        if self._active_snapshot is not None:
            return await self._active_snapshot.query(sql_str, params, schema_type=schema_type)
        return await self._core.select(sql_str, params, schema_type=schema_type)

    async def select_one(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a query and return the first row."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = BaseSyncDriver._normalize_params(parameters)
        if self._active_snapshot is not None:
            return await self._active_snapshot.query_one(sql_str, params, schema_type=schema_type)
        return await self._core.select_one(sql_str, params, schema_type=schema_type)

    async def select_one_or_none(
        self,
        sql: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Asynchronously execute a query and return the first row or ``None``."""
        rows = await self.select(sql, *parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    async def select_value(
        self,
        sql: Any,
        /,
        *parameters: Any,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a query and return the first scalar column of the first row."""
        _ = kwargs
        sql_str = sql.sql if hasattr(sql, "sql") else str(sql)
        params = BaseSyncDriver._normalize_params(parameters)
        if self._active_snapshot is not None:
            return await self._active_snapshot.select_value(sql_str, params, value_type=value_type)
        return await self._core.select_value(sql_str, params, value_type=value_type)

    def snapshot(
        self,
        staleness: StalenessBound | None = None,
        *,
        multi_use: bool = True,
    ) -> Any:
        """Return an async lock-free ``AsyncSpannerSnapshot`` context manager."""
        return self._core.snapshot(staleness, multi_use=multi_use)

    async def run_in_transaction(
        self,
        fn: Any,
        *,
        tag: str = "",
        max_commit_delay_ms: int | None = None,
    ) -> Any:
        """Asynchronously execute ``fn(tx)`` inside a ReadWrite transaction."""
        return await self._core.run_in_transaction(fn, tag=tag, max_commit_delay_ms=max_commit_delay_ms)

    async def execute_batch_dml(
        self,
        statements: Sequence[tuple[str, Mapping[str, Any] | Sequence[Any] | None]],
        *,
        tag: str = "",
    ) -> list[int]:
        """Asynchronously execute multiple DML statements in a single ``ExecuteBatchDml`` RPC."""
        return await self._core.execute_batch_dml(statements, tag=tag)

    async def execute_partitioned_dml(
        self,
        sql: str,
        params: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> int:
        """Asynchronously execute Partitioned DML."""
        return await self._core.execute_partitioned_dml(sql, params)

    async def apply_mutations(
        self,
        mutations: Sequence[Mutation],
        *,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        """Asynchronously commit a sequence of ``Mutation`` operations."""
        return await self._core.apply_mutations(mutations, max_commit_delay_ms=max_commit_delay_ms)

    async def batch_write(
        self,
        mutation_groups: Sequence[MutationGroup | Sequence[Mutation]],
    ) -> list[dict[str, Any]]:
        """Asynchronously execute ``BatchWrite`` across ``MutationGroup`` batches."""
        return await self._core.batch_write(mutation_groups)

    async def update_ddl(self, statements: Sequence[str] | str) -> str:
        """Asynchronously apply Spanner schema DDL statements."""
        return await self._core.update_ddl(statements)

    async def select_to_arrow(
        self,
        sql_or_table: Any,
        /,
        *parameters: Any,
        data_boost: bool = True,
        max_partitions: int | None = None,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously execute a parallel ``PartitionQuery`` scan into a zero-copy ``ArrowCursor``."""
        _ = kwargs
        raw_str = sql_or_table.sql if hasattr(sql_or_table, "sql") else str(sql_or_table)
        sql = raw_str if " " in raw_str.strip() else f"SELECT * FROM {raw_str}"
        params = BaseSyncDriver._normalize_params(parameters)
        return await self._core.select_to_arrow(
            sql,
            params,
            data_boost_enabled=data_boost,
            max_partitions=max_partitions,
            staleness=staleness or self._staleness,
        )

    async def load_from_arrow(
        self,
        table: str,
        source: Any,
        *,
        upsert: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously ingest an ``ArrowCursor`` or PyArrow ``Table`` into ``table``."""
        _ = kwargs
        return await self._core.load_from_arrow(table, source, upsert=upsert)

    async def select_to_storage(
        self,
        sql_or_table: Any,
        destination_path: Any,
        /,
        *parameters: Any,
        format: Literal["parquet", "jsonl", "csv", "arrow"] = "parquet",
        storage_backend: ObjectStoreBackend | None = None,
        data_boost: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously export a Spanner query or table into ``ObjectStoreBackend``."""
        _ = kwargs
        backend = self._require_storage(storage_backend)
        raw_str = sql_or_table.sql if hasattr(sql_or_table, "sql") else str(sql_or_table)
        sql = raw_str if " " in raw_str.strip() else f"SELECT * FROM {raw_str}"
        params = BaseSyncDriver._normalize_params(parameters)
        normalized_fmt: Literal["parquet", "arrow", "jsonl"] = "jsonl" if format == "csv" else format
        return await self._core.export_to_object_store(
            sql,
            backend,
            str(destination_path),
            params=params,
            format=normalized_fmt,
            data_boost_enabled=data_boost,
        )

    async def load_from_storage(
        self,
        table: str,
        source: Any,
        *,
        file_format: Any = None,
        format: Literal["parquet", "jsonl", "csv", "arrow"] = "parquet",
        storage_backend: ObjectStoreBackend | None = None,
        upsert: bool = True,
        **kwargs: Any,
    ) -> Any:
        """Asynchronously load an object from ``ObjectStoreBackend`` into a Spanner table."""
        _ = kwargs
        backend = self._require_storage(storage_backend)
        effective_fmt = str(file_format if file_format is not None else format)
        normalized_fmt: Literal["parquet", "arrow", "jsonl"] = (
            "parquet" if effective_fmt == "parquet" else "arrow" if effective_fmt in {"arrow", "arrow-ipc"} else "jsonl"
        )
        return await self._core.load_from_object_store(
            table,
            backend,
            str(source),
            format=normalized_fmt,
            upsert=upsert,
        )

    def _require_storage(self, override: ObjectStoreBackend | None = None) -> ObjectStoreBackend:
        backend = override or self._storage_backend
        if backend is None:
            raise ImproperConfigurationError("Spanner storage operation requires a configured ObjectStoreBackend")
        return backend


def _resolve_spanner_config(
    config: SpannerConfig | None,
    *,
    dsn: str | None,
    project_id: str | None,
    instance_id: str | None,
    database_id: str | None,
    database_role: str | None,
    dialect: Literal["googlesql", "spangres"],
    endpoint: str | None,
    pool_size: int,
    route_to_leader_enabled: bool,
    max_commit_delay_ms: int,
    data_boost_enabled: bool,
    max_partitions: int,
    credentials_path: str | None,
    service_account_key: str | None,
    quota_project_id: str | None,
    scopes: tuple[str, ...] | None,
) -> SpannerConfig:
    if config is not None:
        return config
    if dsn is not None:
        return SpannerConfig.from_dsn(dsn)
    return SpannerConfig(
        project_id=project_id or "local-project",
        instance_id=instance_id or "local-instance",
        database_id=database_id or "local-db",
        database_role=database_role,
        dialect=dialect,
        endpoint=endpoint or "https://spanner.googleapis.com",
        pool_size=pool_size,
        route_to_leader_enabled=route_to_leader_enabled,
        max_commit_delay_ms=max_commit_delay_ms,
        data_boost_enabled=data_boost_enabled,
        max_partitions=max_partitions,
        credentials_path=credentials_path,
        service_account_key=service_account_key,
        quota_project_id=quota_project_id,
        scopes=scopes or ("https://www.googleapis.com/auth/spanner.data",),
    )


def _resolve_storage_backend(storage_backend: ObjectStoreBackend | str | None) -> ObjectStoreBackend | None:
    if storage_backend is None:
        return None
    if isinstance(storage_backend, ObjectStoreBackend):
        return storage_backend
    return ObjectStoreBackend(storage_backend)


class SpannerSyncConfig(SyncAdapterConfig[SpannerConfig, SpannerSyncDriver]):
    """SQLSpec synchronous configuration for Google Cloud Spanner."""

    dialect: ClassVar[str] = "googlesql"
    _config_cls: ClassVar[type[SpannerConfig]] = SpannerConfig
    _driver_cls: ClassVar[type[SyncSpannerDriver]] = SyncSpannerDriver
    driver_type: ClassVar[type[SpannerSyncDriver]] = SpannerSyncDriver

    def __init__(
        self,
        config: SpannerConfig | None = None,
        *,
        dsn: str | None = None,
        project_id: str | None = None,
        instance_id: str | None = None,
        database_id: str | None = None,
        database_role: str | None = None,
        dialect: Literal["googlesql", "spangres"] = "googlesql",
        endpoint: str | None = None,
        pool_size: int = 4,
        route_to_leader_enabled: bool = True,
        max_commit_delay_ms: int = 0,
        data_boost_enabled: bool = True,
        max_partitions: int = 8,
        credentials_path: str | None = None,
        service_account_key: str | None = None,
        quota_project_id: str | None = None,
        scopes: tuple[str, ...] | None = None,
        storage_backend: ObjectStoreBackend | str | None = None,
        connection_instance: Any = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = False,
        tenant_predicate: TenantPredicate | None = None,
        bind_key: str = "default",
        broker: LeaseBroker | None = None,
        share_pool: bool = True,
        statement_config: Any | None = None,
    ) -> None:
        if tenant_predicate is not None:
            msg = "Tenant session enforcement is unsupported for Spanner"
            raise NotSupportedError(msg)
        self.config = config
        self.dsn = dsn
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.database_role = database_role
        self._spanner_dialect: Literal["googlesql", "spangres"] = dialect
        self.endpoint = endpoint
        self.pool_size = pool_size
        self.route_to_leader_enabled = route_to_leader_enabled
        self.max_commit_delay_ms = max_commit_delay_ms
        self.data_boost_enabled = data_boost_enabled
        self.max_partitions = max_partitions
        self.credentials_path = credentials_path
        self.service_account_key = service_account_key
        self.quota_project_id = quota_project_id
        self.scopes = scopes
        self.storage_backend = storage_backend
        self.connection_instance = connection_instance
        self.statement_cache_size = statement_cache_size
        self.enable_sqlcommenter = enable_sqlcommenter
        self.tenant_predicate = tenant_predicate
        resolved_cfg = self.resolve_spanner_config()
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=share_pool,
            statement_config=statement_config or _spanner_statement_config(resolved_cfg.dialect),
        )
        self.connection_instance = connection_instance
        self._cached_core: SyncSpannerDriver | None = None

    def resolve_spanner_config(self) -> SpannerConfig:
        """Resolve the canonical ``SpannerConfig`` from explicit fields or ``dsn``."""
        return _resolve_spanner_config(
            self.config,
            dsn=self.dsn,
            project_id=self.project_id,
            instance_id=self.instance_id,
            database_id=self.database_id,
            database_role=self.database_role,
            dialect=self._spanner_dialect,
            endpoint=self.endpoint,
            pool_size=self.pool_size,
            route_to_leader_enabled=self.route_to_leader_enabled,
            max_commit_delay_ms=self.max_commit_delay_ms,
            data_boost_enabled=self.data_boost_enabled,
            max_partitions=self.max_partitions,
            credentials_path=self.credentials_path,
            service_account_key=self.service_account_key,
            quota_project_id=self.quota_project_id,
            scopes=self.scopes,
        )

    def resolve_storage_backend(self) -> ObjectStoreBackend | None:
        """Resolve the configured ``ObjectStoreBackend`` instance, if any."""
        return _resolve_storage_backend(self.storage_backend)

    def create_driver(self) -> SpannerSyncDriver:
        """Instantiate a ``SpannerSyncDriver``."""
        core = self._resolve_core_driver()
        cache = StatementCache(self.statement_cache_size) if self.statement_cache_size > 0 else None
        return SpannerSyncDriver(
            core=core,
            statement_cache=cache,
            enable_sqlcommenter=self.enable_sqlcommenter,
            tenant_predicate=self.tenant_predicate,
            storage_backend=self.resolve_storage_backend(),
            statement_config=self.statement_config,
        )

    def provide_driver(
        self,
        broker: LeaseBroker | None = None,
        statement_cache: StatementCache | None = None,
    ) -> SpannerSyncDriver:
        """Return a configured ``SpannerSyncDriver`` instance."""
        del broker, statement_cache
        return self.create_driver()

    def create_connection(self) -> SpannerConnectionShim:
        """Return a ``SpannerConnectionShim`` for upstream SQLSpec compatibility."""
        return SpannerConnectionShim(self._resolve_core_driver())

    @contextmanager
    def provide_session(
        self,
        *args: Any,
        transaction: bool = False,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> Iterator[SpannerSyncDriver]:
        """Yield a ``SpannerSyncDriver`` bound to a lock-free ReadOnly snapshot or ReadWrite transaction.

        Args:
            *args: Ignored positional arguments for adapter signature compatibility.
            transaction: When ``False`` (default), yields a lock-free ReadOnly snapshot
                session that never issues a ``Commit`` RPC. When ``True``, yields a
                ReadWrite transactional session.
            staleness: Optional ``StalenessBound`` applied when ``transaction=False``.
            **kwargs: Ignored keyword arguments for adapter signature compatibility.
        """
        del args, kwargs
        core = self._resolve_core_driver()
        cache = StatementCache(self.statement_cache_size) if self.statement_cache_size > 0 else None
        storage = self.resolve_storage_backend()
        if not transaction:
            with core.snapshot(staleness, multi_use=True) as snap:
                yield SpannerSyncDriver(
                    core=core,
                    statement_cache=cache,
                    enable_sqlcommenter=self.enable_sqlcommenter,
                    tenant_predicate=self.tenant_predicate,
                    storage_backend=storage,
                    statement_config=self.statement_config,
                    read_only=True,
                    in_transaction=False,
                    staleness=staleness,
                    active_snapshot=snap,
                )
        else:
            yield SpannerSyncDriver(
                core=core,
                statement_cache=cache,
                enable_sqlcommenter=self.enable_sqlcommenter,
                tenant_predicate=self.tenant_predicate,
                storage_backend=storage,
                statement_config=self.statement_config,
                read_only=False,
                in_transaction=True,
            )

    def _resolve_core_driver(self) -> SyncSpannerDriver:
        if isinstance(self.connection_instance, SyncSpannerDriver):
            return self.connection_instance
        if isinstance(self.connection_instance, SpannerConnectionShim):
            return self.connection_instance.driver
        if self._cached_core is None:
            self._cached_core = SyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_core


class SpannerAsyncConfig(AsyncAdapterConfig[SpannerConfig, SpannerAsyncDriver]):
    """SQLSpec asynchronous configuration for Google Cloud Spanner."""

    dialect: ClassVar[str] = "googlesql"
    _config_cls: ClassVar[type[SpannerConfig]] = SpannerConfig
    _driver_cls: ClassVar[type[AsyncSpannerDriver]] = AsyncSpannerDriver
    driver_type: ClassVar[type[SpannerAsyncDriver]] = SpannerAsyncDriver

    def __init__(
        self,
        config: SpannerConfig | None = None,
        *,
        dsn: str | None = None,
        project_id: str | None = None,
        instance_id: str | None = None,
        database_id: str | None = None,
        database_role: str | None = None,
        dialect: Literal["googlesql", "spangres"] = "googlesql",
        endpoint: str | None = None,
        pool_size: int = 4,
        route_to_leader_enabled: bool = True,
        max_commit_delay_ms: int = 0,
        data_boost_enabled: bool = True,
        max_partitions: int = 8,
        credentials_path: str | None = None,
        service_account_key: str | None = None,
        quota_project_id: str | None = None,
        scopes: tuple[str, ...] | None = None,
        storage_backend: ObjectStoreBackend | str | None = None,
        connection_instance: Any = None,
        statement_cache_size: int = 256,
        enable_sqlcommenter: bool = False,
        tenant_predicate: TenantPredicate | None = None,
        bind_key: str = "default",
        broker: LeaseBroker | None = None,
        share_pool: bool = True,
        statement_config: Any | None = None,
    ) -> None:
        if tenant_predicate is not None:
            msg = "Tenant session enforcement is unsupported for Spanner"
            raise NotSupportedError(msg)
        self.config = config
        self.dsn = dsn
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.database_role = database_role
        self._spanner_dialect: Literal["googlesql", "spangres"] = dialect
        self.endpoint = endpoint
        self.pool_size = pool_size
        self.route_to_leader_enabled = route_to_leader_enabled
        self.max_commit_delay_ms = max_commit_delay_ms
        self.data_boost_enabled = data_boost_enabled
        self.max_partitions = max_partitions
        self.credentials_path = credentials_path
        self.service_account_key = service_account_key
        self.quota_project_id = quota_project_id
        self.scopes = scopes
        self.storage_backend = storage_backend
        self.connection_instance = connection_instance
        self.statement_cache_size = statement_cache_size
        self.enable_sqlcommenter = enable_sqlcommenter
        self.tenant_predicate = tenant_predicate
        resolved_cfg = self.resolve_spanner_config()
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=share_pool,
            statement_config=statement_config or _spanner_statement_config(resolved_cfg.dialect),
        )
        self.connection_instance = connection_instance
        self._cached_core: AsyncSpannerDriver | None = None
        self._cached_sync_core: SyncSpannerDriver | None = None

    def resolve_spanner_config(self) -> SpannerConfig:
        """Resolve the canonical ``SpannerConfig`` from explicit fields or ``dsn``."""
        return _resolve_spanner_config(
            self.config,
            dsn=self.dsn,
            project_id=self.project_id,
            instance_id=self.instance_id,
            database_id=self.database_id,
            database_role=self.database_role,
            dialect=self._spanner_dialect,
            endpoint=self.endpoint,
            pool_size=self.pool_size,
            route_to_leader_enabled=self.route_to_leader_enabled,
            max_commit_delay_ms=self.max_commit_delay_ms,
            data_boost_enabled=self.data_boost_enabled,
            max_partitions=self.max_partitions,
            credentials_path=self.credentials_path,
            service_account_key=self.service_account_key,
            quota_project_id=self.quota_project_id,
            scopes=self.scopes,
        )

    def resolve_storage_backend(self) -> ObjectStoreBackend | None:
        """Resolve the configured ``ObjectStoreBackend`` instance, if any."""
        return _resolve_storage_backend(self.storage_backend)

    def create_driver(self) -> SpannerAsyncDriver:
        """Instantiate a ``SpannerAsyncDriver``."""
        core = self._resolve_core_driver()
        cache = StatementCache(self.statement_cache_size) if self.statement_cache_size > 0 else None
        return SpannerAsyncDriver(
            core=core,
            statement_cache=cache,
            enable_sqlcommenter=self.enable_sqlcommenter,
            tenant_predicate=self.tenant_predicate,
            storage_backend=self.resolve_storage_backend(),
            statement_config=self.statement_config,
        )

    def provide_driver(
        self,
        broker: LeaseBroker | None = None,
        statement_cache: StatementCache | None = None,
    ) -> SpannerAsyncDriver:
        """Return a configured ``SpannerAsyncDriver`` instance."""
        del broker, statement_cache
        return self.create_driver()

    def create_connection(self) -> Any:
        """Return a ``SpannerConnectionShim`` for upstream SQLSpec compatibility."""
        return SpannerConnectionShim(self._resolve_sync_core_driver())

    @asynccontextmanager
    async def provide_session(
        self,
        *args: Any,
        transaction: bool = False,
        staleness: StalenessBound | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[SpannerAsyncDriver]:
        """Yield a ``SpannerAsyncDriver`` bound to a lock-free ReadOnly snapshot or ReadWrite transaction."""
        del args, kwargs
        core = self._resolve_core_driver()
        cache = StatementCache(self.statement_cache_size) if self.statement_cache_size > 0 else None
        storage = self.resolve_storage_backend()
        if not transaction:
            async with core.snapshot(staleness, multi_use=True) as snap:
                yield SpannerAsyncDriver(
                    core=core,
                    statement_cache=cache,
                    enable_sqlcommenter=self.enable_sqlcommenter,
                    tenant_predicate=self.tenant_predicate,
                    storage_backend=storage,
                    statement_config=self.statement_config,
                    read_only=True,
                    in_transaction=False,
                    staleness=staleness,
                    active_snapshot=snap,
                )
        else:
            yield SpannerAsyncDriver(
                core=core,
                statement_cache=cache,
                enable_sqlcommenter=self.enable_sqlcommenter,
                tenant_predicate=self.tenant_predicate,
                storage_backend=storage,
                statement_config=self.statement_config,
                read_only=False,
                in_transaction=True,
            )

    def _resolve_core_driver(self) -> AsyncSpannerDriver:
        if isinstance(self.connection_instance, AsyncSpannerDriver):
            return self.connection_instance
        if isinstance(self.connection_instance, SyncSpannerDriver):
            if self._cached_core is None:
                self._cached_core = AsyncSpannerDriver(self.resolve_spanner_config())
            return self._cached_core
        if self._cached_core is None:
            self._cached_core = AsyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_core

    def _resolve_sync_core_driver(self) -> SyncSpannerDriver:
        if isinstance(self.connection_instance, SyncSpannerDriver):
            return self.connection_instance
        if isinstance(self.connection_instance, AsyncSpannerDriver):
            if self._cached_sync_core is None:
                self._cached_sync_core = SyncSpannerDriver(self.resolve_spanner_config())
            return self._cached_sync_core
        if isinstance(self.connection_instance, SpannerConnectionShim):
            return self.connection_instance.driver
        if self._cached_sync_core is None:
            self._cached_sync_core = SyncSpannerDriver(self.resolve_spanner_config())
        return self._cached_sync_core


class SpannerSyncADKStore:
    """Zero-SDK synchronous Spanner ADK session and event store with interleaved parent-child tables."""

    def __init__(
        self,
        config: SpannerSyncConfig,
        *,
        sessions_table: str = _DEFAULT_SESSIONS_TABLE,
        events_table: str = _DEFAULT_EVENTS_TABLE,
        ttl_days: int = 90,
    ) -> None:
        self._config = config
        self._driver = config.provide_driver()
        self._sessions_table = sessions_table
        self._events_table = events_table
        self._ttl_days = ttl_days

    @property
    def sessions_table(self) -> str:
        """Return the Spanner ADK sessions parent table name."""
        return self._sessions_table

    @property
    def events_table(self) -> str:
        """Return the Spanner ADK events interleaved child table name."""
        return self._events_table

    def ddl_statements(self) -> tuple[str, str]:
        """Return the ``CREATE TABLE`` DDL statements with ``INTERLEAVE IN PARENT ... ON DELETE CASCADE``."""
        sessions_ddl = (
            f"CREATE TABLE IF NOT EXISTS {self._sessions_table} ("
            "session_id STRING(128) NOT NULL, "
            "app_name STRING(256) NOT NULL, "
            "user_id STRING(256) NOT NULL, "
            "state_json STRING(MAX) NOT NULL, "
            "created_at TIMESTAMP NOT NULL, "
            "updated_at TIMESTAMP NOT NULL"
            ") PRIMARY KEY (session_id)"
        )
        events_ddl = (
            f"CREATE TABLE IF NOT EXISTS {self._events_table} ("
            "session_id STRING(128) NOT NULL, "
            "event_id STRING(128) NOT NULL, "
            "author STRING(128) NOT NULL, "
            "content_json STRING(MAX) NOT NULL, "
            "actions_json STRING(MAX) NOT NULL, "
            "timestamp TIMESTAMP NOT NULL"
            f") PRIMARY KEY (session_id, event_id), "
            f"INTERLEAVE IN PARENT {self._sessions_table} ON DELETE CASCADE, "
            f"ROW DELETION POLICY (OLDER_THAN(timestamp, INTERVAL {int(self._ttl_days)} DAY))"
        )
        return sessions_ddl, events_ddl

    def create_tables(self) -> None:
        """Create the parent ``adk_sessions`` and interleaved child ``adk_events`` tables."""
        self._driver.update_ddl(list(self.ddl_statements()))

    def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Mapping[str, Any] | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """Insert a new ADK session row into Spanner."""
        sid = session_id or uuid4().hex
        now = datetime.now(UTC).isoformat()
        state_dict = dict(state or {})
        state_json = encode_json(state_dict)
        self._driver.apply_mutations(
            [
                Mutation.insert_or_update(
                    self._sessions_table,
                    ("session_id", "app_name", "user_id", "state_json", "created_at", "updated_at"),
                    [(sid, app_name, user_id, state_json, now, now)],
                )
            ]
        )
        return {
            "session_id": sid,
            "app_name": app_name,
            "user_id": user_id,
            "state": state_dict,
            "created_at": now,
            "updated_at": now,
            "events": [],
        }

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        """Fetch an ADK session and its interleaved events using a lock-free ReadOnly snapshot."""
        with self._config.provide_session(transaction=False) as session:
            row = session.select_one_or_none(
                f"SELECT session_id, app_name, user_id, state_json, created_at, updated_at "
                f"FROM {self._sessions_table} WHERE session_id = @session_id",
                {"session_id": session_id},
            )
            if row is None:
                return None
            events = self._fetch_events(session, session_id)
            return {
                "session_id": str(row["session_id"]),
                "app_name": str(row["app_name"]),
                "user_id": str(row["user_id"]),
                "state": decode_json(str(row.get("state_json") or "{}")),
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
                "events": events,
            }

    def update_session_state(self, session_id: str, state: Mapping[str, Any]) -> bool:
        """Update the JSON state dictionary of an existing ADK session."""
        now = datetime.now(UTC).isoformat()
        affected = self._driver.execute(
            f"UPDATE {self._sessions_table} SET state_json = @state_json, updated_at = @updated_at "
            "WHERE session_id = @session_id",
            {
                "state_json": encode_json(dict(state)),
                "updated_at": now,
                "session_id": session_id,
            },
        )
        return bool(affected > 0)

    def list_sessions(self, *, app_name: str, user_id: str) -> list[dict[str, Any]]:
        """List all sessions for ``(app_name, user_id)`` via a ReadOnly snapshot."""
        with self._config.provide_session(transaction=False) as session:
            rows = session.select(
                f"SELECT session_id, app_name, user_id, state_json, created_at, updated_at "
                f"FROM {self._sessions_table} "
                "WHERE app_name = @app_name AND user_id = @user_id "
                "ORDER BY updated_at DESC",
                {"app_name": app_name, "user_id": user_id},
            )
            return [
                {
                    "session_id": str(r["session_id"]),
                    "app_name": str(r["app_name"]),
                    "user_id": str(r["user_id"]),
                    "state": decode_json(str(r.get("state_json") or "{}")),
                    "created_at": r.get("created_at"),
                    "updated_at": r.get("updated_at"),
                }
                for r in rows
            ]

    def delete_session(self, session_id: str) -> bool:
        """Delete a session and its interleaved events."""
        self._driver.execute(
            f"DELETE FROM {self._events_table} WHERE session_id = @session_id",
            {"session_id": session_id},
        )
        affected = self._driver.execute(
            f"DELETE FROM {self._sessions_table} WHERE session_id = @session_id",
            {"session_id": session_id},
        )
        return bool(affected > 0)

    def append_event(
        self,
        session_id: str,
        *,
        author: str,
        content: Any,
        actions: Mapping[str, Any] | None = None,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        """Append an event to the interleaved ``adk_events`` table."""
        eid = event_id or uuid4().hex
        now = datetime.now(UTC).isoformat()
        content_json = encode_json(content)
        actions_json = encode_json(dict(actions or {}))
        self._driver.apply_mutations(
            [
                Mutation.insert_or_update(
                    self._events_table,
                    ("session_id", "event_id", "author", "content_json", "actions_json", "timestamp"),
                    [(session_id, eid, author, content_json, actions_json, now)],
                )
            ]
        )
        return {
            "session_id": session_id,
            "event_id": eid,
            "author": author,
            "content": content,
            "actions": dict(actions or {}),
            "timestamp": now,
        }

    def get_events(self, session_id: str) -> list[dict[str, Any]]:
        """Return all events for ``session_id`` ordered by timestamp."""
        with self._config.provide_session(transaction=False) as session:
            return self._fetch_events(session, session_id)

    def _fetch_events(self, session: SpannerSyncDriver, session_id: str) -> list[dict[str, Any]]:
        rows = session.select(
            f"SELECT session_id, event_id, author, content_json, actions_json, timestamp "
            f"FROM {self._events_table} WHERE session_id = @session_id ORDER BY timestamp ASC",
            {"session_id": session_id},
        )
        return [
            {
                "session_id": str(r["session_id"]),
                "event_id": str(r["event_id"]),
                "author": str(r["author"]),
                "content": decode_json(str(r.get("content_json") or "null")),
                "actions": decode_json(str(r.get("actions_json") or "{}")),
                "timestamp": r.get("timestamp"),
            }
            for r in rows
        ]


class SpannerAsyncADKStore:
    """Zero-SDK asynchronous Spanner ADK session and event store."""

    def __init__(
        self,
        config: SpannerAsyncConfig,
        *,
        sessions_table: str = _DEFAULT_SESSIONS_TABLE,
        events_table: str = _DEFAULT_EVENTS_TABLE,
        ttl_days: int = 90,
    ) -> None:
        self._sync = SpannerSyncADKStore(
            SpannerSyncConfig(
                config=config.resolve_spanner_config(),
                storage_backend=config.storage_backend,
                connection_instance=config._resolve_sync_core_driver(),
            ),
            sessions_table=sessions_table,
            events_table=events_table,
            ttl_days=ttl_days,
        )

    def ddl_statements(self) -> tuple[str, str]:
        """Return the ``CREATE TABLE`` DDL statements."""
        return self._sync.ddl_statements()

    async def create_tables(self) -> None:
        """Asynchronously create the ADK session and interleaved event tables."""
        await asyncio.to_thread(self._sync.create_tables)

    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Mapping[str, Any] | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """Asynchronously create an ADK session."""
        return await asyncio.to_thread(
            self._sync.create_session,
            app_name=app_name,
            user_id=user_id,
            state=state,
            session_id=session_id,
        )

    async def get_session(self, session_id: str) -> dict[str, Any] | None:
        """Asynchronously fetch an ADK session and its interleaved events."""
        return await asyncio.to_thread(self._sync.get_session, session_id)

    async def update_session_state(self, session_id: str, state: Mapping[str, Any]) -> bool:
        """Asynchronously update an ADK session's state."""
        return await asyncio.to_thread(self._sync.update_session_state, session_id, state)

    async def list_sessions(self, *, app_name: str, user_id: str) -> list[dict[str, Any]]:
        """Asynchronously list ADK sessions for ``(app_name, user_id)``."""
        return await asyncio.to_thread(self._sync.list_sessions, app_name=app_name, user_id=user_id)

    async def delete_session(self, session_id: str) -> bool:
        """Asynchronously delete an ADK session and its interleaved events."""
        return await asyncio.to_thread(self._sync.delete_session, session_id)

    async def append_event(
        self,
        session_id: str,
        *,
        author: str,
        content: Any,
        actions: Mapping[str, Any] | None = None,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        """Asynchronously append an event to ``adk_events``."""
        return await asyncio.to_thread(
            self._sync.append_event,
            session_id,
            author=author,
            content=content,
            actions=actions,
            event_id=event_id,
        )

    async def get_events(self, session_id: str) -> list[dict[str, Any]]:
        """Asynchronously fetch all events for ``session_id``."""
        return await asyncio.to_thread(self._sync.get_events, session_id)


def _cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b, strict=False))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


class SpannerSyncADKMemoryStore:
    """Zero-SDK synchronous Spanner ADK memory store with ``SCANN`` vector, ``TOKENLIST`` FTS, RRF, and ``PROPERTY GRAPH``."""

    def __init__(
        self,
        config: SpannerSyncConfig,
        *,
        memories_table: str = _DEFAULT_MEMORIES_TABLE,
        vector_dimensions: int = 768,
        graph_name: str | None = "vertebra_adk_memory_graph",
    ) -> None:
        self._config = config
        self._driver = config.provide_driver()
        self._memories_table = memories_table
        self._vector_dimensions = vector_dimensions
        self._graph_name = graph_name

    @property
    def memories_table(self) -> str:
        """Return the Spanner ADK memories table name."""
        return self._memories_table

    @property
    def vector_dimensions(self) -> int:
        """Return the configured ``ARRAY<FLOAT32>(vector_length=>...)`` dimensionality."""
        return self._vector_dimensions

    def ddl_statements(self) -> tuple[str, ...]:
        """Return the Spanner table, ``SCANN`` vector index, ``TOKENLIST`` FTS index, and ``PROPERTY GRAPH`` DDL."""
        stmts: list[str] = [
            (
                f"CREATE TABLE IF NOT EXISTS {self._memories_table} ("
                "memory_id STRING(128) NOT NULL, "
                "app_name STRING(256) NOT NULL, "
                "user_id STRING(256) NOT NULL, "
                "content STRING(MAX) NOT NULL, "
                f"embedding ARRAY<FLOAT32>(vector_length=>{int(self._vector_dimensions)}), "
                "metadata_json STRING(MAX) NOT NULL, "
                "entity_links_json STRING(MAX) NOT NULL, "
                "created_at TIMESTAMP NOT NULL, "
                "content_tokens TOKENLIST AS (TOKENIZE_FULLTEXT(content)) HIDDEN"
                ") PRIMARY KEY (memory_id)"
            ),
            (
                f"CREATE VECTOR INDEX IF NOT EXISTS idx_{self._memories_table}_embedding "
                f"ON {self._memories_table}(embedding) "
                "STORING (user_id, app_name, content, metadata_json) "
                "WHERE embedding IS NOT NULL "
                "OPTIONS (tree_type = 'SCANN', distance_type = 'COSINE')"
            ),
            (
                f"CREATE SEARCH INDEX IF NOT EXISTS idx_{self._memories_table}_fts "
                f"ON {self._memories_table}(content_tokens) "
                "STORING (user_id, app_name, content, metadata_json)"
            ),
        ]
        if self._graph_name:
            stmts.append(
                f"CREATE OR REPLACE PROPERTY GRAPH {self._graph_name} "
                f"NODE TABLES ({self._memories_table} KEY (memory_id))"
            )
        return tuple(stmts)

    def create_tables(self) -> None:
        """Apply the Spanner ADK memory table, vector/FTS index, and property graph DDL."""
        self._driver.update_ddl(list(self.ddl_statements()))

    def store_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        content: str,
        embedding: Sequence[float] | None = None,
        metadata: Mapping[str, Any] | None = None,
        entity_links: Sequence[str] | None = None,
        memory_id: str | None = None,
    ) -> dict[str, Any]:
        """Store a single ADK memory entry via batched deduplication and mutation commit."""
        results = self.store_memories_batch(
            [
                {
                    "memory_id": memory_id or uuid4().hex,
                    "app_name": app_name,
                    "user_id": user_id,
                    "content": content,
                    "embedding": list(embedding) if embedding is not None else None,
                    "metadata": dict(metadata or {}),
                    "entity_links": list(entity_links or ()),
                }
            ]
        )
        return results[0]

    def store_memories_batch(self, entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Batch-deduplicate via ``WHERE memory_id IN UNNEST(@ids)`` and write via ``batch_write``."""
        if not entries:
            return []
        now = datetime.now(UTC).isoformat()
        normalized: list[dict[str, Any]] = []
        ids: list[str] = []
        for entry in entries:
            mid = str(entry.get("memory_id") or uuid4().hex)
            ids.append(mid)
            normalized.append(
                {
                    "memory_id": mid,
                    "app_name": str(entry["app_name"]),
                    "user_id": str(entry["user_id"]),
                    "content": str(entry["content"]),
                    "embedding": list(entry["embedding"]) if entry.get("embedding") is not None else None,
                    "metadata": dict(entry.get("metadata") or {}),
                    "entity_links": list(entry.get("entity_links") or ()),
                    "created_at": now,
                }
            )

        with self._config.provide_session(transaction=False) as session:
            existing_rows = session.select(
                f"SELECT memory_id FROM {self._memories_table} WHERE memory_id IN UNNEST(@ids)",
                {"ids": ids},
            )
        existing_ids = {str(r["memory_id"]) for r in existing_rows}

        values = [
            (
                item["memory_id"],
                item["app_name"],
                item["user_id"],
                item["content"],
                encode_json(item["embedding"]) if item["embedding"] is not None else None,
                encode_json(item["metadata"]),
                encode_json(item["entity_links"]),
                item["created_at"],
            )
            for item in normalized
        ]
        mutation = Mutation.insert_or_update(
            self._memories_table,
            (
                "memory_id",
                "app_name",
                "user_id",
                "content",
                "embedding",
                "metadata_json",
                "entity_links_json",
                "created_at",
            ),
            values,
        )
        self._driver.batch_write([MutationGroup((mutation,))])
        for item in normalized:
            item["deduplicated"] = item["memory_id"] in existing_ids
        return normalized

    def search_memory(
        self,
        user_id: str,
        *,
        query: str | None = None,
        embedding: Sequence[float] | None = None,
        app_name: str | None = None,
        entity_id: str | None = None,
        top_k: int = 5,
        mode: Literal["vector", "fts", "hybrid_rrf", "graph"] = "hybrid_rrf",
        rrf_k: int = 60,
    ) -> list[dict[str, Any]]:
        """Search ADK memories using ``"vector"``, ``"fts"``, ``"hybrid_rrf"`` (``k=60``), or ``"graph"`` mode."""
        with self._config.provide_session(transaction=False) as session:
            sql = (
                f"SELECT memory_id, app_name, user_id, content, embedding, "
                f"metadata_json, entity_links_json, created_at "
                f"FROM {self._memories_table} WHERE user_id = @user_id"
            )
            params: dict[str, Any] = {"user_id": user_id}
            if app_name is not None:
                sql += " AND app_name = @app_name"
                params["app_name"] = app_name
            raw_rows = session.select(sql, params)

        hydrated: list[dict[str, Any]] = []
        for r in raw_rows:
            raw_emb = r.get("embedding")
            if isinstance(raw_emb, str):
                parsed_emb = decode_json(raw_emb)
            elif isinstance(raw_emb, list):
                parsed_emb = raw_emb
            else:
                parsed_emb = None
            raw_links = r.get("entity_links_json")
            links = decode_json(raw_links) if isinstance(raw_links, str) and raw_links else []
            hydrated.append(
                {
                    "memory_id": str(r["memory_id"]),
                    "app_name": str(r["app_name"]),
                    "user_id": str(r["user_id"]),
                    "content": str(r["content"]),
                    "embedding": parsed_emb,
                    "metadata": decode_json(str(r.get("metadata_json") or "{}")),
                    "entity_links": links if isinstance(links, list) else [],
                    "created_at": r.get("created_at"),
                }
            )

        if mode == "vector":
            q_vec = list(embedding or ())
            scored = []
            for item in hydrated:
                item_vec = item.get("embedding") or []
                sim = _cosine_similarity(q_vec, item_vec)
                copy_item = dict(item)
                copy_item["score"] = sim
                copy_item["distance"] = 1.0 - sim
                scored.append(copy_item)
            scored.sort(key=lambda x: float(x["score"]), reverse=True)
            return scored[:top_k]

        if mode == "fts":
            terms = [t.lower() for t in (query or "").split() if t.strip()]
            scored = []
            for item in hydrated:
                text_lower = item["content"].lower()
                hits = sum(1 for t in terms if t in text_lower)
                if hits > 0 or not terms:
                    copy_item = dict(item)
                    copy_item["score"] = float(hits)
                    scored.append(copy_item)
            scored.sort(key=lambda x: float(x["score"]), reverse=True)
            return scored[:top_k]

        if mode == "graph":
            target_entity = (entity_id or query or "").lower()
            matched = []
            for item in hydrated:
                links_lower = [str(x).lower() for x in item.get("entity_links", [])]
                if not target_entity or target_entity in links_lower or target_entity in item["content"].lower():
                    copy_item = dict(item)
                    copy_item["score"] = 1.0 if target_entity in links_lower else 0.5
                    matched.append(copy_item)
            matched.sort(key=lambda x: float(x["score"]), reverse=True)
            return matched[:top_k]

        vec_ranked = (
            self.search_memory(user_id, embedding=embedding, app_name=app_name, top_k=max(top_k * 2, 10), mode="vector")
            if embedding is not None
            else []
        )
        fts_ranked = (
            self.search_memory(user_id, query=query, app_name=app_name, top_k=max(top_k * 2, 10), mode="fts")
            if query
            else []
        )
        vec_ranks = {item["memory_id"]: rank for rank, item in enumerate(vec_ranked, start=1)}
        fts_ranks = {item["memory_id"]: rank for rank, item in enumerate(fts_ranked, start=1)}

        fused: list[dict[str, Any]] = []
        for item in hydrated:
            mid = item["memory_id"]
            score = 0.0
            if mid in vec_ranks:
                score += 1.0 / (rrf_k + vec_ranks[mid])
            if mid in fts_ranks:
                score += 1.0 / (rrf_k + fts_ranks[mid])
            if score > 0.0 or (not vec_ranks and not fts_ranks):
                copy_item = dict(item)
                copy_item["score"] = score
                copy_item["rrf_score"] = score
                fused.append(copy_item)
        fused.sort(key=lambda x: float(x["score"]), reverse=True)
        return fused[:top_k]


class SpannerAsyncADKMemoryStore:
    """Zero-SDK asynchronous Spanner ADK memory store."""

    def __init__(
        self,
        config: SpannerAsyncConfig,
        *,
        memories_table: str = _DEFAULT_MEMORIES_TABLE,
        vector_dimensions: int = 768,
        graph_name: str | None = "vertebra_adk_memory_graph",
    ) -> None:
        self._sync = SpannerSyncADKMemoryStore(
            SpannerSyncConfig(
                config=config.resolve_spanner_config(),
                storage_backend=config.storage_backend,
                connection_instance=config._resolve_sync_core_driver(),
            ),
            memories_table=memories_table,
            vector_dimensions=vector_dimensions,
            graph_name=graph_name,
        )

    def ddl_statements(self) -> tuple[str, ...]:
        """Return the Spanner ADK memory DDL statements."""
        return self._sync.ddl_statements()

    async def create_tables(self) -> None:
        """Asynchronously apply the Spanner ADK memory DDL statements."""
        await asyncio.to_thread(self._sync.create_tables)

    async def store_memory(
        self,
        *,
        app_name: str,
        user_id: str,
        content: str,
        embedding: Sequence[float] | None = None,
        metadata: Mapping[str, Any] | None = None,
        entity_links: Sequence[str] | None = None,
        memory_id: str | None = None,
    ) -> dict[str, Any]:
        """Asynchronously store an ADK memory item."""
        return await asyncio.to_thread(
            self._sync.store_memory,
            app_name=app_name,
            user_id=user_id,
            content=content,
            embedding=embedding,
            metadata=metadata,
            entity_links=entity_links,
            memory_id=memory_id,
        )

    async def store_memories_batch(self, entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Asynchronously store a batch of ADK memory items."""
        return await asyncio.to_thread(self._sync.store_memories_batch, entries)

    async def search_memory(
        self,
        user_id: str,
        *,
        query: str | None = None,
        embedding: Sequence[float] | None = None,
        app_name: str | None = None,
        entity_id: str | None = None,
        top_k: int = 5,
        mode: Literal["vector", "fts", "hybrid_rrf", "graph"] = "hybrid_rrf",
        rrf_k: int = 60,
    ) -> list[dict[str, Any]]:
        """Asynchronously search ADK memories."""
        return await asyncio.to_thread(
            self._sync.search_memory,
            user_id,
            query=query,
            embedding=embedding,
            app_name=app_name,
            entity_id=entity_id,
            top_k=top_k,
            mode=mode,
            rrf_k=rrf_k,
        )


class SpannerSyncADKArtifactStore:
    """Zero-SDK synchronous Spanner ADK artifact store persisting binary blobs in ``ObjectStoreBackend``."""

    def __init__(
        self,
        config: SpannerSyncConfig,
        *,
        artifacts_table: str = _DEFAULT_ARTIFACTS_TABLE,
        storage_backend: ObjectStoreBackend | str | None = None,
        prefix: str = "adk/artifacts",
    ) -> None:
        self._config = config
        self._driver = config.provide_driver()
        self._artifacts_table = artifacts_table
        resolved = _resolve_storage_backend(storage_backend) or config.resolve_storage_backend()
        if resolved is None:
            resolved = ObjectStoreBackend("memory://")
        self._storage = resolved
        self._prefix = prefix.strip("/")

    def ddl_statements(self) -> tuple[str, ...]:
        """Return the ``CREATE TABLE`` DDL for Spanner ADK artifact metadata."""
        return (
            f"CREATE TABLE IF NOT EXISTS {self._artifacts_table} ("
            "session_id STRING(128) NOT NULL, "
            "artifact_key STRING(256) NOT NULL, "
            "version INT64 NOT NULL, "
            "uri STRING(1024) NOT NULL, "
            "content_type STRING(256) NOT NULL, "
            "size_bytes INT64 NOT NULL, "
            "created_at TIMESTAMP NOT NULL"
            ") PRIMARY KEY (session_id, artifact_key, version)",
        )

    def create_tables(self) -> None:
        """Create the Spanner ADK artifacts metadata table."""
        self._driver.update_ddl(list(self.ddl_statements()))

    def save_artifact(
        self,
        session_id: str,
        artifact_key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        version: int | None = None,
    ) -> dict[str, Any]:
        """Write binary artifact payload to ``ObjectStoreBackend`` and record metadata in Spanner."""
        if version is None:
            existing = self.list_artifacts(session_id, artifact_key=artifact_key)
            version = (max((int(r["version"]) for r in existing), default=0)) + 1
        object_key = f"{self._prefix}/{session_id}/{artifact_key}/v{version}"
        self._storage.sync_store.put(object_key, data)
        uri = f"{self._storage.store_uri.rstrip('/')}/{object_key}"
        now = datetime.now(UTC).isoformat()
        self._driver.apply_mutations(
            [
                Mutation.insert_or_update(
                    self._artifacts_table,
                    ("session_id", "artifact_key", "version", "uri", "content_type", "size_bytes", "created_at"),
                    [(session_id, artifact_key, int(version), uri, content_type, len(data), now)],
                )
            ]
        )
        return {
            "session_id": session_id,
            "artifact_key": artifact_key,
            "version": int(version),
            "uri": uri,
            "object_key": object_key,
            "content_type": content_type,
            "size_bytes": len(data),
            "created_at": now,
        }

    def get_artifact(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
    ) -> dict[str, Any] | None:
        """Fetch artifact metadata from Spanner and payload bytes from ``ObjectStoreBackend``."""
        meta = self._fetch_artifact_meta(session_id, artifact_key, version=version)
        if meta is None:
            return None
        object_key = f"{self._prefix}/{session_id}/{artifact_key}/v{meta['version']}"
        payload = self._storage.sync_store.get(object_key)
        meta["data"] = payload
        meta["object_ref"] = self.get_artifact_ref(session_id, artifact_key, version=int(meta["version"]))
        return meta

    def get_artifact_ref(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
        expires_in_secs: int = 3600,
    ) -> ObjectRef | None:
        """Return a zero-RTT presigned ``ObjectRef`` for the artifact."""
        meta = self._fetch_artifact_meta(session_id, artifact_key, version=version)
        if meta is None:
            return None
        object_key = f"{self._prefix}/{session_id}/{artifact_key}/v{meta['version']}"
        signed_url: str | None = None
        if self._storage.supports_signing:
            try:
                signed_url = self._storage.sign_sync(object_key, expires_in=expires_in_secs)
            except Exception:
                signed_url = None
        return ObjectRef(
            uri=str(meta["uri"]),
            version=str(meta["version"]),
            content_type=str(meta["content_type"]),
            size=int(meta["size_bytes"]),
            updated=str(meta.get("created_at") or ""),
            signed_url=signed_url,
        )

    def list_artifacts(self, session_id: str, *, artifact_key: str | None = None) -> list[dict[str, Any]]:
        """List artifact versions for ``session_id``."""
        with self._config.provide_session(transaction=False) as session:
            if artifact_key is not None:
                rows = session.select(
                    f"SELECT session_id, artifact_key, version, uri, content_type, size_bytes, created_at "
                    f"FROM {self._artifacts_table} "
                    "WHERE session_id = @session_id AND artifact_key = @artifact_key "
                    "ORDER BY version DESC",
                    {"session_id": session_id, "artifact_key": artifact_key},
                )
            else:
                rows = session.select(
                    f"SELECT session_id, artifact_key, version, uri, content_type, size_bytes, created_at "
                    f"FROM {self._artifacts_table} "
                    "WHERE session_id = @session_id "
                    "ORDER BY artifact_key ASC, version DESC",
                    {"session_id": session_id},
                )
            return [dict(r) for r in rows]

    def delete_artifact(self, session_id: str, artifact_key: str) -> bool:
        """Delete all versions of ``artifact_key`` in Spanner and ``ObjectStoreBackend``."""
        versions = self.list_artifacts(session_id, artifact_key=artifact_key)
        for v in versions:
            object_key = f"{self._prefix}/{session_id}/{artifact_key}/v{v['version']}"
            with suppress(Exception):
                self._storage.sync_store.delete(object_key)
        affected = self._driver.execute(
            f"DELETE FROM {self._artifacts_table} WHERE session_id = @session_id AND artifact_key = @artifact_key",
            {"session_id": session_id, "artifact_key": artifact_key},
        )
        return bool(affected > 0)

    def _fetch_artifact_meta(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
    ) -> dict[str, Any] | None:
        with self._config.provide_session(transaction=False) as session:
            if version is not None:
                row = session.select_one_or_none(
                    f"SELECT session_id, artifact_key, version, uri, content_type, size_bytes, created_at "
                    f"FROM {self._artifacts_table} "
                    "WHERE session_id = @session_id AND artifact_key = @artifact_key AND version = @version",
                    {"session_id": session_id, "artifact_key": artifact_key, "version": int(version)},
                )
            else:
                row = session.select_one_or_none(
                    f"SELECT session_id, artifact_key, version, uri, content_type, size_bytes, created_at "
                    f"FROM {self._artifacts_table} "
                    "WHERE session_id = @session_id AND artifact_key = @artifact_key "
                    "ORDER BY version DESC LIMIT 1",
                    {"session_id": session_id, "artifact_key": artifact_key},
                )
            return dict(row) if row is not None else None


class SpannerAsyncADKArtifactStore:
    """Zero-SDK asynchronous Spanner ADK artifact store."""

    def __init__(
        self,
        config: SpannerAsyncConfig,
        *,
        artifacts_table: str = _DEFAULT_ARTIFACTS_TABLE,
        storage_backend: ObjectStoreBackend | str | None = None,
        prefix: str = "adk/artifacts",
    ) -> None:
        self._sync = SpannerSyncADKArtifactStore(
            SpannerSyncConfig(
                config=config.resolve_spanner_config(),
                storage_backend=config.storage_backend,
                connection_instance=config._resolve_sync_core_driver(),
            ),
            artifacts_table=artifacts_table,
            storage_backend=storage_backend,
            prefix=prefix,
        )

    def ddl_statements(self) -> tuple[str, ...]:
        """Return the ``CREATE TABLE`` DDL for Spanner ADK artifact metadata."""
        return self._sync.ddl_statements()

    async def create_tables(self) -> None:
        """Asynchronously create the ADK artifacts table."""
        await asyncio.to_thread(self._sync.create_tables)

    async def save_artifact(
        self,
        session_id: str,
        artifact_key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        version: int | None = None,
    ) -> dict[str, Any]:
        """Asynchronously save an ADK artifact."""
        return await asyncio.to_thread(
            self._sync.save_artifact,
            session_id,
            artifact_key,
            data,
            content_type=content_type,
            version=version,
        )

    async def get_artifact(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
    ) -> dict[str, Any] | None:
        """Asynchronously fetch an ADK artifact."""
        return await asyncio.to_thread(self._sync.get_artifact, session_id, artifact_key, version=version)

    async def get_artifact_ref(
        self,
        session_id: str,
        artifact_key: str,
        *,
        version: int | None = None,
        expires_in_secs: int = 3600,
    ) -> ObjectRef | None:
        """Asynchronously return a zero-RTT presigned ``ObjectRef``."""
        return await asyncio.to_thread(
            self._sync.get_artifact_ref,
            session_id,
            artifact_key,
            version=version,
            expires_in_secs=expires_in_secs,
        )

    async def list_artifacts(self, session_id: str, *, artifact_key: str | None = None) -> list[dict[str, Any]]:
        """Asynchronously list ADK artifacts for ``session_id``."""
        return await asyncio.to_thread(self._sync.list_artifacts, session_id, artifact_key=artifact_key)

    async def delete_artifact(self, session_id: str, artifact_key: str) -> bool:
        """Asynchronously delete an ADK artifact."""
        return await asyncio.to_thread(self._sync.delete_artifact, session_id, artifact_key)


class SpannerSyncStore:
    """Sharded synchronous Spanner key-value / Litestar session store with ``FARM_FINGERPRINT`` prefix and TTL."""

    def __init__(
        self,
        config: SpannerSyncConfig,
        *,
        table_name: str = _DEFAULT_KV_STORE_TABLE,
        shard_count: int = 16,
    ) -> None:
        self._config = config
        self._driver = config.provide_driver()
        self._table_name = table_name
        self._shard_count = max(1, shard_count)

    @property
    def table_name(self) -> str:
        """Return the Spanner KV store table name."""
        return self._table_name

    def compute_shard_id(self, key: str) -> int:
        """Compute the ``FARM_FINGERPRINT`` hash shard prefix for ``key``."""
        return _farm_fingerprint_shard(key, self._shard_count)

    def ddl_statements(self) -> tuple[str, ...]:
        """Return the ``CREATE TABLE`` DDL with ``PRIMARY KEY (shard_id, store_key)`` and ``ROW DELETION POLICY``."""
        if self._driver.core_driver.config.dialect == "spangres":
            return (
                f"CREATE TABLE IF NOT EXISTS {self._table_name} ("
                "shard_id BIGINT NOT NULL, "
                "store_key VARCHAR(512) NOT NULL, "
                "value_text TEXT NOT NULL, "
                "expires_at DOUBLE PRECISION, "
                "updated_at TIMESTAMPTZ NOT NULL, "
                "PRIMARY KEY (shard_id, store_key)"
                ") TTL INTERVAL '0 days' ON updated_at",
            )
        return (
            f"CREATE TABLE IF NOT EXISTS {self._table_name} ("
            "shard_id INT64 NOT NULL, "
            "store_key STRING(512) NOT NULL, "
            "value_text STRING(MAX) NOT NULL, "
            "expires_at FLOAT64, "
            "updated_at TIMESTAMP NOT NULL"
            ") PRIMARY KEY (shard_id, store_key), "
            "ROW DELETION POLICY (OLDER_THAN(updated_at, INTERVAL 30 DAY))",
        )

    def create_tables(self) -> None:
        """Create the sharded Spanner KV store table."""
        self._driver.update_ddl(list(self.ddl_statements()))

    def set(
        self,
        key: str,
        value: str | bytes,
        *,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Upsert a key-value entry using ``INSERT OR UPDATE`` / ``Mutation.insert_or_update``."""
        shard_id = self.compute_shard_id(key)
        val_str = value.decode("utf-8") if isinstance(value, bytes) else str(value)
        ttl_secs: float | None = None
        if isinstance(expires_in, timedelta):
            ttl_secs = expires_in.total_seconds()
        elif expires_in is not None:
            ttl_secs = float(expires_in)
        exp_ts = (time.time() + ttl_secs) if ttl_secs is not None else None
        now = datetime.now(UTC).isoformat()
        self._driver.apply_mutations(
            [
                Mutation.insert_or_update(
                    self._table_name,
                    ("shard_id", "store_key", "value_text", "expires_at", "updated_at"),
                    [(shard_id, key, val_str, exp_ts, now)],
                )
            ]
        )

    def get(self, key: str, *, renew_for: int | timedelta | None = None) -> bytes | None:
        """Fetch an unexpired value by ``(shard_id, store_key)`` via a ReadOnly snapshot."""
        shard_id = self.compute_shard_id(key)
        now_ts = time.time()
        with self._config.provide_session(transaction=False) as session:
            row = session.select_one_or_none(
                f"SELECT value_text, expires_at FROM {self._table_name} "
                "WHERE shard_id = @shard_id AND store_key = @store_key",
                {"shard_id": shard_id, "store_key": key},
            )
        if row is None:
            return None
        exp = row.get("expires_at")
        if exp is not None and float(exp) <= now_ts:
            return None
        raw_val = str(row["value_text"]).encode("utf-8")
        if renew_for is not None:
            self.set(key, raw_val, expires_in=renew_for)
        return raw_val

    def delete(self, key: str) -> bool:
        """Delete a key from its deterministic shard."""
        shard_id = self.compute_shard_id(key)
        affected = self._driver.execute(
            f"DELETE FROM {self._table_name} WHERE shard_id = @shard_id AND store_key = @store_key",
            {"shard_id": shard_id, "store_key": key},
        )
        return bool(affected > 0)

    def delete_all(self) -> int:
        """Delete all entries in the KV store table via Partitioned DML."""
        return self._driver.execute_partitioned_dml(f"DELETE FROM {self._table_name} WHERE 1=1")

    def delete_expired(self) -> int:
        """Delete expired entries via Partitioned DML."""
        return self._driver.execute_partitioned_dml(
            f"DELETE FROM {self._table_name} WHERE expires_at IS NOT NULL AND expires_at <= @now_ts",
            {"now_ts": time.time()},
        )

    def exists(self, key: str) -> bool:
        """Return whether ``key`` exists and has not expired."""
        return self.get(key) is not None

    def expires_in(self, key: str) -> int | None:
        """Return remaining TTL in seconds for ``key``, or ``None`` if persistent or missing."""
        shard_id = self.compute_shard_id(key)
        now_ts = time.time()
        with self._config.provide_session(transaction=False) as session:
            row = session.select_one_or_none(
                f"SELECT expires_at FROM {self._table_name} WHERE shard_id = @shard_id AND store_key = @store_key",
                {"shard_id": shard_id, "store_key": key},
            )
        if row is None or row.get("expires_at") is None:
            return None
        remaining = int(float(row["expires_at"]) - now_ts)
        return max(0, remaining)


class SpannerAsyncStore:
    """Sharded asynchronous Spanner key-value / Litestar session store."""

    def __init__(
        self,
        config: SpannerAsyncConfig,
        *,
        table_name: str = _DEFAULT_KV_STORE_TABLE,
        shard_count: int = 16,
    ) -> None:
        self._sync = SpannerSyncStore(
            SpannerSyncConfig(
                config=config.resolve_spanner_config(),
                storage_backend=config.storage_backend,
                connection_instance=config._resolve_sync_core_driver(),
            ),
            table_name=table_name,
            shard_count=shard_count,
        )

    def compute_shard_id(self, key: str) -> int:
        """Compute the ``FARM_FINGERPRINT`` hash shard prefix for ``key``."""
        return self._sync.compute_shard_id(key)

    def ddl_statements(self) -> tuple[str, ...]:
        """Return the ``CREATE TABLE`` DDL."""
        return self._sync.ddl_statements()

    async def create_tables(self) -> None:
        """Asynchronously create the sharded Spanner KV store table."""
        await asyncio.to_thread(self._sync.create_tables)

    async def set(
        self,
        key: str,
        value: str | bytes,
        *,
        expires_in: int | timedelta | None = None,
    ) -> None:
        """Asynchronously upsert a key-value entry."""
        await asyncio.to_thread(self._sync.set, key, value, expires_in=expires_in)

    async def get(self, key: str, *, renew_for: int | timedelta | None = None) -> bytes | None:
        """Asynchronously fetch an unexpired value."""
        return await asyncio.to_thread(self._sync.get, key, renew_for=renew_for)

    async def delete(self, key: str) -> bool:
        """Asynchronously delete a key."""
        return await asyncio.to_thread(self._sync.delete, key)

    async def delete_all(self) -> int:
        """Asynchronously delete all entries."""
        return await asyncio.to_thread(self._sync.delete_all)

    async def delete_expired(self) -> int:
        """Asynchronously delete expired entries."""
        return await asyncio.to_thread(self._sync.delete_expired)

    async def exists(self, key: str) -> bool:
        """Asynchronously check whether ``key`` exists."""
        return await asyncio.to_thread(self._sync.exists, key)

    async def expires_in(self, key: str) -> int | None:
        """Asynchronously return remaining TTL in seconds."""
        return await asyncio.to_thread(self._sync.expires_in, key)


class SpannerSyncEventQueueStore:
    """Zero-SDK synchronous Spanner event queue store with ``FARM_FINGERPRINT`` sharding, covering index, and native TTL."""

    def __init__(
        self,
        config: SpannerSyncConfig,
        *,
        queue_table: str = _DEFAULT_EVENT_QUEUE_TABLE,
        cleanup_on_ack: bool = False,
        shard_count: int = 16,
        retention_days: int = 7,
    ) -> None:
        self._config = config
        self._driver = config.provide_driver()
        self._queue_table = queue_table
        self._cleanup_on_ack = cleanup_on_ack
        self._shard_count = max(1, shard_count)
        self._retention_days = retention_days

    @property
    def cleanup_on_ack(self) -> bool:
        """Return whether acknowledged events are immediately deleted rather than expired via ``ROW DELETION POLICY``."""
        return self._cleanup_on_ack

    def ddl_statements(self) -> tuple[str, str]:
        """Return the sharded queue table and ``STORING`` / ``INCLUDE`` covering index DDL."""
        if self._driver.core_driver.config.dialect == "spangres":
            return (
                (
                    f"CREATE TABLE IF NOT EXISTS {self._queue_table} ("
                    "shard_id BIGINT NOT NULL, "
                    "event_id VARCHAR(128) NOT NULL, "
                    "channel VARCHAR(256) NOT NULL, "
                    "payload_json TEXT NOT NULL, "
                    "status VARCHAR(32) NOT NULL, "
                    "published_at TIMESTAMPTZ NOT NULL, "
                    "expires_at TIMESTAMPTZ NOT NULL, "
                    "PRIMARY KEY (shard_id, event_id)"
                    ") TTL INTERVAL '0 days' ON expires_at"
                ),
                (
                    f"CREATE INDEX IF NOT EXISTS idx_{self._queue_table}_channel_status "
                    f"ON {self._queue_table}(channel, status, published_at) "
                    "INCLUDE (payload_json, expires_at)"
                ),
            )
        return (
            (
                f"CREATE TABLE IF NOT EXISTS {self._queue_table} ("
                "shard_id INT64 NOT NULL, "
                "event_id STRING(128) NOT NULL, "
                "channel STRING(256) NOT NULL, "
                "payload_json STRING(MAX) NOT NULL, "
                "status STRING(32) NOT NULL, "
                "published_at TIMESTAMP NOT NULL, "
                "expires_at TIMESTAMP NOT NULL"
                ") PRIMARY KEY (shard_id, event_id), "
                "ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 0 DAY))"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS idx_{self._queue_table}_channel_status "
                f"ON {self._queue_table}(channel, status, published_at) "
                "STORING (payload_json, expires_at)"
            ),
        )

    def create_tables(self) -> None:
        """Apply the Spanner event queue table and covering index DDL."""
        self._driver.update_ddl(list(self.ddl_statements()))

    def publish(
        self,
        channel: str,
        payload: Mapping[str, Any],
        *,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        """Publish an event to ``channel`` under a ``FARM_FINGERPRINT`` shard prefix."""
        eid = event_id or uuid4().hex
        shard_id = _farm_fingerprint_shard(eid, self._shard_count)
        now_dt = datetime.now(UTC)
        exp_dt = now_dt + timedelta(days=self._retention_days)
        payload_json = encode_json(dict(payload))
        self._driver.apply_mutations(
            [
                Mutation.insert_or_update(
                    self._queue_table,
                    ("shard_id", "event_id", "channel", "payload_json", "status", "published_at", "expires_at"),
                    [(shard_id, eid, channel, payload_json, "PENDING", now_dt.isoformat(), exp_dt.isoformat())],
                )
            ]
        )
        return {
            "shard_id": shard_id,
            "event_id": eid,
            "channel": channel,
            "payload": dict(payload),
            "status": "PENDING",
            "published_at": now_dt.isoformat(),
            "expires_at": exp_dt.isoformat(),
        }

    def consume(self, channel: str, *, limit: int = 50) -> list[dict[str, Any]]:
        """Read pending events for ``channel`` via a lock-free ReadOnly snapshot."""
        with self._config.provide_session(transaction=False) as session:
            rows = session.select(
                f"SELECT shard_id, event_id, channel, payload_json, status, published_at, expires_at "
                f"FROM {self._queue_table} "
                "WHERE channel = @channel AND status = @status "
                "ORDER BY published_at ASC LIMIT @limit",
                {"channel": channel, "status": "PENDING", "limit": int(limit)},
            )
            return [
                {
                    "shard_id": int(r["shard_id"]),
                    "event_id": str(r["event_id"]),
                    "channel": str(r["channel"]),
                    "payload": decode_json(str(r.get("payload_json") or "{}")),
                    "status": str(r["status"]),
                    "published_at": r.get("published_at"),
                    "expires_at": r.get("expires_at"),
                }
                for r in rows
            ]

    def ack(self, event_id: str) -> bool:
        """Acknowledge ``event_id`` inside ``run_in_transaction`` without full-table delete scans."""
        shard_id = _farm_fingerprint_shard(event_id, self._shard_count)

        def _tx_ack(tx: Any) -> int:
            if self._cleanup_on_ack:
                return int(
                    tx.execute(
                        f"DELETE FROM {self._queue_table} WHERE shard_id = @shard_id AND event_id = @event_id",
                        {"shard_id": shard_id, "event_id": event_id},
                    )
                )
            return int(
                tx.execute(
                    f"UPDATE {self._queue_table} SET status = @status "
                    "WHERE shard_id = @shard_id AND event_id = @event_id",
                    {"status": "ACKED", "shard_id": shard_id, "event_id": event_id},
                )
            )

        affected = self._driver.run_in_transaction(_tx_ack)
        return bool(affected and affected > 0)


class SpannerAsyncEventQueueStore:
    """Zero-SDK asynchronous Spanner event queue store."""

    def __init__(
        self,
        config: SpannerAsyncConfig,
        *,
        queue_table: str = _DEFAULT_EVENT_QUEUE_TABLE,
        cleanup_on_ack: bool = False,
        shard_count: int = 16,
        retention_days: int = 7,
    ) -> None:
        self._sync = SpannerSyncEventQueueStore(
            SpannerSyncConfig(
                config=config.resolve_spanner_config(),
                storage_backend=config.storage_backend,
                connection_instance=config._resolve_sync_core_driver(),
            ),
            queue_table=queue_table,
            cleanup_on_ack=cleanup_on_ack,
            shard_count=shard_count,
            retention_days=retention_days,
        )

    def ddl_statements(self) -> tuple[str, str]:
        """Return the sharded queue table and covering index DDL."""
        return self._sync.ddl_statements()

    async def create_tables(self) -> None:
        """Asynchronously apply the Spanner event queue DDL."""
        await asyncio.to_thread(self._sync.create_tables)

    async def publish(
        self,
        channel: str,
        payload: Mapping[str, Any],
        *,
        event_id: str | None = None,
    ) -> dict[str, Any]:
        """Asynchronously publish an event to ``channel``."""
        return await asyncio.to_thread(self._sync.publish, channel, payload, event_id=event_id)

    async def consume(self, channel: str, *, limit: int = 50) -> list[dict[str, Any]]:
        """Asynchronously consume pending events for ``channel``."""
        return await asyncio.to_thread(self._sync.consume, channel, limit=limit)

    async def ack(self, event_id: str) -> bool:
        """Asynchronously acknowledge ``event_id``."""
        return await asyncio.to_thread(self._sync.ack, event_id)


def _spanner_statement_config(dialect: Literal["googlesql", "spangres"]) -> StatementConfig:
    """Use supported Spanner parsing while retaining native named binds."""
    style = ParameterStyle.NUMERIC if dialect == "spangres" else ParameterStyle.NAMED_AT
    return StatementConfig(
        dialect="spangres" if dialect == "spangres" else "spanner",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=style,
            supported_parameter_styles={style},
            supported_execution_parameter_styles={style},
        ),
    )
