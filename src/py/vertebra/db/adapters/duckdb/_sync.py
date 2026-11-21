"""Synchronous DuckDB analytical driver backed by `vertebra._duckdb`."""

from __future__ import annotations

import inspect
import itertools
import time
from collections.abc import Callable, Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, NoReturn, Self

from vertebra._vertebra import InterfaceError, NotFoundError
from vertebra.db.adapters.duckdb.config import DuckDBConfig, DuckDBSecretConfig
from vertebra.db.adapters.duckdb.core import DUCKDB_DIALECT_TAG, DuckDBCore
from vertebra.db.adapters.duckdb.pipeline import SyncDuckDBPipeline
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
    _table_to_rows_and_cols,
)
from vertebra.db.driver import SyncDatabaseDriver
from vertebra.typing import ImproperConfigurationError
from vertebra.utils.module_loader import ensure_module
from vertebra.utils.schema import to_value_type
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._duckdb import SyncDuckDBClient, SyncDuckDBCursor

__all__ = ("SyncDuckDBDriver",)

_TX_SAVEPOINT_COUNTER = itertools.count(1)


def _raise_closed(*args: Any, **kwargs: Any) -> NoReturn:
    """Raise the closed-driver error in place of a native call."""
    msg = "DuckDB driver is closed"
    raise InterfaceError(msg)


class SyncDuckDBDriver(SyncDatabaseDriver[DuckDBConfig]):
    """Synchronous DuckDB analytical driver backed by `vertebra._duckdb.SyncDuckDBClient`."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG

    __slots__ = (
        "_client",
        "_dynamic_secrets",
        "_executed_bootstrap_statements",
        "_fetch_dicts",
        "_fetch_rows",
        "_fetch_value",
        "_run_dml",
        "_secret_generations",
        "_secret_refresh_times",
    )

    _client: SyncDuckDBClient | None
    _fetch_dicts: Callable[..., Any]
    _fetch_rows: Callable[..., Any]
    _fetch_value: Callable[..., Any]
    _run_dml: Callable[..., Any]

    def __init__(
        self,
        config: DuckDBConfig | None = None,
        *,
        broker: Any = None,
        _client: SyncDuckDBClient | None = None,
    ) -> None:
        resolved = config or DuckDBConfig()
        if int(resolved.pool_max_size) <= 0:
            msg = "DuckDB pool_max_size must be at least 1"
            raise InterfaceError(msg)
        super().__init__(resolved, broker=broker)
        normalized_secrets = self.config.normalized_secrets()
        self._dynamic_secrets: tuple[DuckDBSecretConfig, ...] = tuple(
            s for s in normalized_secrets if s.value_provider is not None
        )
        self._secret_refresh_times: dict[str, float] = {}
        self._secret_generations: dict[str, int] = {}
        self._executed_bootstrap_statements: tuple[str, ...] = tuple(self.config.bootstrap_statements())

        if _client is not None:
            self._bind_client(_client)
        else:
            native_mod = ensure_module(
                "vertebra._duckdb",
                package="vertebra-core[duckdb]",
                extra="duckdb",
            )
            init_stmts = [s.to_create_secret_sql() for s in normalized_secrets if s.value_provider is None]
            init_stmts.extend(self.config.init_statements)
            self._bind_client(
                native_mod.SyncDuckDBClient(
                    self.config.resolved_path(),
                    read_only=self.config.read_only,
                    max_connections=int(self.config.pool_max_size),
                    max_memory_bytes=max(1, int(self.config.memory_limit_mb)) * 1024 * 1024,
                    threads=max(1, int(self.config.threads)),
                    temp_directory=self.config.temp_directory,
                    parquet_metadata_cache=bool(self.config.parquet_metadata_cache),
                    httpfs_connection_caching=bool(self.config.httpfs_connection_caching),
                    enable_http_metadata_cache=bool(self.config.enable_http_metadata_cache),
                    config=self.config.config or None,
                    extensions=list(self.config.extensions) or None,
                    init_statements=init_stmts or None,
                )
            )
        try:
            self._refresh_dynamic_secrets(force=True)
        except BaseException:
            client = self._client
            self._bind_client(None)
            self._closed = True
            if client is not None:
                client.close()
            raise

    def _bind_client(self, client: SyncDuckDBClient | None) -> None:
        """Set the client and bind its single-call methods, or the closed-driver error when there is none."""
        self._client = client
        if client is None:
            self._fetch_dicts = self._fetch_rows = self._fetch_value = self._run_dml = _raise_closed
            return
        self._fetch_dicts = client.fetch_dicts
        self._fetch_rows = client.fetch_rows
        self._fetch_value = client.fetch_value
        self._run_dml = client.execute_dml

    @property
    def executed_bootstrap_statements(self) -> tuple[str, ...]:
        """Return the post-initialization `SET` statements executed on startup."""
        return self._executed_bootstrap_statements

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit DuckDB transaction is active on the writer session."""
        client = self._client
        return bool(client.in_transaction) if client is not None else False

    def cursor(self, *, lazy_begin: bool = False) -> SyncDuckDBCursor:
        """Create a native synchronous DBAPI-style cursor on the driver's client."""
        return self._require_client().cursor(lazy_begin=lazy_begin)

    def pipeline(self, *, transaction: bool = True) -> SyncDuckDBPipeline:
        """Create a synchronous delayed-commit query pipeline bound to this driver."""
        return SyncDuckDBPipeline(self, transaction=transaction)

    def begin(self) -> None:
        """Begin an explicit DuckDB transaction on the writer session."""
        client = self._require_client()
        if not client.in_transaction:
            client.begin()

    def commit(self) -> None:
        """Commit the active DuckDB transaction when one is open."""
        client = self._require_client()
        if client.in_transaction:
            client.commit()

    def rollback(self) -> None:
        """Roll back the active DuckDB transaction when one is open."""
        client = self._require_client()
        if client.in_transaction:
            client.rollback()

    def savepoint(self, name: str) -> None:
        """Create a transactional savepoint on the active DuckDB transaction."""
        self._require_client().savepoint(name)

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back the active DuckDB transaction to savepoint `name`."""
        self._require_client().rollback_to_savepoint(name)

    def release_savepoint(self, name: str) -> None:
        """Release savepoint `name` on the active DuckDB transaction."""
        self._require_client().release_savepoint(name)

    @contextmanager
    def transaction(self) -> Iterator[Self]:
        """Execute a block inside a transaction or nested savepoint when already in a transaction."""
        if self.in_transaction:
            sp_name = f"_vtb_sp_{next(_TX_SAVEPOINT_COUNTER)}"
            self.savepoint(sp_name)
            try:
                yield self
            except BaseException:
                self.rollback_to_savepoint(sp_name)
                raise
            else:
                self.release_savepoint(sp_name)
            return

        self.begin()
        try:
            yield self
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()

    def cancel(self) -> None:
        """Interrupt any in-flight DuckDB query via `duckdb::InterruptHandle`."""
        client = self._client
        if not self._closed and client is not None:
            client.cancel()

    def close(self) -> None:
        """Close the underlying `SyncDuckDBClient` worker pool."""
        self._closed = True
        client = self._client
        self._bind_client(None)
        if client is not None:
            client.close()

    def refresh_secrets(self, *, force: bool = True) -> None:
        """Evaluate configured `value_provider` secrets and apply `CREATE OR REPLACE SECRET` when due."""
        self._refresh_dynamic_secrets(force=force)

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> int:
        """Execute a DDL or DML statement and return the affected row count."""
        _ = kwargs
        try:
            rowcount: int = self._run_dml(sql, parameters)[0]
        except Exception as exc:
            self._on_native_error(exc)
            raise
        return rowcount

    def execute_dml(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        """Execute a DML or DDL statement and return `(rowcount, None)`."""
        _ = kwargs
        try:
            result: tuple[int, int | None] = self._run_dml(sql, parameters)
        except Exception as exc:
            self._on_native_error(exc)
            raise
        return result

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across multiple parameter sets and return the affected row count."""
        _ = kwargs
        if not parameter_sets:
            return 0
        cur = self._require_client().cursor()
        try:
            cur.executemany(sql, parameter_sets)
            return int(cur.rowcount)
        except Exception as exc:
            self._on_native_error(exc)
            raise

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(tuple_rows, column_names)` preserving column names on empty result sets."""
        _ = kwargs
        try:
            result: tuple[list[tuple[Any, ...]], tuple[str, ...]] = self._fetch_rows(sql, parameters)
        except Exception as exc:
            self._on_native_error(exc)
            raise
        return result

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return unboxed tuple rows."""
        _ = kwargs
        try:
            rows: list[tuple[Any, ...]] = self._fetch_rows(sql, parameters)[0]
        except Exception as exc:
            self._on_native_error(exc)
            raise
        return rows

    def select(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Execute a `SELECT` query and hydrate rows into dictionaries or `schema_type` instances."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            try:
                dict_rows: list[Any] = self._fetch_dicts(sql, parameters)
            except Exception as exc:
                self._on_native_error(exc)
                raise
            return dict_rows
        try:
            rows, cols = self._fetch_rows(sql, parameters)
        except Exception as exc:
            self._on_native_error(exc)
            raise
        return self.hydrate_rows(rows, cols, schema_type)

    def select_one(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            try:
                rows = self._fetch_dicts(sql, parameters)
            except Exception as exc:
                self._on_native_error(exc)
                raise
        else:
            try:
                raw_rows, cols = self._fetch_rows(sql, parameters)
            except Exception as exc:
                self._on_native_error(exc)
                raise
            rows = self.hydrate_rows(raw_rows, cols, schema_type)
        if not rows:
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return rows[0]

    def select_one_or_none(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or `None` when empty."""
        _ = kwargs
        if schema_type is None or schema_type is dict:
            try:
                rows = self._fetch_dicts(sql, parameters)
            except Exception as exc:
                self._on_native_error(exc)
                raise
        else:
            try:
                raw_rows, cols = self._fetch_rows(sql, parameters)
            except Exception as exc:
                self._on_native_error(exc)
                raise
            rows = self.hydrate_rows(raw_rows, cols, schema_type)
        return rows[0] if rows else None

    def select_value(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        value_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar column, raising `NotFoundError` when 0 rows match."""
        _ = kwargs
        try:
            val = self._fetch_value(sql, parameters)
        except Exception as exc:
            self._on_native_error(exc)
            raise
        if value_type is not None and val is not None:
            return to_value_type(val, value_type)
        return val

    def select_with_total(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        schema_type: type[Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[Any], int]:
        """Execute a `SELECT` query and return `(rows, total_count)`."""
        rows = self.select(sql, parameters, schema_type=schema_type, **kwargs)
        return rows, len(rows)

    def select_to_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> ArrowCursor:
        """Stream analytical query results into an `ArrowCursor`, spilling to `VTBARROW` SHM at `threshold_rows`."""
        _ = kwargs
        client = self._require_client()
        try:
            native_cursor = client.select_to_arrow(
                sql,
                parameters,
                batch_size=max(1, int(batch_size)),
                threshold_rows=max(0, int(threshold_rows)),
            )
            return native_cursor if isinstance(native_cursor, ArrowCursor) else ArrowCursor(native_cursor)
        except Exception as exc:
            self._on_native_error(exc)
            raise

    def stream_arrow(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Yield Arrow record batches of at most `batch_size` rows."""
        return self.stream_arrow_batches(sql, parameters, batch_size=batch_size, **kwargs)

    def stream_arrow_batches(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        """Stream Arrow record batches from `vertebra._duckdb` shared-memory cursors."""
        _ = kwargs
        chunk = max(1, int(batch_size))
        client = self._require_client()
        try:
            native_cursors = client.stream_arrow_batches(
                sql,
                parameters,
                batch_size=chunk,
            )
        except Exception as exc:
            self._on_native_error(exc)
            raise

        idx_cur = 0
        try:
            while idx_cur < len(native_cursors):
                native_cursor = native_cursors[idx_cur]
                idx_cur += 1
                with ArrowCursor(native_cursor) as cursor:
                    if cursor.row_count == 0:
                        continue
                    table = cursor.to_arrow_table()
                    if hasattr(table, "to_batches"):
                        yield from table.to_batches(max_chunksize=chunk)
                    else:
                        rows = cursor.to_pylist()
                        for idx in range(0, len(rows), chunk):
                            yield rows[idx : idx + chunk]
        except Exception as exc:
            self._on_native_error(exc)
            raise
        finally:
            for remaining in native_cursors[idx_cur:]:
                release_fn = getattr(remaining, "release", None)
                if callable(release_fn):
                    release_fn()

    def load_from_arrow(
        self,
        table_name: str,
        data: Any,
        *,
        columns: Sequence[str] | None = None,
        create_if_missing: bool = False,
        **kwargs: Any,
    ) -> int:
        """Bulk-ingest an `ArrowCursor`, PyArrow Table/RecordBatch, or row sequence via DuckDB `Appender`."""
        _ = kwargs
        validate_identifier(table_name, "table_name", allow_dots=True)
        col_list: list[str] | None = None
        if columns is not None:
            col_list = []
            for col in columns:
                col_str = str(col)
                validate_identifier(col_str, "column", allow_dots=False)
                col_list.append(col_str)

        client = self._require_client()
        schema = getattr(data, "schema", None)
        if isinstance(data, ArrowCursor) or schema is not None or isinstance(data, (bytes, bytearray, memoryview)):
            if schema is not None and col_list is None:
                for field_name in getattr(schema, "names", ()):
                    validate_identifier(str(field_name), "column", allow_dots=False)
            payload = data.buffer_view() if isinstance(data, ArrowCursor) and data.used_shm else data
            try:
                return int(
                    client.load_from_arrow(
                        table_name,
                        payload,
                        columns=col_list,
                        create_if_missing=create_if_missing,
                    )
                )
            except Exception as exc:
                self._on_native_error(exc)
                raise

        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        dict_rows = [{str(col): DuckDBCore.normalize_param(row[idx]) for idx, col in enumerate(cols)} for row in rows]
        try:
            return int(
                client.load_from_arrow(
                    table_name,
                    dict_rows,
                    columns=[str(c) for c in cols],
                    create_if_missing=create_if_missing,
                )
            )
        except Exception as exc:
            self._on_native_error(exc)
            raise

    def _require_client(self) -> SyncDuckDBClient:
        client = self._client
        if client is None:
            msg = "DuckDB driver is closed"
            raise InterfaceError(msg)
        return client

    def _on_native_error(self, exc: BaseException) -> None:
        """Invalidate rejected dynamic secrets and refresh them before the error propagates."""
        if not self._dynamic_secrets:
            return
        if not DuckDBCore.invalidate_rejected_credentials(exc, self._dynamic_secrets, self._secret_generations):
            return
        try:
            self._refresh_dynamic_secrets(force=True)
        except Exception as refresh_exc:
            exc.add_note(f"DuckDB secret refresh after credential rejection failed: {refresh_exc}")

    def _refresh_dynamic_secrets(self, *, force: bool = False) -> None:
        if not self._dynamic_secrets or self._closed:
            return
        client = self._require_client()
        now_monotonic = time.monotonic()
        for secret in self._dynamic_secrets:
            provider = secret.value_provider
            if provider is None:
                continue
            key = secret.name.lower()
            provider_gen = DuckDBCore.probe_provider_generation(provider)
            provider_needs_refresh = DuckDBCore.probe_provider_needs_refresh(provider)
            last_refreshed = self._secret_refresh_times.get(key)
            applied_gen = self._secret_generations.get(key)
            interval_due = (
                last_refreshed is None or (now_monotonic - last_refreshed) >= secret.effective_refresh_interval()
            )
            gen_changed = provider_gen is not None and provider_gen != applied_gen
            if not (force or interval_due or provider_needs_refresh or gen_changed):
                continue
            try:
                resolved = provider()
                if inspect.isawaitable(resolved):
                    close_fn = getattr(resolved, "close", None)
                    if callable(close_fn):
                        close_fn()
                    msg = f"SyncDuckDBDriver secret {secret.name!r} requires a synchronous `value_provider`"
                    raise ImproperConfigurationError(msg)
                ddl = secret.to_create_secret_sql(resolved)
                client.execute(ddl, None)
            except ImproperConfigurationError:
                raise
            except Exception:
                if secret.required:
                    raise
            else:
                self._secret_refresh_times[key] = now_monotonic
                updated_gen = DuckDBCore.probe_provider_generation(provider)
                if updated_gen is not None:
                    self._secret_generations[key] = updated_gen
