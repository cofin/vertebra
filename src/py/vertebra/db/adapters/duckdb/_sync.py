"""Synchronous DuckDB analytical driver backed by `vertebra._duckdb`."""

from __future__ import annotations

import inspect
import itertools
import os
import re
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Self

from vertebra._vertebra import InterfaceError
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
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._duckdb import SyncDuckDBClient

__all__ = (
    "SyncDuckDBDriver",
    "_SyncDuckDBCursor",
)

_TX_SAVEPOINT_COUNTER = itertools.count(1)
_CREDENTIAL_REJECTION_PATTERN = re.compile(r"\(HTTP (?:401|403)\)")


class SyncDuckDBDriver(SyncDatabaseDriver[DuckDBConfig]):
    """Synchronous DuckDB analytical driver backed by `vertebra._duckdb.SyncDuckDBClient`."""

    dialect: ClassVar[str] = "duckdb"
    dialect_tag: ClassVar[int] = DUCKDB_DIALECT_TAG

    __slots__ = (
        "_client",
        "_dynamic_secrets",
        "_secret_generations",
        "_secret_refresh_times",
        "executed_bootstrap_statements",
    )

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
        self.executed_bootstrap_statements: tuple[str, ...] = tuple(self.config.bootstrap_statements())

        if _client is not None:
            self._client: SyncDuckDBClient | None = _client
        else:
            native_mod = ensure_module(
                "vertebra._duckdb",
                package="vertebra-core[duckdb]",
                extra="duckdb",
            )
            init_stmts = [s.to_create_secret_sql() for s in normalized_secrets if s.value_provider is None]
            init_stmts.extend(self.config.init_statements)
            self._client = native_mod.SyncDuckDBClient(
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
        try:
            self._refresh_dynamic_secrets(force=True)
        except BaseException:
            client = self._client
            self._client = None
            self._closed = True
            if client is not None:
                client.close()
            raise

    @property
    def in_transaction(self) -> bool:
        """Return `True` when an explicit DuckDB transaction is active on the writer session."""
        client = self._client
        return bool(client.in_transaction) if client is not None else False

    def cursor(self) -> _SyncDuckDBCursor:
        """Create a synchronous DBAPI-style cursor bound to this driver."""
        return _SyncDuckDBCursor(self)

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
        self._client = None
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
        """Execute a DDL or DML statement under single-writer coordination."""
        _ = kwargs
        self._refresh_dynamic_secrets()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            return int(client.execute(sql, bound))
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        """Execute a statement across multiple parameter sets under single-writer coordination."""
        _ = kwargs
        if not parameter_sets:
            return 0
        self._refresh_dynamic_secrets()
        client = self._require_client()
        normalized_sets = [DuckDBCore.normalize_parameters(item) or () for item in parameter_sets]
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(normalized_sets[0]))
        try:
            return int(client.execute_many(sql, normalized_sets))
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    def execute_tuples(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return positional row tuples."""
        _ = kwargs
        self._refresh_dynamic_secrets()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            return list(client.execute_tuples(sql, bound))
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    def execute_with_description(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        """Execute SQL and return `(tuple_rows, column_names)` preserving column names on empty result sets."""
        _ = kwargs
        self._refresh_dynamic_secrets()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            rows, cols = client.execute_with_description(sql, bound)
            return list(rows), tuple(str(c) for c in cols)
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

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
        self._refresh_dynamic_secrets()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            native_cursor = client.select_to_arrow(
                sql,
                bound,
                batch_size=max(1, int(batch_size)),
                threshold_rows=max(0, int(threshold_rows)),
            )
            return native_cursor if isinstance(native_cursor, ArrowCursor) else ArrowCursor(native_cursor)
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

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
    ) -> Iterator[Any]:
        """Stream Arrow record batches from `vertebra._duckdb` shared-memory cursors."""
        _ = kwargs
        chunk = max(1, int(batch_size))
        self._refresh_dynamic_secrets()
        client = self._require_client()
        bound = DuckDBCore.normalize_parameters(parameters)
        grant = self._acquire_duckdb_lease(sql, DuckDBCore.param_len(bound))
        try:
            native_cursors = client.stream_arrow_batches(
                sql,
                bound,
                batch_size=chunk,
            )
        except Exception as exc:
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

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
            self._invalidate_rejected_credentials(exc)
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

        self._refresh_dynamic_secrets()
        client = self._require_client()
        if isinstance(data, ArrowCursor) or hasattr(data, "schema") or isinstance(data, (bytes, bytearray, memoryview)):
            if hasattr(data, "schema") and col_list is None:
                for field_name in getattr(data.schema, "names", ()):
                    validate_identifier(str(field_name), "column", allow_dots=False)
            payload = data.buffer_view() if isinstance(data, ArrowCursor) and data.used_shm else data
            grant = self._acquire_duckdb_lease(f"APPENDER {table_name}", len(col_list or ()))
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
                self._invalidate_rejected_credentials(exc)
                raise
            finally:
                self._release_duckdb_lease(grant)

        rows, cols = _table_to_rows_and_cols(data, columns)
        for col in cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not cols:
            return 0
        dict_rows = [{str(col): DuckDBCore.normalize_param(row[idx]) for idx, col in enumerate(cols)} for row in rows]
        grant = self._acquire_duckdb_lease(f"APPENDER {table_name}", len(cols))
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
            self._invalidate_rejected_credentials(exc)
            raise
        finally:
            self._release_duckdb_lease(grant)

    def _require_client(self) -> SyncDuckDBClient:
        client = self._client
        if client is None:
            msg = "DuckDB driver is closed"
            raise InterfaceError(msg)
        return client

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
            provider_gen = self._probe_provider_generation(provider)
            provider_needs_refresh = self._probe_provider_needs_refresh(provider)
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
                    if hasattr(resolved, "close"):
                        resolved.close()
                    msg = f"SyncDuckDBDriver secret {secret.name!r} requires a synchronous `value_provider`"
                    raise ImproperConfigurationError(msg)
                ddl = secret.to_create_secret_sql(resolved)
                client.execute(ddl, None)
            except Exception:
                if secret.required:
                    raise
            else:
                self._secret_refresh_times[key] = now_monotonic
                updated_gen = self._probe_provider_generation(provider)
                if updated_gen is not None:
                    self._secret_generations[key] = updated_gen

    def _acquire_duckdb_lease(self, sql: str, param_count: int) -> Any:
        if self._broker is None:
            return None
        if hasattr(self._broker, "statement_cache"):
            sc_attr = self._broker.statement_cache
            sc = sc_attr() if callable(sc_attr) else sc_attr
            if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
                sql_bytes = sql.encode("utf-8")
                try:
                    cached = sc.lookup(DUCKDB_DIALECT_TAG, sql)
                    if cached is None:
                        sc.insert(
                            DUCKDB_DIALECT_TAG,
                            sql,
                            param_count,
                            sql_bytes,
                            b"",
                        )
                except TypeError:
                    cached = sc.lookup(DUCKDB_DIALECT_TAG, sql_bytes)
                    if cached is None:
                        sc.insert(
                            DUCKDB_DIALECT_TAG,
                            sql_bytes,
                            param_count,
                            sql_bytes,
                            b"",
                        )
        if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
            return self._broker.lease_table.acquire(os.getpid())
        if hasattr(self._broker, "acquire"):
            try:
                return self._broker.acquire(os.getpid())
            except TypeError:
                return self._broker.acquire(bind_key="duckdb")
        return None

    def _release_duckdb_lease(self, grant: Any) -> None:
        if grant is not None and self._broker is not None:
            if hasattr(self._broker, "lease_table") and self._broker.lease_table is not None:
                self._broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(self._broker, "release") and hasattr(grant, "slot_index"):
                self._broker.release(grant.slot_index, grant.generation)

    def _invalidate_rejected_credentials(self, exc: BaseException) -> None:
        if not _CREDENTIAL_REJECTION_PATTERN.search(str(exc)):
            return
        for secret in self._dynamic_secrets:
            provider = secret.value_provider
            if provider is None:
                continue
            key = secret.name.lower()
            applied_gen = self._secret_generations.get(key)
            if applied_gen is None:
                continue
            target = getattr(provider, "__self__", provider)
            invalidate_fn = getattr(target, "invalidate", None)
            if callable(invalidate_fn):
                invalidate_fn(applied_gen)
                self._secret_generations.pop(key, None)

    @staticmethod
    def _probe_provider_generation(provider: Any) -> int | None:
        target = getattr(provider, "__self__", provider)
        gen = getattr(target, "generation", None)
        if isinstance(gen, int) and not isinstance(gen, bool):
            return gen
        return None

    @staticmethod
    def _probe_provider_needs_refresh(provider: Any) -> bool:
        target = getattr(provider, "__self__", provider)
        fn = getattr(target, "needs_refresh", None)
        if callable(fn):
            return bool(fn())
        return False


class _SyncDuckDBCursor:
    """Synchronous DBAPI-style cursor delegating to `SyncDuckDBDriver`."""

    __slots__ = ("_driver", "_pos", "_rows", "arraysize", "description", "rowcount")

    def __init__(self, driver: SyncDuckDBDriver) -> None:
        self._driver = driver
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0
        self.arraysize: int = 1
        self.description: tuple[tuple[str, None, None, None, None, None, None], ...] | None = None
        self.rowcount: int = -1

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def execute(
        self,
        sql: str,
        parameters: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> Self:
        """Execute a statement or query and populate cursor state."""
        if DuckDBCore.is_write_statement(sql):
            self.rowcount = self._driver.execute(sql, parameters)
            self._rows = []
            self._pos = 0
            self.description = None
            return self
        rows, cols = self._driver.execute_with_description(sql, parameters)
        self._rows = rows
        self._pos = 0
        self.rowcount = len(rows)
        self.description = tuple((col, None, None, None, None, None, None) for col in cols) if cols else None
        return self

    def executemany(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]],
    ) -> Self:
        """Execute a statement across multiple parameter sets."""
        self.rowcount = self._driver.execute_many(sql, parameter_sets)
        self._rows = []
        self._pos = 0
        self.description = None
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next row tuple or `None` when exhausted."""
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to `size` remaining row tuples."""
        limit = self.arraysize if size is None else max(0, int(size))
        batch = self._rows[self._pos : self._pos + limit]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining row tuples."""
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        """Clear buffered cursor rows."""
        self._rows = []
        self._pos = 0
