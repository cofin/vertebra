"""Unit tests for unified SQL database driver contracts and cloud DB-API semantics."""

from __future__ import annotations

import datetime
import decimal
import gc
import importlib.util
import re
import sqlite3
import uuid
import weakref
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from tests.unit.db.postgres_ports import FakeAsyncPostgresPool, FakePostgresCluster, FakeSyncPostgresPool
from vertebra._mysql import MySQLWireTestServer
from vertebra.db import (
    ArrowCursor,
    AsyncDatabaseDriver,
    DatabaseError,
    IntegrityError,
    NotFoundError,
    OperationalError,
    ProgrammingError,
    SyncDatabaseDriver,
)
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, BigQueryConfig, SyncBigQueryDriver
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    AsyncSpannerTransaction,
    Mutation,
    SpannerConfig,
    SyncSpannerDriver,
    SyncSpannerTransaction,
)
from vertebra.db.adapters.spanner.change_stream import AsyncSpannerChangeStreamSource, SyncSpannerChangeStreamSource
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.dbapi import AsyncDBAPIConnection, NotSupportedError, SyncDBAPIConnection
from vertebra.db.driver._common import (
    SPANGRES_DIALECT_TAG,
    BaseDatabaseConfig,
    CommonDriverMixin,
    _BrokerLeasePlan,
    _lease_releaser,
)
from vertebra.db.driver.capabilities import AsyncChangeSource, SyncChangeSource
from vertebra.db.registry import REGISTERED_CONFIG_TYPES
from vertebra.extensions.sqlalchemy import dialects
from vertebra.extensions.sqlalchemy.async_adapter import AsyncAdapt_vertebra_connection, SQLAlchemyAsyncDBAPI
from vertebra.utils.serializers import (
    decode_json,
    encode_json,
)

if TYPE_CHECKING:
    pass


_DUCKDB_SKIP_REASON = "vertebra._duckdb native extension is not built"
_HAS_SQLALCHEMY_GCP = (
    importlib.util.find_spec("sqlalchemy_bigquery") is not None
    and importlib.util.find_spec("google.cloud") is not None
    and importlib.util.find_spec("google.cloud.sqlalchemy_spanner") is not None
)


_SQL_CONTRACT_ENGINES: tuple[str, ...] = (
    "sqlite",
    "duckdb",
    "postgres",
    "mysql",
    "bigquery",
    "spanner",
)

_LEGACY_DRIVER_ALIASES: tuple[str, ...] = (
    "executemany",
    "fetch",
    "fetchrow",
    "fetchval",
    "fetch_arrow",
    "insert_arrow",
)


class _ContractDBAPICursor:
    """SQLite-backed DB-API cursor double for PostgreSQL and MySQL contract tests."""

    __slots__ = ("_conn", "_rows", "description", "rowcount")

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._rows: list[tuple[Any, ...]] = []
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = 0

    def _run(self, sql: str, params: Any = None) -> None:
        rewritten = sql.replace("%s", "?")
        bound: Any = () if params is None else params
        cur = self._conn.execute(rewritten, bound)
        self.rowcount = max(0, int(cur.rowcount or 0))
        if cur.description:
            self.description = tuple((d[0], None, None, None, None, None, None) for d in cur.description)
            self._rows = [tuple(r) for r in cur.fetchall()]
        else:
            self.description = None
            self._rows = []

    def execute(self, sql: str, params: Any = None) -> None:
        self._run(sql, params)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def close(self) -> None:
        return None

    def __enter__(self) -> _ContractDBAPICursor:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _ContractAsyncCursor:
    """Async SQLite-backed DB-API cursor double for PostgreSQL and MySQL contract tests."""

    __slots__ = ("_sync", "description", "rowcount")

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._sync = _ContractDBAPICursor(conn)
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = 0

    async def execute(self, sql: str, params: Any = None) -> None:
        self._sync.execute(sql, params)
        self.description = self._sync.description
        self.rowcount = self._sync.rowcount

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._sync.fetchall()

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> _ContractAsyncCursor:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _ContractDBAPIConnection:
    """Sync/Async DB-API connection double wrapping an in-memory SQLite connection."""

    __slots__ = ("_async_mode", "_conn")

    def __init__(self, conn: sqlite3.Connection, *, async_mode: bool = False) -> None:
        self._conn = conn
        self._async_mode = async_mode

    def cursor(self) -> Any:
        if self._async_mode:
            return _ContractAsyncCursor(self._conn)
        return _ContractDBAPICursor(self._conn)

    def commit(self) -> Any:
        self._conn.commit()
        if self._async_mode:

            async def _noop() -> None:
                return None

            return _noop()
        return None

    def rollback(self) -> Any:
        self._conn.rollback()
        if self._async_mode:

            async def _noop() -> None:
                return None

            return _noop()
        return None

    def close(self) -> None:
        return None

    def __enter__(self) -> _ContractDBAPIConnection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    async def __aenter__(self) -> _ContractDBAPIConnection:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _ContractDBAPIPool:
    """Connection pool double exposing sync `.connection()` and async `.acquire()`."""

    __slots__ = ("_async_conn", "_conn", "_sync_conn")

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._sync_conn = _ContractDBAPIConnection(conn, async_mode=False)
        self._async_conn = _ContractDBAPIConnection(conn, async_mode=True)

    @contextmanager
    def connection(self) -> Iterator[_ContractDBAPIConnection]:
        yield self._sync_conn

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_ContractDBAPIConnection]:
        yield self._async_conn

    def close(self) -> None:
        return None


class _ContractBigQueryClient:
    """In-memory SQLite-backed BigQuery client double supporting sync and async wire calls."""

    __slots__ = ("_async_mode", "_conn", "_in_tx")

    def __init__(self, conn: sqlite3.Connection, *, async_mode: bool = False) -> None:
        self._conn = conn
        self._async_mode = async_mode
        self._in_tx = False

    @staticmethod
    def _strip_leading_trace_comment(sql: str) -> str:
        stripped = sql.lstrip()
        while stripped.startswith("/*"):
            end_idx = stripped.find("*/")
            if end_idx == -1:
                break
            stripped = stripped[end_idx + 2 :].lstrip()
        return stripped

    def _decode_bq_params(self, sql: str, params_json: str | None) -> tuple[str, Any]:
        clean_sql = self._strip_leading_trace_comment(sql)
        if not params_json:
            return clean_sql, ()
        parsed = decode_json(params_json)
        if not isinstance(parsed, list):
            return clean_sql, ()
        named_values: dict[str, Any] = {}
        positional_values: list[Any] = []
        for entry in parsed:
            if "value" in entry:
                raw_val = entry.get("value")
                param_type = entry.get("type", "STRING")
            else:
                raw_val = entry.get("parameterValue", {}).get("value")
                param_type = entry.get("parameterType", {}).get("type", "STRING")
            val: Any = int(raw_val) if (param_type == "INT64" and raw_val is not None) else raw_val
            name = entry.get("name")
            if name:
                named_values[str(name)] = val
            else:
                positional_values.append(val)
        if named_values:
            rewritten = re.sub(r"@([A-Za-z_][A-Za-z0-9_]*)", r":\1", clean_sql)
            return rewritten, named_values
        return clean_sql, tuple(positional_values)

    def _query_sync(
        self,
        sql: str,
        params_json: str | None = None,
        session_id: str | None = None,
        create_session: bool = False,
        *_extra: Any,
    ) -> dict[str, Any]:
        adapted, bound = self._decode_bq_params(sql, params_json)
        upper = adapted.strip().upper()
        if self._in_tx and upper.startswith("CREATE TABLE"):
            raise RuntimeError("BigQuery does not support permanent DDL in transactions")
        sid = session_id or ("bq-contract-sess" if create_session else None)
        if upper.startswith("BEGIN"):
            if not self._conn.in_transaction:
                self._conn.execute("BEGIN")
            self._in_tx = True
            return {"columns": [], "rows": [], "total_rows": 0, "session_id": sid}
        if upper.startswith("COMMIT"):
            self._conn.commit()
            self._in_tx = False
            return {"columns": [], "rows": [], "total_rows": 0, "session_id": sid}
        if upper.startswith("ROLLBACK"):
            self._conn.rollback()
            self._in_tx = False
            return {"columns": [], "rows": [], "total_rows": 0, "session_id": sid}
        cur = self._conn.execute(adapted, bound)
        if cur.description:
            cols = [str(d[0]) for d in cur.description]
            rows = [list(r) for r in cur.fetchall()]
            return {
                "columns": cols,
                "rows": rows,
                "total_rows": len(rows),
                "session_id": sid,
            }
        affected = max(0, int(cur.rowcount or 0))
        if not self._in_tx:
            self._conn.commit()
        return {
            "columns": [],
            "rows": [],
            "total_rows": 0,
            "num_dml_affected_rows": affected,
            "session_id": sid,
        }

    def query(self, sql: str, *args: Any) -> Any:
        result = self._query_sync(sql, *args)
        if self._async_mode:

            async def _coro() -> dict[str, Any]:
                return result

            return _coro()
        return result

    def cursor(
        self,
        *,
        session_id: str | None = None,
        param_encoder: Any = None,
        lazy_begin: bool = False,
        **_kwargs: Any,
    ) -> _ContractBigQueryCursor:
        if lazy_begin:
            msg = "BigQuery cursors do not support lazy_begin"
            raise NotSupportedError(msg)
        return _ContractBigQueryCursor(
            self,
            session_id=session_id,
            param_encoder=param_encoder,
            async_mode=self._async_mode,
        )

    def close(self) -> Any:
        if self._async_mode:

            async def _coro() -> None:
                return None

            return _coro()
        return None


class _ContractBigQueryCursor:
    """In-memory cursor double for `_ContractBigQueryClient`."""

    __slots__ = (
        "_async_mode",
        "_client",
        "_param_encoder",
        "_pos",
        "_rows",
        "_session_id",
        "arraysize",
        "description",
        "query_metadata",
        "rowcount",
    )

    def __init__(
        self,
        client: _ContractBigQueryClient,
        *,
        session_id: str | None = None,
        param_encoder: Any = None,
        async_mode: bool = False,
    ) -> None:
        self._client = client
        self._session_id = session_id
        self._param_encoder = param_encoder
        self._async_mode = async_mode
        self._rows: list[tuple[Any, ...]] = []
        self._pos = 0
        self.arraysize = 1
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.query_metadata: dict[str, Any] = {}
        self.rowcount = -1

    def _wrap(self, value: Any) -> Any:
        if self._async_mode:

            async def _coro() -> Any:
                return value

            return _coro()
        return value

    def _run_one(self, sql: str, params: Any = None) -> int:
        exec_sql = sql
        params_json: str | None = None
        if params:
            if callable(self._param_encoder):
                encoded = self._param_encoder(sql, params)
                if isinstance(encoded, tuple):
                    exec_sql, params_json = encoded[0], encoded[1]
                else:
                    params_json = encoded if isinstance(encoded, str) else encode_json(encoded)
            elif isinstance(params, str):
                params_json = params
        result = self._client._query_sync(exec_sql, params_json, self._session_id)
        self.query_metadata = dict(result)
        cols = result.get("columns") or []
        if cols:
            self.description = tuple((str(c), None, None, None, None, None, None) for c in cols)
            self._rows = [tuple(r) for r in result.get("rows", ())]
            self._pos = 0
            self.rowcount = len(self._rows)
            return self.rowcount
        self.description = None
        self._rows = []
        self._pos = 0
        affected = int(result.get("num_dml_affected_rows", 0))
        self.rowcount = affected
        return affected

    def execute(self, sql: str, params: Any = None) -> Any:
        self._run_one(sql, params)
        return self._wrap(None)

    def executemany(self, sql: str, seq_of_params: Sequence[Any]) -> Any:
        total = 0
        for params in seq_of_params:
            total += max(0, self._run_one(sql, params))
        self.description = None
        self._rows = []
        self._pos = 0
        self.rowcount = total
        return self._wrap(None)

    def fetchone(self) -> Any:
        if self._pos >= len(self._rows):
            return self._wrap(None)
        row = self._rows[self._pos]
        self._pos += 1
        return self._wrap(row)

    def fetchmany(self, size: int | None = None) -> Any:
        limit = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + limit]
        self._pos += len(batch)
        return self._wrap(batch)

    def fetchall(self) -> Any:
        batch = self._rows[self._pos :]
        self._pos = len(self._rows)
        return self._wrap(batch)

    def close(self) -> Any:
        self._rows = []
        self._pos = 0
        return self._wrap(None)


class _ContractSpannerRwTx:
    """In-memory SQLite-backed read-write transaction handle for `_ContractSpannerClient`."""

    __slots__ = ("_async_mode", "_client", "_closed", "_tx_id")

    def __init__(self, client: _ContractSpannerClient, tx_id: bytes, *, async_mode: bool = False) -> None:
        self._client = client
        self._tx_id = tx_id
        self._async_mode = async_mode
        self._closed = False
        if not self._client._conn.in_transaction:
            self._client._conn.execute("BEGIN")

    @property
    def transaction_id(self) -> bytes:
        return self._tx_id

    @property
    def closed(self) -> bool:
        return self._closed

    def query(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> Any:
        _ = request_tag
        return self._client.query(sql, params_json)

    def execute_dml(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> Any:
        _ = request_tag
        adapted, bound = self._client._adapt_sql(sql, params_json)
        cur = self._client._conn.execute(adapted, bound)
        return self._client._wrap(max(0, int(cur.rowcount or 0)))

    def execute_batch_dml(self, statements_json: str, request_tag: str | None = None) -> Any:
        _ = request_tag
        decoded = decode_json(statements_json) if statements_json else []
        counts: list[int] = []
        for entry in decoded:
            stmt_sql = str(entry.get("sql", ""))
            stmt_params = entry.get("params")
            params_str = encode_json({"values": stmt_params}) if isinstance(stmt_params, dict) else None
            adapted, bound = self._client._adapt_sql(stmt_sql, params_str)
            cur = self._client._conn.execute(adapted, bound)
            counts.append(max(0, int(cur.rowcount or 0)))
        return self._client._wrap(counts)

    def commit(self, mutations_json: str | None = None) -> Any:
        count = self._client._apply_mutations_in_tx(mutations_json) if mutations_json else 0
        self._client._conn.commit()
        self._closed = True
        return self._client._wrap({"commit_timestamp_micros": 1_700_000_000_000_000, "mutation_count": count})

    def rollback(self) -> Any:
        if not self._closed:
            self._client._conn.rollback()
            self._closed = True
        return self._client._wrap(None)

    def cursor(
        self,
        *,
        param_encoder: Any = None,
        lazy_begin: bool = False,
        **_kwargs: Any,
    ) -> _ContractSpannerCursor:
        if lazy_begin:
            msg = "Spanner cursors do not support lazy_begin"
            raise NotSupportedError(msg)
        return _ContractSpannerCursor(
            self._client,
            in_tx=True,
            param_encoder=param_encoder,
            async_mode=self._async_mode,
        )


class _ContractSpannerClient:
    """In-memory SQLite-backed Spanner client double supporting sync and async wire calls."""

    __slots__ = ("_async_mode", "_conn", "_tx_seq")

    def __init__(self, conn: sqlite3.Connection, *, async_mode: bool = False) -> None:
        self._conn = conn
        self._async_mode = async_mode
        self._tx_seq = 0

    def begin_read_write_transaction(
        self,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> Any:
        _ = (transaction_tag, previous_transaction_id, exclude_txn_from_change_streams, max_commit_delay_ms)
        self._tx_seq += 1
        handle = _ContractSpannerRwTx(self, f"contract-tx-{self._tx_seq}".encode(), async_mode=self._async_mode)
        return self._wrap(handle)

    @staticmethod
    def _adapt_sql(sql: str, params_json: str | None) -> tuple[str, Any]:
        stripped = _ContractBigQueryClient._strip_leading_trace_comment(sql)
        if not params_json:
            return stripped, ()
        decoded = decode_json(params_json)
        if isinstance(decoded, dict):
            values = decoded.get("values", decoded)
            if isinstance(values, dict):
                rewritten = re.sub(r"@([A-Za-z_][A-Za-z0-9_]*)", r":\1", stripped)
                coerced = {k: (int(v) if isinstance(v, str) and v.isdigit() else v) for k, v in values.items()}
                return rewritten, coerced
        return stripped, ()

    def _wrap(self, value: Any) -> Any:
        if self._async_mode:

            async def _coro() -> Any:
                return value

            return _coro()
        return value

    def query(self, sql: str, params_json: str | None = None, *_extra: Any) -> Any:
        adapted, bound = self._adapt_sql(sql, params_json)
        cur = self._conn.execute(adapted, bound)
        cols = [str(d[0]) for d in (cur.description or ())]
        rows = [list(r) for r in cur.fetchall()]
        return self._wrap({"columns": cols, "rows": rows, "read_timestamp_micros": 1_700_000_000_000_000})

    def execute_dml(self, sql: str, params_json: str | None = None, *_extra: Any) -> Any:
        adapted, bound = self._adapt_sql(sql, params_json)
        cur = self._conn.execute(adapted, bound)
        self._conn.commit()
        return self._wrap(max(0, int(cur.rowcount or 0)))

    def execute_batch_dml(self, batch_json: str, *_extra: Any) -> Any:
        decoded = decode_json(batch_json) if batch_json else []
        counts: list[int] = []
        for entry in decoded:
            stmt_sql = str(entry.get("sql", ""))
            stmt_params = entry.get("params")
            params_str = encode_json({"values": stmt_params}) if isinstance(stmt_params, dict) else None
            adapted, bound = self._adapt_sql(stmt_sql, params_str)
            cur = self._conn.execute(adapted, bound)
            counts.append(max(0, int(cur.rowcount or 0)))
        self._conn.commit()
        return self._wrap(counts)

    def _apply_mutations_in_tx(self, mutations_json: str | None) -> int:
        decoded = decode_json(mutations_json) if mutations_json else []
        count = 0
        for item in decoded:
            table = str(item.get("table", ""))
            cols = [str(c) for c in item.get("columns", ())]
            vals = item.get("values", ())
            placeholders = ", ".join("?" for _ in cols)
            col_list = ", ".join(cols)
            for row in vals:
                self._conn.execute(f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})", list(row))
                count += 1
        return count

    def apply_mutations(self, mutations_json: str, *_extra: Any) -> Any:
        count = self._apply_mutations_in_tx(mutations_json)
        self._conn.commit()
        return self._wrap({"commit_timestamp_micros": 1_700_000_000_000_000, "mutation_count": count})

    def close(self) -> Any:
        return self._wrap(None)

    def update_ddl(self, statements: Sequence[str]) -> Any:
        for stmt in statements:
            self._conn.execute(stmt)
        self._conn.commit()
        return self._wrap("op-ddl-1")

    def read_arrow(self, *_args: Any) -> Any:
        msg = "UNIMPLEMENTED: fallback to query"
        raise RuntimeError(msg)

    def cursor(
        self,
        *,
        param_encoder: Any = None,
        lazy_begin: bool = False,
        **_kwargs: Any,
    ) -> _ContractSpannerCursor:
        if lazy_begin:
            msg = "Spanner cursors do not support lazy_begin"
            raise NotSupportedError(msg)
        return _ContractSpannerCursor(
            self,
            in_tx=False,
            param_encoder=param_encoder,
            async_mode=self._async_mode,
        )


class _ContractSpannerCursor:
    """In-memory cursor double for `_ContractSpannerClient` and `_ContractSpannerRwTx`."""

    __slots__ = (
        "_async_mode",
        "_client",
        "_in_tx",
        "_param_encoder",
        "_pos",
        "_rows",
        "arraysize",
        "description",
        "rowcount",
    )

    def __init__(
        self,
        client: _ContractSpannerClient,
        *,
        in_tx: bool = False,
        param_encoder: Any = None,
        async_mode: bool = False,
    ) -> None:
        self._client = client
        self._in_tx = in_tx
        self._param_encoder = param_encoder
        self._async_mode = async_mode
        self._rows: list[tuple[Any, ...]] = []
        self._pos = 0
        self.arraysize = 1
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount = -1

    def _run_one(self, sql: str, params: Any = None) -> int:
        params_json: str | None = None
        if params:
            if callable(self._param_encoder):
                encoded = self._param_encoder(params)
                params_json = encoded if isinstance(encoded, str) else encode_json(encoded)
            elif isinstance(params, str):
                params_json = params
        adapted, bound = self._client._adapt_sql(sql, params_json)
        cur = self._client._conn.execute(adapted, bound)
        if cur.description:
            self.description = tuple((str(d[0]), None, None, None, None, None, None) for d in cur.description)
            self._rows = [tuple(r) for r in cur.fetchall()]
            self._pos = 0
            self.rowcount = len(self._rows)
            return self.rowcount
        affected = max(0, int(cur.rowcount or 0))
        if not self._in_tx:
            self._client._conn.commit()
        self.description = None
        self._rows = []
        self._pos = 0
        self.rowcount = affected
        return affected

    def execute(self, sql: str, params: Any = None) -> Any:
        self._run_one(sql, params)
        return self._client._wrap(None)

    def executemany(self, sql: str, seq_of_params: Sequence[Any]) -> Any:
        total = 0
        for params in seq_of_params:
            total += max(0, self._run_one(sql, params))
        self.description = None
        self._rows = []
        self._pos = 0
        self.rowcount = total
        return self._client._wrap(None)

    def fetchone(self) -> Any:
        if self._pos >= len(self._rows):
            return self._client._wrap(None)
        row = self._rows[self._pos]
        self._pos += 1
        return self._client._wrap(row)

    def fetchmany(self, size: int | None = None) -> Any:
        limit = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + limit]
        self._pos += len(batch)
        return self._client._wrap(batch)

    def fetchall(self) -> Any:
        batch = self._rows[self._pos :]
        self._pos = len(self._rows)
        return self._client._wrap(batch)

    def close(self) -> Any:
        self._rows = []
        self._pos = 0
        return self._client._wrap(None)


class _ContractMSSQLClient:
    """In-memory SQLite-backed MSSQL client double supporting sync and async wire calls."""

    __slots__ = ("_async_mode", "_conn", "_in_tx")

    def __init__(self, conn: sqlite3.Connection, *, async_mode: bool = False) -> None:
        self._conn = conn
        self._async_mode = async_mode
        self._in_tx = False

    @staticmethod
    def _adapt_sql(sql: str, parameters: Any) -> tuple[str, tuple[Any, ...]]:
        if not parameters:
            return re.sub(r"@P\d+", "?", sql), ()
        if isinstance(parameters, Mapping):
            ordered: list[Any] = []

            def _rep(match: re.Match[str]) -> str:
                key = match.group(1)
                ordered.append(parameters.get(key, parameters.get(f"@{key}")))
                return "?"

            return re.sub(r"@([A-Za-z_][A-Za-z0-9_]*)", _rep, sql), tuple(ordered)
        return re.sub(r"@P\d+", "?", sql), tuple(parameters)

    def _wrap(self, value: Any) -> Any:
        if self._async_mode:

            async def _coro() -> Any:
                return value

            return _coro()
        return value

    def begin(self) -> Any:
        if not self._conn.in_transaction:
            self._conn.execute("BEGIN")
        self._in_tx = True
        return self._wrap(None)

    def commit(self) -> Any:
        self._conn.commit()
        self._in_tx = False
        return self._wrap(None)

    def rollback(self) -> Any:
        self._conn.rollback()
        self._in_tx = False
        return self._wrap(None)

    def cancel(self) -> Any:
        return self._wrap(False)

    def close(self) -> Any:
        self._in_tx = False
        self._conn.close()
        return self._wrap(None)

    def execute(self, sql: str, parameters: Any = None) -> Any:
        adapted, bound = self._adapt_sql(sql, parameters)
        cur = self._conn.execute(adapted, bound)
        if not self._in_tx:
            self._conn.commit()
        return self._wrap(max(0, int(cur.rowcount or 0)))

    def query(self, sql: str, parameters: Any = None) -> Any:
        adapted, bound = self._adapt_sql(sql, parameters)
        cur = self._conn.execute(adapted, bound)
        cols = [str(d[0]) for d in (cur.description or ())]
        rows = [tuple(r) for r in cur.fetchall()] if cur.description is not None else []
        if cur.description is None and not self._in_tx:
            self._conn.commit()
        return self._wrap({"columns": cols, "rows": rows, "affected_rows": max(0, int(cur.rowcount or 0))})

    def fetch_all(self, sql: str, parameters: Any = None) -> Any:
        adapted, bound = self._adapt_sql(sql, parameters)
        cur = self._conn.execute(adapted, bound)
        cols = [str(d[0]) for d in (cur.description or ())]
        rows = [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
        return self._wrap(rows)

    def fetch_one(self, sql: str, parameters: Any = None) -> Any:
        adapted, bound = self._adapt_sql(sql, parameters)
        cur = self._conn.execute(adapted, bound)
        cols = [str(d[0]) for d in (cur.description or ())]
        fetched = cur.fetchall()
        if not fetched:
            if self._async_mode:

                async def _raise_not_found() -> Any:
                    msg = "Query returned no rows"
                    raise NotFoundError(msg)

                return _raise_not_found()
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return self._wrap(dict(zip(cols, fetched[0], strict=False)))

    def fetch_optional(self, sql: str, parameters: Any = None) -> Any:
        adapted, bound = self._adapt_sql(sql, parameters)
        cur = self._conn.execute(adapted, bound)
        cols = [str(d[0]) for d in (cur.description or ())]
        fetched = cur.fetchall()
        if not fetched:
            return self._wrap(None)
        return self._wrap(dict(zip(cols, fetched[0], strict=False)))

    def select_value(self, sql: str, parameters: Any = None) -> Any:
        adapted, bound = self._adapt_sql(sql, parameters)
        cur = self._conn.execute(adapted, bound)
        fetched = cur.fetchall()
        if not fetched:
            if self._async_mode:

                async def _raise_not_found() -> Any:
                    msg = "Query returned no rows"
                    raise NotFoundError(msg)

                return _raise_not_found()
            msg = "Query returned no rows"
            raise NotFoundError(msg)
        return self._wrap(fetched[0][0] if fetched[0] else None)

    def select_to_arrow(self, sql: str, parameters: Any = None) -> Any:
        adapted, bound = self._adapt_sql(sql, parameters)
        cur = self._conn.execute(adapted, bound)
        cols = [str(d[0]) for d in (cur.description or ())]
        rows = [tuple(r) for r in cur.fetchall()]
        return self._wrap(ArrowCursor.from_rows(rows, cols, threshold_rows=512))

    def stream_arrow_batches(self, sql: str, parameters: Any = None, batch_size: int = 1024) -> Any:
        adapted, bound = self._adapt_sql(sql, parameters)
        cur = self._conn.execute(adapted, bound)
        cols = [str(d[0]) for d in (cur.description or ())]
        rows = [tuple(r) for r in cur.fetchall()]
        chunk = max(1, int(batch_size))
        batches = [
            ArrowCursor.from_rows(rows[idx : idx + chunk], cols, threshold_rows=512)
            for idx in range(0, len(rows), chunk)
        ]
        return self._wrap(batches)


@pytest.mark.anyio
@pytest.mark.parametrize("engine", ["bigquery", "spanner", "spangres"])
async def test_cloud_dbapi_preserves_statement_autocommit(engine: str) -> None:
    """Cloud DBAPI statements retain DDL support, immediate counts, and read-your-writes."""
    raw_sync = sqlite3.connect(":memory:")
    raw_async = sqlite3.connect(":memory:")
    if engine == "bigquery":
        config = BigQueryConfig(project_id="test-proj", dataset_id="test_ds")
        sync_driver: Any = SyncBigQueryDriver(
            config, client=cast("Any", _ContractBigQueryClient(raw_sync)), broker=None
        )
        async_driver: Any = AsyncBigQueryDriver(
            config, client=cast("Any", _ContractBigQueryClient(raw_async, async_mode=True)), broker=None
        )
        url = "bigquery://test-proj/test_ds"
        dialect_name = "BigQuery"
    else:
        spanner_config = SpannerConfig(
            project_id="test-proj",
            instance_id="test-inst",
            database_id="test-db",
            dialect="spangres" if engine == "spangres" else "googlesql",
            use_shm_cache=False,
        )
        sync_driver = SyncSpannerDriver(
            spanner_config, client=cast("Any", _ContractSpannerClient(raw_sync)), broker=None
        )
        async_driver = AsyncSpannerDriver(
            spanner_config, client=cast("Any", _ContractSpannerClient(raw_async, async_mode=True)), broker=None
        )
        url = f"{engine}://projects/test-proj/instances/test-inst/databases/test-db"
        dialect_name = "Spangres" if engine == "spangres" else "Spanner"
    sync_conn = SyncDBAPIConnection(url, driver=sync_driver, broker=None, share_pool=False)
    async_conn = AsyncDBAPIConnection(url, driver=async_driver, broker=None, share_pool=False)
    try:
        adapted = AsyncAdapt_vertebra_connection(SQLAlchemyAsyncDBAPI(dialect=engine), cast("Any", async_conn))
        if _HAS_SQLALCHEMY_GCP:
            sync_dialect = getattr(dialects, f"Sync{dialect_name}Dialect")()
            async_dialect = getattr(dialects, f"Async{dialect_name}Dialect")()
            assert sync_dialect.get_driver_connection(sync_conn) is sync_conn
            assert async_dialect.get_driver_connection(adapted) is async_conn
        assert adapted.autocommit is True
        sync_cursor = sync_conn.cursor()
        async_cursor = async_conn.cursor()
        sync_cursor.execute("CREATE TABLE cloud_items (id INTEGER PRIMARY KEY)")
        await async_cursor.execute("CREATE TABLE cloud_items (id INTEGER PRIMARY KEY)")
        sync_cursor.execute("INSERT INTO cloud_items VALUES (@id)", {"id": 1})
        await async_cursor.execute("INSERT INTO cloud_items VALUES (@id)", {"id": 1})
        assert sync_cursor.rowcount == async_cursor.rowcount == 1
        sync_cursor.executemany("INSERT INTO cloud_items VALUES (@id)", [{"id": 2}, {"id": 3}])
        await async_cursor.executemany("INSERT INTO cloud_items VALUES (@id)", [{"id": 2}, {"id": 3}])
        assert sync_cursor.rowcount == async_cursor.rowcount == 2
        sync_cursor.execute("SELECT id FROM cloud_items ORDER BY id")
        await async_cursor.execute("SELECT id FROM cloud_items ORDER BY id")
        assert sync_cursor.fetchall() == await async_cursor.fetchall() == [(1,), (2,), (3,)]
        sync_conn.rollback()
        await async_conn.rollback()
        assert sync_driver.select_value("SELECT COUNT(*) FROM cloud_items") == 3
        assert await async_driver.select_value("SELECT COUNT(*) FROM cloud_items") == 3
        assert sync_conn.autocommit is async_conn.autocommit is True
        with pytest.raises(NotSupportedError, match="autocommit"):
            sync_conn.autocommit = False
        with pytest.raises(NotSupportedError, match="autocommit"):
            async_conn.autocommit = False
        assert sync_conn.autocommit is async_conn.autocommit is True
    finally:
        sync_conn.close()
        await async_conn.close()
        raw_sync.close()
        raw_async.close()


_ACTIVE_MYSQL_CONTRACT_SERVERS: list[MySQLWireTestServer] = []


def _make_contract_driver_pair(engine: str) -> tuple[Any, Any, str, Any, str, Any]:
    """Create a seeded `(sync_driver, async_driver, insert_sql, insert_fn, by_id_sql, by_id_fn)` tuple."""
    ddl = "CREATE TABLE contract_items (id INTEGER PRIMARY KEY, name TEXT, note TEXT)"
    if engine == "sqlite":
        sync_drv = SyncSQLiteDriver(SQLiteConfig(database=":memory:"))
        async_drv = AsyncSQLiteDriver(SQLiteConfig(database=":memory:"))
        sync_drv.execute(ddl)
        sync_drv.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", (1, "seed", None))
        async_drv._sync.execute(ddl)
        async_drv._sync.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", (1, "seed", None))
        return (
            sync_drv,
            async_drv,
            "INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)",
            lambda row_id, name: (row_id, name, None),
            "SELECT id, name FROM contract_items WHERE id = ?",
            lambda row_id: (row_id,),
        )
    if engine == "duckdb":
        pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
        sync_dd = SyncDuckDBDriver(DuckDBConfig(database=":memory:"))
        async_dd = AsyncDuckDBDriver(DuckDBConfig(database=":memory:"))
        sync_dd.execute(ddl)
        sync_dd.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", [1, "seed", None])
        assert async_dd._client is not None
        sync_for_async = async_dd._client.to_sync()
        sync_for_async.execute(ddl)
        sync_for_async.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", [1, "seed", None])
        return (
            sync_dd,
            async_dd,
            "INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)",
            lambda row_id, name: [row_id, name, None],
            "SELECT id, name FROM contract_items WHERE id = ?",
            lambda row_id: [row_id],
        )
    if engine == "mssql":
        sync_ms_conn = sqlite3.connect(":memory:", check_same_thread=False)
        sync_ms_conn.execute(ddl)
        sync_ms_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        sync_ms_conn.commit()
        async_ms_conn = sqlite3.connect(":memory:", check_same_thread=False)
        async_ms_conn.execute(ddl)
        async_ms_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        async_ms_conn.commit()
        ms_cfg = MSSQLConfig(dsn="mssql://127.0.0.1:1433/master")
        sync_ms = SyncMSSQLDriver(ms_cfg, client=cast("Any", _ContractMSSQLClient(sync_ms_conn, async_mode=False)))
        async_ms = AsyncMSSQLDriver(ms_cfg, client=cast("Any", _ContractMSSQLClient(async_ms_conn, async_mode=True)))
        return (
            sync_ms,
            async_ms,
            "INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)",
            lambda row_id, name: (row_id, name, None),
            "SELECT id, name FROM contract_items WHERE id = ?",
            lambda row_id: (row_id,),
        )
    if engine == "postgres":
        sync_cluster = FakePostgresCluster()
        sync_cluster._conn.execute(ddl)
        sync_cluster._conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        async_cluster = FakePostgresCluster()
        async_cluster._conn.execute(ddl)
        async_cluster._conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        sync_pg = SyncPostgreSQLDriver(
            PostgreSQLConfig(dsn="postgresql://localhost/test"), pool=FakeSyncPostgresPool(sync_cluster)
        )
        async_pg = AsyncPostgreSQLDriver(
            PostgreSQLConfig(dsn="postgresql://localhost/test"), pool=FakeAsyncPostgresPool(async_cluster)
        )
        return (
            sync_pg,
            async_pg,
            "INSERT INTO contract_items (id, name, note) VALUES ($1, $2, $3)",
            lambda row_id, name: (row_id, name, None),
            "SELECT id, name FROM contract_items WHERE id = $1",
            lambda row_id: (row_id,),
        )
    if engine == "mysql":
        sync_srv = MySQLWireTestServer()
        async_srv = MySQLWireTestServer()
        _ACTIVE_MYSQL_CONTRACT_SERVERS.extend((sync_srv, async_srv))
        sync_my = SyncMySQLDriver(MySQLConfig.from_url(sync_srv.dsn))
        async_my = AsyncMySQLDriver(MySQLConfig.from_url(async_srv.dsn))
        for seed_srv in (sync_srv, async_srv):
            seed_drv = SyncMySQLDriver(MySQLConfig.from_url(seed_srv.dsn))
            try:
                seed_drv.execute(ddl)
                seed_drv.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
            finally:
                seed_drv.close()
        return (
            sync_my,
            async_my,
            "INSERT INTO contract_items (id, name, note) VALUES (%s, %s, %s)",
            lambda row_id, name: (row_id, name, None),
            "SELECT id, name FROM contract_items WHERE id = %s",
            lambda row_id: (row_id,),
        )
    if engine == "bigquery":
        sync_bq_conn = sqlite3.connect(":memory:", check_same_thread=False)
        sync_bq_conn.execute(ddl)
        sync_bq_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        sync_bq_conn.commit()
        async_bq_conn = sqlite3.connect(":memory:", check_same_thread=False)
        async_bq_conn.execute(ddl)
        async_bq_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        async_bq_conn.commit()
        bq_cfg = BigQueryConfig(project_id="test-proj", dataset_id="test_ds")
        sync_bq = SyncBigQueryDriver(
            bq_cfg, client=cast("Any", _ContractBigQueryClient(sync_bq_conn, async_mode=False))
        )
        async_bq = AsyncBigQueryDriver(
            bq_cfg, client=cast("Any", _ContractBigQueryClient(async_bq_conn, async_mode=True))
        )
        return (
            sync_bq,
            async_bq,
            "INSERT INTO contract_items (id, name, note) VALUES (@id, @name, @note)",
            lambda row_id, name: {"id": row_id, "name": name, "note": None},
            "SELECT id, name FROM contract_items WHERE id = @id",
            lambda row_id: {"id": row_id},
        )
    if engine == "spanner":
        sync_sp_conn = sqlite3.connect(":memory:", check_same_thread=False)
        sync_sp_conn.execute(ddl)
        sync_sp_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        sync_sp_conn.commit()
        async_sp_conn = sqlite3.connect(":memory:", check_same_thread=False)
        async_sp_conn.execute(ddl)
        async_sp_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        async_sp_conn.commit()
        sp_cfg = SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="test-db")
        sync_sp = SyncSpannerDriver(sp_cfg, client=cast("Any", _ContractSpannerClient(sync_sp_conn, async_mode=False)))
        async_sp = AsyncSpannerDriver(
            sp_cfg, client=cast("Any", _ContractSpannerClient(async_sp_conn, async_mode=True))
        )
        return (
            sync_sp,
            async_sp,
            "INSERT INTO contract_items (id, name, note) VALUES (@id, @name, @note)",
            lambda row_id, name: {"id": row_id, "name": name, "note": None},
            "SELECT id, name FROM contract_items WHERE id = @id",
            lambda row_id: {"id": row_id},
        )
    msg = f"Unsupported engine {engine}"
    raise ValueError(msg)


@pytest.mark.anyio
@pytest.mark.parametrize("engine", _SQL_CONTRACT_ENGINES)
async def test_unified_driver_contract_across_sql_engines(engine: str) -> None:
    """Verify unified signatures, return types, NotFoundError, Arrow, transactions, and alias removal."""
    for alias in _LEGACY_DRIVER_ALIASES:
        assert not hasattr(SyncDatabaseDriver, alias)
        assert not hasattr(AsyncDatabaseDriver, alias)

    sync_drv, async_drv, insert_sql, insert_params, by_id_sql, by_id_params = _make_contract_driver_pair(engine)
    try:
        for alias in _LEGACY_DRIVER_ALIASES:
            assert not hasattr(sync_drv, alias), f"{engine} sync driver still exposes {alias}"
            assert not hasattr(async_drv, alias), f"{engine} async driver still exposes {alias}"

        affected_sync = sync_drv.execute(insert_sql, parameters=insert_params(2, "two"))
        assert type(affected_sync) is int
        assert affected_sync == 1

        affected_async = await async_drv.execute(insert_sql, parameters=insert_params(2, "two"))
        assert type(affected_async) is int
        assert affected_async == 1

        desc_res_sync = sync_drv.execute_with_description(by_id_sql, parameters=by_id_params(1))
        assert isinstance(desc_res_sync, tuple) and len(desc_res_sync) == 2
        rows_sync, cols_sync = desc_res_sync
        assert rows_sync == [(1, "seed")]
        assert cols_sync == ("id", "name")

        desc_res_async = await async_drv.execute_with_description(by_id_sql, parameters=by_id_params(1))
        assert isinstance(desc_res_async, tuple) and len(desc_res_async) == 2
        rows_async, cols_async = desc_res_async
        assert rows_async == [(1, "seed")]
        assert cols_async == ("id", "name")

        arrow_sync = sync_drv.select_to_arrow(by_id_sql, parameters=by_id_params(1), threshold_rows=512)
        assert isinstance(arrow_sync, ArrowCursor)
        assert arrow_sync.row_count == 1

        arrow_async = await async_drv.select_to_arrow(by_id_sql, parameters=by_id_params(1), threshold_rows=512)
        assert isinstance(arrow_async, ArrowCursor)
        assert arrow_async.row_count == 1

        with pytest.raises(NotFoundError) as sync_one_exc:
            sync_drv.select_one(by_id_sql, parameters=by_id_params(999))
        assert isinstance(sync_one_exc.value, (DatabaseError, ValueError))

        with pytest.raises(NotFoundError) as async_one_exc:
            await async_drv.select_one(by_id_sql, parameters=by_id_params(999))
        assert isinstance(async_one_exc.value, (DatabaseError, ValueError))

        assert sync_drv.select_one_or_none(by_id_sql, parameters=by_id_params(999)) is None
        assert await async_drv.select_one_or_none(by_id_sql, parameters=by_id_params(999)) is None

        with pytest.raises(NotFoundError):
            sync_drv.select_value(by_id_sql, parameters=by_id_params(999))
        with pytest.raises(NotFoundError):
            await async_drv.select_value(by_id_sql, parameters=by_id_params(999))

        null_sql = "SELECT note FROM contract_items WHERE id = 1"
        assert sync_drv.select_value(null_sql) is None
        assert await async_drv.select_value(null_sql) is None

        with sync_drv.transaction():
            sync_drv.execute(insert_sql, parameters=insert_params(10, "tx_committed"))
        assert sync_drv.select_one(by_id_sql, parameters=by_id_params(10))["name"] == "tx_committed"

        with pytest.raises(RuntimeError, match="abort_sync_tx"), sync_drv.transaction():
            sync_drv.execute(insert_sql, parameters=insert_params(11, "tx_rolled_back"))
            raise RuntimeError("abort_sync_tx")
        assert sync_drv.select_one_or_none(by_id_sql, parameters=by_id_params(11)) is None

        async with async_drv.transaction():
            await async_drv.execute(insert_sql, parameters=insert_params(10, "tx_committed"))
        assert (await async_drv.select_one(by_id_sql, parameters=by_id_params(10)))["name"] == "tx_committed"

        with pytest.raises(RuntimeError, match="abort_async_tx"):
            async with async_drv.transaction():
                await async_drv.execute(insert_sql, parameters=insert_params(11, "tx_rolled_back"))
                raise RuntimeError("abort_async_tx")
        assert await async_drv.select_one_or_none(by_id_sql, parameters=by_id_params(11)) is None

        sync_batches = list(sync_drv.stream_arrow_batches("SELECT id, name FROM contract_items", batch_size=2))
        assert len(sync_batches) >= 1
        assert sum(len(b) for b in sync_batches) == 3

        async_batches = [
            batch async for batch in async_drv.stream_arrow_batches("SELECT id, name FROM contract_items", batch_size=2)
        ]
        assert len(async_batches) >= 1
        assert sum(len(b) for b in async_batches) == 3

        assert sync_drv.cancel() is None
        assert await async_drv.cancel() is None
    finally:
        sync_drv.close()
        await async_drv.close()


def test_sqlite_driver_error_translation_and_parameter_normalization() -> None:
    """Verify SyncSQLiteDriver maps sqlite3 errors to DatabaseError and normalizes rich bind types."""
    driver = SyncSQLiteDriver(SQLiteConfig(database=":memory:"))
    try:
        driver.execute(
            "CREATE TABLE typed_items (id INTEGER PRIMARY KEY, price TEXT, created_at TEXT, ref_id TEXT, meta TEXT, tags TEXT)"
        )
        ts = datetime.datetime(2026, 11, 4, 17, 30, tzinfo=datetime.UTC)
        ref = uuid.UUID("12345678-1234-5678-1234-567812345678")
        inserted = driver.execute(
            "INSERT INTO typed_items (id, price, created_at, ref_id, meta, tags) VALUES (?, ?, ?, ?, ?, ?)",
            (1, decimal.Decimal("42.75"), ts, ref, {"tier": "gold"}, ["a", "b"]),
        )
        assert inserted == 1

        row = driver.select_one("SELECT price, created_at, ref_id, meta, tags FROM typed_items WHERE id = ?", (1,))
        assert row["price"] == "42.75"
        assert row["created_at"] == ts.isoformat()
        assert row["ref_id"] == str(ref)
        assert decode_json(row["meta"]) == {"tier": "gold"}
        assert decode_json(row["tags"]) == ["a", "b"]

        with pytest.raises(IntegrityError) as dup_exc:
            driver.execute(
                "INSERT INTO typed_items (id, price, created_at, ref_id, meta, tags) VALUES (?, ?, ?, ?, ?, ?)",
                (1, decimal.Decimal("1.00"), ts, ref, {}, []),
            )
        assert isinstance(dup_exc.value, DatabaseError)

        with pytest.raises((OperationalError, ProgrammingError)) as sql_exc:
            driver.execute("SELECT * FROM missing_table_xyz")
        assert isinstance(sql_exc.value, DatabaseError)
    finally:
        driver.close()


@pytest.mark.anyio
async def test_spanner_snapshots_and_transactions_not_found_and_buffered_mutations() -> None:
    """Verify Spanner snapshot/transaction NotFoundError, select_one_or_none, select_to_arrow, and mutation buffering."""
    sync_drv, async_drv, _, _, by_id_sql, by_id_params = _make_contract_driver_pair("spanner")
    try:
        with sync_drv.snapshot() as sync_snap:
            assert not hasattr(sync_snap, "fetch_arrow")
            snap_cursor = sync_snap.select_to_arrow(by_id_sql, by_id_params(1), threshold_rows=256)
            assert isinstance(snap_cursor, ArrowCursor)
            assert snap_cursor.row_count == 1
            assert sync_snap.select_one_or_none(by_id_sql, by_id_params(999)) is None
            with pytest.raises(NotFoundError):
                sync_snap.select_one(by_id_sql, by_id_params(999))
            with pytest.raises(NotFoundError):
                sync_snap.query_one(by_id_sql, by_id_params(999))

        async with async_drv.snapshot() as async_snap:
            assert not hasattr(async_snap, "fetch_arrow")
            async_cursor = await async_snap.select_to_arrow(by_id_sql, by_id_params(1), threshold_rows=256)
            assert isinstance(async_cursor, ArrowCursor)
            assert async_cursor.row_count == 1
            assert await async_snap.select_one_or_none(by_id_sql, by_id_params(999)) is None
            with pytest.raises(NotFoundError):
                await async_snap.select_one(by_id_sql, by_id_params(999))
            with pytest.raises(NotFoundError):
                await async_snap.query_one(by_id_sql, by_id_params(999))

        def _check_sync_tx(sync_tx: SyncSpannerTransaction) -> None:
            assert isinstance(sync_tx, SyncSpannerTransaction)
            assert sync_tx.select_one_or_none(by_id_sql, by_id_params(999)) is None
            with pytest.raises(NotFoundError):
                sync_tx.select_one(by_id_sql, by_id_params(999))
            with pytest.raises(NotFoundError):
                sync_tx.query_one(by_id_sql, by_id_params(999))

        sync_drv.run_in_transaction(_check_sync_tx)

        async def _check_async_tx(async_tx: AsyncSpannerTransaction) -> None:
            assert isinstance(async_tx, AsyncSpannerTransaction)
            assert await async_tx.select_one_or_none(by_id_sql, by_id_params(999)) is None
            with pytest.raises(NotFoundError):
                await async_tx.select_one(by_id_sql, by_id_params(999))
            with pytest.raises(NotFoundError):
                await async_tx.query_one(by_id_sql, by_id_params(999))

        await async_drv.run_in_transaction(_check_async_tx)

        with sync_drv.transaction():
            sync_drv.apply_mutations(
                [Mutation.insert_or_update("contract_items", ("id", "name", "note"), [(50, "mut_ok", None)])]
            )
        assert sync_drv.select_one(by_id_sql, by_id_params(50))["name"] == "mut_ok"

        with pytest.raises(RuntimeError, match="abort_mut"), sync_drv.transaction():
            sync_drv.apply_mutations(
                [Mutation.insert_or_update("contract_items", ("id", "name", "note"), [(51, "mut_abort", None)])]
            )
            raise RuntimeError("abort_mut")
        assert sync_drv.select_one_or_none(by_id_sql, by_id_params(51)) is None
    finally:
        sync_drv.close()
        await async_drv.close()


def test_zero_type_ignore_override_annotations_in_db_drivers() -> None:
    """Verify zero `# type: ignore[override]` annotations remain in vertebra/db/driver and adapters."""
    repo_root = Path(__file__).resolve().parents[5]
    db_root = repo_root / "src/py/vertebra/db"
    forbidden = f"{chr(35)} type: ignore[override]"
    targets = [*sorted((db_root / "driver").glob("*.py")), *sorted((db_root / "adapters").glob("*/driver.py"))]
    assert targets
    offenders = [str(path.relative_to(repo_root)) for path in targets if forbidden in path.read_text(encoding="utf-8")]
    assert offenders == []


@pytest.mark.anyio
async def test_dialect_tag_uniqueness_deterministic_file_urls_and_spanner_cdc_protocol() -> None:
    """Verify non-overlapping dialect_tag values, deterministic SQLite/DuckDB from_url paths, and Spanner CDC protocols."""
    tags = [tag for cfg_cls in REGISTERED_CONFIG_TYPES if (tag := getattr(cfg_cls, "dialect_tag", 0)) != 0]
    tags.append(SPANGRES_DIALECT_TAG)
    assert len(tags) == 12
    assert len(set(tags)) == 12
    assert set(tags) == set(range(1, 13))

    assert SQLiteConfig.from_url("sqlite:///tmp.db").path == "tmp.db"
    assert SQLiteConfig.from_url("sqlite:////tmp/abs.db").path == "/tmp/abs.db"
    assert SQLiteConfig.from_url("sqlite://rel.db").path == "rel.db"
    assert SQLiteConfig.from_url("sqlite:///:memory:").path == ":memory:"
    assert SQLiteConfig.from_url("sqlite:///tmp.db", path="/override.db").path == "/override.db"

    assert DuckDBConfig.from_url("duckdb:///tmp.duckdb").path == "tmp.duckdb"
    assert DuckDBConfig.from_url("duckdb:////tmp/abs.duckdb").path == "/tmp/abs.duckdb"
    assert DuckDBConfig.from_url("duckdb://rel.duckdb").path == "rel.duckdb"
    assert DuckDBConfig.from_url("duckdb:///:memory:").path == ":memory:"
    assert DuckDBConfig.from_url("duckdb:///tmp.duckdb", path="/override.duckdb").path == "/override.duckdb"

    class _EmptySyncSpannerCDCDriver:
        dialect = "googlesql"

        def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
            _ = (sql, parameters)
            return []

    class _EmptyAsyncSpannerCDCDriver:
        dialect = "googlesql"

        async def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
            _ = (sql, parameters)
            return []

    sync_source = SyncSpannerChangeStreamSource(_EmptySyncSpannerCDCDriver(), "orders_stream")
    async_source = AsyncSpannerChangeStreamSource(_EmptyAsyncSpannerCDCDriver(), "orders_stream")
    assert isinstance(sync_source, SyncChangeSource)
    assert isinstance(async_source, AsyncChangeSource)
    assert list(sync_source.stream()) == []
    assert [ev async for ev in async_source.stream()] == []


class _LeaseTestConfig(BaseDatabaseConfig, frozen=True, gc=False):
    """Minimal database config struct for lease helper contract tests."""


class _LeaseTestDriver(CommonDriverMixin[_LeaseTestConfig]):
    """Minimal driver subclass exposing CommonDriverMixin lease helpers."""

    __slots__ = ("__weakref__",)


class _RecordingSlotLease:
    """Lease double identified by slot_index and generation."""

    __slots__ = ("generation", "slot_index")

    def __init__(self, slot_index: int = 7, generation: int = 3) -> None:
        self.slot_index = slot_index
        self.generation = generation


class _SelfReleasingLease:
    """Lease double exposing a synchronous release() method."""

    __slots__ = ("releases",)

    def __init__(self) -> None:
        self.releases = 0

    def release(self) -> None:
        self.releases += 1


class _RecordingBroker:
    """Broker double recording acquire and release calls."""

    def __init__(self, *, fail_acquire: bool = False, lease: Any = None) -> None:
        self.fail_acquire = fail_acquire
        self.lease = lease if lease is not None else _RecordingSlotLease()
        self.acquired: list[tuple[int, int]] = []
        self.released: list[tuple[int, int]] = []

    def acquire(self, pid: int, timeout_ms: int = 5000) -> Any:
        self.acquired.append((pid, timeout_ms))
        if self.fail_acquire:
            msg = "broker saturated"
            raise RuntimeError(msg)
        return self.lease

    def release(self, slot_index: int, generation: int) -> None:
        self.released.append((slot_index, generation))


class _RecordingLeaseTable:
    """Lease table double for brokers exposing broker.lease_table."""

    def __init__(self, lease: _RecordingSlotLease) -> None:
        self.lease = lease
        self.released: list[tuple[int, int]] = []

    def acquire(self, pid: int) -> _RecordingSlotLease:
        _ = pid
        return self.lease

    def release(self, slot_index: int, generation: int) -> None:
        self.released.append((slot_index, generation))


class _TableBackedBroker:
    """Broker double delegating acquire and release to lease_table."""

    def __init__(self, lease: _RecordingSlotLease) -> None:
        self.lease_table = _RecordingLeaseTable(lease)


class _RecordingLeaseClient:
    """Native client double recording attach_lease calls."""

    def __init__(self) -> None:
        self.attached: list[Any] = []

    def attach_lease(self, releaser: Any) -> None:
        self.attached.append(releaser)


def _direct_and_arg_referents(obj: Any) -> list[Any]:
    """Return direct gc referents and elements of any referent tuples."""
    referents = gc.get_referents(obj)
    expanded: list[Any] = list(referents)
    for item in referents:
        if isinstance(item, tuple):
            expanded.extend(item)
    return expanded


@pytest.mark.parametrize("shape", ["broker_release", "self_releasing", "lease_table"])
def test_lease_releaser_invokes_expected_broker_release_shape(shape: str) -> None:
    """_lease_releaser invokes the matching release path for every broker shape."""
    if shape == "broker_release":
        lease = _RecordingSlotLease(slot_index=4, generation=9)
        broker = _RecordingBroker(lease=lease)
        plan = _BrokerLeasePlan(broker)
        _lease_releaser(broker, plan, lease)()
        assert broker.released == [(4, 9)]
    elif shape == "self_releasing":
        self_lease = _SelfReleasingLease()
        broker = _RecordingBroker(lease=self_lease)
        plan = _BrokerLeasePlan(broker)
        _lease_releaser(broker, plan, self_lease)()
        assert self_lease.releases == 1
        assert broker.released == []
    else:
        lease = _RecordingSlotLease(slot_index=11, generation=2)
        table_broker = _TableBackedBroker(lease)
        plan = _BrokerLeasePlan(table_broker)
        _lease_releaser(table_broker, plan, lease)()
        assert table_broker.lease_table.released == [(11, 2)]


def test_lease_releaser_holds_no_driver_reference() -> None:
    """_lease_releaser references only the broker, plan, and lease, allowing driver collection."""
    lease = _RecordingSlotLease(slot_index=5, generation=1)
    broker = _RecordingBroker(lease=lease)
    driver = _LeaseTestDriver(_LeaseTestConfig(), broker=broker)
    plan = driver._lease_plan()
    client = _RecordingLeaseClient()
    assert driver._attach_checkout_lease(client) is True
    assert len(client.attached) == 1
    releaser = client.attached[0]

    referents = _direct_and_arg_referents(releaser)
    assert any(ref is broker for ref in referents)
    assert any(ref is plan for ref in referents)
    assert any(ref is lease for ref in referents)
    assert all(ref is not driver for ref in referents)

    driver_ref = weakref.ref(driver)
    del driver
    gc.collect()
    assert driver_ref() is None

    releaser()
    assert broker.released == [(5, 1)]


@pytest.mark.parametrize(
    ("broker_mode", "expected_attached"),
    [
        ("refused", False),
        ("none", False),
        ("granted", True),
    ],
)
def test_attach_checkout_lease_behavior(broker_mode: str, expected_attached: bool) -> None:
    """_attach_checkout_lease attaches a callable on granted acquire and skips on refusal or no broker."""
    client = _RecordingLeaseClient()
    broker: _RecordingBroker | None
    if broker_mode == "refused":
        broker = _RecordingBroker(fail_acquire=True)
    elif broker_mode == "none":
        broker = None
    else:
        broker = _RecordingBroker(lease=_RecordingSlotLease(slot_index=8, generation=6))

    driver = _LeaseTestDriver(_LeaseTestConfig(), broker=broker)
    attached = driver._attach_checkout_lease(client, timeout_ms=250)
    assert attached is expected_attached

    if expected_attached:
        assert broker is not None
        assert len(broker.acquired) == 1
        assert broker.acquired[0][1] == 250
        assert len(client.attached) == 1
        assert callable(client.attached[0])
        client.attached[0]()
        assert broker.released == [(8, 6)]
    else:
        assert client.attached == []


def test_acquire_broker_lease_calls_recording_broker() -> None:
    """_acquire_broker_lease acquires a lease on a recording broker with the requested timeout."""
    lease = _RecordingSlotLease(slot_index=3, generation=7)
    broker = _RecordingBroker(lease=lease)
    driver = _LeaseTestDriver(_LeaseTestConfig(), broker=broker)

    acquired = driver._acquire_broker_lease(timeout_ms=125)
    assert acquired is lease
    assert len(broker.acquired) == 1
    assert broker.acquired[0][1] == 125
