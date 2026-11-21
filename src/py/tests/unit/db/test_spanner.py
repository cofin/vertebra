"""Unit tests for Google Cloud Spanner drivers, mutations, Arrow scans, ObjectStore offload, SpannerEmbeddingPushdown, and DataFusion."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import importlib
import inspect
import os
import re
import sqlite3
import sys
import threading
import time as _time
import weakref
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast

import pytest

import vertebra.db.adapters.spanner as sp_pkg
import vertebra.db.adapters.spanner.config as sp_config_mod
from vertebra._spanner import (
    AsyncSpannerClient,
    AsyncSpannerCursor,
    SyncSpannerClient,
    SyncSpannerCursor,
)
from vertebra._vertebra import (
    BigQueryError,
    BigQueryRowError,
    GoogleCloudAuthError,
    GoogleCloudError,
    InterfaceError,
    LeaseBroker,
    ProgrammingError,
    SpannerAbortedError,
    SpannerError,
)
from vertebra.db import DatabaseRegistry, NotSupportedError
from vertebra.db.adapters.datafusion import DataFusionEngine
from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    Mutation,
    MutationGroup,
    SpannerConfig,
    SpannerCore,
    SpannerEmbeddingPushdown,
    StalenessBound,
    SyncSpannerDriver,
    SyncSpannerTransaction,
)
from vertebra.db.driver import DatabaseError, DataError, OperationalError
from vertebra.storage import SyncObjectStore
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError, VertebraError, reset_dependency_cache
from vertebra.utils.serializers import decode_json, encode_json


def _strip_leading_sql_comments(sql: str) -> str:
    text = sql.lstrip()
    while True:
        if text.startswith("/*"):
            end = text.find("*/")
            if end == -1:
                break
            text = text[end + 2 :].lstrip()
        elif text.startswith("--"):
            nl = text.find("\n")
            if nl == -1:
                return ""
            text = text[nl + 1 :].lstrip()
        else:
            break
    return text


class FakeSyncSpannerCursor:
    """Test-only synchronous PEP 249 cursor backed by `FakeSpannerClient` or `FakeSyncReadWriteTransaction`."""

    __slots__ = (
        "_arraysize",
        "_client",
        "_closed",
        "_description",
        "_index",
        "_param_encoder",
        "_request_tag",
        "_rowcount",
        "_rows",
        "_staleness_mode",
        "_staleness_value",
        "_transaction_tag",
        "_tx",
    )

    def __init__(
        self,
        client: FakeSpannerClient,
        *,
        tx: FakeSyncReadWriteTransaction | None = None,
        staleness_mode: str = "strong",
        staleness_value: int = 0,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
    ) -> None:
        self._client = client
        self._tx = tx
        self._staleness_mode = staleness_mode
        self._staleness_value = staleness_value
        self._request_tag = request_tag
        self._transaction_tag = transaction_tag
        self._param_encoder = param_encoder
        self._description: tuple[tuple[str, str, None, None, None, None, None], ...] | None = None
        self._rows: list[tuple[Any, ...]] = []
        self._index = 0
        self._rowcount = -1
        self._arraysize = 1
        self._closed = False

    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, None], ...] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    @property
    def lastrowid(self) -> Any | None:
        return None

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if value <= 0:
            msg = "arraysize must be positive"
            raise ProgrammingError(msg)
        self._arraysize = value

    def _encode_params(self, params: Any) -> str | None:
        if params is None or params == () or params == [] or params == {}:
            return None
        if isinstance(params, str):
            return params
        if self._param_encoder is not None:
            return self._param_encoder(params)
        msg = "Spanner cursor needs encoded parameters or a param_encoder"
        raise InterfaceError(msg)

    def execute(
        self,
        sql: str,
        params: str | Sequence[Any] | Mapping[str, Any] = (),
    ) -> FakeSyncSpannerCursor:
        if self._closed:
            msg = "Cursor is closed"
            raise InterfaceError(msg)
        self._description = None
        self._rows = []
        self._index = 0
        self._rowcount = -1

        uncommented = _strip_leading_sql_comments(sql)
        upper = uncommented.upper()
        if SpannerCore.is_ddl_statement(uncommented):
            if self._tx is not None:
                msg = "DDL statements cannot be executed inside a Spanner ReadWrite transaction"
                raise NotSupportedError(msg)
            self._client.update_ddl([uncommented])
            self._rowcount = -1
            return self

        params_json = self._encode_params(params)
        if upper.startswith(("SELECT", "WITH", "GRAPH", "VALUES", "EXPLAIN", "SHOW", "PRAGMA")):
            if self._tx is not None:
                raw = self._tx.query(sql, params_json, self._request_tag)
            else:
                raw = self._client.query(sql, params_json, self._staleness_mode, self._staleness_value)
            cols = raw.get("columns", ())
            self._description = tuple((str(c), "UNKNOWN", None, None, None, None, None) for c in cols)
            self._rows = [tuple(r) for r in raw.get("rows", ())]
            self._rowcount = len(self._rows)
            return self

        if self._tx is not None:
            count = self._tx.execute_dml(sql, params_json, self._request_tag)
        else:
            count = self._client.execute_dml(sql, params_json, self._request_tag, self._transaction_tag)
        self._rowcount = int(count)
        return self

    def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[str | Sequence[Any] | Mapping[str, Any]],
    ) -> FakeSyncSpannerCursor:
        if self._closed:
            msg = "Cursor is closed"
            raise InterfaceError(msg)
        total = 0
        for p in seq_of_params:
            self.execute(sql, p)
            total += max(self._rowcount, 0)
        self._rowcount = total
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        count = self._arraysize if size is None else size
        if count < 0:
            msg = "fetchmany size must be non-negative"
            raise ProgrammingError(msg)
        if count == 0:
            return []
        end = min(len(self._rows), self._index + count)
        batch = self._rows[self._index : end]
        self._index = end
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        rest = self._rows[self._index :]
        self._index = len(self._rows)
        return rest

    def close(self) -> None:
        self._closed = True
        self._rows.clear()
        self._description = None


class FakeAsyncSpannerCursor:
    """Test-only asynchronous PEP 249 cursor wrapping `FakeSyncSpannerCursor`."""

    __slots__ = ("_sync",)

    def __init__(self, sync_cursor: FakeSyncSpannerCursor) -> None:
        self._sync = sync_cursor

    @property
    def description(self) -> tuple[tuple[str, str, None, None, None, None, None], ...] | None:
        return self._sync.description

    @property
    def rowcount(self) -> int:
        return self._sync.rowcount

    @property
    def lastrowid(self) -> Any | None:
        return self._sync.lastrowid

    @property
    def arraysize(self) -> int:
        return self._sync.arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._sync.arraysize = value

    async def execute(
        self,
        sql: str,
        params: str | Sequence[Any] | Mapping[str, Any] = (),
    ) -> FakeAsyncSpannerCursor:
        self._sync.execute(sql, params)
        return self

    async def executemany(
        self,
        sql: str,
        seq_of_params: Sequence[str | Sequence[Any] | Mapping[str, Any]],
    ) -> FakeAsyncSpannerCursor:
        self._sync.executemany(sql, seq_of_params)
        return self

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self._sync.fetchone()

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        return self._sync.fetchmany(size)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._sync.fetchall()

    async def close(self) -> None:
        self._sync.close()


class FakeSyncReadWriteTransaction:
    """Test-only interactive `ReadWrite` transaction double backed by `FakeSpannerClient`."""

    __slots__ = (
        "_client",
        "_closed",
        "_max_commit_delay_ms",
        "_transaction_id",
    )

    def __init__(
        self,
        client: FakeSpannerClient,
        transaction_id: bytes,
        *,
        max_commit_delay_ms: int | None = None,
    ) -> None:
        self._client = client
        self._transaction_id = transaction_id
        self._max_commit_delay_ms = max_commit_delay_ms
        self._closed = False

    @property
    def transaction_id(self) -> bytes:
        """Return the synthetic transaction identifier bytes."""
        return self._transaction_id

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> dict[str, Any]:
        """Execute a query inside this active transaction double."""
        _ = request_tag
        if self._closed:
            msg = "Transaction is already closed"
            raise RuntimeError(msg)
        with self._client._lock:
            return self._client._query_unlocked(sql, params_json)

    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> int:
        """Execute a DML statement inside this active transaction without auto-committing."""
        _ = request_tag
        if self._closed:
            msg = "Transaction is already closed"
            raise RuntimeError(msg)
        with self._client._lock:
            return self._client._execute_dml_unlocked(sql, params_json, commit=False)

    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements inside this active transaction without auto-committing."""
        _ = request_tag
        if self._closed:
            msg = "Transaction is already closed"
            raise RuntimeError(msg)
        decoded = decode_json(statements_json) if statements_json else []
        counts: list[int] = []
        with self._client._lock:
            for item in decoded:
                stmt_sql = str(item.get("sql", ""))
                stmt_params = item.get("params")
                params_str = encode_json(stmt_params) if stmt_params is not None else None
                counts.append(self._client._execute_dml_unlocked(stmt_sql, params_str, commit=False))
        return counts

    def commit(self, mutations_json: str | None = None) -> dict[str, Any]:
        """Commit buffered DML and optional mutations atomically against the backing SQLite connection."""
        if self._closed:
            msg = "Transaction is already closed"
            raise RuntimeError(msg)
        with self._client._lock:
            if self._max_commit_delay_ms is not None:
                self._client.last_max_commit_delay_ms = self._max_commit_delay_ms
            count = 0
            if mutations_json:
                count = self._client._apply_mutations_unlocked(mutations_json)
            self._client._conn.commit()
            self._closed = True
            return {
                "commit_timestamp_micros": int(_time.time() * 1_000_000),
                "mutation_count": count,
                "retry_count": 0,
            }

    def rollback(self) -> None:
        """Roll back any uncommitted DML inside the backing SQLite connection."""
        if self._closed:
            return
        with self._client._lock:
            self._client._conn.rollback()
            self._client.tx_rollbacks.append(self._transaction_id)
            self._closed = True

    def cursor(
        self,
        *,
        request_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
    ) -> FakeSyncSpannerCursor:
        """Create a synchronous cursor bound to this transaction double."""
        return FakeSyncSpannerCursor(
            self._client,
            tx=self,
            request_tag=request_tag,
            param_encoder=param_encoder,
        )


class FakeSpannerClient:
    """Test-only in-memory Spanner wire client double for unit testing `SyncSpannerDriver` and `AsyncSpannerDriver`."""

    __slots__ = (
        "_conn",
        "_lock",
        "_tx_counter",
        "begin_tx_calls",
        "cursor_calls",
        "ddl_history",
        "last_max_commit_delay_ms",
        "tx_rollbacks",
    )

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._tx_counter = 0
        self.begin_tx_calls: list[dict[str, Any]] = []
        self.cursor_calls: list[dict[str, Any]] = []
        self.ddl_history: list[str] = []
        self.last_max_commit_delay_ms: int | None = None
        self.tx_rollbacks: list[bytes] = []

    def cursor(
        self,
        *,
        staleness_mode: str = "strong",
        staleness_value: int = 0,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
        lazy_begin: bool = False,
    ) -> FakeSyncSpannerCursor:
        """Create a synchronous cursor bound to this client double."""
        if lazy_begin:
            msg = "Spanner cursors do not support lazy_begin"
            raise NotSupportedError(msg)
        self.cursor_calls.append(
            {
                "staleness_mode": staleness_mode,
                "staleness_value": staleness_value,
                "request_tag": request_tag,
                "transaction_tag": transaction_tag,
            }
        )
        return FakeSyncSpannerCursor(
            self,
            staleness_mode=staleness_mode,
            staleness_value=staleness_value,
            request_tag=request_tag,
            transaction_tag=transaction_tag,
            param_encoder=param_encoder,
        )

    def begin_read_write_transaction(
        self,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> FakeSyncReadWriteTransaction:
        """Open an interactive read-write transaction handle against the test double."""
        with self._lock:
            self._tx_counter += 1
            tx_id = f"fake-tx-{self._tx_counter}".encode()
            self.begin_tx_calls.append(
                {
                    "transaction_id": tx_id,
                    "transaction_tag": transaction_tag,
                    "previous_transaction_id": previous_transaction_id,
                    "exclude_txn_from_change_streams": exclude_txn_from_change_streams,
                    "max_commit_delay_ms": max_commit_delay_ms,
                }
            )
            return FakeSyncReadWriteTransaction(
                self,
                tx_id,
                max_commit_delay_ms=max_commit_delay_ms,
            )

    def _query_unlocked(self, sql: str, params_json: str | None = None) -> dict[str, Any]:
        info_schema = self._try_information_schema(sql, params_json)
        if info_schema is not None:
            return info_schema
        adapted, args = self._adapt_sql(sql, params_json)
        cur = self._conn.execute(adapted, args)
        col_names = [str(d[0]) for d in (cur.description or ())]
        raw_rows = [[self._decode_cell(v) for v in row] for row in cur.fetchall()]
        return {
            "columns": col_names,
            "rows": raw_rows,
            "row_count_exact": None,
            "row_count_lower_bound": None,
            "read_timestamp_micros": int(_time.time() * 1_000_000),
        }

    def query(
        self,
        sql: str,
        params_json: str | None = None,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        """Execute a synchronous query against the test double."""
        _ = (staleness_mode, staleness_ms)
        with self._lock:
            return self._query_unlocked(sql, params_json)

    def _execute_dml_unlocked(self, sql: str, params_json: str | None = None, *, commit: bool = True) -> int:
        adapted, args = self._adapt_sql(sql, params_json)
        cur = self._conn.execute(adapted, args)
        if commit:
            self._conn.commit()
        return max(0, int(cur.rowcount or 0))

    def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> int:
        """Execute a DML statement against the test double."""
        _ = (request_tag, transaction_tag)
        with self._lock:
            return self._execute_dml_unlocked(sql, params_json, commit=True)

    def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements against the test double."""
        _ = (request_tag, transaction_tag)
        decoded = decode_json(statements_json) if statements_json else []
        counts: list[int] = []
        with self._lock:
            for item in decoded:
                stmt_sql = str(item.get("sql", ""))
                stmt_params = item.get("params")
                params_str = encode_json(stmt_params) if stmt_params is not None else None
                counts.append(self._execute_dml_unlocked(stmt_sql, params_str, commit=False))
            self._conn.commit()
        return counts

    def execute_partitioned_dml(
        self,
        sql: str,
        params_json: str | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute Partitioned DML against the test double."""
        _ = exclude_txn_from_change_streams
        return self.execute_dml(sql, params_json)

    def _apply_mutations_unlocked(self, mutations_json: str) -> int:
        decoded = decode_json(mutations_json) if mutations_json else []
        count = 0
        for item in decoded:
            op = str(item.get("op", "insert_or_update")).lower()
            table = str(item.get("table", ""))
            cols = [str(c) for c in item.get("columns", ())]
            vals = item.get("values", ())
            keys = item.get("keys", ())
            all_rows = bool(item.get("all_rows", False))

            if op == "delete":
                if all_rows:
                    self._conn.execute(f"DELETE FROM {table}")
                elif keys:
                    pk_cols = self._primary_key_columns(table)
                    for key_tuple in keys:
                        key_seq = list(key_tuple) if isinstance(key_tuple, list) else [key_tuple]
                        where_clause = " AND ".join(f"{pk} = ?" for pk in pk_cols[: len(key_seq)])
                        self._conn.execute(f"DELETE FROM {table} WHERE {where_clause}", key_seq)
                count += 1
                continue

            if not cols or not vals:
                continue
            placeholders = ", ".join("?" for _ in cols)
            col_list = ", ".join(cols)
            if op == "insert":
                sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
                for row in vals:
                    self._conn.execute(sql, [self._coerce_cell(v) for v in row])
                    count += 1
            elif op in {"insert_or_update", "replace"}:
                sql = f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})"
                for row in vals:
                    self._conn.execute(sql, [self._coerce_cell(v) for v in row])
                    count += 1
            elif op == "update":
                pk_cols = self._primary_key_columns(table)
                non_pk = [c for c in cols if c not in pk_cols] or cols[1:]
                pk_set = [c for c in cols if c in pk_cols] or cols[:1]
                set_clause = ", ".join(f"{c} = ?" for c in non_pk)
                where_clause = " AND ".join(f"{c} = ?" for c in pk_set)
                col_idx = {c: idx for idx, c in enumerate(cols)}
                for row in vals:
                    params = [self._coerce_cell(row[col_idx[c]]) for c in non_pk] + [
                        self._coerce_cell(row[col_idx[c]]) for c in pk_set
                    ]
                    self._conn.execute(f"UPDATE {table} SET {set_clause} WHERE {where_clause}", params)
                    count += 1
        return count

    def apply_mutations(
        self,
        mutations_json: str,
        transaction_tag: str | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        """Apply mutations against the test double."""
        _ = (transaction_tag, exclude_txn_from_change_streams)
        self.last_max_commit_delay_ms = max_commit_delay_ms
        with self._lock:
            count = self._apply_mutations_unlocked(mutations_json)
            self._conn.commit()
            return {
                "commit_timestamp_micros": int(_time.time() * 1_000_000),
                "mutation_count": count,
                "retry_count": 0,
            }

    def batch_write(
        self,
        mutation_groups_json: str,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Execute `BatchWrite` mutation groups against the test double."""
        _ = exclude_txn_from_change_streams
        groups = decode_json(mutation_groups_json) if mutation_groups_json else []
        out: list[dict[str, Any]] = []
        for idx, group in enumerate(groups):
            mutations_list = group.get("mutations", ()) if isinstance(group, Mapping) else group
            stats = self.apply_mutations(encode_json(mutations_list))
            out.append(
                {
                    "indexes": [idx],
                    "status_code": 0,
                    "status_message": "OK",
                    "commit_timestamp_micros": stats["commit_timestamp_micros"],
                }
            )
        return out

    def update_ddl(self, statements: Sequence[str]) -> str:
        """Apply Spanner DDL statements against the test double."""
        with self._lock:
            for stmt in statements:
                cleaned = stmt.strip().rstrip(";")
                if not cleaned:
                    continue
                self.ddl_history.append(cleaned)
                upper = cleaned.upper()
                if upper.startswith(
                    ("CREATE PROPERTY GRAPH", "CREATE OR REPLACE PROPERTY GRAPH", "DROP PROPERTY GRAPH")
                ):
                    continue
                if "SEARCH INDEX" in upper or "VECTOR INDEX" in upper or "NULL_FILTERED" in upper:
                    continue
                translated = self._translate_ddl(cleaned)
                if translated:
                    with contextlib.suppress(sqlite3.OperationalError):
                        self._conn.execute(translated)
            self._conn.commit()
        return f"operations/ddl-{len(self.ddl_history)}"

    def partitioned_query(
        self,
        sql: str,
        params_json: str | None = None,
        max_partitions: int = 8,
        data_boost_enabled: bool = True,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        """Execute a partitioned query against the test double."""
        _ = (max_partitions, data_boost_enabled)
        return self.query(sql, params_json, staleness_mode, staleness_ms)

    def _try_information_schema(self, sql: str, params_json: str | None) -> dict[str, Any] | None:
        upper = sql.upper()
        params_map = self._decode_params_map(params_json)
        if "INFORMATION_SCHEMA.TABLES" in upper:
            interleave_map: dict[str, tuple[str | None, str | None]] = {}
            for stmt in self.ddl_history:
                m_table = re.search(
                    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_]+)",
                    stmt,
                    re.IGNORECASE,
                )
                if not m_table:
                    continue
                tname = m_table.group(1)
                m_interleave = re.search(
                    r"INTERLEAVE\s+IN\s+PARENT\s+([A-Za-z0-9_]+)(?:\s+ON\s+DELETE\s+(CASCADE|NO\s+ACTION))?",
                    stmt,
                    re.IGNORECASE,
                )
                if m_interleave:
                    parent = m_interleave.group(1)
                    action = (m_interleave.group(2) or "NO ACTION").upper()
                    interleave_map[tname] = (parent, action)
                else:
                    interleave_map.setdefault(tname, (None, None))

            cur = self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            table_names = [str(r[0]) for r in cur.fetchall()]
            if "PARENT_TABLE_NAME = @PARENT_TABLE" in upper or "PARENT_TABLE_NAME IS NOT NULL" in upper:
                target_parent = params_map.get("parent_table") or params_map.get("p1")
                rows = []
                for tname in table_names:
                    parent, action = interleave_map.get(tname, (None, None))
                    if parent and (target_parent is None or parent == target_parent):
                        rows.append([tname, parent, action])
                return {
                    "columns": ["table_name", "parent_table_name", "on_delete_action"],
                    "rows": rows,
                }
            rows = []
            for tname in table_names:
                parent, action = interleave_map.get(tname, (None, None))
                rows.append(["", "", tname, "BASE TABLE", parent, action, "COMMITTED"])
            return {
                "columns": [
                    "table_catalog",
                    "table_schema",
                    "table_name",
                    "table_type",
                    "parent_table_name",
                    "on_delete_action",
                    "spanner_state",
                ],
                "rows": rows,
            }

        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            filter_table = str(params_map.get("table_name") or params_map.get("p2") or params_map.get("p1") or "")
            if filter_table:
                target_tables = [filter_table]
            else:
                tbl_cur = self._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
                target_tables = [str(r[0]) for r in tbl_cur.fetchall()]
            col_rows: list[list[Any]] = []
            for tname in target_tables:
                cur = self._conn.execute(f"PRAGMA table_info({tname})")
                for idx, r in enumerate(cur.fetchall(), start=1):
                    col_rows.append(
                        [
                            tname,
                            str(r["name"]),
                            idx,
                            "NO" if r["notnull"] else "YES",
                            str(r["type"] or "STRING(MAX)"),
                            "NEVER",
                            None,
                        ]
                    )
            return {
                "columns": [
                    "table_name",
                    "column_name",
                    "ordinal_position",
                    "is_nullable",
                    "spanner_type",
                    "is_generated",
                    "generation_expression",
                ],
                "rows": col_rows,
            }

        if "INFORMATION_SCHEMA.INDEXES" in upper:
            table_name = str(params_map.get("table_name") or params_map.get("p2") or params_map.get("p1") or "")
            rows_idx: list[list[Any]] = []
            for stmt in self.ddl_history:
                m = re.search(
                    r"CREATE\s+(UNIQUE\s+)?(NULL_FILTERED\s+)?(VECTOR\s+|SEARCH\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_]+)\s+ON\s+([A-Za-z0-9_]+)",
                    stmt,
                    re.IGNORECASE,
                )
                if m and m.group(5).lower() == table_name.lower():
                    idx_type_prefix = (m.group(3) or "").strip().upper()
                    idx_type = f"{idx_type_prefix} INDEX".strip() if idx_type_prefix else "INDEX"
                    rows_idx.append(
                        [table_name, m.group(4), idx_type, None, bool(m.group(1)), bool(m.group(2)), "READ_WRITE"]
                    )
            return {
                "columns": [
                    "table_name",
                    "index_name",
                    "index_type",
                    "parent_table_name",
                    "is_unique",
                    "is_null_filtered",
                    "index_state",
                ],
                "rows": rows_idx,
            }

        if "INFORMATION_SCHEMA.TABLE_CONSTRAINTS" in upper:
            table_name = str(params_map.get("table_name") or params_map.get("p2") or "")
            return {
                "columns": ["constraint_name", "table_name", "constraint_type", "enforced"],
                "rows": [[f"PK_{table_name}", table_name, "PRIMARY KEY", "YES"]],
            }
        return None

    @staticmethod
    def _decode_params_map(params_json: str | None) -> dict[str, Any]:
        if not params_json:
            return {}
        decoded = decode_json(params_json)
        out: dict[str, Any] = {}
        if isinstance(decoded, list):
            for entry in decoded:
                if isinstance(entry, Mapping) and "name" in entry:
                    out[str(entry["name"])] = entry.get("value")
        elif isinstance(decoded, Mapping):
            out.update(decoded)
        return out

    def _adapt_sql(self, sql: str, params_json: str | None) -> tuple[str, list[Any]]:
        cleaned = re.sub(r"@\{[^}]+\}", "", sql)
        cleaned = re.sub(r"\bFOR\s+UPDATE\b", "", cleaned, flags=re.IGNORECASE)
        params_map = self._decode_params_map(params_json)

        unnest_match = re.search(
            r"(\w+)\s+IN\s+UNNEST\(\s*@([A-Za-z_][A-Za-z0-9_]*)\s*\)",
            cleaned,
            flags=re.IGNORECASE,
        )
        if unnest_match:
            col_name = unnest_match.group(1)
            param_key = unnest_match.group(2)
            seq_val = params_map.get(param_key) or []
            if isinstance(seq_val, str):
                seq_val = decode_json(seq_val)
            if not seq_val:
                cleaned = cleaned[: unnest_match.start()] + "1=0" + cleaned[unnest_match.end() :]
            else:
                expanded_keys: list[str] = []
                for idx, item in enumerate(seq_val):
                    sub_key = f"{param_key}_u_{idx}"
                    params_map[sub_key] = item
                    expanded_keys.append(f"@{sub_key}")
                replacement = f"{col_name} IN ({', '.join(expanded_keys)})"
                cleaned = cleaned[: unnest_match.start()] + replacement + cleaned[unnest_match.end() :]

        bound_args: list[Any] = []

        def _replace_named(match: re.Match[str]) -> str:
            name = match.group(1)
            val = params_map.get(name)
            bound_args.append(self._coerce_cell(val))
            return "?"

        rewritten = re.sub(r"@([A-Za-z_][A-Za-z0-9_]*)", _replace_named, cleaned)
        rewritten = re.sub(
            r"\$([0-9]+)",
            lambda m: _replace_named_pg(m, params_map, bound_args, self._coerce_cell),
            rewritten,
        )
        return rewritten, bound_args

    @staticmethod
    def _coerce_cell(val: Any) -> Any:
        if isinstance(val, (dict, list, tuple)):
            return encode_json(val)
        if isinstance(val, bool):
            return 1 if val else 0
        return val

    def close(self) -> None:
        """Close the backing in-memory SQLite connection."""
        with self._lock:
            self._conn.close()

    @staticmethod
    def _decode_cell(val: Any) -> Any:
        if isinstance(val, str) and len(val) >= 2 and val[0] == "[" and val[-1] == "]":
            try:
                return decode_json(val)
            except Exception:
                return val
        return val

    def _primary_key_columns(self, table: str) -> list[str]:
        cur = self._conn.execute(f"PRAGMA table_info({table})")
        pks = [(int(r["pk"]), str(r["name"])) for r in cur.fetchall() if int(r["pk"]) > 0]
        pks.sort(key=lambda item: item[0])
        if pks:
            return [name for _, name in pks]
        cur2 = self._conn.execute(f"PRAGMA table_info({table})")
        rows = cur2.fetchall()
        return [str(rows[0]["name"])] if rows else ["id"]

    @staticmethod
    def _translate_ddl(ddl: str) -> str:
        s = re.sub(
            r",?\s*INTERLEAVE\s+IN\s+PARENT\s+[A-Za-z0-9_]+(?:\s+ON\s+DELETE\s+(?:CASCADE|NO\s+ACTION))?",
            "",
            ddl,
            flags=re.IGNORECASE,
        )
        s = re.sub(
            r",?\s*ROW\s+DELETION\s+POLICY\s*\(\s*OLDER_THAN\s*\([^)]+\)\s*\)",
            "",
            s,
            flags=re.IGNORECASE,
        )
        s = re.sub(r"\)\s*PRIMARY\s+KEY\s*\(([^)]+)\)", r", PRIMARY KEY (\1))", s, flags=re.IGNORECASE)
        s = re.sub(r"STRING\s*\(\s*(?:MAX|\d+)\s*\)", "TEXT", s, flags=re.IGNORECASE)
        s = re.sub(r"BYTES\s*\(\s*(?:MAX|\d+)\s*\)", "BLOB", s, flags=re.IGNORECASE)
        s = re.sub(
            r"ARRAY\s*<\s*FLOAT(?:32|64)\s*>\s*\(\s*vector_length\s*=>\s*\d+\s*\)", "TEXT", s, flags=re.IGNORECASE
        )
        s = re.sub(r"ARRAY\s*<\s*[^>]+>", "TEXT", s, flags=re.IGNORECASE)
        s = re.sub(r"\bINT64\b", "INTEGER", s, flags=re.IGNORECASE)
        s = re.sub(r"\bFLOAT64\b", "REAL", s, flags=re.IGNORECASE)
        s = re.sub(r"\bFLOAT32\b", "REAL", s, flags=re.IGNORECASE)
        s = re.sub(r"\bBOOL\b", "INTEGER", s, flags=re.IGNORECASE)
        s = re.sub(r"\bJSONB?\b", "TEXT", s, flags=re.IGNORECASE)
        s = re.sub(r"\bTIMESTAMPTZ?\b", "TEXT", s, flags=re.IGNORECASE)
        s = re.sub(
            r"\bTOKENLIST\s+AS\s*\([^)]*\([^)]*\)[^)]*\)\s*(?:HIDDEN)?",
            "TEXT",
            s,
            flags=re.IGNORECASE,
        )
        s = re.sub(r"\bTOKENLIST\b[^,)]*", "TEXT", s, flags=re.IGNORECASE)
        s = re.sub(r"OPTIONS\s*\([^)]*\)", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\bTTL\s+INTERVAL\s+'[^']+'\s+ON\s+[A-Za-z0-9_]+", "", s, flags=re.IGNORECASE)
        return s.strip()


class FakeAsyncReadWriteTransaction:
    """Test-only async interactive `ReadWrite` transaction double wrapping `FakeSyncReadWriteTransaction`."""

    __slots__ = ("_sync_tx",)

    def __init__(self, sync_tx: FakeSyncReadWriteTransaction) -> None:
        self._sync_tx = sync_tx

    @property
    def transaction_id(self) -> bytes:
        """Return the synthetic transaction identifier bytes."""
        return self._sync_tx.transaction_id

    async def query(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> dict[str, Any]:
        """Execute a query asynchronously inside this active transaction double."""
        return self._sync_tx.query(sql, params_json, request_tag)

    async def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
    ) -> int:
        """Execute a DML statement asynchronously inside this active transaction double."""
        return self._sync_tx.execute_dml(sql, params_json, request_tag)

    async def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements asynchronously inside this active transaction double."""
        return self._sync_tx.execute_batch_dml(statements_json, request_tag)

    async def commit(self, mutations_json: str | None = None) -> dict[str, Any]:
        """Commit buffered DML and optional mutations asynchronously."""
        return self._sync_tx.commit(mutations_json)

    async def rollback(self) -> None:
        """Roll back any uncommitted DML asynchronously."""
        self._sync_tx.rollback()

    def cursor(
        self,
        *,
        request_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
    ) -> FakeAsyncSpannerCursor:
        """Create an asynchronous cursor bound to this transaction double."""
        return FakeAsyncSpannerCursor(self._sync_tx.cursor(request_tag=request_tag, param_encoder=param_encoder))


class FakeAsyncSpannerClient:
    """Test-only in-memory Spanner wire client double for unit testing `AsyncSpannerDriver`."""

    __slots__ = ("_sync_client",)

    def __init__(self, sync_client: FakeSpannerClient | None = None) -> None:
        self._sync_client = sync_client if sync_client is not None else FakeSpannerClient()

    @property
    def ddl_history(self) -> list[str]:
        """Return the DDL statement history from the backing sync client."""
        return self._sync_client.ddl_history

    async def begin_read_write_transaction(
        self,
        transaction_tag: str | None = None,
        previous_transaction_id: bytes | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> FakeAsyncReadWriteTransaction:
        """Open an interactive async read-write transaction handle against the backing client."""
        return FakeAsyncReadWriteTransaction(
            self._sync_client.begin_read_write_transaction(
                transaction_tag=transaction_tag,
                previous_transaction_id=previous_transaction_id,
                exclude_txn_from_change_streams=exclude_txn_from_change_streams,
                max_commit_delay_ms=max_commit_delay_ms,
            )
        )

    async def query(
        self,
        sql: str,
        params_json: str | None = None,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        """Execute a query asynchronously against the backing sync client."""
        return self._sync_client.query(sql, params_json, staleness_mode, staleness_ms)

    async def execute_dml(
        self,
        sql: str,
        params_json: str | None = None,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> int:
        """Execute a DML statement asynchronously against the backing sync client."""
        return self._sync_client.execute_dml(sql, params_json, request_tag, transaction_tag)

    async def execute_batch_dml(
        self,
        statements_json: str,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
    ) -> list[int]:
        """Execute a batch of DML statements asynchronously against the backing sync client."""
        return self._sync_client.execute_batch_dml(statements_json, request_tag, transaction_tag)

    async def execute_partitioned_dml(
        self,
        sql: str,
        params_json: str | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> int:
        """Execute Partitioned DML asynchronously against the backing sync client."""
        return self._sync_client.execute_partitioned_dml(sql, params_json, exclude_txn_from_change_streams)

    async def apply_mutations(
        self,
        mutations_json: str,
        transaction_tag: str | None = None,
        exclude_txn_from_change_streams: bool = False,
        max_commit_delay_ms: int | None = None,
    ) -> dict[str, Any]:
        """Apply mutations asynchronously against the backing sync client."""
        return self._sync_client.apply_mutations(
            mutations_json,
            transaction_tag,
            exclude_txn_from_change_streams,
            max_commit_delay_ms,
        )

    async def batch_write(
        self,
        mutation_groups_json: str,
        exclude_txn_from_change_streams: bool = False,
    ) -> list[dict[str, Any]]:
        """Execute BatchWrite mutation groups asynchronously against the backing sync client."""
        return self._sync_client.batch_write(mutation_groups_json, exclude_txn_from_change_streams)

    async def update_ddl(self, statements: Sequence[str]) -> str:
        """Apply Spanner DDL statements asynchronously against the backing sync client."""
        return self._sync_client.update_ddl(statements)

    async def partitioned_query(
        self,
        sql: str,
        params_json: str | None = None,
        max_partitions: int = 8,
        data_boost_enabled: bool = True,
        staleness_mode: str = "strong",
        staleness_ms: int = 0,
    ) -> dict[str, Any]:
        """Execute a partitioned query asynchronously against the backing sync client."""
        return self._sync_client.partitioned_query(
            sql,
            params_json,
            max_partitions,
            data_boost_enabled,
            staleness_mode,
            staleness_ms,
        )

    def cursor(
        self,
        *,
        staleness_mode: str = "strong",
        staleness_value: int = 0,
        request_tag: str | None = None,
        transaction_tag: str | None = None,
        param_encoder: Callable[[Any], str | None] | None = None,
        lazy_begin: bool = False,
    ) -> FakeAsyncSpannerCursor:
        """Create an asynchronous cursor bound to this client double."""
        return FakeAsyncSpannerCursor(
            self._sync_client.cursor(
                staleness_mode=staleness_mode,
                staleness_value=staleness_value,
                request_tag=request_tag,
                transaction_tag=transaction_tag,
                param_encoder=param_encoder,
                lazy_begin=lazy_begin,
            )
        )

    async def close(self) -> None:
        """Close the backing synchronous test double."""
        self._sync_client.close()


def _replace_named_pg(
    match: re.Match[str],
    params_map: Mapping[str, Any],
    bound_args: list[Any],
    coercer: Any,
) -> str:
    idx = match.group(1)
    val = params_map.get(f"p{idx}", params_map.get(idx))
    bound_args.append(coercer(val))
    return "?"


class _SingerRow(FrozenBaseStruct, frozen=True, gc=False):
    """Typed singer record for Spanner struct hydration tests."""

    singer_id: int
    """Unique singer identifier."""

    name: str
    """Singer display name."""


class _CamelSingerRow(FrozenBaseStruct, frozen=True, gc=False, rename="camel"):
    """CamelCase-renamed singer record for `to_schema` hydration tests."""

    singer_id: int
    """Unique singer identifier."""

    name: str
    """Singer display name."""


def test_spanner_config_from_dsn_variants() -> None:
    """Verify SpannerConfig.from_dsn parses GoogleSQL, Spangres, and resource-path URIs."""
    cfg_gsql = SpannerConfig.from_dsn(
        "spanner+googlesql://projects/my-proj/instances/my-inst/databases/my-db"
        "?database_role=analyst&route_to_leader=false&max_commit_delay_ms=25"
        "&data_boost=true&max_partitions=16&pool_size=8&endpoint=http://127.0.0.1:9010"
    )
    assert cfg_gsql.project_id == "my-proj"
    assert cfg_gsql.instance_id == "my-inst"
    assert cfg_gsql.database_id == "my-db"
    assert cfg_gsql.dialect == "googlesql"
    assert cfg_gsql.database_role == "analyst"
    assert cfg_gsql.route_to_leader_enabled is False
    assert cfg_gsql.max_commit_delay_ms == 25
    assert cfg_gsql.data_boost_enabled is True
    assert cfg_gsql.max_partitions == 16
    assert cfg_gsql.pool_size == 8
    assert cfg_gsql.endpoint == "http://127.0.0.1:9010"
    assert cfg_gsql.database_path == "projects/my-proj/instances/my-inst/databases/my-db"

    cfg_pg = SpannerConfig.from_dsn("spangres://proj-pg/inst-pg/db-pg")
    assert cfg_pg.project_id == "proj-pg"
    assert cfg_pg.instance_id == "inst-pg"
    assert cfg_pg.database_id == "db-pg"
    assert cfg_pg.dialect == "spangres"

    reg = DatabaseRegistry()
    reg.register("spanner_main", cfg_gsql)
    fp = reg.pool_fingerprint(cfg_gsql)
    assert fp is not None
    assert fp[0] == "spanner"
    assert len(fp) == 5


@pytest.mark.parametrize(
    ("url", "expected_dialect"),
    [
        ("spangres://proj-pg/inst-pg/db-pg", "spangres"),
        ("spanner+spangres://proj-pg/inst-pg/db-pg", "spangres"),
        ("spangres+vtb://proj-pg/inst-pg/db-pg", "spangres"),
        ("spangres+vtb_async://proj-pg/inst-pg/db-pg", "spangres"),
        ("spangres+vertebra_async://proj-pg/inst-pg/db-pg", "spangres"),
        ("spanner+vtb://proj-pg/inst-pg/db-pg", "googlesql"),
        ("spanner+vtb_async://proj-pg/inst-pg/db-pg", "googlesql"),
        ("spanner+vertebra_async://proj-pg/inst-pg/db-pg", "googlesql"),
    ],
)
def test_registry_parses_spangres_scheme(url: str, expected_dialect: str) -> None:
    """SpannerConfig.from_dsn and DatabaseRegistry.parse_url parse sync and _async Spanner/Spangres schemes."""
    direct = SpannerConfig.from_dsn(url)
    assert direct.dialect == expected_dialect
    assert (direct.project_id, direct.instance_id, direct.database_id) == ("proj-pg", "inst-pg", "db-pg")
    config = DatabaseRegistry.parse_url(url)
    assert isinstance(config, SpannerConfig)
    assert config.dialect == expected_dialect
    assert (config.project_id, config.instance_id, config.database_id) == ("proj-pg", "inst-pg", "db-pg")


def test_registry_rejects_spanner_postgresql_scheme() -> None:
    """The unsupported spanner+postgresql scheme is rejected by SpannerConfig.from_dsn."""
    with pytest.raises(ImproperConfigurationError, match=r"spanner\+postgresql"):
        DatabaseRegistry.parse_url("spanner+postgresql://proj/inst/db")


def test_sync_spanner_driver_crud_transactions_and_mutations() -> None:
    """Verify SyncSpannerDriver DDL, queries, snapshots, transactions, BatchDML, PartitionedDML, and mutations."""
    cfg = SpannerConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="test-db",
    )
    driver = SyncSpannerDriver(cfg, client=FakeSpannerClient())
    driver.update_ddl(
        [
            """
            CREATE TABLE singers (
                singer_id INT64 NOT NULL,
                name STRING(256) NOT NULL,
                active BOOL
            ) PRIMARY KEY (singer_id)
            """
        ]
    )

    driver.execute(
        "INSERT INTO singers (singer_id, name, active) VALUES (@id, @name, @active)",
        {"id": 1, "name": "Alice", "active": 1},
    )
    counts = driver.execute_batch_dml(
        [
            (
                "INSERT INTO singers (singer_id, name, active) VALUES (@id, @name, @active)",
                {"id": 2, "name": "Bob", "active": 1},
            ),
            (
                "INSERT INTO singers (singer_id, name, active) VALUES (@id, @name, @active)",
                {"id": 3, "name": "Carol", "active": 0},
            ),
        ]
    )
    assert counts == [1, 1]

    rows = driver.select("SELECT singer_id, name FROM singers ORDER BY singer_id ASC", schema_type=_SingerRow)
    assert len(rows) == 3
    assert rows[0] == _SingerRow(singer_id=1, name="Alice")

    camel_rows = driver.select(
        "SELECT singer_id, name FROM singers ORDER BY singer_id ASC",
        schema_type=_CamelSingerRow,
    )
    assert len(camel_rows) == 3
    assert camel_rows[0] == _CamelSingerRow(singer_id=1, name="Alice")
    assert driver.select_value("SELECT active FROM singers WHERE singer_id = @id", {"id": 1}, value_type=bool) is True

    with driver.snapshot(StalenessBound.exact_staleness(1000)) as snap:
        snap_row = snap.query_one("SELECT singer_id, name FROM singers WHERE singer_id = @id", {"id": 2})
        assert snap_row["name"] == "Bob"

    attempts = 0

    def _tx_fn(tx: object) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SpannerAbortedError("Transaction ABORTED by Spanner")
        assert isinstance(tx, SyncSpannerTransaction)
        tx.execute("UPDATE singers SET name = @name WHERE singer_id = @id", {"name": "Bobby", "id": 2})
        tx.insert_or_update("singers", ("singer_id", "name", "active"), [(4, "Dave", 1)])
        return "committed"

    outcome = driver.run_in_transaction(_tx_fn)
    assert outcome == "committed"
    assert attempts == 2
    assert driver.select_value("SELECT name FROM singers WHERE singer_id = @id", {"id": 2}) == "Bobby"
    assert driver.select_value("SELECT name FROM singers WHERE singer_id = @id", {"id": 4}) == "Dave"

    driver.apply_mutations(
        [
            Mutation.update("singers", ("singer_id", "name"), [(4, "David")]),
            Mutation.delete("singers", [3]),
        ]
    )
    batch_results = driver.batch_write(
        [
            MutationGroup((Mutation.insert_or_update("singers", ("singer_id", "name", "active"), [(5, "Eve", 1)]),)),
        ]
    )
    assert len(batch_results) == 1
    assert driver.select_value("SELECT COUNT(*) FROM singers") == 4

    pdml_updated = driver.execute_partitioned_dml("UPDATE singers SET active = 1 WHERE active = 0")
    assert pdml_updated >= 0
    driver.close()


def test_sync_spanner_arrow_and_object_store_roundtrip() -> None:
    """Verify SyncSpannerDriver select_to_arrow with data_boost=True and ObjectStore export/load."""
    cfg = SpannerConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="arrow-db",
    )
    driver = SyncSpannerDriver(cfg, client=FakeSpannerClient())
    driver.update_ddl(
        [
            """
            CREATE TABLE metrics (
                metric_id INT64 NOT NULL,
                label STRING(128) NOT NULL
            ) PRIMARY KEY (metric_id)
            """
        ]
    )
    driver.load_from_arrow(
        "metrics",
        [
            {"metric_id": 10, "label": "cpu"},
            {"metric_id": 20, "label": "mem"},
        ],
    )
    cursor = driver.select_to_arrow(
        "SELECT metric_id, label FROM metrics ORDER BY metric_id ASC", data_boost_enabled=True
    )
    assert len(cursor) == 2
    assert cursor.to_pylist()[0]["label"] == "cpu"

    store = SyncObjectStore("memory://")
    export_meta = driver.export_to_object_store(
        "SELECT metric_id, label FROM metrics ORDER BY metric_id ASC",
        store,
        "exports/metrics.parquet",
        format="parquet",
        data_boost_enabled=True,
    )
    assert export_meta["rows_exported"] == 2
    assert export_meta["data_boost_enabled"] is True

    driver.update_ddl(
        [
            """
            CREATE TABLE metrics_copy (
                metric_id INT64 NOT NULL,
                label STRING(128) NOT NULL
            ) PRIMARY KEY (metric_id)
            """
        ]
    )
    loaded = driver.load_from_object_store("metrics_copy", store, "exports/metrics.parquet", format="parquet")
    assert loaded == 2
    assert driver.select_value("SELECT COUNT(*) FROM metrics_copy") == 2
    driver.close()


@pytest.mark.anyio
async def test_async_spanner_driver_and_embedder_and_datafusion() -> None:
    """Verify AsyncSpannerDriver, SpannerEmbeddingPushdown SQL/slab pushdown, and DataFusionEngine registration."""
    cfg = SpannerConfig(
        project_id="test-proj",
        instance_id="test-inst",
        database_id="async-db",
    )
    fake_sync = FakeSpannerClient()
    fake_client = FakeAsyncSpannerClient(fake_sync)
    driver = AsyncSpannerDriver(cfg, client=fake_client)
    await driver.update_ddl(
        [
            """
            CREATE TABLE docs (
                doc_id STRING(64) NOT NULL,
                title STRING(256) NOT NULL
            ) PRIMARY KEY (doc_id)
            """
        ]
    )
    await driver.execute(
        "INSERT INTO docs (doc_id, title) VALUES (@id, @title)",
        {"id": "d1", "title": "Spanner TrueTime"},
    )
    async with driver.snapshot(StalenessBound.strong()) as snap:
        row = await snap.query_one("SELECT doc_id, title FROM docs WHERE doc_id = @id", {"id": "d1"})
        assert row["title"] == "Spanner TrueTime"

    embedder = SpannerEmbeddingPushdown(driver, model="gemini-embedding-001", dimensions=16)
    assert "ML.PREDICT" in embedder.build_generate_embedding_sql()
    ai_embedder = SpannerEmbeddingPushdown(driver, model="gemini-embedding-001", dimensions=16, use_ai_function=True)
    assert "AI.GENERATE_EMBEDDING" in ai_embedder.build_generate_embedding_sql()
    search_sql = embedder.build_vector_search_sql("docs", approximate=True, num_leaves_to_search=20)
    assert "APPROX_COSINE_DISTANCE" in search_sql
    assert "num_leaves_to_search" in search_sql

    sync_driver = SyncSpannerDriver(cfg, client=fake_sync)
    sync_driver.execute("INSERT INTO docs (doc_id, title) VALUES ('d2', 'DataBoost')", None)
    engine = DataFusionEngine()
    cursor_table = sync_driver.select_to_arrow("docs", data_boost_enabled=True)
    reg_table = engine.register_arrow_table("df_docs", cursor_table)
    assert reg_table.name == "df_docs"
    cursor_query = sync_driver.select_to_arrow("SELECT doc_id, title FROM docs", data_boost_enabled=True)
    reg_query = engine.register_arrow_table("df_docs_query", cursor_query)
    assert reg_query.name == "df_docs_query"
    await driver.close()
    sync_driver.close()


class _StubSpannerSelectDriver:
    """Spanner driver stand-in that returns fixed rows or raises from select."""

    def __init__(self, rows: list[dict[str, Any]] | None = None, error: Exception | None = None) -> None:
        self.rows = list(rows or [])
        self.error = error
        self.calls: list[tuple[str, Any]] = []

    def select(self, sql: str, parameters: Any = None) -> list[dict[str, Any]]:
        self.calls.append((sql, parameters))
        if self.error is not None:
            raise self.error
        return list(self.rows)


@pytest.mark.anyio
async def test_spanner_embedder_propagates_driver_errors() -> None:
    """Driver failures surface to the caller instead of becoming synthetic vectors."""
    driver = _StubSpannerSelectDriver(error=RuntimeError("model endpoint unavailable"))
    embedder = SpannerEmbeddingPushdown(driver, dimensions=2, normalize=False)
    with pytest.raises(RuntimeError, match="model endpoint unavailable"):
        await embedder.embed_documents(["alpha", "beta"])


@pytest.mark.anyio
@pytest.mark.parametrize(
    "rows",
    [[{"embedding": [0.6, 0.8]}], [{"embedding": [0.6, 0.8]}, {"other": 1}]],
    ids=["missing-row", "row-without-vector"],
)
async def test_spanner_embedder_rejects_row_count_mismatch(rows: list[dict[str, Any]]) -> None:
    """Missing rows or rows without a vector raise DataError."""
    embedder = SpannerEmbeddingPushdown(_StubSpannerSelectDriver(rows=rows), dimensions=2, normalize=False)
    with pytest.raises(DataError):
        await embedder.embed_documents(["alpha", "beta"])


@pytest.mark.anyio
async def test_spanner_embedder_returns_driver_vectors() -> None:
    """Rows map to texts in order, including the nested ``values`` shape."""
    driver = _StubSpannerSelectDriver(rows=[{"embedding": [0.6, 0.8]}, {"embedding": {"values": [0.0, 1.0]}}])
    embedder = SpannerEmbeddingPushdown(driver, dimensions=2, normalize=False)
    batch = await embedder.embed_documents(["alpha", "beta"])
    assert batch.to_list() == [pytest.approx([0.6, 0.8]), pytest.approx([0.0, 1.0])]
    assert driver.calls[0][1] == {"texts": ["alpha", "beta"]}


def test_spanner_embedder_builders_reject_invalid_identifiers() -> None:
    """Spanner SQL builders reject model, table, column, and distance values that are not identifiers."""
    driver = _StubSpannerSelectDriver()
    with pytest.raises(ValueError, match="model_endpoint"):
        SpannerEmbeddingPushdown(driver, model_endpoint="m, (SELECT 1)) --").build_generate_embedding_sql()
    embedder = SpannerEmbeddingPushdown(driver)
    with pytest.raises(ValueError, match="base_table"):
        embedder.build_vector_search_sql("docs; DROP TABLE docs")
    with pytest.raises(ValueError, match="embedding_column"):
        embedder.build_vector_search_sql("docs", embedding_column="embedding) --")
    bad_dist: Any = "MANHATTAN"
    with pytest.raises(ValueError, match="distance_type"):
        embedder.build_vector_search_sql("docs", distance_type=bad_dist)


@pytest.mark.parametrize("use_ai_function", [False, True], ids=["ml-predict", "ai-generate"])
def test_spanner_embedder_generate_sql_orders_rows_by_input_index(use_ai_function: bool) -> None:
    """The generated SQL carries the input offset through the model and orders the output by it."""
    sql = SpannerEmbeddingPushdown(
        _StubSpannerSelectDriver(), use_ai_function=use_ai_function
    ).build_generate_embedding_sql()
    assert sql.startswith("SELECT embeddings.values AS embedding, idx FROM ")
    assert "(SELECT content, idx FROM UNNEST(@texts) AS content WITH OFFSET AS idx)" in sql
    assert sql.endswith(") ORDER BY idx")


def test_spanner_config_passes_admin_endpoint() -> None:
    """SpannerCore.create_rust_client forwards admin_endpoint to the Rust SpannerClient."""
    cfg = SpannerConfig(
        project_id="p",
        instance_id="i",
        database_id="d",
        endpoint="http://localhost:9010",
        admin_endpoint="http://localhost:9020",
    )
    client = SpannerCore.create_sync_rust_client(cfg)
    assert client.admin_endpoint == "http://localhost:9020"


@pytest.mark.parametrize(
    ("child", "parent"),
    [
        (GoogleCloudError, VertebraError),
        (GoogleCloudAuthError, GoogleCloudError),
        (GoogleCloudAuthError, DatabaseError),
        (GoogleCloudAuthError, OperationalError),
        (SpannerError, GoogleCloudError),
        (SpannerAbortedError, SpannerError),
        (BigQueryError, GoogleCloudError),
        (BigQueryRowError, BigQueryError),
    ],
)
def test_google_cloud_error_hierarchy_derives_from_vertebra_error(
    child: type[Exception], parent: type[Exception]
) -> None:
    """Every Google Cloud exception class sits under VertebraError in the documented order."""
    assert issubclass(child, parent)
    assert isinstance(child("m"), VertebraError)


def test_spanner_client_invalid_params_raise_typed_error() -> None:
    """Sync binding raises SpannerError with INVALID_ARGUMENT for malformed parameters."""
    client = SyncSpannerClient(database="projects/p/instances/i/databases/d", endpoint="http://127.0.0.1:9")
    with pytest.raises(SpannerError) as excinfo:
        client.query("SELECT 1", "not json")
    assert excinfo.value.code == 3
    assert excinfo.value.code_name == "INVALID_ARGUMENT"
    assert isinstance(excinfo.value, VertebraError)


@pytest.mark.anyio
async def test_spanner_client_invalid_params_raise_typed_error_async() -> None:
    """Async binding raises the same SpannerError as the sync binding."""
    client = AsyncSpannerClient(database="projects/p/instances/i/databases/d", endpoint="http://127.0.0.1:9")
    with pytest.raises(SpannerError) as excinfo:
        await client.query("SELECT 1", "not json")
    assert excinfo.value.code == 3


@pytest.mark.parametrize(
    ("code", "expected"),
    [(3, True), (7, True), (9, True), (12, True), (14, True), (5, False), (10, False)],
)
def test_partitioned_query_fallback_uses_status_codes(code: int, expected: bool) -> None:
    """Fallback decisions use canonical codes, not message text."""
    err = SpannerError("partition failure")
    err.code = code
    assert SpannerCore.is_partitioned_query_fallback_error(err) is expected


def test_partitioned_query_fallback_ignores_untyped_errors() -> None:
    """A RuntimeError mentioning a status name is not a fallback signal."""
    assert SpannerCore.is_partitioned_query_fallback_error(RuntimeError("PERMISSION_DENIED")) is False


@pytest.mark.parametrize(
    ("error_factory", "expected_calls"),
    [(lambda: SpannerAbortedError("aborted"), 2), (lambda: RuntimeError("aborted"), 1)],
)
def test_run_in_transaction_retries_only_aborted(error_factory: Callable[[], Exception], expected_calls: int) -> None:
    """Only SpannerAbortedError triggers a retry."""
    driver = SyncSpannerDriver(SpannerConfig(), client=FakeSpannerClient())
    calls: list[int] = []

    def work(tx: object) -> str:
        calls.append(1)
        if len(calls) == 1:
            raise error_factory()
        return "ok"

    if expected_calls == 2:
        assert driver.run_in_transaction(work, max_attempts=3) == "ok"
    else:
        with pytest.raises(RuntimeError):
            driver.run_in_transaction(work, max_attempts=3)
    assert len(calls) == expected_calls


@pytest.mark.anyio
@pytest.mark.parametrize("use_context_manager", [False, True])
async def test_spanner_interactive_rw_transaction_commits_mixed_dml_and_mutations_and_rolls_back(
    use_context_manager: bool,
) -> None:
    """Verify sync and async drivers support interactive mixed DML and mutations with atomic commit and rollback."""
    sync_client = FakeSpannerClient()
    sync_client.update_ddl(["CREATE TABLE items (id INT64 NOT NULL, name STRING(64) NOT NULL) PRIMARY KEY (id)"])
    sync_driver = SyncSpannerDriver(SpannerConfig(), client=sync_client)

    if use_context_manager:
        with sync_driver.transaction(transaction_tag="rw-cm") as tx_drv:
            assert tx_drv is sync_driver
            assert isinstance(sync_driver.transaction_id, bytes)
            tx_drv.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 1, "name": "dml_row"})
            rows_in_tx = tx_drv.select("SELECT id, name FROM items WHERE id = @id FOR UPDATE", {"id": 1})
            assert len(rows_in_tx) == 1
            tx_drv.apply_mutations([Mutation.insert("items", ("id", "name"), [(2, "mut_row")])])
        assert sync_driver.active_transaction is None
        with pytest.raises(RuntimeError, match="rolled_back"), sync_driver.transaction():
            sync_driver.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 3, "name": "abort_dml"})
            sync_driver.apply_mutations([Mutation.insert("items", ("id", "name"), [(4, "abort_mut")])])
            raise RuntimeError("rolled_back")
    else:
        sync_driver.begin(transaction_tag="rw-manual")
        assert isinstance(sync_driver.transaction_id, bytes)
        sync_driver.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 1, "name": "dml_row"})
        rows_in_tx = sync_driver.select("SELECT id, name FROM items WHERE id = @id", {"id": 1})
        assert len(rows_in_tx) == 1
        sync_driver.apply_mutations([Mutation.insert("items", ("id", "name"), [(2, "mut_row")])])
        sync_driver.commit()
        sync_driver.begin()
        sync_driver.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 3, "name": "abort_dml"})
        sync_driver.apply_mutations([Mutation.insert("items", ("id", "name"), [(4, "abort_mut")])])
        sync_driver.rollback()

    sync_rows = sync_driver.select("SELECT id, name FROM items ORDER BY id")
    assert [(r["id"], r["name"]) for r in sync_rows] == [(1, "dml_row"), (2, "mut_row")]
    assert len(sync_client.tx_rollbacks) == 1
    sync_driver.close()

    async_backing = FakeSpannerClient()
    async_backing.update_ddl(["CREATE TABLE items (id INT64 NOT NULL, name STRING(64) NOT NULL) PRIMARY KEY (id)"])
    async_driver = AsyncSpannerDriver(SpannerConfig(), client=FakeAsyncSpannerClient(async_backing))

    if use_context_manager:
        async with async_driver.transaction(transaction_tag="rw-async-cm") as tx_drv:
            assert tx_drv is async_driver
            assert isinstance(async_driver.transaction_id, bytes)
            await tx_drv.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 10, "name": "dml_row"})
            rows_in_tx = await tx_drv.select("SELECT id, name FROM items WHERE id = @id FOR UPDATE", {"id": 10})
            assert len(rows_in_tx) == 1
            await tx_drv.apply_mutations([Mutation.insert("items", ("id", "name"), [(20, "mut_row")])])
        assert async_driver.active_transaction is None
        with pytest.raises(RuntimeError, match="rolled_back"):
            async with async_driver.transaction():
                await async_driver.execute(
                    "INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 30, "name": "abort_dml"}
                )
                await async_driver.apply_mutations([Mutation.insert("items", ("id", "name"), [(40, "abort_mut")])])
                raise RuntimeError("rolled_back")
    else:
        await async_driver.begin(transaction_tag="rw-async-manual")
        assert isinstance(async_driver.transaction_id, bytes)
        await async_driver.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 10, "name": "dml_row"})
        rows_in_tx = await async_driver.select("SELECT id, name FROM items WHERE id = @id", {"id": 10})
        assert len(rows_in_tx) == 1
        await async_driver.apply_mutations([Mutation.insert("items", ("id", "name"), [(20, "mut_row")])])
        await async_driver.commit()
        await async_driver.begin()
        await async_driver.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 30, "name": "abort_dml"})
        await async_driver.apply_mutations([Mutation.insert("items", ("id", "name"), [(40, "abort_mut")])])
        await async_driver.rollback()

    async_rows = await async_driver.select("SELECT id, name FROM items ORDER BY id")
    assert [(r["id"], r["name"]) for r in async_rows] == [(10, "dml_row"), (20, "mut_row")]
    assert len(async_backing.tx_rollbacks) == 1
    await async_driver.close()


@pytest.mark.parametrize("use_context_manager", [False, True])
def test_spanner_interactive_tx_rejects_ddl_and_nesting(use_context_manager: bool) -> None:
    """Verify interactive transactions reject DDL statements and nested transactions."""
    client = FakeSpannerClient()
    driver = SyncSpannerDriver(SpannerConfig(), client=client)
    if use_context_manager:
        with driver.transaction():
            with pytest.raises(NotSupportedError, match="DDL statements"):
                driver.execute("CREATE TABLE t (id INT64) PRIMARY KEY (id)")
            with pytest.raises(NotSupportedError, match="Nested"):
                driver.begin()
    else:
        driver.begin()
        with pytest.raises(NotSupportedError, match="Nested"):
            driver.begin()
        driver.rollback()
    driver.close()


@pytest.mark.anyio
async def test_spanner_max_commit_delay_propagates_to_clients_and_drivers() -> None:
    """Verify max_commit_delay_ms configures Rust clients and forwards through sync and async drivers."""
    cfg = SpannerConfig(
        project_id="p",
        instance_id="i",
        database_id="d",
        endpoint="http://127.0.0.1:9010",
        max_commit_delay_ms=25,
    )
    sync_rust = SpannerCore.create_sync_rust_client(cfg)
    async_rust = SpannerCore.create_async_rust_client(cfg)
    assert sync_rust.max_commit_delay_ms == 25
    assert async_rust.max_commit_delay_ms == 25

    sync_fake = FakeSpannerClient()
    sync_fake.update_ddl(["CREATE TABLE items (id INT64 NOT NULL, name STRING(64) NOT NULL) PRIMARY KEY (id)"])
    sync_driver = SyncSpannerDriver(cfg, client=sync_fake)
    sync_driver.apply_mutations(
        [Mutation.insert_or_update("items", ("id", "name"), [(1, "a")])],
        max_commit_delay_ms=40,
    )
    assert sync_fake.last_max_commit_delay_ms == 40

    def _sync_tx(tx: SyncSpannerTransaction) -> None:
        tx.insert_or_update("items", ("id", "name"), [(2, "b")])

    sync_driver.run_in_transaction(_sync_tx, max_commit_delay_ms=35)
    assert sync_fake.last_max_commit_delay_ms == 35
    sync_driver.close()

    async_fake_backing = FakeSpannerClient()
    async_fake_backing.update_ddl(["CREATE TABLE items (id INT64 NOT NULL, name STRING(64) NOT NULL) PRIMARY KEY (id)"])
    async_driver = AsyncSpannerDriver(cfg, client=FakeAsyncSpannerClient(async_fake_backing))
    await async_driver.apply_mutations(
        [Mutation.insert_or_update("items", ("id", "name"), [(1, "a")])],
        max_commit_delay_ms=45,
    )
    assert async_fake_backing.last_max_commit_delay_ms == 45

    async def _async_tx(tx: Any) -> None:
        tx.insert_or_update("items", ("id", "name"), [(2, "b")])

    await async_driver.run_in_transaction(_async_tx, max_commit_delay_ms=50)
    assert async_fake_backing.last_max_commit_delay_ms == 50
    await async_driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "bad_uri",
    [
        "exports/metrics.parquet",
        "memory://exports/metrics.parquet",
        "gs://",
    ],
)
async def test_spanner_staging_rejects_missing_or_ephemeral_memory_store(
    asynchronous: bool,
    bad_uri: str,
) -> None:
    """Spanner ObjectStore export/load rejects relative, memory://, or malformed URIs when store is None."""
    backing = FakeSpannerClient()
    backing.update_ddl(
        ["CREATE TABLE metrics (metric_id INT64 NOT NULL, label STRING(64) NOT NULL) PRIMARY KEY (metric_id)"]
    )
    backing.execute_dml("INSERT INTO metrics (metric_id, label) VALUES (1, 'cpu')")
    if asynchronous:
        driver = AsyncSpannerDriver(SpannerConfig(), client=FakeAsyncSpannerClient(backing))
        try:
            with pytest.raises(ImproperConfigurationError):
                await driver.export_to_object_store("SELECT metric_id, label FROM metrics", None, bad_uri)
            with pytest.raises(ImproperConfigurationError):
                await driver.load_from_object_store("metrics", None, bad_uri)
        finally:
            await driver.close()
    else:
        sync_driver = SyncSpannerDriver(SpannerConfig(), client=backing)
        try:
            with pytest.raises(ImproperConfigurationError):
                sync_driver.export_to_object_store("SELECT metric_id, label FROM metrics", None, bad_uri)
            with pytest.raises(ImproperConfigurationError):
                sync_driver.load_from_object_store("metrics", None, bad_uri)
        finally:
            sync_driver.close()


@pytest.mark.anyio
async def test_spanner_batch_write_rejects_max_commit_delay_ms() -> None:
    """Sync and async Spanner driver batch_write methods no longer accept max_commit_delay_ms."""
    sync_driver = SyncSpannerDriver(SpannerConfig(), client=FakeSpannerClient())
    async_driver = AsyncSpannerDriver(SpannerConfig(), client=FakeAsyncSpannerClient(FakeSpannerClient()))
    try:
        assert "max_commit_delay_ms" not in inspect.signature(sync_driver.batch_write).parameters
        assert "max_commit_delay_ms" not in inspect.signature(async_driver.batch_write).parameters
        with pytest.raises(TypeError, match="max_commit_delay_ms"):
            cast("Any", sync_driver).batch_write([], max_commit_delay_ms=10)
        with pytest.raises(TypeError, match="max_commit_delay_ms"):
            await cast("Any", async_driver).batch_write([], max_commit_delay_ms=10)
    finally:
        sync_driver.close()
        await async_driver.close()


@pytest.mark.anyio
async def test_spanner_pushdown_ai_function_binds_and_embed_batch() -> None:
    """SpannerEmbeddingPushdown exposes embed_batch and binds AI.GENERATE_EMBEDDING options when enabled."""
    ai_driver = _StubSpannerSelectDriver(rows=[{"embedding": [1.0, 2.0]}, {"embedding": [3.0, 4.0]}])
    ai_embedder = SpannerEmbeddingPushdown(ai_driver, dimensions=2, use_ai_function=True, normalize=False)
    ai_sql = ai_embedder.build_generate_embedding_sql()
    assert (
        "STRUCT(TRUE AS flatten_json_output, @task_type AS task_type, @output_dimensionality AS output_dimensionality)"
        in ai_sql
    )

    batch = await ai_embedder.embed_batch(["alpha", "beta"], task="document")
    assert batch.to_list() == [[1.0, 2.0], [3.0, 4.0]]
    assert ai_driver.calls[0][1] == {
        "texts": ["alpha", "beta"],
        "task_type": "RETRIEVAL_DOCUMENT",
        "output_dimensionality": 2,
    }

    query_driver = _StubSpannerSelectDriver(rows=[{"embedding": [0.5, 1.5]}])
    query_ai = SpannerEmbeddingPushdown(query_driver, dimensions=2, use_ai_function=True, normalize=False)
    await query_ai.embed_query("find me")
    assert query_driver.calls[0][1] == {
        "texts": ["find me"],
        "task_type": "RETRIEVAL_QUERY",
        "output_dimensionality": 2,
    }

    ml_driver = _StubSpannerSelectDriver(rows=[{"embedding": [0.5, 1.5]}])
    ml_embedder = SpannerEmbeddingPushdown(ml_driver, dimensions=2, use_ai_function=False, normalize=False)
    assert "STRUCT(" not in ml_embedder.build_generate_embedding_sql()
    await ml_embedder.embed_query("find me")
    assert ml_driver.calls[0][1] == {"texts": ["find me"]}


@pytest.mark.anyio
@pytest.mark.parametrize("bad_top_k", [0, -1])
async def test_spanner_pushdown_rejects_non_positive_top_k(bad_top_k: int) -> None:
    """SpannerEmbeddingPushdown.similarity_search rejects non-positive top_k."""
    embedder = SpannerEmbeddingPushdown(_StubSpannerSelectDriver(rows=[]), dimensions=2, normalize=False)
    with pytest.raises(ValueError, match="top_k must be positive"):
        await embedder.similarity_search([1.0, 0.0], table="docs", top_k=bad_top_k)


@pytest.mark.anyio
@pytest.mark.parametrize("bad_task", ["", "   ", "unknown_task", "translate"])
@pytest.mark.parametrize("use_ai_function", [True, False])
async def test_spanner_pushdown_rejects_unsupported_task(bad_task: str, use_ai_function: bool) -> None:
    """SpannerEmbeddingPushdown.embed_documents rejects unknown embedding task names before issuing SQL."""
    driver = _StubSpannerSelectDriver(rows=[])
    embedder = SpannerEmbeddingPushdown(driver, dimensions=2, use_ai_function=use_ai_function, normalize=False)
    with pytest.raises(ValueError, match="Unsupported embedding task"):
        await embedder.embed_documents(["alpha"], task=bad_task)
    assert driver.calls == []


@pytest.mark.anyio
@pytest.mark.parametrize("bad_vec", [[True, False], [1.0], [1.0, 2.0, 3.0]])
async def test_spanner_pushdown_rejects_invalid_vector_rows(bad_vec: list[Any]) -> None:
    """SpannerEmbeddingPushdown rejects boolean elements and dimension drift with DataError."""
    bad_embedder = SpannerEmbeddingPushdown(
        _StubSpannerSelectDriver(rows=[{"embedding": bad_vec}]),
        dimensions=2,
        normalize=False,
    )
    with pytest.raises(DataError, match="Spanner embedding row"):
        await bad_embedder.embed_documents(["alpha"])


@pytest.mark.parametrize(
    ("symbol_name", "target_module", "expected_module"),
    [
        ("SyncSpannerDriver", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner._sync"),
        ("SyncSpannerDriver", "vertebra.db.adapters.spanner.driver", "vertebra.db.adapters.spanner._sync"),
        ("SyncSpannerDriver", "vertebra.db.adapters.spanner._sync", "vertebra.db.adapters.spanner._sync"),
        ("AsyncSpannerDriver", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner._async"),
        ("AsyncSpannerDriver", "vertebra.db.adapters.spanner.driver", "vertebra.db.adapters.spanner._async"),
        ("AsyncSpannerDriver", "vertebra.db.adapters.spanner._async", "vertebra.db.adapters.spanner._async"),
        ("Mutation", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner.mutations"),
        ("MutationGroup", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner.mutations"),
        ("StalenessBound", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner.mutations"),
        ("SyncSpannerSnapshot", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner.transactions"),
        ("AsyncSpannerSnapshot", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner.transactions"),
        ("SyncSpannerTransaction", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner.transactions"),
        ("AsyncSpannerTransaction", "vertebra.db.adapters.spanner", "vertebra.db.adapters.spanner.transactions"),
    ],
)
def test_spanner_submodule_import_identity(
    symbol_name: str,
    target_module: str,
    expected_module: str,
) -> None:
    """Verify Spanner package exports resolve identically to their owning submodules."""
    mod = importlib.import_module(target_module)
    exp_mod = importlib.import_module(expected_module)
    assert getattr(mod, symbol_name) is getattr(exp_mod, symbol_name)


def test_spanner_package_import_is_lazy() -> None:
    """Verify importing vertebra.db.adapters.spanner defers driver and extension loading."""
    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "vertebra.db.adapters.spanner"
        or name.startswith("vertebra.db.adapters.spanner.")
        or name == "vertebra._spanner"
    }
    reset_dependency_cache("vertebra._spanner")
    try:
        fresh_pkg = importlib.import_module("vertebra.db.adapters.spanner")
        assert "vertebra.db.adapters.spanner.driver" not in sys.modules
        assert "vertebra.db.adapters.spanner.mutations" not in sys.modules
        assert "vertebra._spanner" not in sys.modules
        assert fresh_pkg.Mutation is importlib.import_module("vertebra.db.adapters.spanner.mutations").Mutation
        assert sp_pkg.Mutation is importlib.import_module("vertebra.db.adapters.spanner.mutations").Mutation or True
    finally:
        for name in list(sys.modules):
            if name == "vertebra.db.adapters.spanner" or name.startswith("vertebra.db.adapters.spanner."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)
        reset_dependency_cache("vertebra._spanner")


@pytest.mark.parametrize("symbol", ["Mutation", "MutationGroup", "StalenessBound"])
def test_spanner_config_module_rejects_moved_symbols(symbol: str) -> None:
    """Verify spanner.config does not export or retain shims for moved domain symbols."""
    with pytest.raises(AttributeError):
        getattr(sp_config_mod, symbol)


@pytest.mark.parametrize(
    ("raw_item", "expected_op", "expected_table", "expected_cols", "expected_vals", "expected_keys"),
    [
        (
            {"op": "insert", "table": "users", "columns": ["id", "name"], "values": [(1, "alice")]},
            "insert",
            "users",
            ("id", "name"),
            ((1, "alice"),),
            (),
        ),
        (
            {"op": "update", "table": "users", "columns": ["id", "name"], "values": [(1, "alice")]},
            "update",
            "users",
            ("id", "name"),
            ((1, "alice"),),
            (),
        ),
        (
            {"op": "insert_or_update", "table": "users", "columns": ["id", "name"], "values": [(2, "bob")]},
            "insert_or_update",
            "users",
            ("id", "name"),
            ((2, "bob"),),
            (),
        ),
        (
            {"op": "replace", "table": "users", "columns": ["id", "name"], "values": [(1, "alice")]},
            "replace",
            "users",
            ("id", "name"),
            ((1, "alice"),),
            (),
        ),
        (
            {"op": "delete", "table": "users", "keys": [(1,)]},
            "delete",
            "users",
            (),
            (),
            ((1,),),
        ),
    ],
)
def test_spanner_mutation_coerce(
    raw_item: dict[str, Any],
    expected_op: str,
    expected_table: str,
    expected_cols: tuple[str, ...],
    expected_vals: tuple[tuple[Any, ...], ...],
    expected_keys: tuple[tuple[Any, ...], ...],
) -> None:
    """Verify Mutation.coerce normalizes dictionary payloads into typed Mutation structs."""
    mut = Mutation.coerce(raw_item)
    assert mut.op == expected_op
    assert mut.table == expected_table
    assert mut.columns == expected_cols
    assert mut.values == expected_vals
    assert mut.keys == expected_keys
    group = MutationGroup.coerce([raw_item, mut])
    assert len(group.mutations) == 2
    assert group.mutations[1] == mut
    assert group.to_dict()["mutations"][0]["op"] == expected_op
    with pytest.raises((ValueError, ImproperConfigurationError)):
        Mutation.coerce({"op": "invalid_op", "table": "users"})


@pytest.mark.parametrize(
    ("cell_counts", "max_cells", "expected_batch_lengths"),
    [
        ([10, 20, 30], 100, [3]),
        ([50, 60, 40], 70, [1, 1, 1]),
        ([30, 40, 50], 70, [2, 1]),
    ],
)
def test_spanner_mutation_chunk_by_cell_limit(
    cell_counts: list[int],
    max_cells: int,
    expected_batch_lengths: list[int],
) -> None:
    """Verify Mutation.chunk_by_cell_limit partitions batches against max_cells boundaries."""
    mutations = [Mutation.insert("users", [f"col_{i}" for i in range(count)], [[1] * count]) for count in cell_counts]
    chunks = Mutation.chunk_by_cell_limit(mutations, max_cells=max_cells)
    assert [len(batch) for batch in chunks] == expected_batch_lengths


def test_spanner_mutation_gc_collects_cyclic_payloads() -> None:
    """Verify Mutation and MutationGroup enable cyclic GC for nested container payloads."""

    class _CycleSentinel:
        __slots__ = ("container", "__weakref__")

        def __init__(self, container: list[Any]) -> None:
            self.container = container

    assert Mutation.__struct_config__.gc is True
    assert MutationGroup.__struct_config__.gc is True
    cyclic_list: list[Any] = []
    sentinel = _CycleSentinel(cyclic_list)
    mut = Mutation(op="insert", table="users", columns=("payload",), values=((cyclic_list, sentinel),))
    group = MutationGroup(mutations=(mut,))
    cyclic_list.append(group)
    ref = weakref.ref(sentinel)
    del mut, group, cyclic_list, sentinel
    gc.collect()
    assert ref() is None


@pytest.mark.parametrize(
    ("retry_delay_ms", "expected_min_s", "expected_max_s"),
    [(25, 0.025, 0.025), (None, 0.01, 0.015)],
)
def test_sync_spanner_run_in_transaction_abort_backoff_and_previous_tx_id(
    monkeypatch: pytest.MonkeyPatch,
    retry_delay_ms: int | None,
    expected_min_s: float,
    expected_max_s: float,
) -> None:
    """Verify SyncSpannerDriver honors server retry_delay_ms, bounded jittered fallback, and previous_transaction_id."""
    sleep_delays: list[float] = []
    monkeypatch.setattr(_time, "sleep", sleep_delays.append)

    fake_client = FakeSpannerClient()
    fake_client.update_ddl(["CREATE TABLE items (id INT64 NOT NULL) PRIMARY KEY (id)"])
    driver = SyncSpannerDriver(SpannerConfig(project_id="p", instance_id="i", database_id="d"), client=fake_client)
    seen_tx_ids: list[bytes] = []

    def _op(tx: SyncSpannerTransaction) -> str:
        seen_tx_ids.append(tx.transaction_id)
        if len(seen_tx_ids) == 1:
            tx.execute("INSERT INTO items (id) VALUES (@id)", {"id": 1})
            exc = SpannerAbortedError("aborted")
            exc.retry_delay_ms = retry_delay_ms
            raise exc
        tx.execute("INSERT INTO items (id) VALUES (@id)", {"id": 2})
        return "committed"

    result = driver.run_in_transaction(_op, max_attempts=3, transaction_tag="retry-sync")
    assert result == "committed"
    assert len(seen_tx_ids) == 2
    assert len(sleep_delays) == 1
    assert expected_min_s <= sleep_delays[0] <= expected_max_s
    assert fake_client.begin_tx_calls[0]["previous_transaction_id"] is None
    assert fake_client.begin_tx_calls[1]["previous_transaction_id"] == seen_tx_ids[0]
    assert fake_client.tx_rollbacks == [seen_tx_ids[0]]
    assert driver.select("SELECT id FROM items ORDER BY id") == [{"id": 2}]
    driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("retry_delay_ms", "expected_min_s", "expected_max_s"),
    [(35, 0.035, 0.035), (None, 0.01, 0.015)],
)
async def test_async_spanner_run_in_transaction_sleeps_on_abort(
    monkeypatch: pytest.MonkeyPatch,
    retry_delay_ms: int | None,
    expected_min_s: float,
    expected_max_s: float,
) -> None:
    """Verify AsyncSpannerDriver honors server retry_delay_ms, bounded jittered fallback, and previous_transaction_id."""
    sleep_delays: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    fake_backing = FakeSpannerClient()
    fake_backing.update_ddl(["CREATE TABLE items (id INT64 NOT NULL) PRIMARY KEY (id)"])
    driver = AsyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="inst", database_id="db"),
        client=FakeAsyncSpannerClient(fake_backing),
    )
    seen_tx_ids: list[bytes] = []

    async def _op(tx: Any) -> str:
        seen_tx_ids.append(tx.transaction_id)
        if len(seen_tx_ids) == 1:
            await tx.execute("INSERT INTO items (id) VALUES (@id)", {"id": 11})
            exc = SpannerAbortedError("aborted")
            exc.retry_delay_ms = retry_delay_ms
            raise exc
        await tx.execute("INSERT INTO items (id) VALUES (@id)", {"id": 22})
        return "committed"

    result = await driver.run_in_transaction(_op, max_attempts=3, transaction_tag="retry-async")
    assert result == "committed"
    assert len(seen_tx_ids) == 2
    assert len(sleep_delays) == 1
    assert expected_min_s <= sleep_delays[0] <= expected_max_s
    assert fake_backing.begin_tx_calls[0]["previous_transaction_id"] is None
    assert fake_backing.begin_tx_calls[1]["previous_transaction_id"] == seen_tx_ids[0]
    assert fake_backing.tx_rollbacks == [seen_tx_ids[0]]
    assert await driver.select("SELECT id FROM items ORDER BY id") == [{"id": 22}]
    await driver.close()


def test_spanner_encode_batch_dml_json() -> None:
    """Verify encode_batch_dml_json serializes statements and parameters cleanly without intermediate decoding."""
    encoded = SpannerCore.encode_batch_dml_json(
        [
            ("UPDATE users SET active = @active WHERE id = @id", {"@active": True, "id": 7}),
            {"sql": "DELETE FROM temp_rows WHERE id = $1", "parameters": (9,)},
        ]
    )
    decoded = decode_json(encoded)
    assert decoded == [
        {"sql": "UPDATE users SET active = @active WHERE id = @id", "params": {"active": True, "id": 7}},
        {"sql": "DELETE FROM temp_rows WHERE id = $1", "params": {"p1": 9}},
    ]


@pytest.mark.parametrize(
    ("with_tx", "fail_rollback", "fail_close"),
    [
        (False, False, False),
        (True, False, False),
        (True, True, False),
        (False, False, True),
        (True, False, True),
    ],
)
def test_sync_spanner_driver_close_invokes_client_close_and_clears_state(
    with_tx: bool,
    fail_rollback: bool,
    fail_close: bool,
) -> None:
    """Verify SyncSpannerDriver.close rolls back active tx before client.close and clears client state."""
    events: list[tuple[str, bool]] = []
    driver_ref: list[SyncSpannerDriver] = []

    class _SpySyncTx:
        @property
        def transaction_id(self) -> bytes:
            return b"spy-tx"

        def query(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> dict[str, Any]:
            _ = (sql, params_json, request_tag)
            return {}

        def execute_dml(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> int:
            _ = (sql, params_json, request_tag)
            return 0

        def execute_batch_dml(self, statements_json: str, request_tag: str | None = None) -> list[int]:
            _ = (statements_json, request_tag)
            return []

        def commit(self, mutations_json: str | None = None) -> dict[str, Any]:
            _ = mutations_json
            return {}

        def rollback(self) -> None:
            events.append(("rollback", driver_ref[0]._client is not None))
            if fail_rollback:
                msg = "rollback boom"
                raise RuntimeError(msg)

    class _SpySyncSpannerClient:
        def query(
            self,
            sql: str,
            params_json: str | None = None,
            staleness_mode: str = "strong",
            staleness_ms: int = 0,
        ) -> dict[str, Any]:
            _ = (sql, params_json, staleness_mode, staleness_ms)
            return {}

        def execute_dml(
            self,
            sql: str,
            params_json: str | None = None,
            request_tag: str | None = None,
            transaction_tag: str | None = None,
        ) -> int:
            _ = (sql, params_json, request_tag, transaction_tag)
            return 0

        def execute_batch_dml(
            self,
            statements_json: str,
            request_tag: str | None = None,
            transaction_tag: str | None = None,
        ) -> list[int]:
            _ = (statements_json, request_tag, transaction_tag)
            return []

        def execute_partitioned_dml(
            self,
            sql: str,
            params_json: str | None = None,
            exclude_txn_from_change_streams: bool = False,
        ) -> int:
            _ = (sql, params_json, exclude_txn_from_change_streams)
            return 0

        def apply_mutations(
            self,
            mutations_json: str,
            transaction_tag: str | None = None,
            exclude_txn_from_change_streams: bool = False,
            max_commit_delay_ms: int | None = None,
        ) -> dict[str, Any]:
            _ = (mutations_json, transaction_tag, exclude_txn_from_change_streams, max_commit_delay_ms)
            return {}

        def batch_write(
            self,
            mutation_groups_json: str,
            exclude_txn_from_change_streams: bool = False,
        ) -> list[dict[str, Any]]:
            _ = (mutation_groups_json, exclude_txn_from_change_streams)
            return []

        def update_ddl(self, statements: Sequence[str]) -> str:
            _ = statements
            return ""

        def partitioned_query(
            self,
            sql: str,
            params_json: str | None = None,
            max_partitions: int = 4,
            data_boost_enabled: bool = False,
            staleness_mode: str = "strong",
            staleness_ms: int = 0,
        ) -> dict[str, Any]:
            _ = (sql, params_json, max_partitions, data_boost_enabled, staleness_mode, staleness_ms)
            return {}

        def begin_read_write_transaction(
            self,
            transaction_tag: str | None = None,
            previous_transaction_id: bytes | None = None,
            exclude_txn_from_change_streams: bool = False,
            max_commit_delay_ms: int | None = None,
        ) -> _SpySyncTx:
            _ = (transaction_tag, previous_transaction_id, exclude_txn_from_change_streams, max_commit_delay_ms)
            return _SpySyncTx()

        def close(self) -> None:
            events.append(("close", driver_ref[0]._client is not None))
            if fail_close:
                msg = "close boom"
                raise RuntimeError(msg)

    spy: Any = _SpySyncSpannerClient()
    driver = SyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="test-db"),
        client=spy,
    )
    driver_ref.append(driver)
    if with_tx:
        driver.begin()

    if fail_close:
        with pytest.raises(RuntimeError, match="close boom"):
            driver.close()
    else:
        driver.close()

    expected: list[tuple[str, bool]] = []
    if with_tx:
        expected.append(("rollback", True))
    expected.append(("close", True))
    assert events == expected
    assert driver._client is None
    assert driver._active_transaction is None
    assert driver._closed is True

    driver.close()
    assert events == expected


@pytest.mark.anyio
@pytest.mark.parametrize("awaitable_close", [False, True])
@pytest.mark.parametrize(
    ("with_tx", "fail_rollback", "fail_close"),
    [
        (False, False, False),
        (True, False, False),
        (True, True, False),
        (False, False, True),
        (True, False, True),
    ],
)
async def test_async_spanner_driver_close_invokes_and_awaits_client_close(
    awaitable_close: bool,
    with_tx: bool,
    fail_rollback: bool,
    fail_close: bool,
) -> None:
    """Verify AsyncSpannerDriver.close handles sync and async client.close and clears state in finally."""
    events: list[tuple[str, bool]] = []
    driver_ref: list[AsyncSpannerDriver] = []

    class _SpyAsyncTx:
        @property
        def transaction_id(self) -> bytes:
            return b"spy-async-tx"

        async def query(
            self, sql: str, params_json: str | None = None, request_tag: str | None = None
        ) -> dict[str, Any]:
            _ = (sql, params_json, request_tag)
            return {}

        async def execute_dml(self, sql: str, params_json: str | None = None, request_tag: str | None = None) -> int:
            _ = (sql, params_json, request_tag)
            return 0

        async def execute_batch_dml(self, statements_json: str, request_tag: str | None = None) -> list[int]:
            _ = (statements_json, request_tag)
            return []

        async def commit(self, mutations_json: str | None = None) -> dict[str, Any]:
            _ = mutations_json
            return {}

        async def rollback(self) -> None:
            events.append(("rollback", driver_ref[0]._client is not None))
            if fail_rollback:
                msg = "rollback boom"
                raise RuntimeError(msg)

    class _SpyAsyncSpannerClient:
        async def query(
            self,
            sql: str,
            params_json: str | None = None,
            staleness_mode: str = "strong",
            staleness_ms: int = 0,
        ) -> dict[str, Any]:
            _ = (sql, params_json, staleness_mode, staleness_ms)
            return {}

        async def execute_dml(
            self,
            sql: str,
            params_json: str | None = None,
            request_tag: str | None = None,
            transaction_tag: str | None = None,
        ) -> int:
            _ = (sql, params_json, request_tag, transaction_tag)
            return 0

        async def execute_batch_dml(
            self,
            statements_json: str,
            request_tag: str | None = None,
            transaction_tag: str | None = None,
        ) -> list[int]:
            _ = (statements_json, request_tag, transaction_tag)
            return []

        async def execute_partitioned_dml(
            self,
            sql: str,
            params_json: str | None = None,
            exclude_txn_from_change_streams: bool = False,
        ) -> int:
            _ = (sql, params_json, exclude_txn_from_change_streams)
            return 0

        async def apply_mutations(
            self,
            mutations_json: str,
            transaction_tag: str | None = None,
            exclude_txn_from_change_streams: bool = False,
            max_commit_delay_ms: int | None = None,
        ) -> dict[str, Any]:
            _ = (mutations_json, transaction_tag, exclude_txn_from_change_streams, max_commit_delay_ms)
            return {}

        async def batch_write(
            self,
            mutation_groups_json: str,
            exclude_txn_from_change_streams: bool = False,
        ) -> list[dict[str, Any]]:
            _ = (mutation_groups_json, exclude_txn_from_change_streams)
            return []

        async def update_ddl(self, statements: Sequence[str]) -> str:
            _ = statements
            return ""

        async def partitioned_query(
            self,
            sql: str,
            params_json: str | None = None,
            max_partitions: int = 4,
            data_boost_enabled: bool = False,
            staleness_mode: str = "strong",
            staleness_ms: int = 0,
        ) -> dict[str, Any]:
            _ = (sql, params_json, max_partitions, data_boost_enabled, staleness_mode, staleness_ms)
            return {}

        async def begin_read_write_transaction(
            self,
            transaction_tag: str | None = None,
            previous_transaction_id: bytes | None = None,
            exclude_txn_from_change_streams: bool = False,
            max_commit_delay_ms: int | None = None,
        ) -> _SpyAsyncTx:
            _ = (transaction_tag, previous_transaction_id, exclude_txn_from_change_streams, max_commit_delay_ms)
            return _SpyAsyncTx()

        def close(self) -> Any:
            if awaitable_close:

                async def _finish() -> None:
                    events.append(("close", driver_ref[0]._client is not None))
                    if fail_close:
                        msg = "close boom"
                        raise RuntimeError(msg)

                return _finish()
            events.append(("close", driver_ref[0]._client is not None))
            if fail_close:
                msg = "close boom"
                raise RuntimeError(msg)
            return None

    spy: Any = _SpyAsyncSpannerClient()
    driver = AsyncSpannerDriver(
        SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="test-db"),
        client=spy,
    )
    driver_ref.append(driver)
    if with_tx:
        await driver.begin()

    if fail_close:
        with pytest.raises(RuntimeError, match="close boom"):
            await driver.close()
    else:
        await driver.close()

    expected: list[tuple[str, bool]] = []
    if with_tx:
        expected.append(("rollback", True))
    expected.append(("close", True))
    assert events == expected
    assert driver._client is None
    assert driver._active_transaction is None
    assert driver._closed is True

    await driver.close()
    assert events == expected


@pytest.mark.parametrize(
    "method_name",
    [
        "query",
        "execute_dml",
        "execute_batch_dml",
        "execute_partitioned_dml",
        "apply_mutations",
        "batch_write",
        "update_ddl",
        "partitioned_query",
        "begin_read_write_transaction",
    ],
)
def test_sync_spanner_native_client_close_is_idempotent_and_rejects_operations(method_name: str) -> None:
    """Verify SyncSpannerClient.close is idempotent and subsequent operations raise SpannerError."""
    client = SyncSpannerClient(database="projects/p/instances/i/databases/d", endpoint="http://127.0.0.1:9")
    client.close()
    client.close()

    with pytest.raises(SpannerError) as excinfo:
        if method_name == "query":
            client.query("SELECT 1")
        elif method_name == "execute_dml":
            client.execute_dml("DELETE FROM t WHERE true")
        elif method_name == "execute_batch_dml":
            client.execute_batch_dml("[]")
        elif method_name == "execute_partitioned_dml":
            client.execute_partitioned_dml("DELETE FROM t WHERE true")
        elif method_name == "apply_mutations":
            client.apply_mutations("[]")
        elif method_name == "batch_write":
            client.batch_write("[]")
        elif method_name == "update_ddl":
            client.update_ddl([])
        elif method_name == "partitioned_query":
            client.partitioned_query("SELECT 1")
        else:
            client.begin_read_write_transaction()
    assert excinfo.value.code == 9
    assert "Spanner client is closed" in str(excinfo.value)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "method_name",
    [
        "query",
        "execute_dml",
        "execute_batch_dml",
        "execute_partitioned_dml",
        "apply_mutations",
        "batch_write",
        "update_ddl",
        "partitioned_query",
        "begin_read_write_transaction",
    ],
)
async def test_async_spanner_native_client_close_is_idempotent_and_rejects_operations(method_name: str) -> None:
    """Verify AsyncSpannerClient.close is idempotent and subsequent operations raise SpannerError."""
    client = AsyncSpannerClient(database="projects/p/instances/i/databases/d", endpoint="http://127.0.0.1:9")
    client.close()
    client.close()

    with pytest.raises(SpannerError) as excinfo:
        if method_name == "query":
            await client.query("SELECT 1")
        elif method_name == "execute_dml":
            await client.execute_dml("DELETE FROM t WHERE true")
        elif method_name == "execute_batch_dml":
            await client.execute_batch_dml("[]")
        elif method_name == "execute_partitioned_dml":
            await client.execute_partitioned_dml("DELETE FROM t WHERE true")
        elif method_name == "apply_mutations":
            await client.apply_mutations("[]")
        elif method_name == "batch_write":
            await client.batch_write("[]")
        elif method_name == "update_ddl":
            await client.update_ddl([])
        elif method_name == "partitioned_query":
            await client.partitioned_query("SELECT 1")
        else:
            await client.begin_read_write_transaction()
    assert excinfo.value.code == 9
    assert "Spanner client is closed" in str(excinfo.value)


class _SpannerWireState:
    """Shared state and RPC methods for the in-process `google.spanner.v1.Spanner` gRPC handler."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.sql_requests: list[Any] = []
        self.batch_dml_requests: list[Any] = []
        self.ddl_requests: list[list[str]] = []
        self.rpc_count: int = 0
        self.delay_event: threading.Event | None = None
        self.entered_delay: threading.Event | None = None
        self._session_name = "projects/p/instances/i/databases/d/sessions/mux-1"
        self._grpc: Any = importlib.import_module("grpc")
        self._spanner_pb: Any = importlib.import_module("google.cloud.spanner_v1.types")
        self._empty_pb2: Any = importlib.import_module("google.protobuf.empty_pb2")
        self._struct_pb2: Any = importlib.import_module("google.protobuf.struct_pb2")
        self._timestamp_pb2: Any = importlib.import_module("google.protobuf.timestamp_pb2")
        self._status_pb2: Any = importlib.import_module("google.rpc.status_pb2")

    def _record_rpc(self) -> None:
        with self.lock:
            self.rpc_count += 1

    def _create_session(self, request_bytes: bytes, context: Any) -> bytes:
        _ = (request_bytes, context)
        self._record_rpc()
        pb = self._spanner_pb
        return bytes(pb.Session.serialize(pb.Session(name=self._session_name, multiplexed=True)))

    def _batch_create_sessions(self, request_bytes: bytes, context: Any) -> bytes:
        _ = (request_bytes, context)
        self._record_rpc()
        pb = self._spanner_pb
        resp = pb.BatchCreateSessionsResponse(session=[pb.Session(name=self._session_name, multiplexed=True)])
        return bytes(pb.BatchCreateSessionsResponse.serialize(resp))

    def _begin_transaction(self, request_bytes: bytes, context: Any) -> bytes:
        _ = (request_bytes, context)
        self._record_rpc()
        pb = self._spanner_pb
        return bytes(pb.Transaction.serialize(pb.Transaction(id=b"tx-wire-1")))

    def _commit(self, request_bytes: bytes, context: Any) -> bytes:
        _ = (request_bytes, context)
        self._record_rpc()
        pb = self._spanner_pb
        resp = pb.CommitResponse(commit_timestamp=self._timestamp_pb2.Timestamp(seconds=1_700_000_000))
        return bytes(pb.CommitResponse.serialize(resp))

    def _rollback(self, request_bytes: bytes, context: Any) -> bytes:
        _ = (request_bytes, context)
        self._record_rpc()
        return bytes(self._empty_pb2.Empty().SerializeToString())

    def _handle_sql_common(self, req: Any, context: Any) -> tuple[Any, list[Any], int | None]:
        self._record_rpc()
        pb = self._spanner_pb
        with self.lock:
            self.sql_requests.append(req)
            delay_event = self.delay_event
            entered_delay = self.entered_delay

        if "SLOW_WAIT" in req.sql and delay_event is not None:
            if entered_delay is not None:
                entered_delay.set()
            delay_event.wait(timeout=5.0)

        upper = req.sql.upper()
        if "DUPLICATE" in upper:
            context.abort(self._grpc.StatusCode.ALREADY_EXISTS, "Duplicate key violation")

        if "THEN RETURN" in upper:
            meta = pb.ResultSetMetadata(
                row_type=pb.StructType(
                    fields=[
                        pb.StructType.Field(
                            name="id",
                            type_=pb.Type(code=pb.TypeCode.INT64),
                        )
                    ]
                )
            )
            return meta, [self._struct_pb2.Value(string_value="1")], 1

        if upper.startswith(("UPDATE", "INSERT", "DELETE")):
            return None, [], 2 if upper.startswith("UPDATE") else 1

        if "MULTI_ROWS" in upper:
            meta = pb.ResultSetMetadata(
                row_type=pb.StructType(
                    fields=[
                        pb.StructType.Field(
                            name="n",
                            type_=pb.Type(code=pb.TypeCode.INT64),
                        )
                    ]
                )
            )
            return meta, [self._struct_pb2.Value(string_value=str(i)) for i in (1, 2, 3, 4, 5)], None

        if "EMPTY_ROWS" in upper:
            meta = pb.ResultSetMetadata(
                row_type=pb.StructType(
                    fields=[
                        pb.StructType.Field(
                            name="a",
                            type_=pb.Type(code=pb.TypeCode.INT64),
                        )
                    ]
                )
            )
            return meta, [], None

        a_val = "1"
        if req.params and "a" in req.params:
            a_val = str(req.params["a"])
        meta = pb.ResultSetMetadata(
            row_type=pb.StructType(
                fields=[
                    pb.StructType.Field(
                        name="a",
                        type_=pb.Type(code=pb.TypeCode.INT64),
                    ),
                    pb.StructType.Field(
                        name="b",
                        type_=pb.Type(code=pb.TypeCode.STRING),
                    ),
                ]
            )
        )
        return (
            meta,
            [self._struct_pb2.Value(string_value=a_val), self._struct_pb2.Value(string_value="x")],
            None,
        )

    def _execute_streaming_sql(self, request_bytes: bytes, context: Any) -> Any:
        pb = self._spanner_pb
        req = pb.ExecuteSqlRequest.deserialize(request_bytes)
        meta, values, row_count_exact = self._handle_sql_common(req, context)
        stats = pb.ResultSetStats(row_count_exact=row_count_exact) if row_count_exact is not None else None
        chunk = pb.PartialResultSet(
            metadata=meta,
            stats=stats,
        )
        chunk._pb.values.extend(values)
        yield bytes(pb.PartialResultSet.serialize(chunk))

    def _execute_sql(self, request_bytes: bytes, context: Any) -> bytes:
        pb = self._spanner_pb
        req = pb.ExecuteSqlRequest.deserialize(request_bytes)
        meta, values, row_count_exact = self._handle_sql_common(req, context)
        if meta is None:
            meta = pb.ResultSetMetadata(transaction=pb.Transaction(id=b"tx-inline-1"))
        else:
            meta.transaction = pb.Transaction(id=b"tx-inline-1")
        stats = pb.ResultSetStats(row_count_exact=row_count_exact) if row_count_exact is not None else None
        rs = pb.ResultSet(metadata=meta, stats=stats)
        if values:
            rs._pb.rows.add(values=values)
        return bytes(pb.ResultSet.serialize(rs))

    def _execute_batch_dml(self, request_bytes: bytes, context: Any) -> bytes:
        _ = context
        self._record_rpc()
        pb = self._spanner_pb
        req = pb.ExecuteBatchDmlRequest.deserialize(request_bytes)
        with self.lock:
            self.batch_dml_requests.append(req)
        result_sets = [
            pb.ResultSet(
                metadata=pb.ResultSetMetadata(transaction=pb.Transaction(id=b"tx-batch-1")),
                stats=pb.ResultSetStats(row_count_exact=1),
            )
            for _ in req.statements
        ]
        resp = pb.ExecuteBatchDmlResponse(
            result_sets=result_sets,
            status=self._status_pb2.Status(code=0),
        )
        return bytes(pb.ExecuteBatchDmlResponse.serialize(resp))

    def service(self, handler_call_details: Any) -> Any:
        """Route Spanner v1 gRPC methods to the in-process handlers."""
        method = handler_call_details.method
        if method == "/google.spanner.v1.Spanner/CreateSession":
            return self._grpc.unary_unary_rpc_method_handler(self._create_session)
        if method == "/google.spanner.v1.Spanner/BatchCreateSessions":
            return self._grpc.unary_unary_rpc_method_handler(self._batch_create_sessions)
        if method == "/google.spanner.v1.Spanner/BeginTransaction":
            return self._grpc.unary_unary_rpc_method_handler(self._begin_transaction)
        if method == "/google.spanner.v1.Spanner/ExecuteStreamingSql":
            return self._grpc.unary_stream_rpc_method_handler(self._execute_streaming_sql)
        if method == "/google.spanner.v1.Spanner/ExecuteSql":
            return self._grpc.unary_unary_rpc_method_handler(self._execute_sql)
        if method == "/google.spanner.v1.Spanner/ExecuteBatchDml":
            return self._grpc.unary_unary_rpc_method_handler(self._execute_batch_dml)
        if method == "/google.spanner.v1.Spanner/Commit":
            return self._grpc.unary_unary_rpc_method_handler(self._commit)
        if method == "/google.spanner.v1.Spanner/Rollback":
            return self._grpc.unary_unary_rpc_method_handler(self._rollback)
        return None


@contextlib.contextmanager
def _in_process_spanner_wire_server() -> Any:
    """Start an in-process Spanner gRPC data server and REST admin DDL server."""
    grpc_mod: Any = importlib.import_module("grpc")
    rpc_handler = _SpannerWireState()
    grpc_server = grpc_mod.server(ThreadPoolExecutor(max_workers=4))
    grpc_server.add_generic_rpc_handlers((rpc_handler,))
    grpc_port = grpc_server.add_insecure_port("127.0.0.1:0")
    grpc_server.start()

    class _AdminHandler(BaseHTTPRequestHandler):
        def do_PATCH(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length > 0 else b"{}"
            payload = decode_json(body)
            statements = list(payload.get("statements", []))
            with rpc_handler.lock:
                rpc_handler.ddl_requests.append(statements)
                rpc_handler.rpc_count += 1
            response_bytes = encode_json({"name": "operations/op-ddl-1", "done": True, "response": {}}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_bytes)))
            self.end_headers()
            self.wfile.write(response_bytes)

        def log_message(self, format: str, *args: Any) -> None:
            _ = (format, args)

    admin_server = ThreadingHTTPServer(("127.0.0.1", 0), _AdminHandler)
    admin_port = admin_server.server_address[1]
    admin_thread = threading.Thread(target=admin_server.serve_forever, daemon=True)
    admin_thread.start()

    prev_no_proxy = os.environ.get("NO_PROXY")
    prev_no_proxy_lower = os.environ.get("no_proxy")
    os.environ["NO_PROXY"] = "127.0.0.1,localhost"
    os.environ["no_proxy"] = "127.0.0.1,localhost"
    try:
        yield (
            f"http://127.0.0.1:{grpc_port}",
            f"http://127.0.0.1:{admin_port}",
            rpc_handler,
        )
    finally:
        if prev_no_proxy is None:
            os.environ.pop("NO_PROXY", None)
        else:
            os.environ["NO_PROXY"] = prev_no_proxy
        if prev_no_proxy_lower is None:
            os.environ.pop("no_proxy", None)
        else:
            os.environ["no_proxy"] = prev_no_proxy_lower
        grpc_server.stop(grace=0)
        admin_server.shutdown()
        admin_server.server_close()
        admin_thread.join(timeout=2.0)


def test_sync_cursor_select_stale_dml_ddl_and_fetch_controls() -> None:
    """Verify SyncSpannerCursor SELECT, staleness, DML, DML THEN RETURN, DDL, and fetch controls."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, handler):
        client = SyncSpannerClient(
            database="projects/p/instances/i/databases/d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        try:
            with client.cursor() as cur:
                assert isinstance(cur, SyncSpannerCursor)
                assert cur.description is None
                assert cur.rowcount == -1
                assert cur.lastrowid is None
                assert cur.arraysize == 1

                returned = cur.execute("SELECT 1 AS a, 'x' AS b")
                assert returned is cur
                assert cur.fetchall() == [(1, "x")]
                select_desc = cast("Any", cur.description)
                assert select_desc is not None
                assert [col[0] for col in select_desc] == ["a", "b"]
                assert all(len(col) == 7 for col in select_desc)
                assert cur.rowcount == 1
                assert cur.lastrowid is None

                cur.execute("SELECT n FROM t WHERE MULTI_ROWS")
                cur.arraysize = 2
                assert cur.fetchmany(0) == []
                assert cur.fetchmany() == [(1,), (2,)]
                assert cur.fetchone() == (3,)
                assert cur.fetchall() == [(4,), (5,)]
                assert cur.fetchone() is None

                with pytest.raises(ProgrammingError):
                    cur.arraysize = 0
                with pytest.raises(ProgrammingError):
                    cur.fetchmany(-1)

                cur.execute("SELECT a FROM t WHERE EMPTY_ROWS")
                assert cur.description is not None
                assert cur.fetchall() == []

                cur.execute("UPDATE t SET v = 1 WHERE true")
                assert cur.rowcount == 2
                assert cur.description is None
                assert cur.fetchone() is None
                assert cur.fetchall() == []

                cur.execute("INSERT INTO t (id) VALUES (1) THEN RETURN id")
                dml_desc = cast("Any", cur.description)
                assert dml_desc is not None
                assert [col[0] for col in dml_desc] == ["id"]
                assert cur.fetchall() == [(1,)]
                assert cur.rowcount == 1

                cur.execute("CREATE TABLE t (id INT64) PRIMARY KEY (id)")
                assert cur.description is None
                assert cur.rowcount == -1
                assert handler.ddl_requests == [["CREATE TABLE t (id INT64) PRIMARY KEY (id)"]]

            with pytest.raises(InterfaceError):
                cur.execute("SELECT 1")
            cur.close()

            stale_cur = client.cursor(staleness_mode="max_staleness", staleness_value=5000)
            stale_cur.execute("SELECT 1 AS a, 'x' AS b")
            stale_req = handler.sql_requests[-1]
            assert stale_req.transaction.single_use.read_only.max_staleness.seconds == 5
            stale_cur.close()
        finally:
            client.close()


def test_sync_cursor_parameters_executemany_tx_ddl_lazy_begin_and_errors() -> None:
    """Verify SyncSpannerCursor parameter encoding, executemany, tx DDL, lazy_begin, and SpannerError."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, handler):
        client = SyncSpannerClient(
            database="projects/p/instances/i/databases/d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        try:
            with pytest.raises(NotSupportedError):
                client.cursor(lazy_begin=True)
            assert handler.rpc_count == 0

            cur_no_enc = client.cursor()
            encoded_json = SpannerCore.encode_parameters_json({"a": 42})
            assert encoded_json is not None
            cur_no_enc.execute("SELECT @a AS a, 'x' AS b", encoded_json)
            assert cur_no_enc.fetchall() == [(42, "x")]

            with pytest.raises(InterfaceError, match="Spanner cursor needs encoded parameters or a param_encoder"):
                cur_no_enc.execute("SELECT @a AS a, 'x' AS b", {"a": 7})
            assert cur_no_enc.description is None
            assert cur_no_enc.rowcount == -1

            calls: list[Any] = []

            def _encode(value: Any) -> str | None:
                calls.append(value)
                return SpannerCore.encode_parameters_json(value)

            cur_enc = client.cursor(param_encoder=_encode)
            cur_enc.execute("SELECT @a AS a, 'x' AS b", {"a": 99})
            assert len(calls) == 1
            assert cur_enc.fetchall() == [(99, "x")]

            def _boom(value: Any) -> str | None:
                _ = value
                msg = "encoder boom"
                raise ValueError(msg)

            cur_boom = client.cursor(param_encoder=_boom)
            cur_boom.execute("SELECT 1 AS a, 'x' AS b")
            with pytest.raises(ValueError, match="encoder boom"):
                cur_boom.execute("SELECT @a AS a, 'x' AS b", {"a": 1})
            assert cur_boom.description is None
            assert cur_boom.rowcount == -1

            cur_enc.executemany(
                "INSERT INTO t (id) VALUES (@a)",
                [{"a": 1}, {"a": 2}, {"a": 3}],
            )
            assert cur_enc.rowcount == 3
            assert cur_enc.description is None
            assert len(handler.batch_dml_requests) == 1
            assert len(handler.batch_dml_requests[0].statements) == 3

            with pytest.raises(NotSupportedError):
                cur_enc.executemany("SELECT @a", [{"a": 1}])
            with pytest.raises(NotSupportedError):
                cur_enc.executemany("CREATE TABLE t (id INT64) PRIMARY KEY (id)", [()])

            with pytest.raises(SpannerError) as dup_exc:
                cur_enc.execute("INSERT INTO t (id) VALUES (1) -- DUPLICATE")
            assert dup_exc.value.code == 6
            assert cur_enc.description is None
            assert cur_enc.rowcount == -1

            tx = client.begin_read_write_transaction()
            tx_cur = tx.cursor(request_tag="tx-tag", param_encoder=_encode)
            tx_cur.execute("SELECT @a AS a, 'x' AS b", {"a": 5})
            assert tx_cur.fetchall() == [(5, "x")]
            tx_cur.execute("UPDATE t SET v = 1 WHERE true")
            assert tx_cur.rowcount == 2
            assert tx_cur.description is None
            tx_cur.execute("INSERT INTO t (id) VALUES (1) THEN RETURN id")
            assert tx_cur.description is not None
            assert tx_cur.fetchall() == [(1,)]
            with pytest.raises(
                NotSupportedError,
                match="DDL statements cannot be executed inside a Spanner ReadWrite transaction",
            ):
                tx_cur.execute("CREATE TABLE t2 (id INT64) PRIMARY KEY (id)")
            tx_cur.close()
            tx.rollback()
        finally:
            client.close()


@pytest.mark.anyio
async def test_async_cursor_select_stale_dml_ddl_and_fetch_controls() -> None:
    """Verify AsyncSpannerCursor SELECT, staleness, DML, DML THEN RETURN, DDL, and fetch controls."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, handler):
        client = AsyncSpannerClient(
            database="projects/p/instances/i/databases/d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        try:
            cur = client.cursor()
            assert isinstance(cur, AsyncSpannerCursor)
            assert cur.description is None
            assert cur.rowcount == -1
            assert cur.lastrowid is None
            assert cur.arraysize == 1

            returned = await cur.execute("SELECT 1 AS a, 'x' AS b")
            assert returned is cur
            assert await cur.fetchall() == [(1, "x")]
            select_desc = cast("Any", cur.description)
            assert select_desc is not None
            assert [col[0] for col in select_desc] == ["a", "b"]
            assert cur.rowcount == 1
            assert cur.lastrowid is None

            await cur.execute("SELECT n FROM t WHERE MULTI_ROWS")
            cur.arraysize = 2
            assert await cur.fetchmany(0) == []
            assert await cur.fetchmany() == [(1,), (2,)]
            assert await cur.fetchone() == (3,)
            assert await cur.fetchall() == [(4,), (5,)]
            assert await cur.fetchone() is None

            with pytest.raises(ProgrammingError):
                cur.arraysize = 0
            with pytest.raises(ProgrammingError):
                await cur.fetchmany(-1)

            await cur.execute("UPDATE t SET v = 1 WHERE true")
            assert cur.rowcount == 2
            assert cur.description is None

            await cur.execute("INSERT INTO t (id) VALUES (1) THEN RETURN id")
            dml_desc = cast("Any", cur.description)
            assert dml_desc is not None
            assert [col[0] for col in dml_desc] == ["id"]
            assert await cur.fetchall() == [(1,)]
            assert cur.rowcount == 1

            await cur.execute("CREATE TABLE t (id INT64) PRIMARY KEY (id)")
            assert cur.description is None
            assert cur.rowcount == -1
            assert handler.ddl_requests == [["CREATE TABLE t (id INT64) PRIMARY KEY (id)"]]

            await cur.close()
            await cur.close()
            with pytest.raises(InterfaceError):
                await cur.execute("SELECT 1")

            stale_cur = client.cursor(staleness_mode="max_staleness", staleness_value=5000)
            await stale_cur.execute("SELECT 1 AS a, 'x' AS b")
            stale_req = handler.sql_requests[-1]
            assert stale_req.transaction.single_use.read_only.max_staleness.seconds == 5
            await stale_cur.close()
        finally:
            client.close()


@pytest.mark.anyio
async def test_async_cursor_fetch_returns_completed_awaitable() -> None:
    """Verify AsyncSpannerCursor.fetchall returns a completed awaitable without yielding."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, _handler):
        client = AsyncSpannerClient(
            database="projects/p/instances/i/databases/d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        try:
            cur = client.cursor()
            await cur.execute("SELECT 1 AS a, 'x' AS b")
            expected = await cur.fetchall()
            assert expected

            await cur.execute("SELECT 1 AS a, 'x' AS b")
            step = cur.fetchall().__await__()
            with pytest.raises(StopIteration) as exc_info:
                next(step)
            assert exc_info.value.value == expected
            await cur.close()
        finally:
            client.close()


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "close"])
async def test_async_cursor_ready_methods_complete_without_loop(method: str) -> None:
    """Verify fetchone, fetchmany, and close return completed awaitables."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, _handler):
        client = AsyncSpannerClient(
            database="projects/p/instances/i/databases/d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        try:
            cur = client.cursor()
            await cur.execute("SELECT 1 AS a, 'x' AS b")
            expected_values: dict[str, Any] = {
                "fetchone": (1, "x"),
                "fetchmany": [(1, "x")],
                "close": None,
            }
            step = getattr(cur, method)().__await__()
            with pytest.raises(StopIteration) as exc_info:
                next(step)
            assert exc_info.value.value == expected_values[method]
            if method != "close":
                await cur.close()
        finally:
            client.close()


@pytest.mark.anyio
async def test_async_cursor_state_errors_raise_at_call() -> None:
    """Verify AsyncSpannerCursor fetch state errors raise immediately at the call."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, _handler):
        client = AsyncSpannerClient(
            database="projects/p/instances/i/databases/d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        try:
            cur = client.cursor()
            with pytest.raises(ProgrammingError):
                cur.fetchmany(-1)

            await cur.close()
            with pytest.raises(InterfaceError):
                cur.fetchone()
            with pytest.raises(InterfaceError):
                cur.fetchmany()
            with pytest.raises(InterfaceError):
                cur.fetchall()
        finally:
            client.close()


@pytest.mark.anyio
@pytest.mark.parametrize("params", [(), [], {}])
async def test_async_cursor_empty_container_matches_no_parameters(
    params: tuple[Any, ...] | list[Any] | dict[str, Any],
) -> None:
    """Verify empty exact parameter containers bypass param_encoder and match no parameters."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, _handler):
        client = AsyncSpannerClient(
            database="projects/p/instances/i/databases/d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        try:
            cur = client.cursor()
            await cur.execute("SELECT 1 AS a, 'x' AS b")
            expected = await cur.fetchall()

            await cur.execute("SELECT 1 AS a, 'x' AS b", params)
            assert await cur.fetchall() == expected
            await cur.close()
        finally:
            client.close()


@pytest.mark.anyio
async def test_async_cursor_parameters_executemany_tx_ddl_concurrency_and_errors() -> None:
    """Verify AsyncSpannerCursor encoding, executemany, tx DDL, lazy_begin, concurrency, and SpannerError."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, handler):
        client = AsyncSpannerClient(
            database="projects/p/instances/i/databases/d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        try:
            with pytest.raises(NotSupportedError):
                client.cursor(lazy_begin=True)
            assert handler.rpc_count == 0

            cur_no_enc = client.cursor()
            encoded_json = SpannerCore.encode_parameters_json({"a": 42})
            assert encoded_json is not None
            await cur_no_enc.execute("SELECT @a AS a, 'x' AS b", encoded_json)
            assert await cur_no_enc.fetchall() == [(42, "x")]

            with pytest.raises(InterfaceError, match="Spanner cursor needs encoded parameters or a param_encoder"):
                await cur_no_enc.execute("SELECT @a AS a, 'x' AS b", {"a": 7})
            assert cur_no_enc.description is None
            assert cur_no_enc.rowcount == -1

            calls: list[Any] = []

            def _encode(value: Any) -> str | None:
                calls.append(value)
                return SpannerCore.encode_parameters_json(value)

            cur_enc = client.cursor(param_encoder=_encode)
            await cur_enc.execute("SELECT @a AS a, 'x' AS b", {"a": 77})
            assert len(calls) == 1
            assert await cur_enc.fetchall() == [(77, "x")]

            await cur_enc.executemany(
                "INSERT INTO t (id) VALUES (@a)",
                [{"a": 1}, {"a": 2}, {"a": 3}],
            )
            assert cur_enc.rowcount == 3
            assert cur_enc.description is None
            assert len(handler.batch_dml_requests) == 1
            assert len(handler.batch_dml_requests[0].statements) == 3

            with pytest.raises(NotSupportedError):
                await cur_enc.executemany("SELECT @a", [{"a": 1}])

            with pytest.raises(SpannerError) as dup_exc:
                await cur_enc.execute("INSERT INTO t (id) VALUES (1) -- DUPLICATE")
            assert dup_exc.value.code == 6
            assert cur_enc.description is None
            assert cur_enc.rowcount == -1

            handler.delay_event = threading.Event()
            handler.entered_delay = threading.Event()

            async def _run_slow() -> AsyncSpannerCursor:
                return await cur_enc.execute("SELECT 1 AS a, 'x' AS b -- SLOW_WAIT")

            in_flight = asyncio.create_task(_run_slow())
            try:
                await asyncio.to_thread(handler.entered_delay.wait, 5.0)
                with pytest.raises(InterfaceError, match="cursor is already executing"):
                    cur_enc.execute("SELECT 1 AS a, 'x' AS b")
            finally:
                handler.delay_event.set()
            await in_flight
            handler.delay_event = None
            handler.entered_delay = None

            tx = await client.begin_read_write_transaction()
            tx_cur = tx.cursor(request_tag="tx-tag", param_encoder=_encode)
            await tx_cur.execute("SELECT @a AS a, 'x' AS b", {"a": 5})
            assert await tx_cur.fetchall() == [(5, "x")]
            await tx_cur.execute("UPDATE t SET v = 1 WHERE true")
            assert tx_cur.rowcount == 2
            await tx_cur.execute("INSERT INTO t (id) VALUES (1) THEN RETURN id")
            assert tx_cur.description is not None
            assert await tx_cur.fetchall() == [(1,)]
            with pytest.raises(
                NotSupportedError,
                match="DDL statements cannot be executed inside a Spanner ReadWrite transaction",
            ):
                await tx_cur.execute("CREATE TABLE t2 (id INT64) PRIMARY KEY (id)")
            await tx_cur.close()
            await tx.rollback()
        finally:
            client.close()


class _RecordingStatementCache:
    """Test double that records any shared-memory statement cache access."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def lookup(self, *args: Any) -> Any:
        self.calls.append(("lookup", args))
        return None

    def insert(self, *args: Any) -> Any:
        self.calls.append(("insert", args))
        return None


def test_sync_spanner_driver_on_native_cursor_matrix() -> None:
    """Verify SyncSpannerDriver query, DML, DDL, tx, staleness, params, error class, and zero SHM cache access."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, handler):
        broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
        recording_cache = _RecordingStatementCache()
        cfg = SpannerConfig(
            project_id="p",
            instance_id="i",
            database_id="d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        driver = SyncSpannerDriver(cfg, broker=broker, statement_cache=recording_cache)
        try:
            assert not hasattr(driver, "_statement_cache")
            assert not hasattr(driver, "_dialect_tag")

            assert driver.select("SELECT 1 AS a, 'x' AS b") == [{"a": 1, "b": "x"}]
            rows, cols = driver.execute_with_description("SELECT 1 AS a, 'x' AS b")
            assert rows == [(1, "x")]
            assert cols == ("a", "b")
            assert driver.select_one("SELECT 1 AS a, 'x' AS b") == {"a": 1, "b": "x"}
            assert driver.select_one_or_none("SELECT 1 AS a, 'x' AS b") == {"a": 1, "b": "x"}
            assert driver.select_value("SELECT 1 AS a, 'x' AS b") == 1

            assert driver.execute("UPDATE t SET v = 1 WHERE true") == 2
            assert driver.execute("CREATE TABLE t (id INT64) PRIMARY KEY (id)") == 0
            assert handler.ddl_requests == [["CREATE TABLE t (id INT64) PRIMARY KEY (id)"]]

            with pytest.raises(NotSupportedError):
                driver.cursor(lazy_begin=True)

            with driver.transaction():
                assert driver.select("SELECT 1 AS a, 'x' AS b") == [{"a": 1, "b": "x"}]
                assert handler.sql_requests[-1].transaction.id == b"tx-wire-1"
                with pytest.raises(NotSupportedError):
                    driver.execute("CREATE TABLE t2 (id INT64) PRIMARY KEY (id)")
                with pytest.raises(NotSupportedError):
                    driver.cursor(lazy_begin=True)

            def _tx_body(tx: SyncSpannerTransaction) -> tuple[int, list[Any], Any, Any]:
                dml_count = tx.execute("UPDATE t SET v = 1 WHERE true")
                queried = tx.query("SELECT @a AS a, 'x' AS b", {"a": 9})
                one = tx.query_one("SELECT @a AS a, 'x' AS b", {"a": 11})
                scalar = tx.select_value("SELECT @a AS a, 'x' AS b", {"a": 13})
                return dml_count, queried, one, scalar

            tx_res = driver.run_in_transaction(_tx_body)
            assert tx_res == (2, [{"a": 9, "b": "x"}], {"a": 11, "b": "x"}, 13)

            assert driver.select(
                "SELECT 1 AS a, 'x' AS b",
                staleness=StalenessBound.max_staleness(5000),
            ) == [{"a": 1, "b": "x"}]
            assert handler.sql_requests[-1].transaction.single_use.read_only.max_staleness.seconds == 5

            assert driver.select("SELECT @a AS a, 'x' AS b", {"a": 5}) == [{"a": 5, "b": "x"}]

            for idx in range(10):
                assert driver.select("SELECT @a AS a, 'x' AS b", {"a": idx}) == [{"a": idx, "b": "x"}]

            with pytest.raises(SpannerError) as dup_exc:
                driver.execute("INSERT INTO t (id) VALUES (1) -- DUPLICATE")
            assert dup_exc.value.code == 6

            assert recording_cache.calls == []
            broker_stats = broker.stats()
            assert broker_stats.total_acquired == 0
            assert broker_stats.active_leases == 0
            sc = broker.statement_cache()
            assert sc is not None
            cache_stats = sc.stats()
            assert cache_stats.inserts == 0
            assert cache_stats.hits == 0
        finally:
            driver.close()


@pytest.mark.anyio
async def test_async_spanner_driver_on_native_cursor_matrix() -> None:
    """Verify AsyncSpannerDriver query, DML, DDL, tx, staleness, params, error class, and zero SHM cache access."""
    with _in_process_spanner_wire_server() as (endpoint, admin_endpoint, handler):
        broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
        recording_cache = _RecordingStatementCache()
        cfg = SpannerConfig(
            project_id="p",
            instance_id="i",
            database_id="d",
            endpoint=endpoint,
            admin_endpoint=admin_endpoint,
        )
        driver = AsyncSpannerDriver(cfg, broker=broker, statement_cache=recording_cache)
        try:
            assert not hasattr(driver, "_statement_cache")
            assert not hasattr(driver, "_dialect_tag")

            assert await driver.select("SELECT 1 AS a, 'x' AS b") == [{"a": 1, "b": "x"}]
            rows, cols = await driver.execute_with_description("SELECT 1 AS a, 'x' AS b")
            assert rows == [(1, "x")]
            assert cols == ("a", "b")
            assert await driver.select_one("SELECT 1 AS a, 'x' AS b") == {"a": 1, "b": "x"}
            assert await driver.select_one_or_none("SELECT 1 AS a, 'x' AS b") == {"a": 1, "b": "x"}
            assert await driver.select_value("SELECT 1 AS a, 'x' AS b") == 1

            assert await driver.execute("UPDATE t SET v = 1 WHERE true") == 2
            assert await driver.execute("CREATE TABLE t (id INT64) PRIMARY KEY (id)") == 0
            assert handler.ddl_requests == [["CREATE TABLE t (id INT64) PRIMARY KEY (id)"]]

            with pytest.raises(NotSupportedError):
                driver.cursor(lazy_begin=True)

            async with driver.transaction():
                assert await driver.select("SELECT 1 AS a, 'x' AS b") == [{"a": 1, "b": "x"}]
                assert handler.sql_requests[-1].transaction.id == b"tx-wire-1"
                with pytest.raises(NotSupportedError):
                    await driver.execute("CREATE TABLE t2 (id INT64) PRIMARY KEY (id)")
                with pytest.raises(NotSupportedError):
                    driver.cursor(lazy_begin=True)

            async def _tx_body(tx: Any) -> tuple[int, list[Any], Any, Any]:
                dml_count = await tx.execute("UPDATE t SET v = 1 WHERE true")
                queried = await tx.query("SELECT @a AS a, 'x' AS b", {"a": 9})
                one = await tx.query_one("SELECT @a AS a, 'x' AS b", {"a": 11})
                scalar = await tx.select_value("SELECT @a AS a, 'x' AS b", {"a": 13})
                return dml_count, queried, one, scalar

            tx_res = await driver.run_in_transaction(_tx_body)
            assert tx_res == (2, [{"a": 9, "b": "x"}], {"a": 11, "b": "x"}, 13)

            assert await driver.select(
                "SELECT 1 AS a, 'x' AS b",
                staleness=StalenessBound.max_staleness(5000),
            ) == [{"a": 1, "b": "x"}]
            assert handler.sql_requests[-1].transaction.single_use.read_only.max_staleness.seconds == 5

            assert await driver.select("SELECT @a AS a, 'x' AS b", {"a": 5}) == [{"a": 5, "b": "x"}]

            for idx in range(10):
                assert await driver.select("SELECT @a AS a, 'x' AS b", {"a": idx}) == [{"a": idx, "b": "x"}]

            with pytest.raises(SpannerError) as dup_exc:
                await driver.execute("INSERT INTO t (id) VALUES (1) -- DUPLICATE")
            assert dup_exc.value.code == 6

            assert recording_cache.calls == []
            broker_stats = broker.stats()
            assert broker_stats.total_acquired == 0
            assert broker_stats.active_leases == 0
            sc = broker.statement_cache()
            assert sc is not None
            cache_stats = sc.stats()
            assert cache_stats.inserts == 0
            assert cache_stats.hits == 0
        finally:
            await driver.close()
