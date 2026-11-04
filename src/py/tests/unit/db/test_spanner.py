"""Unit tests for Google Cloud Spanner drivers, mutations, Arrow scans, ObjectStore offload, SpannerEmbedder, and DataFusion."""

from __future__ import annotations

import contextlib
import re
import sqlite3
import threading
import time as _time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import pytest

from vertebra._spanner import (
    AsyncSpannerClient,
    SyncSpannerClient,
)
from vertebra._vertebra import (
    BigQueryError,
    BigQueryRowError,
    GoogleCloudAuthError,
    GoogleCloudError,
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
    StalenessBound,
    SyncSpannerDriver,
    SyncSpannerTransaction,
)
from vertebra.embeddings import EmbeddingError, SpannerEmbedder
from vertebra.storage import SyncObjectStore
from vertebra.typing import FrozenBaseStruct, ImproperConfigurationError, VertebraError
from vertebra.utils.serializers import decode_json, encode_json


class FakeSpannerClient:
    """Test-only in-memory Spanner wire client double for unit testing `SyncSpannerDriver` and `AsyncSpannerDriver`."""

    __slots__ = ("_conn", "_lock", "ddl_history")

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.ddl_history: list[str] = []

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
            adapted, args = self._adapt_sql(sql, params_json)
            cur = self._conn.execute(adapted, args)
            self._conn.commit()
            return max(0, int(cur.rowcount or 0))

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
                counts.append(self.execute_dml(stmt_sql, params_str))
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

    def apply_mutations(
        self,
        mutations_json: str,
        transaction_tag: str | None = None,
        exclude_txn_from_change_streams: bool = False,
    ) -> dict[str, Any]:
        """Apply mutations against the test double."""
        _ = (transaction_tag, exclude_txn_from_change_streams)
        decoded = decode_json(mutations_json) if mutations_json else []
        with self._lock:
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
            table_name = str(params_map.get("table_name") or params_map.get("p2") or "")
            cur = self._conn.execute(f"PRAGMA table_info({table_name})")
            rows = []
            for idx, r in enumerate(cur.fetchall(), start=1):
                rows.append(
                    [
                        table_name,
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
                "rows": rows,
            }

        if "INFORMATION_SCHEMA.INDEXES" in upper:
            table_name = str(params_map.get("table_name") or params_map.get("p2") or "")
            rows = []
            for stmt in self.ddl_history:
                m = re.search(
                    r"CREATE\s+(UNIQUE\s+)?(NULL_FILTERED\s+)?(VECTOR\s+|SEARCH\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_]+)\s+ON\s+([A-Za-z0-9_]+)",
                    stmt,
                    re.IGNORECASE,
                )
                if m and m.group(5).lower() == table_name.lower():
                    idx_type_prefix = (m.group(3) or "").strip().upper()
                    idx_type = f"{idx_type_prefix} INDEX".strip() if idx_type_prefix else "INDEX"
                    rows.append(
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
                "rows": rows,
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


class FakeAsyncSpannerClient:
    """Test-only in-memory Spanner wire client double for unit testing `AsyncSpannerDriver`."""

    __slots__ = ("_sync_client",)

    def __init__(self, sync_client: FakeSpannerClient | None = None) -> None:
        self._sync_client = sync_client if sync_client is not None else FakeSpannerClient()

    @property
    def ddl_history(self) -> list[str]:
        """Return the DDL statement history from the backing sync client."""
        return self._sync_client.ddl_history

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
    ) -> dict[str, Any]:
        """Apply mutations asynchronously against the backing sync client."""
        return self._sync_client.apply_mutations(mutations_json, transaction_tag, exclude_txn_from_change_streams)

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
    assert fp[0] == "spanner"
    assert len(fp) == 5


@pytest.mark.parametrize("url", ["spangres://proj-pg/inst-pg/db-pg", "spanner+spangres://proj-pg/inst-pg/db-pg"])
def test_registry_parses_spangres_scheme(url: str) -> None:
    """DatabaseRegistry.parse_url routes Spangres URLs to SpannerConfig with the spangres dialect."""
    config = DatabaseRegistry.parse_url(url)
    assert isinstance(config, SpannerConfig)
    assert config.dialect == "spangres"
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
            MutationGroup([Mutation.insert_or_update("singers", ("singer_id", "name", "active"), [(5, "Eve", 1)])]),
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
    """Verify AsyncSpannerDriver, SpannerEmbedder SQL/slab pushdown, and DataFusionEngine registration."""
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

    embedder = SpannerEmbedder(driver, model="gemini-embedding-001", dimensions=16)
    assert "ML.PREDICT" in embedder.build_generate_embedding_sql()
    ai_embedder = SpannerEmbedder(driver, model="gemini-embedding-001", dimensions=16, use_ai_function=True)
    assert "AI.GENERATE_EMBEDDING" in ai_embedder.build_generate_embedding_sql()
    search_sql = embedder.build_vector_search_sql("docs", approximate=True, num_leaves_to_search=20)
    assert "APPROX_COSINE_DISTANCE" in search_sql
    assert "num_leaves_to_search" in search_sql

    sync_driver = SyncSpannerDriver(cfg, client=fake_sync)
    sync_driver.execute("INSERT INTO docs (doc_id, title) VALUES ('d2', 'DataBoost')", None)
    engine = DataFusionEngine()
    reg_table = engine.register_spanner_table("df_docs", sync_driver, "docs", data_boost_enabled=True)
    assert reg_table.name == "df_docs"
    reg_query = engine.register_spanner_query(
        "df_docs_query",
        sync_driver,
        "SELECT doc_id, title FROM docs",
        data_boost_enabled=True,
    )
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
    embedder = SpannerEmbedder(driver, dimensions=2, normalize=False)
    with pytest.raises(RuntimeError, match="model endpoint unavailable"):
        await embedder.embed_many(["alpha", "beta"])


@pytest.mark.anyio
@pytest.mark.parametrize(
    "rows",
    [[{"embedding": [0.6, 0.8]}], [{"embedding": [0.6, 0.8]}, {"other": 1}]],
    ids=["missing-row", "row-without-vector"],
)
async def test_spanner_embedder_rejects_row_count_mismatch(rows: list[dict[str, Any]]) -> None:
    """Missing rows or rows without a vector raise EmbeddingError."""
    embedder = SpannerEmbedder(_StubSpannerSelectDriver(rows=rows), dimensions=2, normalize=False)
    with pytest.raises(EmbeddingError):
        await embedder.embed_many(["alpha", "beta"])


@pytest.mark.anyio
async def test_spanner_embedder_returns_driver_vectors() -> None:
    """Rows map to texts in order, including the nested ``values`` shape."""
    driver = _StubSpannerSelectDriver(rows=[{"embedding": [0.6, 0.8]}, {"embedding": {"values": [0.0, 1.0]}}])
    embedder = SpannerEmbedder(driver, dimensions=2, normalize=False)
    batch = await embedder.embed_many(["alpha", "beta"])
    assert batch.to_list() == [pytest.approx([0.6, 0.8]), pytest.approx([0.0, 1.0])]
    assert driver.calls[0][1] == {"texts": ["alpha", "beta"]}


def test_spanner_embedder_builders_reject_invalid_identifiers() -> None:
    """Spanner SQL builders reject model, table, column, and distance values that are not identifiers."""
    driver = _StubSpannerSelectDriver()
    with pytest.raises(ValueError, match="model_endpoint"):
        SpannerEmbedder(driver, model_endpoint="m, (SELECT 1)) --").build_generate_embedding_sql()
    embedder = SpannerEmbedder(driver)
    with pytest.raises(ValueError, match="base_table"):
        embedder.build_vector_search_sql("docs; DROP TABLE docs")
    with pytest.raises(ValueError, match="embedding_column"):
        embedder.build_vector_search_sql("docs", embedding_column="embedding) --")
    with pytest.raises(ValueError, match="distance_type"):
        embedder.build_vector_search_sql("docs", distance_type="MANHATTAN")


@pytest.mark.parametrize("use_ai_function", [False, True], ids=["ml-predict", "ai-generate"])
def test_spanner_embedder_generate_sql_orders_rows_by_input_index(use_ai_function: bool) -> None:
    """The generated SQL carries the input offset through the model and orders the output by it."""
    sql = SpannerEmbedder(_StubSpannerSelectDriver(), use_ai_function=use_ai_function).build_generate_embedding_sql()
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
async def test_spanner_commit_rejects_mixed_buffered_dml_and_mutations_atomically() -> None:
    """Verify SyncSpannerDriver.commit and AsyncSpannerDriver.commit reject mixed DML and mutations before issuing RPCs."""
    sync_client = FakeSpannerClient()
    sync_client.update_ddl(["CREATE TABLE items (id INT64 NOT NULL, name STRING(64) NOT NULL) PRIMARY KEY (id)"])
    sync_driver = SyncSpannerDriver(SpannerConfig(), client=sync_client)
    sync_driver.begin()
    sync_driver.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 1, "name": "dml_row"})
    sync_driver.apply_mutations([Mutation.insert("items", ("id", "name"), [(2, "mut_row")])])
    with pytest.raises(NotSupportedError, match="Cannot mix SQL DML and key-value mutations"):
        sync_driver.commit()
    assert sync_driver.select("SELECT id, name FROM items") == []
    sync_driver.close()

    async_backing = FakeSpannerClient()
    async_backing.update_ddl(["CREATE TABLE items (id INT64 NOT NULL, name STRING(64) NOT NULL) PRIMARY KEY (id)"])
    async_driver = AsyncSpannerDriver(SpannerConfig(), client=FakeAsyncSpannerClient(async_backing))
    await async_driver.begin()
    await async_driver.execute("INSERT INTO items (id, name) VALUES (@id, @name)", {"id": 10, "name": "dml_row"})
    await async_driver.apply_mutations([Mutation.insert("items", ("id", "name"), [(20, "mut_row")])])
    with pytest.raises(NotSupportedError, match="Cannot mix SQL DML and key-value mutations"):
        await async_driver.commit()
    assert await async_driver.select("SELECT id, name FROM items") == []
    await async_driver.close()
