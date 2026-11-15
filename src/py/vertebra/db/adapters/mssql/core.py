"""SQL Server (TDS 7.4) `@P1` placeholder rewriter, client factory, and Arrow helpers."""

from __future__ import annotations

import os
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from vertebra.db.arrow import ArrowCursor, _table_to_rows_and_cols
from vertebra.db.driver._common import MSSQL_DIALECT_TAG
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.text import validate_identifier

if TYPE_CHECKING:
    from vertebra._mssql import AsyncMSSQLClient, SyncMSSQLClient
    from vertebra.db.adapters.mssql.config import MSSQLConfig

__all__ = (
    "MSSQL_DIALECT_TAG",
    "MSSQLCore",
    "rewrite_mssql_placeholders",
)


class MSSQLCore:
    """T-SQL placeholder translation, native client construction, and Arrow coercion helpers."""

    @staticmethod
    def require_native_module() -> Any:
        """Import and return `vertebra._mssql`, raising `MissingDependencyError` when absent."""
        return ensure_module(
            "vertebra._mssql",
            package="vertebra-core[mssql]",
            install_package="vertebra-core[mssql]",
            extra="mssql",
        )

    @classmethod
    def create_sync_rust_client(
        cls,
        config: MSSQLConfig,
        *,
        dialer: Any = None,
    ) -> SyncMSSQLClient:
        """Instantiate the native Rust `SyncMSSQLClient` from `config` and optional `dialer`."""
        mssql_mod = cls.require_native_module()
        kwargs = cls._build_rust_client_kwargs(config, dialer=dialer)
        client: SyncMSSQLClient = mssql_mod.SyncMSSQLClient(config.dsn, **kwargs)
        return client

    @classmethod
    def create_async_rust_client(
        cls,
        config: MSSQLConfig,
        *,
        dialer: Any = None,
    ) -> AsyncMSSQLClient:
        """Instantiate the native Rust `AsyncMSSQLClient` from `config` and optional `dialer`."""
        mssql_mod = cls.require_native_module()
        kwargs = cls._build_rust_client_kwargs(config, dialer=dialer)
        client: AsyncMSSQLClient = mssql_mod.AsyncMSSQLClient(config.dsn, **kwargs)
        return client

    @staticmethod
    def prepare_and_lease(broker: Any, sql: str) -> tuple[str, Any]:
        """Compile T-SQL `@P1` placeholders (caching via broker if enabled) and acquire a lease grant."""
        sc_attr = getattr(broker, "statement_cache", None) if broker is not None else None
        sc = sc_attr() if callable(sc_attr) else sc_attr
        if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
            sql_bytes = sql.encode("utf-8")
            try:
                cached = sc.lookup(MSSQL_DIALECT_TAG, sql)
                use_bytes_key = False
            except TypeError:
                cached = sc.lookup(MSSQL_DIALECT_TAG, sql_bytes)
                use_bytes_key = True
            if cached is not None and cached.compiled_sql:
                rewritten = cached.compiled_sql.decode("utf-8")
            else:
                rewritten, count = MSSQLCore.rewrite_placeholders(sql)
                sc.insert(
                    MSSQL_DIALECT_TAG,
                    sql_bytes if use_bytes_key else sql,
                    count,
                    rewritten.encode("utf-8"),
                    b"",
                )
        else:
            rewritten, _ = MSSQLCore.rewrite_placeholders(sql)

        if broker is not None and hasattr(broker, "lease_table") and broker.lease_table is not None:
            return rewritten, broker.lease_table.acquire(os.getpid())
        if broker is not None and hasattr(broker, "acquire"):
            try:
                return rewritten, broker.acquire(os.getpid())
            except TypeError:
                return rewritten, broker.acquire(bind_key="mssql")
        return rewritten, None

    @staticmethod
    def release_broker_lease(broker: Any, grant: Any) -> None:
        """Release a previously acquired SQL Server broker lease grant."""
        if grant is not None and broker is not None:
            if hasattr(broker, "lease_table") and broker.lease_table is not None:
                broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(broker, "release") and hasattr(grant, "slot_index"):
                broker.release(grant.slot_index, grant.generation)

    @classmethod
    def _build_rust_client_kwargs(
        cls,
        config: MSSQLConfig,
        *,
        dialer: Any = None,
    ) -> dict[str, Any]:
        max_overflow = max(0, int(config.pool_max_size) - max(1, int(config.pool_min_size)))
        (
            cloudsql_instance,
            cloudsql_ip_type,
            cloudsql_auth_type,
            ca_pem,
            client_cert_pem,
            client_key_pem,
        ) = cls._resolve_dialer_fields(config, dialer)
        return {
            "host": config.host,
            "port": int(config.port),
            "database": config.database,
            "user": config.user,
            "password": config.password,
            "application_name": config.application_name,
            "encryption": config.encryption,
            "trust_server_certificate": config.trust_server_certificate,
            "ca_pem": ca_pem,
            "client_cert_pem": client_cert_pem,
            "client_key_pem": client_key_pem,
            "server_name": config.server_name,
            "connect_timeout_ms": max(1, int(config.connect_timeout_ms)),
            "query_timeout_ms": max(1, int(config.query_timeout_ms)),
            "pool_size": max(1, int(config.pool_min_size)),
            "max_overflow": max_overflow,
            "statement_cache_capacity": max(0, int(config.statement_cache_size)),
            "read_only_intent": bool(config.read_only_intent),
            "cloudsql_instance": cloudsql_instance,
            "cloudsql_ip_type": cloudsql_ip_type,
            "cloudsql_auth_type": cloudsql_auth_type,
            "cloudsql_proxy_addr": config.cloudsql_proxy_addr,
        }

    @staticmethod
    def coerce_arrow_source(
        data: Any,
        columns: Sequence[str] | None = None,
    ) -> tuple[Any, list[str] | None]:
        """Coerce an `ArrowCursor`, Arrow IPC buffer, SHM segment name, or row sequence for `load_from_arrow`."""
        col_list = [str(c) for c in columns] if columns is not None else None
        if isinstance(data, str):
            return (data if data.strip() else None), col_list
        if isinstance(data, bytes):
            return (data if len(data) > 0 else None), col_list
        if isinstance(data, (bytearray, memoryview)):
            raw = bytes(data)
            return (raw if len(raw) > 0 else None), col_list
        if isinstance(data, ArrowCursor):
            for col in data._columns:
                validate_identifier(str(col), "column", allow_dots=False)
            if data._shm_cursor is not None:
                return data._shm_cursor, col_list
            view = data.buffer_view()
            if len(view) > 0:
                return bytes(view), col_list
            if not data._rows:
                return None, col_list
            cur = ArrowCursor.from_rows(
                data._rows,
                col_list or data._columns,
                threshold_rows=10_000_000,
            )
            return bytes(cur.buffer_view()), col_list
        if hasattr(data, "shm_name") or hasattr(data, "to_ipc_bytes"):
            return data, col_list

        rows, inferred_cols = _table_to_rows_and_cols(data, columns)
        for col in inferred_cols:
            validate_identifier(str(col), "column", allow_dots=False)
        if not rows or not inferred_cols:
            return None, col_list
        cur = ArrowCursor.from_rows(
            rows,
            inferred_cols,
            threshold_rows=10_000_000,
        )
        effective_cols = col_list if col_list is not None else [str(c) for c in inferred_cols]
        return bytes(cur.buffer_view()), effective_cols

    @staticmethod
    def rewrite_placeholders(sql: str) -> tuple[str, int]:
        """Rewrite `?` and `$1`/`$2` placeholders to T-SQL `@P1, @P2, ...` outside quotes, brackets, and comments."""
        mssql_mod = import_optional("vertebra._mssql")
        if mssql_mod is not None and hasattr(mssql_mod, "rewrite_mssql_placeholders"):
            rewritten, count = mssql_mod.rewrite_mssql_placeholders(sql)
            return str(rewritten), int(count)

        native = import_optional("vertebra._vertebra")
        if native is not None and hasattr(native, "rewrite_mssql_placeholders"):
            rewritten, count = native.rewrite_mssql_placeholders(sql)
            return str(rewritten), int(count)

        out: list[str] = []
        state = "NORMAL"
        i = 0
        n = len(sql)
        seq_index = 0
        max_param = 0

        while i < n:
            ch = sql[i]
            if state == "NORMAL":
                if ch == "'":
                    out.append(ch)
                    state = "SINGLE_QUOTE"
                    i += 1
                elif ch == '"':
                    out.append(ch)
                    state = "DOUBLE_QUOTE"
                    i += 1
                elif ch == "[":
                    out.append(ch)
                    state = "BRACKET"
                    i += 1
                elif ch == "-" and i + 1 < n and sql[i + 1] == "-":
                    out.append("--")
                    state = "LINE_COMMENT"
                    i += 2
                elif ch == "/" and i + 1 < n and sql[i + 1] == "*":
                    out.append("/*")
                    state = "BLOCK_COMMENT"
                    i += 2
                elif ch == "?":
                    seq_index += 1
                    max_param = max(max_param, seq_index)
                    out.append(f"@P{seq_index}")
                    i += 1
                elif ch == "$" and i + 1 < n and sql[i + 1].isdigit() and sql[i + 1] != "0":
                    j = i + 1
                    while j < n and sql[j].isdigit():
                        j += 1
                    num = int(sql[i + 1 : j])
                    seq_index = max(seq_index, num)
                    max_param = max(max_param, num)
                    out.append(f"@P{num}")
                    i = j
                else:
                    out.append(ch)
                    i += 1
            elif state == "SINGLE_QUOTE":
                out.append(ch)
                if ch == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        out.append("'")
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "DOUBLE_QUOTE":
                out.append(ch)
                if ch == '"':
                    if i + 1 < n and sql[i + 1] == '"':
                        out.append('"')
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "BRACKET":
                out.append(ch)
                if ch == "]":
                    if i + 1 < n and sql[i + 1] == "]":
                        out.append("]")
                        i += 2
                    else:
                        state = "NORMAL"
                        i += 1
                else:
                    i += 1
            elif state == "LINE_COMMENT":
                out.append(ch)
                i += 1
                if ch == "\n":
                    state = "NORMAL"
            elif state == "BLOCK_COMMENT":
                if ch == "*" and i + 1 < n and sql[i + 1] == "/":
                    out.append("*/")
                    state = "NORMAL"
                    i += 2
                else:
                    out.append(ch)
                    i += 1

        return "".join(out), max_param

    @staticmethod
    def _resolve_dialer_fields(
        config: MSSQLConfig,
        dialer: Any = None,
    ) -> tuple[str | None, str, str, bytes | None, bytes | None, bytes | None]:
        """Extract Cloud SQL instance, IP type, auth mode, and optional mTLS PEMs from `config` and `dialer`."""
        cloudsql_instance = config.cloud_sql_instance
        cloudsql_ip_type = config.cloudsql_ip_type
        cloudsql_auth_type = config.cloudsql_auth_type
        ca_pem = config.ca_pem
        client_cert_pem = config.client_cert_pem
        client_key_pem = config.client_key_pem

        if dialer is not None:
            dialer_cfg = getattr(dialer, "config", None)
            if dialer_cfg is not None and not cloudsql_instance:
                cloudsql_instance = getattr(dialer_cfg, "instance_connection_name", None)
                cloudsql_ip_type = str(getattr(dialer_cfg, "ip_type", cloudsql_ip_type))
                if getattr(dialer_cfg, "enable_iam_auth", False):
                    cloudsql_auth_type = "IAM"
            bundle = getattr(dialer, "_cached_cert_bundle", None)
            if bundle is not None:
                if ca_pem is None:
                    ca_pem = getattr(bundle, "ca_cert_pem", None)
                if client_cert_pem is None:
                    client_cert_pem = getattr(bundle, "client_cert_pem", None)
                if client_key_pem is None:
                    client_key_pem = getattr(bundle, "private_key_pem", None)

        return (
            cloudsql_instance,
            cloudsql_ip_type,
            cloudsql_auth_type,
            ca_pem,
            client_cert_pem,
            client_key_pem,
        )


def rewrite_mssql_placeholders(sql: str) -> tuple[str, int]:
    """Rewrite `?` and `$1`/`$2` placeholders to T-SQL `@P1, @P2, ...` outside quotes, brackets, and comments."""
    return MSSQLCore.rewrite_placeholders(sql)
