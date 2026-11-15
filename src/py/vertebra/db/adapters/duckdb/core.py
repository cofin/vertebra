"""DuckDB analytical SQL inspection helpers."""

from __future__ import annotations

import datetime
import os
import re
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any
from uuid import UUID

from vertebra.db.driver._common import DUCKDB_DIALECT_TAG
from vertebra.utils.serializers import encode_json

__all__ = (
    "DUCKDB_DIALECT_TAG",
    "DuckDBCore",
)

_CREDENTIAL_REJECTION_PATTERN = re.compile(r"\(HTTP (?:401|403)\)")

_MUTATING_SQL_VERBS: frozenset[str] = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "COPY",
        "MERGE",
        "TRUNCATE",
        "VACUUM",
        "ATTACH",
        "DETACH",
    }
)


class DuckDBCore:
    """Protocol and SQL helpers for DuckDB analytical execution."""

    @staticmethod
    def is_write_statement(sql: str) -> bool:
        """Return True if the SQL statement mutates state outside comments and quoted literals."""
        tokens = DuckDBCore._scan_sql_keywords_outside_literals(sql)
        if not tokens:
            return False
        first = tokens[0]
        if first in _MUTATING_SQL_VERBS:
            return True
        if first == "WITH":
            return any(tok in _MUTATING_SQL_VERBS for tok in tokens[1:])
        return False

    @staticmethod
    def normalize_param(value: Any) -> Any:
        """Normalize rich Python bind values into DuckDB-compatible scalar types."""
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return value.isoformat()
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, (dict, list, tuple)):
            return encode_json(value)
        return value

    @staticmethod
    def normalize_parameters(
        parameters: Sequence[Any] | Mapping[str, Any] | None,
    ) -> tuple[Any, ...] | dict[str, Any] | None:
        """Normalize positional or named bind parameters for `vertebra._duckdb`."""
        if parameters is None:
            return None
        if isinstance(parameters, Mapping):
            if not parameters:
                return None
            return {str(key): DuckDBCore.normalize_param(val) for key, val in parameters.items()}
        if not parameters:
            return None
        return tuple(DuckDBCore.normalize_param(val) for val in parameters)

    @staticmethod
    def param_len(bound: tuple[Any, ...] | dict[str, Any] | None) -> int:
        """Return the parameter count for statement cache registration."""
        if bound is None:
            return 0
        return len(bound)

    @staticmethod
    def acquire_broker_lease(broker: Any, sql: str, param_count: int) -> Any:
        """Register SQL in the broker statement cache and acquire a lease grant."""
        if broker is None:
            return None
        if hasattr(broker, "statement_cache"):
            sc_attr = broker.statement_cache
            sc = sc_attr() if callable(sc_attr) else sc_attr
            if sc is not None and hasattr(sc, "lookup") and hasattr(sc, "insert"):
                sql_bytes = sql.encode("utf-8")
                try:
                    cached = sc.lookup(DUCKDB_DIALECT_TAG, sql)
                    if cached is None:
                        sc.insert(DUCKDB_DIALECT_TAG, sql, param_count, sql_bytes, b"")
                except TypeError:
                    cached = sc.lookup(DUCKDB_DIALECT_TAG, sql_bytes)
                    if cached is None:
                        sc.insert(DUCKDB_DIALECT_TAG, sql_bytes, param_count, sql_bytes, b"")
        if hasattr(broker, "lease_table") and broker.lease_table is not None:
            return broker.lease_table.acquire(os.getpid())
        if hasattr(broker, "acquire"):
            try:
                return broker.acquire(os.getpid())
            except TypeError:
                return broker.acquire(bind_key="duckdb")
        return None

    @staticmethod
    def release_broker_lease(broker: Any, grant: Any) -> None:
        """Release a previously acquired broker lease grant."""
        if grant is not None and broker is not None:
            if hasattr(broker, "lease_table") and broker.lease_table is not None:
                broker.lease_table.release(grant.slot_index, grant.generation)
            elif hasattr(broker, "release") and hasattr(grant, "slot_index"):
                broker.release(grant.slot_index, grant.generation)

    @staticmethod
    def invalidate_rejected_credentials(
        exc: BaseException,
        dynamic_secrets: Sequence[Any],
        secret_generations: dict[str, int],
    ) -> None:
        """Invalidate tracked dynamic secret generations on HTTP 401/403 httpfs rejections."""
        if not _CREDENTIAL_REJECTION_PATTERN.search(str(exc)):
            return
        for secret in dynamic_secrets:
            provider = secret.value_provider
            if provider is None:
                continue
            key = secret.name.lower()
            applied_gen = secret_generations.get(key)
            if applied_gen is None:
                continue
            target = getattr(provider, "__self__", provider)
            invalidate_fn = getattr(target, "invalidate", None)
            if callable(invalidate_fn):
                invalidate_fn(applied_gen)
                secret_generations.pop(key, None)

    @staticmethod
    def probe_provider_generation(provider: Any) -> int | None:
        """Read an integer generation counter from a dynamic secret provider if exposed."""
        target = getattr(provider, "__self__", provider)
        gen = getattr(target, "generation", None)
        if isinstance(gen, int) and not isinstance(gen, bool):
            return gen
        return None

    @staticmethod
    def probe_provider_needs_refresh(provider: Any) -> bool:
        """Query whether a dynamic secret provider signals an urgent credential refresh."""
        target = getattr(provider, "__self__", provider)
        fn = getattr(target, "needs_refresh", None)
        if callable(fn):
            return bool(fn())
        return False

    @staticmethod
    def _scan_sql_keywords_outside_literals(sql: str) -> list[str]:
        """Extract uppercased SQL identifier/keyword tokens outside comments and quoted literals."""
        tokens: list[str] = []
        length = len(sql)
        idx = 0
        while idx < length:
            ch = sql[idx]
            if ch.isspace():
                idx += 1
                continue
            if ch == "-" and idx + 1 < length and sql[idx + 1] == "-":
                idx += 2
                while idx < length and sql[idx] != "\n":
                    idx += 1
                continue
            if ch == "/" and idx + 1 < length and sql[idx + 1] == "*":
                idx += 2
                while idx < length and not (sql[idx] == "*" and idx + 1 < length and sql[idx + 1] == "/"):
                    idx += 1
                idx = min(idx + 2, length)
                continue
            if ch in ("'", '"'):
                quote = ch
                idx += 1
                while idx < length:
                    if sql[idx] == quote:
                        idx += 1
                        if idx < length and sql[idx] == quote:
                            idx += 1
                        else:
                            break
                    else:
                        idx += 1
                continue
            if ch.isalpha() or ch == "_":
                start = idx
                idx += 1
                while idx < length and (sql[idx].isalnum() or sql[idx] == "_"):
                    idx += 1
                tokens.append(sql[start:idx].upper())
                continue
            idx += 1
        return tokens
