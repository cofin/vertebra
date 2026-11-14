"""DuckDB analytical SQL inspection helpers."""

from __future__ import annotations

import datetime
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
                while idx < length and not (sql[idx] == "*" and sql[idx + 1] == "/"):
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
