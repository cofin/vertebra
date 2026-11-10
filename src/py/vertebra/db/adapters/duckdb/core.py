"""DuckDB analytical SQL inspection helpers."""

from __future__ import annotations

from vertebra.db.driver._common import DUCKDB_DIALECT_TAG

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
        tokens = _scan_sql_keywords_outside_literals(sql)
        if not tokens:
            return False
        first = tokens[0]
        if first in _MUTATING_SQL_VERBS:
            return True
        if first == "WITH":
            return any(tok in _MUTATING_SQL_VERBS for tok in tokens[1:])
        return False


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
            while idx + 1 < length and not (sql[idx] == "*" and sql[idx + 1] == "/"):
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
