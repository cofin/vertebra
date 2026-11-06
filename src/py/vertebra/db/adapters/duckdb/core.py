"""DuckDB analytical SQL inspection and GCS secret helpers."""

from __future__ import annotations

from vertebra.db.driver._common import DUCKDB_DIALECT_TAG

__all__ = (
    "DUCKDB_DIALECT_TAG",
    "DuckDBCore",
)


class DuckDBCore:
    """Protocol and SQL helpers for DuckDB analytical execution."""

    @staticmethod
    def is_write_statement(sql: str) -> bool:
        """Return True if the SQL statement mutates state and requires the single-writer lock."""
        head = sql.lstrip().split(None, 1)
        if not head:
            return False
        verb = head[0].upper()
        return verb in {
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "COPY",
            "MERGE",
            "TRUNCATE",
            "ATTACH",
        }
