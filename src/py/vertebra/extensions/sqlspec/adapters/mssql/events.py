"""SQL Server (MSSQL) event queue store with T-SQL `IF OBJECT_ID` DDL guards."""

from typing import Any, Final

from sqlspec.extensions.events import BaseEventQueueStore

from vertebra.extensions.sqlspec.adapters.mssql.core import (
    escape_sql_literal,
    mssql_table_ref,
)

__all__ = ("MSSQLEventQueueStore",)

_NVARCHAR_MAX_THRESHOLD: Final[int] = 4000


class MSSQLEventQueueStore(BaseEventQueueStore[Any]):
    """T-SQL event queue store guarded by `IF OBJECT_ID` and `sys.indexes` checks."""

    __slots__ = ()

    adapter_name = "vertebra_mssql"

    def _column_types(self) -> tuple[str, str, str]:
        """Return `(payload_type, metadata_type, timestamp_type)` for SQL Server DDL."""
        return "NVARCHAR(MAX)", "NVARCHAR(MAX)", "DATETIMEOFFSET(6)"

    def _string_type(self, length: int) -> str:
        """Return bounded `NVARCHAR(n)` or `NVARCHAR(MAX)` for large string columns."""
        if length >= _NVARCHAR_MAX_THRESHOLD:
            return "NVARCHAR(MAX)"
        return f"NVARCHAR({length})"

    def _integer_type(self) -> str:
        """Return the T-SQL integer type for queue attempt counters."""
        return "INT"

    def _timestamp_default(self) -> str:
        """Return the SQL Server UTC offset timestamp default."""
        return "SYSDATETIMEOFFSET()"

    def _wrap_create_statement(self, statement: str, object_type: str) -> str:
        """Wrap T-SQL table or index creation inside idempotent existence guards."""
        object_name = escape_sql_literal(mssql_table_ref(self.table_name))
        if object_type == "table":
            return f"IF OBJECT_ID(N'{object_name}', N'U') IS NULL BEGIN {statement}; END"
        if object_type == "index":
            index_name = escape_sql_literal(self._index_name())
            return (
                f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{index_name}' "
                f"AND object_id = OBJECT_ID(N'{object_name}')) BEGIN {statement}; END"
            )
        return statement

    def _wrap_drop_statement(self, statement: str) -> str:
        """Wrap T-SQL `DROP TABLE` inside `IF OBJECT_ID` existence checks."""
        object_name = escape_sql_literal(mssql_table_ref(self.table_name))
        return f"IF OBJECT_ID(N'{object_name}', N'U') IS NOT NULL {statement};"
