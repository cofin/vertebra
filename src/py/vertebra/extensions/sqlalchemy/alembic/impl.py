"""Alembic DefaultImpl subclasses registered via __dialect__ for custom Vertebra dialects."""

from __future__ import annotations

import inspect
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from alembic import util
from alembic.ddl.impl import DefaultImpl
from sqlalchemy import schema, text
from sqlalchemy.util.concurrency import await_

from vertebra.utils.module_loader import import_optional

if TYPE_CHECKING:
    from sqlalchemy.engine.cursor import CursorResult
    from sqlalchemy.sql.base import Executable

__all__ = (
    "BigQueryAlembicImpl",
    "DuckDBAlembicImpl",
    "SpangresAlembicImpl",
    "SpannerAlembicImpl",
)


def _resolve_update_ddl_target(connection: Any) -> Any:
    if connection is None:
        return None
    fairy = getattr(connection, "connection", None)
    dbapi_conn = getattr(fairy, "dbapi_connection", None) if fairy is not None else None
    async_inner = getattr(dbapi_conn, "_connection", None) if dbapi_conn is not None else None
    candidates = (
        connection,
        fairy,
        dbapi_conn,
        getattr(dbapi_conn, "driver", None) if dbapi_conn is not None else None,
        getattr(dbapi_conn, "_driver", None) if dbapi_conn is not None else None,
        async_inner,
        getattr(async_inner, "driver", None) if async_inner is not None else None,
        getattr(async_inner, "_driver", None) if async_inner is not None else None,
    )
    for candidate in candidates:
        if candidate is not None and callable(getattr(candidate, "update_ddl", None)):
            return candidate
    return None


class DuckDBAlembicImpl(DefaultImpl):
    """Alembic DefaultImpl for DuckDB dialects."""

    __dialect__ = "duckdb"
    transactional_ddl: bool = True


import_optional("sqlalchemy_bigquery.base")


class BigQueryAlembicImpl(DefaultImpl):
    """Alembic DefaultImpl for Google BigQuery dialects."""

    __dialect__ = "bigquery"
    transactional_ddl: bool = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize BigQueryAlembicImpl."""
        if args or kwargs:
            super().__init__(*args, **kwargs)


class SpannerAlembicImpl(DefaultImpl):
    """DDL-batching Alembic DefaultImpl for Cloud Spanner GoogleSQL dialects."""

    __dialect__ = "spanner"
    transactional_ddl: bool = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize SpannerAlembicImpl with an empty DDL batch buffer."""
        if args or kwargs:
            super().__init__(*args, **kwargs)
        self._ddl_buffer: list[str] = []

    def _flush_ddl_buffer(self) -> Any:
        if not self._ddl_buffer:
            return None
        statements = list(self._ddl_buffer)
        self._ddl_buffer.clear()
        target = _resolve_update_ddl_target(self.connection)
        if target is not None:
            result = target.update_ddl(statements)
            if inspect.isawaitable(result):
                return await_(result)
            return result
        for stmt in statements:
            super()._exec(text(stmt))
        return None

    def _exec(
        self,
        construct: Executable | str,
        execution_options: Mapping[str, Any] | None = None,
        multiparams: Sequence[Mapping[str, Any]] | None = None,
        params: Mapping[str, Any] = util.immutabledict(),
    ) -> CursorResult[Any] | None:
        """Batch DDLElement statements in online mode and flush before DML or commit."""
        if self.as_sql:
            return super()._exec(
                construct,
                execution_options=execution_options,
                multiparams=multiparams,
                params=params,
            )
        if isinstance(construct, schema.DDLElement):
            compiled = construct.compile(dialect=self.dialect)
            stmt = str(compiled).strip().rstrip(";")
            if stmt:
                self._ddl_buffer.append(stmt)
            return None
        self._flush_ddl_buffer()
        return super()._exec(
            construct,
            execution_options=execution_options,
            multiparams=multiparams,
            params=params,
        )

    def emit_commit(self) -> None:
        """Flush buffered DDL statements via update_ddl in online mode, or emit COMMIT in offline mode."""
        if self.as_sql:
            super().emit_commit()
            return
        self._flush_ddl_buffer()


class SpangresAlembicImpl(SpannerAlembicImpl):
    """DDL-batching Alembic DefaultImpl for Cloud Spanner Spangres dialects."""

    __dialect__ = "spangres"
    transactional_ddl: bool = False
