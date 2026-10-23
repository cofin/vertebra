"""SQLAlchemy 2.1 dialects, Vector/BinaryJson types, Alembic DefaultImpl, and ORM optimizations."""

from __future__ import annotations

import array
import struct
from typing import TYPE_CHECKING, Any, Literal

import msgspec

from vertebra.extensions.sqlalchemy.alembic import (
    AlembicImpl,
    AlembicRunner,
    CreateVectorIndexOp,
    CreateViewOp,
    DropVectorIndexOp,
    DropViewOp,
)
from vertebra.extensions.sqlalchemy.dialects import (
    ALL_DIALECT_SCHEMES,
    DIALECT_SCHEMES,
    DBAPIConnection,
    DBAPICursor,
    DBAPIModule,
    DialectDescriptor,
    DuckdbDialect,
    MssqlDialect,
    MysqlDialect,
    OracleDialect,
    PostgresDialect,
    SqliteDialect,
    SyncEngineAdapter,
    attach_serverless_disconnect_guard,
)
from vertebra.utils.serializers import decode_json, encode_json

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = (
    "ALL_DIALECT_SCHEMES",
    "DIALECT_SCHEMES",
    "AlembicImpl",
    "AlembicRunner",
    "BinaryJson",
    "CreateVectorIndexOp",
    "CreateViewOp",
    "DBAPIConnection",
    "DBAPICursor",
    "DBAPIModule",
    "DeleteUsingClause",
    "DialectDescriptor",
    "DropVectorIndexOp",
    "DropViewOp",
    "DuckdbDialect",
    "InsertSentinelSpec",
    "MssqlDialect",
    "MysqlDialect",
    "OracleDialect",
    "ParameterizedSql",
    "PostgresDialect",
    "SelectInLoadSpec",
    "SqliteDialect",
    "SyncEngineAdapter",
    "Vector",
    "VectorDistanceExpr",
    "attach_serverless_disconnect_guard",
)

type VectorStorageFormat = Literal["float32", "float64", "int8", "binary"]
type VectorDistanceMetric = Literal["cosine", "l2", "inner_product"]


class VectorDistanceExpr:
    """Dialect-aware SQL expression descriptor for vector similarity operations."""

    __slots__ = ("_column_name", "_metric", "_param_name", "_query_vector")

    def __init__(
        self,
        column_name: str,
        query_vector: Sequence[float],
        *,
        metric: VectorDistanceMetric = "cosine",
        param_name: str = "query_vec",
    ) -> None:
        """Initialize vector distance expression with target column, vector, and metric."""
        self._column_name = column_name
        self._query_vector = tuple(float(v) for v in query_vector)
        self._metric = metric
        self._param_name = param_name

    @property
    def column_name(self) -> str:
        """Return the target vector column name."""
        return self._column_name

    @property
    def query_vector(self) -> tuple[float, ...]:
        """Return the query vector values."""
        return self._query_vector

    @property
    def metric(self) -> VectorDistanceMetric:
        """Return the similarity metric."""
        return self._metric

    @property
    def param_name(self) -> str:
        """Return the bind parameter name."""
        return self._param_name

    def compile_sql(self, dialect_name: str = "postgresql") -> str:
        """Compile the vector distance expression for the target database dialect."""
        base = dialect_name.split("+", 1)[0].lower()
        if base == "oracle":
            ora_metric = {
                "cosine": "COSINE",
                "l2": "EUCLIDEAN",
                "inner_product": "DOT",
            }[self._metric]
            return f"VECTOR_DISTANCE({self._column_name}, :{self._param_name}, {ora_metric})"
        if base in ("postgres", "postgresql"):
            op = {
                "cosine": "<=>",
                "l2": "<->",
                "inner_product": "<#>",
            }[self._metric]
            return f"{self._column_name} {op} :{self._param_name}"
        if base in ("mysql", "mariadb"):
            fn = {
                "cosine": "DISTANCE_COSINE",
                "l2": "DISTANCE_EUCLIDEAN",
                "inner_product": "DISTANCE_DOT",
            }[self._metric]
            return f"{fn}({self._column_name}, :{self._param_name})"
        return f"vec_distance_{self._metric}({self._column_name}, :{self._param_name})"


class Vector:
    """Cross-database dense vector column descriptor supporting PostgreSQL, Oracle 26ai, MySQL 9.0, and SQLite."""

    cache_ok: bool = True
    __slots__ = ("column_name", "dim", "storage_format")

    def __init__(
        self,
        dim: int | None = None,
        *,
        storage_format: VectorStorageFormat = "float32",
        column_name: str = "embedding",
    ) -> None:
        """Initialize vector column descriptor with dimension and storage format."""
        self.dim = dim
        self.storage_format: VectorStorageFormat = storage_format
        self.column_name = column_name

    def get_col_spec(self, dialect_name: str = "postgresql") -> str:
        """Render the DDL column type specification for the given dialect."""
        base = dialect_name.split("+", 1)[0].lower()
        if base == "oracle":
            ora_fmt = {
                "float32": "FLOAT32",
                "float64": "FLOAT64",
                "int8": "INT8",
                "binary": "BINARY",
            }[self.storage_format]
            if self.dim is None:
                return f"VECTOR(*, {ora_fmt})"
            return f"VECTOR({self.dim}, {ora_fmt})"
        if base in ("postgres", "postgresql", "mysql", "mariadb"):
            return f"VECTOR({self.dim})" if self.dim is not None else "VECTOR"
        return "BLOB"

    def process_bind_param(
        self, value: Sequence[float] | array.array[float] | None, dialect_name: str = "postgresql"
    ) -> Any:
        """Convert a Python float sequence into a dialect-optimal bind representation."""
        if value is None:
            return None
        base = dialect_name.split("+", 1)[0].lower()
        typecode = "d" if self.storage_format == "float64" else "f"
        if base == "oracle":
            if isinstance(value, array.array):
                return value
            return array.array(typecode, value)
        if base == "sqlite":
            arr = value if isinstance(value, array.array) else array.array(typecode, value)
            return arr.tobytes()
        return "[" + ",".join(str(float(x)) for x in value) + "]"

    def process_result_value(self, value: Any, dialect_name: str = "postgresql") -> list[float] | None:
        """Decode a database vector result into a Python list of floats."""
        if value is None:
            return None
        if isinstance(value, array.array):
            return list(value)
        if isinstance(value, list):
            return [float(x) for x in value]
        if isinstance(value, (bytes, bytearray, memoryview)) and dialect_name.startswith("sqlite"):
            raw = bytes(value)
            count = len(raw) // 4
            return list(struct.unpack(f"<{count}f", raw))
        if isinstance(value, str):
            trimmed = value.strip().lstrip("[").rstrip("]")
            return [float(item) for item in trimmed.split(",") if item.strip()] if trimmed else []
        return list(value)

    def cosine_distance(self, query_vector: Sequence[float], *, param_name: str = "query_vec") -> VectorDistanceExpr:
        """Build a cosine distance expression for this vector column."""
        return VectorDistanceExpr(self.column_name, query_vector, metric="cosine", param_name=param_name)

    def l2_distance(self, query_vector: Sequence[float], *, param_name: str = "query_vec") -> VectorDistanceExpr:
        """Build an L2 / Euclidean distance expression for this vector column."""
        return VectorDistanceExpr(self.column_name, query_vector, metric="l2", param_name=param_name)

    def inner_product(self, query_vector: Sequence[float], *, param_name: str = "query_vec") -> VectorDistanceExpr:
        """Build an inner-product / dot-product expression for this vector column."""
        return VectorDistanceExpr(self.column_name, query_vector, metric="inner_product", param_name=param_name)


class BinaryJson:
    """Cross-database binary JSON column descriptor (JSONB on PG and SQLite 3.45+, OSON JSON on Oracle 26ai)."""

    cache_ok: bool = True
    __slots__ = ()

    def get_col_spec(self, dialect_name: str = "postgresql") -> str:
        """Render the DDL column type specification for the target dialect."""
        base = dialect_name.split("+", 1)[0].lower()
        if base in ("postgres", "postgresql", "sqlite"):
            return "JSONB"
        return "JSON"

    def process_bind_param(self, value: Any, dialect_name: str = "postgresql") -> bytes | str | None:
        """Encode a Python value into UTF-8 JSON via vertebra.utils.serializers."""
        _ = dialect_name
        if value is None:
            return None
        if isinstance(value, (bytes, str)):
            return value
        return encode_json(value)

    def process_result_value(self, value: Any, dialect_name: str = "postgresql") -> Any:
        """Decode a JSON/JSONB/OSON payload via vertebra.utils.serializers."""
        _ = dialect_name
        if value is None or isinstance(value, (dict, list)):
            return value
        if isinstance(value, memoryview):
            raw = bytes(value)
            if raw.startswith(b"\x01"):
                raw = raw[1:]
            return decode_json(raw)
        if isinstance(value, (bytes, bytearray)):
            raw_b = bytes(value)
            if raw_b.startswith(b"\x01"):
                raw_b = raw_b[1:]
            return decode_json(raw_b)
        if isinstance(value, str):
            return decode_json(value)
        return value


class SelectInLoadSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 selectinload(chunksize=..., omit_join=...) configuration."""

    relationship_name: str
    chunksize: int = 500
    omit_join: bool = True
    recursion_depth: int | None = None

    @property
    def batch_size(self) -> int:
        """Return the batch chunk size."""
        return self.chunksize

    def num_batches(self, total_keys: int) -> int:
        """Return the number of IN (...) batches required for total_keys."""
        if total_keys <= 0:
            return 0
        return (total_keys + self.chunksize - 1) // self.chunksize

    @classmethod
    def create(
        cls,
        relationship_name: str | Any,
        *,
        recursion_depth: int | None = None,
        batch_size: int | None = None,
        chunksize: int = 500,
        omit_join: bool = True,
    ) -> SelectInLoadSpec:
        """Configure SQLAlchemy 2.1 selectinload with Oracle-safe chunksize and omit_join=True."""
        effective_chunksize = batch_size if batch_size is not None else chunksize
        if effective_chunksize <= 0 or effective_chunksize > 1000:
            msg = f"selectinload chunksize must be between 1 and 1000 (got {effective_chunksize})"
            raise ValueError(msg)
        name = (
            relationship_name
            if isinstance(relationship_name, str)
            else getattr(relationship_name, "key", str(relationship_name))
        )
        return cls(
            relationship_name=name,
            chunksize=effective_chunksize,
            omit_join=omit_join,
            recursion_depth=recursion_depth,
        )


class InsertSentinelSpec(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 orm_insert_sentinel() descriptor for deterministic bulk INSERT ... RETURNING."""

    column_name: str = "_insert_sentinel"
    table_name: str = ""
    page_size: int = 1000
    use_insertmanyvalues: bool = True
    insert_executemany_returning: bool = True

    @property
    def sentinel_column(self) -> str:
        """Return the sentinel column name."""
        return self.column_name

    @classmethod
    def create(
        cls,
        table_name: str = "",
        *,
        sentinel_column: str = "_insert_sentinel",
        column_name: str | None = None,
        page_size: int = 1000,
    ) -> InsertSentinelSpec:
        """Configure SQLAlchemy 2.1 orm_insert_sentinel() for bulk INSERT ... RETURNING correlation."""
        resolved_col = column_name if column_name is not None else sentinel_column
        return cls(
            column_name=resolved_col,
            table_name=table_name,
            page_size=page_size,
        )


class DeleteUsingClause(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """SQLAlchemy 2.1 Delete.using() multi-table delete descriptor."""

    target_table: str
    using_tables: tuple[str, ...]
    where_sql: str | None = None

    def to_sql(self) -> str:
        """Render the DELETE FROM target USING tables WHERE statement."""
        using_part = ", ".join(self.using_tables)
        stmt = f"DELETE FROM {self.target_table} USING {using_part}"
        if self.where_sql:
            stmt = f"{stmt} WHERE {self.where_sql}"
        return stmt

    @classmethod
    def create(
        cls,
        target_table: str,
        *using_tables: str,
        join_condition: str | None = None,
        where_condition: str | None = None,
        where_sql: str | None = None,
    ) -> DeleteUsingClause:
        """Build a SQLAlchemy 2.1 Delete.using() multi-table delete statement."""
        if not using_tables:
            msg = "Delete.using() requires at least one USING table"
            raise ValueError(msg)
        resolved_tables = list(using_tables)
        inferred_join: str | None = None
        if len(resolved_tables) >= 2 and ("=" in resolved_tables[-1] or " " in resolved_tables[-1].strip()):
            inferred_join = resolved_tables.pop()
        conditions = [c for c in (inferred_join, join_condition, where_condition, where_sql) if c]
        combined_where = " AND ".join(conditions) if conditions else None
        return cls(
            target_table=target_table,
            using_tables=tuple(resolved_tables),
            where_sql=combined_where,
        )


class ParameterizedSql(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Compiled parameterized SQL statement produced from a PEP 750 / SQLAlchemy 2.1 tstring."""

    sql: str
    params: dict[str, Any]

    @classmethod
    def from_template(
        cls,
        template: Any,
        parts: Sequence[Any] | None = None,
        *,
        param_prefix: str = "p",
    ) -> ParameterizedSql:
        """Compile static template parts and interpolated values into a bind-parameterized SQL query."""
        if hasattr(template, "strings") and hasattr(template, "interpolations"):
            static_parts: Sequence[str] = tuple(template.strings)
            interpolated_values: Sequence[Any] = tuple(
                getattr(interp, "value", interp) for interp in template.interpolations
            )
        elif isinstance(template, str):
            raw_pairs = list(parts or ())
            return cls(sql=template, params=dict(raw_pairs))
        else:
            static_parts = tuple(template)
            interpolated_values = tuple(parts or ())

        if len(static_parts) != len(interpolated_values) + 1:
            msg = "static_parts must have length len(interpolated_values) + 1"
            raise ValueError(msg)
        fragments: list[str] = [static_parts[0]]
        params: dict[str, Any] = {}
        for idx, val in enumerate(interpolated_values, start=1):
            pname = f"{param_prefix}_{idx}"
            params[pname] = val
            fragments.append(f":{pname}")
            fragments.append(static_parts[idx])
        return cls(sql="".join(fragments), params=params)
